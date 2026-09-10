import assert from "node:assert/strict";
import { appendFile, mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createServer } from "node:net";
import test from "node:test";
import { stringify } from "yaml";
import { startDaemon } from "../src/daemon.js";
import { AcpExecutor } from "../src/executors/acp.js";
import { ProcessExecutor } from "../src/executors/process.js";
import type { AcpRequest, Execution, Executor, Hooks } from "../src/executors/types.js";
import { EventBus, RpcClient } from "../src/rpc.js";
import { Scheduler } from "../src/scheduler.js";
import { Store } from "../src/store.js";
import { Transcripts } from "../src/transcript.js";
import type { Config, Json, RunView, TaskResult } from "../src/types.js";
import { DagmarError } from "../src/types.js";
import { WorkflowRepository } from "../src/workflow.js";

test("process DAG runs a diamond concurrently and persists transcripts", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-diamond-"));
  const script = join(root, "task.mjs");
  await writeFile(script, `
import {existsSync,writeFileSync} from 'node:fs';
const input=[]; for await (const c of process.stdin) input.push(c); const data=JSON.parse(input.join(''));
const lane=process.argv[2], peer=process.argv[3], dir=process.argv[4];
if (peer) { writeFileSync(dir+'/'+lane,''); while(!existsSync(dir+'/'+peer)) await new Promise(r=>setTimeout(r,10)); }
console.log(JSON.stringify({outcome:'completed',message:lane,output:{lane,input:data}}));
`);
  const workflow = {
    id: "diamond",
    tasks: {
      a: { executor: "local", inputs: { request: "$run.input" }, run: [process.execPath, script, "a", "", root] },
      b: { executor: "local", dependsOn: ["a"], inputs: { fromA: "$tasks.a.output.lane" }, run: [process.execPath, script, "b", "c", root] },
      c: { executor: "local", dependsOn: ["a"], inputs: { fromA: "$tasks.a.output.lane" }, run: [process.execPath, script, "c", "b", root] },
      d: { executor: "local", dependsOn: ["b", "c"], inputs: { b: "$tasks.b.output.lane", c: "$tasks.c.output.lane" }, run: [process.execPath, script, "d", "", root] },
    },
  };
  const app = await fixture(root, workflow);
  const started = await app.scheduler.start("diamond", { hello: "world" });
  const view = await waitFor(app.scheduler, started.workflowRunId, "completed");
  assert.deepEqual(Object.fromEntries(Object.entries(view.tasks).map(([id, x]) => [id, x.state])), { a: "completed", b: "completed", c: "completed", d: "completed" });
  assert.equal(view.tasks.b!.attempts.length, 1);
  const transcript = await app.transcripts.read(view.id, view.tasks.b!.attempts[0]!.id);
  assert.ok(transcript.records.some((x) => x.type === "stdio" && x.stream === "stdout"));
  app.store.close();
});

test("blocked runs resume only the blocker and cancellation is terminal", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-resume-")), marker = join(root, "marker");
  const block = join(root, "block.mjs"), wait = join(root, "wait.mjs");
  await writeFile(block, `import{existsSync,writeFileSync}from'node:fs';for await(const _ of process.stdin){};const first=!existsSync(${JSON.stringify(marker)});if(first)writeFileSync(${JSON.stringify(marker)},'');console.log(JSON.stringify({outcome:first?'blocked':'completed',message:'x',output:{}}));`);
  await writeFile(wait, `for await(const _ of process.stdin){};setTimeout(()=>{},10000);`);
  const app = await fixture(root, { id: "resume", tasks: { step: { executor: "local", inputs: {}, run: [process.execPath, block] } } });
  const started = await app.scheduler.start("resume", {});
  const blocked = await waitFor(app.scheduler, started.workflowRunId, "blocked");
  assert.equal(blocked.tasks.step!.attempts.length, 1);
  const completed = await app.scheduler.resume(started.workflowRunId);
  const resumed = completed.status === "completed" ? completed : await waitFor(app.scheduler, started.workflowRunId, "completed");
  assert.equal(resumed.tasks.step!.attempts.length, 2);

  await writeFile(join(root, "workflows", "resume.yaml"), stringify({ id: "resume", tasks: { step: { executor: "local", inputs: {}, run: [process.execPath, wait] } } }));
  const second = await app.scheduler.start("resume", {});
  await waitFor(app.scheduler, second.workflowRunId, "running", true);
  const cancelled = await app.scheduler.cancelRun(second.workflowRunId);
  assert.equal(cancelled.status, "cancelled");
  assert.equal(cancelled.tasks.step!.state, "cancelled");
  app.store.close();
});

test("a failed branch does not stop its sibling and task cancellation blocks dependants", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-branches-")), bad = join(root, "bad.mjs"), good = join(root, "good.mjs"), wait = join(root, "wait.mjs");
  await writeFile(bad, `for await(const _ of process.stdin){};console.log('noise');console.log(JSON.stringify({outcome:'completed',message:'bad',output:{}}));`);
  await writeFile(good, `for await(const _ of process.stdin){};await new Promise(r=>setTimeout(r,50));console.log(JSON.stringify({outcome:'completed',message:'good',output:{}}));`);
  await writeFile(wait, `for await(const _ of process.stdin){};setInterval(()=>{},1000);`);
  const app = await fixture(root, { id: "branches", tasks: { bad: { executor: "local", inputs: {}, run: [process.execPath, bad] }, good: { executor: "local", inputs: {}, run: [process.execPath, good] } } });
  const started = await app.scheduler.start("branches", {}), blocked = await waitFor(app.scheduler, started.workflowRunId, "blocked");
  assert.equal(blocked.tasks.bad!.state, "failed"); assert.equal(blocked.tasks.good!.state, "completed");
  await writeFile(join(root, "workflows", "branches.yaml"), stringify({ id: "branches", tasks: { wait: { executor: "local", inputs: {}, run: [process.execPath, wait] }, after: { executor: "local", dependsOn: ["wait"], inputs: {}, run: [process.execPath, good] } } }));
  const second = await app.scheduler.start("branches", {}); let running = await waitFor(app.scheduler, second.workflowRunId, "running", true);
  const taskRunId = running.tasks.wait!.attempts[0]!.id; running = await app.scheduler.cancelTask(taskRunId);
  assert.equal(running.status, "blocked"); assert.equal(running.tasks.after!.state, "blocked_by_dependency");
  app.store.close();
});

test("ACP interactions suspend and resume an attempt", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-interaction-"));
  const app = await fixture(root, { id: "acp", tasks: { ask: { executor: "agent", inputs: {}, prompt: "ask" } } }, new InteractiveExecutor());
  const started = await app.scheduler.start("acp", {});
  await waitFor(app.scheduler, started.workflowRunId, "waiting");
  const interaction = app.scheduler.interactions()[0]!;
  assert.equal(interaction.kind, "permission");
  await app.scheduler.answer(interaction.id, { outcome: { outcome: "selected", optionId: "yes" } });
  const completed = await waitFor(app.scheduler, started.workflowRunId, "completed");
  assert.equal(completed.tasks.ask!.state, "completed");
  app.store.close();
});

test("ACP executor speaks one fresh stdio session and validates its result", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-acp-")), script = join(root, "agent.mjs");
  await writeFile(script, `
import{createInterface}from'node:readline';
const send=x=>process.stdout.write(JSON.stringify(x)+'\\n');
createInterface({input:process.stdin}).on('line',line=>{const m=JSON.parse(line);
if(m.method==='initialize')send({jsonrpc:'2.0',id:m.id,result:{protocolVersion:1,agentCapabilities:{sessionCapabilities:{close:{}}}}});
if(m.method==='session/new')send({jsonrpc:'2.0',id:m.id,result:{sessionId:'fresh-1'}});
if(m.method==='session/prompt'){send({jsonrpc:'2.0',method:'session/update',params:{sessionId:'fresh-1',update:{sessionUpdate:'agent_message_chunk',content:{type:'text',text:'{"outcome":"completed","message":"ok","output":{}}'}}}});send({jsonrpc:'2.0',id:m.id,result:{stopReason:'end_turn'}})}
if(m.method==='session/close')send({jsonrpc:'2.0',id:m.id,result:{}});
});
`);
  const records: unknown[] = []; let session = "";
  const execution = await new AcpExecutor().start({ type: "acp", runId: "wr_x", taskRunId: "tr_x", taskId: "x", profile: "agent", cwd: root, env: process.env, inputs: {}, run: [process.execPath, script], prompt: "Do it" }, { transcript: async (record) => { records.push(record); return records.length; }, session: async (id) => { session = id; }, interact: async () => { throw new Error("unexpected"); } });
  const settled = await execution.done;
  assert.equal(session, "fresh-1");
  assert.equal("result" in settled && settled.result.outcome, "completed");
  assert.ok(records.some((x) => (x as { type?: string }).type === "acp"));
});

test("daemon exposes the process runtime through JSON-RPC", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-daemon-")), workflows = join(root, "workflows"), state = join(root, "state"), script = join(root, "done.mjs");
  await import("node:fs/promises").then((fs) => fs.mkdir(workflows, { recursive: true }));
  await writeFile(script, `for await(const _ of process.stdin){};console.log(JSON.stringify({outcome:'completed',message:'done',output:{}}));`);
  await writeFile(join(workflows, "one.yaml"), stringify({ id: "one", tasks: { task: { executor: "local", inputs: {}, run: [process.execPath, script] } } }));
  const port = await freePort(), config = join(root, "config.yaml");
  await writeFile(config, stringify({ workflowDir: workflows, storageDir: state, listen: { host: "127.0.0.1", port }, executors: { local: { type: "process", cwd: root, env: {} } } }));
  const daemon = await startDaemon(config), client = new RpcClient(`ws://127.0.0.1:${port}`); await client.connect();
  try {
    const ping = await client.request("system.ping") as JsonObject; assert.equal(ping.ok, true);
    const started = await client.request("run.start", { workflowId: "one", input: {} }) as JsonObject;
    let view: JsonObject = {}; for (let i = 0; i < 200; i++) { view = await client.request("run.get", { workflowRunId: started.workflowRunId! }) as JsonObject; if (view.status === "completed") break; await new Promise((resolve) => setTimeout(resolve, 10)); }
    assert.equal(view.status, "completed");
  } finally { client.close(); await daemon.stop(); }
});

test("startup recovery blocks persisted active attempts", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-recovery-"));
  const app = await fixture(root, { id: "recovery", tasks: { task: { executor: "local", inputs: {}, run: [process.execPath, "missing"] } } });
  const now = new Date().toISOString();
  app.store.insertRun({ id: "wr_recovery", workflowId: "recovery", input: {}, status: "running", startedAt: now, updatedAt: now, endedAt: null });
  app.store.insertAttempt({ id: "tr_recovery", workflowRunId: "wr_recovery", taskId: "task", attempt: 1, executorProfile: "local", executorType: "process", status: "running", result: null, error: null, acpSessionId: null, startedAt: now, updatedAt: now, endedAt: null });
  await app.scheduler.recover();
  assert.equal(app.store.run("wr_recovery")!.status, "blocked");
  assert.equal(app.store.attempt("tr_recovery")!.error!.code, "executor_lost");
  app.store.close();
});

test("graceful shutdown fails active attempts and waits for their process", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-shutdown-")), script = join(root, "wait.mjs");
  await writeFile(script, `for await(const _ of process.stdin){};setInterval(()=>{},1000);`);
  const app = await fixture(root, { id: "shutdown", tasks: { task: { executor: "local", inputs: {}, run: [process.execPath, script] } } });
  const started = await app.scheduler.start("shutdown", {}); await waitFor(app.scheduler, started.workflowRunId, "running", true);
  await app.scheduler.shutdown();
  assert.equal(app.store.run(started.workflowRunId)!.status, "blocked");
  assert.equal(app.store.attempts(started.workflowRunId)[0]!.error!.code, "daemon_shutdown");
  app.store.close();
});

// Regression: store.insertRun must map the active-run partial-index violation to the
// active_run_exists DagmarError (via SQLite's extended result code), not leak a raw error.
test("a second active run of the same workflow is rejected", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-active-")), wait = join(root, "wait.mjs");
  await writeFile(wait, `for await(const _ of process.stdin){};setInterval(()=>{},1000);`);
  const app = await fixture(root, { id: "solo", tasks: { task: { executor: "local", inputs: {}, run: [process.execPath, wait] } } });
  try {
    const first = await app.scheduler.start("solo", {});
    await waitFor(app.scheduler, first.workflowRunId, "running", true);
    await assert.rejects(app.scheduler.start("solo", {}), (error) => error instanceof DagmarError && error.code === "active_run_exists");
  } finally { await app.scheduler.shutdown(); app.store.close(); }
});

// Regression: a process task that never reads stdin (and exits first) makes our large
// stdin write fail with EPIPE. Without a stdin 'error' listener that is an unhandled
// exception that crashes the runtime; the attempt must instead settle as failed.
test("a process task that ignores stdin and exits does not crash the runtime", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-epipe-")), script = join(root, "exit.mjs");
  await writeFile(script, `process.exit(0);`);
  const app = await fixture(root, { id: "epipe", tasks: { task: { executor: "local", inputs: { blob: "$run.input" }, run: [process.execPath, script] } } });
  // A payload larger than the OS pipe buffer guarantees the write cannot flush before the
  // child's read end closes, so the EPIPE path is exercised deterministically.
  const started = await app.scheduler.start("epipe", { blob: "x".repeat(200_000) });
  const blocked = await waitFor(app.scheduler, started.workflowRunId, "blocked");
  assert.equal(blocked.tasks.task!.state, "failed");
  app.store.close();
});

// Regression: transcript.read must drop an incomplete final line left by a crash (the
// collapsed pop), returning only the complete records.
test("transcript.read drops an incomplete final line", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-transcript-"));
  const transcripts = new Transcripts(join(root, "state"));
  await transcripts.append("wr_read", "tr_read", { type: "lifecycle", direction: "internal", event: "a" });
  await transcripts.append("wr_read", "tr_read", { type: "lifecycle", direction: "internal", event: "b" });
  await appendFile(transcripts.path("wr_read", "tr_read"), '{"type":"lifecycle","direction":"internal","event":"c"');
  const read = await transcripts.read("wr_read", "tr_read");
  assert.equal(read.records.length, 2);
  assert.equal(read.nextLine, 2);
  assert.deepEqual(read.records.map((x) => (x as { event: string }).event), ["a", "b"]);
});

// Regression: once a transcript path goes idle its cached line count is evicted; the next
// append must re-derive the count from disk rather than restarting numbering at 1.
test("transcript line numbering survives idle eviction of cached counts", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-evict-"));
  const transcripts = new Transcripts(join(root, "state"));
  const n1 = await transcripts.append("wr_evict", "tr_evict", { type: "lifecycle", direction: "internal", event: "a" });
  const n2 = await transcripts.append("wr_evict", "tr_evict", { type: "lifecycle", direction: "internal", event: "b" });
  assert.deepEqual([n1, n2], [1, 2]);
  await new Promise((resolve) => setTimeout(resolve, 20)); // let idle eviction run
  const n3 = await transcripts.append("wr_evict", "tr_evict", { type: "lifecycle", direction: "internal", event: "c" });
  assert.equal(n3, 3);
  const read = await transcripts.read("wr_evict", "tr_evict");
  assert.equal(read.records.length, 3);
});

// Regression: WorkflowRepository.get resolves only the requested workflow, so an invalid
// sibling does not break it, and a duplicated id is not resolvable.
test("workflow.get resolves the requested workflow without being broken by invalid siblings", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-get-")), dir = join(root, "workflows");
  await mkdir(dir, { recursive: true });
  const profiles: Config["executors"] = { local: { type: "process", cwd: root, env: {} } };
  await writeFile(join(dir, "alpha.yaml"), stringify({ id: "alpha", tasks: { t: { executor: "local", inputs: {}, run: ["node"] } } }));
  await writeFile(join(dir, "beta.yaml"), stringify({ id: "beta", tasks: { t: { executor: "ghost", inputs: {} } } }));
  const repo = new WorkflowRepository(dir, profiles);
  assert.equal((await repo.get("alpha")).id, "alpha");
  await assert.rejects(repo.get("beta"), (error) => error instanceof DagmarError);
  await assert.rejects(repo.get("missing"), (error) => error instanceof DagmarError && error.code === "workflow_not_found");
  await writeFile(join(dir, "dup-a.yaml"), stringify({ id: "gamma", tasks: { t: { executor: "local", inputs: {}, run: ["node"] } } }));
  await writeFile(join(dir, "dup-b.yaml"), stringify({ id: "gamma", tasks: { t: { executor: "local", inputs: {}, run: ["node"] } } }));
  await assert.rejects(repo.get("gamma"), (error) => error instanceof DagmarError && error.code === "workflow_not_found");
});

async function fixture(root: string, workflow: object, acp: Executor<AcpRequest> = new InteractiveExecutor()) {
  const workflowDir = join(root, "workflows"), storageDir = join(root, "state");
  await import("node:fs/promises").then((fs) => fs.mkdir(workflowDir, { recursive: true }));
  await writeFile(join(workflowDir, `${(workflow as { id: string }).id}.yaml`), stringify(workflow));
  const profiles: Config["executors"] = { local: { type: "process", cwd: root, env: {} }, agent: { type: "acp", cwd: root, env: {}, run: ["unused"] } };
  const store = new Store(":memory:"), transcripts = new Transcripts(storageDir), events = new EventBus();
  const scheduler = new Scheduler(store, transcripts, new WorkflowRepository(workflowDir, profiles), events, profiles, { process: new ProcessExecutor(), acp });
  return { store, transcripts, events, scheduler };
}

class InteractiveExecutor implements Executor<AcpRequest> {
  async start(_request: AcpRequest, hooks: Hooks): Promise<Execution> {
    let cancelled = false;
    const done = (async () => {
      const response = await hooks.interact({ kind: "permission", method: "session/request_permission", request: { options: [{ optionId: "yes" }] }, validate: (value: Json) => value });
      const result: TaskResult = { outcome: "completed", message: "answered", output: response };
      return cancelled ? { error: { code: "cancelled", message: "cancelled" } } : { result };
    })();
    return { done, cancel: async () => { cancelled = true; } };
  }
}

async function waitFor(scheduler: Scheduler, id: string, status: RunView["status"], allowInitial = false): Promise<RunView> {
  for (let i = 0; i < 500; i++) {
    const view = await scheduler.get(id);
    if (view.status === status && (allowInitial || Object.values(view.tasks).some((x) => x.attempts.length))) return view;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`Run did not reach ${status}`);
}

async function freePort(): Promise<number> {
  const server = createServer(); await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address(); if (!address || typeof address === "string") throw new Error("No port");
  await new Promise<void>((resolve) => server.close(() => resolve())); return address.port;
}
