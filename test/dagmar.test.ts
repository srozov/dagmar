import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { appendFile, mkdir, mkdtemp, readFile, realpath, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createServer } from "node:net";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { stringify } from "yaml";
import { loadConfig } from "../src/config.js";
import { startDaemon } from "../src/daemon.js";
import { AcpExecutor, REPAIR_INSTRUCTION } from "../src/executors/acp.js";
import { ProcessExecutor } from "../src/executors/process.js";
import type { AcpRequest, Execution, Executor, Hooks, Settlement } from "../src/executors/types.js";
import { EventBus, RpcClient } from "../src/rpc.js";
import { Scheduler } from "../src/scheduler.js";
import { Store } from "../src/store.js";
import { Transcripts } from "../src/transcript.js";
import type { ExecutorProfile, Json, RunView, TaskResult, Workflow } from "../src/types.js";
import { DagmarError } from "../src/types.js";
import { WorkflowRepository, validateWorkflow } from "../src/workflow.js";

test("process DAG runs a diamond concurrently and persists transcripts", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-diamond-"));
  const script = join(root, "task.mjs");
  await writeFile(
    script,
    `
import {existsSync,writeFileSync} from 'node:fs';
const input=[]; for await (const c of process.stdin) input.push(c); const data=JSON.parse(input.join(''));
const lane=process.argv[2], peer=process.argv[3], dir=process.argv[4];
if (peer) { writeFileSync(dir+'/'+lane,''); while(!existsSync(dir+'/'+peer)) await new Promise(r=>setTimeout(r,10)); }
console.log(JSON.stringify({outcome:'completed',message:lane,output:{lane,input:data}}));
`,
  );
  const workflow = {
    id: "diamond",
    tasks: {
      a: {
        executor: "local",
        inputs: { request: "$run.input" },
        run: [process.execPath, script, "a", "", root],
      },
      b: {
        executor: "local",
        dependsOn: ["a"],
        inputs: { fromA: "$tasks.a.output.lane" },
        run: [process.execPath, script, "b", "c", root],
      },
      c: {
        executor: "local",
        dependsOn: ["a"],
        inputs: { fromA: "$tasks.a.output.lane" },
        run: [process.execPath, script, "c", "b", root],
      },
      d: {
        executor: "local",
        dependsOn: ["b", "c"],
        inputs: { b: "$tasks.b.output.lane", c: "$tasks.c.output.lane" },
        run: [process.execPath, script, "d", "", root],
      },
    },
  };
  const app = await fixture(root, workflow);
  const started = await app.scheduler.start("diamond", { hello: "world" });
  const view = await waitFor(app.scheduler, started.workflowRunId, "completed");
  assert.deepEqual(
    Object.fromEntries(Object.entries(view.tasks).map(([id, x]) => [id, x.state])),
    { a: "completed", b: "completed", c: "completed", d: "completed" },
  );
  assert.equal(view.tasks.b!.attempts.length, 1);
  const transcript = await app.transcripts.read(view.id, view.tasks.b!.attempts[0]!.id);
  assert.ok(transcript.records.some((x) => x.type === "stdio" && x.stream === "stdout"));
  app.store.close();
});

test("blocked runs resume only the blocker and cancellation ends a running run", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-resume-"));
  const marker = join(root, "marker");
  const block = join(root, "block.mjs");
  const wait = join(root, "wait.mjs");
  await writeFile(
    block,
    `import{existsSync,writeFileSync}from'node:fs';for await(const _ of process.stdin){};const first=!existsSync(${JSON.stringify(marker)});if(first)writeFileSync(${JSON.stringify(marker)},'');console.log(JSON.stringify({outcome:first?'blocked':'completed',message:'x',output:{}}));`,
  );
  await writeFile(wait, `for await(const _ of process.stdin){};setTimeout(()=>{},10000);`);
  const app = await fixture(root, {
    id: "resume",
    tasks: { step: { executor: "local", inputs: {}, run: [process.execPath, block] } },
  });
  const started = await app.scheduler.start("resume", {});
  const blocked = await waitFor(app.scheduler, started.workflowRunId, "blocked");
  assert.equal(blocked.tasks.step!.attempts.length, 1);
  const completed = await app.scheduler.resume(started.workflowRunId);
  const resumed =
    completed.status === "completed"
      ? completed
      : await waitFor(app.scheduler, started.workflowRunId, "completed");
  assert.equal(resumed.tasks.step!.attempts.length, 2);
  await assert.rejects(app.scheduler.cancelRun(started.workflowRunId), (error) => error instanceof DagmarError && error.code === "invalid_run_state");

  await writeFile(
    join(root, "workflows", "resume.yaml"),
    stringify({
      id: "resume",
      executors: defaultExecutors(root),
      tasks: { step: { executor: "local", inputs: {}, run: [process.execPath, wait] } },
    }),
  );
  const second = await app.scheduler.start("resume", {});
  await waitFor(app.scheduler, second.workflowRunId, "running", true);
  const cancelled = await app.scheduler.cancelRun(second.workflowRunId);
  assert.equal(cancelled.status, "cancelled");
  assert.equal(cancelled.tasks.step!.state, "cancelled");
  app.store.close();
});

test("a failed branch does not stop its sibling and task cancellation blocks dependants", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-branches-"));
  const bad = join(root, "bad.mjs");
  const good = join(root, "good.mjs");
  const wait = join(root, "wait.mjs");
  await writeFile(
    bad,
    `for await(const _ of process.stdin){};console.log('noise');console.log(JSON.stringify({outcome:'completed',message:'bad',output:{}}));`,
  );
  await writeFile(
    good,
    `for await(const _ of process.stdin){};await new Promise(r=>setTimeout(r,50));console.log(JSON.stringify({outcome:'completed',message:'good',output:{}}));`,
  );
  await writeFile(wait, `for await(const _ of process.stdin){};setInterval(()=>{},1000);`);
  const app = await fixture(root, {
    id: "branches",
    tasks: {
      bad: { executor: "local", inputs: {}, run: [process.execPath, bad] },
      good: { executor: "local", inputs: {}, run: [process.execPath, good] },
    },
  });
  const started = await app.scheduler.start("branches", {});
  const blocked = await waitFor(app.scheduler, started.workflowRunId, "blocked");
  assert.equal(blocked.tasks.bad!.state, "failed");
  assert.equal(blocked.tasks.good!.state, "completed");
  await writeFile(
    join(root, "workflows", "branches.yaml"),
    stringify({
      id: "branches",
      executors: defaultExecutors(root),
      tasks: {
        wait: { executor: "local", inputs: {}, run: [process.execPath, wait] },
        after: {
          executor: "local",
          dependsOn: ["wait"],
          inputs: {},
          run: [process.execPath, good],
        },
      },
    }),
  );
  const second = await app.scheduler.start("branches", {});
  let running = await waitFor(app.scheduler, second.workflowRunId, "running", true);
  const taskRunId = running.tasks.wait!.attempts[0]!.id;
  running = await app.scheduler.cancelTask(taskRunId);
  assert.equal(running.status, "blocked");
  assert.equal(running.tasks.after!.state, "blocked_by_dependency");
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
  await writeFile(join(workflows, "one.yaml"), stringify({ id: "one", executors: { local: { type: "process", cwd: root } }, tasks: { task: { executor: "local", inputs: {}, run: [process.execPath, script] } } }));
  const port = await freePort(), config = join(root, "config.yaml");
  await writeFile(config, stringify({ workflowDir: workflows, storageDir: state, listen: { host: "127.0.0.1", port } }));
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
  app.store.insertRun({ id: "wr_recovery", workflowId: "recovery", cwd: null, input: {}, status: "running", startedAt: now, updatedAt: now, endedAt: null });
  app.store.insertAttempt({ id: "tr_recovery", workflowRunId: "wr_recovery", taskId: "task", attempt: 1, executorProfile: "local", executorType: "process", status: "running", result: null, error: null, acpSessionId: null, startedAt: now, updatedAt: now, endedAt: null, item: null, roundId: null });
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

async function runProcess(prefix: string, source: string): Promise<{ settled: Settlement; stderr: string }> {
  const root = await mkdtemp(join(tmpdir(), prefix)), script = join(root, "task.mjs");
  await writeFile(script, source);
  let stderr = "";
  const execution = await new ProcessExecutor().start({ type: "process", runId: "wr_p", taskRunId: "tr_p", taskId: "p", profile: "local", cwd: root, env: process.env, inputs: {}, run: [process.execPath, script] }, { transcript: async (record) => { if (record.type === "stdio" && record.stream === "stderr") stderr += record.data; return 0; }, session: async () => undefined, interact: async () => { throw new Error("unexpected"); } });
  return { settled: await execution.done, stderr };
}

test("a failed process carries the tail of its stderr in the error message", async () => {
  const { settled, stderr } = await runProcess("dagmar-stderr-", `for await(const _ of process.stdin){};process.stderr.write('starting\\n\\nError: config file missing\\n\\n');process.exitCode=1;`);
  assert.deepEqual(settled, { error: { code: "process_exit_nonzero", message: "Process exited with code 1: starting\nError: config file missing" } });
  assert.equal(stderr, "starting\n\nError: config file missing\n\n");
});

test("a long stderr tail is cut from the front to the bound", async () => {
  const { settled, stderr } = await runProcess("dagmar-stderr-long-", `for await(const _ of process.stdin){};for(let i=0;i<2000;i++)process.stderr.write('line '+i+'\\n');process.stderr.write('last line\\n');process.exitCode=1;`);
  const message = (settled as { error: { message: string } }).error.message;
  const tail = message.slice("Process exited with code 1: ".length);
  assert.ok(message.startsWith("Process exited with code 1: …"));
  assert.ok(tail.endsWith("line 1999\nlast line"));
  assert.equal(tail.length, 500);
  assert.ok(stderr.startsWith("line 0\n"), "the transcript keeps the whole stream");
});

test("a failed process without stderr keeps the plain error message", async () => {
  const { settled } = await runProcess("dagmar-stderr-none-", `for await(const _ of process.stdin){};process.exit(3);`);
  assert.deepEqual(settled, { error: { code: "process_exit_nonzero", message: "Process exited with code 3" } });
});

test("missing and invalid stdout carry the tail of stderr too", async () => {
  const missing = await runProcess("dagmar-stderr-missing-", `for await(const _ of process.stdin){};console.error('nothing to report');`);
  assert.deepEqual(missing.settled, { error: { code: "process_stdout_missing", message: "Process produced no result: nothing to report" } });
  const invalid = await runProcess("dagmar-stderr-invalid-", `for await(const _ of process.stdin){};console.error('debug print went to stdout');console.log('oops');console.log('{}');`);
  assert.deepEqual(invalid.settled, { error: { code: "process_stdout_invalid", message: "Process stdout must be exactly one JSON value: debug print went to stdout" } });
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

// Regression: a writer without a cached count (a restarted daemon) must re-derive it from
// disk rather than restarting numbering at 1, and must drop an incomplete final line first.
test("transcript line numbering continues from disk without a cached count", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-evict-"));
  const transcripts = new Transcripts(join(root, "state"));
  const n1 = await transcripts.append("wr_evict", "tr_evict", { type: "lifecycle", direction: "internal", event: "a" });
  const n2 = await transcripts.append("wr_evict", "tr_evict", { type: "lifecycle", direction: "internal", event: "b" });
  assert.deepEqual([n1, n2], [1, 2]);
  await appendFile(transcripts.path("wr_evict", "tr_evict"), '{"type":"lifecycle"');
  const n3 = await new Transcripts(join(root, "state")).append("wr_evict", "tr_evict", { type: "lifecycle", direction: "internal", event: "c" });
  assert.equal(n3, 3);
  const read = await transcripts.read("wr_evict", "tr_evict");
  assert.equal(read.records.length, 3);
});

// Regression: an append after an idle gap must not re-read the file (that made a streamed
// reply cost O(file size) per chunk). Shown by a line written behind the cache's back: a
// re-derived count would include it.
test("transcript append keeps its line count across idle gaps", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-idle-"));
  const transcripts = new Transcripts(join(root, "state"));
  await transcripts.append("wr_idle", "tr_idle", { type: "lifecycle", direction: "internal", event: "a" });
  await new Promise((resolve) => setTimeout(resolve, 20)); // the path goes idle
  await appendFile(transcripts.path("wr_idle", "tr_idle"), '{"type":"lifecycle","direction":"internal","event":"x"}\n');
  const n = await transcripts.append("wr_idle", "tr_idle", { type: "lifecycle", direction: "internal", event: "b" });
  assert.equal(n, 2);
});

// Regression: WorkflowRepository.get resolves only the requested workflow, so an invalid
// sibling does not break it, and a duplicated id is not resolvable.
test("workflow.get resolves the requested workflow without being broken by invalid siblings", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-get-")), dir = join(root, "workflows");
  await mkdir(dir, { recursive: true });
  const executors: Record<string, ExecutorProfile> = { local: { type: "process", cwd: root, env: {} } };
  await writeFile(join(dir, "alpha.yaml"), stringify({ id: "alpha", executors, tasks: { t: { executor: "local", inputs: {}, run: ["node"] } } }));
  await writeFile(join(dir, "beta.yaml"), stringify({ id: "beta", executors, tasks: { t: { executor: "ghost", inputs: {} } } }));
  const repo = new WorkflowRepository(dir);
  assert.equal((await repo.get("alpha")).id, "alpha");
  await assert.rejects(repo.get("beta"), (error) => error instanceof DagmarError);
  await assert.rejects(repo.get("missing"), (error) => error instanceof DagmarError && error.code === "workflow_not_found");
  await writeFile(join(dir, "dup-a.yaml"), stringify({ id: "gamma", executors, tasks: { t: { executor: "local", inputs: {}, run: ["node"] } } }));
  await writeFile(join(dir, "dup-b.yaml"), stringify({ id: "gamma", executors, tasks: { t: { executor: "local", inputs: {}, run: ["node"] } } }));
  await assert.rejects(repo.get("gamma"), (error) => error instanceof DagmarError && error.code === "workflow_not_found");
});

// Regression: an ACP agent that requires authentication (auth_required, JSON-RPC -32000 on
// session/new) must settle the attempt as failed with a clear code, never crash the runtime.
test("an unauthenticated ACP agent fails the attempt with acp_authentication_required", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-acpauth-")), script = join(root, "agent.mjs");
  await writeFile(script, `
import{createInterface}from'node:readline';
const send=x=>process.stdout.write(JSON.stringify(x)+'\\n');
createInterface({input:process.stdin}).on('line',line=>{const m=JSON.parse(line);
if(m.method==='initialize')send({jsonrpc:'2.0',id:m.id,result:{protocolVersion:1,agentCapabilities:{}}});
if(m.method==='session/new')send({jsonrpc:'2.0',id:m.id,error:{code:-32000,message:'Authentication required'}});
});
`);
  const execution = await new AcpExecutor().start(
    { type: "acp", runId: "wr_a", taskRunId: "tr_a", taskId: "a", profile: "agent", cwd: root, env: process.env, inputs: {}, run: [process.execPath, script], prompt: "Do it" },
    { transcript: async () => 1, session: async () => {}, interact: async () => { throw new Error("unexpected"); } },
  );
  const settled = await execution.done;
  assert.ok("error" in settled, "expected a failed settlement, not a crash");
  assert.equal("error" in settled && settled.error.code, "acp_authentication_required");
});

// Regression: in a cancelled run, an unstarted task must be exposed as `cancelled` in
// RunView, not left `undefined` because taskStates returned without memoizing it.
test("a cancelled run exposes unstarted tasks as cancelled", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-cancelstate-")), wait = join(root, "wait.mjs");
  await writeFile(wait, `for await(const _ of process.stdin){};setInterval(()=>{},1000);`);
  const app = await fixture(root, { id: "cancelstate", tasks: {
    first: { executor: "local", inputs: {}, run: [process.execPath, wait] },
    second: { executor: "local", dependsOn: ["first"], inputs: {}, run: [process.execPath, wait] },
  } });
  const started = await app.scheduler.start("cancelstate", {});
  await waitFor(app.scheduler, started.workflowRunId, "running");
  const cancelled = await app.scheduler.cancelRun(started.workflowRunId);
  assert.equal(cancelled.status, "cancelled");
  assert.equal(cancelled.tasks.first!.state, "cancelled");
  assert.equal(cancelled.tasks.second!.state, "cancelled"); // was undefined before the fix
  assert.equal(cancelled.tasks.second!.attempts.length, 0);
  app.store.close();
});

// `step <name> [wait]` logs its name, then, given a wait file, waits for it before completing.
async function resumeFixture(prefix: string) {
  const root = await mkdtemp(join(tmpdir(), prefix)), step = join(root, "step.mjs"), log = join(root, "log"), go = join(root, "go");
  await writeFile(step, `import{appendFileSync,existsSync}from'node:fs';for await(const _ of process.stdin){};const[name,wait]=process.argv.slice(2);appendFileSync(${JSON.stringify(log)},name+'\\n');while(wait&&!existsSync(wait))await new Promise(r=>setTimeout(r,10));console.log(JSON.stringify({outcome:'completed',message:name,output:{name}}));`);
  const app = await fixture(root, { id: "chain", tasks: {
    first: { executor: "local", inputs: {}, run: [process.execPath, step, "first"] },
    second: { executor: "local", dependsOn: ["first"], inputs: {}, run: [process.execPath, step, "second", go] },
    third: { executor: "local", dependsOn: ["second"], inputs: {}, run: [process.execPath, step, "third"] },
    side: { executor: "local", dependsOn: ["first"], inputs: {}, run: [process.execPath, step, "side"] },
  } });
  const ran = async (): Promise<Record<string, number>> => {
    const counts: Record<string, number> = {};
    for (const name of (await readFile(log, "utf8")).split("\n").filter(Boolean)) counts[name] = (counts[name] ?? 0) + 1;
    return counts;
  };
  return { ...app, go, ran };
}

async function waitForTask(scheduler: Scheduler, id: string, taskId: string, state: string): Promise<void> {
  for (let i = 0; i < 500; i++) {
    if ((await scheduler.get(id)).tasks[taskId]!.state === state) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`Task ${taskId} did not reach ${state}`);
}

test("a cancelled run resumes where it stopped, unless its workflow has another active run", async () => {
  const app = await resumeFixture("dagmar-resume-cancelled-");
  const run = await app.scheduler.start("chain", {});
  await waitForTask(app.scheduler, run.workflowRunId, "second", "running");
  await waitForTask(app.scheduler, run.workflowRunId, "side", "completed");
  const cancelled = await app.scheduler.cancelRun(run.workflowRunId);
  assert.equal(cancelled.tasks.third!.state, "cancelled");
  await assert.rejects(app.scheduler.resume(run.workflowRunId, "third"), (error) => error instanceof DagmarError && error.code === "invalid_run_state", "third's dependency was cancelled");

  const other = await app.scheduler.start("chain", {});
  await waitForTask(app.scheduler, other.workflowRunId, "second", "running");
  await waitForTask(app.scheduler, other.workflowRunId, "side", "completed");
  await assert.rejects(app.scheduler.resume(run.workflowRunId), (error) => error instanceof DagmarError && error.code === "active_run_exists");
  assert.equal((await app.scheduler.get(run.workflowRunId)).status, "cancelled");
  await app.scheduler.cancelRun(other.workflowRunId);

  await writeFile(app.go, "");
  const before = await app.ran();
  await app.scheduler.resume(run.workflowRunId);
  const view = await waitFor(app.scheduler, run.workflowRunId, "completed");
  assert.deepEqual(view.tasks.second!.attempts.map((a) => a.status), ["cancelled", "completed"]);
  assert.equal(view.tasks.first!.attempts.length, 1);
  assert.equal(view.tasks.side!.attempts.length, 1);
  assert.equal(view.tasks.third!.attempts.length, 1);
  assert.equal(view.endedAt !== null, true);
  const after = await app.ran();
  assert.deepEqual({ first: after.first! - before.first!, second: after.second! - before.second!, side: after.side! - before.side!, third: after.third ?? 0 }, { first: 0, second: 1, side: 0, third: 1 });
  await assert.rejects(app.scheduler.resume(run.workflowRunId), (error) => error instanceof DagmarError && error.code === "invalid_run_state", "a completed run resumes only from a task");
  app.store.close();
});

test("a completed run resumes from a task: that task and its downstream run again, the rest keep their results", async () => {
  const app = await resumeFixture("dagmar-resume-from-");
  await writeFile(app.go, "");
  const run = await app.scheduler.start("chain", {});
  await waitFor(app.scheduler, run.workflowRunId, "completed");
  await assert.rejects(app.scheduler.resume(run.workflowRunId, "nope"), (error) => error instanceof DagmarError && error.code === "invalid_params");
  await app.scheduler.resume(run.workflowRunId, "second");
  const view = await waitFor(app.scheduler, run.workflowRunId, "completed");
  assert.deepEqual(Object.fromEntries(Object.entries(view.tasks).map(([id, x]) => [id, x.attempts.map((a) => a.status).join(",")])), {
    first: "completed",
    second: "completed,completed",
    third: "completed,completed",
    side: "completed",
  });
  assert.deepEqual(await app.ran(), { first: 1, second: 2, third: 2, side: 1 });
  app.store.close();
});

// T1 (session continuation): validateWorkflow accepts a continue task whose 'from' is a
// same-agent ACP dependency, and rejects every variant the plan enumerates. Profiles are
// passed in directly so each rejection can pin a specific failure mode without disk I/O.
test("validateWorkflow accepts continue with a same-agent ACP dependency and rejects variants", () => {
  const profiles: Record<string, ExecutorProfile> = {
    acpA: { type: "acp", cwd: "/tmp", env: {}, run: ["agent"] },
    acpB: { type: "acp", cwd: "/tmp", env: {}, run: ["agent"] }, // same agent as acpA
    acpOther: { type: "acp", cwd: "/tmp", env: {}, run: ["other"] }, // different agent
    proc: { type: "process", cwd: "/tmp", env: {} },
  };
  // Accept: same-agent acp dependency.
  const ok = validate({
    id: "ok",
    tasks: {
      A: { executor: "acpA", inputs: {}, prompt: "a" },
      B: { executor: "acpB", dependsOn: ["A"], inputs: {}, prompt: "b", session: { mode: "continue", from: "A" } },
    },
  }, profiles);
  assert.equal(ok.tasks.B!.session?.mode, "continue");
  assert.equal((ok.tasks.B!.session as { from?: string }).from, "A");

  // Reject: missing `from` for continue.
  assert.throws(
    () => validate({
      id: "x",
      tasks: {
        A: { executor: "acpA", inputs: {}, prompt: "a" },
        B: { executor: "acpA", dependsOn: ["A"], inputs: {}, prompt: "b", session: { mode: "continue" } },
      },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_task" && /continue requires from/.test(error.message),
  );

  // Reject: `from` not in dependsOn.
  assert.throws(
    () => validate({
      id: "x",
      tasks: {
        A: { executor: "acpA", inputs: {}, prompt: "a" },
        C: { executor: "acpA", inputs: {}, prompt: "c" },
        B: { executor: "acpA", dependsOn: ["A"], inputs: {}, prompt: "b", session: { mode: "continue", from: "C" } },
      },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_dependency",
  );

  // Reject: `from` is a process task (can't continue a process session).
  assert.throws(
    () => validate({
      id: "x",
      tasks: {
        P: { executor: "proc", inputs: {}, run: ["true"] },
        B: { executor: "acpA", dependsOn: ["P"], inputs: {}, prompt: "b", session: { mode: "continue", from: "P" } },
      },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_task" && /ACP task/.test(error.message),
  );

  // Reject: `from` uses a different agent (different run argv).
  assert.throws(
    () => validate({
      id: "x",
      tasks: {
        A: { executor: "acpA", inputs: {}, prompt: "a" },
        B: { executor: "acpOther", dependsOn: ["A"], inputs: {}, prompt: "b", session: { mode: "continue", from: "A" } },
      },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_task" && /same agent/.test(error.message),
  );

  // Reject: mode: fork (still unsupported).
  assert.throws(
    () => validate({
      id: "x",
      tasks: { A: { executor: "acpA", inputs: {}, prompt: "a", session: { mode: "fork" } } },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "unsupported_session_mode",
  );
});

// T1 (scheduler resolution): the scheduler forwards the source task's persisted
// acp_session_id on the continuing task's request as loadSessionId. The fake ACP executor
// captures B's request after A completes with hooks.session("sid-A"); assert the forwarded
// id matches and the run completes.
test("scheduler resolves loadSessionId from a completed dependency's persisted acp_session_id", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-resolve-"));
  const workflow = {
    id: "resolve",
    tasks: {
      A: { executor: "agent", inputs: {}, prompt: "a" },
      B: { executor: "agent", dependsOn: ["A"], inputs: {}, prompt: "b", session: { mode: "continue", from: "A" } },
    },
  };
  // fixture()'s default `agent` profile has run:["unused"] so A and B share the same agent argv.
  const captured = { request: undefined as AcpRequest | undefined };
  const stub = new StubAcpExecutor(async (request, hooks) => {
    if (request.taskId === "A") {
      // Mirrors AcpExecutor: the executor persists its session id via this hook so the
      // scheduler can hand it to a later continuing task.
      await hooks.session("sid-A");
      return { result: { outcome: "completed", message: "a", output: {} } };
    }
    captured.request = request;
    return { result: { outcome: "completed", message: "b", output: {} } };
  });
  const app = await fixture(root, workflow, stub);
  const started = await app.scheduler.start("resolve", {});
  const view = await waitFor(app.scheduler, started.workflowRunId, "completed");
  assert.equal(view.tasks.A!.state, "completed");
  assert.equal(view.tasks.B!.state, "completed");
  assert.equal(captured.request?.loadSessionId, "sid-A");
  app.store.close();
});

test("scheduler forwards the executor's loadSettleMs only to a continuing task", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-settle-"));
  const workflow = {
    id: "settle",
    executors: { agent: { type: "acp", cwd: root, run: ["unused"], loadSettleMs: 5000 } },
    tasks: {
      A: { executor: "agent", inputs: {}, prompt: "a" },
      B: { executor: "agent", dependsOn: ["A"], inputs: {}, prompt: "b", session: { mode: "continue", from: "A" } },
    },
  };
  const requests: Record<string, AcpRequest> = {};
  const stub = new StubAcpExecutor(async (request, hooks) => {
    requests[request.taskId] = request;
    if (request.taskId === "A") await hooks.session("sid-A");
    return { result: { outcome: "completed", message: request.taskId, output: {} } };
  });
  const app = await fixture(root, workflow, stub);
  const started = await app.scheduler.start("settle", {});
  await waitFor(app.scheduler, started.workflowRunId, "completed");
  assert.equal(requests.A?.loadSettleMs, undefined);
  assert.equal(requests.B?.loadSettleMs, 5000);
  app.store.close();
});

// T1 (ACP load branch): when loadSessionId is set and the agent advertises loadSession,
// AcpExecutor calls session/load (not session/new) and then one session/prompt, settling
// with the parsed TaskResult. Inspect the captured transcript for both the load method
// (and the absence of session/new) and the resulting completion.
test("AcpExecutor uses session/load when loadSessionId is set and the agent advertises loadSession", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-acp-load-")), script = join(root, "agent.mjs");
  await writeFile(script, `
import{createInterface}from'node:readline';
const send=x=>process.stdout.write(JSON.stringify(x)+'\\n');
createInterface({input:process.stdin}).on('line',line=>{const m=JSON.parse(line);
if(m.method==='initialize')send({jsonrpc:'2.0',id:m.id,result:{protocolVersion:1,agentCapabilities:{loadSession:true}}});
if(m.method==='session/load'){send({jsonrpc:'2.0',id:m.id,result:{}})}
if(m.method==='session/prompt'){send({jsonrpc:'2.0',method:'session/update',params:{sessionId:m.params.sessionId,update:{sessionUpdate:'agent_message_chunk',content:{type:'text',text:'{"outcome":"completed","message":"loaded","output":{}}'}}}});send({jsonrpc:'2.0',id:m.id,result:{stopReason:'end_turn'}})}
});
`);
  const records: unknown[] = []; let session = "";
  const execution = await new AcpExecutor().start({ type: "acp", runId: "wr_y", taskRunId: "tr_y", taskId: "y", profile: "agent", cwd: root, env: process.env, inputs: {}, run: [process.execPath, script], prompt: "Continue", loadSessionId: "sid-X" }, { transcript: async (record) => { records.push(record); return records.length; }, session: async (id) => { session = id; }, interact: async () => { throw new Error("unexpected"); } });
  const settled = await execution.done;
  assert.equal(session, "sid-X");
  assert.equal("result" in settled && settled.result.outcome, "completed");
  const methods = records.filter((r) => (r as { type?: string }).type === "acp").map((r) => (r as { message?: { method?: string } }).message?.method);
  assert.ok(methods.includes("session/load"), "expected session/load to be issued");
  assert.ok(!methods.includes("session/new"), "session/new must not be issued on a load path");
});

// Slow MCP on load: the fake agent answers session/load at once but reports its MCP servers
// connected only `readyMs` later; a prompt sent before then gets a "blocked" result citing
// disconnected tools, as claude-agent-acp's model does. The margins (a 200 ms settle against
// 50 ms, or no settle against 60 s) keep both outcomes independent of scheduling jitter.
async function slowMcpAgent(readyMs: number): Promise<{ root: string; script: string }> {
  const root = await mkdtemp(join(tmpdir(), "dagmar-acp-settle-")), script = join(root, "agent.mjs");
  await writeFile(script, `
import{createInterface}from'node:readline';
const send=x=>process.stdout.write(JSON.stringify(x)+'\\n');
let loadedAt;
createInterface({input:process.stdin}).on('line',line=>{const m=JSON.parse(line);
if(m.method==='initialize')send({jsonrpc:'2.0',id:m.id,result:{protocolVersion:1,agentCapabilities:{loadSession:true}}});
if(m.method==='session/load'){loadedAt=Date.now();send({jsonrpc:'2.0',id:m.id,result:{}})}
if(m.method==='session/prompt'){const ready=Date.now()-loadedAt>=${readyMs};const r=ready?{outcome:'completed',message:'tools ready',output:{}}:{outcome:'blocked',message:'MCP tools disconnected',output:{}};send({jsonrpc:'2.0',method:'session/update',params:{sessionId:m.params.sessionId,update:{sessionUpdate:'agent_message_chunk',content:{type:'text',text:JSON.stringify(r)}}}});send({jsonrpc:'2.0',id:m.id,result:{stopReason:'end_turn'}})}
});
`);
  return { root, script };
}

test("AcpExecutor without loadSettleMs prompts a loaded session before its MCP servers connect", async () => {
  const { root, script } = await slowMcpAgent(60_000);
  const { settled, records } = await runModelAgent(root, script, { loadSessionId: "sid-X" });
  assert.equal("result" in settled && settled.result.outcome, "blocked");
  assert.ok(!records.some((r) => (r as { event?: string }).event === "acp_load_settle"));
});

test("AcpExecutor waits loadSettleMs after session/load before the first prompt", async () => {
  const { root, script } = await slowMcpAgent(50);
  const { settled, records, sent } = await runModelAgent(root, script, { loadSessionId: "sid-X", loadSettleMs: 200 });
  assert.equal("result" in settled && settled.result.outcome, "completed");
  const settle = records.findIndex((r) => (r as { event?: string }).event === "acp_load_settle");
  assert.deepEqual((records[settle] as { data?: unknown }).data, { ms: 200 });
  const prompt = records.findIndex((r) => (r as { type?: string; message?: { method?: string } }).type === "acp" && (r as { message: { method?: string } }).message.method === "session/prompt");
  assert.ok(settle >= 0 && settle < prompt, "the settle event precedes the first prompt");
  assert.equal(sent.filter((m) => m.method === "session/prompt").length, 1);
});

test("AcpExecutor ignores loadSettleMs on a new session", async () => {
  const { root, script } = await modelAgent("dagmar-acp-settle-new-", null);
  const { settled, records } = await runModelAgent(root, script, { loadSettleMs: 60_000 });
  assert.equal("result" in settled && settled.result.outcome, "completed");
  assert.ok(!records.some((r) => (r as { event?: string }).event === "acp_load_settle"));
});

test("AcpExecutor cancel interrupts the loadSettleMs wait without prompting", async () => {
  const { root, script } = await slowMcpAgent(0);
  const records: unknown[] = [];
  let settling!: () => void;
  const waiting = new Promise<void>((resolve) => { settling = resolve; });
  const execution = await new AcpExecutor().start({ type: "acp", runId: "wr_c", taskRunId: "tr_c", taskId: "c", profile: "agent", cwd: root, env: process.env, inputs: {}, run: [process.execPath, script], prompt: "Go", loadSessionId: "sid-X", loadSettleMs: 60_000 }, { transcript: async (record) => { records.push(record); if ((record as { event?: string }).event === "acp_load_settle") settling(); return records.length; }, session: async () => undefined, interact: async () => { throw new Error("unexpected"); } });
  await waiting;
  const started = Date.now();
  await execution.cancel();
  const settled = await execution.done;
  assert.ok(Date.now() - started < 30_000, "cancel must not wait out the settle period");
  assert.equal("error" in settled && settled.error.code, "acp_cancelled");
  assert.ok(!records.some((r) => (r as { type?: string; message?: { method?: string } }).type === "acp" && (r as { message: { method?: string } }).message.method === "session/prompt"));
});

// T1 (acp_load_unsupported): when loadSessionId is set but the agent does not advertise
// loadSession, AcpExecutor must fail fast with a clear code rather than calling
// session/load and crashing the runtime.
test("AcpExecutor fails with acp_load_unsupported when loadSessionId is set but the agent does not advertise loadSession", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-acp-noload-")), script = join(root, "agent.mjs");
  await writeFile(script, `
import{createInterface}from'node:readline';
const send=x=>process.stdout.write(JSON.stringify(x)+'\\n');
createInterface({input:process.stdin}).on('line',line=>{const m=JSON.parse(line);
if(m.method==='initialize')send({jsonrpc:'2.0',id:m.id,result:{protocolVersion:1,agentCapabilities:{}}});
});
`);
  const records: unknown[] = [];
  const execution = await new AcpExecutor().start({ type: "acp", runId: "wr_z", taskRunId: "tr_z", taskId: "z", profile: "agent", cwd: root, env: process.env, inputs: {}, run: [process.execPath, script], prompt: "Continue", loadSessionId: "sid-X" }, { transcript: async (record) => { records.push(record); return records.length; }, session: async () => { throw new Error("hooks.session must not be called when the agent lacks loadSession"); }, interact: async () => { throw new Error("unexpected"); } });
  const settled = await execution.done;
  assert.ok("error" in settled, "expected a failed settlement, not a crash");
  assert.equal("error" in settled && settled.error.code, "acp_load_unsupported");
  assert.ok(records.some((r) => (r as { type?: string; event?: string }).type === "lifecycle" && (r as { event?: string }).event === "acp_load_unsupported"));
});

// Profile model selection: the fake agent advertises `configOptions` on session/new and
// session/load; `apply: false` makes it acknowledge set_config_option without switching.
const MODEL_OPTION = { id: "model", name: "Model", category: "model", type: "select", currentValue: "default", options: [{ value: "default", name: "Default" }, { value: "sonnet", name: "Sonnet" }, { value: "opus", name: "Opus" }] };
async function modelAgent(prefix: string, configOptions: unknown, apply = true): Promise<{ root: string; script: string }> {
  const root = await mkdtemp(join(tmpdir(), prefix)), script = join(root, "agent.mjs");
  await writeFile(script, `
import{createInterface}from'node:readline';
const send=x=>process.stdout.write(JSON.stringify(x)+'\\n');
const options=${JSON.stringify(configOptions)};
createInterface({input:process.stdin}).on('line',line=>{const m=JSON.parse(line);
if(m.method==='initialize')send({jsonrpc:'2.0',id:m.id,result:{protocolVersion:1,agentCapabilities:{loadSession:true}}});
if(m.method==='session/new')send({jsonrpc:'2.0',id:m.id,result:{sessionId:'m-1',...(options?{configOptions:options}:{})}});
if(m.method==='session/load')send({jsonrpc:'2.0',id:m.id,result:options?{configOptions:options}:{}});
if(m.method==='session/set_config_option')send({jsonrpc:'2.0',id:m.id,result:{configOptions:options.map(o=>o.id===m.params.configId&&${apply}?{...o,currentValue:m.params.value}:o)}});
if(m.method==='session/prompt'){send({jsonrpc:'2.0',method:'session/update',params:{sessionId:m.params.sessionId,update:{sessionUpdate:'agent_message_chunk',content:{type:'text',text:'{"outcome":"completed","message":"ok","output":{}}'}}}});send({jsonrpc:'2.0',id:m.id,result:{stopReason:'end_turn'}})}
});
`);
  return { root, script };
}
async function runModelAgent(root: string, script: string, extra: Partial<AcpRequest>): Promise<{ settled: Settlement; records: unknown[]; sent: Array<{ method?: string; params?: Record<string, unknown> }> }> {
  const records: unknown[] = [];
  const execution = await new AcpExecutor().start({ type: "acp", runId: "wr_m", taskRunId: "tr_m", taskId: "m", profile: "agent", cwd: root, env: process.env, inputs: {}, run: [process.execPath, script], prompt: "Go", ...extra }, { transcript: async (record) => { records.push(record); return records.length; }, session: async () => undefined, interact: async () => { throw new Error("unexpected"); } });
  const settled = await execution.done;
  const sent = records.filter((r) => (r as { type?: string; direction?: string }).type === "acp" && (r as { direction?: string }).direction === "to_executor").map((r) => (r as { message: { method?: string; params?: Record<string, unknown> } }).message);
  return { settled, records, sent };
}

test("AcpExecutor selects the profile model via session/set_config_option before prompting", async () => {
  const { root, script } = await modelAgent("dagmar-acp-model-", [MODEL_OPTION]);
  const { settled, records, sent } = await runModelAgent(root, script, { model: "opus" });
  assert.equal("result" in settled && settled.result.outcome, "completed");
  const methods = sent.map((m) => m.method);
  assert.ok(methods.indexOf("session/set_config_option") > methods.indexOf("session/new"));
  assert.ok(methods.indexOf("session/set_config_option") < methods.indexOf("session/prompt"));
  const set = sent.find((m) => m.method === "session/set_config_option")!;
  assert.equal(set.params?.configId, "model");
  assert.equal(set.params?.value, "opus");
  assert.ok(records.some((r) => (r as { event?: string }).event === "acp_model_selected"));
});

test("AcpExecutor re-applies the profile model after session/load (grouped options)", async () => {
  const grouped = { ...MODEL_OPTION, options: [{ group: "claude", name: "Claude", options: MODEL_OPTION.options }] };
  const { root, script } = await modelAgent("dagmar-acp-model-load-", [grouped]);
  const { settled, sent } = await runModelAgent(root, script, { model: "sonnet", loadSessionId: "sid-X" });
  assert.equal("result" in settled && settled.result.outcome, "completed");
  const methods = sent.map((m) => m.method);
  assert.ok(methods.indexOf("session/set_config_option") > methods.indexOf("session/load"));
  assert.equal(sent.find((m) => m.method === "session/set_config_option")?.params?.value, "sonnet");
});

test("AcpExecutor skips set_config_option when the profile model is already current", async () => {
  const { root, script } = await modelAgent("dagmar-acp-model-current-", [MODEL_OPTION]);
  const { settled, sent } = await runModelAgent(root, script, { model: "default" });
  assert.equal("result" in settled && settled.result.outcome, "completed");
  assert.ok(!sent.some((m) => m.method === "session/set_config_option"));
});

test("AcpExecutor fails with acp_model_unknown for a model the agent does not offer", async () => {
  const { root, script } = await modelAgent("dagmar-acp-model-unknown-", [MODEL_OPTION]);
  const { settled, sent } = await runModelAgent(root, script, { model: "claude-3-opus-latest" });
  assert.equal("error" in settled && settled.error.code, "acp_model_unknown");
  assert.match("error" in settled ? settled.error.message : "", /available: default, sonnet, opus/);
  assert.ok(!sent.some((m) => m.method === "session/set_config_option" || m.method === "session/prompt"));
});

test("AcpExecutor fails with acp_model_unsupported when the agent has no model selector", async () => {
  const { root, script } = await modelAgent("dagmar-acp-model-none-", null);
  const { settled, sent } = await runModelAgent(root, script, { model: "opus" });
  assert.equal("error" in settled && settled.error.code, "acp_model_unsupported");
  assert.ok(!sent.some((m) => m.method === "session/prompt"));
});

test("AcpExecutor fails with acp_model_not_applied when the agent does not switch", async () => {
  const { root, script } = await modelAgent("dagmar-acp-model-ignored-", [MODEL_OPTION], false);
  const { settled, sent } = await runModelAgent(root, script, { model: "opus" });
  assert.equal("error" in settled && settled.error.code, "acp_model_not_applied");
  assert.ok(!sent.some((m) => m.method === "session/prompt"));
});

test("scheduler forwards the executor profile's model and mode on the ACP request", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-model-fwd-"));
  const workflowDir = join(root, "workflows");
  await mkdir(workflowDir, { recursive: true });
  await writeFile(join(workflowDir, "fwd.yaml"), stringify({ id: "fwd", executors: { reviewer: { type: "acp", cwd: root, run: ["unused"], model: "opus", mode: "plan" } }, tasks: { A: { executor: "reviewer", inputs: {}, prompt: "a" } } }));
  const captured = { request: undefined as AcpRequest | undefined };
  const stub = new StubAcpExecutor(async (request) => { captured.request = request; return { result: { outcome: "completed", message: "a", output: {} } }; });
  const store = new Store(":memory:");
  const scheduler = new Scheduler(store, new Transcripts(join(root, "state")), new WorkflowRepository(workflowDir), new EventBus(), { process: new ProcessExecutor(), acp: stub });
  const started = await scheduler.start("fwd", {});
  await waitFor(scheduler, started.workflowRunId, "completed");
  assert.equal(captured.request?.model, "opus");
  assert.equal(captured.request?.mode, "plan");
  store.close();
});

test("loadConfig rejects executors, which a workflow declares", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-config-"));
  const path = join(root, "config.yaml"), base = { workflowDir: root, storageDir: root, listen: { host: "127.0.0.1", port: 7331 } };
  await writeFile(path, stringify(base));
  assert.deepEqual(await loadConfig(path), base);
  await writeFile(path, stringify({ ...base, executors: { local: { type: "process", cwd: root } } }));
  await assert.rejects(loadConfig(path), /unknown field executors/);
});

test("validateWorkflow reads the workflow's executors and rejects malformed ones", () => {
  const tasks = { a: { executor: "agent", inputs: {}, prompt: "a" }, p: { executor: "proc", inputs: {}, run: ["node"] } };
  const agent = { type: "acp", cwd: "/tmp", run: ["/bin/agent"] }, proc = { type: "process", cwd: "/tmp" };
  const ok = validateWorkflow({ id: "x", executors: { agent: { ...agent, env: { A: "1" }, model: "opus", mode: "acceptEdits" }, proc }, tasks });
  assert.deepEqual(ok.executors, { agent: { type: "acp", cwd: "/tmp", run: ["/bin/agent"], env: { A: "1" }, model: "opus", mode: "acceptEdits" }, proc: { type: "process", cwd: "/tmp", env: {} } });
  const invalid = (executors: object) => assert.throws(() => validateWorkflow({ id: "x", executors, tasks }), (error) => error instanceof DagmarError && error.code === "workflow_invalid");
  invalid({ agent, proc: { ...proc, model: "opus" } }); // model and mode are ACP-only
  invalid({ agent, proc: { ...proc, mode: "acceptEdits" } });
  invalid({ agent: { ...agent, model: "" }, proc });
  invalid({ agent: { ...agent, mode: "" }, proc });
  assert.equal(validateWorkflow({ id: "x", executors: { agent: { ...agent, loadSettleMs: 120000 }, proc }, tasks }).executors.agent?.loadSettleMs, 120000);
  invalid({ agent, proc: { ...proc, loadSettleMs: 1000 } }); // loadSettleMs is ACP-only
  invalid({ agent: { ...agent, loadSettleMs: 0 }, proc });
  invalid({ agent: { ...agent, loadSettleMs: 120001 }, proc });
  invalid({ agent: { ...agent, loadSettleMs: 1.5 }, proc });
  assert.deepEqual(validateWorkflow({ id: "x", executors: { agent: { ...agent, retry: { max: 2, delaySeconds: 0 } }, proc }, tasks }).executors.agent?.retry, { max: 2, delaySeconds: 0 });
  invalid({ agent, proc: { ...proc, retry: { max: 2, delaySeconds: 300 } } }); // retry is ACP-only
  assert.deepEqual(validateWorkflow({ id: "x", executors: { agent: { ...agent, retry: { max: 0, delaySeconds: 0 } }, proc }, tasks }).executors.agent?.retry, { max: 0, delaySeconds: 0 });
  invalid({ agent: { ...agent, retry: { max: -1, delaySeconds: 300 } }, proc });
  invalid({ agent: { ...agent, retry: { max: 2, delaySeconds: -1 } }, proc });
  invalid({ agent: { ...agent, retry: { max: 2 } }, proc });
  invalid({ agent: { ...agent, retry: { max: 2, delaySeconds: 300, on: "any" } }, proc });
  invalid({ agent: { ...agent, extra: true }, proc });
  invalid({ agent: { type: "acp", cwd: "/tmp" }, proc }); // an ACP executor needs run
  invalid({ agent: { ...agent, run: [""] }, proc });
  invalid({ agent, proc: { ...proc, cwd: "relative" } });
  invalid({ agent, proc: { ...proc, env: { A: 1 } } });
  invalid({ agent, proc: { ...proc, env: { "A=B": "1" } } });
  invalid({ agent, proc: { type: "gate", cwd: "/tmp" } });
  invalid({ agent, proc, "-bad": proc });
  assert.throws(() => validateWorkflow({ id: "x", executors: { agent }, tasks }), (error) => error instanceof DagmarError && error.code === "unknown_executor");
});

// Profile mode selection reuses the model fake agent with a "mode" config option.
const MODE_OPTION = { id: "mode", name: "Mode", category: "mode", type: "select", currentValue: "default", options: [{ value: "default", name: "Default" }, { value: "acceptEdits", name: "Accept edits" }, { value: "plan", name: "Plan" }] };

test("AcpExecutor selects the profile mode after the model, before prompting", async () => {
  const { root, script } = await modelAgent("dagmar-acp-mode-", [MODEL_OPTION, MODE_OPTION]);
  const { settled, records, sent } = await runModelAgent(root, script, { model: "opus", mode: "acceptEdits" });
  assert.equal("result" in settled && settled.result.outcome, "completed");
  const sets = sent.filter((m) => m.method === "session/set_config_option").map((m) => [m.params?.configId, m.params?.value]);
  assert.deepEqual(sets, [["model", "opus"], ["mode", "acceptEdits"]]);
  const methods = sent.map((m) => m.method);
  assert.ok(methods.lastIndexOf("session/set_config_option") < methods.indexOf("session/prompt"));
  assert.ok(records.some((r) => (r as { event?: string; data?: { mode?: string } }).event === "acp_mode_selected" && (r as { data?: { mode?: string } }).data?.mode === "acceptEdits"));
});

test("AcpExecutor re-applies the profile mode after session/load", async () => {
  const { root, script } = await modelAgent("dagmar-acp-mode-load-", [MODE_OPTION]);
  const { settled, sent } = await runModelAgent(root, script, { mode: "plan", loadSessionId: "sid-X" });
  assert.equal("result" in settled && settled.result.outcome, "completed");
  const methods = sent.map((m) => m.method);
  assert.ok(methods.indexOf("session/set_config_option") > methods.indexOf("session/load"));
  assert.equal(sent.find((m) => m.method === "session/set_config_option")?.params?.value, "plan");
});

test("AcpExecutor fails with acp_mode_unknown for a mode the agent does not offer", async () => {
  const { root, script } = await modelAgent("dagmar-acp-mode-unknown-", [MODE_OPTION]);
  const { settled, sent } = await runModelAgent(root, script, { mode: "dontAsk" });
  assert.equal("error" in settled && settled.error.code, "acp_mode_unknown");
  assert.match("error" in settled ? settled.error.message : "", /available: default, acceptEdits, plan/);
  assert.ok(!sent.some((m) => m.method === "session/set_config_option" || m.method === "session/prompt"));
});

test("AcpExecutor fails with acp_mode_unsupported when the agent has no mode selector", async () => {
  const { root, script } = await modelAgent("dagmar-acp-mode-none-", [MODEL_OPTION]);
  const { settled, sent } = await runModelAgent(root, script, { mode: "acceptEdits" });
  assert.equal("error" in settled && settled.error.code, "acp_mode_unsupported");
  assert.ok(!sent.some((m) => m.method === "session/prompt"));
});

test("AcpExecutor fails with acp_mode_not_applied when the agent does not switch", async () => {
  const { root, script } = await modelAgent("dagmar-acp-mode-ignored-", [MODE_OPTION], false);
  const { settled, sent } = await runModelAgent(root, script, { mode: "plan" });
  assert.equal("error" in settled && settled.error.code, "acp_mode_not_applied");
  assert.ok(!sent.some((m) => m.method === "session/prompt"));
});

// T1 (continuation_unavailable): when the source task completes without persisting an
// acp_session_id (e.g. an interactive executor that doesn't call hooks.session), the
// continuing task's attempt must fail-fast with continuation_unavailable — never silently
// fall back to a fresh session.
test("scheduler fails the continuing task with continuation_unavailable when the source has no acp_session_id", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-continuation-unavail-"));
  const workflow = {
    id: "unavail",
    tasks: {
      A: { executor: "agent", inputs: {}, prompt: "a" },
      B: { executor: "agent", dependsOn: ["A"], inputs: {}, prompt: "b", session: { mode: "continue", from: "A" } },
    },
  };
  // The default InteractiveExecutor does NOT call hooks.session, so A's acp_session_id
  // stays null in the store — exactly the source-side failure this test exercises.
  const app = await fixture(root, workflow);
  const started = await app.scheduler.start("unavail", {});
  await waitFor(app.scheduler, started.workflowRunId, "waiting");
  const interaction = app.scheduler.interactions()[0]!;
  await app.scheduler.answer(interaction.id, { outcome: { outcome: "selected", optionId: "yes" } });
  const view = await waitFor(app.scheduler, started.workflowRunId, "blocked");
  assert.equal(view.tasks.A!.state, "completed");
  assert.equal(view.tasks.B!.state, "failed");
  assert.equal(view.tasks.B!.attempts[0]?.error?.code, "continuation_unavailable");
  app.store.close();
});

// T2 (workflow validation): validateWorkflow accepts an ACP task with interactive:true
// (and normalizes it onto the TaskDef) but rejects it on a process task — interactive
// is an ACP-only flag. Mirrors the T1 validateWorkflow test (lines 348-426): profiles
// are passed directly so each case pins a specific failure mode without disk I/O.
test("validateWorkflow accepts interactive on ACP tasks and rejects it on process tasks", () => {
  const profiles: Record<string, ExecutorProfile> = {
    acp: { type: "acp", cwd: "/tmp", env: {}, run: ["agent"] },
    proc: { type: "process", cwd: "/tmp", env: {} },
  };

  // Accept: ACP task with interactive: true; the normalized TaskDef carries it.
  const ok = validate({
    id: "ok",
    tasks: { A: { executor: "acp", inputs: {}, prompt: "talk", interactive: true } },
  }, profiles);
  assert.equal(ok.tasks.A!.interactive, true);

  // Reject: process task with interactive: true. The validator must surface a clear
  // `invalid_task` code (not a schema shape failure — the JSON schema accepts boolean).
  assert.throws(
    () => validate({
      id: "x",
      tasks: { P: { executor: "proc", inputs: {}, run: ["true"], interactive: true } },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_task" && /Process task/.test(error.message),
  );
});

// A tool-using agent sends several messages in one turn (progress notes, then the answer), each
// tagged with an ACP messageId. The executor parses only the last message. `chunks` is the list of
// [messageId | null, text] the fake agent streams for its single prompt.
async function messagesAgent(prefix: string, chunks: Array<[string | null, string]>): Promise<{ root: string; script: string }> {
  const root = await mkdtemp(join(tmpdir(), prefix)), script = join(root, "agent.mjs");
  await writeFile(script, `
import{createInterface}from'node:readline';
const send=x=>process.stdout.write(JSON.stringify(x)+'\\n');
const chunks=${JSON.stringify(chunks)};
createInterface({input:process.stdin}).on('line',line=>{const m=JSON.parse(line);
if(m.method==='initialize')send({jsonrpc:'2.0',id:m.id,result:{protocolVersion:1,agentCapabilities:{}}});
if(m.method==='session/new')send({jsonrpc:'2.0',id:m.id,result:{sessionId:'m-1'}});
if(m.method==='session/prompt'){for(const [id,text] of chunks)send({jsonrpc:'2.0',method:'session/update',params:{sessionId:'m-1',update:{sessionUpdate:'agent_message_chunk',...(id?{messageId:id}:{}),content:{type:'text',text}}}});send({jsonrpc:'2.0',id:m.id,result:{stopReason:'end_turn'}})}
});
`);
  return { root, script };
}
async function runMessagesAgent(root: string, script: string, interactive: boolean): Promise<{ settled: Settlement; turns: number }> {
  let turns = 0;
  const execution = await new AcpExecutor().start({ type: "acp", runId: "wr_msg", taskRunId: "tr_msg", taskId: "msg", profile: "agent", cwd: root, env: process.env, inputs: {}, run: [process.execPath, script], prompt: "Go", ...(interactive ? { interactive: true } : {}) }, { transcript: async () => 1, session: async () => undefined, interact: async () => { turns++; throw new Error("unexpected turn"); } });
  return { settled: await execution.done, turns };
}
const ENVELOPE_PARTS = ['{"outcome":"completed",', '"message":"ok","output":{"n":1}}'];

test("AcpExecutor parses only the turn's last agent message as the result", async () => {
  const { root, script } = await messagesAgent("dagmar-acp-msgs-", [["m1", "Reading files."], ["m2", "Exit 0. Committing."], ["m3", ENVELOPE_PARTS[0]!], ["m3", ENVELOPE_PARTS[1]!]]);
  const { settled } = await runMessagesAgent(root, script, false);
  assert.ok("result" in settled, `expected a result, got ${JSON.stringify(settled)}`);
  assert.deepEqual(settled.result.output, { n: 1 });
});

test("an interactive task completes when its last message is the envelope after progress notes", async () => {
  const { root, script } = await messagesAgent("dagmar-acp-msgs-int-", [["m1", "Let me check the tests."], ["m2", ENVELOPE_PARTS.join("")]]);
  const { settled, turns } = await runMessagesAgent(root, script, true);
  assert.equal(turns, 0, "a final envelope must not be delivered to the user as a turn");
  assert.ok("result" in settled && settled.result.message === "ok");
});

test("chunks without a messageId still accumulate, so prose before the envelope fails", async () => {
  const { root, script } = await messagesAgent("dagmar-acp-msgs-noid-", [[null, "Reading files."], [null, ENVELOPE_PARTS.join("")]]);
  const { settled } = await runMessagesAgent(root, script, false);
  assert.equal("error" in settled && settled.error.code, "acp_failed");
});

// Result repair: a single-shot agent whose reply is not a valid result gets exactly one repair turn
// on the same session. `replies` is what the fake agent answers to its 1st, 2nd, ... prompt.
async function repairAgent(prefix: string, replies: string[]): Promise<{ root: string; script: string }> {
  const root = await mkdtemp(join(tmpdir(), prefix)), script = join(root, "agent.mjs");
  await writeFile(script, `
import{createInterface}from'node:readline';
const send=x=>process.stdout.write(JSON.stringify(x)+'\\n');
const replies=${JSON.stringify(replies)}; let n=0;
createInterface({input:process.stdin}).on('line',line=>{const m=JSON.parse(line);
if(m.method==='initialize')send({jsonrpc:'2.0',id:m.id,result:{protocolVersion:1,agentCapabilities:{}}});
if(m.method==='session/new')send({jsonrpc:'2.0',id:m.id,result:{sessionId:'r-1'}});
if(m.method==='session/prompt'){const text=replies[Math.min(n++,replies.length-1)];send({jsonrpc:'2.0',method:'session/update',params:{sessionId:'r-1',update:{sessionUpdate:'agent_message_chunk',content:{type:'text',text}}}});send({jsonrpc:'2.0',id:m.id,result:{stopReason:'end_turn'}})}
});
`);
  return { root, script };
}
async function runRepairAgent(root: string, script: string, outputSchema?: AcpRequest["outputSchema"]): Promise<{ settled: Settlement; prompts: string[]; events: string[] }> {
  const records: unknown[] = [];
  const execution = await new AcpExecutor().start({ type: "acp", runId: "wr_r", taskRunId: "tr_r", taskId: "r", profile: "agent", cwd: root, env: process.env, inputs: {}, run: [process.execPath, script], prompt: "Go", ...(outputSchema === undefined ? {} : { outputSchema }) }, { transcript: async (record) => { records.push(record); return records.length; }, session: async () => undefined, interact: async () => { throw new Error("unexpected"); } });
  const settled = await execution.done;
  const prompts = records.filter((r) => (r as { type?: string; direction?: string }).type === "acp" && (r as { direction?: string }).direction === "to_executor").map((r) => (r as { message: { method?: string; params?: { prompt?: Array<{ text: string }> } } }).message).filter((m) => m.method === "session/prompt").map((m) => m.params!.prompt![0]!.text);
  const events = records.filter((r) => (r as { type?: string }).type === "lifecycle").map((r) => (r as { event: string }).event);
  return { settled, prompts, events };
}
const VALID_RESULT = '{"outcome":"completed","message":"ok","output":{"n":1}}';

test("AcpExecutor repairs an invalid result with one follow-up turn on the same session", async () => {
  const { root, script } = await repairAgent("dagmar-acp-repair-", ['{"outcome":"completed","message":"ok","output":{"n":1}', VALID_RESULT]);
  const { settled, prompts, events } = await runRepairAgent(root, script);
  assert.ok("result" in settled, `expected a result, got ${JSON.stringify(settled)}`);
  assert.deepEqual(settled.result.output, { n: 1 });
  assert.equal(prompts.length, 2);
  assert.ok(prompts[1]!.startsWith(REPAIR_INSTRUCTION), "the repair prompt must not repeat the task");
  assert.match(prompts[1]!, /exactly one JSON value/);
  assert.ok(events.includes("acp_result_repair"));
});

test("AcpExecutor fails after the repair turn also returns an invalid result", async () => {
  const { root, script } = await repairAgent("dagmar-acp-repair-twice-", ["Done, see above.", "Still prose."]);
  const { settled, prompts } = await runRepairAgent(root, script);
  assert.equal("error" in settled && settled.error.code, "acp_failed");
  assert.match("error" in settled ? settled.error.message : "", /exactly one JSON value/);
  assert.equal(prompts.length, 2, "exactly one repair turn");
});

test("AcpExecutor does not repair a task whose own outputSchema is invalid", async () => {
  const { root, script } = await repairAgent("dagmar-acp-repair-schema-", [VALID_RESULT]);
  const { settled, prompts } = await runRepairAgent(root, script, { type: 123 });
  assert.equal("error" in settled && settled.error.message, "Task outputSchema is invalid");
  assert.equal(prompts.length, 1);
});

// Transient-error retry: each attempt spawns a new agent process, so the fake agent counts its
// processes in a file. The first `failures` of them answer `failAt` with a JSON-RPC error: the
// model selection (as claude-agent-acp does when it cannot confirm the model), the prompt, or the
// prompt after a tool call. Later processes complete the task.
async function retryAgent(prefix: string, failAt: "model" | "prompt" | "tool", failures: number): Promise<{ root: string; script: string }> {
  const root = await mkdtemp(join(tmpdir(), prefix)), script = join(root, "agent.mjs");
  await writeFile(script, `
import{createInterface}from'node:readline';import{appendFileSync,readFileSync}from'node:fs';
appendFileSync(${JSON.stringify(join(root, "count"))},'x');const failing=readFileSync(${JSON.stringify(join(root, "count"))},'utf8').length<=${failures};
const send=x=>process.stdout.write(JSON.stringify(x)+'\\n');
const fail=id=>send({jsonrpc:'2.0',id,error:{code:-32603,message:'Internal error',data:{details:'Try again'}}});
const options=${JSON.stringify([MODEL_OPTION])};
createInterface({input:process.stdin}).on('line',line=>{const m=JSON.parse(line);
if(m.method==='initialize')send({jsonrpc:'2.0',id:m.id,result:{protocolVersion:1,agentCapabilities:{}}});
if(m.method==='session/new')send({jsonrpc:'2.0',id:m.id,result:{sessionId:'t-1',configOptions:options}});
if(m.method==='session/set_config_option'){if(failing&&${JSON.stringify(failAt)}==='model')fail(m.id);else send({jsonrpc:'2.0',id:m.id,result:{configOptions:options.map(o=>({...o,currentValue:m.params.value}))}})}
if(m.method==='session/prompt'){
if(failing&&${JSON.stringify(failAt)}==='tool')send({jsonrpc:'2.0',method:'session/update',params:{sessionId:'t-1',update:{sessionUpdate:'tool_call',toolCallId:'c-1',title:'Edit'}}});
if(failing&&${JSON.stringify(failAt)}!=='model'){fail(m.id);return}
send({jsonrpc:'2.0',method:'session/update',params:{sessionId:'t-1',update:{sessionUpdate:'agent_message_chunk',content:{type:'text',text:'{"outcome":"completed","message":"ok","output":{}}'}}}});send({jsonrpc:'2.0',id:m.id,result:{stopReason:'end_turn'}})}
});
`);
  return { root, script };
}
// `retry` null leaves the executor on Dagmar's default retry.
function retryWorkflow(root: string, script: string, retry: { max: number; delaySeconds: number } | null = { max: 2, delaySeconds: 0 }) {
  return { id: "retry", executors: { agent: { type: "acp", cwd: root, run: [process.execPath, script], model: "opus", ...(retry ? { retry } : {}) } }, tasks: { a: { executor: "agent", inputs: {}, prompt: "Go" } } };
}

test("AcpExecutor marks an error the agent answers before any work as retryable", async () => {
  for (const failAt of ["model", "prompt"] as const) {
    const { root, script } = await retryAgent(`dagmar-acp-retryable-${failAt}-`, failAt, 1);
    const { settled } = await runModelAgent(root, script, { model: "opus" });
    assert.deepEqual(settled, { error: { code: "acp_failed", message: "Internal error" }, retryable: true }, failAt);
  }
  const { root, script } = await retryAgent("dagmar-acp-retryable-tool-", "tool", 1);
  const { settled } = await runModelAgent(root, script, { model: "opus" });
  assert.deepEqual(settled, { error: { code: "acp_failed", message: "Internal error" } });
});

test("scheduler retries an ACP attempt that failed before any work as a new attempt", async () => {
  const { root, script } = await retryAgent("dagmar-retry-", "model", 1);
  const app = await fixture(root, retryWorkflow(root, script), new AcpExecutor());
  const seen: Array<{ data: { attempt?: number; status?: string } }> = [];
  app.events.subscribe({ readyState: 1, send: (text: string) => seen.push(JSON.parse(text).params) } as never, {}).activate();
  const run = await app.scheduler.start("retry", {});
  const view = await waitFor(app.scheduler, run.workflowRunId, "completed");
  const attempts = view.tasks.a!.attempts;
  assert.deepEqual(attempts.map((x) => [x.attempt, x.status, x.error?.code ?? null]), [[1, "failed", "acp_failed"], [2, "completed", null]]);
  assert.deepEqual(seen.filter((x) => x.data.attempt !== undefined).map((x) => `${x.data.attempt}:${x.data.status}`), ["1:running", "1:failed", "2:running", "2:completed"]);
  const { records } = await app.transcripts.read(run.workflowRunId, attempts[1]!.id);
  assert.deepEqual(records.find((x) => x.type === "lifecycle" && x.event === "attempt_retry")?.data, { retryOf: attempts[0]!.id, retry: 1, max: 2, delaySeconds: 0 });
  app.store.close();
});

test("scheduler fails an ACP task as before once its retries reach max", async () => {
  const { root, script } = await retryAgent("dagmar-retry-max-", "prompt", 99);
  const app = await fixture(root, retryWorkflow(root, script), new AcpExecutor());
  const run = await app.scheduler.start("retry", {});
  const view = await waitFor(app.scheduler, run.workflowRunId, "blocked");
  assert.deepEqual(view.tasks.a!.attempts.map((x) => x.status), ["failed", "failed", "failed"]);
  app.store.close();
});

test("scheduler does not retry an ACP attempt that failed after a tool call", async () => {
  const { root, script } = await retryAgent("dagmar-retry-tool-", "tool", 1);
  const app = await fixture(root, retryWorkflow(root, script), new AcpExecutor());
  const run = await app.scheduler.start("retry", {});
  const view = await waitFor(app.scheduler, run.workflowRunId, "blocked");
  assert.deepEqual(view.tasks.a!.attempts.map((x) => [x.status, x.error?.code]), [["failed", "acp_failed"]]);
  app.store.close();
});

test("an ACP executor without retry gets the default retry, and cancelling a task interrupts its delay", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-retry-cancel-"));
  let starts = 0;
  const stub = new StubAcpExecutor(async () => { starts++; return { error: { code: "acp_failed", message: "Internal error" }, retryable: true }; });
  const app = await fixture(root, retryWorkflow(root, "unused", null), stub);
  const run = await app.scheduler.start("retry", {});
  for (let i = 0; i < 500 && (await app.scheduler.get(run.workflowRunId)).tasks.a!.attempts.length < 2; i++) await new Promise((resolve) => setTimeout(resolve, 10));
  const waiting = (await app.scheduler.get(run.workflowRunId)).tasks.a!.attempts;
  assert.deepEqual(waiting.map((x) => x.status), ["failed", "running"]);
  const { records } = await app.transcripts.read(run.workflowRunId, waiting[1]!.id);
  assert.deepEqual(records.find((x) => x.type === "lifecycle" && x.event === "attempt_retry")?.data, { retryOf: waiting[0]!.id, retry: 1, max: 2, delaySeconds: 300 });
  await app.scheduler.cancelTask(waiting[1]!.id);
  const view = await waitFor(app.scheduler, run.workflowRunId, "blocked");
  assert.deepEqual(view.tasks.a!.attempts.map((x) => x.status), ["failed", "cancelled"]);
  assert.equal(starts, 1);
  app.store.close();
});

test("retry max 0 turns the ACP retry off", async () => {
  const { root, script } = await retryAgent("dagmar-retry-off-", "model", 1);
  const app = await fixture(root, retryWorkflow(root, script, { max: 0, delaySeconds: 0 }), new AcpExecutor());
  const run = await app.scheduler.start("retry", {});
  const view = await waitFor(app.scheduler, run.workflowRunId, "blocked");
  assert.deepEqual(view.tasks.a!.attempts.map((x) => [x.status, x.error?.code]), [["failed", "acp_failed"]]);
  app.store.close();
});

// T2 (executor multi-turn): the AcpExecutor loops session/prompt on one live session
// when request.interactive === true. Turn 1 is conversational ("need input"); turn 2
// emits the envelope. The same agent connection takes both turns (two session/prompt
// sends on one sessionId) and the attempt settles `completed` with the envelope output.
test("AcpExecutor loops session/prompt across turns for an interactive task", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-acp-interactive-")), script = join(root, "agent.mjs");
  await writeFile(script, `
import{createInterface}from'node:readline';
const send=x=>process.stdout.write(JSON.stringify(x)+'\\n');
let prompts=0;
createInterface({input:process.stdin}).on('line',line=>{const m=JSON.parse(line);
if(m.method==='initialize')send({jsonrpc:'2.0',id:m.id,result:{protocolVersion:1,agentCapabilities:{sessionCapabilities:{close:{}}}}});
if(m.method==='session/new')send({jsonrpc:'2.0',id:m.id,result:{sessionId:'i-1'}});
if(m.method==='session/prompt'){
  prompts++;
  // Turn 1: conversational (non-envelope) → expect a turn interaction. Turn 2: envelope → done.
  const text=prompts===1?'need input':'{"outcome":"completed","message":"ok","output":{"recalled":"next"}}';
  send({jsonrpc:'2.0',method:'session/update',params:{sessionId:'i-1',update:{sessionUpdate:'agent_message_chunk',content:{type:'text',text}}}});
  send({jsonrpc:'2.0',id:m.id,result:{stopReason:'end_turn'}});
}
if(m.method==='session/close')send({jsonrpc:'2.0',id:m.id,result:{}});
});
`);
  const records: unknown[] = []; let interactCalls = 0; let capturedMessage = "";
  const execution = await new AcpExecutor().start(
    { type: "acp", runId: "wr_i", taskRunId: "tr_i", taskId: "i", profile: "agent", cwd: root, env: process.env, inputs: {}, run: [process.execPath, script], prompt: "Converse", interactive: true },
    {
      transcript: async (record) => { records.push(record); return records.length; },
      session: async () => {},
      interact: async (req) => {
        interactCalls++;
        assert.equal(req.kind, "turn");
        assert.equal(req.method, "turn/next");
        assert.equal((req.request as { message?: string }).message, "need input");
        capturedMessage = (req.request as { message: string }).message;
        // Validate runs the answer through the executor's `turn` validator.
        return req.validate("do it");
      },
    },
  );
  const settled = await execution.done;
  assert.equal("result" in settled && settled.result.outcome, "completed");
  assert.deepEqual("result" in settled ? settled.result.output : undefined, { recalled: "next" });
  assert.equal(interactCalls, 1, "interact must be called exactly once (one conversational turn)");
  assert.equal(capturedMessage, "need input");
  // Same live session took two turns: count session/prompt sends on the acp transcript.
  const promptMethods = records.filter((r) => (r as { type?: string }).type === "acp").map((r) => (r as { message?: { method?: string } }).message?.method);
  const promptCount = promptMethods.filter((x) => x === "session/prompt").length;
  assert.equal(promptCount, 2, "expected exactly two session/prompt sends on the live session");
  // The suspend lifecycle event must be recorded between the two turns.
  const events = records.map((r) => (r as { event?: string }).event).filter((x) => typeof x === "string");
  assert.ok(events.includes("acp_turn_suspended"), "expected an acp_turn_suspended lifecycle event");
  assert.ok(events.includes("acp_turn_completed"), "expected an acp_turn_completed lifecycle event");
});

// T2 (scheduler park→turn→complete): an ACP task with interactive:true whose fake
// executor issues one `turn` interaction via hooks.interact then completes after the
// user answers. The scheduler must park the attempt as awaiting_input with kind="turn",
// surface the interaction, and resume on answer(). Mirrors the existing ACP-interaction
// park/answer test (lines 162-173) but with a `turn` interaction instead of `permission`.
test("scheduler parks an interactive task awaiting a turn and completes on answer", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-turn-park-"));
  const workflow = { id: "turn", tasks: { talk: { executor: "agent", inputs: {}, prompt: "converse", interactive: true } } };
  const app = await fixture(root, workflow, new TurnInteractionExecutor());
  const started = await app.scheduler.start("turn", {});
  await waitFor(app.scheduler, started.workflowRunId, "waiting");
  const interaction = app.scheduler.interactions()[0]!;
  assert.equal(interaction.kind, "turn");
  assert.equal(interaction.method, "turn/next");
  await app.scheduler.answer(interaction.id, "next turn please");
  const completed = await waitFor(app.scheduler, started.workflowRunId, "completed");
  assert.equal(completed.tasks.talk!.state, "completed");
  assert.deepEqual(completed.tasks.talk!.attempts[0]!.result, { outcome: "completed", message: "turn", output: { turn: "next turn please" } });
  app.store.close();
});

// T2 (regression): the non-interactive single-shot path must be unchanged — a mock
// agent emitting non-envelope prose ("hello") + end_turn must settle the attempt as
// `failed` (the existing "ACP response must be exactly one JSON value" throw), and
// must NOT park a `turn` interaction. Guards AC5: no silent behavior change.
test("non-interactive ACP task with non-envelope prose fails fast without suspending", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-acp-nonenv-")), script = join(root, "agent.mjs");
  await writeFile(script, `
import{createInterface}from'node:readline';
const send=x=>process.stdout.write(JSON.stringify(x)+'\\n');
createInterface({input:process.stdin}).on('line',line=>{const m=JSON.parse(line);
if(m.method==='initialize')send({jsonrpc:'2.0',id:m.id,result:{protocolVersion:1,agentCapabilities:{sessionCapabilities:{close:{}}}}});
if(m.method==='session/new')send({jsonrpc:'2.0',id:m.id,result:{sessionId:'s-1'}});
if(m.method==='session/prompt'){send({jsonrpc:'2.0',method:'session/update',params:{sessionId:'s-1',update:{sessionUpdate:'agent_message_chunk',content:{type:'text',text:'hello'}}}});send({jsonrpc:'2.0',id:m.id,result:{stopReason:'end_turn'}})}
if(m.method==='session/close')send({jsonrpc:'2.0',id:m.id,result:{}});
});
`);
  let interactCalls = 0;
  const execution = await new AcpExecutor().start(
    { type: "acp", runId: "wr_n", taskRunId: "tr_n", taskId: "n", profile: "agent", cwd: root, env: process.env, inputs: {}, run: [process.execPath, script], prompt: "Do it" },
    {
      transcript: async () => 1,
      session: async () => {},
      interact: async () => { interactCalls++; throw new Error("non-interactive task must not request interactions"); },
    },
  );
  const settled = await execution.done;
  assert.ok("error" in settled, "expected a failed settlement, not a parked turn");
  assert.equal("error" in settled && settled.error.code, "acp_failed");
  assert.match("error" in settled ? settled.error.message : "", /ACP response must be exactly one JSON value/);
  assert.equal(interactCalls, 0);
});

// T3 (workflow validation): gate tasks are accepted (with normalized gate block) and every reject
// variant the plan enumerates is surfaced as a clear invalid_task. Mirrors the T1 validateWorkflow
// test (lines 348-426): profiles are passed in directly so each case pins a specific failure mode
// without disk I/O.
test("validateWorkflow accepts gate tasks and rejects invalid variants", () => {
  const profiles: Record<string, ExecutorProfile> = { agent: { type: "acp", cwd: "/tmp", env: {}, run: ["agent"] } };

  // Accept: a gate task with no executor and a prompt.
  const ok1 = validate({ id: "g1", tasks: { g: { inputs: {}, gate: { prompt: "Approve?" } } } }, profiles);
  assert.equal(ok1.tasks.g!.gate?.prompt, "Approve?");
  assert.equal(ok1.tasks.g!.gate?.schema, undefined);
  assert.equal(ok1.tasks.g!.executor, undefined);

  // Accept: a gate task with a schema; the schema flows through.
  const ok2 = validate({
    id: "g2",
    tasks: { g: { inputs: {}, gate: { prompt: "Review", schema: { type: "string" } } } },
  }, profiles);
  assert.deepEqual(ok2.tasks.g!.gate?.schema, { type: "string" });

  // Accept: a gate task with deps + inputs (references will resolve during prepare()).
  const ok3 = validate({
    id: "g3",
    tasks: {
      dep: { executor: "agent", inputs: {}, prompt: "a" },
      g: { dependsOn: ["dep"], inputs: { x: "$tasks.dep.output" }, gate: { prompt: "?" } },
    },
  }, profiles);
  assert.deepEqual(ok3.tasks.g!.dependsOn, ["dep"]);

  // Reject: task with both executor and gate.
  assert.throws(
    () => validate({ id: "x", tasks: { g: { executor: "agent", inputs: {}, prompt: "p", gate: { prompt: "?" } } } }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_task" && /cannot have both/.test(error.message),
  );

  // Reject: task with neither executor nor gate.
  assert.throws(
    () => validate({ id: "x", tasks: { g: { inputs: {} } } }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_task" && /requires executor or gate/.test(error.message),
  );

  // Reject: gate task with prompt (the task-level field, not gate.prompt).
  assert.throws(
    () => validate({ id: "x", tasks: { g: { inputs: {}, prompt: "p", gate: { prompt: "?" } } } }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_task" && /must not have/.test(error.message),
  );

  // Reject: gate task with run.
  assert.throws(
    () => validate({ id: "x", tasks: { g: { inputs: {}, run: ["x"], gate: { prompt: "?" } } } }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_task" && /must not have/.test(error.message),
  );

  // Reject: gate task with session.
  assert.throws(
    () => validate({ id: "x", tasks: { g: { inputs: {}, gate: { prompt: "?" }, session: { mode: "fresh" } } } }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_task" && /must not have/.test(error.message),
  );

  // Reject: gate task with interactive.
  assert.throws(
    () => validate({ id: "x", tasks: { g: { inputs: {}, gate: { prompt: "?" }, interactive: true } } }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_task" && /must not have/.test(error.message),
  );

  // Reject: gate task with outputSchema.
  assert.throws(
    () => validate({ id: "x", tasks: { g: { inputs: {}, gate: { prompt: "?" }, outputSchema: { type: "object" } } } }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_task" && /must not have/.test(error.message),
  );

  // Reject: empty gate prompt.
  assert.throws(
    () => validate({ id: "x", tasks: { g: { inputs: {}, gate: { prompt: "   " } } } }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_task" && /non-empty prompt/.test(error.message),
  );

  // Reject: invalid gate schema (JSON Schema that fails ajv.compile).
  assert.throws(
    () => validate({ id: "x", tasks: { g: { inputs: {}, gate: { prompt: "?", schema: { type: 123 } } } } }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_output_schema",
  );
});

// T3 (gate park/answer/complete): a gate task after a dependency parks the attempt as
// awaiting_input with kind:"gate", surfaces the interaction, and completes when the human
// answers via interaction.answer. Mirrors the ACP interaction park/answer test at lines 162-173
// but with a `gate` interaction instead of `permission`.
test("scheduler parks a gate task awaiting input and completes on answer", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-gate-park-"));
  const workflow = {
    id: "gatepark",
    tasks: {
      dep: { executor: "agent", inputs: {}, prompt: "do" },
      gate: { dependsOn: ["dep"], inputs: {}, gate: { prompt: "Approve?" } },
    },
  };
  const app = await fixture(root, workflow);
  const started = await app.scheduler.start("gatepark", {});
  // Answer the dep's permission interaction first; the dep then completes and the gate parks.
  await waitFor(app.scheduler, started.workflowRunId, "waiting");
  const permissionInteraction = app.scheduler.interactions().find((x) => x.kind === "permission")!;
  await app.scheduler.answer(permissionInteraction.id, { outcome: { outcome: "selected", optionId: "yes" } });
  // Now the gate should park.
  for (let i = 0; i < 500; i++) {
    const v = await app.scheduler.get(started.workflowRunId);
    if (v.tasks.gate!.state === "awaiting_input") break;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  const depView = (await app.scheduler.get(started.workflowRunId)).tasks.dep!;
  assert.equal(depView.state, "completed");
  const parkedView = await app.scheduler.get(started.workflowRunId);
  assert.equal(parkedView.tasks.gate!.executor, "gate", "view() surfaces 'gate' as the executor for gate tasks (no task.executor was set)");
  const gateInteraction = app.scheduler.interactions().find((x) => x.kind === "gate")!;
  assert.ok(gateInteraction, "expected a gate interaction");
  assert.equal(gateInteraction.method, "gate/answer");
  // Answer the gate.
  await app.scheduler.answer(gateInteraction.id, "approved");
  const completed = await waitFor(app.scheduler, started.workflowRunId, "completed");
  assert.equal(completed.tasks.gate!.state, "completed");
  assert.equal(completed.tasks.gate!.attempts[0]!.result!.output, "approved");
  assert.equal(completed.tasks.gate!.attempts[0]!.result!.message, "Approve?");
  app.store.close();
});

// T3 (gate with schema): a gate with a JSON Schema validates the answer. A valid answer
// completes the gate, an invalid one raises invalid_interaction_response (same shape as other
// interactions). Mirrors the ACP-interaction park/answer pattern.
test("scheduler validates a gate answer against its schema", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-gate-schema-"));
  const workflow = {
    id: "gateschema",
    tasks: {
      gate: { inputs: {}, gate: { prompt: "Decide", schema: { type: "object", required: ["decision"], properties: { decision: { type: "string" } } } } },
    },
  };
  const app = await fixture(root, workflow);
  const started = await app.scheduler.start("gateschema", {});
  await waitFor(app.scheduler, started.workflowRunId, "waiting");
  const interaction = app.scheduler.interactions()[0]!;
  assert.equal(interaction.kind, "gate");
  // Valid answer.
  await app.scheduler.answer(interaction.id, { decision: "approve" });
  const completed = await waitFor(app.scheduler, started.workflowRunId, "completed");
  assert.deepEqual(completed.tasks.gate!.attempts[0]!.result!.output, { decision: "approve" });
  app.store.close();

  // Invalid answer: a second run with the same workflow, gate answer that fails schema.
  const root2 = await mkdtemp(join(tmpdir(), "dagmar-gate-schema-bad-"));
  const app2 = await fixture(root2, workflow);
  const started2 = await app2.scheduler.start("gateschema", {});
  await waitFor(app2.scheduler, started2.workflowRunId, "waiting");
  const interaction2 = app2.scheduler.interactions()[0]!;
  await assert.rejects(
    app2.scheduler.answer(interaction2.id, 42),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_interaction_response",
  );
  // The gate is still parked (no settlement on bad answer).
  const still = await app2.scheduler.get(started2.workflowRunId);
  assert.equal(still.tasks.gate!.state, "awaiting_input");
  assert.equal(still.status, "waiting");
  app2.store.close();
});

// T3 (root gate): a gate task with no dependencies parks immediately on run start, then
// completes when answered. Validates that the gate branch fires for the first ready task too.
test("a root gate with no dependencies parks immediately on run start", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-gate-root-"));
  const workflow = { id: "gateroot", tasks: { gate: { inputs: {}, gate: { prompt: "Start?" } } } };
  const app = await fixture(root, workflow);
  const started = await app.scheduler.start("gateroot", {});
  const waiting = await waitFor(app.scheduler, started.workflowRunId, "waiting");
  assert.equal(waiting.tasks.gate!.state, "awaiting_input");
  const interaction = app.scheduler.interactions()[0]!;
  assert.equal(interaction.kind, "gate");
  await app.scheduler.answer(interaction.id, "go");
  const completed = await waitFor(app.scheduler, started.workflowRunId, "completed");
  assert.equal(completed.tasks.gate!.state, "completed");
  assert.equal(completed.tasks.gate!.attempts[0]!.result!.output, "go");
  app.store.close();
});

// T3 (recovery carve-out): a gate attempt inserted into the store with awaiting_input +
// executorType:"gate" must survive a daemon restart. recover() does NOT mark it executor_lost —
// it reconstructs the Pending entry from the persisted row + workflow definition. The answer
// remains valid (the gate is answerable after recovery). Mirrors the recovery test at 211-217.
test("recover() preserves a gate attempt and reconstructs its interaction", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-gate-recover-"));
  const workflow = { id: "gaterecover", tasks: { gate: { inputs: {}, gate: { prompt: "Approve?" } } } };
  const app = await fixture(root, workflow);
  const now = new Date().toISOString();
  app.store.insertRun({ id: "wr_g", workflowId: "gaterecover", cwd: null, input: {}, status: "waiting", startedAt: now, updatedAt: now, endedAt: null });
  app.store.insertAttempt({ id: "tr_g", workflowRunId: "wr_g", taskId: "gate", attempt: 1, executorProfile: "gate", executorType: "gate", status: "awaiting_input", result: null, error: null, acpSessionId: null, startedAt: now, updatedAt: now, endedAt: null, item: null, roundId: null });
  await app.scheduler.recover();
  // Gate survives.
  const attempt = app.store.attempt("tr_g")!;
  assert.equal(attempt.status, "awaiting_input");
  assert.equal(attempt.error, null);
  // Run status recomputed (waiting, not blocked).
  assert.equal(app.store.run("wr_g")!.status, "waiting");
  // Interaction reconstructed.
  const interaction = app.scheduler.interactions()[0]!;
  assert.equal(interaction.kind, "gate");
  assert.equal(interaction.method, "gate/answer");
  // Answer still works.
  await app.scheduler.answer(interaction.id, "yes");
  const completed = await waitFor(app.scheduler, "wr_g", "completed");
  assert.equal(completed.tasks.gate!.state, "completed");
  app.store.close();
});

// T3 (mixed recovery): a run with both a gate attempt (awaiting_input, executorType:"gate") and
// a non-gate attempt (running, executorType:"process") recovers the gate and fails the non-gate.
// The run's final state is waiting because the surviving gate is still active.
test("recover() fails non-gate attempts but preserves gate attempts in the same run", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-gate-mixed-recover-"));
  const workflow = {
    id: "gatemix",
    tasks: {
      dep: { executor: "agent", inputs: {}, prompt: "do" },
      gate: { dependsOn: ["dep"], inputs: {}, gate: { prompt: "Approve?" } },
    },
  };
  const app = await fixture(root, workflow);
  const now = new Date().toISOString();
  app.store.insertRun({ id: "wr_m", workflowId: "gatemix", cwd: null, input: {}, status: "waiting", startedAt: now, updatedAt: now, endedAt: null });
  // Non-gate (process) attempt in 'running'.
  app.store.insertAttempt({ id: "tr_m_dep", workflowRunId: "wr_m", taskId: "dep", attempt: 1, executorProfile: "local", executorType: "process", status: "running", result: null, error: null, acpSessionId: null, startedAt: now, updatedAt: now, endedAt: null, item: null, roundId: null });
  // Gate attempt in 'awaiting_input'.
  app.store.insertAttempt({ id: "tr_m_gate", workflowRunId: "wr_m", taskId: "gate", attempt: 1, executorProfile: "gate", executorType: "gate", status: "awaiting_input", result: null, error: null, acpSessionId: null, startedAt: now, updatedAt: now, endedAt: null, item: null, roundId: null });
  await app.scheduler.recover();
  // Non-gate: failed with executor_lost.
  const dep = app.store.attempt("tr_m_dep")!;
  assert.equal(dep.status, "failed");
  assert.equal(dep.error?.code, "executor_lost");
  // Gate: preserved.
  const gate = app.store.attempt("tr_m_gate")!;
  assert.equal(gate.status, "awaiting_input");
  assert.equal(gate.error, null);
  // Run status is still waiting (gate survived).
  assert.equal(app.store.run("wr_m")!.status, "waiting");
  app.store.close();
});

// T3 (gate input-resolution failure): a gate task whose `inputs` reference a path that does not
// resolve against a completed dep's output fails synchronously at prepare() time. The attempt
// is inserted with status: "failed" and error.code "input_resolution_failed", no Pending entry
// is registered, and the run reaches "blocked" without ever producing a gate interaction.
test("a gate with an unresolvable input fails at prepare() rather than parking", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-gate-input-fail-"));
  const stub = new StubAcpExecutor(async () => ({ result: { outcome: "completed", message: "ok", output: null } }));
  const workflow = {
    id: "gateinputfail",
    tasks: {
      dep: { executor: "agent", inputs: {}, prompt: "do" },
      // $tasks.dep.output.x on a null output: validation accepts (path syntax is valid), but
      // resolveInputs() throws at prepare() time because !current short-circuits the path walk.
      gate: { dependsOn: ["dep"], inputs: { v: "$tasks.dep.output.x" }, gate: { prompt: "?" } },
    },
  };
  const app = await fixture(root, workflow, stub);
  const started = await app.scheduler.start("gateinputfail", {});
  const blocked = await waitFor(app.scheduler, started.workflowRunId, "blocked");
  assert.equal(blocked.tasks.gate!.state, "failed");
  assert.equal(blocked.tasks.gate!.attempts[0]!.error!.code, "input_resolution_failed");
  assert.equal(blocked.status, "blocked");
  assert.equal(app.scheduler.interactions().length, 0, "a failed-at-prepare gate must not register a Pending interaction");
  app.store.close();
});

// T3 (recover() clears stale endedAt on non-terminal restoration): when a run arrives at recover()
// with endedAt set (because a prior shutdown() wrote it via failActiveLocked + skipGates:true),
// and the recomputed run status is non-terminal ("waiting", because a gate survived), the run's
// endedAt must be cleared — mirroring the symmetric advance() pattern. Without this, run.list /
// run.get surface a stale (status="waiting", endedAt!=null) view.
test("recover() clears endedAt when restoring a mixed gate+non-gate run to waiting", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-gate-recover-endedat-"));
  const workflow = {
    id: "gatemix2",
    tasks: {
      dep: { executor: "agent", inputs: {}, prompt: "do" },
      gate: { dependsOn: ["dep"], inputs: {}, gate: { prompt: "Approve?" } },
    },
  };
  const app = await fixture(root, workflow);
  const now = new Date().toISOString();
  // Run with endedAt set (simulating the state left by the prior shutdown).
  app.store.insertRun({ id: "wr_m2", workflowId: "gatemix2", cwd: null, input: {}, status: "waiting", startedAt: now, updatedAt: now, endedAt: now });
  app.store.insertAttempt({ id: "tr_m2_dep", workflowRunId: "wr_m2", taskId: "dep", attempt: 1, executorProfile: "local", executorType: "process", status: "running", result: null, error: null, acpSessionId: null, startedAt: now, updatedAt: now, endedAt: null, item: null, roundId: null });
  app.store.insertAttempt({ id: "tr_m2_gate", workflowRunId: "wr_m2", taskId: "gate", attempt: 1, executorProfile: "gate", executorType: "gate", status: "awaiting_input", result: null, error: null, acpSessionId: null, startedAt: now, updatedAt: now, endedAt: null, item: null, roundId: null });
  assert.equal(app.store.run("wr_m2")!.endedAt, now, "precondition: run arrived with stale endedAt");
  await app.scheduler.recover();
  const run = app.store.run("wr_m2")!;
  assert.equal(run.status, "waiting");
  assert.equal(run.endedAt, null, "recover() must clear endedAt when the recomputed status is non-terminal");
  assert.equal(app.store.attempt("tr_m2_gate")!.status, "awaiting_input");
  assert.equal(app.store.attempt("tr_m2_dep")!.error!.code, "executor_lost");
  // run.get view: endedAt is null on the surfaced shape too (no non-JSON-null leak).
  const view = await app.scheduler.get("wr_m2");
  assert.equal(view.endedAt, null);
  app.store.close();
});

// T3 (shutdown preserves gates): shutdown() does NOT mark gate attempts as daemon_shutdown —
// the skipGates:true flag keeps them alive across a graceful shutdown. A fresh scheduler on
// the same store recovers them and the human can still answer.
test("shutdown() preserves gate attempts across restart", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-gate-shutdown-"));
  const workflow = { id: "gateshutdown", tasks: { gate: { inputs: {}, gate: { prompt: "Approve?" } } } };
  const storePath = join(root, "state", "dagmar.sqlite");
  // First scheduler — start a run that parks the gate, then shut down.
  const { mkdir } = await import("node:fs/promises");
  await mkdir(join(root, "workflows"), { recursive: true });
  await mkdir(join(root, "state"), { recursive: true });
  await writeFile(join(root, "workflows", `${workflow.id}.yaml`), stringify(workflow));
  const store1 = new Store(storePath), transcripts1 = new Transcripts(join(root, "state")), events1 = new EventBus();
  const scheduler1 = new Scheduler(store1, transcripts1, new WorkflowRepository(join(root, "workflows")), events1, { process: new ProcessExecutor(), acp: new AcpExecutor() });
  const started = await scheduler1.start("gateshutdown", {});
  await waitFor(scheduler1, started.workflowRunId, "waiting");
  await scheduler1.shutdown();
  // Gate attempt is still awaiting_input (not daemon_shutdown).
  const attempts1 = store1.attempts(started.workflowRunId);
  assert.equal(attempts1.length, 1);
  assert.equal(attempts1[0]!.executorType, "gate");
  assert.equal(attempts1[0]!.status, "awaiting_input");
  store1.close();

  // Second scheduler on the same store — recover() reconstructs the gate.
  const store2 = new Store(storePath), transcripts2 = new Transcripts(join(root, "state")), events2 = new EventBus();
  const scheduler2 = new Scheduler(store2, transcripts2, new WorkflowRepository(join(root, "workflows")), events2, { process: new ProcessExecutor(), acp: new AcpExecutor() });
  await scheduler2.recover();
  const after = await scheduler2.get(started.workflowRunId);
  assert.equal(after.tasks.gate!.state, "awaiting_input");
  assert.equal(after.status, "waiting");
  const interaction = scheduler2.interactions()[0]!;
  assert.equal(interaction.kind, "gate");
  await scheduler2.answer(interaction.id, "yes");
  const completed = await waitFor(scheduler2, started.workflowRunId, "completed");
  assert.equal(completed.tasks.gate!.state, "completed");
  assert.equal(completed.tasks.gate!.attempts[0]!.result!.output, "yes");
  store2.close();
});

// T3 (gate output feeds downstream tasks): $tasks.<gate>.output resolves to the human's answer
// in a downstream task. Mirrors the diamond test's input-reference wiring but with a gate source.
test("gate output is available as $tasks.<gate>.output to downstream tasks", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-gate-output-"));
  const captured = { output: undefined as Json | undefined };
  const stub = new StubAcpExecutor(async (request) => {
    captured.output = request.inputs.value;
    return { result: { outcome: "completed", message: "got it", output: {} } };
  });
  const workflow = {
    id: "gateout",
    tasks: {
      gate: { inputs: {}, gate: { prompt: "Answer?" } },
      task: { executor: "agent", dependsOn: ["gate"], inputs: { value: "$tasks.gate.output" }, prompt: "use value" },
    },
  };
  const app = await fixture(root, workflow, stub);
  const started = await app.scheduler.start("gateout", {});
  await waitFor(app.scheduler, started.workflowRunId, "waiting");
  const interaction = app.scheduler.interactions()[0]!;
  assert.equal(interaction.kind, "gate");
  await app.scheduler.answer(interaction.id, { answer: "yes" });
  const completed = await waitFor(app.scheduler, started.workflowRunId, "completed");
  assert.equal(completed.status, "completed");
  assert.equal(completed.tasks.gate!.state, "completed");
  assert.equal(completed.tasks.task!.state, "completed");
  assert.deepEqual(captured.output, { answer: "yes" });
  app.store.close();
});


// T4 (when validation matrix): validateWorkflow accepts well-formed when clauses (ref/equals/in/dep
// checks, run-input ref with no deps, multi-clause AND, gate+when), and rejects the variants the
// plan enumerates. Profiles are passed directly so each rejection pins a specific failure mode.
test("validateWorkflow accepts when guards and rejects malformed variants", () => {
  const profiles: Record<string, ExecutorProfile> = {
    local: { type: "process", cwd: "/tmp", env: {} },
  };
  // Accept: ref + equals, dep [decide].
  const ok1 = validate({
    id: "ok1",
    tasks: {
      decide: { executor: "local", inputs: {}, run: ["true"] },
      branch: { executor: "local", dependsOn: ["decide"], inputs: {}, run: ["true"], when: [{ ref: "$tasks.decide.output.route", equals: "a" }] },
    },
  }, profiles);
  assert.equal(ok1.tasks.branch!.when!.length, 1);
  assert.equal(ok1.tasks.branch!.when![0]!.ref, "$tasks.decide.output.route");
  assert.equal(ok1.tasks.branch!.when![0]!.equals, "a");

  // Accept: ref + in.
  const ok2 = validate({
    id: "ok2",
    tasks: {
      decide: { executor: "local", inputs: {}, run: ["true"] },
      branch: { executor: "local", dependsOn: ["decide"], inputs: {}, run: ["true"], when: [{ ref: "$tasks.decide.output.route", in: ["a", "b"] }] },
    },
  }, profiles);
  assert.deepEqual(ok2.tasks.branch!.when![0]!.in, ["a", "b"]);

  // Accept: run-input ref with NO deps.
  const ok3 = validate({
    id: "ok3",
    tasks: { t: { executor: "local", inputs: {}, run: ["true"], when: [{ ref: "$run.input.mode", equals: "fast" }] } },
  }, profiles);
  assert.equal(ok3.tasks.t!.when![0]!.ref, "$run.input.mode");

  // Accept: multi-clause AND.
  const ok4 = validate({
    id: "ok4",
    tasks: {
      decide: { executor: "local", inputs: {}, run: ["true"] },
      branch: {
        executor: "local", dependsOn: ["decide"], inputs: {}, run: ["true"],
        when: [{ ref: "$tasks.decide.output.route", equals: "a" }, { ref: "$run.input.mode", in: ["fast", "slow"] }],
      },
    },
  }, profiles);
  assert.equal(ok4.tasks.branch!.when!.length, 2);

  // Accept: when on a gate task (guard evaluated before the gate branch).
  const ok5 = validate({
    id: "ok5",
    tasks: {
      decide: { executor: "local", inputs: {}, run: ["true"] },
      gate: { dependsOn: ["decide"], inputs: {}, gate: { prompt: "Approve?" }, when: [{ ref: "$tasks.decide.output.route", equals: "a" }] },
    },
  }, profiles);
  assert.equal(ok5.tasks.gate!.when!.length, 1);
  assert.ok(ok5.tasks.gate!.gate);

  // Reject: clause with neither equals nor in.
  assert.throws(
    () => validate({
      id: "x",
      tasks: { t: { executor: "local", inputs: {}, run: ["true"], when: [{ ref: "$run.input.mode" }] } },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_guard" && /exactly one/.test(error.message),
  );

  // Reject: clause with BOTH equals and in.
  assert.throws(
    () => validate({
      id: "x",
      tasks: { t: { executor: "local", inputs: {}, run: ["true"], when: [{ ref: "$run.input.mode", equals: "a", in: ["a"] }] } },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_guard" && /exactly one/.test(error.message),
  );

  // Reject: empty in.
  assert.throws(
    () => validate({
      id: "x",
      tasks: { t: { executor: "local", inputs: {}, run: ["true"], when: [{ ref: "$run.input.mode", in: [] }] } },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_guard" && /non-empty/.test(error.message),
  );

  // Reject: $tasks ref to a non-dependency.
  assert.throws(
    () => validate({
      id: "x",
      tasks: {
        a: { executor: "local", inputs: {}, run: ["true"] },
        b: { executor: "local", inputs: {}, run: ["true"], when: [{ ref: "$tasks.a.output.route", equals: "x" }] },
      },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_guard" && /non-dependency/.test(error.message),
  );

  // Reject: plain-string ref (not a $ reference).
  assert.throws(
    () => validate({
      id: "x",
      tasks: { t: { executor: "local", inputs: {}, run: ["true"], when: [{ ref: "route", equals: "a" }] } },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_guard" && /\$tasks or \$run/.test(error.message),
  );

  // Reject: malformed $ ref -> invalid_input_reference (propagated from reference()).
  assert.throws(
    () => validate({
      id: "x",
      tasks: { t: { executor: "local", inputs: {}, run: ["true"], when: [{ ref: "$tasks.", equals: "a" }] } },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_input_reference",
  );
});

// T4 (N-way decider): an upstream decide task picks a route; three branches each gate on a different
// value. Exactly one branch runs, the other two skip, and the run completes (not blocked).
// Uses the local process executor with an echo script that emits {outcome,output:<inputs>}.
test("scheduler runs only the matching branch of an N-way decider and skips the rest", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-decider-"));
  const script = join(root, "task.mjs");
  await writeFile(
    script,
    `
import {readFileSync} from 'node:fs';
const input=[]; for await (const c of process.stdin) input.push(c);
const data=JSON.parse(input.join(''));
console.log(JSON.stringify({outcome:'completed',message:'ok',output:{lane:data.route ?? 'decide',got:data}}));
`,
  );
  const workflow = {
    id: "decider",
    tasks: {
      decide: {
        executor: "local",
        inputs: { route: "$run.input.route" },
        run: [process.execPath, script],
      },
      branchA: {
        executor: "local",
        dependsOn: ["decide"],
        inputs: { route: "$tasks.decide.output.got.route" },
        run: [process.execPath, script],
        when: [{ ref: "$tasks.decide.output.got.route", equals: "a" }],
      },
      branchB: {
        executor: "local",
        dependsOn: ["decide"],
        inputs: { route: "$tasks.decide.output.got.route" },
        run: [process.execPath, script],
        when: [{ ref: "$tasks.decide.output.got.route", equals: "b" }],
      },
      branchC: {
        executor: "local",
        dependsOn: ["decide"],
        inputs: { route: "$tasks.decide.output.got.route" },
        run: [process.execPath, script],
        when: [{ ref: "$tasks.decide.output.got.route", equals: "c" }],
      },
    },
  };
  const app = await fixture(root, workflow);
  const started = await app.scheduler.start("decider", { route: "b" });
  const view = await waitFor(app.scheduler, started.workflowRunId, "completed");
  assert.equal(view.status, "completed");
  assert.deepEqual(
    Object.fromEntries(Object.entries(view.tasks).map(([id, x]) => [id, x.state])),
    { decide: "completed", branchA: "skipped", branchB: "completed", branchC: "skipped" },
  );
  // Skipped attempts have a single row with status: skipped and endedAt set; no request was sent.
  assert.equal(view.tasks.branchA!.attempts.length, 1);
  assert.equal(view.tasks.branchA!.attempts[0]!.status, "skipped");
  assert.notEqual(view.tasks.branchA!.attempts[0]!.endedAt, null);
  assert.equal(view.tasks.branchB!.attempts[0]!.status, "completed");
  app.store.close();
});

// T4 (cascade via guard referencing a skipped output): a1's guard references decide.output.route;
// a2's guard references a1.output. If decide.output.route != "a", a1 skips; a2's guard then
// references a skipped dep, so the cascade short-circuits and a2 also skips.
test("scheduler cascades a skip through a guard referencing a skipped output", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-cascade-"));
  const script = join(root, "task.mjs");
  await writeFile(
    script,
    `
const input=[]; for await (const c of process.stdin) input.push(c);
const data=JSON.parse(input.join(''));
console.log(JSON.stringify({outcome:'completed',message:'ok',output:{x:1,route:data.route ?? null}}));
`,
  );
  const workflow = {
    id: "cascade",
    tasks: {
      decide: { executor: "local", inputs: { route: "$run.input.route" }, run: [process.execPath, script] },
      a1: {
        executor: "local", dependsOn: ["decide"], inputs: { route: "$tasks.decide.output.route" }, run: [process.execPath, script],
        when: [{ ref: "$tasks.decide.output.route", equals: "a" }],
      },
      a2: {
        executor: "local", dependsOn: ["a1"], inputs: {}, run: [process.execPath, script],
        when: [{ ref: "$tasks.a1.output.x", equals: 1 }],
      },
    },
  };
  const app = await fixture(root, workflow);
  const started = await app.scheduler.start("cascade", { route: "b" });
  const view = await waitFor(app.scheduler, started.workflowRunId, "completed");
  assert.deepEqual(
    Object.fromEntries(Object.entries(view.tasks).map(([id, x]) => [id, x.state])),
    { decide: "completed", a1: "skipped", a2: "skipped" },
  );
  assert.equal(view.status, "completed");
  app.store.close();
});

// T4 (no-guard join past a skipped sibling): a1 has a guard and skips; keep has NO guard and
// references decide (not a1) so it runs past the skip and completes.
test("a no-guard task runs past a skipped sibling it does not reference", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-noguard-join-"));
  const script = join(root, "task.mjs");
  await writeFile(
    script,
    `
const input=[]; for await (const c of process.stdin) input.push(c);
const data=JSON.parse(input.join(''));
console.log(JSON.stringify({outcome:'completed',message:'ok',output:{route:data.r ?? null}}));
`,
  );
  const workflow = {
    id: "noguard",
    tasks: {
      decide: { executor: "local", inputs: { r: "$run.input.route" }, run: [process.execPath, script] },
      a1: {
        executor: "local", dependsOn: ["decide"], inputs: {}, run: [process.execPath, script],
        when: [{ ref: "$tasks.decide.output.route", equals: "a" }],
      },
      keep: {
        executor: "local", dependsOn: ["decide"], inputs: { r: "$tasks.decide.output.route" }, run: [process.execPath, script],
      },
    },
  };
  const app = await fixture(root, workflow);
  const started = await app.scheduler.start("noguard", { route: "b" });
  const view = await waitFor(app.scheduler, started.workflowRunId, "completed");
  assert.deepEqual(
    Object.fromEntries(Object.entries(view.tasks).map(([id, x]) => [id, x.state])),
    { decide: "completed", a1: "skipped", keep: "completed" },
  );
  assert.equal(view.status, "completed");
  app.store.close();
});

// T4 (no-guard task referencing a skipped output fails): convention violation surface. a1 skips;
// join has no guard and references a1.output -> input_resolution_failed at prepare() time. Run
// reaches blocked, not completed.
test("a no-guard task referencing a skipped output fails with input_resolution_failed", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-skip-ref-"));
  const script = join(root, "task.mjs");
  await writeFile(
    script,
    `
const input=[]; for await (const c of process.stdin) input.push(c);
const data=JSON.parse(input.join(''));
console.log(JSON.stringify({outcome:'completed',message:'ok',output:{route:data.route ?? null}}));
`,
  );
  const workflow = {
    id: "skipref",
    tasks: {
      decide: { executor: "local", inputs: { route: "$run.input.route" }, run: [process.execPath, script] },
      a1: {
        executor: "local", dependsOn: ["decide"], inputs: {}, run: [process.execPath, script],
        when: [{ ref: "$tasks.decide.output.route", equals: "a" }],
      },
      join: {
        executor: "local", dependsOn: ["a1"], inputs: { x: "$tasks.a1.output" }, run: [process.execPath, script],
      },
    },
  };
  const app = await fixture(root, workflow);
  const started = await app.scheduler.start("skipref", { route: "b" });
  const blocked = await waitFor(app.scheduler, started.workflowRunId, "blocked");
  assert.equal(blocked.tasks.a1!.state, "skipped");
  assert.equal(blocked.tasks.join!.state, "failed");
  assert.equal(blocked.tasks.join!.attempts[0]!.error!.code, "input_resolution_failed");
  assert.equal(blocked.status, "blocked");
  const resumed = await app.scheduler.resume(started.workflowRunId);
  assert.deepEqual(resumed.tasks.join!.attempts.map((a) => a.status), ["failed", "failed"]);
  app.store.close();
});

// T4 (missing path -> skip, never throws): decide.output lacks `flavor`; b's guard references
// $tasks.decide.output.flavor. walk() returns undefined for the missing path, the clause is
// false, b skips. Run completes (not blocked).
test("a guard referencing a missing path causes the task to skip (not fail)", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-missing-"));
  const script = join(root, "task.mjs");
  await writeFile(
    script,
    `
const input=[]; for await (const c of process.stdin) input.push(c);
const data=JSON.parse(input.join(''));
console.log(JSON.stringify({outcome:'completed',message:'ok',output:{route:data.route ?? null}}));
`,
  );
  const workflow = {
    id: "missing",
    tasks: {
      decide: { executor: "local", inputs: { route: "$run.input.route" }, run: [process.execPath, script] },
      b: {
        executor: "local", dependsOn: ["decide"], inputs: {}, run: [process.execPath, script],
        when: [{ ref: "$tasks.decide.output.flavor", equals: "x" }],
      },
    },
  };
  const app = await fixture(root, workflow);
  const started = await app.scheduler.start("missing", { route: "a" });
  const view = await waitFor(app.scheduler, started.workflowRunId, "completed");
  assert.equal(view.tasks.decide!.state, "completed");
  assert.equal(view.tasks.b!.state, "skipped");
  assert.equal(view.status, "completed");
  app.store.close();
});

// T4 (run-input guard): a single task with no deps and a guard on $run.input. Two separate runs
// (different workflow ids because one active run per workflow), one with mode:slow -> skip, one
// with mode:fast -> complete.
test("a run-input guard decides on the run input and skips or completes accordingly", async () => {
  // Skip case.
  const root1 = await mkdtemp(join(tmpdir(), "dagmar-runinput-skip-"));
  const script = join(root1, "task.mjs");
  await writeFile(script, `for await (const _ of process.stdin){};console.log(JSON.stringify({outcome:'completed',message:'ok',output:{}}));`);
  const skipWf = {
    id: "runinputskip",
    tasks: { t: { executor: "local", inputs: {}, run: [process.execPath, script], when: [{ ref: "$run.input.mode", equals: "fast" }] } },
  };
  const app1 = await fixture(root1, skipWf);
  const started1 = await app1.scheduler.start("runinputskip", { mode: "slow" });
  const view1 = await waitFor(app1.scheduler, started1.workflowRunId, "completed");
  assert.equal(view1.tasks.t!.state, "skipped");
  assert.equal(view1.status, "completed");
  app1.store.close();

  // Run case (different workflow id).
  const root2 = await mkdtemp(join(tmpdir(), "dagmar-runinput-run-"));
  const runWf = {
    id: "runinputrun",
    tasks: { t: { executor: "local", inputs: {}, run: [process.execPath, script], when: [{ ref: "$run.input.mode", equals: "fast" }] } },
  };
  const app2 = await fixture(root2, runWf);
  const started2 = await app2.scheduler.start("runinputrun", { mode: "fast" });
  const view2 = await waitFor(app2.scheduler, started2.workflowRunId, "completed");
  assert.equal(view2.tasks.t!.state, "completed");
  assert.equal(view2.status, "completed");
  app2.store.close();
});


// Rerun validation: validateWorkflow accepts rerun { from, max, atMax? } on a source that `from` is
// upstream of, by any number of sources, and rejects every malformed variant. Profiles are passed in
// directly so each case pins a specific failure mode.
test("validateWorkflow accepts rerun from an upstream task and rejects malformed variants", () => {
  const profiles: Record<string, ExecutorProfile> = {
    local: { type: "process", cwd: "/tmp", env: {} },
  };

  // Accept: rerun from a direct dependency, with a when guard; rerun fields flow through onto the TaskDef.
  const ok1 = validate({
    id: "rerun-ok1",
    tasks: {
      verify: { executor: "local", inputs: {}, run: ["true"] },
      fixup: {
        executor: "local", dependsOn: ["verify"], inputs: {}, run: ["true"],
        when: [{ ref: "$tasks.verify.output.passed", equals: false }],
        rerun: { from: "verify", max: 3 },
      },
    },
  }, profiles);
  assert.equal(ok1.tasks.fixup!.rerun!.from, "verify");
  assert.equal(ok1.tasks.fixup!.rerun!.max, 3);
  assert.equal(ok1.tasks.fixup!.rerun!.atMax, undefined);

  // Accept: rerun from a task further upstream, with atMax: proceed.
  const ok2 = validate({
    id: "rerun-ok2",
    tasks: {
      verify: { executor: "local", inputs: {}, run: ["true"] },
      middle: { executor: "local", dependsOn: ["verify"], inputs: {}, run: ["true"] },
      again: { executor: "local", dependsOn: ["middle"], inputs: {}, run: ["true"], rerun: { from: "verify", max: 2, atMax: "proceed" } },
    },
  }, profiles);
  assert.equal(ok2.tasks.again!.rerun!.atMax, "proceed");

  // Accept: a rerun condition on the source's own output.
  const okWhen = validate({
    id: "rerun-ok-when",
    tasks: {
      verify: { executor: "local", inputs: {}, run: ["true"] },
      changed: { executor: "local", dependsOn: ["verify"], inputs: {}, run: ["true"], rerun: { from: "verify", max: 2, when: [{ ref: "$tasks.changed.output.changed", equals: true }] } },
    },
  }, profiles);
  assert.deepEqual(okWhen.tasks.changed!.rerun!.when, [{ ref: "$tasks.changed.output.changed", equals: true }]);

  // Reject: rerun.when reads anything but the source's own output.
  for (const ref of ["$tasks.verify.output.passed", "$run.input.x"]) {
    assert.throws(
      () => validate({
        id: "x",
        tasks: {
          verify: { executor: "local", inputs: {}, run: ["true"] },
          changed: { executor: "local", dependsOn: ["verify"], inputs: {}, run: ["true"], rerun: { from: "verify", max: 2, when: [{ ref, equals: true }] } },
        },
      }, profiles),
      (error: unknown) => error instanceof DagmarError && error.code === "invalid_rerun" && /its own output/.test(error.message),
    );
  }

  // Accept: several sources rerun from the same task.
  const ok3 = validate({
    id: "rerun-ok3",
    tasks: {
      verify: { executor: "local", inputs: {}, run: ["true"] },
      fixup1: { executor: "local", dependsOn: ["verify"], inputs: {}, run: ["true"], rerun: { from: "verify", max: 3 } },
      fixup2: { executor: "local", dependsOn: ["verify"], inputs: {}, run: ["true"], rerun: { from: "verify", max: 2 } },
    },
  }, profiles);
  assert.equal(ok3.tasks.fixup2!.rerun!.from, "verify");

  // Reject: rerun.from refers to an unknown task.
  assert.throws(
    () => validate({
      id: "x",
      tasks: {
        verify: { executor: "local", inputs: {}, run: ["true"] },
        fixup: { executor: "local", dependsOn: ["verify"], inputs: {}, run: ["true"], rerun: { from: "ghost", max: 3 } },
      },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_rerun" && /not a task/.test(error.message),
  );

  // Reject: rerun.from is not upstream of the source (here, downstream of it).
  assert.throws(
    () => validate({
      id: "x",
      tasks: {
        fixup: { executor: "local", inputs: {}, run: ["true"], rerun: { from: "verify", max: 3 } },
        verify: { executor: "local", dependsOn: ["fixup"], inputs: {}, run: ["true"] },
      },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_rerun" && /must be upstream/.test(error.message),
  );

  // Reject: rerun.from === id.
  assert.throws(
    () => validate({
      id: "x",
      tasks: {
        step: { executor: "local", inputs: {}, run: ["true"], rerun: { from: "step", max: 2 } },
      },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_rerun" && /must be upstream/.test(error.message),
  );

  // Reject: max < 1, an unknown atMax and an empty rerun.when (the schema rejects them with workflow_invalid).
  for (const rerun of [{ from: "verify", max: 0 }, { from: "verify", max: 1, atMax: "retry" }, { from: "verify", max: 1, when: [] }]) {
    assert.throws(
      () => validate({
        id: "x",
        tasks: {
          verify: { executor: "local", inputs: {}, run: ["true"] },
          fixup: { executor: "local", dependsOn: ["verify"], inputs: {}, run: ["true"], rerun },
        },
      }, profiles),
      (error: unknown) => error instanceof DagmarError && error.code === "workflow_invalid",
    );
  }

  // Regression: a raw dependsOn cycle is still rejected as dependency_cycle.
  assert.throws(
    () => validate({
      id: "x",
      tasks: {
        a: { executor: "local", inputs: {}, run: ["true"], dependsOn: ["b"] },
        b: { executor: "local", inputs: {}, run: ["true"], dependsOn: ["a"] },
      },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "dependency_cycle",
  );
});

// Rerun then pass: verify/fixup/review workflow where verify.mjs fails for the first K-1
// invocations (read from a fixture-root file counter) and passes on the Kth. fixup is the source
// (when:[passed==false], rerun:{from:verify,max:3}); review depends on verify and gates on
// passed==true, so it is decided (skipped) after each failing verify and runs after the passing one.
test("scheduler reruns from verify until the source's guard fails, deciding review each time", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-rerun-pass-"));
  const counter = join(root, "counter");
  const verifyScript = join(root, "verify.mjs");
  const echoScript = join(root, "echo.mjs");
  await writeFile(
    verifyScript,
    "import {existsSync, readFileSync, writeFileSync} from 'node:fs';\nfor await (const _ of process.stdin) {}\nconst counterFile = " + JSON.stringify(counter) + ";\nconst threshold = 3;\nconst n = existsSync(counterFile) ? parseInt(readFileSync(counterFile, 'utf8'), 10) : 0;\nconst next = n + 1;\nwriteFileSync(counterFile, String(next));\nconst passed = next >= threshold;\nconsole.log(JSON.stringify({outcome:'completed',message:'v',output:{passed, count: next}}));\n",
  );
  await writeFile(echoScript, "for await (const _ of process.stdin) {} console.log(JSON.stringify({outcome:'completed',message:'ok',output:{}}));");
  const workflow = {
    id: "rerunpass",
    tasks: {
      verify: { executor: "local", inputs: {}, run: [process.execPath, verifyScript] },
      fixup: {
        executor: "local", dependsOn: ["verify"], inputs: {},
        run: [process.execPath, echoScript],
        when: [{ ref: "$tasks.verify.output.passed", equals: false }],
        rerun: { from: "verify", max: 3 },
      },
      review: {
        executor: "local", dependsOn: ["verify"], inputs: {},
        run: [process.execPath, echoScript],
        when: [{ ref: "$tasks.verify.output.passed", equals: true }],
      },
    },
  };
  const app = await fixture(root, workflow);
  const started = await app.scheduler.start("rerunpass", {});
  const view = await waitFor(app.scheduler, started.workflowRunId, "completed");
  assert.equal(view.status, "completed");
  assert.equal(view.tasks.verify!.attempts.length, 3, "verify runs until the Kth pass; here K=3");
  assert.ok(view.tasks.verify!.attempts.every((a) => a.status === "completed"));
  const fixupCompleted = view.tasks.fixup!.attempts.filter((a) => a.status === "completed").length;
  const fixupSkipped = view.tasks.fixup!.attempts.filter((a) => a.status === "skipped").length;
  assert.equal(fixupCompleted, 2, "fixup completes exactly twice (after verify#1 and verify#2)");
  assert.equal(fixupSkipped, 1, "fixup skips on the final iteration when verify passes");
  assert.equal(view.tasks.review!.state, "completed");
  assert.deepEqual(view.tasks.review!.attempts.map((a) => a.status), ["skipped", "skipped", "completed"]);
  app.store.close();
});

// Rerun max: verify always fails, so fixup completes `max` (3) times and verify runs 4 times. The
// fourth time fixup would run again: by default it blocks the run with rerun_exhausted; with
// `atMax: proceed` it is skipped and the run completes with the exit branch skipped.
for (const atMax of [undefined, "proceed"] as const) test(`a rerun source at its max ${atMax ? "is skipped with atMax: proceed" : "blocks the run"}`, async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-rerun-max-"));
  const verifyScript = join(root, "verify.mjs");
  const echoScript = join(root, "echo.mjs");
  await writeFile(verifyScript, "for await (const _ of process.stdin) {} console.log(JSON.stringify({outcome:'completed',message:'v',output:{passed:false, count: 1}}));");
  await writeFile(echoScript, "for await (const _ of process.stdin) {} console.log(JSON.stringify({outcome:'completed',message:'ok',output:{}}));");
  const workflow = {
    id: "rerunmax",
    tasks: {
      verify: { executor: "local", inputs: {}, run: [process.execPath, verifyScript] },
      fixup: {
        executor: "local", dependsOn: ["verify"], inputs: {},
        run: [process.execPath, echoScript],
        when: [{ ref: "$tasks.verify.output.passed", equals: false }],
        rerun: { from: "verify", max: 3, ...(atMax ? { atMax } : {}) },
      },
      review: {
        executor: "local", dependsOn: ["verify"], inputs: {},
        run: [process.execPath, echoScript],
        when: [{ ref: "$tasks.verify.output.passed", equals: true }],
      },
    },
  };
  const app = await fixture(root, workflow);
  const started = await app.scheduler.start("rerunmax", {});
  const view = await waitFor(app.scheduler, started.workflowRunId, atMax ? "completed" : "blocked");
  assert.equal(view.tasks.verify!.attempts.length, 4, "verify runs max+1 times (1 initial + 3 reruns)");
  assert.deepEqual(view.tasks.fixup!.attempts.map((a) => a.status), ["completed", "completed", "completed", atMax ? "skipped" : "blocked"]);
  assert.equal(view.tasks.fixup!.attempts[3]!.error?.code, atMax ? undefined : "rerun_exhausted");
  assert.deepEqual(view.tasks.review!.attempts.map((a) => a.status), ["skipped", "skipped", "skipped", "skipped"]);
  app.store.close();
});

// Immediate pass: verify passes on the first invocation, so fixup's guard fails, nothing reruns and
// review is decided once and runs.
test("nothing reruns when verify passes immediately", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-rerun-immediate-"));
  const verifyScript = join(root, "verify.mjs");
  const echoScript = join(root, "echo.mjs");
  await writeFile(verifyScript, "for await (const _ of process.stdin) {} console.log(JSON.stringify({outcome:'completed',message:'v',output:{passed:true, count: 1}}));");
  await writeFile(echoScript, "for await (const _ of process.stdin) {} console.log(JSON.stringify({outcome:'completed',message:'ok',output:{}}));");
  const workflow = {
    id: "rerunimmediate",
    tasks: {
      verify: { executor: "local", inputs: {}, run: [process.execPath, verifyScript] },
      fixup: {
        executor: "local", dependsOn: ["verify"], inputs: {},
        run: [process.execPath, echoScript],
        when: [{ ref: "$tasks.verify.output.passed", equals: false }],
        rerun: { from: "verify", max: 3 },
      },
      review: {
        executor: "local", dependsOn: ["verify"], inputs: {},
        run: [process.execPath, echoScript],
        when: [{ ref: "$tasks.verify.output.passed", equals: true }],
      },
    },
  };
  const app = await fixture(root, workflow);
  const started = await app.scheduler.start("rerunimmediate", {});
  const view = await waitFor(app.scheduler, started.workflowRunId, "completed");
  assert.equal(view.status, "completed");
  assert.equal(view.tasks.verify!.attempts.length, 1);
  assert.equal(view.tasks.fixup!.state, "skipped");
  assert.equal(view.tasks.fixup!.attempts.length, 1);
  assert.equal(view.tasks.fixup!.attempts[0]!.status, "skipped");
  assert.equal(view.tasks.review!.state, "completed");
  assert.equal(view.tasks.review!.attempts.length, 1);
  app.store.close();
});

// Restart mid-rerun: a rerun is in flight when the daemon is killed; the next scheduler
// recovers the executor_lost attempt, the operator resumes, and the reruns continue with the
// completed count preserved (resume continues it, never resets it). Pre-inserting the running
// attempt mirrors the recovery test at lines 211-217 and the gateshutdown restart test at
// lines 977-1013 ��� deterministic, no real daemon kill, same store path. verify is the rerun's
// from task; pre-inserting it `running` makes recover() fail it with `executor_lost`, which
// cascades blocked_by_dependency through fixup and review, so the run lands `blocked` and
// resume() can pick it up.
test("restart mid-rerun: recover() blocks, resume() continues, completed count preserved", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-rerun-restart-"));
  const counter = join(root, "counter");
  const verifyScript = join(root, "verify.mjs");
  const echoScript = join(root, "echo.mjs");
  await writeFile(
    verifyScript,
    "import {existsSync, readFileSync, writeFileSync} from 'node:fs';\nfor await (const _ of process.stdin) {}\nconst counterFile = " + JSON.stringify(counter) + ";\nconst n = existsSync(counterFile) ? parseInt(readFileSync(counterFile, 'utf8'), 10) : 0;\nconst next = n + 1;\nwriteFileSync(counterFile, String(next));\nconst passed = next >= 4;\nconsole.log(JSON.stringify({outcome:'completed',message:'v',output:{passed, count: next}}));\n",
  );
  await writeFile(echoScript, "for await (const _ of process.stdin) {} console.log(JSON.stringify({outcome:'completed',message:'ok',output:{}}));");
  const workflow = {
    id: "rerunrestart",
    executors: defaultExecutors(root),
    tasks: {
      verify: { executor: "local", inputs: {}, run: [process.execPath, verifyScript] },
      fixup: {
        executor: "local", dependsOn: ["verify"], inputs: {},
        run: [process.execPath, echoScript],
        when: [{ ref: "$tasks.verify.output.passed", equals: false }],
        rerun: { from: "verify", max: 3 },
      },
      review: {
        executor: "local", dependsOn: ["verify"], inputs: {},
        run: [process.execPath, echoScript],
        when: [{ ref: "$tasks.verify.output.passed", equals: true }],
      },
    },
  };
  const storePath = join(root, "state", "dagmar.sqlite");
  await mkdir(join(root, "workflows"), { recursive: true });
  await mkdir(join(root, "state"), { recursive: true });
  await writeFile(join(root, "workflows", `${workflow.id}.yaml`), stringify(workflow));
  // Pre-write the counter so verify#2 (the first verify run by scheduler2) reads count=1.
  await writeFile(counter, "1");

  // Pre-insert a mid-rerun state: verify#1 was running on a scheduler that has since died.
  const now = new Date().toISOString();
  const store1 = new Store(storePath);
  store1.insertRun({ id: "wr_rerun_r", workflowId: workflow.id, cwd: null, input: {}, status: "running", startedAt: now, updatedAt: now, endedAt: null });
  store1.insertAttempt({ id: "tr_v1", workflowRunId: "wr_rerun_r", taskId: "verify", attempt: 1, executorProfile: "local", executorType: "process", status: "running", result: null, error: null, acpSessionId: null, startedAt: now, updatedAt: now, endedAt: null, item: null, roundId: null });
  store1.close();

  // Second scheduler on the same store — recovers the in-flight attempt then resumes.
  const store2 = new Store(storePath);
  const transcripts2 = new Transcripts(join(root, "state"));
  const events2 = new EventBus();
  const scheduler2 = new Scheduler(store2, transcripts2, new WorkflowRepository(join(root, "workflows")), events2, { process: new ProcessExecutor(), acp: new InteractiveExecutor() });
  await scheduler2.recover();
  // After recover: in-flight attempt is executor_lost and the run is blocked (the failed
  // task cascades blocked_by_dependency through fixup and review).
  assert.equal(store2.run("wr_rerun_r")!.status, "blocked");
  assert.equal(store2.attempt("tr_v1")!.status, "failed");
  assert.equal(store2.attempt("tr_v1")!.error!.code, "executor_lost");

  // Resume retries the failed attempt; the reruns continue and verify eventually passes.
  await scheduler2.resume("wr_rerun_r");
  const view = await waitFor(scheduler2, "wr_rerun_r", "completed");
  assert.equal(view.status, "completed");

  // Completed count preserved across the restart: only `completed` counts toward max.
  const fixupCompleted = view.tasks.fixup!.attempts.filter((a) => a.status === "completed").length;
  assert.ok(fixupCompleted <= 3, `fixup completed count (${fixupCompleted}) must be <= max (3)`);

  // verify ran 3 successful times on scheduler2; verify#1 was the pre-inserted failed attempt.
  const verifyCompleted = view.tasks.verify!.attempts.filter((a) => a.status === "completed");
  assert.equal(verifyCompleted.length, 3, "verify ran three times on scheduler2 (2 fail, 1 pass)");
  const lastVerify = verifyCompleted[verifyCompleted.length - 1]!;
  assert.deepEqual(lastVerify.result!.output, { passed: true, count: 4 });

  // review runs after the passing verify.
  assert.equal(view.tasks.review!.state, "completed");
  store2.close();
});


// Several reruns from one task (a review workflow's shape): fixup reruns from verify while it fails;
// `changed` reruns from verify when it reports that the reviewers changed something (its rerun.when);
// revise reruns from verify after the gate. Each rerun decides verify and everything after it again. `verify` reads its pass/fail
// answers from a list, one per invocation (`crash` makes that invocation exit without a result);
// `changed` reads its answers from `changes` the same way (false once the list runs out).
async function sdlcFixture(prefix: string, answers: Array<boolean | "crash">, { reviseMax = 3, changes = [] as boolean[], fixAtMax = "proceed" } = {}) {
  const root = await mkdtemp(join(tmpdir(), prefix));
  const answerScript = async (name: string, list: Array<boolean | "crash">, key: string, missing: string) => {
    const counter = join(root, `${name}.count`), script = join(root, `${name}.mjs`);
    await writeFile(script, `import {existsSync, readFileSync, writeFileSync} from 'node:fs';
for await (const _ of process.stdin) {}
const n = existsSync(${JSON.stringify(counter)}) ? Number(readFileSync(${JSON.stringify(counter)}, 'utf8')) : 0;
writeFileSync(${JSON.stringify(counter)}, String(n + 1));
const answer = ${JSON.stringify(list)}[n] ?? ${missing};
if (answer === 'crash') process.exit(1);
console.log(JSON.stringify({outcome:'completed',message:'${name}',output:{${key}: answer, run: n + 1}}));`);
    return [process.execPath, script];
  };
  const echoScript = join(root, "echo.mjs");
  await writeFile(echoScript, "for await (const _ of process.stdin) {} console.log(JSON.stringify({outcome:'completed',message:'ok',output:{}}));");
  const echo = [process.execPath, echoScript], passed = { ref: "$tasks.verify.output.passed", equals: true }, failed = { ref: "$tasks.verify.output.passed", equals: false };
  const workflow = {
    id: "sdlc",
    tasks: {
      implement: { executor: "local", inputs: {}, run: echo },
      verify: { executor: "local", dependsOn: ["implement"], inputs: {}, run: await answerScript("verify", answers, "passed", "'crash'") },
      fixup: { executor: "local", dependsOn: ["verify"], inputs: {}, run: echo, when: [failed], rerun: { from: "verify", max: 2 } },
      publish: { executor: "local", dependsOn: ["verify"], inputs: {}, run: echo, when: [passed] },
      review: { executor: "local", dependsOn: ["publish", "verify"], inputs: {}, run: echo, when: [passed] },
      qa: { executor: "local", dependsOn: ["publish", "verify"], inputs: {}, run: echo, when: [passed] },
      changed: { executor: "local", dependsOn: ["review", "qa", "verify"], inputs: {}, run: await answerScript("changed", changes, "changed", "false"), when: [passed], rerun: { from: "verify", max: 2, atMax: fixAtMax, when: [{ ref: "$tasks.changed.output.changed", equals: true }] } },
      gate: { dependsOn: ["changed", "verify"], inputs: {}, when: [passed], gate: { prompt: "approve or revise?", schema: { type: "object", required: ["decision"], properties: { decision: { enum: ["approve", "revise"] } } } } },
      ready: { executor: "local", dependsOn: ["gate"], inputs: {}, run: echo, when: [{ ref: "$tasks.gate.output.decision", equals: "approve" }] },
      revise: { executor: "local", dependsOn: ["gate"], inputs: {}, run: echo, when: [{ ref: "$tasks.gate.output.decision", equals: "revise" }], rerun: { from: "verify", max: reviseMax } },
    },
  };
  return fixture(root, workflow);
}
// Answers the next gate of the run that has not been answered yet.
async function answerNextGate(app: Awaited<ReturnType<typeof fixture>>, runId: string, decision: string, answered: Set<string>): Promise<void> {
  for (let i = 0; i < 500; i++) {
    const gate = app.scheduler.interactions().find((x) => x.kind === "gate" && x.workflowRunId === runId && !answered.has(x.taskRunId));
    if (gate) { answered.add(gate.taskRunId); await app.scheduler.answer(gate.id, { decision }); return; }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`No new gate: ${JSON.stringify({ status: (await app.scheduler.get(runId)).status, tasks: Object.fromEntries(Object.entries((await app.scheduler.get(runId)).tasks).map(([id, x]) => [id, `${x.state} [${x.attempts.map((a) => a.status).join(",")}]`])) })}`);
}
const states = (view: RunView) => Object.fromEntries(Object.entries(view.tasks).map(([id, x]) => [id, x.attempts.map((a) => a.status).join(",")]));

test("a revise rerun decides verify, the reviews and the gate again, then approve completes the run", async () => {
  const app = await sdlcFixture("dagmar-sdlc-revise-", [false, true, true]);
  const run = await app.scheduler.start("sdlc", {});
  const answered = new Set<string>();
  await answerNextGate(app, run.workflowRunId, "revise", answered);
  await answerNextGate(app, run.workflowRunId, "approve", answered);
  const view = await waitFor(app.scheduler, run.workflowRunId, "completed");
  assert.deepEqual(states(view), {
    implement: "completed",
    verify: "completed,completed,completed",
    fixup: "completed,skipped,skipped",
    publish: "skipped,completed,completed",
    review: "skipped,completed,completed",
    qa: "skipped,completed,completed",
    changed: "skipped,completed,completed",
    gate: "skipped,completed,completed",
    ready: "skipped,skipped,completed",
    revise: "skipped,completed,skipped",
  });
  app.store.close();
});

// The dagmar#22 deadlock: with a loop's exit branch held until the loop was final, a fixup inside a
// revise round waited for the revise loop to end. Reruns hold nothing, so fixup runs at once.
test("verify failing inside a revise round goes through fixup and back to the gate", async () => {
  const app = await sdlcFixture("dagmar-sdlc-inner-fix-", [true, false, true]);
  const run = await app.scheduler.start("sdlc", {});
  const answered = new Set<string>();
  await answerNextGate(app, run.workflowRunId, "revise", answered);
  await answerNextGate(app, run.workflowRunId, "approve", answered);
  const view = await waitFor(app.scheduler, run.workflowRunId, "completed");
  assert.deepEqual(states(view), {
    implement: "completed",
    verify: "completed,completed,completed",
    fixup: "skipped,completed,skipped",
    publish: "completed,skipped,completed",
    review: "completed,skipped,completed",
    qa: "completed,skipped,completed",
    changed: "completed,skipped,completed",
    gate: "completed,skipped,completed",
    ready: "skipped,skipped,completed",
    revise: "completed,skipped,skipped",
  });
  app.store.close();
});

test("fixup at its max inside a revise round blocks the run", async () => {
  const app = await sdlcFixture("dagmar-sdlc-inner-max-", [true, false, false, false]);
  const run = await app.scheduler.start("sdlc", {});
  await answerNextGate(app, run.workflowRunId, "revise", new Set());
  const view = await waitFor(app.scheduler, run.workflowRunId, "blocked");
  assert.deepEqual(states(view), {
    implement: "completed",
    verify: "completed,completed,completed,completed",
    fixup: "skipped,completed,completed,blocked",
    publish: "completed,skipped,skipped,skipped",
    review: "completed,skipped,skipped,skipped",
    qa: "completed,skipped,skipped,skipped",
    changed: "completed,skipped,skipped,skipped",
    gate: "completed,skipped,skipped,skipped",
    ready: "skipped,skipped,skipped,skipped",
    revise: "completed,skipped,skipped,skipped",
  });
  assert.equal(view.tasks.fixup!.attempts[3]!.error!.code, "rerun_exhausted");
  app.store.close();
});

test("revise at its max blocks the run, a resume blocks it again at once, and so does a resume after cancel", async (t) => {
  const app = await sdlcFixture("dagmar-sdlc-revise-max-", [true, true, true], { reviseMax: 2 });
  const run = await app.scheduler.start("sdlc", {});
  const answered = new Set<string>();
  for (let i = 0; i < 3; i++) await answerNextGate(app, run.workflowRunId, "revise", answered);
  const view = await waitFor(app.scheduler, run.workflowRunId, "blocked");
  assert.deepEqual(view.tasks.revise!.attempts.map((a) => a.status), ["completed", "completed", "blocked"]);
  assert.equal(view.tasks.revise!.attempts[2]!.error!.code, "rerun_exhausted");
  assert.equal(view.tasks.gate!.attempts.length, 3, "one gate per round: the first, plus one after each revise");
  assert.equal(view.tasks.ready!.state, "skipped");
  const publish = t.mock.method(app.events, "publish");
  const resumed = await app.scheduler.resume(run.workflowRunId);
  assert.deepEqual(resumed.tasks.revise!.attempts.map((a) => a.status), ["completed", "completed", "blocked", "blocked"]);
  assert.ok(publish.mock.calls.some(({ arguments: [event] }) => event.type === "workflow.status_changed" && event.data.status === "blocked"), "clients learn that the resume blocked again");
  const cancelled = await app.scheduler.cancelRun(run.workflowRunId);
  assert.equal(cancelled.status, "cancelled");
  assert.deepEqual(publish.mock.calls.at(-1)!.arguments[0].data, { status: "cancelled" });
  const again = await app.scheduler.resume(run.workflowRunId);
  assert.equal(again.status, "blocked");
  assert.deepEqual(again.tasks.revise!.attempts.map((a) => a.status), ["completed", "completed", "blocked", "blocked", "blocked"]);
  app.store.close();
});

test("changed reruns from verify while the reviewers changed something, and proceeds to the gate at its max", async () => {
  const app = await sdlcFixture("dagmar-sdlc-fixes-", [true, true, true], { changes: [true, true, true] });
  const run = await app.scheduler.start("sdlc", {});
  await answerNextGate(app, run.workflowRunId, "approve", new Set());
  const view = await waitFor(app.scheduler, run.workflowRunId, "completed");
  assert.deepEqual(states(view), {
    implement: "completed",
    verify: "completed,completed,completed",
    fixup: "skipped,skipped,skipped",
    publish: "completed,completed,completed",
    review: "completed,completed,completed",
    qa: "completed,completed,completed",
    changed: "completed,completed,completed",
    gate: "completed",
    ready: "completed",
    revise: "skipped",
  });
  app.store.close();
});

test("a rerun.when source that asks for a rerun beyond its max blocks the run by default", async () => {
  const app = await sdlcFixture("dagmar-sdlc-fixes-block-", [true, true, true], { changes: [true, true, true], fixAtMax: "block" });
  const run = await app.scheduler.start("sdlc", {});
  const view = await waitFor(app.scheduler, run.workflowRunId, "blocked");
  assert.deepEqual(view.tasks.changed!.attempts.map((a) => a.status), ["completed", "completed", "blocked"]);
  assert.equal(view.tasks.changed!.attempts[2]!.error!.code, "rerun_exhausted");
  assert.deepEqual(view.tasks.changed!.attempts[2]!.result!.output, { changed: true, run: 3 }, "the blocked attempt keeps its output");
  assert.equal(view.tasks.verify!.attempts.length, 3, "no rerun beyond the max");
  assert.equal(view.tasks.gate!.state, "blocked_by_dependency");
  app.store.close();
});

test("resume after a failure in a revise round keeps the completed counts", async () => {
  const app = await sdlcFixture("dagmar-sdlc-resume-", [true, "crash", true, true], { reviseMax: 2 });
  const run = await app.scheduler.start("sdlc", {});
  const answered = new Set<string>();
  await answerNextGate(app, run.workflowRunId, "revise", answered);
  const blocked = await waitFor(app.scheduler, run.workflowRunId, "blocked");
  assert.equal(blocked.tasks.verify!.state, "failed");
  await app.scheduler.resume(run.workflowRunId);
  await answerNextGate(app, run.workflowRunId, "revise", answered);
  await answerNextGate(app, run.workflowRunId, "approve", answered);
  const view = await waitFor(app.scheduler, run.workflowRunId, "completed");
  assert.equal(view.tasks.revise!.attempts.filter((a) => a.status === "completed").length, 2, "the revise before the failure still counts");
  assert.equal(view.tasks.gate!.attempts.length, 3);
  assert.equal(view.tasks.ready!.state, "completed");
  app.store.close();
});

// A process task that reports the directory it runs in.
async function cwdScript(root: string): Promise<string> {
  const script = join(root, "cwd.mjs");
  await writeFile(script, `for await(const _ of process.stdin){};console.log(JSON.stringify({outcome:'completed',message:'cwd',output:{cwd:process.cwd()}}));`);
  return script;
}

test("a run's cwd is where its executors without a cwd run; an executor's own cwd wins", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "dagmar-cwd-"))), project = join(root, "project"), fixed = join(root, "fixed");
  await mkdir(project); await mkdir(fixed);
  const script = await cwdScript(root);
  const app = await fixture(root, {
    id: "cwd",
    executors: { project: { type: "process" }, fixed: { type: "process", cwd: fixed } },
    tasks: { a: { executor: "project", inputs: {}, run: [process.execPath, script] }, b: { executor: "fixed", inputs: {}, run: [process.execPath, script] } },
  });
  const started = await app.scheduler.start("cwd", {}, project);
  const view = await waitFor(app.scheduler, started.workflowRunId, "completed");
  assert.equal(view.cwd, project);
  assert.deepEqual(view.tasks.a!.attempts[0]!.result!.output, { cwd: project });
  assert.deepEqual(view.tasks.b!.attempts[0]!.result!.output, { cwd: fixed });
  assert.equal(app.scheduler.list()[0]!.cwd, project);
  app.store.close();
});

test("run.start checks the run's cwd against the workflow's executors", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-cwd-check-"));
  const app = await fixture(root, { id: "needs", executors: { project: { type: "process" } }, tasks: { t: { executor: "project", inputs: {}, run: ["node"] } } });
  await writeFile(join(root, "workflows", "own.yaml"), stringify({ id: "own", executors: defaultExecutors(root), tasks: { t: { executor: "local", inputs: {}, run: ["node"] } } }));
  const rejects = (cwd: string | null, workflowId: string, code: string) => assert.rejects(app.scheduler.start(workflowId, {}, cwd), (error) => error instanceof DagmarError && error.code === code);
  await rejects(null, "needs", "cwd_required");
  await rejects("relative", "needs", "invalid_params");
  await rejects(join(root, "missing"), "needs", "invalid_params");
  await rejects(join(root, "workflows", "own.yaml"), "needs", "invalid_params"); // a file, not a directory
  await rejects(root, "own", "cwd_unused");
  assert.equal(app.scheduler.list().length, 0);
  app.store.close();
});

test("one workflow runs on two cwds at once, but once per cwd", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-cwd-active-")), one = join(root, "one"), two = join(root, "two"), wait = join(root, "wait.mjs");
  await mkdir(one); await mkdir(two);
  await writeFile(wait, `for await(const _ of process.stdin){};setInterval(()=>{},1000);`);
  const app = await fixture(root, { id: "long", executors: { project: { type: "process" } }, tasks: { t: { executor: "project", inputs: {}, run: [process.execPath, wait] } } });
  try {
    await app.scheduler.start("long", {}, one);
    await app.scheduler.start("long", {}, two);
    await assert.rejects(app.scheduler.start("long", {}, one), (error) => error instanceof DagmarError && error.code === "active_run_exists" && error.message.includes(one));
  } finally { await app.scheduler.shutdown(); app.store.close(); }
});

test("a database from before run cwds is upgraded in place", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-upgrade-")), path = join(root, "dagmar.sqlite");
  const old = new DatabaseSync(path);
  old.exec(`CREATE TABLE workflow_runs (
 id TEXT PRIMARY KEY, workflow_id TEXT NOT NULL, input_json TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('running','waiting','completed','blocked','cancelled')),
 started_at TEXT NOT NULL, updated_at TEXT NOT NULL, ended_at TEXT
);
CREATE UNIQUE INDEX one_active_run_per_workflow ON workflow_runs(workflow_id) WHERE status IN ('running','waiting');
INSERT INTO workflow_runs VALUES('wr_old','w','{"task":"t"}','completed','2026-10-07T00:00:00.000Z','2026-10-07T00:00:00.000Z','2026-10-07T00:00:00.000Z');`);
  old.close();
  const store = new Store(path);
  try {
    assert.deepEqual(store.run("wr_old"), { id: "wr_old", workflowId: "w", cwd: null, input: { task: "t" }, status: "completed", startedAt: "2026-10-07T00:00:00.000Z", updatedAt: "2026-10-07T00:00:00.000Z", endedAt: "2026-10-07T00:00:00.000Z" });
    const now = new Date().toISOString(), active = (id: string, cwd: string | null) => store.insertRun({ id, workflowId: "w", cwd, input: {}, status: "running", startedAt: now, updatedAt: now, endedAt: null });
    active("wr_a", "/a"); active("wr_b", "/b"); active("wr_none", null);
    assert.throws(() => active("wr_a2", "/a"), (error) => error instanceof DagmarError && error.code === "active_run_exists");
    assert.throws(() => active("wr_none2", null), (error) => error instanceof DagmarError && error.code === "active_run_exists");
  } finally { store.close(); }
  const reopened = new Store(path); // the upgrade is idempotent
  assert.equal(reopened.runs().length, 4);
  reopened.close();
});

test("a database from before map tasks is upgraded in place", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-upgrade-map-")), path = join(root, "dagmar.sqlite"), at = "2026-10-15T00:00:00.000Z";
  const old = new DatabaseSync(path);
  old.exec(`CREATE TABLE workflow_runs (
 id TEXT PRIMARY KEY, workflow_id TEXT NOT NULL, input_json TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('running','waiting','completed','blocked','cancelled')),
 started_at TEXT NOT NULL, updated_at TEXT NOT NULL, ended_at TEXT, cwd TEXT
);
CREATE TABLE task_runs (
 id TEXT PRIMARY KEY, workflow_run_id TEXT NOT NULL REFERENCES workflow_runs(id), task_id TEXT NOT NULL,
 attempt INTEGER NOT NULL CHECK(attempt>=1), executor_profile TEXT NOT NULL,
 executor_type TEXT NOT NULL CHECK(executor_type IN ('acp','process','gate')),
 status TEXT NOT NULL CHECK(status IN ('running','awaiting_permission','awaiting_input','completed','blocked','failed','cancelled','skipped')),
 result_json TEXT, error_json TEXT, acp_session_id TEXT, started_at TEXT NOT NULL, updated_at TEXT NOT NULL, ended_at TEXT,
 UNIQUE(workflow_run_id,task_id,attempt)
);
INSERT INTO workflow_runs VALUES('wr_old','w','{}','completed','${at}','${at}','${at}',NULL);
INSERT INTO task_runs VALUES('tr_old','wr_old','t',1,'local','process','completed',NULL,NULL,NULL,'${at}','${at}','${at}');`);
  old.close();
  const store = new Store(path);
  try {
    assert.deepEqual([store.attempt("tr_old")!.item, store.attempt("tr_old")!.roundId], [null, null]);
    store.insertAttempt({ id: "tr_item", workflowRunId: "wr_old", taskId: "t", attempt: 2, executorProfile: "local", executorType: "process", status: "completed", result: null, error: null, acpSessionId: null, startedAt: at, updatedAt: at, endedAt: at, item: "a", roundId: "tr_old" });
    assert.deepEqual([store.attempt("tr_item")!.item, store.attempt("tr_item")!.roundId], ["a", "tr_old"]);
  } finally { store.close(); }
  const reopened = new Store(path); // the upgrade is idempotent
  assert.equal(reopened.attempts("wr_old").length, 2);
  reopened.close();
});

test("a run's cwd survives a daemon restart", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "dagmar-cwd-restart-"))), project = join(root, "project"), storePath = join(root, "state", "dagmar.sqlite");
  await mkdir(project); await mkdir(join(root, "workflows")); await mkdir(join(root, "state"));
  const script = await cwdScript(root);
  await writeFile(join(root, "workflows", "later.yaml"), stringify({
    id: "later",
    executors: { project: { type: "process" } },
    tasks: { gate: { inputs: {}, gate: { prompt: "Go?" } }, after: { executor: "project", dependsOn: ["gate"], inputs: {}, run: [process.execPath, script] } },
  }));
  const scheduler = (store: Store) => new Scheduler(store, new Transcripts(join(root, "state")), new WorkflowRepository(join(root, "workflows")), new EventBus(), { process: new ProcessExecutor(), acp: new InteractiveExecutor() });
  const store1 = new Store(storePath), scheduler1 = scheduler(store1);
  const started = await scheduler1.start("later", {}, project);
  await waitFor(scheduler1, started.workflowRunId, "waiting");
  await scheduler1.shutdown(); store1.close();
  const store2 = new Store(storePath), scheduler2 = scheduler(store2);
  await scheduler2.recover();
  await scheduler2.answer(scheduler2.interactions()[0]!.id, "go");
  const view = await waitFor(scheduler2, started.workflowRunId, "completed");
  assert.deepEqual(view.tasks.after!.attempts[0]!.result!.output, { cwd: project });
  store2.close();
});

test("the CLI sends --cwd as an absolute path, resolved in the caller's directory", async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), "dagmar-cli-cwd-"))), project = join(root, "project"), workflows = join(root, "workflows");
  await mkdir(project); await mkdir(workflows);
  const script = await cwdScript(root);
  await writeFile(join(workflows, "here.yaml"), stringify({ id: "here", executors: { project: { type: "process" } }, tasks: { t: { executor: "project", inputs: {}, run: [process.execPath, script] } } }));
  const port = await freePort(), config = join(root, "config.yaml");
  await writeFile(config, stringify({ workflowDir: workflows, storageDir: join(root, "state"), listen: { host: "127.0.0.1", port } }));
  const daemon = await startDaemon(config), client = new RpcClient(`ws://127.0.0.1:${port}`); await client.connect();
  try {
    const cli = spawn(process.execPath, ["--import", import.meta.resolve("tsx"), fileURLToPath(new URL("../src/cli.ts", import.meta.url)), "--config", config, "run", "here", "--cwd", "."], { cwd: project });
    let stdout = ""; cli.stdout.on("data", (chunk) => { stdout += chunk; });
    assert.equal(await new Promise((resolve) => cli.once("close", resolve)), 0);
    const { workflowRunId } = JSON.parse(stdout) as { workflowRunId: string };
    let view: RunView | undefined;
    for (let i = 0; i < 200; i++) { view = await client.request("run.get", { workflowRunId }) as unknown as RunView; if (view.status === "completed") break; await new Promise((resolve) => setTimeout(resolve, 10)); }
    assert.equal(view!.cwd, project);
    assert.deepEqual(view!.tasks.t!.attempts[0]!.result!.output, { cwd: project });
  } finally { client.close(); await daemon.stop(); }
});

test("an executor's envFile reaches its task between the daemon's env and its own env", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-envfile-")), script = join(root, "env.mjs"), file = join(root, "secrets.env");
  await writeFile(script, `for await(const _ of process.stdin){};const {FROM_FILE,OVERRIDDEN,HOME}=process.env;console.log(JSON.stringify({outcome:'completed',message:'env',output:{FROM_FILE,OVERRIDDEN,HOME}}));`);
  await writeFile(file, "# a comment\nFROM_FILE=file\nOVERRIDDEN=file\nHOME=/from-file\n");
  const app = await fixture(root, {
    id: "envfile",
    executors: { secret: { type: "process", cwd: root, envFile: file, env: { OVERRIDDEN: "literal" } }, broken: { type: "process", cwd: root, envFile: join(root, "missing.env") } },
    tasks: { ok: { executor: "secret", inputs: {}, run: [process.execPath, script] }, bad: { executor: "broken", inputs: {}, run: [process.execPath, script] } },
  });
  const started = await app.scheduler.start("envfile", {});
  const view = await waitFor(app.scheduler, started.workflowRunId, "blocked");
  assert.deepEqual(view.tasks.ok!.attempts[0]!.result!.output, { FROM_FILE: "file", OVERRIDDEN: "literal", HOME: "/from-file" });
  assert.equal(view.tasks.bad!.attempts[0]!.error!.code, "env_file_unreadable");
  assert.throws(() => validateWorkflow({ id: "x", executors: { e: { type: "process", envFile: "relative.env" } }, tasks: { t: { executor: "e", inputs: {}, run: ["node"] } } }), (error) => error instanceof DagmarError && error.code === "workflow_invalid");
  app.store.close();
});

test("validateWorkflow accepts map tasks and rejects malformed ones", () => {
  const profiles: Record<string, ExecutorProfile> = { agent: { type: "acp", cwd: "/tmp", env: {}, run: ["agent"] } };
  const list = { executor: "agent", inputs: {}, prompt: "list" };
  const map = { over: "$tasks.list.output.docs", key: "id", concurrency: 3 };
  const review = { executor: "agent", dependsOn: ["list"], map, inputs: { doc: "$item", title: "$item.title" }, prompt: "review" };
  const check = (tasks: object) => validate({ id: "m", tasks }, profiles);
  const rejects = (tasks: object, code: string) => assert.throws(() => check(tasks), (error) => error instanceof DagmarError && error.code === code, JSON.stringify(tasks));
  assert.deepEqual(check({ list, review }).tasks.review!.map, map);
  check({ review: { ...review, dependsOn: [], map: { ...map, over: "$run.input.docs" } } });
  for (const bad of [{ over: map.over, key: "id" }, { ...map, concurrency: 0 }, { ...map, key: "" }, { ...map, extra: 1 }]) rejects({ list, review: { ...review, map: bad } }, "workflow_invalid");
  rejects({ list, gate: { dependsOn: ["list"], inputs: {}, gate: { prompt: "ok?" }, map } }, "invalid_task");
  for (const over of ["docs", "$item", "$tasks.other.output"]) rejects({ list, other: list, review: { ...review, map: { ...map, over } } }, "invalid_map");
  rejects({ list, review: { ...review, rerun: { from: "list", max: 1 } } }, "invalid_map");
  rejects({ list, review: { ...review, session: { mode: "continue", from: "list" } } }, "invalid_map");
  rejects({ list, review, next: { executor: "agent", dependsOn: ["review"], inputs: {}, prompt: "next", session: { mode: "continue", from: "review" } } }, "invalid_map");
  rejects({ list: { ...list, inputs: { doc: "$item" } } }, "invalid_input_reference");
  rejects({ list, review: { ...review, when: [{ ref: "$item.title", equals: "x" }] } }, "invalid_guard");
});

test("a map task runs 36 items 3 at a time, keeps the others when one fails, and a resume reruns only that one", { timeout: 20_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-map-"));
  const ids = Array.from({ length: 36 }, (_, i) => `doc${String(i + 1).padStart(2, "0")}`), runs: Record<string, number> = {};
  let inflight = 0, most = 0, summary: Json = null;
  const stub = new StubAcpExecutor(async (request) => {
    if (request.taskId === "list") return { result: { outcome: "completed", message: "list", output: { docs: ids.map((id) => ({ id })) } } };
    if (request.taskId === "summary") { summary = request.inputs.reviews!; return { result: { outcome: "completed", message: "summary", output: {} } }; }
    const id = (request.inputs.doc as { id: string }).id;
    runs[id] = (runs[id] ?? 0) + 1; inflight++; most = Math.max(most, inflight);
    await new Promise((resolve) => setTimeout(resolve, 5)); inflight--;
    if (id === "doc17" && runs[id] === 1) return { error: { code: "boom", message: "boom" } };
    return { result: { outcome: "completed", message: id, output: { id, ok: true } } };
  });
  const app = await fixture(root, { id: "map", tasks: {
    list: { executor: "agent", inputs: {}, prompt: "list" },
    review: { executor: "agent", dependsOn: ["list"], map: { over: "$tasks.list.output.docs", key: "id", concurrency: 3 }, inputs: { doc: "$item" }, prompt: "review" },
    summary: { executor: "agent", dependsOn: ["review"], inputs: { reviews: "$tasks.review.output" }, prompt: "summary" },
  } }, stub);
  const seen: Array<{ type: string; data: Record<string, Json> }> = [];
  app.events.subscribe({ readyState: 1, send: (text: string) => seen.push(JSON.parse(text).params) } as never, {}).activate();
  const run = await app.scheduler.start("map", {});
  const blocked = await waitFor(app.scheduler, run.workflowRunId, "blocked");
  const items = (view: RunView) => view.tasks.review!.attempts.filter((a) => a.item !== null);
  assert.equal(most, 3);
  assert.equal(items(blocked).length, 36);
  assert.deepEqual(items(blocked).filter((a) => a.status !== "completed").map((a) => [a.item, a.status, a.error?.code]), [["doc17", "failed", "boom"]]);
  assert.deepEqual([blocked.tasks.review!.state, blocked.tasks.review!.attempts[0]!.error!.code], ["blocked", "map_items_incomplete"]);
  assert.deepEqual([blocked.tasks.summary!.state, blocked.tasks.summary!.attempts.length], ["blocked_by_dependency", 0]);
  assert.deepEqual(seen.filter((e) => e.type === "task.status_changed" && e.data.item === "doc17").map((e) => e.data.status), ["running", "failed"]);

  await app.scheduler.resume(run.workflowRunId);
  const done = await waitFor(app.scheduler, run.workflowRunId, "completed");
  assert.deepEqual([Object.keys(runs).length, Object.entries(runs).filter(([, n]) => n !== 1)], [36, [["doc17", 2]]]);
  assert.equal(most, 3);
  assert.deepEqual(done.tasks.review!.attempts.filter((a) => a.item === null).map((a) => a.status), ["blocked", "completed"]);
  assert.deepEqual(items(done).filter((a) => a.item === "doc17").map((a) => a.status), ["failed", "completed"]);
  assert.equal(items(done).length, 37);
  assert.deepEqual(Object.keys(summary as object), ids);
  assert.deepEqual((summary as Record<string, Json>).doc17, { id: "doc17", ok: true });
  app.store.close();
});

test("a map over the run input: an empty list completes at once, an invalid one fails before any item, a false guard skips it", { timeout: 20_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-map-lists-")), echo = join(root, "echo.mjs");
  await writeFile(echo, `const c=[];for await(const x of process.stdin)c.push(x);console.log(JSON.stringify({outcome:'completed',message:'echo',output:JSON.parse(c.join(''))}));`);
  const app = await fixture(root, { id: "lists", tasks: {
    review: { executor: "local", map: { over: "$run.input.docs", key: "id", concurrency: 2 }, inputs: { doc: "$item" }, run: [process.execPath, echo], when: [{ ref: "$run.input.go", equals: true }] },
    after: { executor: "local", dependsOn: ["review"], inputs: { reviews: "$tasks.review.output" }, run: [process.execPath, echo] },
  } });
  const empty = await app.scheduler.start("lists", { go: true, docs: [] });
  const done = await waitFor(app.scheduler, empty.workflowRunId, "completed");
  assert.deepEqual(done.tasks.review!.attempts.map((a) => [a.status, a.result?.output]), [["completed", {}]]);
  assert.deepEqual(done.tasks.after!.attempts[0]!.result!.output, { reviews: {} });
  for (const docs of [{}, [1], [{}], [{ id: "" }], [{ id: "a" }, { id: "a" }]]) {
    const started = await app.scheduler.start("lists", { go: true, docs });
    const view = await waitFor(app.scheduler, started.workflowRunId, "blocked");
    assert.deepEqual(view.tasks.review!.attempts.map((a) => [a.status, a.item, a.error?.code]), [["failed", null, "map_items_invalid"]], JSON.stringify(docs));
  }
  const skipped = await app.scheduler.start("lists", { go: false, docs: [{ id: "a" }] });
  const view = await waitFor(app.scheduler, skipped.workflowRunId, "blocked");
  assert.deepEqual(view.tasks.review!.attempts.map((a) => a.status), ["skipped"]);
  app.store.close();
});

test("after a restart, a resume reruns only the items that did not complete", { timeout: 20_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-map-restart-")), runs: string[] = [];
  const stub = new StubAcpExecutor(async (request) => { const id = (request.inputs.doc as { id: string }).id; runs.push(id); return { result: { outcome: "completed", message: id, output: { id, again: true } } }; });
  const app = await fixture(root, { id: "restart", tasks: {
    list: { executor: "agent", inputs: {}, prompt: "list" },
    review: { executor: "agent", dependsOn: ["list"], map: { over: "$tasks.list.output.docs", key: "id", concurrency: 3 }, inputs: { doc: "$item" }, prompt: "review" },
  } }, stub);
  const now = new Date().toISOString(), row = { workflowRunId: "wr_map", executorProfile: "agent", executorType: "acp" as const, error: null, acpSessionId: null, startedAt: now, updatedAt: now };
  app.store.insertRun({ id: "wr_map", workflowId: "restart", cwd: null, input: {}, status: "running", startedAt: now, updatedAt: now, endedAt: null });
  app.store.insertAttempt({ ...row, id: "tr_list", taskId: "list", attempt: 1, status: "completed", result: { outcome: "completed", message: "list", output: { docs: [{ id: "a" }, { id: "b" }, { id: "c" }] } }, endedAt: now, item: null, roundId: null });
  app.store.insertAttempt({ ...row, id: "tr_map", taskId: "review", attempt: 1, status: "running", result: null, endedAt: null, item: null, roundId: "tr_map" });
  app.store.insertAttempt({ ...row, id: "tr_a", taskId: "review", attempt: 2, status: "completed", result: { outcome: "completed", message: "a", output: { id: "a" } }, endedAt: now, item: "a", roundId: "tr_map" });
  app.store.insertAttempt({ ...row, id: "tr_b", taskId: "review", attempt: 3, status: "running", result: null, endedAt: null, item: "b", roundId: "tr_map" });
  await app.scheduler.recover();
  assert.equal(app.store.run("wr_map")!.status, "blocked");
  assert.deepEqual(["tr_map", "tr_b"].map((id) => app.store.attempt(id)!.error!.code), ["executor_lost", "executor_lost"]);
  await app.scheduler.resume("wr_map");
  const view = await waitFor(app.scheduler, "wr_map", "completed");
  assert.deepEqual(runs.sort(), ["b", "c"]);
  const maps = view.tasks.review!.attempts.filter((a) => a.item === null);
  assert.deepEqual(maps.map((a) => [a.status, a.roundId]), [["failed", "tr_map"], ["completed", "tr_map"]]);
  assert.deepEqual(maps[1]!.result!.output, { a: { id: "a" }, b: { id: "b", again: true }, c: { id: "c", again: true } });
  app.store.close();
});

test("cancelling one item leaves its siblings running and a resume reruns only it; the map attempt itself cannot be cancelled", { timeout: 20_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-map-cancel-")), step = join(root, "step.mjs"), log = join(root, "log"), go = join(root, "go-");
  await writeFile(step, `import{appendFileSync,existsSync}from'node:fs';const c=[];for await(const x of process.stdin)c.push(x);const{doc}=JSON.parse(c.join(''));appendFileSync(${JSON.stringify(log)},doc.id+'\\n');while(!existsSync(${JSON.stringify(go)}+doc.id))await new Promise(r=>setTimeout(r,10));console.log(JSON.stringify({outcome:'completed',message:doc.id,output:{id:doc.id}}));`);
  const app = await fixture(root, { id: "cancel", tasks: {
    review: { executor: "local", map: { over: "$run.input.docs", key: "id", concurrency: 3 }, inputs: { doc: "$item" }, run: [process.execPath, step] },
  } });
  const run = await app.scheduler.start("cancel", { docs: [{ id: "a" }, { id: "b" }, { id: "c" }] });
  const running = await waitFor(app.scheduler, run.workflowRunId, "running");
  const attempt = (item: string | null) => running.tasks.review!.attempts.find((a) => a.item === item)!;
  await assert.rejects(app.scheduler.cancelTask(attempt(null).id), (error) => error instanceof DagmarError && error.code === "invalid_task_state");
  const logged = async () => (await readFile(log, "utf8").catch(() => "")).split("\n").filter(Boolean).sort();
  for (let i = 0; i < 500 && (await logged()).length < 3; i++) await new Promise((resolve) => setTimeout(resolve, 10));
  await app.scheduler.cancelTask(attempt("b").id);
  await writeFile(go + "a", ""); await writeFile(go + "c", "");
  const blocked = await waitFor(app.scheduler, run.workflowRunId, "blocked");
  assert.deepEqual(blocked.tasks.review!.attempts.map((a) => [a.item, a.status]), [[null, "blocked"], ["a", "completed"], ["b", "cancelled"], ["c", "completed"]]);
  await writeFile(go + "b", "");
  await app.scheduler.resume(run.workflowRunId);
  const done = await waitFor(app.scheduler, run.workflowRunId, "completed");
  assert.deepEqual(done.tasks.review!.attempts.filter((a) => a.item === "b").map((a) => a.status), ["cancelled", "completed"]);
  assert.deepEqual(await logged(), ["a", "b", "b", "c"]);
  app.store.close();
});

test("resume --from a map task runs every item again in a new round", { timeout: 20_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-map-from-")), runs: string[] = [];
  const stub = new StubAcpExecutor(async (request) => {
    if (request.taskId === "summary") return { result: { outcome: "completed", message: "summary", output: {} } };
    const id = (request.inputs.doc as { id: string }).id; runs.push(id);
    return { result: { outcome: "completed", message: id, output: { id } } };
  });
  const app = await fixture(root, { id: "again", tasks: {
    review: { executor: "agent", map: { over: "$run.input.docs", key: "id", concurrency: 2 }, inputs: { doc: "$item" }, prompt: "review" },
    summary: { executor: "agent", dependsOn: ["review"], inputs: { reviews: "$tasks.review.output" }, prompt: "summary" },
  } }, stub);
  const run = await app.scheduler.start("again", { docs: [{ id: "a" }, { id: "b" }] });
  await waitFor(app.scheduler, run.workflowRunId, "completed");
  await app.scheduler.resume(run.workflowRunId, "review");
  const view = await waitFor(app.scheduler, run.workflowRunId, "completed");
  assert.deepEqual(runs.sort(), ["a", "a", "b", "b"]);
  const maps = view.tasks.review!.attempts.filter((a) => a.item === null);
  assert.deepEqual([maps.length, maps[0]!.roundId === maps[1]!.roundId, view.tasks.summary!.attempts.length], [2, false, 2]);
  assert.equal(new Set(view.tasks.review!.attempts.map((a) => a.attempt)).size, 6);
  app.store.close();
});

test("an item's permission request names its item", { timeout: 20_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-map-ask-"));
  const app = await fixture(root, { id: "ask", tasks: {
    review: { executor: "agent", map: { over: "$run.input.docs", key: "id", concurrency: 2 }, inputs: { doc: "$item" }, prompt: "review" },
  } }, new InteractiveExecutor());
  const run = await app.scheduler.start("ask", { docs: [{ id: "a" }, { id: "b" }] });
  for (let i = 0; i < 500 && app.scheduler.interactions().length < 2; i++) await new Promise((resolve) => setTimeout(resolve, 10));
  assert.deepEqual(app.scheduler.interactions().map((x) => x.item).sort(), ["a", "b"]);
  assert.equal((await app.scheduler.get(run.workflowRunId)).status, "running"); // a running map keeps its run running
  for (const x of app.scheduler.interactions()) await app.scheduler.answer(x.id, { outcome: { outcome: "selected", optionId: "yes" } });
  const view = await waitFor(app.scheduler, run.workflowRunId, "completed");
  assert.deepEqual(Object.keys(view.tasks.review!.attempts[0]!.result!.output as object), ["a", "b"]);
  app.store.close();
});

// Writes the workflow with the default `local` and `agent` executors unless it declares its own.
async function fixture(root: string, workflow: object, acp: Executor<AcpRequest> = new InteractiveExecutor()) {
  const workflowDir = join(root, "workflows"), storageDir = join(root, "state");
  await import("node:fs/promises").then((fs) => fs.mkdir(workflowDir, { recursive: true }));
  await writeFile(join(workflowDir, `${(workflow as { id: string }).id}.yaml`), stringify({ executors: defaultExecutors(root), ...workflow }));
  const store = new Store(":memory:"), transcripts = new Transcripts(storageDir), events = new EventBus();
  const scheduler = new Scheduler(store, transcripts, new WorkflowRepository(workflowDir), events, { process: new ProcessExecutor(), acp });
  return { store, transcripts, events, scheduler };
}

function defaultExecutors(root: string): Record<string, ExecutorProfile> {
  return { local: { type: "process", cwd: root, env: {} }, agent: { type: "acp", cwd: root, env: {}, run: ["unused"] } };
}

// Validation tests pass a workflow's executors alongside its tasks.
function validate(raw: object, executors: Record<string, ExecutorProfile>): Workflow {
  return validateWorkflow({ ...raw, executors });
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

// T2 helper: like InteractiveExecutor but emits a `turn` interaction instead of a
// `permission` interaction. The validate function passes the answer through as-is so
// the scheduler sees the user's next-turn text in the resulting TaskResult.output.
class TurnInteractionExecutor implements Executor<AcpRequest> {
  async start(_request: AcpRequest, hooks: Hooks): Promise<Execution> {
    let cancelled = false;
    const done = (async () => {
      const answer = await hooks.interact({ kind: "turn", method: "turn/next", request: { message: "first turn" }, validate: (value: Json) => value });
      const result: TaskResult = { outcome: "completed", message: "turn", output: { turn: answer } };
      return cancelled ? { error: { code: "cancelled", message: "cancelled" } } : { result };
    })();
    return { done, cancel: async () => { cancelled = true; } };
  }
}

// Stub ACP executor used by the scheduler-resolution test: dispatches per taskId via the
// caller-supplied behavior. The behavior may call hooks.session (to simulate a successful
// executor that persisted its session id) and must return a Settlement.
class StubAcpExecutor implements Executor<AcpRequest> {
  constructor(private readonly behavior: (request: AcpRequest, hooks: Hooks) => Promise<Settlement>) {}
  async start(request: AcpRequest, hooks: Hooks): Promise<Execution> {
    let cancelled = false;
    const done = this.behavior(request, hooks).then((value) => cancelled ? { error: { code: "cancelled", message: "cancelled" } } : value);
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
