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
import type { AcpRequest, Execution, Executor, Hooks, Settlement } from "../src/executors/types.js";
import { EventBus, RpcClient } from "../src/rpc.js";
import { Scheduler } from "../src/scheduler.js";
import { Store } from "../src/store.js";
import { Transcripts } from "../src/transcript.js";
import type { Config, Json, RunView, TaskResult } from "../src/types.js";
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

test("blocked runs resume only the blocker and cancellation is terminal", async () => {
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

  await writeFile(
    join(root, "workflows", "resume.yaml"),
    stringify({
      id: "resume",
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

// T1 (session continuation): validateWorkflow accepts a continue task whose 'from' is a
// same-agent ACP dependency, and rejects every variant the plan enumerates. Profiles are
// passed in directly so each rejection can pin a specific failure mode without disk I/O.
test("validateWorkflow accepts continue with a same-agent ACP dependency and rejects variants", () => {
  const profiles: Config["executors"] = {
    acpA: { type: "acp", cwd: "/tmp", env: {}, run: ["agent"] },
    acpB: { type: "acp", cwd: "/tmp", env: {}, run: ["agent"] }, // same agent as acpA
    acpOther: { type: "acp", cwd: "/tmp", env: {}, run: ["other"] }, // different agent
    proc: { type: "process", cwd: "/tmp", env: {} },
  };
  // Accept: same-agent acp dependency.
  const ok = validateWorkflow({
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
    () => validateWorkflow({
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
    () => validateWorkflow({
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
    () => validateWorkflow({
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
    () => validateWorkflow({
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
    () => validateWorkflow({
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
  const profiles: Config["executors"] = {
    acp: { type: "acp", cwd: "/tmp", env: {}, run: ["agent"] },
    proc: { type: "process", cwd: "/tmp", env: {} },
  };

  // Accept: ACP task with interactive: true; the normalized TaskDef carries it.
  const ok = validateWorkflow({
    id: "ok",
    tasks: { A: { executor: "acp", inputs: {}, prompt: "talk", interactive: true } },
  }, profiles);
  assert.equal(ok.tasks.A!.interactive, true);

  // Reject: process task with interactive: true. The validator must surface a clear
  // `invalid_task` code (not a schema shape failure — the JSON schema accepts boolean).
  assert.throws(
    () => validateWorkflow({
      id: "x",
      tasks: { P: { executor: "proc", inputs: {}, run: ["true"], interactive: true } },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_task" && /Process task/.test(error.message),
  );
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
  const profiles: Config["executors"] = { agent: { type: "acp", cwd: "/tmp", env: {}, run: ["agent"] } };

  // Accept: a gate task with no executor and a prompt.
  const ok1 = validateWorkflow({ id: "g1", tasks: { g: { inputs: {}, gate: { prompt: "Approve?" } } } }, profiles);
  assert.equal(ok1.tasks.g!.gate?.prompt, "Approve?");
  assert.equal(ok1.tasks.g!.gate?.schema, undefined);
  assert.equal(ok1.tasks.g!.executor, undefined);

  // Accept: a gate task with a schema; the schema flows through.
  const ok2 = validateWorkflow({
    id: "g2",
    tasks: { g: { inputs: {}, gate: { prompt: "Review", schema: { type: "string" } } } },
  }, profiles);
  assert.deepEqual(ok2.tasks.g!.gate?.schema, { type: "string" });

  // Accept: a gate task with deps + inputs (references will resolve during prepare()).
  const ok3 = validateWorkflow({
    id: "g3",
    tasks: {
      dep: { executor: "agent", inputs: {}, prompt: "a" },
      g: { dependsOn: ["dep"], inputs: { x: "$tasks.dep.output" }, gate: { prompt: "?" } },
    },
  }, profiles);
  assert.deepEqual(ok3.tasks.g!.dependsOn, ["dep"]);

  // Reject: task with both executor and gate.
  assert.throws(
    () => validateWorkflow({ id: "x", tasks: { g: { executor: "agent", inputs: {}, prompt: "p", gate: { prompt: "?" } } } }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_task" && /cannot have both/.test(error.message),
  );

  // Reject: task with neither executor nor gate.
  assert.throws(
    () => validateWorkflow({ id: "x", tasks: { g: { inputs: {} } } }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_task" && /requires executor or gate/.test(error.message),
  );

  // Reject: gate task with prompt (the task-level field, not gate.prompt).
  assert.throws(
    () => validateWorkflow({ id: "x", tasks: { g: { inputs: {}, prompt: "p", gate: { prompt: "?" } } } }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_task" && /must not have/.test(error.message),
  );

  // Reject: gate task with run.
  assert.throws(
    () => validateWorkflow({ id: "x", tasks: { g: { inputs: {}, run: ["x"], gate: { prompt: "?" } } } }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_task" && /must not have/.test(error.message),
  );

  // Reject: gate task with session.
  assert.throws(
    () => validateWorkflow({ id: "x", tasks: { g: { inputs: {}, gate: { prompt: "?" }, session: { mode: "fresh" } } } }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_task" && /must not have/.test(error.message),
  );

  // Reject: gate task with interactive.
  assert.throws(
    () => validateWorkflow({ id: "x", tasks: { g: { inputs: {}, gate: { prompt: "?" }, interactive: true } } }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_task" && /must not have/.test(error.message),
  );

  // Reject: gate task with outputSchema.
  assert.throws(
    () => validateWorkflow({ id: "x", tasks: { g: { inputs: {}, gate: { prompt: "?" }, outputSchema: { type: "object" } } } }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_task" && /must not have/.test(error.message),
  );

  // Reject: empty gate prompt.
  assert.throws(
    () => validateWorkflow({ id: "x", tasks: { g: { inputs: {}, gate: { prompt: "   " } } } }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_task" && /non-empty prompt/.test(error.message),
  );

  // Reject: invalid gate schema (JSON Schema that fails ajv.compile).
  assert.throws(
    () => validateWorkflow({ id: "x", tasks: { g: { inputs: {}, gate: { prompt: "?", schema: { type: 123 } } } } }, profiles),
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
  app.store.insertRun({ id: "wr_g", workflowId: "gaterecover", input: {}, status: "waiting", startedAt: now, updatedAt: now, endedAt: null });
  app.store.insertAttempt({ id: "tr_g", workflowRunId: "wr_g", taskId: "gate", attempt: 1, executorProfile: "gate", executorType: "gate", status: "awaiting_input", result: null, error: null, acpSessionId: null, startedAt: now, updatedAt: now, endedAt: null });
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
  app.store.insertRun({ id: "wr_m", workflowId: "gatemix", input: {}, status: "waiting", startedAt: now, updatedAt: now, endedAt: null });
  // Non-gate (process) attempt in 'running'.
  app.store.insertAttempt({ id: "tr_m_dep", workflowRunId: "wr_m", taskId: "dep", attempt: 1, executorProfile: "local", executorType: "process", status: "running", result: null, error: null, acpSessionId: null, startedAt: now, updatedAt: now, endedAt: null });
  // Gate attempt in 'awaiting_input'.
  app.store.insertAttempt({ id: "tr_m_gate", workflowRunId: "wr_m", taskId: "gate", attempt: 1, executorProfile: "gate", executorType: "gate", status: "awaiting_input", result: null, error: null, acpSessionId: null, startedAt: now, updatedAt: now, endedAt: null });
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
  const profiles1: Config["executors"] = { local: { type: "process", cwd: root, env: {} }, agent: { type: "acp", cwd: root, env: {}, run: ["unused"] } };
  const store1 = new Store(storePath), transcripts1 = new Transcripts(join(root, "state")), events1 = new EventBus();
  const scheduler1 = new Scheduler(store1, transcripts1, new WorkflowRepository(join(root, "workflows"), profiles1), events1, profiles1, { process: new ProcessExecutor(), acp: new AcpExecutor() });
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
  const scheduler2 = new Scheduler(store2, transcripts2, new WorkflowRepository(join(root, "workflows"), profiles1), events2, profiles1, { process: new ProcessExecutor(), acp: new AcpExecutor() });
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
