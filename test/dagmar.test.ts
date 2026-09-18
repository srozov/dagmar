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
  app.store.insertRun({ id: "wr_m2", workflowId: "gatemix2", input: {}, status: "waiting", startedAt: now, updatedAt: now, endedAt: now });
  app.store.insertAttempt({ id: "tr_m2_dep", workflowRunId: "wr_m2", taskId: "dep", attempt: 1, executorProfile: "local", executorType: "process", status: "running", result: null, error: null, acpSessionId: null, startedAt: now, updatedAt: now, endedAt: null });
  app.store.insertAttempt({ id: "tr_m2_gate", workflowRunId: "wr_m2", taskId: "gate", attempt: 1, executorProfile: "gate", executorType: "gate", status: "awaiting_input", result: null, error: null, acpSessionId: null, startedAt: now, updatedAt: now, endedAt: null });
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


// T4 (when validation matrix): validateWorkflow accepts well-formed when clauses (ref/equals/in/dep
// checks, run-input ref with no deps, multi-clause AND, gate+when), and rejects the variants the
// plan enumerates. Profiles are passed directly so each rejection pins a specific failure mode.
test("validateWorkflow accepts when guards and rejects malformed variants", () => {
  const profiles: Config["executors"] = {
    local: { type: "process", cwd: "/tmp", env: {} },
    gate: { type: "gate", cwd: "/tmp", env: {} } as never, // gate tasks don't need a profile lookup
    // Gate tasks short-circuit validation (item.gate branch) before the executor profile lookup, so
    // we register a "gate" key just so any non-gate test that names it as executor can fail loudly.
  };
  // Accept: ref + equals, dep [decide].
  const ok1 = validateWorkflow({
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
  const ok2 = validateWorkflow({
    id: "ok2",
    tasks: {
      decide: { executor: "local", inputs: {}, run: ["true"] },
      branch: { executor: "local", dependsOn: ["decide"], inputs: {}, run: ["true"], when: [{ ref: "$tasks.decide.output.route", in: ["a", "b"] }] },
    },
  }, profiles);
  assert.deepEqual(ok2.tasks.branch!.when![0]!.in, ["a", "b"]);

  // Accept: run-input ref with NO deps.
  const ok3 = validateWorkflow({
    id: "ok3",
    tasks: { t: { executor: "local", inputs: {}, run: ["true"], when: [{ ref: "$run.input.mode", equals: "fast" }] } },
  }, profiles);
  assert.equal(ok3.tasks.t!.when![0]!.ref, "$run.input.mode");

  // Accept: multi-clause AND.
  const ok4 = validateWorkflow({
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
  const ok5 = validateWorkflow({
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
    () => validateWorkflow({
      id: "x",
      tasks: { t: { executor: "local", inputs: {}, run: ["true"], when: [{ ref: "$run.input.mode" }] } },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_guard" && /exactly one/.test(error.message),
  );

  // Reject: clause with BOTH equals and in.
  assert.throws(
    () => validateWorkflow({
      id: "x",
      tasks: { t: { executor: "local", inputs: {}, run: ["true"], when: [{ ref: "$run.input.mode", equals: "a", in: ["a"] }] } },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_guard" && /exactly one/.test(error.message),
  );

  // Reject: empty in.
  assert.throws(
    () => validateWorkflow({
      id: "x",
      tasks: { t: { executor: "local", inputs: {}, run: ["true"], when: [{ ref: "$run.input.mode", in: [] }] } },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_guard" && /non-empty/.test(error.message),
  );

  // Reject: $tasks ref to a non-dependency.
  assert.throws(
    () => validateWorkflow({
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
    () => validateWorkflow({
      id: "x",
      tasks: { t: { executor: "local", inputs: {}, run: ["true"], when: [{ ref: "route", equals: "a" }] } },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_guard" && /\$tasks or \$run/.test(error.message),
  );

  // Reject: malformed $ ref -> invalid_input_reference (propagated from reference()).
  assert.throws(
    () => validateWorkflow({
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


// T5 (loop validation matrix): validateWorkflow accepts loop { to, maxVisits } on a source
// whose `to` is a direct dependency, and rejects every variant the plan enumerates. Mirrors
// the T4 validateWorkflow matrix at lines 1049-1165: profiles are passed in directly so each
// case pins a specific failure mode.
test("validateWorkflow accepts loop on a direct-dependency source and rejects malformed variants", () => {
  const profiles: Config["executors"] = {
    local: { type: "process", cwd: "/tmp", env: {} },
  };

  // Accept: direct-dep target with both when and loop; loop fields flow through onto the TaskDef.
  const ok1 = validateWorkflow({
    id: "loop-ok1",
    tasks: {
      verify: { executor: "local", inputs: {}, run: ["true"] },
      fixup: {
        executor: "local", dependsOn: ["verify"], inputs: {}, run: ["true"],
        when: [{ ref: "$tasks.verify.output.passed", equals: false }],
        loop: { to: "verify", maxVisits: 3 },
      },
    },
  }, profiles);
  assert.equal(ok1.tasks.fixup!.loop!.to, "verify");
  assert.equal(ok1.tasks.fixup!.loop!.maxVisits, 3);

  // Accept: a loop source without a `when` guard (topology-only check).
  const ok2 = validateWorkflow({
    id: "loop-ok2",
    tasks: {
      verify: { executor: "local", inputs: {}, run: ["true"] },
      fixup: {
        executor: "local", dependsOn: ["verify"], inputs: {}, run: ["true"],
        loop: { to: "verify", maxVisits: 5 },
      },
    },
  }, profiles);
  assert.equal(ok2.tasks.fixup!.loop!.maxVisits, 5);

  // Reject: loop.to refers to an unknown task.
  assert.throws(
    () => validateWorkflow({
      id: "x",
      tasks: {
        verify: { executor: "local", inputs: {}, run: ["true"] },
        fixup: { executor: "local", dependsOn: ["verify"], inputs: {}, run: ["true"], loop: { to: "ghost", maxVisits: 3 } },
      },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_loop" && /not a task/.test(error.message),
  );

  // Reject: loop.to is not a direct dependency.
  assert.throws(
    () => validateWorkflow({
      id: "x",
      tasks: {
        verify: { executor: "local", inputs: {}, run: ["true"] },
        middle: { executor: "local", dependsOn: ["verify"], inputs: {}, run: ["true"] },
        fixup: { executor: "local", dependsOn: ["middle"], inputs: {}, run: ["true"], loop: { to: "verify", maxVisits: 3 } },
      },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_loop" && /direct dependency/.test(error.message),
  );

  // Reject: loop.to === id (self-loop).
  assert.throws(
    () => validateWorkflow({
      id: "x",
      tasks: {
        step: { executor: "local", inputs: {}, run: ["true"], loop: { to: "step", maxVisits: 2 } },
      },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_loop" && /cannot be itself/.test(error.message),
  );

  // Reject: maxVisits < 1 (schema rejects 0 with workflow_invalid).
  assert.throws(
    () => validateWorkflow({
      id: "x",
      tasks: {
        verify: { executor: "local", inputs: {}, run: ["true"] },
        fixup: { executor: "local", dependsOn: ["verify"], inputs: {}, run: ["true"], loop: { to: "verify", maxVisits: 0 } },
      },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && (error.code === "workflow_invalid" || (error.code === "invalid_loop" && /maxVisits must be an integer/.test(error.message))),
  );

  // Reject: two sources sharing the same loop.to.
  assert.throws(
    () => validateWorkflow({
      id: "x",
      tasks: {
        verify: { executor: "local", inputs: {}, run: ["true"] },
        fixup1: { executor: "local", dependsOn: ["verify"], inputs: {}, run: ["true"], loop: { to: "verify", maxVisits: 3 } },
        fixup2: { executor: "local", dependsOn: ["verify"], inputs: {}, run: ["true"], loop: { to: "verify", maxVisits: 2 } },
      },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "invalid_loop" && /more than one loop/.test(error.message),
  );

  // Regression: a raw dependsOn cycle is still rejected as dependency_cycle.
  assert.throws(
    () => validateWorkflow({
      id: "x",
      tasks: {
        a: { executor: "local", inputs: {}, run: ["true"], dependsOn: ["b"] },
        b: { executor: "local", inputs: {}, run: ["true"], dependsOn: ["a"] },
      },
    }, profiles),
    (error: unknown) => error instanceof DagmarError && error.code === "dependency_cycle",
  );
});

// T5 (loop then pass): verify/fixup/review workflow where verify.mjs fails for the first K-1
// invocations (read from a fixture-root file counter) and passes on the Kth. verify is the
// loop target, fixup is the source (when:[passed==false], loop:{to:verify,maxVisits:3}), review
// depends on verify and gates on passed==true.
test("scheduler runs the bounded loop, exits on the source's guard, and decides review once", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-loop-pass-"));
  const counter = join(root, "counter");
  const verifyScript = join(root, "verify.mjs");
  const echoScript = join(root, "echo.mjs");
  await writeFile(
    verifyScript,
    "import {existsSync, readFileSync, writeFileSync} from 'node:fs';\nfor await (const _ of process.stdin) {}\nconst counterFile = " + JSON.stringify(counter) + ";\nconst threshold = 3;\nconst n = existsSync(counterFile) ? parseInt(readFileSync(counterFile, 'utf8'), 10) : 0;\nconst next = n + 1;\nwriteFileSync(counterFile, String(next));\nconst passed = next >= threshold;\nconsole.log(JSON.stringify({outcome:'completed',message:'v',output:{passed, count: next}}));\n",
  );
  await writeFile(echoScript, "for await (const _ of process.stdin) {} console.log(JSON.stringify({outcome:'completed',message:'ok',output:{}}));");
  const workflow = {
    id: "looppass",
    tasks: {
      verify: { executor: "local", inputs: {}, run: [process.execPath, verifyScript] },
      fixup: {
        executor: "local", dependsOn: ["verify"], inputs: {},
        run: [process.execPath, echoScript],
        when: [{ ref: "$tasks.verify.output.passed", equals: false }],
        loop: { to: "verify", maxVisits: 3 },
      },
      review: {
        executor: "local", dependsOn: ["verify"], inputs: {},
        run: [process.execPath, echoScript],
        when: [{ ref: "$tasks.verify.output.passed", equals: true }],
      },
    },
  };
  const app = await fixture(root, workflow);
  const started = await app.scheduler.start("looppass", {});
  const view = await waitFor(app.scheduler, started.workflowRunId, "completed");
  assert.equal(view.status, "completed");
  assert.equal(view.tasks.verify!.attempts.length, 3, "verify runs until the Kth pass; here K=3");
  assert.ok(view.tasks.verify!.attempts.every((a) => a.status === "completed"));
  const fixupCompleted = view.tasks.fixup!.attempts.filter((a) => a.status === "completed").length;
  const fixupSkipped = view.tasks.fixup!.attempts.filter((a) => a.status === "skipped").length;
  assert.equal(fixupCompleted, 2, "fixup completes exactly twice (after verify#1 and verify#2)");
  assert.equal(fixupSkipped, 1, "fixup skips on the final iteration when verify passes");
  assert.equal(view.tasks.review!.state, "completed");
  assert.equal(view.tasks.review!.attempts.length, 1);
  app.store.close();
});

// T5 (exhaustion): maxVisits:3 and verify always fails — source saturates its cap (3 completions),
// target runs maxVisits+1 times (4), review skips because loopFinal pulls the run to completed
// with the exit branch skipped (gate #3).
test("loop exhausts at maxVisits and the exit branch is decided once as skipped", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-loop-exhaust-"));
  const verifyScript = join(root, "verify.mjs");
  const echoScript = join(root, "echo.mjs");
  await writeFile(verifyScript, "for await (const _ of process.stdin) {} console.log(JSON.stringify({outcome:'completed',message:'v',output:{passed:false, count: 1}}));");
  await writeFile(echoScript, "for await (const _ of process.stdin) {} console.log(JSON.stringify({outcome:'completed',message:'ok',output:{}}));");
  const workflow = {
    id: "loopexhaust",
    tasks: {
      verify: { executor: "local", inputs: {}, run: [process.execPath, verifyScript] },
      fixup: {
        executor: "local", dependsOn: ["verify"], inputs: {},
        run: [process.execPath, echoScript],
        when: [{ ref: "$tasks.verify.output.passed", equals: false }],
        loop: { to: "verify", maxVisits: 3 },
      },
      review: {
        executor: "local", dependsOn: ["verify"], inputs: {},
        run: [process.execPath, echoScript],
        when: [{ ref: "$tasks.verify.output.passed", equals: true }],
      },
    },
  };
  const app = await fixture(root, workflow);
  const started = await app.scheduler.start("loopexhaust", {});
  const view = await waitFor(app.scheduler, started.workflowRunId, "completed");
  assert.equal(view.status, "completed");
  assert.equal(view.tasks.verify!.attempts.length, 4, "target runs maxVisits+1 times (1 initial + 3 re-verifies)");
  const fixupCompleted = view.tasks.fixup!.attempts.filter((a) => a.status === "completed").length;
  assert.equal(fixupCompleted, 3, "source completes exactly maxVisits times before exhaustion");
  assert.equal(view.tasks.review!.state, "skipped", "exit branch skipped when the loop exhausts");
  assert.equal(view.tasks.review!.attempts[0]!.status, "skipped");
  app.store.close();
});

// T5 (immediate pass): verify passes on the first invocation — loop never iterates; fixup's
// guard fails, review is decided once and runs.
test("loop exits on the first iteration when verify passes immediately", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-loop-immediate-"));
  const verifyScript = join(root, "verify.mjs");
  const echoScript = join(root, "echo.mjs");
  await writeFile(verifyScript, "for await (const _ of process.stdin) {} console.log(JSON.stringify({outcome:'completed',message:'v',output:{passed:true, count: 1}}));");
  await writeFile(echoScript, "for await (const _ of process.stdin) {} console.log(JSON.stringify({outcome:'completed',message:'ok',output:{}}));");
  const workflow = {
    id: "loopimmediate",
    tasks: {
      verify: { executor: "local", inputs: {}, run: [process.execPath, verifyScript] },
      fixup: {
        executor: "local", dependsOn: ["verify"], inputs: {},
        run: [process.execPath, echoScript],
        when: [{ ref: "$tasks.verify.output.passed", equals: false }],
        loop: { to: "verify", maxVisits: 3 },
      },
      review: {
        executor: "local", dependsOn: ["verify"], inputs: {},
        run: [process.execPath, echoScript],
        when: [{ ref: "$tasks.verify.output.passed", equals: true }],
      },
    },
  };
  const app = await fixture(root, workflow);
  const started = await app.scheduler.start("loopimmediate", {});
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

// T5 (restart mid-loop): the loop is in flight when the daemon is killed; the next scheduler
// recovers the executor_lost attempt, the operator resumes, and the loop continues with the
// visit count preserved (gate #4 — resume continues, never resets). Pre-inserting the running
// attempt mirrors the recovery test at lines 211-217 and the gateshutdown restart test at
// lines 977-1013 ��� deterministic, no real daemon kill, same store path. verify is the loop
// target; pre-inserting it `running` makes recover() fail it with `executor_lost`, which
// cascades blocked_by_dependency through fixup and review, so the run lands `blocked` and
// resume() can pick it up.
test("restart mid-loop: recover() blocks, resume() continues, visit count preserved", async () => {
  const root = await mkdtemp(join(tmpdir(), "dagmar-loop-restart-"));
  const counter = join(root, "counter");
  const verifyScript = join(root, "verify.mjs");
  const echoScript = join(root, "echo.mjs");
  await writeFile(
    verifyScript,
    "import {existsSync, readFileSync, writeFileSync} from 'node:fs';\nfor await (const _ of process.stdin) {}\nconst counterFile = " + JSON.stringify(counter) + ";\nconst n = existsSync(counterFile) ? parseInt(readFileSync(counterFile, 'utf8'), 10) : 0;\nconst next = n + 1;\nwriteFileSync(counterFile, String(next));\nconst passed = next >= 4;\nconsole.log(JSON.stringify({outcome:'completed',message:'v',output:{passed, count: next}}));\n",
  );
  await writeFile(echoScript, "for await (const _ of process.stdin) {} console.log(JSON.stringify({outcome:'completed',message:'ok',output:{}}));");
  const workflow = {
    id: "looprestart",
    tasks: {
      verify: { executor: "local", inputs: {}, run: [process.execPath, verifyScript] },
      fixup: {
        executor: "local", dependsOn: ["verify"], inputs: {},
        run: [process.execPath, echoScript],
        when: [{ ref: "$tasks.verify.output.passed", equals: false }],
        loop: { to: "verify", maxVisits: 3 },
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

  // Pre-insert a mid-loop state: verify#1 was running on a scheduler that has since died.
  const profiles: Config["executors"] = { local: { type: "process", cwd: root, env: {} } };
  const now = new Date().toISOString();
  const store1 = new Store(storePath);
  store1.insertRun({ id: "wr_loop_r", workflowId: workflow.id, input: {}, status: "running", startedAt: now, updatedAt: now, endedAt: null });
  store1.insertAttempt({ id: "tr_v1", workflowRunId: "wr_loop_r", taskId: "verify", attempt: 1, executorProfile: "local", executorType: "process", status: "running", result: null, error: null, acpSessionId: null, startedAt: now, updatedAt: now, endedAt: null });
  store1.close();

  // Second scheduler on the same store — recovers the in-flight attempt then resumes.
  const store2 = new Store(storePath);
  const transcripts2 = new Transcripts(join(root, "state"));
  const events2 = new EventBus();
  const scheduler2 = new Scheduler(store2, transcripts2, new WorkflowRepository(join(root, "workflows"), profiles), events2, profiles, { process: new ProcessExecutor(), acp: new InteractiveExecutor() });
  await scheduler2.recover();
  // After recover: in-flight attempt is executor_lost and the run is blocked (the failed
  // task cascades blocked_by_dependency through fixup and review).
  assert.equal(store2.run("wr_loop_r")!.status, "blocked");
  assert.equal(store2.attempt("tr_v1")!.status, "failed");
  assert.equal(store2.attempt("tr_v1")!.error!.code, "executor_lost");

  // Resume retries the failed attempt; the loop continues and verify eventually passes.
  await scheduler2.resume("wr_loop_r");
  const view = await waitFor(scheduler2, "wr_loop_r", "completed");
  assert.equal(view.status, "completed");

  // Visit count preserved across the restart: only `completed` counts toward the cap.
  const fixupCompleted = view.tasks.fixup!.attempts.filter((a) => a.status === "completed").length;
  assert.ok(fixupCompleted <= 3, `fixup completed count (${fixupCompleted}) must be <= maxVisits (3)`);

  // verify ran 3 successful times on scheduler2; verify#1 was the pre-inserted failed attempt.
  const verifyCompleted = view.tasks.verify!.attempts.filter((a) => a.status === "completed");
  assert.equal(verifyCompleted.length, 3, "verify ran three times on scheduler2 (2 fail, 1 pass)");
  const lastVerify = verifyCompleted[verifyCompleted.length - 1]!;
  assert.deepEqual(lastVerify.result!.output, { passed: true, count: 4 });

  // review decided once after loopFinal.
  assert.equal(view.tasks.review!.state, "completed");
  store2.close();
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
