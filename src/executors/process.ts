import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import { validateResult } from "../result.js";
import { DagmarError, type TaskError } from "../types.js";
import type { Execution, Executor, Hooks, ProcessRequest, Settlement } from "./types.js";

export class ProcessExecutor implements Executor<ProcessRequest> {
  async start(request: ProcessRequest, hooks: Hooks): Promise<Execution> {
    let child: ChildProcessWithoutNullStreams;
    try { child = spawn(request.run[0], request.run.slice(1), { cwd: request.cwd, env: request.env, shell: false, detached: true, stdio: ["pipe", "pipe", "pipe"] }); }
    catch (error) { return finished(failure("process_spawn_failed", error)); }
    return run(child, request, hooks);
  }
}

function run(child: ChildProcessWithoutNullStreams, request: ProcessRequest, hooks: Hooks): Execution {
  let stdout = "", cancelled = false, cancelPromise: Promise<void> | undefined, records = Promise.resolve();
  const record = (value: Parameters<Hooks["transcript"]>[0]): void => { records = records.then(() => hooks.transcript(value)).then(() => undefined); };
  // A task that never reads stdin (or exits first) makes our write fail with EPIPE.
  // Without a listener that is an unhandled 'error' that crashes the daemon; record it
  // and let the attempt settle on exit code / missing output instead.
  child.stdin.on("error", (error: Error) => record({ type: "lifecycle", direction: "internal", event: "process.stdin_error", data: { message: error.message } }));
  child.stdout.setEncoding("utf8"); child.stdout.on("data", (chunk: string) => { stdout += chunk; });
  child.stderr.setEncoding("utf8"); child.stderr.on("data", (chunk: string) => record({ type: "stdio", direction: "from_executor", stream: "stderr", data: chunk }));
  const spawned = new Promise<void>((resolve, reject) => { child.once("spawn", resolve); child.once("error", reject); });
  const closed = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) => child.once("close", (code, signal) => resolve({ code, signal })));
  const done = (async (): Promise<Settlement> => {
    try {
      await spawned;
      record({ type: "lifecycle", direction: "internal", event: "process.spawned", data: { pid: child.pid ?? null } });
      const input = JSON.stringify(request.inputs);
      record({ type: "stdio", direction: "to_executor", stream: "stdin", data: input });
      child.stdin.end(input);
      const exit = await closed;
      record({ type: "stdio", direction: "from_executor", stream: "stdout", data: stdout });
      record({ type: "lifecycle", direction: "internal", event: "process.exited", data: { code: exit.code, signal: exit.signal } });
      await records;
      if (cancelled) return { error: { code: "cancelled", message: "Process was cancelled" } };
      if (exit.code !== 0) return { error: { code: "process_exit_nonzero", message: `Process exited with code ${exit.code}` } };
      if (!stdout.trim()) return { error: { code: "process_stdout_missing", message: "Process produced no result" } };
      let value: unknown; try { value = JSON.parse(stdout); } catch { return { error: { code: "process_stdout_invalid", message: "Process stdout must be exactly one JSON value" } }; }
      try { return { result: validateResult(value, request.outputSchema) }; }
      catch (error) { return failure("result_validation_failed", error); }
    } catch (error) { return failure("process_failed", error); }
  })();
  return {
    done,
    cancel() {
      cancelled = true;
      return cancelPromise ??= (async () => {
        try { await spawned; } catch { await done; return; }
        if (child.exitCode === null && child.pid) {
          signal(child, "SIGTERM");
          if (await Promise.race([closed.then(() => false), delay(2000, true, { ref: false })])) signal(child, "SIGKILL");
        }
        await closed; await done;
      })();
    },
  };
}

function signal(child: ChildProcessWithoutNullStreams, value: NodeJS.Signals): void {
  try { process.kill(-(child.pid as number), value); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") child.kill(value); }
}
function failure(code: string, error: unknown): { error: TaskError } { return { error: { code: error instanceof DagmarError ? error.code : code, message: error instanceof Error ? error.message : "Process failed" } }; }
function finished(value: Settlement): Execution { return { done: Promise.resolve(value), cancel: async () => undefined }; }
