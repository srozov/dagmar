#!/usr/bin/env node
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { loadConfig } from "./config.js";
import { AcpExecutor } from "./executors/acp.js";
import { ProcessExecutor } from "./executors/process.js";
import { EventBus, RpcServer } from "./rpc.js";
import { Scheduler } from "./scheduler.js";
import { Store } from "./store.js";
import { Transcripts } from "./transcript.js";
import type { Config, Json, JsonObject } from "./types.js";
import { DagmarError } from "./types.js";
import { WorkflowRepository } from "./workflow.js";

type Handler = (params: JsonObject) => Json | Promise<Json>;

export interface Daemon { address: { host: string; port: number }; stop(): Promise<void> }

export async function startDaemon(configPath?: string): Promise<Daemon> {
  const config = await loadConfig(configPath);
  const startedAt = new Date().toISOString();
  const events = new EventBus();
  const store = new Store(join(config.storageDir, "dagmar.sqlite"));
  const transcripts = new Transcripts(config.storageDir);
  const workflows = new WorkflowRepository(config.workflowDir, config.executors);
  const scheduler = new Scheduler(store, transcripts, workflows, events, config.executors, {
    process: new ProcessExecutor(),
    acp: new AcpExecutor(),
  });
  const handlers: Record<string, Handler> = {
    "system.ping": (p: JsonObject) => {
      empty(p);
      return json({
        ok: true,
        version: "0.0.0",
        startedAt,
        currentSequence: events.current,
      });
    },
    "executor.list": (p: JsonObject) => {
      empty(p);
      return json(
        Object.entries(config.executors)
          .sort()
          .map(([name, x]) => ({ name, type: x.type })),
      );
    },
    "workflow.list": async (p: JsonObject) => {
      empty(p);
      return json(await workflows.list());
    },
    "workflow.get": async (p: JsonObject) => json(await workflows.get(string(p, "workflowId"))),
    "run.start": async (p: JsonObject) =>
      json(await scheduler.start(string(p, "workflowId"), required(p, "input"))),
    "run.list": (p: JsonObject) => {
      empty(p);
      return json(scheduler.list());
    },
    "run.get": async (p: JsonObject) => json(await scheduler.get(string(p, "workflowRunId"))),
    "run.resume": async (p: JsonObject) =>
      json(await scheduler.resume(string(p, "workflowRunId"))),
    "run.cancel": async (p: JsonObject) =>
      json(await scheduler.cancelRun(string(p, "workflowRunId"))),
    "task.cancel": async (p: JsonObject) =>
      json(await scheduler.cancelTask(string(p, "taskRunId"))),
    "interaction.list": (p: JsonObject) => {
      empty(p);
      return json(scheduler.interactions());
    },
    "interaction.answer": async (p: JsonObject) =>
      json(await scheduler.answer(string(p, "interactionId"), required(p, "response"))),
    "transcript.read": async (p: JsonObject) => {
      const id = string(p, "taskRunId");
      const task = store.attempt(id);
      if (!task) throw new DagmarError("task_run_not_found", "Task run was not found");
      const after = p.afterLine === undefined ? 0 : integer(p, "afterLine");
      return json(await transcripts.read(task.workflowRunId, id, after));
    },
  };
  const rpc = new RpcServer(config.listen.host, config.listen.port, handlers, events);
  await scheduler.recover();
  const address = await rpc.start();
  let stopping: Promise<void> | undefined;
  const stop = () =>
    (stopping ??= (async () => {
      const schedulerStop = scheduler.shutdown();
      await Promise.all([schedulerStop, rpc.stop()]);
      store.close();
    })());
  return { address, stop };
}

function empty(p: JsonObject): void {
  if (Object.keys(p).length) throw new DagmarError("invalid_params", "Method takes no parameters");
}

function string(p: JsonObject, key: string): string {
  const value = p[key];
  if (typeof value !== "string") throw new DagmarError("invalid_params", `${key} must be a string`);
  return value;
}

function integer(p: JsonObject, key: string): number {
  const value = p[key];
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw new DagmarError("invalid_params", `${key} must be a non-negative integer`);
  }
  return value as number;
}

function required(p: JsonObject, key: string): Json {
  if (!Object.hasOwn(p, key)) throw new DagmarError("invalid_params", `${key} is required`);
  return p[key]!;
}

function json(value: unknown): Json {
  return value as Json;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const args = process.argv.slice(2);
  let config: string | undefined;
  if (args.length) {
    if (args.length !== 2 || args[0] !== "--config") {
      throw new Error("Usage: dagmard [--config <path>]");
    }
    config = args[1];
  }
  const daemon = await startDaemon(config);
  const stop = async () => {
    await daemon.stop();
    process.exitCode = 0;
  };
  process.once("SIGINT", () => void stop());
  process.once("SIGTERM", () => void stop());
}
