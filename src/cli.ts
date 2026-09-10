#!/usr/bin/env node
import { pathToFileURL } from "node:url";
import { loadConfig } from "./config.js";
import { RpcClient } from "./rpc.js";
import type { Json, JsonObject } from "./types.js";

async function main(argv: string[]): Promise<void> {
  const args = [...argv]; let configPath: string | undefined;
  const at = args.indexOf("--config"); if (at >= 0) { if (!args[at + 1]) usage(); configPath = args[at + 1]; args.splice(at, 2); }
  const config = await loadConfig(configPath), host = config.listen.host === "::1" ? "[::1]" : config.listen.host;
  const client = new RpcClient(`ws://${host}:${config.listen.port}`); await client.connect();
  try {
    const [command, ...rest] = args; let method = "", params: JsonObject = {};
    switch (command) {
      case "ping": method = "system.ping"; break;
      case "executors": method = "executor.list"; break;
      case "workflows": method = "workflow.list"; break;
      case "workflow": method = "workflow.get"; params = { workflowId: one(rest) }; break;
      case "runs": method = "run.list"; break;
      case "status": method = "run.get"; params = { workflowRunId: one(rest) }; break;
      case "resume": method = "run.resume"; params = { workflowRunId: one(rest) }; break;
      case "cancel": method = "run.cancel"; params = { workflowRunId: one(rest) }; break;
      case "cancel-task": method = "task.cancel"; params = { taskRunId: one(rest) }; break;
      case "pending": method = "interaction.list"; break;
      case "run": { const workflowId = rest.shift(); if (!workflowId) usage(); let input: Json = {}; if (rest.length) { if (rest.length !== 2) usage(); if (rest[0] === "--input") input = rest[1]!; else if (rest[0] === "--input-json") input = parse(rest[1]!); else usage(); } method = "run.start"; params = { workflowId, input }; break; }
      case "answer": { const interactionId = rest.shift(); if (!interactionId || rest[0] !== "--json" || rest.length !== 2) usage(); method = "interaction.answer"; params = { interactionId, response: parse(rest[1]!) }; break; }
      case "transcript": { const taskRunId = rest.shift(); if (!taskRunId) usage(); params = { taskRunId }; if (rest.length) { if (rest[0] !== "--after-line" || rest.length !== 2 || !/^\d+$/.test(rest[1]!)) usage(); params.afterLine = Number(rest[1]); } method = "transcript.read"; break; }
      case "watch": { const workflowRunId = one(rest), view = await client.request("run.get", { workflowRunId }); console.log(JSON.stringify(view, null, 2)); client.onEvent((event) => console.log(JSON.stringify(event))); const sequence = (view as JsonObject).sequence; await client.request("events.subscribe", { workflowRunId, ...(typeof sequence === "number" ? { afterSequence: sequence } : {}) }); await Promise.race([client.closed, signal()]); return; }
      default: usage();
    }
    console.log(JSON.stringify(await client.request(method, params), null, 2));
  } finally { client.close(); }
}

function one(args: string[]): string { if (args.length !== 1) usage(); return args[0]!; }
function parse(text: string): Json { try { return JSON.parse(text) as Json; } catch { throw new Error("Invalid JSON"); } }
function signal(): Promise<void> { return new Promise((resolve) => { process.once("SIGINT", resolve); process.once("SIGTERM", resolve); }); }
function usage(): never { throw new Error("Usage: dagmar [--config <path>] ping|executors|workflows|workflow|run|runs|status|watch|resume|cancel|cancel-task|pending|answer|transcript"); }

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) main(process.argv.slice(2)).catch((error) => { console.error(error instanceof Error ? error.message : String(error)); process.exitCode = 1; });
