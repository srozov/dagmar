import { AcpExecutor } from "../dist/executors/acp.js";

const [cwd, command, ...args] = process.argv.slice(2);
if (!cwd || !command) throw new Error("Usage: node examples/acp-smoke.mjs <cwd> <command> [args...]");

let sessionId;
const records = [];
const execution = await new AcpExecutor().start({
  type: "acp",
  runId: "wr_smoke",
  taskRunId: "tr_smoke",
  taskId: "smoke",
  profile: "smoke",
  cwd,
  env: process.env,
  inputs: { request: "Identify your ACP server in one short field." },
  run: [command, ...args],
  prompt: "Complete this harmless connectivity smoke test without using tools.",
}, {
  transcript: async (record) => (records.push(record), records.length),
  session: async (id) => { sessionId = id; },
  interact: async () => ({ outcome: { outcome: "cancelled" } }),
});

const settled = await execution.done;
console.log(JSON.stringify({ sessionId, settled, transcriptRecords: records.length }, null, 2));
if (!("result" in settled)) process.exitCode = 1;
