// T7 manual probe: what does an ACP agent advertise? NOT part of the automated suite.
//
// Spawns the agent, sends `initialize` then `session/new` (no prompt, so no quota), prints the
// protocol version (and whether it matches dagmar's SDK), agentCapabilities, and the
// `category: "model"` config option dagmar selects models from; then kills the agent.
//
// Usage (from the repo root):
//   node scratchpad/probe-acp-agent.mjs --agent /absolute/path/to/claude-agent-acp/dist/index.js

import { spawn } from "node:child_process";
import { Readable, Writable } from "node:stream";
import { client, methods, ndJsonStream, PROTOCOL_VERSION } from "@agentclientprotocol/sdk";

const argv = process.argv.slice(2);
const flag = argv.indexOf("--agent");
const agent = flag >= 0 ? argv[flag + 1] : process.env.AGENT;
if (!agent) throw new Error("Usage: node scratchpad/probe-acp-agent.mjs --agent <path/to/claude-agent-acp>");

const run = agent.endsWith(".js") ? [process.execPath, agent] : [agent];
const child = spawn(run[0], run.slice(1), { stdio: ["pipe", "pipe", "inherit"] });
try {
  const stream = ndJsonStream(Writable.toWeb(child.stdin), Readable.toWeb(child.stdout));
  const connection = client({ name: "dagmar-probe" }).connect(stream);
  const initialized = await connection.agent.request(methods.agent.initialize, {
    protocolVersion: PROTOCOL_VERSION,
    clientCapabilities: { elicitation: { form: {} } },
    clientInfo: { name: "dagmar-probe", version: "0.0.0" },
  });
  const session = await connection.agent.request(methods.agent.session.new, { cwd: process.cwd(), mcpServers: [] });
  console.log(JSON.stringify({
    protocolVersion: initialized.protocolVersion,
    dagmarProtocolVersion: PROTOCOL_VERSION,
    protocolMatches: initialized.protocolVersion === PROTOCOL_VERSION,
    agentInfo: initialized.agentInfo,
    agentCapabilities: initialized.agentCapabilities,
    modelOption: session.configOptions?.find((o) => o.category === "model") ?? null,
  }, null, 2));
} finally {
  child.kill();
}
