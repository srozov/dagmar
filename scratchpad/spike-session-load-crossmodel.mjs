// T1 (ticket t1) manual smoke: cross-task + cross-model session/load recall.
//
// This script is the AC5 manual validation. It is NOT part of the automated suite:
// it requires an authenticated `claude-agent-acp` and consumes Pro quota.
//
// Goal: prove that a `claude-agent-acp` session created under one model
// (sonnet) can be loaded into a fresh process running a different model
// (opus) and still recall facts the first turn deposited. This is the
// "different-model reviewer continues the builder's session" path that
// the WP1 / T1 ticket relies on for the implement → fixup → review loop.
//
// Usage:
//   1. Ensure claude-agent-acp is installed and authenticated out-of-band
//      (the MCP auth flow that dagmar does NOT participate in).
//   2. From the repo root:
//        node scratchpad/spike-session-load-crossmodel.mjs \
//          --agent /absolute/path/to/claude-agent-acp
//      (Pass the agent binary explicitly; AGENT env var also accepted.)
//
// Expected: a JSON line printed on stdout containing
//   { "recalled": true, "sessionA": "...", "sessionB": "..." }
// — `recalled` is true iff the opus process's transcript contains the
// token that the sonnet process wrote in turn 1. `sessionB` should equal
// `sessionA` (loaded, not new).

import { AcpExecutor } from "../dist/executors/acp.js";

const argv = process.argv.slice(2);
let agent;
for (let i = 0; i < argv.length; i++) {
  if (argv[i] === "--agent" && argv[i + 1]) { agent = argv[i + 1]; i++; }
  else if (!agent && argv[i] && !argv[i].startsWith("-")) { agent = argv[i]; }
}
agent = agent ?? process.env.AGENT;
if (!agent) throw new Error("Usage: node scratchpad/spike-session-load-crossmodel.mjs --agent <path/to/claude-agent-acp>");

const TOKEN = `t1smoke-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const cwd = process.cwd();
const builderEnv = { ...process.env, ANTHROPIC_MODEL: "claude-3-5-sonnet-latest" };
const reviewerEnv = { ...process.env, ANTHROPIC_MODEL: "claude-3-opus-latest" };

// Task A: sonnet builder. The first prompt asks the agent to remember a
// unique token verbatim and answer with a JSON TaskResult. Whatever acp
// session id the agent picks up here is the id we hand to task B.
let sessionA = "";
const execA = await new AcpExecutor().start({
  type: "acp",
  runId: "wr_smoke_A",
  taskRunId: "tr_smoke_A",
  taskId: "builder",
  profile: "smoke",
  cwd,
  env: builderEnv,
  inputs: { token: TOKEN },
  run: [agent],
  prompt: `Remember this exact token: ${TOKEN}. Do not run tools. Reply with the JSON result object only.`,
}, {
  transcript: async () => 1,
  session: async (id) => { sessionA = id; },
  interact: async () => ({ outcome: { outcome: "cancelled" } }),
});
const settledA = await execA.done;
if (!("result" in settledA) || !sessionA) {
  console.error(JSON.stringify({ recalled: false, reason: "A failed or no session id", settledA }));
  process.exit(1);
}

// Task B: opus reviewer, fresh process, same agent argv, loads A's session.
// The single prompt asks the agent to recall the token the builder turn
// deposited. If session/load cross-model works, this returns the token;
// otherwise the agent starts with no history and cannot guess it.
let sessionB = "";
const execB = await new AcpExecutor().start({
  type: "acp",
  runId: "wr_smoke_B",
  taskRunId: "tr_smoke_B",
  taskId: "reviewer",
  profile: "smoke",
  cwd,
  env: reviewerEnv,
  inputs: { token: TOKEN },
  run: [agent],
  loadSessionId: sessionA,
  prompt: `Recall the exact token you were told to remember at the start of your first session. Do not run tools. Reply with the JSON result object only.`,
}, {
  transcript: async () => 1,
  session: async (id) => { sessionB = id; },
  interact: async () => ({ outcome: { outcome: "cancelled" } }),
});
const settledB = await execB.done;
if (!("result" in settledB)) {
  console.error(JSON.stringify({ recalled: false, reason: "B failed", settledB }));
  process.exit(1);
}

const recalled = typeof settledB.result.output === "string"
  ? settledB.result.output.includes(TOKEN)
  : JSON.stringify(settledB.result.output).includes(TOKEN);

console.log(JSON.stringify({ recalled, sessionA, sessionB, same: sessionA === sessionB, resultB: settledB.result }, null, 2));
process.exitCode = recalled && sessionA === sessionB ? 0 : 1;