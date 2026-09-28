// Verify gate for examples/review-iteration-loop.yaml.
//
//   node verify.example.mjs [command...]      (default: pnpm test)
//
// Runs the command in the task's cwd and prints exactly one result envelope on stdout:
//   {"outcome":"completed","message":"...","output":{"passed":<exit code 0>,"exitCode":n,"summary":"<tail>"}}
// A failing test run is a normal `passed: false` result (it drives the fixup loop). The command's
// own output goes to stderr, which dagmar keeps in the task transcript. If the command cannot be
// started, this script exits non-zero without an envelope, so the attempt fails and the run blocks.
//
// Seeded failures (acceptance restart test only): with VERIFY_FAIL_FIRST=<n> and
// VERIFY_COUNTER_FILE=<absolute path>, the first n invocations report passed:false regardless of
// the command's result.
import { spawn } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";

for await (const _ of process.stdin); // inputs are unused; drain stdin so dagmar's write succeeds

const [command, ...args] = process.argv.length > 2 ? process.argv.slice(2) : ["pnpm", "test"];
const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"] });
let output = "";
for (const stream of [child.stdout, child.stderr]) {
  stream.on("data", (chunk) => {
    process.stderr.write(chunk);
    output += chunk;
  });
}
const exitCode = await new Promise((resolve, reject) => {
  child.once("error", reject);
  child.once("close", (code) => resolve(code ?? 1));
});

let passed = exitCode === 0;
let seeded = false;
const failFirst = Number(process.env.VERIFY_FAIL_FIRST ?? 0);
if (failFirst > 0) {
  const file = process.env.VERIFY_COUNTER_FILE;
  if (!file) throw new Error("VERIFY_FAIL_FIRST requires VERIFY_COUNTER_FILE");
  let count = 0;
  try { count = Number(readFileSync(file, "utf8")); } catch {}
  writeFileSync(file, String(count + 1));
  if (count < failFirst) { passed = false; seeded = true; }
}

const summary = output.split("\n").slice(-40).join("\n");
const message = seeded ? "verify failed (seeded)" : passed ? "verify passed" : `verify failed (exit ${exitCode})`;
console.log(JSON.stringify({ outcome: "completed", message, output: { passed, exitCode, seeded, summary } }));
