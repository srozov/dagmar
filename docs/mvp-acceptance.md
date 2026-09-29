# MVP acceptance: the review-iteration loop

Runbook and results for running [`examples/review-iteration-loop.yaml`](../examples/review-iteration-loop.yaml)
end to end on real ACP agents. It proves (or disproves) the review-iteration MVP:

| # | Criterion | Shown by |
|---|---|---|
| 1 | A real coding task runs implement → verify → fixup (loop) → review → gate → done; run `completed` | Main run |
| 2 | The loop iterates (verify fails, fixup, verify again) and exits on pass, or stops at `maxVisits` | Main or seeded run |
| 3 | The reviewer (different model) references builder-session content it was never re-fed | Main run, reviewer result |
| 4 | Killing `dagmard` mid-loop → `blocked` → restart → `resume` → loop continues, visit count kept | Restart run |
| 5 | Gate `revise` → human-driven multi-turn conversation with the builder → run `completed` | Revise run |
| 6 | No new `src` for the loop; `pnpm check` / `test` / `build` green | Final check |

## How the workflow works

```
implement ─► verify ─┬─ passed=false ─► fixup ──(loop, maxVisits 3)──► verify
  (builder)  (check) │                  (builder, continues implement)
                     ├─ passed=true ──► review ─► gate ─┬─ approve ─► done
                     │                 (reviewer,       └─ revise ──► revise (builder, interactive)
                     │                  continues implement)
                     └─ passed=false after last visit ─► exhausted (run ends blocked)
```

- **Context** travels through the builder's ACP session: `fixup`, `review` and `revise` all
  `continue from implement` (dagmar `session/load`s its session id). **Code** travels through the
  shared working tree.
- `review` gets no builder notes in its prompt or inputs, so whatever it says about the builder's
  reasoning must come from the loaded session (criterion 3).
- `review`, `gate` and `exhausted` depend on `verify`, the loop target, so dagmar holds them until
  the loop is final, then decides them once.

## Sharp edges

- **`builder.run` and `reviewer.run` must be byte-identical.** A session can only be continued by the
  same agent, and dagmar compares the `run` argv exactly. Select the model with the profile's `model`
  field, never with an argv flag.
- **`model` must be a value the agent offers, exactly.** Otherwise the task fails with
  `acp_model_unknown`, which lists the valid values. With `claude-agent-acp` 0.84.0 on this account:
  `default` (Opus 5.5), `opus` (Opus 5.5), `sonnet` (Sonnet 5.5), `haiku` (Haiku 4.5), plus pinned
  IDs such as `claude-fable-5-1`, `claude-sonnet-5` and `claude-opus-4-8`. `default` and `opus` are the
  same model, so the builder uses `sonnet` to keep the reviewer a different model.
- **All three profiles share one `cwd`**, the target repo, or verify and review won't see the builder's
  files.
- **`revise` is live-only.** Restarting the daemon during `revise` fails it (`executor_lost`); that is
  by design (T2). Do the restart test during the loop.
- **Use a separate config.** Don't add these profiles to `~/.config/dagmar/config.yaml`; run the
  acceptance daemon with its own config, storage and port.

## Setup

Prerequisites: Node 24, `pnpm`, an authenticated `claude-agent-acp` 0.84.0, and dagmar built
from a branch that has the profile `model` field (`pnpm install --frozen-lockfile && pnpm build`).

```bash
DAGMAR=/home/agi01/dagmar                      # this checkout
T6=$HOME/.local/state/dagmar-t6                 # acceptance config, workflows, state
TARGET=/absolute/path/to/target-worktree        # repo the agents will change
mkdir -p "$T6/workflows" "$T6/state"
```

**Agent.** `claude-agent-acp` is pinned in its own install outside the repo (not global, not a
dagmar dependency); it reuses the host's Claude login (`~/.claude`):

```bash
mkdir -p ~/.local/share/dagmar-agents && cd ~/.local/share/dagmar-agents
echo '{"private":true,"dependencies":{"@agentclientprotocol/claude-agent-acp":"0.84.0"}}' > package.json
pnpm install
node "$DAGMAR/scratchpad/probe-acp-agent.mjs" --agent "$PWD/node_modules/@agentclientprotocol/claude-agent-acp/dist/index.js"
```

The probe prints the protocol version (must match dagmar's), `loadSession: true`, and the offered
model values.

**Target repo.** A git worktree with a working `pnpm test`. Confirm the baseline is green before
starting (a red baseline makes verify meaningless):

```bash
git -C "$DAGMAR" worktree add "$TARGET" -b claude/t6-acceptance-target main
(cd "$TARGET" && pnpm install --frozen-lockfile && pnpm test)
```

**Agent permissions.** `claude-agent-acp` asks before editing files or running commands; each ask
parks the task as `awaiting_permission` until answered with `dagmar answer`. The builder profile sets
`mode: acceptEdits` (see Config), so file edits go through without asking. The mode is only the
baseline: the agent also honors the host's own Claude Code permission rules in `~/.claude/settings.json`
(deny, then ask, then allow; ask and deny rules apply in every mode). On this host `Bash` is allowed
and `Bash(rm *)` asks, so builder commands run without asking and an `rm` parks for `dagmar answer`.
The reviewer pins `mode: default`, so an edit by the reviewer would park as a permission ask. (Each
attempt is a fresh agent process, and a loaded session starts in the host's `defaultMode`, not the
builder's mode; pinning it keeps the reviewer independent of host settings.) No
settings file is written into the target: a target `.claude/settings*.json` would apply to every
profile working in it.

**Config** — `$T6/config.yaml` (machine-local, not committed):

```yaml
workflowDir: /home/agi01/.local/state/dagmar-t6/workflows
storageDir: /home/agi01/.local/state/dagmar-t6/state
listen: { host: 127.0.0.1, port: 7341 }
executors:
  builder:
    type: acp
    cwd: /absolute/path/to/target-worktree
    run: [node, /home/agi01/.local/share/dagmar-agents/node_modules/@agentclientprotocol/claude-agent-acp/dist/index.js]
    env: {}
    model: sonnet
    mode: acceptEdits
  reviewer:
    type: acp
    cwd: /absolute/path/to/target-worktree
    run: [node, /home/agi01/.local/share/dagmar-agents/node_modules/@agentclientprotocol/claude-agent-acp/dist/index.js]
    env: {}
    model: opus
    mode: default
  check:
    type: process
    cwd: /absolute/path/to/target-worktree
    env: {}
```

**Workflow** — copy it and fill in the verify wrapper's absolute path:

```bash
sed "s#/ABSOLUTE/PATH/TO/dagmar#$DAGMAR#" "$DAGMAR/examples/review-iteration-loop.yaml" > "$T6/workflows/review-iteration-loop.yaml"
```

Check the wrapper on its own (it prints exactly one envelope on stdout):

```bash
echo '{}' | node "$DAGMAR/examples/verify.example.mjs" node -e "process.exit(1)"
```

Expect `"passed":false`; with `process.exit(0)` expect `"passed":true`.

## Run

```bash
dagmar() { node "$DAGMAR/dist/cli.js" --config "$T6/config.yaml" "$@"; }
node "$DAGMAR/dist/daemon.js" --config "$T6/config.yaml" >"$T6/daemon.log" 2>&1 & DAEMON=$!
dagmar workflows                                      # review-iteration-loop listed, no errors
dagmar run review-iteration-loop --input-json '{"task":"<the coding task>"}'
dagmar watch <runId>                                  # or: dagmar status <runId>
dagmar pending                                        # permission asks, the gate, revise turns
dagmar transcript <taskRunId>                         # full ACP / process traffic for one attempt
```

What to check while it runs:

- Every ACP attempt's transcript has an `acp_model_selected` event with the expected model (builder
  `sonnet`, reviewer `opus`) and an `acp_mode_selected` event (builder `acceptEdits`, reviewer
  `default`).
- `fixup` and `review` transcripts show `session/load` with `implement`'s session id (the
  `acpSessionId` on implement's attempt in `dagmar status`).
- `verify` attempts alternate with `fixup` attempts while the tests fail.

**Gate.** When the run is `waiting` with a `gate` interaction, read the review (`dagmar status`, the
`review` result), then decide:

```bash
dagmar answer <interactionId> --json '{"decision":"approve"}'   # → done runs, run completed
```

## Revise run (criterion 5)

Start another run (same or small follow-up task) and answer the gate with
`{"decision":"revise"}`. `revise` then alternates: the agent replies, the task parks with a `turn`
interaction, and the human's next message goes in as a JSON string:

```bash
dagmar pending                                        # kind "turn"; request.message = agent's reply
dagmar answer <interactionId> --json '"Rename X to Y and add a test for the empty case"'
```

Repeat for at least two turns. The task ends when the agent returns its result envelope; `done`
shows `skipped` and the run `completed`.

## Restart run (criterion 4)

The loop has to iterate. If the task's first verify tends to pass, force one failure by restarting
the daemon with the seeded-failure env on the `check` profile:

```yaml
  check:
    type: process
    cwd: /absolute/path/to/target-worktree
    env: { VERIFY_FAIL_FIRST: "1", VERIFY_COUNTER_FILE: /home/agi01/.local/state/dagmar-t6/verify-count }
```

The daemon reads its config at start, so restart it after editing, and delete the counter file before
each seeded run. Then, while `fixup` is running:

Dagmar starts agents as separate process groups, so killing the daemon does not necessarily kill the
running `fixup` agent. Note the agent's PID **before** the kill (the CLI needs a running daemon): it
is the `pid` of the `acp_process_started` event in the fixup attempt's transcript.

```bash
dagmar transcript <fixupTaskRunId> | grep -o '"acp_process_started"[^}]*'   # → "data":{"pid":<agentPid>
kill -9 "$DAEMON"                                     # simulate a crash
ps -o pid,cmd -p <agentPid>                           # still alive = orphan
```

An orphan can keep editing the worktree while the resumed run starts a second agent, so record it as a
finding and stop it before resuming: `kill <agentPid>` (only that PID; never a pattern match).

Then restart and resume:

```bash
node "$DAGMAR/dist/daemon.js" --config "$T6/config.yaml" >>"$T6/daemon.log" 2>&1 & DAEMON=$!
dagmar status <runId>                                 # expect blocked; fixup attempt failed: executor_lost
dagmar resume <runId>                                 # fixup re-runs, verify re-runs, loop exits
```

Check: the new fixup attempt still loads `implement`'s session; completed fixup attempts never exceed
`maxVisits` (3); the run reaches the gate.

## Error codes you may see

| Code | Meaning |
|---|---|
| `acp_model_unknown` | `model` not offered by the agent; the message lists valid values |
| `acp_model_unsupported` / `acp_model_not_applied` | agent has no model selector / didn't switch |
| `acp_mode_unknown` / `acp_mode_unsupported` / `acp_mode_not_applied` | the same checks for the profile's `mode` |
| `acp_load_unsupported` | agent can't `session/load`, so it can't continue a session |
| `acp_authentication_required` | authenticate `claude-agent-acp` out-of-band |
| `continuation_unavailable` | the source task has no persisted session id |
| `executor_lost` | the daemon died while the attempt ran; `resume` the blocked run |
| `invalid_task` … `continue must target the same agent` | builder/reviewer `run` differ |

## Results

Run 2026-09-28 to 2026-09-30 on `claude-agent-acp` 0.84.0: builder `sonnet` + `acceptEdits`, reviewer
`opus` + `default`. The target was OpsDash's T6 integration plan, one milestone slice per run, in a
worktree on branch `claude/t6-integration`, with verify = `bun run typecheck && bun run build && bun
test --pass-with-no-tests`. Raw transcripts are in `~/.local/state/dagmar-t6/state`.

| Run | Task | Outcome |
|---|---|---|
| `wr_65aa29e4-8937-426a-b645-a17ad0f2f9b6` | OpsDash M1 | `completed` via gate → revise (1 human turn) |
| `wr_9dac61cb-53fe-455b-b188-e08d1a72508e` | OpsDash M2 | `completed` via gate → revise (1 human turn) |
| `wr_1d091387-70bb-4b9b-b006-ca37e2b32fdc` | OpsDash M3, first commit; seeded verify failure + restart | `completed` via gate → revise (2 human turns) |

| # | Result | Run id | Evidence |
|---|---|---|---|
| 1 | Pass | all three | Each ran implement → verify → review → gate → revise to `completed`. Approve → `done` was not taken live (it is covered by the dry run). |
| 2 | Pass (mechanics only) | `wr_1d091387…` | With `VERIFY_FAIL_FIRST=1`: verify #1 `passed:false` (seeded), then fixup #1 continued the implement session and changed nothing ("the report is marked seeded … its own summary shows every check passing"), then verify #3 passed, fixup #2 was `skipped` and the loop exited at 1/3 visits. No verify failed for real in three milestones, so an agent fixing a genuine failure was not observed. |
| 3 | Pass | all three | The reviewer's prompt had no builder notes, yet it answered them point by point. M1: it took a position on the builder's doubt about S2's counts, which appears in no file or commit. M3: it found a real bug the builder's tests hid (skipped tasks always have attempt rows, so the inspector never showed their note). |
| 4 | Pass | `wr_1d091387…` | `kill -9` of the daemon one loop step in: the interrupted verify #2 became `failed (executor_lost)` and the run `blocked`. After restart, `resume` re-ran only verify; fixup's completed visit was kept, the loop exited, and the run went on to review and gate. No orphaned agent or verify process remained. |
| 5 | Pass | all three | `revise` parked as `turn` interactions answered with `dagmar answer`. In M3 the builder asked for scope, made five commits, then asked "Are you satisfied?" before finishing. |
| 6 | Pass | — | `src` is 2,308 lines (budget 3–4k); dagmar `pnpm check` / `test` (66 pass) / `build` green. OpsDash after M3a: typecheck, build and 19 tests green, re-run independently of the builder. |

Findings, surprises and fold-backs:

- **Fold-back 1, fixed (`claude/acp-last-message`):** the executor parsed all agent text of a turn
  as the result. A tool-using agent sends progress notes between tool calls, so a correct final JSON
  failed with "exactly one JSON value" (`wr_f1484d0c…`, M1 attempt 1, work done). Only the last
  message (by ACP `messageId`) is parsed now.
- **Fold-back 2, built (`claude/acp-result-repair`):** an invalid single-shot result gets one repair
  turn on the same session. In `wr_4548a0c6…` the repair fired but the builder repeated the same
  mistake: it nested `notes` one level deeper and closed with one `}` too few. Both it and
  `wr_bfdcd3dd…` lost a finished M2. Requiring a flat result shape in the task text is what made
  the result parse first time.
- Shell commands that write files or chain steps (heredocs, `cd … &&`, `rm`, `kill`) asked for
  permission despite `mode: acceptEdits`: 3–5 per milestone, all in scope. The operator answered
  them; there is no "always allow" option.
- `wr_bfdcd3dd…`: the first prompt failed with an OAuth token refresh collision with other Claude
  Code processes on the host (environmental). The run blocked cleanly and `resume` retried it.
- `kill -9` left no orphans: the agent and the verify wrapper exit when their pipes to the daemon
  close. The restart step still checks the recorded agent PID.
- The fixup for a seeded failure took 7 s, so a kill timed "5 s into fixup" hit the next verify
  instead. That is still a crash mid-loop.
- Not done: no browser check of the OpsDash pages (the fixture is only reachable over `wss://` with
  a `tailscale serve` mapping, which the task forbade).
