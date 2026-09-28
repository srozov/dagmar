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

```bash
kill -9 "$DAEMON"                                     # simulate a crash
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

*Not run yet.* Fill in per criterion: pass/fail, run id, and the evidence (a short excerpt; raw
transcripts stay in `$T6/state`).

| # | Result | Run id | Evidence |
|---|---|---|---|
| 1 | | | |
| 2 | | | |
| 3 | | | |
| 4 | | | |
| 5 | | | |
| 6 | | | |

Findings, surprises and fold-backs:

- (none yet)
