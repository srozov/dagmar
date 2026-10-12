# Dagmar

**Dagmar** /ˈdɑːɡ.mɑːr/ — from Old Norse *dag* (day) and *mær* (maiden): the orchestrator of purposeful flow.

**D A G M A R** stands for **Directed Acyclic Graph Manager And Runner**.

Light agentic workflow orchestration for the command line. Dagmar speaks YAML for workflow definitions and ACP for agent execution.

Define an explicit task graph in YAML. Dagmar starts dependency-ready tasks through ACP agents or local processes, then gives you a CLI to start, inspect, watch, resume, and cancel runs. It records run state in SQLite and streams control-plane events over loopback WebSocket JSON-RPC.

Dagmar coordinates work; agents retain their own models, tools, skills, sessions, and reasoning. You can run a complex internal loop inside one agent task, then use Dagmar to record that turn in a larger graph and pass declared JSON outputs across dependency edges.

Use Dagmar when you need durable task state, explicit handoffs, and operator control around a small set of agent or process steps.

## What Dagmar provides

- A small YAML language for static task graphs, with guards, human gates, and bounded reruns.
- ACP and local-process executors, declared in each workflow.
- Concurrent execution of independent ready tasks.
- A CLI and loopback API for run control, live observation, and ACP interactions.
- A SQLite run ledger and an append-only JSONL transcript for each task attempt.

## Requirements

- Node.js 24 (the supported range is `>=24 <25`)
- pnpm 12

## Install and build

```sh
pnpm install --frozen-lockfile
pnpm build
```

For development, run `pnpm check` for type checking and `pnpm test` for the test suite.

## Configure Dagmar

The daemon reads `~/.config/dagmar/config.yaml` by default. Pass `--config <path>` to `dagmard` or `dagmar` to use another file.

```yaml
workflowDir: /absolute/path/to/workflows
storageDir: /absolute/path/to/dagmar-state

listen:
  host: 127.0.0.1
  port: 7331
```

Both directory paths must be absolute. The server accepts only the loopback hosts `127.0.0.1` and `::1`. The daemon reads the config once at start. Executors are not configured here: each workflow declares its own.

## Define a workflow

Place YAML files in `workflowDir`. A workflow has a unique ID, the executors its tasks use, and a non-empty mapping of task IDs to tasks:

```yaml
id: process-example

executors:
  local-process:
    type: process
  claude-acp:
    type: acp
    run: [/absolute/path/to/claude-agent-acp]
    model: sonnet

tasks:
  collect:
    executor: local-process
    inputs:
      request: $run.input
    run: [node, scripts/collect.mjs]

  assess:
    executor: claude-acp
    dependsOn: [collect]
    inputs:
      collected: $tasks.collect.output
    prompt: |
      Assess the collected information.
    session:
      mode: fresh
```

An executor has a `type`, `process` or `acp`, and may set:

- `cwd`, an absolute path where its tasks run. Without one, they run in the run's directory, given when the run starts (`--cwd`). A workflow that leaves `cwd` out can serve any repository of its kind; one that always works in one place sets it.
- `env`, string values added to the daemon's environment.
- `envFile`, the absolute path of a `KEY=VALUE` file read each time one of the executor's tasks starts. Its values go between the daemon's environment and `env`, so a secret stays out of the workflow file and reaches only the executors that name the file. An unreadable file fails the task with `env_file_unreadable`.

An ACP executor also requires `run`, the command that starts its agent, and may set `model`. Dagmar applies the model through the agent's ACP model selector (`session/set_config_option`) on every new or loaded session, and fails the attempt if the agent has no model selector (`acp_model_unsupported`), does not offer that exact value (`acp_model_unknown`, the error lists the offered values), or does not switch (`acp_model_not_applied`).

An ACP executor may likewise set `mode`, applied the same way through the agent's ACP mode selector (the config option with category `mode`), failing with `acp_mode_unsupported`, `acp_mode_unknown` or `acp_mode_not_applied`. For `claude-agent-acp` the mode is its permission mode (for example `acceptEdits` accepts file edits without asking). Executors that differ only in `model` or `mode` are the same agent, so one can `continue` the other's session.

To run the same tasks with other executors, for example another provider for the agents that share a session, write a second workflow file. Dagmar reads workflows from disk on every step, so a new or edited workflow needs no restart; an edit also applies to the workflow's active runs from their next task, so do not change a workflow's executors while it has active runs.

A task's `executor` names one of its workflow's executors. A process task requires `run` and cannot use `prompt`, `session` or `interactive`; an ACP task requires `prompt` and cannot use `run`. Dependencies must exist and form an acyclic graph. Independent dependency-ready tasks run concurrently.

Input values may be literals or references:

- `$run.input` and `$run.input.key` read the input supplied when the run starts.
- `$tasks.<task-id>.output` and `$tasks.<task-id>.output.key` read a dependency's output.

A task may also declare `outputSchema`, a JSON Schema applied to its `output`.

An ACP task starts a new agent session unless it sets `session: { mode: continue, from: <task-id> }`, which loads that task's session through ACP `session/load`. The source must be a dependency (`invalid_dependency`) and an ACP task whose executor has a byte-identical `run` (`invalid_task`); `model` and `mode` may differ. The attempt fails with `acp_load_unsupported` if the agent cannot load sessions, or `continuation_unavailable` if the source has no recorded session (for example, it was skipped). `session: { mode: fork }` is rejected (`unsupported_session_mode`).

An ACP task with `interactive: true` is a human-driven conversation. Each agent reply that is not a result parks the task as `awaiting_input` with a `turn` interaction carrying the reply in `request.message`; answer with the next message as a non-empty JSON string, for example `dagmar answer <interaction-id> --json '"Add a test"'`. The task ends when the agent replies with a result. Interactive tasks are live-only: a daemon restart fails the attempt with `executor_lost`, and `resume` starts a new attempt.

A task with `gate: { prompt, schema? }` instead of `executor` waits for a human decision; it cannot use `prompt`, `run`, `session`, `interactive`, `outputSchema` or `rerun`. When it becomes ready it parks as `awaiting_input` with a `gate` interaction showing its prompt and schema. `dagmar answer <interaction-id> --json <value>` completes it with that value as its `output`; a value that does not match `schema` is rejected and the gate stays pending. A pending gate is persisted and survives a daemon restart, after which `dagmar pending` lists it under a new interaction ID.

`when` guards a task with a list of clauses, all of which must hold. Each clause has a `ref` to `$run.input…` or `$tasks.<dependency>.output…` and exactly one of `equals` (a JSON value) or `in` (a non-empty array of JSON values), compared by JSON equality; other shapes fail validation with `invalid_guard`. If a clause is false when the task's dependencies settle, the task is `skipped` instead of run. A missing path, or a reference to a skipped task, makes a clause false. A skipped task counts as settled for its dependants, but an unguarded input reference to its output fails with `input_resolution_failed`. A run completes when every task is `completed` or `skipped`.

`rerun: { from, max, atMax?, when? }` makes a task (the source) rerun the run from `from`, a task upstream of it (`invalid_rerun` otherwise). The rerun is not a dependency edge, so the graph stays acyclic. Each time the source completes and asks for a rerun, `from` runs again, and every task downstream of it that was already decided is decided again, in dependency order, on the new outputs, the source included. Nothing waits for a rerun: a task is decided as soon as its dependencies are, and decided again after each rerun, so guard the tasks after `from` that must not run on an intermediate result (for example `passed = true` after a `verify` that `fixup` reruns from). Several tasks may rerun from the same task.

Without `rerun.when`, every completion asks for a rerun. `rerun.when` is a list of clauses like `when`'s, read from the source's own output once it has completed (`$tasks.<source>.output…` only, `invalid_rerun` otherwise); the source asks for a rerun only when all of them hold, and otherwise the run goes on to the tasks after it.

A source asks for at most `max` reruns per run; only completed attempts that asked count, and `resume` continues the count. A rerun asked for beyond `max` is not done. By default the source's attempt then ends `blocked` with `rerun_exhausted`, and so does the run; with `atMax: proceed` the run goes on to the tasks after the source instead. Without `rerun.when`, Dagmar knows before the source runs that it would ask, so a source that has used up `max` is not run again: its attempt is `blocked`, or `skipped` with `atMax: proceed`. With `rerun.when`, the source runs and its completed attempt is `blocked`, keeping its output, or stays `completed` with `atMax: proceed`.

In a review workflow, `fixup` reruns from `verify` while verification fails; `changed`, a check after the reviews, reruns from `verify` when they changed something (`rerun.when` on its `changed` output, and `atMax: proceed`, so the gate gets the change at the max); and `revise` after the gate reruns from `verify` too. Each new commit goes through verification and the reviews again.

In `examples/review-iteration-loop.yaml`, `fixup` continues `implement`'s session and reruns from `verify` while verification fails, at most three times:

```yaml
  fixup:
    executor: builder
    dependsOn: [verify, implement]
    inputs: { report: $tasks.verify.output }
    prompt: |
      Verification failed; the report is in the inputs. Fix the change so the checks pass.
    session: { mode: continue, from: implement }
    when: [{ ref: $tasks.verify.output.passed, equals: false }]
    rerun: { from: verify, max: 3 }
```

The same workflow also uses a gate and an interactive task. `docs/mvp-acceptance.md` is its runbook, including its sharp edges.

## Process task contract

Dagmar writes the resolved task inputs as one JSON value to the process's standard input. The process must write exactly one JSON result to standard output and exit with status 0:

```json
{
  "outcome": "completed",
  "message": "Human-readable summary",
  "output": {}
}
```

`outcome` may be `completed` or `blocked`; all three fields are required. Standard error is captured in the task transcript.

An ACP task's agent returns the same JSON result as its final message; earlier messages in the turn (progress notes between tool calls) are ignored. If that final message is not a valid result, Dagmar sends one repair prompt on the same session asking for only the corrected result (transcript event `acp_result_repair`). A second invalid result fails the attempt. Interactive tasks treat a non-result reply as conversation instead.

The repository includes a runnable process example at `examples/process-workflow.yaml`. Its helper program is `examples/process-result.mjs`.

## Run the daemon and CLI

Start the daemon in one terminal:

```sh
pnpm dev:daemon -- --config /absolute/path/to/config.yaml
```

Then use the CLI from another terminal:

```sh
pnpm dev:cli -- --config /absolute/path/to/config.yaml ping
pnpm dev:cli -- --config /absolute/path/to/config.yaml workflows
pnpm dev:cli -- --config /absolute/path/to/config.yaml run process-example --cwd . --input-json '{"request":"hello"}'
```

`run <workflow-id> [--cwd <dir>] [--input <text> | --input-json <json>]` starts a run. `--cwd` gives the run's directory for every executor without its own `cwd`; a relative path resolves in the shell's current directory, and the daemon receives an absolute path (`run.start`'s `cwd` param). Starting fails with `cwd_required` if an executor needs a directory and none is given, with `cwd_unused` if every executor has its own, and with `invalid_params` if the path is not an existing directory. A workflow has at most one active run per directory (`active_run_exists`), so it can run on several repositories at once; runs without a directory count as one. `runs` and `status` show each run's `cwd`.

The CLI also supports `workflow <id>`, `runs`, `status <run-id>`, `watch <run-id>`, `resume <run-id>`, `cancel <run-id>`, `cancel-task <task-run-id>`, `pending`, `answer <interaction-id> --json <value>`, and `transcript <task-run-id> [--after-line <number>]`.

The daemon persists a SQLite ledger and JSONL task-attempt transcripts under `storageDir`.

## v0 scope

Dagmar supports static task graphs with `when` guards, human gates, interactive ACP tasks and bounded reruns; local-process and ACP execution, including continuing an earlier task's ACP session; manual cancellation and resume; and ACP permission/input interactions. It intentionally does not include scheduling, automatic retries or timeouts, concurrency limits, dynamic tasks, session forking, distributed workers, or automatic redaction of child-process output.
