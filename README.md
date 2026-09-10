# Dagmar

**DAGMAR** stands for **Directed Acyclic Graph Manager And Runner**.

Light agentic workflow orchestration for the command line. Dagmar speaks YAML for workflow definitions and ACP for agent execution.

Define an explicit task graph in YAML. Dagmar starts dependency-ready tasks through ACP agents or local processes, then gives you a CLI to start, inspect, watch, resume, and cancel runs. It records run state in SQLite and streams control-plane events over loopback WebSocket JSON-RPC.

Dagmar coordinates work; agents retain their own models, tools, skills, sessions, and reasoning. You can run a complex internal loop inside one agent task, then use Dagmar to record that turn in a larger graph and pass declared JSON outputs across dependency edges.

Use Dagmar when you need durable task state, explicit handoffs, and operator control around a small set of agent or process steps.

## What Dagmar provides

- A small YAML language for static dependency graphs.
- ACP and local-process executors behind named profiles.
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

executors:
  local-process:
    type: process
    cwd: /absolute/path/to/working-directory
    env: {}

  claude-acp:
    type: acp
    cwd: /absolute/path/to/working-directory
    run: [/absolute/path/to/claude-agent-acp]
    env: {}
```

All directory paths must be absolute. The server accepts only the loopback hosts `127.0.0.1` and `::1`. Each executor profile provides its working directory and environment overrides; ACP profiles also require the command used to start their agent.

## Define a workflow

Place YAML files in `workflowDir`. A workflow has a unique ID and a non-empty mapping of task IDs to tasks:

```yaml
id: process-example

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

`executor` selects a configured profile. A process task requires `run` and cannot use `prompt` or `session`; an ACP task requires `prompt` and cannot use `run`. Dependencies must exist and form an acyclic graph. Independent dependency-ready tasks run concurrently.

Input values may be literals or references:

- `$run.input` and `$run.input.key` read the input supplied when the run starts.
- `$tasks.<task-id>.output` and `$tasks.<task-id>.output.key` read a dependency's output.

A task may also declare `outputSchema`, a JSON Schema applied to its `output`. Only `session: { mode: fresh }` is supported in v0.

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
pnpm dev:cli -- --config /absolute/path/to/config.yaml run process-example --input-json '{"request":"hello"}'
```

The CLI also supports `executors`, `workflow <id>`, `runs`, `status <run-id>`, `watch <run-id>`, `resume <run-id>`, `cancel <run-id>`, `cancel-task <task-run-id>`, `pending`, `answer <interaction-id> --json <value>`, and `transcript <task-run-id> [--after-line <number>]`.

The daemon persists a SQLite ledger and JSONL task-attempt transcripts under `storageDir`.

## v0 scope

Dagmar supports static dependency-only DAGs, local-process and ACP execution, manual cancellation and resume, and ACP permission/input interactions. It intentionally does not include scheduling, automatic retries or timeouts, concurrency limits, dynamic tasks, distributed workers, or automatic redaction of child-process output.
