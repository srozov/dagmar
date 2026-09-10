# Dagmar v0 — Implementation Plan

## Objective

Build the smallest useful headless DAG runtime in TypeScript.

Dagmar loads static YAML workflows, runs dependency-ready tasks concurrently,
executes tasks through either ACP or a direct local process, validates a
universal task result, and persists run and attempt state in SQLite. A
JSON-RPC 2.0 API over one loopback WebSocket exposes everything needed by the
CLI and a future OpsDash DAG view.

## v0 boundaries

Implement:

- static dependency-only DAGs;
- ACP and direct-process executors;
- parallel execution of independent ready tasks;
- a two-table SQLite ledger;
- one append-only JSONL transcript per task attempt;
- manual cancellation and resume;
- ACP permission/input handling through the CLI;
- a manually started daemon, WebSocket JSON-RPC API, and thin CLI.

Do not implement:

- cron or scheduling;
- automatic retries or task timeouts;
- conditions, dynamic tasks, loops, or fan-out;
- concurrency limits;
- workflow definition snapshots;
- artifacts, workflow memory, or mutable shared context;
- session continuation/forking;
- reconnecting to ACP sessions after daemon restart;
- tmux, containers, workers, queues, or distributed locking;
- an OpenClaw plugin, OpsDash UI, or domain-specific integration;
- automatic secret redaction of child-process output.

## 1. Create the package

Create one package:

```text
dagmar/
  package.json
  tsconfig.json
  result.schema.json
  workflow.schema.json
  src/
    cli.ts
    daemon.ts
    config.ts
    rpc.ts
    workflow.ts
    scheduler.ts
    store.ts
    transcript.ts
    result.ts
    executors/
      types.ts
      acp.ts
      process.ts
  examples/
```

Runtime dependencies:

- `@agentclientprotocol/sdk`
- `ajv`
- `yaml`
- `ws`

Development dependencies:

- `typescript`
- `tsx`
- required Node and WebSocket type packages only

Use Node built-ins for SQLite, child processes, files, UUIDs, and signals. Add
scripts for `tsc` compilation and `tsx` development execution. Do not add a
bundler, web framework, ORM, CLI framework, or executor framework.

## 2. Define and validate the contracts

### Universal task result

Create `result.schema.json` for exactly:

```json
{
  "outcome": "completed | blocked",
  "message": "Human-readable summary",
  "output": {}
}
```

The JSON Schema must require all three fields, reject additional fields, allow
only `completed` or `blocked` as `outcome`, require a non-empty `message`, and
accept any JSON value for `output`.

Compile the base schema once with Ajv. When a task declares `outputSchema`,
validate `output` against it in addition to the universal envelope.

### Workflow definition

Support this shape:

```yaml
id: example

tasks:
  collect:
    executor: local-process
    run: [node, scripts/collect.mjs]
    inputs:
      request: $run.input
    outputSchema:
      type: object

  assess:
    executor: claude-acp
    dependsOn: [collect]
    prompt: |
      Assess the collected information using the supplied inputs.
    inputs:
      collected: $tasks.collect.output
    session:
      mode: fresh
```

`tasks` is a mapping keyed by task ID. Do not duplicate the ID inside a task.
Allowed task fields are only:

- `executor`
- `dependsOn`
- `inputs`
- `prompt`
- `run`
- `outputSchema`
- `session`

Validate files during discovery:

- workflow ID is present and unique;
- `tasks` is non-empty and has no duplicate YAML keys;
- referenced executor profiles exist;
- process tasks have a non-empty `run: string[]` and no `prompt`;
- ACP tasks have a non-empty `prompt` and no `run`;
- dependencies exist, are not self-references, and form an acyclic graph;
- `$tasks.*` references point only to declared dependencies;
- `outputSchema` is valid JSON Schema;
- unknown fields are rejected;
- only `session: { mode: fresh }` is accepted in v0. Recognize `continue` and
  `fork` as reserved modes but reject them explicitly as unsupported.

Multiple root tasks and multiple terminal tasks are valid.

## 3. Load machine-local configuration

Read `~/.config/dagmar/config.yaml`, overridable with
`dagmard --config <path>`:

```yaml
workflowDir: /absolute/path/to/workflows
storageDir: /absolute/path/to/dagmar-state

listen:
  host: 127.0.0.1
  port: 7331

executors:
  local-process:
    type: process
    cwd: /absolute/default/working/directory
    env: {}

  claude-acp:
    type: acp
    cwd: /absolute/working/directory
    run: [command, that, starts, claude-agent-acp]
    env: {}

  openclaw-acp:
    type: acp
    cwd: /absolute/working/directory
    run: [openclaw, acp]
    env: {}
```

Require absolute `workflowDir`, `storageDir`, and executor `cwd` paths. ACP
profiles require a non-empty `run` argv vector; process profiles do not.
Executor `env` overrides the inherited daemon environment.

The API and ledger expose the executor profile name, not environment values or
the merged environment object. v0 does not inspect or redact secrets that a
child itself writes to stdout or stderr.

## 4. Implement persistence and transcripts

### SQLite ledger

Implement this schema in `store.ts`:

```sql
PRAGMA foreign_keys = ON;

CREATE TABLE workflow_runs (
  id          TEXT PRIMARY KEY,
  workflow_id TEXT NOT NULL,
  input_json  TEXT NOT NULL,
  status      TEXT NOT NULL CHECK (
    status IN ('running', 'waiting', 'completed', 'blocked', 'cancelled')
  ),
  started_at  TEXT NOT NULL,
  updated_at  TEXT NOT NULL,
  ended_at    TEXT
);

CREATE TABLE task_runs (
  id               TEXT PRIMARY KEY,
  workflow_run_id  TEXT NOT NULL REFERENCES workflow_runs(id),
  task_id          TEXT NOT NULL,
  attempt          INTEGER NOT NULL CHECK (attempt >= 1),
  executor_profile TEXT NOT NULL,
  executor_type    TEXT NOT NULL CHECK (executor_type IN ('acp', 'process')),
  status           TEXT NOT NULL CHECK (
    status IN (
      'running',
      'awaiting_permission',
      'awaiting_input',
      'completed',
      'blocked',
      'failed',
      'cancelled'
    )
  ),
  result_json      TEXT,
  error_json       TEXT,
  acp_session_id   TEXT,
  started_at       TEXT NOT NULL,
  updated_at       TEXT NOT NULL,
  ended_at         TEXT,
  UNIQUE (workflow_run_id, task_id, attempt)
);

CREATE UNIQUE INDEX one_active_run_per_workflow
ON workflow_runs(workflow_id)
WHERE status IN ('running', 'waiting');

CREATE INDEX task_runs_by_workflow
ON task_runs(workflow_run_id, task_id, attempt);
```

Use UTC RFC 3339 timestamps and serialized JSON in `_json` columns. A
`task_runs` row represents one attempt and is updated as that attempt changes
state. Never overwrite a previous attempt when resuming.

Keep mutations that jointly change attempt and workflow state in SQLite
transactions.

### JSONL transcripts

Implement transcript ownership separately in `transcript.ts`. Every attempt
gets:

```text
<storageDir>/runs/<workflowRunId>/tasks/<taskRunId>/transcript.jsonl
```

Requirements:

- append-only, one complete JSON object per line;
- timestamp and direction/lifecycle metadata on every record;
- relevant ACP JSON-RPC messages preserved without lossy normalization;
- process lifecycle, streamed stderr, and raw stdout recorded;
- complete-line writes followed by flush;
- ignore or truncate an incomplete final line after a crash;
- support line-based reads for `transcript.read` and live append callbacks for
  events.

SQLite stores validated results and scheduler state, not transcript events.

## 5. Implement input resolution

Every task has an explicit `inputs` mapping. Support literal JSON values and
only these exact reference forms:

```text
$run.input
$run.input.<path>
$tasks.<dependency-id>.output
$tasks.<dependency-id>.output.<path>
```

Resolve references only after the referenced dependency completed. Missing
runtime paths fail the attempt with a structured input-resolution error.

For ACP, append the resolved input object as JSON after the authored prompt.
For a process, serialize the same object to stdin and close stdin. Do not
interpolate values into prompts or command arguments.

## 6. Implement the executor interface

Define one small internal adapter contract that can start an attempt, report
completion or failure, surface ACP interactions, cancel the concrete
execution, and clean up its owned process and protocol resources.

The scheduler owns DAG semantics and ledger transitions. Executors own only
their external execution.

### Direct-process executor

For each attempt:

1. Spawn `run[0]` with `run.slice(1)` using `shell: false`, the profile's
   `cwd`, merged environment, piped stdio, and a separate process group.
2. Write the resolved input JSON to stdin and close it.
3. Stream stderr into the attempt transcript.
4. Collect stdout and record it in the transcript.
5. After exit, require stdout to contain exactly one JSON value with only
   surrounding whitespace.
6. Validate it as the TaskResult and against the optional `outputSchema`.

Operational logs belong on stderr. Non-JSON stdout, multiple JSON values,
trailing content, missing output, schema failure, or a non-zero exit code makes
the attempt `failed`. Store only the parsed, validated TaskResult in
`result_json`; raw output remains in the transcript.

An explicit shell remains possible in workflow YAML:

```yaml
run: [/bin/bash, -lc, "some shell expression"]
```

Cancellation sends `SIGTERM` to the process group, waits two seconds, then
sends `SIGKILL` if it is still alive.

### ACP executor

For each attempt:

1. Spawn the configured ACP server argv in its own process group.
2. Initialize ACP and negotiate capabilities.
3. Create one fresh session with `session/new` and store its native session ID
   on the task-attempt row.
4. Issue exactly one `session/prompt` containing the task prompt, resolved
   inputs, and the exact TaskResult-only instruction.
5. Record relevant bidirectional ACP messages in the attempt transcript.
6. Treat the response to that prompt with `stopReason: end_turn` as terminal
   only after its content parses and validates as one TaskResult.
7. Close the ACP session/connection where supported and terminate the process
   group if the server remains alive.

Each attempt gets its own ACP process and session. Do not pool servers or reuse
sessions. Parallel tasks therefore remain independent.

Map ACP interactions as follows:

- `session/request_permission` -> `awaiting_permission`
- negotiated `elicitation/create` -> `awaiting_input`

Keep the ACP process and request alive while waiting. Store live pending
interaction handles only in memory, but write the request and response to the
transcript. `interaction.answer` sends a response matching the original ACP
method's native response schema and returns the same attempt to `running`.
Unsupported interaction capabilities block the task explicitly.

On cancellation, send ACP `session/cancel`, close the protocol connection,
then use the same two-second `SIGTERM`/`SIGKILL` process-group fallback.

## 7. Implement the scheduler

Use an event-driven scheduler with per-workflow-run in-memory serialization.
Advance a run after:

- `run.start`;
- any terminal attempt transition;
- `run.resume`;
- task or run cancellation;
- an ACP interaction answer that lets an attempt continue.

On every advancement:

1. Load the current workflow definition and persisted attempts.
2. Derive every task's logical state.
3. Start every task with no prior successful/current attempt whose dependencies
   all completed.
4. Let independent ready tasks run concurrently.
5. Recompute and persist the aggregate workflow status.

Do not add polling or a global concurrency limit.

Attempt settlement:

- valid `outcome: completed` -> `completed`;
- valid `outcome: blocked` -> `blocked`;
- executor, protocol, input-resolution, or result-validation error -> `failed`;
- explicit task/run cancellation -> `cancelled`.

Independent branches continue after another branch fails, blocks, or is
cancelled. Dependants of that task never start.

Workflow status derivation:

- `running`: an attempt executes or runnable work remains;
- `waiting`: nothing executes or is ready, but at least one ACP attempt awaits
  permission/input;
- `completed`: every task completed;
- `blocked`: no executable/waiting work remains because a task failed, blocked,
  or was individually cancelled;
- `cancelled`: set only by `run.cancel`.

`blocked`, `completed`, and `cancelled` set `endedAt`.

### Resume

Accept `run.resume` only for a blocked workflow. Clear `endedAt`, return the run
to `running`, and create a new attempt for every currently blocking task whose
dependencies completed. Completed tasks and successful sibling branches are
not replayed. There is no selective task resume and no automatic retry.

Resume reads the current workflow YAML. Exact historical workflow definitions
are not retained in v0.

### Cancellation

`run.cancel` stops new scheduling in that run, cancels all active attempts,
preserves completed results, exposes unstarted tasks as cancelled, and marks
the run terminal `cancelled`. It never starts another run.

`task.cancel` cancels only that attempt. Independent branches continue;
dependants cannot start; the workflow eventually becomes `blocked` and can be
resumed.

## 8. Implement startup recovery and shutdown

On daemon startup, find attempts left in `running`, `awaiting_permission`, or
`awaiting_input`. Mark them `failed` with reason `executor_lost`, preserve their
results/session IDs/transcripts, and mark their workflow runs `blocked`.
Nothing restarts automatically.

On `SIGINT` or `SIGTERM`:

1. Stop accepting new runs and stop scheduling new tasks.
2. Cancel active ACP sessions and process groups.
3. Mark interrupted attempts `failed` with reason `daemon_shutdown`.
4. Mark their workflow runs `blocked` so they can be resumed later.
5. Close SQLite and the WebSocket server.

A hard crash may leave child processes behind. Do not add PID persistence or
orphan adoption in v0.

## 9. Implement RunView, events, and JSON-RPC

Run `dagmard` on the configured loopback WebSocket. Implement JSON-RPC 2.0
request/response correlation and server notifications over the same
connection.

Implement exactly these methods:

```text
system.ping
executor.list
workflow.list
workflow.get
run.start
run.list
run.get
run.resume
run.cancel
task.cancel
interaction.list
interaction.answer
transcript.read
events.subscribe
events.unsubscribe
```

Do not add `task.get`, `run.retry`, `run.wait`, or `workflow.validate`.

Minimum request/response behavior:

- `system.ping({})` -> health, version, daemon start time, current sequence.
- `executor.list({})` -> executor names and types only.
- `workflow.list({})` -> valid workflow summaries plus file validation errors.
- `workflow.get({ workflowId })` -> complete parsed definition, including
  dependency edges and task executor profile references.
- `run.start({ workflowId, input })` -> validate, enforce the active-run
  constraint, insert durably, hand off to the scheduler, and immediately return
  `{ workflowRunId, status: "running" }`.
- `run.list({})` -> run summaries.
- `run.get({ workflowRunId })` -> the complete `RunView` defined below.
- `run.resume`, `run.cancel`, `task.cancel`, and `interaction.answer` -> the
  resulting `RunView` after the requested transition is persisted.
- `interaction.list({})` -> all currently live pending ACP interactions.
- `transcript.read({ taskRunId, afterLine? })` -> records and `nextLine`.
- `events.subscribe({ workflowRunId?, afterSequence? })` -> `subscriptionId`
  and `currentSequence`.
- `events.unsubscribe({ subscriptionId })` -> `{ unsubscribed: true }`.

`RunView` has this shape:

```json
{
  "sequence": 42,
  "id": "wr_123",
  "workflowId": "example",
  "status": "running",
  "input": {},
  "startedAt": "2026-09-10T10:00:00Z",
  "updatedAt": "2026-09-10T10:00:05Z",
  "endedAt": null,
  "tasks": {
    "research": {
      "dependsOn": [],
      "executor": "claude-acp",
      "state": "completed",
      "attempts": [
        {
          "id": "tr_101",
          "attempt": 1,
          "status": "completed",
          "result": {
            "outcome": "completed",
            "message": "Research complete",
            "output": {}
          },
          "error": null,
          "acpSessionId": "session-native-id",
          "startedAt": "2026-09-10T10:00:00Z",
          "updatedAt": "2026-09-10T10:00:05Z",
          "endedAt": "2026-09-10T10:00:05Z"
        }
      ]
    }
  }
}
```

The `tasks` mapping is keyed by workflow task ID. Each task includes its
dependency edges, executor profile, derived logical state, and every persisted
attempt. A process attempt has `acpSessionId: null`; an unstarted task has an
empty `attempts` array. `sequence` is the newest daemon event incorporated into
the view.

Derived task states exposed by `RunView` are:

```text
pending
ready
running
awaiting_permission
awaiting_input
completed
blocked
failed
cancelled
blocked_by_dependency
```

A task exposed as `running` must have a corresponding running attempt. Task
IDs and `dependsOn` provide the nodes and edges needed for a dashboard to lay
out the DAG; Dagmar stores no visual coordinates.

Publish notifications as:

```json
{
  "jsonrpc": "2.0",
  "method": "event",
  "params": {
    "sequence": 42,
    "timestamp": "2026-09-10T10:00:05Z",
    "type": "task.status_changed",
    "workflowRunId": "wr_123",
    "taskRunId": "tr_101",
    "data": {}
  }
}
```

Emit only:

- `workflow.status_changed`
- `task.status_changed`
- `interaction.changed`
- `transcript.appended`

Use a daemon-local monotonic sequence and a bounded in-memory buffer of the
last 1,000 notifications. Replay available entries after `afterSequence`; if
the cursor is unavailable after a restart or buffer rollover, return
`cursor_unavailable`. The client then rebuilds from `run.get` and
`transcript.read`. Do not persist an events table.

## 10. Implement the CLI as an API client

The CLI contains no scheduler or executor logic. It connects to `dagmard` and
uses the same JSON-RPC methods as future OpsDash clients.

Provide minimal commands:

```text
dagmar ping
dagmar executors
dagmar workflows
dagmar workflow <workflow-id>
dagmar run <workflow-id> [--input <text> | --input-json <json>]
dagmar runs
dagmar status <workflow-run-id>
dagmar watch <workflow-run-id>
dagmar resume <workflow-run-id>
dagmar cancel <workflow-run-id>
dagmar cancel-task <task-run-id>
dagmar pending
dagmar answer <interaction-id> --json <response>
dagmar transcript <task-run-id> [--after-line <n>]
```

`dagmar run` returns after the durable start acknowledgement. `dagmar watch`
holds a subscription open. There is no blocking run mode.

## 11. Basic smoke verification

Do not build CI or a broad test framework. Verify with small local workflows
and capture the commands/results.

### Process smoke

Create a harmless process DAG with one parallel branch:

```text
      -> B -
A ---       -> D
      -> C -
```

Confirm:

- A completes before B/C; B and C overlap; D starts only after both complete;
- run/task states and edges appear correctly in `run.get`;
- each attempt has one JSONL transcript;
- only the validated TaskResult lands in SQLite;
- malformed or noisy stdout fails the attempt;
- a blocked/failed branch leaves independent work running and then blocks the
  workflow;
- `run.resume` creates a new attempt without replaying completed tasks;
- task and run cancellation produce the specified states.

### ACP smoke

Run the same minimal ACP task contract through two named profiles:

- `@agentclientprotocol/claude-agent-acp`
- `openclaw acp`

Confirm each creates a fresh native ACP session, returns a validated TaskResult,
records the ACP transcript, and terminates its ACP server process. If supported
by the server, trigger one permission or input request and answer it through
`dagmar pending` / `dagmar answer`.

### Recovery/API smoke

Confirm:

- a second active run of the same workflow ID is rejected while a different
  workflow can run concurrently;
- `run.start` returns immediately and execution survives client disconnect;
- stopping `dagmard` gracefully blocks interrupted work with
  `daemon_shutdown`;
- restarting marks hard-interrupted active attempts `executor_lost` and allows
  explicit resume;
- `watch`, transcript reads, event subscription/unsubscription, and
  `cursor_unavailable` behave as specified.

## Definition of done

Dagmar v0 is complete when:

- the package builds with `tsc` and runs in development with `tsx`;
- both schemas and workflow validation are active;
- the exact two-table SQLite ledger and per-attempt JSONL transcripts work;
- dependency-ready process and ACP tasks execute concurrently;
- results, failure, waiting, cancellation, resume, startup recovery, and
  graceful shutdown follow this plan;
- the complete JSON-RPC surface and thin CLI work;
- both target ACP servers and the local-process smokes pass;
- no deferred v0 features or career-pipeline-specific behavior were added.
