# Design

This document specifies how Dagmar works: its contracts, persistence,
scheduler, run and task lifecycle, recovery and shutdown, and API. It is kept in
step with the code. The workflow language, the executors and the CLI are
documented in [workflows.md](workflows.md), [executors.md](executors.md) and
[cli.md](cli.md).

## Overview

Dagmar is a small headless DAG runtime in TypeScript.

Dagmar loads static YAML workflows, runs dependency-ready tasks concurrently,
executes tasks through either ACP or a direct local process, validates a
universal task result, and persists run and attempt state in SQLite. A
JSON-RPC 2.0 API over one loopback WebSocket exposes everything needed by the
CLI and a future dashboard's DAG view.

## Scope

Dagmar provides:

- static dependency DAGs, with `when` guards, human gates, bounded reruns and
  map tasks;
- ACP and direct-process executors, declared in each workflow;
- parallel execution of independent ready tasks;
- interactive (multi-turn) ACP tasks and continuing an earlier task's ACP
  session;
- one automatic retry policy: an ACP attempt that failed before doing any work;
- a two-table SQLite ledger;
- one append-only JSONL transcript per task attempt;
- manual cancellation and resume;
- ACP permission/input handling through the CLI;
- a manually started daemon, WebSocket JSON-RPC API, and thin CLI.

It deliberately leaves out:

- cron or scheduling;
- task timeouts, or automatic retries of an attempt that did any work;
- dynamic tasks, unbounded loops, nested maps, or fan-out other than a map
  task's items;
- concurrency limits other than a map task's own `concurrency`;
- workflow definition snapshots;
- artifacts, workflow memory, or mutable shared context;
- session forking;
- reconnecting to ACP sessions after daemon restart;
- tmux, containers, workers, queues, or distributed locking;
- an OpenClaw plugin, dashboard UI, or domain-specific integration;
- automatic secret redaction of child-process output.

## 1. Package

One package:

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
  test/
  examples/
```

Runtime dependencies:

- `@agentclientprotocol/sdk`, and `zod`, which it requires as a peer
- `ajv`
- `yaml`
- `ws`

Development dependencies:

- `typescript`
- `tsx`
- required Node and WebSocket type packages only

Use Node built-ins for SQLite, child processes, files, UUIDs, signals, and the
test runner. Scripts cover `tsc` compilation, type checking of `src` and
`test`, and `tsx` development execution. There is no bundler, web framework,
ORM, CLI framework, or executor framework.

## 2. Contracts

### Universal task result

`result.schema.json` defines exactly:

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
validate `output` against it in addition to the universal envelope. Workflows
are read again on every step, so compile each distinct task schema once and
find it again by its text.

### Workflow definition

Support this shape:

```yaml
id: example

executors:
  local-process:
    type: process
  claude-acp:
    type: acp
    run: [/absolute/path/to/claude-agent-acp]

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

`executors` is a mapping keyed by executor name. An executor has a `type`,
`process` or `acp`, and may set:

- `cwd`, an absolute path. Without one, its tasks run in the run's directory
  (`run.start`'s `cwd`);
- `env`, string values on top of the daemon's environment;
- `envFile`, the absolute path of a `KEY=VALUE` file read each time one of its
  tasks starts, applied between the daemon's environment and `env`.

An ACP executor requires `run`, a non-empty argv vector that starts its agent,
and may set `model`, `mode`, `effort`, `tools`, `loadSettleMs` and
`retry: { max, delaySeconds }`. A process executor takes none of these.

`tasks` is a mapping keyed by task ID. Do not duplicate the ID inside a task.
Allowed task fields are only:

- `executor`
- `dependsOn`
- `inputs`
- `prompt`
- `run`
- `outputSchema`
- `session`
- `interactive`
- `gate`
- `when`
- `rerun`
- `map`

Validate a file whenever it is read:

- workflow ID is present and unique;
- `tasks` is non-empty and has no duplicate YAML keys;
- every task names one of the workflow's own executors, or is a gate;
- process tasks have a non-empty `run: string[]` and no `prompt`, `session` or
  `interactive`;
- ACP tasks have a non-empty `prompt` and no `run`;
- a gate task has `gate: { prompt, schema? }` and none of `executor`, `prompt`,
  `run`, `session`, `interactive`, `outputSchema`, `rerun` or `map`;
- dependencies exist, are not self-references, and form an acyclic graph;
- `$tasks.*` references point only to declared dependencies;
- a reference is the whole value of a top-level input, never nested in one;
- `outputSchema` and a gate's `schema` are valid JSON Schema;
- unknown fields are rejected;
- `session: { mode: continue, from }` names a dependency that is an ACP task
  whose executor has the same `run`; `session: { mode: fork }` is recognized
  and rejected explicitly as unsupported;
- each `when` and `rerun.when` clause has a `ref` and exactly one of `equals`
  or `in`; `when` reads the run input or a dependency's output, `rerun.when`
  the task's own output;
- `rerun.from` is a task upstream of the task, and `rerun.max` is at least 1;
- `map.over` reads the run input or a dependency's output; a map task cannot
  `rerun` or continue a session, and no task continues a map task's session.

Multiple root tasks and multiple terminal tasks are valid.

## 3. Configuration

Read `~/.config/dagmar/config.yaml`, overridable with
`dagmard --config <path>` and `dagmar --config <path>`:

```yaml
workflowDir: /absolute/path/to/workflows
storageDir: /absolute/path/to/dagmar-state

listen:
  host: 127.0.0.1
  port: 7331
```

Require absolute `workflowDir` and `storageDir` paths, a loopback `listen.host`
(`127.0.0.1` or `::1`), and reject unknown fields. The config is read once at
start. Executors are not configured here: each workflow declares its own, so a
workflow file is complete in itself.

The API and ledger expose the executor name, not environment values or the
merged environment object. Dagmar does not inspect or redact secrets that a child
itself writes to stdout or stderr.

## 4. Persistence and transcripts

### SQLite ledger

`store.ts` holds this schema:

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
  ended_at    TEXT,
  cwd         TEXT
);

CREATE TABLE task_runs (
  id               TEXT PRIMARY KEY,
  workflow_run_id  TEXT NOT NULL REFERENCES workflow_runs(id),
  task_id          TEXT NOT NULL,
  attempt          INTEGER NOT NULL CHECK (attempt >= 1),
  executor_profile TEXT NOT NULL,
  executor_type    TEXT NOT NULL CHECK (
    executor_type IN ('acp', 'process', 'gate')
  ),
  status           TEXT NOT NULL CHECK (
    status IN (
      'running',
      'awaiting_permission',
      'awaiting_input',
      'completed',
      'blocked',
      'failed',
      'cancelled',
      'skipped'
    )
  ),
  result_json      TEXT,
  error_json       TEXT,
  acp_session_id   TEXT,
  started_at       TEXT NOT NULL,
  updated_at       TEXT NOT NULL,
  ended_at         TEXT,
  item_key         TEXT,
  round_id         TEXT,
  UNIQUE (workflow_run_id, task_id, attempt)
);

CREATE UNIQUE INDEX one_active_run_per_workflow_cwd
ON workflow_runs(workflow_id, ifnull(cwd, ''))
WHERE status IN ('running', 'waiting');

CREATE INDEX task_runs_by_workflow
ON task_runs(workflow_run_id, task_id, attempt);
```

Use UTC RFC 3339 timestamps and serialized JSON in `_json` columns. A
`task_runs` row represents one attempt and is updated as that attempt changes
state. Never overwrite a previous attempt when resuming.

A workflow has at most one active run per working directory; runs without a
`cwd` count as one directory. A gate's attempt has executor type and profile
`gate`.

A map task's items are `task_runs` rows of that task too. `item_key` holds an
item attempt's key; `round_id` holds the id of the map task's attempt that
started the round, on the map attempt and its items alike. Both are null on
every other attempt. Attempt numbers stay unique per task, items included.

Keep mutations that jointly change attempt and workflow state in SQLite
transactions.

### JSONL transcripts

`transcript.ts` owns transcripts, apart from the store. Every attempt
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
- support line-based reads for `transcript.read`; an append returns its line
  number, which the scheduler publishes as a `transcript.appended` event.

SQLite stores validated results and scheduler state, not transcript events.

## 5. Input resolution

Every task has an explicit `inputs` mapping. Support literal JSON values and
only these exact reference forms:

```text
$run.input
$run.input.<path>
$run.id
$tasks.<dependency-id>.output
$tasks.<dependency-id>.output.<path>
$item
$item.<path>
```

`$item` forms are allowed only in the `inputs` of a map task, and read the item
that attempt runs. `$run.id` is the run's id and takes no path. A reference is
the whole value of a top-level input; objects and arrays of literals pass
through unchanged.

Resolve references only after the referenced dependency settled. Missing
runtime paths, and the output of a skipped dependency, fail the attempt with a
structured input-resolution error.

For ACP, append the resolved input object as JSON after the authored prompt.
For a process, serialize the same object to stdin and close stdin. Do not
interpolate values into prompts or command arguments.

## 6. Executors

Define one small internal adapter contract that can start an attempt, report
completion or failure, surface ACP interactions, cancel the concrete
execution, and clean up its owned process and protocol resources.

The scheduler owns DAG semantics and ledger transitions. Executors own only
their external execution.

Both executors run in the executor's `cwd`, or the run's when it has none, with
the daemon's environment, then the executor's `envFile`, then its `env`. A
command that cannot be started fails the attempt; it never stops the daemon.

### Direct-process executor

For each attempt:

1. Spawn `run[0]` with `run.slice(1)` using `shell: false`, piped stdio, and a
   separate process group.
2. Write the resolved input JSON to stdin and close it.
3. Stream stderr into the attempt transcript.
4. Collect stdout and record it in the transcript.
5. After exit, require stdout to contain exactly one JSON value with only
   surrounding whitespace.
6. Validate it as the TaskResult and against the optional `outputSchema`.

Operational logs belong on stderr. Non-JSON stdout, multiple JSON values,
trailing content, missing output, schema failure, or a non-zero exit code makes
the attempt `failed`; the error message ends with the tail of stderr, and names
the signal that killed the process. Store only the parsed, validated TaskResult
in `result_json`; raw output remains in the transcript.

An explicit shell remains possible in workflow YAML:

```yaml
run: [/bin/bash, -lc, "some shell expression"]
```

Cancellation sends `SIGTERM` to the process group, waits two seconds, then
sends `SIGKILL` if it is still alive.

### ACP executor

For each attempt:

1. Spawn the executor's ACP server argv in its own process group.
2. Initialize ACP and negotiate capabilities.
3. Create one fresh session with `session/new`, or, for a task that continues
   another task's session, load that session with `session/load`. Store the
   native session ID on the task-attempt row.
4. Apply the executor's `model`, `mode` and `effort` through the agent's config
   options, and fail the attempt if the agent does not offer or apply one.
   `tools` is passed with the session and needs an agent that takes it.
5. Issue one `session/prompt` containing the task prompt, resolved inputs, and
   the exact TaskResult-only instruction.
6. Record relevant bidirectional ACP messages in the attempt transcript.
7. Treat the response to that prompt with `stopReason: end_turn` as terminal
   only after the turn's final message parses and validates as one TaskResult.
   An invalid result gets one repair prompt on the same session; a second
   invalid result fails the attempt.
8. Close the ACP session/connection where supported and terminate the process
   group if the server remains alive.

Each attempt gets its own ACP process. Do not pool servers. A session is
reused only by a task that declares `session: { mode: continue, from }`, which
loads it in its own process once the source task completed. Parallel tasks
therefore remain independent.

An `interactive` task keeps its session across turns: a reply that is not a
TaskResult parks the attempt as `awaiting_input` with a `turn` interaction, the
answer is sent as the next prompt, and the task ends when the agent replies
with a result. Interactive tasks are live-only.

Map ACP interactions as follows:

- `session/request_permission` -> `awaiting_permission`
- negotiated `elicitation/create` -> `awaiting_input`
- an interactive task's conversational reply -> `awaiting_input`

Keep the ACP process and request alive while waiting. Store live pending
interaction handles only in memory, but write the request and response to the
transcript. `interaction.answer` sends a response matching the original ACP
method's native response schema and returns the same attempt to `running`.
Unsupported interaction capabilities block the task explicitly.

An error the agent answers to one of Dagmar's requests before its first tool
call, output or interaction left nothing to undo. The executor reports such a
failure as retryable; every other failure is final.

On cancellation, send ACP `session/cancel`, close the protocol connection,
then use the same two-second `SIGTERM`/`SIGKILL` process-group fallback.

## 7. Scheduler

Use an event-driven scheduler with per-workflow-run in-memory serialization.
Advance a run after:

- `run.start`;
- any terminal attempt transition;
- `run.resume`;
- a task cancellation that leaves the run `running`;
- a gate answer.

On every advancement:

1. Load the current workflow definition and persisted attempts.
2. Derive every task's logical state.
3. Decide every task that has no attempt, or is due again, once all its
   dependencies are `completed` or `skipped`:
   - a task whose `when` clauses do not all hold gets a `skipped` attempt;
   - a gate parks as `awaiting_input` with a `gate` interaction;
   - a map task gets one attempt for the round and starts its items, at most
     `concurrency` at a time;
   - any other task starts on its executor.
4. Let independent ready tasks run concurrently.
5. Recompute and persist the aggregate workflow status.

Do not add polling or a global concurrency limit.

Attempt settlement:

- valid `outcome: completed` -> `completed`;
- valid `outcome: blocked` -> `blocked`;
- executor, protocol, input-resolution, or result-validation error -> `failed`;
- explicit task/run cancellation -> `cancelled`;
- a false `when` guard -> `skipped`, without running;
- a gate answer that matches the gate's schema -> `completed`, with the answer
  as its output.

A retryable ACP failure leaves its attempt `failed` and starts a new attempt of
the same task after the executor's `retry.delaySeconds`, at most `retry.max`
times (by default 2 times, 300 seconds apart; `max: 0` turns it off). Nothing
else is retried automatically.

`rerun: { from, max, atMax?, when? }` on a task makes its completion ask for a
rerun from `from`, a task upstream of it: `from` and every task downstream of
it that was already decided are decided again, in dependency order. A rerun
asked for beyond `max` is not done; the source's attempt then ends `blocked`
with `rerun_exhausted`, or the run goes on with `atMax: proceed`.

A map task settles once every item of its round has ended: `completed` with
`{ "<key>": <item output> }`, or `blocked` with `map_items_incomplete` if an
item did not complete.

Independent branches continue after another branch fails, blocks, or is
cancelled. Dependants of that task never start. A skipped task counts as
settled for its dependants.

Workflow status derivation:

- `running`: an attempt executes or runnable work remains;
- `waiting`: nothing executes or is ready, but at least one attempt awaits
  permission or input, a gate included;
- `completed`: every task is completed or skipped;
- `blocked`: no executable/waiting work remains because a task failed, blocked,
  or was individually cancelled;
- `cancelled`: set only by `run.cancel`.

`blocked`, `completed`, and `cancelled` set `endedAt`.

The scheduler reads the workflow file on every step and keeps no snapshot. A
step that would run or settle tasks against a file that no longer has a task of
the run does not go on: it fails with `workflow_definition_unavailable`, and an
active run's next step blocks the run. Reading a run never depends on it.

### Resume

Accept `run.resume` for a blocked or cancelled workflow. Clear `endedAt`, return
the run to `running`, and create a new attempt for every currently blocking task
whose dependencies completed, and start the tasks that never ran. Completed
tasks and successful sibling branches are not replayed. A resume starts the ACP
retry count again and keeps the rerun counts.

`run.resume` with `from` also accepts a completed workflow and creates a new
attempt for that task, whatever its state, once its dependencies are completed
or skipped. The tasks downstream of it are decided again, as after a rerun.
A resume that would make the workflow active while it has another active run
in the same directory fails with `active_run_exists`.

A map task resumed without `from` continues its round: its new attempt runs
only the items whose latest attempt in the round did not complete. With `from`
on the map task, or after a rerun upstream of it, a new round runs every item.

Resume reads the current workflow YAML. Exact historical workflow definitions
are not retained.

### Cancellation

`run.cancel` is accepted for a running, waiting or blocked run. It stops new
scheduling in that run, cancels all active attempts, preserves completed
results, exposes unstarted tasks as cancelled, and marks the run terminal
`cancelled`. It never starts another run. A completed run cannot be cancelled.

`task.cancel` cancels only that attempt. Independent branches continue;
dependants cannot start; the workflow eventually becomes `blocked` and can be
resumed. A map task's own attempt cannot be cancelled: cancel its items or the
run.

## 8. Recovery and shutdown

On daemon startup, find attempts left in `running`, `awaiting_permission`, or
`awaiting_input`.

- An executor's attempt is marked `failed` with reason `executor_lost`; its
  result, session ID and transcript are preserved.
- A gate's attempt in `awaiting_input` is kept: its interaction is pending
  again under a new interaction ID. If its definition can no longer be read,
  it is marked `failed` with reason `gate_definition_unavailable`.

Then recompute each such run's status from its attempts. A run with a failed
attempt is `blocked`; one that is not terminal, such as a run `waiting` on a
pending gate, has its `endedAt` cleared. Nothing restarts automatically.

On `SIGINT` or `SIGTERM`:

1. Stop accepting new runs and stop scheduling new tasks.
2. Cancel active ACP sessions and process groups.
3. Mark interrupted attempts `failed` with reason `daemon_shutdown`. Pending
   gates are left as they are.
4. Mark their workflow runs `blocked` so they can be resumed later.
5. Close SQLite and the WebSocket server.

A hard crash may leave child processes behind. Do not add PID persistence or
orphan adoption.

## 9. RunView, events and JSON-RPC

Run `dagmard` on the configured loopback WebSocket. Implement JSON-RPC 2.0
request/response correlation and server notifications over the same
connection.

The API has exactly these methods:

```text
system.ping
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
- `workflow.list({})` -> valid workflow summaries plus file validation errors.
- `workflow.get({ workflowId })` -> complete parsed definition, including its
  executors, dependency edges and task executor references.
- `run.start({ workflowId, input, cwd? })` -> validate, enforce the active-run
  constraint, insert durably, hand off to the scheduler, and immediately return
  `{ workflowRunId, status: "running" }`. `cwd` is required when an executor of
  the workflow has none of its own (`cwd_required`) and rejected when every
  executor has one (`cwd_unused`).
- `run.list({})` -> run summaries.
- `run.get({ workflowRunId })` -> the complete `RunView` defined below.
- `run.resume({ workflowRunId, from? })`, `run.cancel`, `task.cancel`, and
  `interaction.answer` -> the resulting `RunView` after the requested
  transition is persisted.
- `interaction.list({})` -> all currently pending interactions: ACP permission
  and input requests, interactive turns, and gates.
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
  "cwd": "/absolute/path/to/project",
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
          "endedAt": "2026-09-10T10:00:05Z",
          "item": null,
          "roundId": null
        }
      ]
    }
  }
}
```

The `tasks` mapping is keyed by workflow task ID. Each task includes its
dependency edges, executor name (`gate` for a gate), derived logical state, and
every persisted attempt, a map task's items included. A process attempt has
`acpSessionId: null`; an unstarted task has an empty `attempts` array. A run
without a directory has `cwd: null`. `sequence` is the newest daemon event
incorporated into the view. A task the workflow file no longer has still shows,
from its stored attempts: no dependencies, and the state of its latest attempt.

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
skipped
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

## 10. CLI

The CLI contains no scheduler or executor logic. It connects to `dagmard` and
uses the same JSON-RPC methods as future dashboard clients.

Provide minimal commands:

```text
dagmar ping
dagmar workflows
dagmar workflow <workflow-id>
dagmar run <workflow-id> [--cwd <dir>] [--input <text> | --input-json <json>]
dagmar runs
dagmar status <workflow-run-id>
dagmar watch <workflow-run-id>
dagmar resume <workflow-run-id> [--from <task-id>]
dagmar cancel <workflow-run-id>
dagmar cancel-task <task-run-id>
dagmar pending
dagmar answer <interaction-id> --json <response>
dagmar transcript <task-run-id> [--after-line <n>]
```

`dagmar run` returns after the durable start acknowledgement; it resolves a
relative `--cwd` in the shell's directory. `dagmar watch` holds a subscription
open. There is no blocking run mode.

## 11. Verification

The automated suite (`pnpm test`, Node's test runner) covers the scheduler,
both executors against scripted fake agents, the store, transcripts, workflow
validation and the JSON-RPC surface. Keep it deterministic: no real agents, no
network, no unguarded timing. CI runs `pnpm check`, `pnpm test` and
`pnpm build`.

Real agents are verified by hand with small local workflows; capture the
commands and results.

### Process smoke

Run a harmless process DAG with one parallel branch:

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

Run the same minimal ACP task contract (`examples/acp-smoke.mjs`) through two
ACP servers:

- `@agentclientprotocol/claude-agent-acp`
- `openclaw acp`

Confirm each creates a fresh native ACP session, returns a validated TaskResult,
records the ACP transcript, and terminates its ACP server process. If supported
by the server, trigger one permission or input request and answer it through
`dagmar pending` / `dagmar answer`. [mvp-acceptance.md](mvp-acceptance.md) is the runbook for
a full review loop on real agents.

### Recovery/API smoke

Confirm:

- a second active run of the same workflow in the same directory is rejected
  while a different workflow, or the same one in another directory, can run
  concurrently;
- `run.start` returns immediately and execution survives client disconnect;
- stopping `dagmard` gracefully blocks interrupted work with
  `daemon_shutdown`;
- restarting marks hard-interrupted active attempts `executor_lost`, keeps
  pending gates, and allows explicit resume;
- `watch`, transcript reads, event subscription/unsubscription, and
  `cursor_unavailable` behave as specified.
