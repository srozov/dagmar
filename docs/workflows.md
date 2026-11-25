# Workflows

A workflow is a YAML file in the daemon's `workflowDir`. It has a unique `id`, the `executors` its tasks use, and a non-empty mapping of task IDs to `tasks`:

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
```

`workflow.schema.json` is the schema of the file; Dagmar also checks the rules below and names the broken one in its error.

## Executors

An executor has a `type`, `process` or `acp`, and may set:

- `cwd`, an absolute path where its tasks run. Without one, they run in the run's directory, given when the run starts (`--cwd`). A workflow that leaves `cwd` out can serve any repository of its kind; one that always works in one place sets it.
- `env`, string values added to the daemon's environment.
- `envFile`, the absolute path of a `KEY=VALUE` file read each time one of the executor's tasks starts. Its values go between the daemon's environment and `env`, so a secret stays out of the workflow file and reaches only the executors that name the file. An unreadable file fails the task with `env_file_unreadable`.

An ACP executor also requires `run`, the command that starts its agent, and may set `model`, `mode`, `effort`, `tools`, `loadSettleMs` and `retry`. [Executors](executors.md) describes both executor types.

Dagmar reads workflows from disk on every step, so a new or edited workflow needs no restart. An edit applies to the workflow's active runs from their next task, so do not change a workflow's executors while it has active runs; to run the same tasks with other executors, write a second workflow file.

Reading a run (`run.get`, `status`) does not depend on the file. A task the file no longer has, renamed or removed, still shows with its stored attempts, no dependencies and the state of its latest attempt. A step that would run or settle tasks against such a workflow does not go on: `resume`, `answer` and `cancel-task` fail, and an active run's next step blocks the run, with `workflow_definition_unavailable` naming the missing tasks.

## Tasks

A task's `executor` names one of its workflow's executors. A process task requires `run` and cannot use `prompt`, `session` or `interactive`; an ACP task requires `prompt` and cannot use `run`. Dependencies (`dependsOn`) must exist and form an acyclic graph. Independent tasks whose dependencies have settled run concurrently.

## Inputs

Input values are literals or references:

- `$run.input` and `$run.input.<path>` read the input given when the run starts.
- `$run.id` reads the run's ID. It is the same in every task of the run and every item of a map task. Any other `$run.<name>` fails validation with `invalid_input_reference`.
- `$tasks.<task-id>.output` and `$tasks.<task-id>.output.<path>` read a dependency's output.
- `$item` and `$item.<path>` read the item of a map task (see [Map tasks](#map-tasks)).

A reference must be the whole value of a top-level input. A reference inside an object or array value, such as `gaps: { a: $tasks.collect-a.output.gaps }`, fails validation with `invalid_input_reference`, naming the input and the path; give each such value its own top-level input instead. Objects and arrays of literals pass through unchanged. A path that does not resolve when the task starts fails it with `input_resolution_failed`.

## Output schema

`outputSchema` is a JSON Schema for the task's `output`. An output that does not match fails with `output_validation_failed`, whose message names each violation by path, for example `output/items/2/evidence must NOT have fewer than 1 items; output/note must be string` (cut to 500 characters).

## Sessions

An ACP task starts a new agent session unless it sets `session: { mode: continue, from: <task-id> }`, which loads that task's session through ACP `session/load`. The source must be a dependency (`invalid_dependency`) and an ACP task whose executor has a byte-identical `run` (`invalid_task`); `model`, `mode`, `effort` and `tools` may differ. The attempt fails with `acp_load_unsupported` if the agent cannot load sessions, or `continuation_unavailable` if the source has no recorded session, for example because it was skipped. `session: { mode: fork }` is rejected with `unsupported_session_mode`.

## Interactive tasks

An ACP task with `interactive: true` is a conversation with a person. Each agent reply that is not a result parks the task as `awaiting_input` with a `turn` interaction carrying the reply in `request.message`. The answer is the next message, as a non-empty JSON string:

```sh
dagmar answer <interaction-id> --json '"Add a test"'
```

The task ends when the agent replies with a result. Interactive tasks are live-only: a daemon restart fails the attempt with `executor_lost`, and `resume` starts a new attempt.

## Gates

A task with `gate: { prompt, schema? }` instead of `executor` waits for a human decision. It cannot use `prompt`, `run`, `session`, `interactive`, `outputSchema`, `rerun` or `map`. When it becomes ready it parks as `awaiting_input` with a `gate` interaction showing its prompt and schema. `dagmar answer <interaction-id> --json <value>` completes it with that value as its `output`; a value that does not match `schema` is rejected and the gate stays pending. A pending gate is persisted and survives a daemon restart, after which `dagmar pending` lists it under a new interaction ID.

## Guards

`when` guards a task with a list of clauses, all of which must hold. Each clause has a `ref` to `$run.input…` or `$tasks.<dependency>.output…` and exactly one of `equals` (a JSON value) or `in` (a non-empty array of JSON values), compared by JSON equality; other shapes fail validation with `invalid_guard`.

If a clause is false when the task's dependencies settle, the task is `skipped` instead of run. A missing path, or a reference to a skipped task, makes a clause false. A skipped task counts as settled for its dependants, but an unguarded input reference to its output fails with `input_resolution_failed`. A run completes when every task is `completed` or `skipped`.

## Reruns

`rerun: { from, max, atMax?, when? }` makes a task, the source, rerun the run from `from`, a task upstream of it (`invalid_rerun` otherwise). A rerun is not a dependency edge, so the graph stays acyclic. Each time the source completes and asks for a rerun, `from` runs again, and every task downstream of it that was already decided is decided again, in dependency order, on the new outputs, the source included. Nothing waits for a rerun: a task is decided as soon as its dependencies are, and again after each rerun, so guard the tasks after `from` that must not run on an intermediate result. Several tasks may rerun from the same task.

Without `rerun.when`, every completion asks for a rerun. `rerun.when` is a list of clauses like `when`'s, read from the source's own output once it has completed (`$tasks.<source>.output…` only, `invalid_rerun` otherwise). The source asks for a rerun only when all of them hold; otherwise the run goes on to the tasks after it.

A source asks for at most `max` reruns per run; only completed attempts that asked count, and `resume` continues the count. A rerun asked for beyond `max` is not done. By default the source's attempt then ends `blocked` with `rerun_exhausted`, and so does the run; with `atMax: proceed` the run goes on to the tasks after the source instead. Without `rerun.when`, Dagmar knows before the source runs that it would ask, so a source that has used up `max` is not run again: its attempt is `blocked`, or `skipped` with `atMax: proceed`, and a `resume` blocks the run again at once. With `rerun.when`, the source runs and its completed attempt is `blocked`, keeping its output, or stays `completed` with `atMax: proceed`.

In [`examples/review-iteration-loop.yaml`](../examples/review-iteration-loop.yaml), `fixup` continues `implement`'s session and reruns from `verify` while verification fails, at most three times:

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

The same workflow uses a gate and an interactive task; [mvp-acceptance.md](mvp-acceptance.md) is its runbook.

## Map tasks

`map: { over, key, concurrency }` runs a task once per item of a list known only at run time, for example one review per document:

```yaml
  review:
    executor: reviewer
    dependsOn: [list]
    map: { over: $tasks.list.output.docs, key: id, concurrency: 3 }
    inputs: { doc: $item }
    prompt: |
      Review the document in your inputs.
```

All three fields are required. `over` reads `$tasks.<dependency>.output…` or `$run.input…` and must resolve to a list of objects whose `key` field is a non-empty string, unique in the list; otherwise the task fails before any item starts, with `map_items_invalid`. `$item` and `$item.<path>` read the item, in a map task's `inputs` only.

Each item runs as an attempt of its own, with its own session, transcript and status, and `outputSchema` applies to each item's output. At most `concurrency` items are active at once, including those waiting for an answer; they start in list order, the next one whenever one ends. The task's output is `{ "<key>": <item output> }`, so a later task reads every item's result from `$tasks.review.output`. An empty list completes the task at once with `{}`. `when` guards the whole task.

An item that fails, blocks or is cancelled (`cancel-task` on its attempt) leaves the others running. Once every item has ended, the task completes, or ends `blocked` with `map_items_incomplete` if any item did not complete. The task keeps one attempt of its own per round, with `item: null`; its items' attempts are listed with it, each with its `item` key and the round's `roundId`, and take the task's next attempt numbers. `resume` continues the round and runs only the items that did not complete, also after a daemon restart or a cancelled run; `resume --from <map task>`, or a rerun from upstream of it, starts a new round with every item. The task's own attempt cannot be cancelled (`invalid_task_state`): cancel its items or the run. While a map runs, its run is `running`, even when its active items wait for answers; an item's interactions and `task.status_changed` events carry its `item`.

A map task cannot `rerun` or continue a session, no task can continue a map task's session (`invalid_map`), and maps do not nest.
