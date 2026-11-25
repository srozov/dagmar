# Running Dagmar

Dagmar has two programs: `dagmard`, the daemon that runs workflows, and `dagmar`, a CLI that talks to it over a loopback WebSocket (JSON-RPC 2.0). From a checkout, run them as `node dist/daemon.js` and `node dist/cli.js` after `pnpm build`, or as `pnpm dev:daemon --` and `pnpm dev:cli --` without building.

## Configuration

Both programs read `~/.config/dagmar/config.yaml`, or the file given with `--config <path>`:

```yaml
workflowDir: /absolute/path/to/workflows
storageDir: /absolute/path/to/dagmar-state

listen:
  host: 127.0.0.1
  port: 7331
```

Both directories must be absolute paths. The daemon listens only on a loopback host, `127.0.0.1` or `::1`, and reads the config once at start. Executors are not configured here: each workflow declares its own. [`config.example.yaml`](../config.example.yaml) is a template.

The daemon keeps a SQLite ledger of runs and task attempts, and an append-only JSONL transcript per attempt, under `storageDir`. On `SIGINT` or `SIGTERM` it fails its running attempts with `daemon_shutdown` and blocks their runs so they can be resumed; pending gates are kept.

## Commands

Every command prints the daemon's answer as JSON.

| Command | Does |
|---|---|
| `ping` | Checks that the daemon answers. |
| `workflows` | Lists the workflows in `workflowDir`, and the files that fail validation with their errors. |
| `workflow <id>` | Shows a workflow's parsed definition. |
| `run <workflow-id> [--cwd <dir>] [--input <text> \| --input-json <json>]` | Starts a run and prints its `workflowRunId`. |
| `runs` | Lists runs. |
| `status <run-id>` | Shows a run: each task's state and attempts. |
| `watch <run-id>` | Prints the run, then streams its events until you stop it. |
| `pending` | Lists the permission requests, inputs, turns and gates waiting for an answer. |
| `answer <interaction-id> --json <value>` | Answers one of them. |
| `transcript <task-run-id> [--after-line <n>]` | Prints an attempt's transcript. |
| `cancel <run-id>` | Cancels a run. |
| `cancel-task <task-run-id>` | Cancels one attempt. |
| `resume <run-id> [--from <task>]` | Resumes a run. |

## Starting a run

`--cwd` gives the run's directory, used by every executor without its own `cwd`. A relative path resolves in the shell's current directory. Starting fails with `cwd_required` if an executor needs a directory and none is given, `cwd_unused` if every executor has its own, and `invalid_params` if the path is not an existing directory.

A workflow has at most one active run per directory (`active_run_exists`), so it can run on several repositories at once; runs without a directory count as one. `runs` and `status` show each run's `cwd`. `run` returns once the run is recorded; use `watch` or `status` to follow it.

## Cancelling and resuming

`cancel <run-id>` ends a `running`, `waiting` or `blocked` run as `cancelled`, for example a run blocked at a rerun's `max`. It stops the run's active attempts, keeps its other attempts, and shows the tasks that never started as `cancelled`. A completed run cannot be cancelled. `cancel-task` cancels one attempt: independent branches go on, its dependants do not start, and the run ends `blocked`.

`resume <run-id>` continues a `blocked` or `cancelled` run where it stopped: its blocked, failed and cancelled tasks get a new attempt, the tasks that never started start, and completed tasks keep their results.

`resume <run-id> --from <task>` also runs `<task>` again, whatever its state, and then every task downstream of it, as after a rerun; the other tasks keep their results. It works on a `completed`, `blocked` or `cancelled` run whose `<task>` has all its dependencies completed or skipped, so a finished run can redo its last steps without repeating the ones before.

A resume adds new attempts to each task's history; `when` guards read the run's own input, and rerun sources keep their counts. It fails with `active_run_exists` if the workflow already has another active run in the same directory.
