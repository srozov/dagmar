# Dagmar

[![CI](https://github.com/srozov/dagmar/actions/workflows/ci.yml/badge.svg)](https://github.com/srozov/dagmar/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**Dagmar** /ˈdɑːɡ.mɑːr/ — from Old Norse *dag* (day) and *mær* (maiden): the orchestrator of purposeful flow.

**D A G M A R** stands for **Directed Acyclic Graph Manager And Runner**.

Light agentic workflow orchestration for the command line. Dagmar speaks YAML for workflow definitions and ACP for agent execution.

Define an explicit task graph in YAML. Dagmar starts dependency-ready tasks through ACP agents or local processes, then gives you a CLI to start, inspect, watch, resume, and cancel runs. It records run state in SQLite and streams control-plane events over loopback WebSocket JSON-RPC.

Dagmar coordinates work; agents retain their own models, tools, skills, sessions, and reasoning. You can run a complex internal loop inside one agent task, then use Dagmar to record that turn in a larger graph and pass declared JSON outputs across dependency edges.

Use Dagmar when you need durable task state, explicit handoffs, and operator control around a small set of agent or process steps.

## Features

- **Task graphs in YAML:** dependencies, `when` guards, human gates, bounded reruns, and map tasks that run once per item of a list.
- **Two executors:** [Agent Client Protocol](https://agentclientprotocol.com) agents and local processes, declared in each workflow, with the model, mode, effort and tool set chosen per ACP executor.
- **Agent sessions:** a task can continue an earlier task's session, and interactive tasks hold a conversation with a person.
- **Concurrency:** independent ready tasks run at the same time.
- **Operator control:** a CLI and a loopback JSON-RPC API to start, watch, cancel and resume runs, and to answer agent permission and input requests.
- **Durable state:** a SQLite ledger of runs and attempts, an append-only JSONL transcript per attempt, and gates that survive a restart.

## Example

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

`collect` runs a local script on the run's input. Once it completes, `assess` sends its output to an ACP agent with a prompt. Each task ends with a JSON result (`outcome`, `message`, `output`) that later tasks read through `$tasks.<id>.output`.

## Quick start

Dagmar needs Node.js 24 and pnpm 12.

```sh
git clone https://github.com/srozov/dagmar.git
cd dagmar
pnpm install --frozen-lockfile
pnpm build
```

Write a config that serves the repository's examples, with absolute paths:

```yaml
# config.yaml
workflowDir: /absolute/path/to/dagmar/examples
storageDir: /absolute/path/to/dagmar-state
listen: { host: 127.0.0.1, port: 7331 }
```

Start the daemon, then run the process example from another terminal in the checkout:

```sh
node dist/daemon.js --config config.yaml
```

```sh
node dist/cli.js --config config.yaml run process-example --cwd . --input-json '{"hello":"world"}'
node dist/cli.js --config config.yaml status <workflowRunId>
```

## Documentation

- [Workflows](docs/workflows.md): the workflow language, from executors and inputs to guards, gates, reruns and map tasks.
- [Executors](docs/executors.md): the process and ACP executors, the task result, and the ACP retry.
- [Running Dagmar](docs/cli.md): configuration, the daemon, the CLI, and cancelling and resuming runs.
- [Design](docs/design.md): contracts, persistence, the scheduler, recovery and the JSON-RPC API.
- [Examples](examples/README.md), including a review loop with a builder agent, a reviewer agent and a human gate, and [its runbook](docs/mvp-acceptance.md).

## Development

```sh
pnpm check   # type-check src and tests
pnpm test    # run the test suite
pnpm build   # compile to dist/
```

## License

[MIT](LICENSE)
