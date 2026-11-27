# Executors

Each workflow declares its executors (see [Workflows](workflows.md#executors)). Both types run in the executor's `cwd`, or the run's directory when it has none, with the daemon's environment, then the executor's `envFile`, then its `env`. A command that cannot be started fails the attempt; it never stops the daemon.

## The task result

Every task ends with one JSON result:

```json
{
  "outcome": "completed",
  "message": "Human-readable summary",
  "output": {}
}
```

`outcome` is `completed` or `blocked`, and all three fields are required. `output` is any JSON value, checked against the task's `outputSchema`, if it has one, when `outcome` is `completed`. `result.schema.json` is the schema of the result.

## Process executors

Dagmar writes the resolved task inputs as one JSON value to the process's standard input. The process writes exactly one result to standard output and exits with status 0.

Standard error goes to the task's transcript. When the process exits non-zero, or its standard output is missing or not one JSON value, the attempt fails, and its error message ends with the last non-empty lines of standard error (at most 500 characters, cut from the front). A process killed by a signal fails with `Process was killed by <signal>`.

[`examples/process-workflow.yaml`](../examples/process-workflow.yaml) runs [`examples/process-result.mjs`](../examples/process-result.mjs), a minimal process task.

## ACP executors

An ACP executor's `run` is the command that starts an [Agent Client Protocol](https://agentclientprotocol.com) agent, such as `claude-agent-acp` or `openclaw acp`. Each attempt starts its own agent process and session; Dagmar sends the task prompt, the resolved inputs and an instruction to answer with the result.

The agent returns the result as its final message; earlier messages in the turn, such as progress notes between tool calls, are ignored. If that final message is not a valid result, Dagmar sends one repair prompt on the same session asking for only the corrected result (transcript event `acp_result_repair`). A second invalid result fails the attempt. [Interactive tasks](workflows.md#interactive-tasks) treat a reply that is not a result as conversation instead.

Agent permission and input requests park the attempt as `awaiting_permission` or `awaiting_input` until they are answered with `dagmar answer`.

### Model, mode and effort

`model` is applied through the agent's ACP model selector (`session/set_config_option`) on every new or loaded session. The attempt fails if the agent has no model selector (`acp_model_unsupported`), does not offer that exact value (`acp_model_unknown`, whose message lists the offered values), or does not switch (`acp_model_not_applied`).

`mode` is applied the same way through the agent's mode selector, the config option with category `mode`, and fails with `acp_mode_unsupported`, `acp_mode_unknown` or `acp_mode_not_applied`. For `claude-agent-acp` the mode is its permission mode; for example, `acceptEdits` accepts file edits without asking.

`effort` is applied the same way through the config option with category `thought_level`, after the model, because the levels an agent offers depend on the model. It fails with `acp_thought_level_unsupported`, `acp_thought_level_unknown` or `acp_thought_level_not_applied`. Dagmar sets the effort even when the agent already shows that value, since an agent may show an effort it does not apply. For `claude-agent-acp` the levels are those of the selected model, for example `low`, `medium` and `high`; its `default` level leaves the effort to the agent. Without `effort`, the agent picks its own.

### Tools

`tools` lists the tools the agent is offered, by name: built-in tools such as `Read` or `Grep`, and MCP tools as `mcp__<server>__<tool>`. Every listed tool runs without a permission request, and no unlisted built-in tool is offered. `tools: []` offers no tool at all; without `tools`, the agent offers the tools its own settings give it.

Dagmar passes the list on every new or loaded session through `claude-agent-acp`'s session options (`_meta.claudeCode.options`), so it needs that agent: any other agent would ignore the options and offer every tool, so it fails the attempt before the session with `acp_tools_unsupported`. The session is recorded with an `acp_tools_selected` transcript event.

Claude Code narrows MCP tools only by server. The list keeps the MCP servers it names, from the user's or the project's settings, and drops every other server and the tools a Claude Code plugin registers; a kept server offers all its tools, and its unlisted ones ask for permission. A list with no MCP tool drops every MCP server. Dagmar keeps the named servers through Claude Code's policy-tier `allowedMcpServers` setting, which Claude Code ignores on a machine whose administrator manages its settings; there, the other servers stay offered.

### Continuing a session

Executors that differ only in `model`, `mode`, `effort` or `tools` run the same agent, so a task on one can [continue](workflows.md#sessions) a task's session on the other.

An agent may answer `session/load` before its own MCP servers have connected. `claude-agent-acp` connects the stdio servers of a project `.mcp.json` in the background, and a first prompt sent in that window can tell the model that those servers' tools were removed. ACP offers no readiness signal, so an executor may set `loadSettleMs`, an integer from 1 to 120000: a continuing task on that executor waits that long after `session/load` before its first prompt, recorded as an `acp_load_settle` transcript event. New sessions do not wait, and cancelling the task interrupts the wait. Set it above the slowest server's connection time.

### Retry

An agent can fail a task with a transient error before it has done anything, for example when it cannot confirm the selected model with its API. Dagmar runs such a task again, by default up to 2 times, 300 seconds apart. An executor may set its own `retry: { max, delaySeconds }` (non-negative integers); `retry: { max: 0, delaySeconds: 0 }` turns retries off.

A retry applies only when the agent answered one of Dagmar's requests (session setup, model, mode or effort selection, or the prompt) with an error before its first tool call, output (message, thought or plan), permission request or elicitation, so no work is left to undo; the error's text does not matter. A failure after any work, a lost connection, or `acp_authentication_required` is never retried.

The failed attempt stays `failed` in the task's attempt history, and a new attempt starts after `delaySeconds`, recorded as an `attempt_retry` transcript event. After `max` retries the task fails. Cancelling the task interrupts the wait. A daemon restart during the wait fails the new attempt with `executor_lost`, and a `resume` starts the retry count again.
