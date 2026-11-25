# Examples

| File | What it shows |
|---|---|
| [`process-workflow.yaml`](process-workflow.yaml) | Two process tasks in a chain, the second reading the first's output. Runs without an agent; see the [quick start](../README.md#quick-start). |
| [`process-result.mjs`](process-result.mjs) | A minimal process task: reads its inputs on stdin and prints one result. |
| [`acp-workflow.yaml`](acp-workflow.yaml) | One ACP task. Set `run` to the absolute path of an ACP agent such as `claude-agent-acp`. |
| [`review-iteration-loop.yaml`](review-iteration-loop.yaml) | A review loop: implement, verify, fix up until the checks pass, review with a second model that continues the builder's session, then a human gate. [`docs/mvp-acceptance.md`](../docs/mvp-acceptance.md) is its runbook. |
| [`verify.example.mjs`](verify.example.mjs) | The review loop's verify step: runs the test command and reports `passed`. |
| [`acp-smoke.mjs`](acp-smoke.mjs) | Runs the ACP executor once against an agent, without the daemon: `node examples/acp-smoke.mjs <cwd> <agent command...>`. |
