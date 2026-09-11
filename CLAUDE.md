## Project overview

Dagmar is a small headless TypeScript DAG runtime that loads static YAML workflows, concurrently runs dependency-ready tasks through ACP or local processes, and persists runs in SQLite. It exposes a loopback WebSocket JSON-RPC API and thin CLI.

## Engineering principles

These are implementation constraints, not slogans. Apply them by default.

- **KISS:** Prefer the minimum straightforward code that solves the accepted problem: explicit control flow, typed interfaces, and localized error paths over clever or implicit behavior.
- **YAGNI:** You Aren't Gonna Need It – do not add configuration, feature flags, interfaces, workflow branches, or abstractions without a concrete accepted v0 use case. Dagmar makes no backward-compatibility promise to earlier drafts, legacy code, libraries, APIs, or workflow formats; do not preserve obsolete behavior. Reject unsupported paths explicitly rather than offering partial or fake support.
- **DRY, after the rule of three:** Keep small local duplication when it is clearer. Extract a shared utility only after the pattern has appeared at least three times and stabilized.
- **Focused boundaries:** Keep modules responsible for one concern. Use narrow interfaces; do not create god modules that mix scheduling policy, transport, persistence, and executor behavior.
- **Fail fast and explicitly:** Surface unsupported or unsafe states with clear errors. Never silently swallow failures, broaden permissions, or introduce an undocumented fallback.
- **Lifecycle safety:** Never mutate non-terminal persisted run or task state solely because it appears stale or its owner is ambiguous. Follow the explicit recovery and lifecycle rules in the repository specification instead.
- **Determinism and reversibility:** Keep validation reproducible, avoid unguarded timing or network-dependent tests, and make changes small enough to review and roll back safely.
- **Surgical scope:** Change only what the task requires. Do not refactor, reformat, or remove unrelated code; remove only imports, variables, or functions made unused by the current change.
- **Think before coding:** State material assumptions and tradeoffs. Do not silently choose between interpretations that would materially change the design; surface the ambiguity and ask.
- **Verifiable goals:** Define concrete success criteria before a multi-step change and verify each meaningful step rather than declaring the task done on plausibility alone.


## Package management

- Use `pnpm` for dagmar dependency management and project scripts.
- Commit and maintain `pnpm-lock.yaml`; do not create or update `package-lock.json` or use Yarn.
- Use `pnpm install` when intentionally changing dependencies; use
  `pnpm install --frozen-lockfile` for verification after the lockfile exists.
- Do not install dagmar dependencies globally. Host-provided executables such
  as `openclaw` and `claude-agent-acp` are configured by absolute path in
  dagmar’s machine-local config.

### Commit strategy
- Use "Conventional Commits" style for commit messages. Each commit should have a clear, imperative subject line and a body that explains the reasoning behind the change.
- Work in small, cohesive commits. Each commit should implement one logical change and have an imperative, descriptive subject.
- Stage only files relevant to the current task. Do not commit credentials, local configuration, generated artifacts, or unrelated changes.
- Commit completed feature work only to its `claude/<feature>` branch. Do not merge, rebase, force-push, reset shared branches, delete worktrees, or update `main` unless the user explicitly asks.
- Before asking to integrate a feature, provide its branch name, commit list, and test results for review.

### Pull requests

- Before proposing a pull request, inspect the branch diff against `main` and ensure its commits are focused, the documentation reflects behavior changes, and no secrets or unrelated files are included.
- A pull-request description must state the problem and approach, the user-visible or behavioral changes, tests/checks run, and known limitations or follow-up work.
- Do not push a branch or open a pull request unless the user explicitly asks.
