# Codex remote development workflow

These instructions apply **only to Codex running from this Mac**. They do not apply to Claude or other agents already running locally on `agi01`; those agents should use their own local working directory and command environment.

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

This repository lives on the remote host **agi01**. Its local SSHFS mount is:

```text
/Users/semion/Volumes/agi01/dagmar
```

## Files

- Read and edit repository files through the local mounted path above.
- Use the normal local editing workflow (for example, `apply_patch`) against that path.
- Do not write repository files through an SSH shell unless the user explicitly asks for that.

## Commands

- Run project commands on the remote host, not on the Mac.
- Connect with Tailscale SSH as `agi01`:

  ```sh
  tailscale ssh agi01@100.70.202.1 -- 'cd /home/agi01/dagmar && <command>'
  ```

- Use `/home/agi01/dagmar` as the remote working directory. It is the same repository exposed locally at `/Users/semion/Volumes/agi01/dagmar`.
- Before testing, inspecting dependencies, or running Git commands, ensure the command is executed on `agi01` through Tailscale SSH.

## Parallel feature work and Git

- Treat `/home/agi01/dagmar` (branch `main`) as the clean integration checkout. Do not implement features directly in it.
- Give every independent Codex feature task its own remote Git worktree and feature branch:

  ```text
  Remote worktree: /home/agi01/worktrees/dagmar/<feature>
  Local mount path: /Users/semion/Volumes/agi01/worktrees/dagmar/<feature>
  Branch: codex/<feature>
  ```

- Create and manage worktrees with Git commands on `agi01`; read and edit their files through the corresponding local mounted path.
- Before editing, run `git status --short` in the relevant worktree. Preserve any changes that do not belong to the current task.
- Avoid assigning separate agents to overlapping files. Agree on shared interfaces before implementing dependent features.

### Commit strategy

- Work in small, cohesive commits. Each commit should implement one logical change and have an imperative, descriptive subject.
- Stage only files relevant to the current task. Do not commit credentials, local configuration, generated artifacts, or unrelated changes.
- Commit completed feature work only to its `codex/<feature>` branch. Do not merge, rebase, force-push, reset shared branches, delete worktrees, or update `main` unless the user explicitly asks.
- Before asking to integrate a feature, provide its branch name, commit list, and test results for review.

### Pull requests

- Before proposing a pull request, inspect the branch diff against `main` and ensure its commits are focused, the documentation reflects behavior changes, and no secrets or unrelated files are included.
- A pull-request description must state the problem and approach, the user-visible or behavioral changes, tests/checks run, and known limitations or follow-up work.
- Do not push a branch or open a pull request unless the user explicitly asks.
