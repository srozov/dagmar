# Changelog

Notable changes to Dagmar, by version. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html): before 1.0, a breaking change or a new feature raises the minor version, and a release of only fixes, documentation or tooling raises the patch version.

## [Unreleased]

### Added

- `timeoutSeconds` on a task, at most 2147483 (about 24 days), limits how long one attempt may run; an attempt past it is cancelled and fails with `task_timeout` ([#52](https://github.com/srozov/dagmar/pull/52)).

## [0.9.1] - 2026-11-25

### Changed

- An MIT license, a shorter README, and reference documentation in `docs/`: workflows, executors, running Dagmar, and the design ([#59](https://github.com/srozov/dagmar/pull/59)).
- This changelog, and a release workflow that publishes a GitHub Release for each version tag.

### Fixed

- An ACP executor whose `run` command cannot be started fails its attempt with `acp_spawn_failed` instead of stopping the daemon; a missing `cwd` is named in the error ([#58](https://github.com/srozov/dagmar/pull/58)).
- Task schemas are compiled once instead of on every workflow read, which leaked memory on every scheduler step and failed a schema with an `$id` on its second read ([#58](https://github.com/srozov/dagmar/pull/58)).
- Names that only exist on `Object.prototype`, such as `constructor`, are rejected as executors, dependencies and rerun targets, and never resolve as input or guard paths ([#58](https://github.com/srozov/dagmar/pull/58)).

## [0.9.0] - 2026-11-16

### Added

- `tools` on an ACP executor names the tools its agent may use ([#50](https://github.com/srozov/dagmar/pull/50)).

### Fixed

- A run whose workflow renamed or removed a task can still be read ([#54](https://github.com/srozov/dagmar/pull/54)).

## [0.8.0] - 2026-11-04

### Added

- `effort` on an ACP executor sets the agent's thought level ([#48](https://github.com/srozov/dagmar/pull/48)).
- An `outputSchema` failure names each violation by path ([#46](https://github.com/srozov/dagmar/pull/46)).
- The `$run.id` input reference ([#42](https://github.com/srozov/dagmar/pull/42)).
- A failed process's error ends with the tail of its standard error and names the signal that killed it ([#40](https://github.com/srozov/dagmar/pull/40)).

### Fixed

- A reference nested inside an input value is rejected at validation instead of reaching the task as a literal string ([#45](https://github.com/srozov/dagmar/pull/45)).

## [0.7.0] - 2026-10-31

### Added

- An ACP attempt that fails before doing any work is retried, by default up to 2 times, 300 seconds apart ([#37](https://github.com/srozov/dagmar/pull/37), [#38](https://github.com/srozov/dagmar/pull/38)).
- `loadSettleMs` lets a continuing task wait for the agent's MCP servers after `session/load` ([#32](https://github.com/srozov/dagmar/pull/32)).

### Fixed

- Appending to a long transcript no longer rereads the whole file, which made a long agent reply take minutes to record ([#31](https://github.com/srozov/dagmar/pull/31)).

## [0.6.0] - 2026-10-15

### Added

- Map tasks: `map: { over, key, concurrency }` runs a task once per item of a list, a bounded number at a time ([#30](https://github.com/srozov/dagmar/pull/30)).
- A cancelled run can be resumed, and any finished run can be resumed from a task with `resume --from` ([#27](https://github.com/srozov/dagmar/pull/27)).

## [0.5.0] - 2026-10-12

### Changed

- **Breaking:** `rerun: { from, max, atMax?, when? }` replaces `loop` ([#24](https://github.com/srozov/dagmar/pull/24)).

### Fixed

- A resume that blocks again no longer hangs the daemon, and a blocked run can be cancelled ([#25](https://github.com/srozov/dagmar/pull/25)).

## [0.4.0] - 2026-10-11

### Changed

- **Breaking:** executors are declared in each workflow instead of the daemon config; a run takes its working directory from `--cwd`; an executor can load an `envFile` ([#23](https://github.com/srozov/dagmar/pull/23)).
- A loop reruns everything downstream of its target ([#22](https://github.com/srozov/dagmar/pull/22)).

## [0.3.0] - 2026-09-29

### Added

- The model and permission mode are chosen per ACP executor ([#18](https://github.com/srozov/dagmar/pull/18)).
- An invalid ACP result gets one repair prompt on the same session ([#19](https://github.com/srozov/dagmar/pull/19)).
- A review-loop example workflow and its acceptance runbook ([#20](https://github.com/srozov/dagmar/pull/20)).

### Fixed

- The result is parsed from the turn's last agent message only ([#17](https://github.com/srozov/dagmar/pull/17)).

## [0.2.0] - 2026-09-18

### Added

- A task can continue an earlier task's ACP session through `session/load` ([#12](https://github.com/srozov/dagmar/pull/12)).
- Interactive, multi-turn ACP tasks ([#13](https://github.com/srozov/dagmar/pull/13)).
- Human gates, persisted across restarts ([#14](https://github.com/srozov/dagmar/pull/14)).
- `when` guards, with skipped tasks cascading to their dependants ([#15](https://github.com/srozov/dagmar/pull/15)).
- Bounded loops ([#16](https://github.com/srozov/dagmar/pull/16)).

## [0.1.0] - 2026-09-11

### Added

- The runtime: static YAML workflows, process and ACP executors, concurrent dependency-ready tasks, a SQLite ledger, JSONL transcripts, a loopback WebSocket JSON-RPC API and a CLI.
- An ACP example workflow and a config template ([#4](https://github.com/srozov/dagmar/pull/4)), and CI ([#10](https://github.com/srozov/dagmar/pull/10)).

### Fixed

- Six code-review findings in the initial runtime, including a daemon crash when a process does not read its input ([#1](https://github.com/srozov/dagmar/pull/1)).
- A cancelled run shows its unstarted tasks as cancelled ([#5](https://github.com/srozov/dagmar/pull/5)).
- ACP `auth_required` is reported as a clear error ([#6](https://github.com/srozov/dagmar/pull/6)).

[Unreleased]: https://github.com/srozov/dagmar/compare/v0.9.1...HEAD
[0.9.1]: https://github.com/srozov/dagmar/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/srozov/dagmar/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/srozov/dagmar/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/srozov/dagmar/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/srozov/dagmar/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/srozov/dagmar/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/srozov/dagmar/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/srozov/dagmar/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/srozov/dagmar/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/srozov/dagmar/releases/tag/v0.1.0
