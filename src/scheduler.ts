import { randomUUID } from "node:crypto";
import type { AcpRequest, Execution, Executor, Hooks, InteractionRequest, ProcessRequest, Settlement } from "./executors/types.js";
import { Store } from "./store.js";
import { Transcripts } from "./transcript.js";
import type { AttemptRow, Config, DagmarEvent, ExecutorProfile, Interaction, Json, JsonObject, RunRow, RunStatus, RunView, TaskDef, TaskError, TaskState, Workflow } from "./types.js";
import { DagmarError } from "./types.js";
import { WorkflowRepository, resolveInputs } from "./workflow.js";

type Events = { readonly current: number; publish(event: Omit<DagmarEvent, "sequence" | "timestamp">): DagmarEvent };
type Executors = { process: Executor<ProcessRequest>; acp: Executor<AcpRequest> };
type Live = { runId: string; handle?: Execution; ready: Promise<Execution>; finished: Promise<void> };
type Pending = { view: Interaction; request: InteractionRequest; resolve(value: Json): void; reject(error: Error): void };
const active = new Set(["running", "awaiting_permission", "awaiting_input"]);
const blocking = new Set(["failed", "blocked", "cancelled"]);

export class Scheduler {
  private readonly tails = new Map<string, Promise<unknown>>();
  private readonly live = new Map<string, Live>();
  private readonly pending = new Map<string, Pending>();
  private stopping = false;
  private stopPromise?: Promise<void>;
  constructor(private readonly store: Store, private readonly transcripts: Transcripts, private readonly workflows: WorkflowRepository, private readonly events: Events, private readonly profiles: Config["executors"], private readonly executors: Executors, private readonly env: NodeJS.ProcessEnv = process.env) {}

  async start(workflowId: string, input: Json): Promise<{ workflowRunId: string; status: "running" }> {
    if (this.stopping) throw new DagmarError("daemon_shutting_down", "Daemon is shutting down");
    await this.workflows.get(workflowId);
    const id = `wr_${randomUUID()}`, now = timestamp();
    this.store.insertRun({ id, workflowId, input, status: "running", startedAt: now, updatedAt: now, endedAt: null });
    this.emitRun(id, "running");
    this.background(this.serial(id, () => this.advance(id, false, false)));
    return { workflowRunId: id, status: "running" };
  }
  list(): Omit<RunRow, "input">[] { return this.store.runs().map(({ input: _, ...run }) => run); }
  get(id: string): Promise<RunView> { return this.serial(id, async () => this.view(id, await this.definition(id))); }
  resume(id: string): Promise<RunView> { return this.serial(id, async () => { const run = this.requiredRun(id); if (run.status !== "blocked") throw new DagmarError("invalid_run_state", "Only blocked runs can resume"); await this.advance(id, true, true); return this.view(id, await this.definition(id)); }); }

  async cancelRun(id: string): Promise<RunView> {
    const handles = await this.serial(id, async () => {
      const run = this.requiredRun(id); if (run.status === "cancelled") return [];
      if (run.status !== "running" && run.status !== "waiting") throw new DagmarError("invalid_run_state", "Run is terminal");
      const attempts = this.store.attempts(id).filter((x) => active.has(x.status)), now = timestamp();
      this.store.transaction(() => { for (const a of attempts) this.store.updateAttempt(a.id, { status: "cancelled", updatedAt: now, endedAt: now }); this.store.updateRun(id, { status: "cancelled", updatedAt: now, endedAt: now }); });
      for (const a of attempts) { this.emitTask(a, "cancelled"); this.dropInteraction(a.id); }
      this.emitRun(id, "cancelled");
      return attempts.map((a) => a.id);
    });
    await Promise.all(handles.map((id) => this.cancelLive(id)));
    return this.get(id);
  }

  async cancelTask(taskRunId: string): Promise<RunView> {
    const first = this.store.attempt(taskRunId); if (!first) throw new DagmarError("task_run_not_found", "Task run was not found");
    let cancelId: string | undefined;
    await this.serial(first.workflowRunId, async () => {
      const attempt = this.store.attempt(taskRunId); if (!attempt || !active.has(attempt.status)) return;
      const workflow = await this.definition(first.workflowRunId), now = timestamp();
      const changed = { ...attempt, status: "cancelled" as const, updatedAt: now, endedAt: now };
      const status = aggregate(workflow, replace(this.store.attempts(first.workflowRunId), changed));
      this.store.transaction(() => { this.store.updateAttempt(taskRunId, { status: "cancelled", updatedAt: now, endedAt: now }); this.store.updateRun(first.workflowRunId, { status, updatedAt: now, ...(terminal(status) ? { endedAt: now } : {}) }); });
      this.emitTask(attempt, "cancelled"); this.emitRun(first.workflowRunId, status); this.dropInteraction(taskRunId); cancelId = taskRunId;
      if (status === "running") await this.advance(first.workflowRunId, false, false);
    });
    if (cancelId) await this.cancelLive(cancelId); return this.get(first.workflowRunId);
  }

  interactions(): Interaction[] { return [...this.pending.values()].map((x) => x.view); }
  async answer(id: string, response: Json): Promise<RunView> {
    const first = this.pending.get(id); if (!first) throw new DagmarError("interaction_not_found", "Interaction is not pending");
    return this.serial(first.view.workflowRunId, async () => {
      const item = this.pending.get(id); if (!item) throw new DagmarError("interaction_not_found", "Interaction is not pending");
      let value: Json; try { value = item.request.validate(response); } catch { throw new DagmarError("invalid_interaction_response", "Interaction response is invalid"); }
      const a = this.store.attempt(item.view.taskRunId); if (!a || !active.has(a.status)) throw new DagmarError("interaction_not_found", "Interaction is no longer pending");
      const workflow = await this.definition(a.workflowRunId), now = timestamp();
      const changed = { ...a, status: "running" as const, updatedAt: now };
      const status = aggregate(workflow, replace(this.store.attempts(a.workflowRunId), changed));
      this.store.transaction(() => { this.store.updateAttempt(a.id, { status: "running", updatedAt: now }); this.store.updateRun(a.workflowRunId, { status, updatedAt: now }); });
      this.pending.delete(id); this.emitTask(a, "running"); this.events.publish({ type: "interaction.changed", workflowRunId: a.workflowRunId, taskRunId: a.id, data: { interactionId: id, state: "answered" } }); this.emitRun(a.workflowRunId, status); item.resolve(value);
      return this.view(a.workflowRunId, workflow);
    });
  }

  async recover(): Promise<void> {
    for (const runId of new Set(this.store.activeAttempts().map((x) => x.workflowRunId))) await this.serial(runId, () => this.failActiveLocked(runId, "executor_lost", "Executor was lost on daemon restart", false));
  }
  shutdown(): Promise<void> {
    if (this.stopPromise) return this.stopPromise;
    this.stopping = true;
    return this.stopPromise = (async () => {
      const runIds = new Set(this.store.activeAttempts().map((x) => x.workflowRunId));
      const running = [...this.live.values()];
      const taskIds: string[] = [];
      for (const id of runIds) taskIds.push(...await this.serial(id, () => this.failActiveLocked(id, "daemon_shutdown", "Executor stopped during daemon shutdown", true)));
      await Promise.all(taskIds.map((id) => this.cancelLive(id)));
      await Promise.all(running.map((x) => x.finished));
      await Promise.all([...this.tails.values()]);
    })();
  }

  private async advance(id: string, retry: boolean, resuming: boolean): Promise<void> {
    const run = this.requiredRun(id); if (this.stopping || (!resuming && run.status !== "running" && run.status !== "waiting")) return;
    let workflow: Workflow;
    try { workflow = await this.definition(id); } catch (error) { await this.blockDefinition(id, error); return; }
    const existing = this.store.attempts(id), latest = latestAttempts(existing), now = timestamp();
    const ready = Object.entries(workflow.tasks).filter(([taskId, task]) => depsDone(task, latest) && (!latest.has(taskId) || (retry && blocking.has(latest.get(taskId)!.status))));
    const prepared = ready.map(([taskId, task]) => this.prepare(run, taskId, task, latest, now));
    const combined = [...existing, ...prepared.map((x) => x.row)], status = aggregate(workflow, combined);
    if (prepared.length || resuming || status !== run.status) this.store.transaction(() => { for (const item of prepared) this.store.insertAttempt(item.row); this.store.updateRun(id, { status, updatedAt: now, ...(terminal(status) ? { endedAt: now } : { endedAt: null }) }); });
    for (const item of prepared) this.emitTask(item.row, item.row.status);
    if (prepared.length || status !== run.status) this.emitRun(id, status);
    const launchable = [];
    for (const item of prepared) {
      if (!item.request) continue;
      try { await this.append(item.row, { type: "lifecycle", direction: "internal", event: "attempt_started" }); launchable.push(item); }
      catch (error) { await this.settle(item.row.id, { error: taskError("transcript_write_failed", error) }); }
    }
    for (const item of launchable) this.launch(item.row, item.request!);
  }

  private prepare(run: RunRow, taskId: string, task: TaskDef, latest: Map<string, AttemptRow>, now: string): { row: AttemptRow; request?: ProcessRequest | AcpRequest } {
    const profile = this.profiles[task.executor], base = { id: `tr_${randomUUID()}`, workflowRunId: run.id, taskId, attempt: this.store.nextAttempt(run.id, taskId), executorProfile: task.executor, executorType: profile?.type ?? "process", result: null, acpSessionId: null, startedAt: now, updatedAt: now } as const;
    try {
      if (!profile) throw new DagmarError("executor_unavailable", "Executor profile is unavailable");
      const inputs = resolveInputs(task, run.input, Object.fromEntries(task.dependsOn.map((x) => [x, latest.get(x)!.result!.output])));
      const common = { runId: run.id, taskRunId: base.id, taskId, profile: task.executor, cwd: profile.cwd, env: { ...this.env, ...profile.env }, inputs, ...(task.outputSchema === undefined ? {} : { outputSchema: task.outputSchema }) };
      const request = profile.type === "process" ? { ...common, type: "process" as const, run: task.run! } : { ...common, type: "acp" as const, run: profile.run!, prompt: task.prompt! };
      return { row: { ...base, executorType: profile.type, status: "running", error: null, endedAt: null }, request };
    } catch (error) { return { row: { ...base, status: "failed", error: taskError("input_resolution_failed", error), endedAt: now } }; }
  }

  private launch(row: AttemptRow, request: ProcessRequest | AcpRequest): void {
    let ready!: (handle: Execution) => void, failed!: (error: unknown) => void;
    const readyPromise = new Promise<Execution>((resolve, reject) => { ready = resolve; failed = reject; }); readyPromise.catch(() => undefined);
    const live: Live = { runId: row.workflowRunId, ready: readyPromise, finished: Promise.resolve() }; this.live.set(row.id, live);
    live.finished = (async () => {
      try {
        const hooks: Hooks = { transcript: (record) => this.append(row, record), session: (session) => this.serial(row.workflowRunId, async () => { if (this.store.attempt(row.id) && active.has(this.store.attempt(row.id)!.status)) this.store.updateAttempt(row.id, { acpSessionId: session, updatedAt: timestamp() }); }), interact: (item) => this.requestInteraction(row, item) };
        live.handle = request.type === "process" ? await this.executors.process.start(request, hooks) : await this.executors.acp.start(request, hooks); ready(live.handle);
        await live.handle.done.then((value) => this.serial(row.workflowRunId, () => this.settle(row.id, value)));
      } catch (error) { failed(error); await this.serial(row.workflowRunId, () => this.settle(row.id, { error: taskError("executor_failed", error) })); }
    })();
    this.background(live.finished);
  }

  private async settle(id: string, value: Settlement): Promise<void> {
    const old = this.store.attempt(id); if (!old || !active.has(old.status)) { this.live.delete(id); return; }
    let workflow: Workflow; try { workflow = await this.definition(old.workflowRunId); } catch (error) { await this.blockDefinition(old.workflowRunId, error); return; }
    const now = timestamp(), status = "result" in value ? value.result.outcome : "failed", changed = { ...old, status, result: "result" in value ? value.result : null, error: "error" in value ? value.error : null, updatedAt: now, endedAt: now } as AttemptRow;
    const runStatus = aggregate(workflow, replace(this.store.attempts(old.workflowRunId), changed));
    this.store.transaction(() => { this.store.updateAttempt(id, { status, result: changed.result, error: changed.error, updatedAt: now, endedAt: now }); this.store.updateRun(old.workflowRunId, { status: runStatus, updatedAt: now, ...(terminal(runStatus) ? { endedAt: now } : {}) }); });
    this.emitTask(old, status); this.emitRun(old.workflowRunId, runStatus); this.dropInteraction(id); this.live.delete(id);
    if (!this.stopping && runStatus === "running") await this.advance(old.workflowRunId, false, false);
  }

  private requestInteraction(row: AttemptRow, request: InteractionRequest): Promise<Json> {
    let answer!: Promise<Json>;
    return this.serial(row.workflowRunId, async () => {
      const old = this.store.attempt(row.id); if (!old || old.status !== "running") throw new DagmarError("invalid_task_state", "Task cannot request interaction");
      const id = `ix_${randomUUID()}`, now = timestamp(), status = request.kind === "permission" ? "awaiting_permission" : "awaiting_input";
      let resolve!: (value: Json) => void, reject!: (error: Error) => void; answer = new Promise<Json>((a, b) => { resolve = a; reject = b; }); answer.catch(() => undefined);
      const view: Interaction = { id, workflowRunId: row.workflowRunId, taskRunId: row.id, kind: request.kind, method: request.method, request: request.request, createdAt: now };
      const workflow = await this.definition(row.workflowRunId), changed = { ...old, status, updatedAt: now } as AttemptRow, runStatus = aggregate(workflow, replace(this.store.attempts(row.workflowRunId), changed));
      this.store.transaction(() => { this.store.updateAttempt(row.id, { status, updatedAt: now }); this.store.updateRun(row.workflowRunId, { status: runStatus, updatedAt: now }); });
      this.pending.set(id, { view, request, resolve, reject }); this.emitTask(old, status); this.events.publish({ type: "interaction.changed", workflowRunId: row.workflowRunId, taskRunId: row.id, data: { interactionId: id, state: "pending" } }); this.emitRun(row.workflowRunId, runStatus);
    }).then(() => answer);
  }

  private async failActiveLocked(id: string, code: string, message: string, collect: boolean): Promise<string[]> {
      const run = this.store.run(id); if (!run) return [];
      const attempts = this.store.attempts(id).filter((x) => active.has(x.status)), now = timestamp();
      this.store.transaction(() => { for (const a of attempts) this.store.updateAttempt(a.id, { status: "failed", error: { code, message }, updatedAt: now, endedAt: now }); this.store.updateRun(id, { status: "blocked", updatedAt: now, endedAt: now }); });
      for (const a of attempts) { this.emitTask(a, "failed"); this.dropInteraction(a.id); } this.emitRun(id, "blocked");
      const taskIds = attempts.map((x) => x.id);
      if (!collect) for (const taskId of taskIds) this.background(this.cancelLive(taskId));
      return taskIds;
  }

  private async blockDefinition(id: string, error: unknown): Promise<void> { await this.failActiveLocked(id, "workflow_definition_unavailable", error instanceof Error ? error.message : "Workflow definition unavailable", false); }
  private async definition(id: string): Promise<Workflow> { const run = this.requiredRun(id), workflow = await this.workflows.get(run.workflowId), known = new Set(Object.keys(workflow.tasks)); if (this.store.attempts(id).some((x) => !known.has(x.taskId))) throw new DagmarError("workflow_definition_unavailable", "Workflow task set changed"); return workflow; }
  private requiredRun(id: string): RunRow { const run = this.store.run(id); if (!run) throw new DagmarError("run_not_found", `Run ${id} was not found`); return run; }
  private async view(id: string, workflow: Workflow): Promise<RunView> {
    const run = this.requiredRun(id), attempts = this.store.attempts(id), latest = latestAttempts(attempts), states = taskStates(workflow, latest, run.status);
    return { ...run, sequence: this.events.current, tasks: Object.fromEntries(Object.entries(workflow.tasks).map(([taskId, task]) => [taskId, { dependsOn: task.dependsOn, executor: task.executor, state: states.get(taskId)!, attempts: attempts.filter((x) => x.taskId === taskId).map(({ workflowRunId: _, taskId: _t, executorProfile: _p, executorType: _e, ...a }) => a) }])) };
  }
  private append(row: AttemptRow, record: Parameters<Hooks["transcript"]>[0]): Promise<number> { return this.transcripts.append(row.workflowRunId, row.id, record).then((line) => { this.events.publish({ type: "transcript.appended", workflowRunId: row.workflowRunId, taskRunId: row.id, data: { line } }); return line; }); }
  private async cancelLive(taskRunId: string): Promise<void> { const live = this.live.get(taskRunId); if (!live) return; try { await (await live.ready).cancel(); } catch {} }
  private emitRun(id: string, status: RunStatus): void { this.events.publish({ type: "workflow.status_changed", workflowRunId: id, data: { status } }); }
  private emitTask(a: AttemptRow, status: AttemptRow["status"]): void { this.events.publish({ type: "task.status_changed", workflowRunId: a.workflowRunId, taskRunId: a.id, data: { taskId: a.taskId, attempt: a.attempt, status } }); }
  private dropInteraction(taskRunId: string): void { for (const [id, item] of this.pending) if (item.view.taskRunId === taskRunId) { this.pending.delete(id); item.reject(new Error("Interaction cancelled")); this.events.publish({ type: "interaction.changed", workflowRunId: item.view.workflowRunId, taskRunId, data: { interactionId: id, state: "cancelled" } }); } }
  private serial<T>(id: string, fn: () => Promise<T> | T): Promise<T> { const previous = this.tails.get(id) ?? Promise.resolve(); const next = previous.then(fn, fn); this.tails.set(id, next.catch(() => undefined)); return next; }
  private background(promise: Promise<unknown>): void { void promise.catch((error) => process.emitWarning(error instanceof Error ? error.message : String(error))); }
}

function latestAttempts(rows: AttemptRow[]): Map<string, AttemptRow> { const map = new Map<string, AttemptRow>(); for (const row of rows) map.set(row.taskId, row); return map; }
function depsDone(task: TaskDef, latest: Map<string, AttemptRow>): boolean { return task.dependsOn.every((id) => latest.get(id)?.status === "completed"); }
function replace(rows: AttemptRow[], changed: AttemptRow): AttemptRow[] { return rows.map((x) => x.id === changed.id ? changed : x); }
function aggregate(workflow: Workflow, rows: AttemptRow[]): RunStatus { const states = taskStates(workflow, latestAttempts(rows), "running"), values = [...states.values()]; if (values.every((x) => x === "completed")) return "completed"; if (values.some((x) => x === "running" || x === "ready")) return "running"; if (values.some((x) => x === "awaiting_input" || x === "awaiting_permission")) return "waiting"; return "blocked"; }
function taskStates(workflow: Workflow, latest: Map<string, AttemptRow>, runStatus: RunStatus): Map<string, TaskState> {
  const states = new Map<string, TaskState>();
  const state = (id: string): TaskState => { if (states.has(id)) return states.get(id)!; const row = latest.get(id); if (row) { states.set(id, row.status); return row.status; } if (runStatus === "cancelled") return "cancelled"; const deps = workflow.tasks[id]!.dependsOn.map(state); const value = deps.some((x) => ["blocked","failed","cancelled","blocked_by_dependency"].includes(x)) ? "blocked_by_dependency" : deps.every((x) => x === "completed") ? "ready" : "pending"; states.set(id, value); return value; };
  for (const id of Object.keys(workflow.tasks)) state(id); return states;
}
function terminal(status: RunStatus): boolean { return status === "completed" || status === "blocked" || status === "cancelled"; }
function taskError(code: string, error: unknown): TaskError { return { code: error instanceof DagmarError ? error.code : code, message: error instanceof Error ? error.message : code }; }
function timestamp(): string { return new Date().toISOString(); }
