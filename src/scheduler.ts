import { randomUUID } from "node:crypto";
import type { AcpRequest, Execution, Executor, Hooks, InteractionRequest, ProcessRequest, Settlement } from "./executors/types.js";
import { Store } from "./store.js";
import { Transcripts } from "./transcript.js";
import type { AttemptRow, Config, DagmarEvent, ExecutorProfile, Interaction, Json, JsonObject, RunRow, RunStatus, RunView, TaskDef, TaskError, TaskResult, TaskState, Workflow } from "./types.js";
import { DagmarError } from "./types.js";
import { WorkflowRepository, resolveInputs, reference } from "./workflow.js";
import { gateValidator } from "./result.js";

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
  constructor(
    private readonly store: Store,
    private readonly transcripts: Transcripts,
    private readonly workflows: WorkflowRepository,
    private readonly events: Events,
    private readonly profiles: Config["executors"],
    private readonly executors: Executors,
    private readonly env: NodeJS.ProcessEnv = process.env,
  ) {}

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
      // Gate: settle the attempt directly (no executor to resume). result.message is the gate's
      // prompt (a human-readable summary); result.output is the human's validated answer.
      if (item.request.kind === "gate") {
        const result: TaskResult = { outcome: "completed", message: String((item.request.request as { prompt?: string })?.prompt ?? "gate"), output: value };
        const changed = { ...a, status: "completed" as const, result, updatedAt: now, endedAt: now };
        const runStatus = aggregate(workflow, replace(this.store.attempts(a.workflowRunId), changed));
        this.store.transaction(() => {
          this.store.updateAttempt(a.id, { status: "completed", result, updatedAt: now, endedAt: now });
          this.store.updateRun(a.workflowRunId, { status: runStatus, updatedAt: now, ...(terminal(runStatus) ? { endedAt: now } : {}) });
        });
        this.pending.delete(id);
        this.emitTask(a, "completed");
        this.events.publish({ type: "interaction.changed", workflowRunId: a.workflowRunId, taskRunId: a.id, data: { interactionId: id, state: "answered" } });
        this.emitRun(a.workflowRunId, runStatus);
        if (!this.stopping && runStatus === "running") await this.advance(a.workflowRunId, false, false);
        return this.view(a.workflowRunId, workflow);
      }
      // Non-gate: resume the executor by flipping the attempt back to running and resolving its Promise.
      const changed = { ...a, status: "running" as const, updatedAt: now };
      const status = aggregate(workflow, replace(this.store.attempts(a.workflowRunId), changed));
      this.store.transaction(() => { this.store.updateAttempt(a.id, { status: "running", updatedAt: now }); this.store.updateRun(a.workflowRunId, { status, updatedAt: now }); });
      this.pending.delete(id); this.emitTask(a, "running"); this.events.publish({ type: "interaction.changed", workflowRunId: a.workflowRunId, taskRunId: a.id, data: { interactionId: id, state: "answered" } }); this.emitRun(a.workflowRunId, status); item.resolve(value);
      return this.view(a.workflowRunId, workflow);
    });
  }

  async recover(): Promise<void> {
    const allActive = this.store.activeAttempts();
    if (!allActive.length) return;
    // Group active attempts by run so each run is processed once under its serial() lock.
    const byRun = new Map<string, AttemptRow[]>();
    for (const a of allActive) { const arr = byRun.get(a.workflowRunId) ?? []; arr.push(a); byRun.set(a.workflowRunId, arr); }
    for (const [runId, attempts] of byRun) {
      await this.serial(runId, async () => {
        const now = timestamp();
        // Hoist the workflow definition: gates below and the run-status recompute after both need
        // it, and loading once instead of N+1 times keeps recovery cheap for gate-heavy runs.
        let workflow: Workflow | undefined;
        const loadDefinition = async (): Promise<Workflow> => workflow ??= await this.definition(runId);
        const gates: AttemptRow[] = [], nonGates: AttemptRow[] = [];
        for (const a of attempts) (a.executorType === "gate" ? gates : nonGates).push(a);
        // Non-gates: every active attempt was running on a now-dead executor. Fail them all.
        if (nonGates.length) {
          this.store.transaction(() => { for (const a of nonGates) this.store.updateAttempt(a.id, { status: "failed", error: { code: "executor_lost", message: "Executor was lost on daemon restart" }, updatedAt: now, endedAt: now }); });
          for (const a of nonGates) this.emitTask(a, "failed");
          for (const a of nonGates) this.background(this.cancelLive(a.id));
        }
        // Gates: survive restart. Reconstruct the in-memory Pending entry from the persisted
        // attempt row + the workflow definition; the answer is still in flight, just in a fresh
        // process. A gate that isn't actually awaiting_input (e.g. mid-prompt on a previous life)
        // is a real state error and fails with executor_lost.
        for (const a of gates) {
          if (a.status !== "awaiting_input") { this.store.updateAttempt(a.id, { status: "failed", error: { code: "executor_lost", message: "Gate in unexpected state on recovery" }, updatedAt: now, endedAt: now }); this.emitTask(a, "failed"); continue; }
          try { const wf = await loadDefinition(); const task = wf.tasks[a.taskId]; if (!task?.gate) throw new Error("not a gate"); this.registerGate(a, task.gate); }
          catch { this.store.updateAttempt(a.id, { status: "failed", error: { code: "gate_definition_unavailable", message: "Gate definition unavailable after restart" }, updatedAt: now, endedAt: now }); this.emitTask(a, "failed"); }
        }
        // Recompute the run status from the (possibly corrected) attempts. A non-terminal status
        // also clears any endedAt the run may have carried on arrival (e.g. a prior shutdown()
        // that failed only the non-gate attempts), so run.list/run.get never surface a stale
        // (status='waiting', endedAt!=null) view.
        let status: RunStatus;
        try { status = aggregate(await loadDefinition(), this.store.attempts(runId)); }
        catch { status = "blocked"; }
        this.store.updateRun(runId, { status, updatedAt: now, ...(terminal(status) ? { endedAt: now } : { endedAt: null }) });
        this.emitRun(runId, status);
      });
    }
  }
  shutdown(): Promise<void> {
    if (this.stopPromise) return this.stopPromise;
    this.stopping = true;
    return this.stopPromise = (async () => {
      const runIds = new Set(this.store.activeAttempts().map((x) => x.workflowRunId));
      const running = [...this.live.values()];
      const taskIds: string[] = [];
      for (const id of runIds) taskIds.push(...await this.serial(id, () => this.failActiveLocked(id, "daemon_shutdown", "Executor stopped during daemon shutdown", true, true)));
      await Promise.all(taskIds.map((id) => this.cancelLive(id)));
      await Promise.all(running.map((x) => x.finished));
      await Promise.all([...this.tails.values()]);
    })();
  }

  private async advance(id: string, retry: boolean, resuming: boolean): Promise<void> {
    const run = this.requiredRun(id);
    if (this.stopping || (!resuming && run.status !== "running" && run.status !== "waiting")) return;
    let workflow: Workflow;
    try { workflow = await this.definition(id); }
    catch (error) { await this.blockDefinition(id, error); return; }
    const existing = this.store.attempts(id);
    const latest = latestAttempts(existing);
    // Loop machinery: build the per-workflow loops map (keyed by target) once and an insertion-order
    // position map from the rowid-ordered `existing`. Body re-arm comparisons only need already-persisted
    // attempts (rows prepared this pass are `running`/terminal, not re-arm candidates in this scan).
    const loops = loopsOf(workflow);
    const pos = new Map(existing.map((r, i) => [r.id, i]));
    const now = timestamp();
    // Decide every task whose deps have settled (completed or skipped). A `skipped` decision settles
    // synchronously, so re-scanning lets a skip cascade to its dependants within this same advance;
    // runnable/gate tasks are prepared once and their dependants wait for the async settle. Terminates:
    // a decided row is no longer a candidate (its status is not in `blocking`), EXCEPT for the loop's
    // own re-arm (a terminal completed/skipped attempt may be re-prepared when loopRearm says so).
    const prepared: Array<{ row: AttemptRow; request?: ProcessRequest | AcpRequest }> = [];
    for (;;) {
      let progressed = false;
      for (const [taskId, task] of Object.entries(workflow.tasks)) {
        const current = latest.get(taskId);
        let candidate = !current || (retry && blocking.has(current.status));
        if (!candidate && current && (current.status === "completed" || current.status === "skipped"))
          candidate = this.loopRearm(taskId, task, current, latest, pos, existing, loops);
        if (!candidate || !this.depsSettled(workflow, task, taskId, latest, loops, existing, run)) continue;
        const item = this.prepare(run, taskId, task, latest, now);
        prepared.push(item); latest.set(taskId, item.row); progressed = true;
      }
      if (!progressed) break;
    }
    const combined = [...existing, ...prepared.map((x) => x.row)];
    const status = aggregate(workflow, combined);
    if (prepared.length || resuming || status !== run.status) {
      this.store.transaction(() => {
        for (const item of prepared) this.store.insertAttempt(item.row);
        this.store.updateRun(id, { status, updatedAt: now, ...(terminal(status) ? { endedAt: now } : { endedAt: null }) });
      });
    }
    for (const item of prepared) this.emitTask(item.row, item.row.status);
    if (prepared.length || status !== run.status) this.emitRun(id, status);
    const launchable = [];
    for (const item of prepared) {
      if (!item.request) continue;
      try { await this.append(item.row, { type: "lifecycle", direction: "internal", event: "attempt_started" }); launchable.push(item); }
      catch (error) { await this.settle(item.row.id, { error: taskError("transcript_write_failed", error) }); }
    }
    for (const item of launchable) this.launch(item.row, item.request!);
    for (const item of prepared) {
      if (item.row.executorType !== "gate" || item.row.status !== "awaiting_input") continue;
      try {
        await this.append(item.row, { type: "lifecycle", direction: "internal", event: "gate_parked" });
        this.registerGate(item.row, workflow.tasks[item.row.taskId]!.gate!);
      } catch (error) { await this.settle(item.row.id, { error: taskError("transcript_write_failed", error) }); }
    }
  }

  private prepare(run: RunRow, taskId: string, task: TaskDef, latest: Map<string, AttemptRow>, now: string): { row: AttemptRow; request?: ProcessRequest | AcpRequest } {
    // Guard branch first: a task whose `when` clauses are unsatisfied becomes a terminal, non-launching
    // skipped attempt. The real executorType/executorProfile are preserved so view() surfaces the
    // task's kind accurately. Sits ABOVE the gate branch so a conditional gate is decided by the
    // guard, not parked.
    if (task.when && task.when.length && !this.guardPasses(task, run.input, latest)) {
      const isGate = Boolean(task.gate);
      const executorType = isGate ? "gate" as const : (this.profiles[task.executor!]?.type ?? "process");
      const executorProfile = isGate ? "gate" : task.executor!;
      return { row: { id: `tr_${randomUUID()}`, workflowRunId: run.id, taskId, attempt: this.store.nextAttempt(run.id, taskId), executorProfile, executorType, status: "skipped" as const, result: null, error: null, acpSessionId: null, startedAt: now, updatedAt: now, endedAt: now } };
    }
    if (task.gate) {
      const base = { id: `tr_${randomUUID()}`, workflowRunId: run.id, taskId, attempt: this.store.nextAttempt(run.id, taskId), executorProfile: "gate", executorType: "gate" as const, result: null, acpSessionId: null, startedAt: now, updatedAt: now } as const;
      try {
        resolveInputs(task, run.input, this.depOutputs(task, latest));
        return { row: { ...base, status: "awaiting_input" as const, error: null, endedAt: null } };
      } catch (error) {
        return { row: { ...base, status: "failed" as const, error: taskError("input_resolution_failed", error), endedAt: now } };
      }
    }
    const profile = this.profiles[task.executor!];
    const base = { id: `tr_${randomUUID()}`, workflowRunId: run.id, taskId, attempt: this.store.nextAttempt(run.id, taskId), executorProfile: task.executor!, executorType: profile?.type ?? "process", result: null, acpSessionId: null, startedAt: now, updatedAt: now } as const;
    try {
      if (!profile) throw new DagmarError("executor_unavailable", "Executor profile is unavailable");
      const inputs = resolveInputs(task, run.input, this.depOutputs(task, latest));
      const common = { runId: run.id, taskRunId: base.id, taskId, profile: task.executor!, cwd: profile.cwd, env: { ...this.env, ...profile.env }, inputs, ...(task.outputSchema === undefined ? {} : { outputSchema: task.outputSchema }) };
      let loadSessionId: string | undefined;
      if (task.session?.mode === "continue") {
        // Validation guarantees `from` is a completed dependency, so latest.get(from) is its
        // completed attempt. A null acpSessionId is a real source-side failure (e.g. the source
        // task errored before persisting an id) — surface it explicitly, never fall back to fresh.
        const sid = latest.get(task.session.from)?.acpSessionId;
        if (!sid) throw new DagmarError("continuation_unavailable", `Source session id for ${task.session.from} is unavailable`);
        loadSessionId = sid;
      }
      const request = profile.type === "process"
        ? { ...common, type: "process" as const, run: task.run! }
        : { ...common, type: "acp" as const, run: profile.run!, prompt: task.prompt!, ...(loadSessionId ? { loadSessionId } : {}), ...(task.interactive ? { interactive: true } : {}) };
      return { row: { ...base, executorType: profile.type, status: "running", error: null, endedAt: null }, request };
    } catch (error) {
      // taskError preserves a DagmarError's own code (e.g. continuation_unavailable, executor_unavailable),
      // so validation/scheduling failures surface with their semantic code instead of a generic one.
      return { row: { ...base, status: "failed", error: taskError("input_resolution_failed", error), endedAt: now } };
    }
  }

  // Skip-safe replacement for the inline output-map builds. Excludes deps whose latest attempt
  // has no result (e.g. skipped), so a no-guard task referencing a skipped dep output gets
  // input_resolution_failed instead of a null-deref.
  private depOutputs(task: TaskDef, latest: Map<string, AttemptRow>): Record<string, Json> {
    const out: Record<string, Json> = {};
    for (const dep of task.dependsOn) { const r = latest.get(dep)?.result; if (r) out[dep] = r.output; }
    return out;
  }

  // Evaluate every `when` clause. Validation guarantees each ref is well-formed and every $tasks
  // ref is a declared dependency; depsSettled() guarantees those deps are terminal (completed or
  // skipped) before we get here. A clause whose $tasks ref points to a skipped dep returns false
  // immediately (the output can never exist). Missing paths return undefined -> clause false -> skip.
  // A guardless task (no `when`) is vacuously satisfied: every guard clause passes, so the guard
  // evaluator returns true. This matches the documented "guardless source loops to the cap" assumption
  // used by `loopFinal` (the only caller that does not already short-circuit on empty `when`).
  private guardPasses(task: TaskDef, runInput: Json, latest: Map<string, AttemptRow>): boolean {
    const clauses = task.when;
    if (!clauses || clauses.length === 0) return true;
    for (const clause of clauses) {
      const ref = reference(clause.ref)!;
      let value: Json | undefined;
      if (ref.task) { const dep = latest.get(ref.task)!; if (dep.status === "skipped") return false; value = walk(dep.result?.output, ref.path); }
      else value = walk(runInput, ref.path);
      const ok = clause.in !== undefined ? clause.in.some((v) => jsonEqual(value, v)) : jsonEqual(value, clause.equals!);
      if (!ok) return false;
    }
    return true;
  }

  // Loop re-arm predicate. A task with a TERMINAL (completed/skipped) attempt may be re-prepared when
  // the loop's other side has produced a NEWER completed attempt (rowid insertion order):
  //   (a) the task is the loop's TARGET (loop.to === taskId): re-arm when its SOURCE completed newer.
  //   (b) the task is the loop's SOURCE (task.loop?.to): re-arm when its TARGET completed newer AND
  //       the source has not yet completed `maxVisits` times. Only `completed` counts toward the cap
  //       so a failed+retried or skipped source attempt does not consume a visit (gate #4).
  private loopRearm(taskId: string, task: TaskDef, current: AttemptRow, latest: Map<string, AttemptRow>, pos: Map<string, number>, rows: AttemptRow[], loops: Map<string, { source: string; maxVisits: number }>): boolean {
    const asTarget = loops.get(taskId);
    if (asTarget) {
      const s = latest.get(asTarget.source);
      if (s && s.status === "completed" && pos.get(s.id)! > pos.get(current.id)!) return true;
    }
    if (task.loop) {
      const tgt = latest.get(task.loop.to);
      if (tgt && tgt.status === "completed" && pos.get(tgt.id)! > pos.get(current.id)! && completedCount(taskId, rows) < task.loop.maxVisits) return true;
    }
    return false;
  }

  // Loop-final predicate. The loop is final when the target has a completed attempt and either the
  // source has already exhausted its cap OR the source's exit guard would fire on the target's latest
  // output (target passed; source's `when` is now false). A guardless source (no `when`) only exits
  // at the cap (guardPasses returns true vacuously, so !guardPasses is false).
  private loopFinal(workflow: Workflow, target: string, latest: Map<string, AttemptRow>, rows: AttemptRow[], loops: Map<string, { source: string; maxVisits: number }>, run: RunRow): boolean {
    const t = latest.get(target);
    if (!t || t.status !== "completed") return false;
    const entry = loops.get(target)!;
    if (completedCount(entry.source, rows) >= entry.maxVisits) return true;
    const sourceTask = workflow.tasks[entry.source];
    return sourceTask ? !this.guardPasses(sourceTask, run.input, latest) : false;
  }

  // Loop-aware depsSettled. Body edges (source depends on target; source is a member) settle when the
  // target is completed — same as before. Exit-branch edges (dependants outside the loop) require the
  // loop to be `final`. When `loops` is empty the new check never fires and behavior is identical to
  // the prior implementation.
  private depsSettled(workflow: Workflow, task: TaskDef, taskId: string, latest: Map<string, AttemptRow>, loops: Map<string, { source: string; maxVisits: number }>, rows: AttemptRow[], run: RunRow): boolean {
    for (const dep of task.dependsOn) {
      const d = latest.get(dep);
      if (!d || !(d.status === "completed" || d.status === "skipped")) return false;
      const target = loops.has(dep) ? dep : workflow.tasks[dep]?.loop?.to;
      if (!target) continue;
      const entry = loops.get(target);
      if (!entry) continue;
      const members = new Set([target, entry.source]);
      if (!members.has(taskId) && !this.loopFinal(workflow, target, latest, rows, loops, run)) return false;
    }
    return true;
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
      // Gate attempts never reach this path: their interactions are registered by registerGate(),
      // not requested by an executor. The `awaiting_input` branch below only fires for executor
      // requests whose kind is something other than permission (e.g. "input", "turn").
      const old = this.store.attempt(row.id); if (!old || old.status !== "running") throw new DagmarError("invalid_task_state", "Task cannot request interaction");
      const id = `ix_${randomUUID()}`, now = timestamp(), status = request.kind === "permission" ? "awaiting_permission" : "awaiting_input";
      let resolve!: (value: Json) => void, reject!: (error: Error) => void; answer = new Promise<Json>((a, b) => { resolve = a; reject = b; }); answer.catch(() => undefined);
      const view: Interaction = { id, workflowRunId: row.workflowRunId, taskRunId: row.id, kind: request.kind, method: request.method, request: request.request, createdAt: now };
      const workflow = await this.definition(row.workflowRunId), changed = { ...old, status, updatedAt: now } as AttemptRow, runStatus = aggregate(workflow, replace(this.store.attempts(row.workflowRunId), changed));
      this.store.transaction(() => { this.store.updateAttempt(row.id, { status, updatedAt: now }); this.store.updateRun(row.workflowRunId, { status: runStatus, updatedAt: now }); });
      this.pending.set(id, { view, request, resolve, reject }); this.emitTask(old, status); this.events.publish({ type: "interaction.changed", workflowRunId: row.workflowRunId, taskRunId: row.id, data: { interactionId: id, state: "pending" } }); this.emitRun(row.workflowRunId, runStatus);
    }).then(() => answer);
  }

  // Register a Pending entry for a gate attempt. Gate Pending entries do not await an executor's
  // Promise: answer() detects kind:"gate" and settles the attempt directly, so resolve/reject are
  // never called. The fresh ix_ id is fine — the user re-queries dagmar pending after a restart.
  private registerGate(row: AttemptRow, gate: NonNullable<TaskDef["gate"]>): void {
    const id = `ix_${randomUUID()}`, now = timestamp();
    const validate = gateValidator(gate.schema);
    const view: Interaction = { id, workflowRunId: row.workflowRunId, taskRunId: row.id, kind: "gate", method: "gate/answer", request: { prompt: gate.prompt, ...(gate.schema !== undefined ? { schema: gate.schema } : {}) } as Json, createdAt: now };
    this.pending.set(id, { view, request: { kind: "gate", method: "gate/answer", request: view.request, validate }, resolve: () => {}, reject: () => {} });
    this.events.publish({ type: "interaction.changed", workflowRunId: row.workflowRunId, taskRunId: row.id, data: { interactionId: id, state: "pending" } });
  }

  private async failActiveLocked(id: string, code: string, message: string, collect: boolean, skipGates = false): Promise<string[]> {
      const run = this.store.run(id); if (!run) return [];
      const attempts = this.store.attempts(id).filter((x) => active.has(x.status) && !(skipGates && x.executorType === "gate")), now = timestamp();
      if (!attempts.length) return [];
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
    const run = this.requiredRun(id);
    const attempts = this.store.attempts(id);
    const latest = latestAttempts(attempts);
    const states = taskStates(workflow, latest, run.status);
    const tasks = Object.fromEntries(Object.entries(workflow.tasks).map(([taskId, task]) => [taskId, {
      dependsOn: task.dependsOn,
      executor: task.executor ?? "gate",
      state: states.get(taskId)!,
      attempts: attempts.filter((x) => x.taskId === taskId).map(({ workflowRunId: _, taskId: _t, executorProfile: _p, executorType: _e, ...a }) => a),
    }]));
    return { ...run, sequence: this.events.current, tasks };
  }
  private append(row: AttemptRow, record: Parameters<Hooks["transcript"]>[0]): Promise<number> { return this.transcripts.append(row.workflowRunId, row.id, record).then((line) => { this.events.publish({ type: "transcript.appended", workflowRunId: row.workflowRunId, taskRunId: row.id, data: { line } }); return line; }); }
  private async cancelLive(taskRunId: string): Promise<void> { const live = this.live.get(taskRunId); if (!live) return; try { await (await live.ready).cancel(); } catch {} }
  private emitRun(id: string, status: RunStatus): void { this.events.publish({ type: "workflow.status_changed", workflowRunId: id, data: { status } }); }
  private emitTask(a: AttemptRow, status: AttemptRow["status"]): void { this.events.publish({ type: "task.status_changed", workflowRunId: a.workflowRunId, taskRunId: a.id, data: { taskId: a.taskId, attempt: a.attempt, status } }); }
  private dropInteraction(taskRunId: string): void { for (const [id, item] of this.pending) if (item.view.taskRunId === taskRunId) { this.pending.delete(id); item.reject(new Error("Interaction cancelled")); this.events.publish({ type: "interaction.changed", workflowRunId: item.view.workflowRunId, taskRunId, data: { interactionId: id, state: "cancelled" } }); } }
  private serial<T>(id: string, fn: () => Promise<T> | T): Promise<T> {
    const previous = this.tails.get(id) ?? Promise.resolve();
    const next = previous.then(fn, fn);
    const guarded = next.catch(() => undefined);
    this.tails.set(id, guarded);
    // Evict the tail once this run is idle (no newer op has replaced it) so the map does
    // not accumulate one closure-holding entry per run for the daemon's lifetime.
    void guarded.then(() => { if (this.tails.get(id) === guarded) this.tails.delete(id); });
    return next;
  }
  private background(promise: Promise<unknown>): void { void promise.catch((error) => process.emitWarning(error instanceof Error ? error.message : String(error))); }
}

function latestAttempts(rows: AttemptRow[]): Map<string, AttemptRow> { const map = new Map<string, AttemptRow>(); for (const row of rows) map.set(row.taskId, row); return map; }
// Per-workflow loops map, keyed by target. The source carries `loop: { to, maxVisits }`; the inverse
// map lets the scheduler answer "who loops to this target?" in O(1) for both the re-arm predicate
// and the exit-branch gating in depsSettled. Empty for loop-free workflows (no behavioral change).
function loopsOf(workflow: Workflow): Map<string, { source: string; maxVisits: number }> {
  const m = new Map<string, { source: string; maxVisits: number }>();
  for (const [id, t] of Object.entries(workflow.tasks)) if (t.loop) m.set(t.loop.to, { source: id, maxVisits: t.loop.maxVisits });
  return m;
}
// Counts `completed` attempts of a task across ALL its rows (not just latest). Only `completed`
// counts toward the loop cap (a failed+retried or skipped attempt does not consume a visit).
function completedCount(taskId: string, rows: AttemptRow[]): number {
  let n = 0;
  for (const r of rows) if (r.taskId === taskId && r.status === "completed") n++;
  return n;
}
function replace(rows: AttemptRow[], changed: AttemptRow): AttemptRow[] { return rows.map((x) => x.id === changed.id ? changed : x); }
function aggregate(workflow: Workflow, rows: AttemptRow[]): RunStatus { const states = taskStates(workflow, latestAttempts(rows), "running"), values = [...states.values()]; if (values.every((x) => x === "completed" || x === "skipped")) return "completed"; if (values.some((x) => x === "running" || x === "ready")) return "running"; if (values.some((x) => x === "awaiting_input" || x === "awaiting_permission")) return "waiting"; return "blocked"; }
function taskStates(workflow: Workflow, latest: Map<string, AttemptRow>, runStatus: RunStatus): Map<string, TaskState> {
  const states = new Map<string, TaskState>();
  const state = (id: string): TaskState => {
    if (states.has(id)) return states.get(id)!;
    const row = latest.get(id);
    if (row) { states.set(id, row.status); return row.status; }
    if (runStatus === "cancelled") { states.set(id, "cancelled"); return "cancelled"; }
    const deps = workflow.tasks[id]!.dependsOn.map(state);
    const value = deps.some((x) => ["blocked", "failed", "cancelled", "blocked_by_dependency"].includes(x)) ? "blocked_by_dependency"
      : deps.every((x) => x === "completed") ? "ready"
      : "pending";
    states.set(id, value);
    return value;
  };
  for (const id of Object.keys(workflow.tasks)) state(id);
  return states;
}
function terminal(status: RunStatus): boolean { return status === "completed" || status === "blocked" || status === "cancelled"; }
function taskError(code: string, error: unknown): TaskError { return { code: error instanceof DagmarError ? error.code : code, message: error instanceof Error ? error.message : code }; }
function timestamp(): string { return new Date().toISOString(); }
function walk(value: Json | undefined, path: string[]): Json | undefined {
  let current = value;
  for (const key of path) { if (!current || typeof current !== "object" || Array.isArray(current) || !(key in current)) return undefined; current = current[key]; }
  return current;
}
function jsonEqual(a: Json | undefined, b: Json | undefined): boolean {
  if (a === b) return true;
  if (a === null || b === null || typeof a !== "object" || typeof b !== "object") return false;
  if (Array.isArray(a) !== Array.isArray(b)) return false;
  if (Array.isArray(a) && Array.isArray(b)) return a.length === b.length && a.every((x, i) => jsonEqual(x, b[i]));
  const ak = Object.keys(a), bk = Object.keys(b as object);
  return ak.length === bk.length && ak.every((k) => Object.hasOwn(b as object, k) && jsonEqual((a as {[k:string]:Json})[k], (b as {[k:string]:Json})[k]));
}
