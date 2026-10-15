import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { DatabaseSync } from "node:sqlite";
import type { AttemptRow, RunRow, TaskError, TaskResult } from "./types.js";
import { DagmarError } from "./types.js";

const SQL = `
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS workflow_runs (
 id TEXT PRIMARY KEY, workflow_id TEXT NOT NULL, input_json TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('running','waiting','completed','blocked','cancelled')),
 started_at TEXT NOT NULL, updated_at TEXT NOT NULL, ended_at TEXT, cwd TEXT
);
CREATE TABLE IF NOT EXISTS task_runs (
 id TEXT PRIMARY KEY, workflow_run_id TEXT NOT NULL REFERENCES workflow_runs(id), task_id TEXT NOT NULL,
 attempt INTEGER NOT NULL CHECK(attempt>=1), executor_profile TEXT NOT NULL,
 executor_type TEXT NOT NULL CHECK(executor_type IN ('acp','process','gate')),
 status TEXT NOT NULL CHECK(status IN ('running','awaiting_permission','awaiting_input','completed','blocked','failed','cancelled','skipped')),
 result_json TEXT, error_json TEXT, acp_session_id TEXT, started_at TEXT NOT NULL, updated_at TEXT NOT NULL, ended_at TEXT, item_key TEXT, round_id TEXT,
 UNIQUE(workflow_run_id,task_id,attempt)
);
CREATE INDEX IF NOT EXISTS task_runs_by_workflow ON task_runs(workflow_run_id,task_id,attempt);`;

type Row = Record<string, unknown>;

// One active run per workflow and working directory. Runs without a cwd share one key per workflow:
// ifnull, because SQLite treats NULLs as distinct in a unique index.
const ACTIVE_RUN_INDEX = `CREATE UNIQUE INDEX IF NOT EXISTS one_active_run_per_workflow_cwd ON workflow_runs(workflow_id,ifnull(cwd,'')) WHERE status IN ('running','waiting')`;

// SQLite extended result code for a UNIQUE-constraint violation. The only such
// index that can fire on insertRun or updateRun is one_active_run_per_workflow_cwd (run ids
// are fresh UUIDs, so the primary key never collides), so this maps to active_run_exists.
const SQLITE_CONSTRAINT_UNIQUE = 2067;

export class Store {
  private readonly db: DatabaseSync;
  constructor(readonly path: string) {
    if (path !== ":memory:") mkdirSync(dirname(path), { recursive: true });
    this.db = new DatabaseSync(path);
    this.db.exec(SQL);
    // Upgrade a database from before runs had a cwd: add the column, and replace the old
    // one-active-run-per-workflow index. Each step is idempotent.
    if (!(this.db.prepare("PRAGMA table_info(workflow_runs)").all() as Row[]).some((column) => column.name === "cwd")) this.db.exec("ALTER TABLE workflow_runs ADD COLUMN cwd TEXT");
    this.db.exec("DROP INDEX IF EXISTS one_active_run_per_workflow");
    this.db.exec(ACTIVE_RUN_INDEX);
    // Upgrade a database from before map tasks: add each attempt's item and round.
    const columns = (this.db.prepare("PRAGMA table_info(task_runs)").all() as Row[]).map((column) => column.name);
    for (const name of ["item_key", "round_id"]) if (!columns.includes(name)) this.db.exec(`ALTER TABLE task_runs ADD COLUMN ${name} TEXT`);
  }
  close(): void {
    if (this.db.isOpen) this.db.close();
  }
  transaction<T>(fn: () => T): T {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const value = fn();
      this.db.exec("COMMIT");
      return value;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  insertRun(run: RunRow): void {
    try {
      this.db.prepare("INSERT INTO workflow_runs(id,workflow_id,cwd,input_json,status,started_at,updated_at,ended_at) VALUES(?,?,?,?,?,?,?,?)").run(run.id, run.workflowId, run.cwd, JSON.stringify(run.input), run.status, run.startedAt, run.updatedAt, run.endedAt);
    } catch (error) {
      if ((error as { errcode?: number }).errcode === SQLITE_CONSTRAINT_UNIQUE) throw new DagmarError("active_run_exists", `Workflow ${run.workflowId} already has an active run${run.cwd === null ? "" : ` in ${run.cwd}`}`);
      throw error;
    }
  }
  insertAttempt(a: AttemptRow): void {
    this.db.prepare("INSERT INTO task_runs(id,workflow_run_id,task_id,attempt,executor_profile,executor_type,status,result_json,error_json,acp_session_id,started_at,updated_at,ended_at,item_key,round_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)").run(a.id, a.workflowRunId, a.taskId, a.attempt, a.executorProfile, a.executorType, a.status, encode(a.result), encode(a.error), a.acpSessionId, a.startedAt, a.updatedAt, a.endedAt, a.item, a.roundId);
  }
  run(id: string): RunRow | undefined {
    const row = this.db.prepare("SELECT * FROM workflow_runs WHERE id=?").get(id) as Row | undefined;
    return row && run(row);
  }
  runs(): RunRow[] {
    return (this.db.prepare("SELECT * FROM workflow_runs ORDER BY started_at DESC").all() as Row[]).map(run);
  }
  attempt(id: string): AttemptRow | undefined {
    const row = this.db.prepare("SELECT * FROM task_runs WHERE id=?").get(id) as Row | undefined;
    return row && attempt(row);
  }
  attempts(runId: string): AttemptRow[] {
    return (this.db.prepare("SELECT * FROM task_runs WHERE workflow_run_id=? ORDER BY rowid").all(runId) as Row[]).map(attempt);
  }
  activeAttempts(): AttemptRow[] {
    return (this.db.prepare("SELECT * FROM task_runs WHERE status IN ('running','awaiting_permission','awaiting_input')").all() as Row[]).map(attempt);
  }
  nextAttempt(runId: string, taskId: string): number {
    return Number((this.db.prepare("SELECT COALESCE(MAX(attempt),0)+1 n FROM task_runs WHERE workflow_run_id=? AND task_id=?").get(runId, taskId) as Row).n);
  }

  updateRun(id: string, patch: Partial<Pick<RunRow, "status" | "updatedAt" | "endedAt">>): RunRow {
    try {
      update(this.db, "workflow_runs", id, patch, { status: "status", updatedAt: "updated_at", endedAt: "ended_at" });
    } catch (error) {
      // A resumed run becoming active again while its workflow has another active run in its cwd.
      const run = this.run(id);
      if ((error as { errcode?: number }).errcode === SQLITE_CONSTRAINT_UNIQUE && run) throw new DagmarError("active_run_exists", `Workflow ${run.workflowId} already has an active run${run.cwd === null ? "" : ` in ${run.cwd}`}`);
      throw error;
    }
    const value = this.run(id);
    if (!value) throw new DagmarError("run_not_found", `Run ${id} was not found`);
    return value;
  }
  updateAttempt(id: string, patch: Partial<Pick<AttemptRow, "status" | "result" | "error" | "acpSessionId" | "updatedAt" | "endedAt">>): AttemptRow {
    const values: Record<string, string | null> = {};
    if (patch.status !== undefined) values.status = patch.status;
    if (Object.hasOwn(patch, "result")) values.result = encode(patch.result ?? null);
    if (Object.hasOwn(patch, "error")) values.error = encode(patch.error ?? null);
    if (Object.hasOwn(patch, "acpSessionId")) values.acpSessionId = patch.acpSessionId ?? null;
    if (patch.updatedAt !== undefined) values.updatedAt = patch.updatedAt;
    if (Object.hasOwn(patch, "endedAt")) values.endedAt = patch.endedAt ?? null;
    update(this.db, "task_runs", id, values, { status: "status", result: "result_json", error: "error_json", acpSessionId: "acp_session_id", updatedAt: "updated_at", endedAt: "ended_at" });
    const value = this.attempt(id);
    if (!value) throw new DagmarError("task_run_not_found", `Task run ${id} was not found`);
    return value;
  }
}

function update(db: DatabaseSync, table: string, id: string, patch: Record<string, unknown>, names: Record<string, string>): void {
  const entries = Object.entries(patch).filter(([key]) => names[key]);
  if (!entries.length) return;
  const values = entries.map(([, value]) => value == null ? null : String(value));
  const result = db.prepare(`UPDATE ${table} SET ${entries.map(([key]) => `${names[key]}=?`).join(",")} WHERE id=?`).run(...values, id);
  if (!Number(result.changes)) throw new DagmarError(table === "workflow_runs" ? "run_not_found" : "task_run_not_found", `${id} was not found`);
}

function text(row: Row, key: string): string { return row[key] as string; }
function nullable(row: Row, key: string): string | null { return row[key] as string | null; }
function decode<T>(value: unknown): T | null { return value === null ? null : JSON.parse(value as string) as T; }
function encode(value: TaskResult | TaskError | null | undefined): string | null { return value == null ? null : JSON.stringify(value); }

function run(r: Row): RunRow {
  return {
    id: text(r, "id"),
    workflowId: text(r, "workflow_id"),
    cwd: nullable(r, "cwd"),
    input: JSON.parse(text(r, "input_json")),
    status: text(r, "status") as RunRow["status"],
    startedAt: text(r, "started_at"),
    updatedAt: text(r, "updated_at"),
    endedAt: nullable(r, "ended_at"),
  };
}

function attempt(r: Row): AttemptRow {
  return {
    id: text(r, "id"),
    workflowRunId: text(r, "workflow_run_id"),
    taskId: text(r, "task_id"),
    attempt: Number(r.attempt),
    executorProfile: text(r, "executor_profile"),
    executorType: text(r, "executor_type") as AttemptRow["executorType"],
    status: text(r, "status") as AttemptRow["status"],
    result: decode<TaskResult>(r.result_json),
    error: decode<TaskError>(r.error_json),
    acpSessionId: nullable(r, "acp_session_id"),
    startedAt: text(r, "started_at"),
    updatedAt: text(r, "updated_at"),
    endedAt: nullable(r, "ended_at"),
    item: nullable(r, "item_key"),
    roundId: nullable(r, "round_id"),
  };
}
