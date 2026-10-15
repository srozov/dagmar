import { readFileSync } from "node:fs";
import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { Ajv } from "ajv";
import { parseDocument } from "yaml";
import { validateOutputSchema } from "./result.js";
import type { ExecutorProfile, Json, JsonObject, TaskDef, Workflow, WorkflowErrorView } from "./types.js";
import { DagmarError } from "./types.js";

const shape = new Ajv({ allErrors: true, strict: false }).compile(JSON.parse(readFileSync(new URL("../workflow.schema.json", import.meta.url), "utf8")));

export class WorkflowRepository {
  constructor(private readonly dir: string) {}

  async list(): Promise<{ workflows: Array<{ id: string; taskCount: number; file: string }>; errors: WorkflowErrorView[] }> {
    const found = await this.discover();
    return { workflows: [...found.valid].map(([id, x]) => ({ id, taskCount: Object.keys(x.workflow.tasks).length, file: x.file })), errors: found.errors };
  }

  async get(id: string): Promise<Workflow> {
    // Resolve only the requested workflow instead of validating the whole directory:
    // this runs on every scheduler advancement, so it must stay cheap.
    let names: string[];
    try { names = await this.files(); }
    catch { throw new DagmarError("workflow_not_found", `Workflow ${id} was not found`); }
    const matches: unknown[] = [];
    for (const file of names) {
      let raw: unknown;
      try { raw = yaml(await readFile(join(this.dir, file), "utf8")); } catch { continue; }
      if (idOf(raw) === id) matches.push(raw);
    }
    // A duplicated id is excluded from the valid set by discover(), so it is not resolvable here either.
    if (matches.length !== 1) throw new DagmarError("workflow_not_found", `Workflow ${id} was not found`);
    return validateWorkflow(matches[0]);
  }

  private async files(): Promise<string[]> {
    return (await readdir(this.dir, { withFileTypes: true })).filter((x) => x.isFile() && /\.ya?ml$/.test(x.name)).map((x) => x.name).sort();
  }

  private async discover(): Promise<{ valid: Map<string, { file: string; workflow: Workflow }>; errors: WorkflowErrorView[] }> {
    let names: string[];
    try { names = await this.files(); }
    catch { return { valid: new Map(), errors: [{ file: this.dir, code: "workflow_directory_unreadable", message: "Workflow directory cannot be read" }] }; }
    const rows: Array<{ file: string; id?: string; workflow?: Workflow; error?: WorkflowErrorView }> = [];
    for (const file of names) {
      try {
        const text = await readFile(join(this.dir, file), "utf8");
        const raw = yaml(text);
        const id = idOf(raw);
        rows.push({ file, ...(id ? { id } : {}), workflow: validateWorkflow(raw) });
      } catch (error) {
        const e = error instanceof DagmarError ? error : new DagmarError("workflow_invalid", "Workflow is invalid");
        let id: string | undefined;
        try { id = idOf(yaml(await readFile(join(this.dir, file), "utf8"))); } catch {}
        rows.push({ file, ...(id ? { id } : {}), error: { file, code: e.code, message: e.message } });
      }
    }
    const counts = new Map<string, number>();
    for (const row of rows) if (row.id) counts.set(row.id, (counts.get(row.id) ?? 0) + 1);
    const valid = new Map<string, { file: string; workflow: Workflow }>();
    const errors: WorkflowErrorView[] = [];
    for (const row of rows) {
      if (row.id && (counts.get(row.id) ?? 0) > 1) errors.push({ file: row.file, code: "duplicate_workflow_id", message: `Workflow ID ${row.id} is duplicated` });
      else if (row.workflow) valid.set(row.workflow.id, { file: row.file, workflow: row.workflow });
      else if (row.error) errors.push(row.error);
    }
    return { valid, errors };
  }
}

export function validateWorkflow(value: unknown): Workflow {
  if (!shape(value)) throw new DagmarError("workflow_invalid", "Workflow does not match workflow.schema.json");
  const raw = value as { id: string; executors?: Record<string, Omit<ExecutorProfile, "env"> & { env?: Record<string, string> }>; tasks: Record<string, Omit<TaskDef, "dependsOn" | "session"> & { dependsOn?: string[]; session?: { mode: string; from?: unknown } }> };
  const profiles: Record<string, ExecutorProfile> = {};
  for (const [name, executor] of Object.entries(raw.executors ?? {})) profiles[name] = { ...executor, env: executor.env ?? {} };
  const tasks: Record<string, TaskDef> = {};
  for (const [id, item] of Object.entries(raw.tasks)) {
    if (item.gate) {
      if (item.executor) throw new DagmarError("invalid_task", `Task ${id} cannot have both executor and gate`);
      if (item.prompt !== undefined || item.run !== undefined || item.session !== undefined || item.interactive !== undefined || item.outputSchema !== undefined || item.rerun !== undefined || item.map !== undefined)
        throw new DagmarError("invalid_task", `Gate task ${id} must not have prompt, run, session, interactive, outputSchema, rerun, or map`);
      if (typeof item.gate.prompt !== "string" || !item.gate.prompt.trim())
        throw new DagmarError("invalid_task", `Gate task ${id} requires a non-empty prompt`);
      if (item.gate.schema !== undefined) validateOutputSchema(item.gate.schema);
      tasks[id] = { ...item, dependsOn: item.dependsOn ?? [], gate: { prompt: item.gate.prompt, ...(item.gate.schema !== undefined ? { schema: item.gate.schema } : {}) } } as TaskDef;
      continue;
    }
    if (!item.executor) throw new DagmarError("invalid_task", `Task ${id} requires executor or gate`);
    const profile = profiles[item.executor];
    if (!profile) throw new DagmarError("unknown_executor", `Task ${id} references unknown executor ${item.executor}`);
    let session: TaskDef["session"] | undefined;
    if (item.session) {
      if (item.session.mode === "fork") throw new DagmarError("unsupported_session_mode", `Task ${id} session mode is unsupported`);
      if (item.session.mode === "continue") {
        if (typeof item.session.from !== "string" || !item.session.from) throw new DagmarError("invalid_task", `Task ${id} continue requires from`);
        session = { mode: "continue", from: item.session.from };
      } else if (item.session.mode === "fresh") {
        session = { mode: "fresh" };
      }
    }
    if (profile.type === "process" && (!item.run?.[0] || item.prompt !== undefined || item.session !== undefined || item.interactive !== undefined)) throw new DagmarError("invalid_task", `Process task ${id} requires run only`);
    if (profile.type === "acp" && (!item.prompt?.trim() || item.run !== undefined)) throw new DagmarError("invalid_task", `ACP task ${id} requires prompt only`);
    if (item.outputSchema !== undefined) validateOutputSchema(item.outputSchema);
    tasks[id] = { ...item, dependsOn: item.dependsOn ?? [], session } as TaskDef;
  }
  for (const [id, task] of Object.entries(tasks)) {
    for (const dep of task.dependsOn) {
      if (dep === id || !tasks[dep]) throw new DagmarError("invalid_dependency", `Task ${id} has invalid dependency ${dep}`);
    }
    for (const value of Object.values(task.inputs)) {
      if (typeof value !== "string") continue;
      const ref = reference(value);
      if (ref?.task && !task.dependsOn.includes(ref.task)) throw new DagmarError("invalid_input_reference", `Task ${id} references a non-dependency`);
      if (ref?.item && !task.map) throw new DagmarError("invalid_input_reference", `Task ${id} reads $item but has no map`);
    }
    // `when` reads the outputs of the task's dependencies or the run input; `rerun.when` reads only the
    // task's own output, after it has run.
    for (const [field, clauses] of [["when", task.when], ["rerun.when", task.rerun?.when]] as const) {
      for (const clause of clauses ?? []) {
        const hasEquals = Object.hasOwn(clause, "equals");
        const hasIn = clause.in !== undefined;
        if (hasEquals === hasIn) throw new DagmarError("invalid_guard", `Task ${id} ${field} clause needs exactly one of equals or in`);
        if (hasIn && clause.in!.length === 0) throw new DagmarError("invalid_guard", `Task ${id} ${field} in must be a non-empty array`);
        const ref = reference(clause.ref);
        if (!ref || ref.item) throw new DagmarError("invalid_guard", `Task ${id} ${field} ref must be a $tasks or $run reference`);
        if (field === "rerun.when" && ref.task !== id) throw new DagmarError("invalid_rerun", `Task ${id} rerun.when must read its own output`);
        if (field === "when" && ref.task && !task.dependsOn.includes(ref.task)) throw new DagmarError("invalid_guard", `Task ${id} when references non-dependency ${ref.task}`);
      }
    }
    if (task.rerun !== undefined) {
      const from = task.rerun.from;
      if (!tasks[from]) throw new DagmarError("invalid_rerun", `Task ${id} rerun.from ${from} is not a task`);
      if (!upstream(tasks, id).has(from)) throw new DagmarError("invalid_rerun", `Task ${id} rerun.from ${from} must be upstream of it`);
      if (!Number.isInteger(task.rerun.max) || task.rerun.max < 1) throw new DagmarError("invalid_rerun", `Task ${id} rerun.max must be an integer >= 1`);
    }
    if (task.map) {
      const ref = reference(task.map.over);
      if (!ref || ref.item || (ref.task && !task.dependsOn.includes(ref.task))) throw new DagmarError("invalid_map", `Task ${id} map.over must read the run input or a dependency's output`);
      if (task.rerun || task.session?.mode === "continue") throw new DagmarError("invalid_map", `Map task ${id} cannot rerun or continue a session`);
    }
    if (task.session?.mode === "continue") {
      const from = task.session.from;
      if (!task.dependsOn.includes(from)) throw new DagmarError("invalid_dependency", `Task ${id} continue from ${from} must be a dependency`);
      const source = tasks[from]!;
      if (source.map) throw new DagmarError("invalid_map", `Task ${id} cannot continue map task ${from}`);
      const sourceProfile = source.executor ? profiles[source.executor] : undefined;
      if (!sourceProfile || sourceProfile.type !== "acp") throw new DagmarError("invalid_task", `Task ${id} continue from ${from} must target an ACP task`);
      const thisProfile = profiles[task.executor!]!;
      if (!sameAgent(thisProfile.run, sourceProfile.run)) throw new DagmarError("invalid_task", `Task ${id} continue must target the same agent`);
    }
  }
  const visiting = new Set<string>();
  const done = new Set<string>();
  const visit = (id: string): void => {
    if (visiting.has(id)) throw new DagmarError("dependency_cycle", "Workflow contains a dependency cycle");
    if (done.has(id)) return;
    visiting.add(id);
    for (const dep of tasks[id]!.dependsOn) visit(dep);
    visiting.delete(id);
    done.add(id);
  };
  for (const id of Object.keys(tasks)) visit(id);
  return { id: raw.id, executors: profiles, tasks };
}

export function resolveInputs(task: TaskDef, runInput: Json, outputs: Readonly<Record<string, Json>>, item?: JsonObject): JsonObject {
  const result: JsonObject = {};
  for (const [name, value] of Object.entries(task.inputs)) {
    if (typeof value !== "string") { result[name] = value; continue; }
    const ref = reference(value);
    if (!ref) { result[name] = value; continue; }
    let current: Json = ref.item ? item as Json : ref.task ? outputs[ref.task] as Json : runInput;
    if (current === undefined) throw new DagmarError("input_resolution_failed", `Missing input ${value}`);
    for (const key of ref.path) {
      if (!current || typeof current !== "object" || Array.isArray(current) || !(key in current)) throw new DagmarError("input_resolution_failed", `Missing input path ${value}`);
      current = current[key]!;
    }
    result[name] = current;
  }
  return result;
}

// The keyed items a map task runs over, in list order; a list it cannot run fails the task before any item starts.
export function mapItems(task: TaskDef, runInput: Json, outputs: Readonly<Record<string, Json>>): Array<[string, JsonObject]> {
  const { over, key } = task.map!, list = resolveInputs({ ...task, inputs: { list: over } }, runInput, outputs).list, keys = new Set<string>();
  if (!Array.isArray(list)) throw new DagmarError("map_items_invalid", `${over} is not a list`);
  return list.map((item, i) => {
    const value = item && typeof item === "object" && !Array.isArray(item) ? item[key] : undefined;
    if (typeof value !== "string" || !value || keys.has(value)) throw new DagmarError("map_items_invalid", `Item ${i} of ${over} is not an object with a unique non-empty string ${key}`);
    keys.add(value); return [value, item as JsonObject];
  });
}

// Every task that `id` depends on, directly or through other tasks.
function upstream(tasks: Record<string, TaskDef>, id: string): Set<string> {
  const seen = new Set<string>(), stack = [...tasks[id]!.dependsOn];
  while (stack.length) { const next = stack.pop()!; if (!seen.has(next)) { seen.add(next); stack.push(...(tasks[next]?.dependsOn ?? [])); } }
  return seen;
}
function idOf(raw: unknown): string | undefined {
  return typeof raw === "object" && raw !== null && !Array.isArray(raw) && typeof (raw as Record<string, unknown>).id === "string" ? (raw as { id: string }).id : undefined;
}
function yaml(text: string): unknown {
  const doc = parseDocument(text, { uniqueKeys: true });
  if (doc.errors.length) throw new DagmarError("invalid_yaml", "Workflow is not valid YAML");
  return doc.toJS({ maxAliasCount: 100 });
}
export function reference(value: string): { task?: string; item?: true; path: string[] } | undefined {
  if (value === "$item") return { item: true, path: [] };
  if (value.startsWith("$item.")) return { item: true, path: value.slice(6).split(".") };
  if (value === "$run.input") return { path: [] };
  if (value.startsWith("$run.input.")) return { path: value.slice(11).split(".") };
  const match = /^\$tasks\.([A-Za-z0-9_-]+)\.output(?:\.(.+))?$/.exec(value);
  if (match) return { task: match[1]!, path: match[2]?.split(".") ?? [] };
  if (value.startsWith("$run.") || value.startsWith("$tasks.")) throw new DagmarError("invalid_input_reference", `Invalid input reference ${value}`);
  return undefined;
}
// "Same agent" means the executor profile's `run` argv is byte-identical. A different model/env
// (e.g. sonnet vs opus under claude-agent-acp) is allowed because the model is per-process, not
// per-session — this is what makes a different-model reviewer able to continue a builder's session.
function sameAgent(a?: readonly string[], b?: readonly string[]): boolean {
  if (!a || !b || a.length !== b.length) return false;
  return a.every((value, i) => value === b[i]);
}
