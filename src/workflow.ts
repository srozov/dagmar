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
  constructor(private readonly dir: string, private readonly profiles: Readonly<Record<string, ExecutorProfile>>) {}

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
    return validateWorkflow(matches[0], this.profiles);
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
        rows.push({ file, ...(id ? { id } : {}), workflow: validateWorkflow(raw, this.profiles) });
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

export function validateWorkflow(value: unknown, profiles: Readonly<Record<string, ExecutorProfile>>): Workflow {
  if (!shape(value)) throw new DagmarError("workflow_invalid", "Workflow does not match workflow.schema.json");
  const raw = value as { id: string; tasks: Record<string, Omit<TaskDef, "dependsOn"> & { dependsOn?: string[]; session?: { mode: string } }> };
  const tasks: Record<string, TaskDef> = {};
  for (const [id, item] of Object.entries(raw.tasks)) {
    const profile = profiles[item.executor];
    if (!profile) throw new DagmarError("unknown_executor", `Task ${id} references unknown executor ${item.executor}`);
    if (item.session && item.session.mode !== "fresh") throw new DagmarError("unsupported_session_mode", `Task ${id} session mode is unsupported`);
    if (profile.type === "process" && (!item.run?.[0] || item.prompt !== undefined || item.session !== undefined)) throw new DagmarError("invalid_task", `Process task ${id} requires run only`);
    if (profile.type === "acp" && (!item.prompt?.trim() || item.run !== undefined)) throw new DagmarError("invalid_task", `ACP task ${id} requires prompt only`);
    if (item.outputSchema !== undefined) validateOutputSchema(item.outputSchema);
    tasks[id] = { ...item, dependsOn: item.dependsOn ?? [], session: item.session ? { mode: "fresh" } : undefined } as TaskDef;
  }
  for (const [id, task] of Object.entries(tasks)) {
    for (const dep of task.dependsOn) {
      if (dep === id || !tasks[dep]) throw new DagmarError("invalid_dependency", `Task ${id} has invalid dependency ${dep}`);
    }
    for (const value of Object.values(task.inputs)) {
      if (typeof value !== "string") continue;
      const ref = reference(value);
      if (ref?.task && !task.dependsOn.includes(ref.task)) throw new DagmarError("invalid_input_reference", `Task ${id} references a non-dependency`);
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
  return { id: raw.id, tasks };
}

export function resolveInputs(task: TaskDef, runInput: Json, outputs: Readonly<Record<string, Json>>): JsonObject {
  const result: JsonObject = {};
  for (const [name, value] of Object.entries(task.inputs)) {
    if (typeof value !== "string") { result[name] = value; continue; }
    const ref = reference(value);
    if (!ref) { result[name] = value; continue; }
    let current: Json = ref.task ? outputs[ref.task] as Json : runInput;
    if (current === undefined) throw new DagmarError("input_resolution_failed", `Missing input ${value}`);
    for (const key of ref.path) {
      if (!current || typeof current !== "object" || Array.isArray(current) || !(key in current)) throw new DagmarError("input_resolution_failed", `Missing input path ${value}`);
      current = current[key]!;
    }
    result[name] = current;
  }
  return result;
}

function idOf(raw: unknown): string | undefined {
  return typeof raw === "object" && raw !== null && !Array.isArray(raw) && typeof (raw as Record<string, unknown>).id === "string" ? (raw as { id: string }).id : undefined;
}
function yaml(text: string): unknown {
  const doc = parseDocument(text, { uniqueKeys: true });
  if (doc.errors.length) throw new DagmarError("invalid_yaml", "Workflow is not valid YAML");
  return doc.toJS({ maxAliasCount: 100 });
}
function reference(value: string): { task?: string; path: string[] } | undefined {
  if (value === "$run.input") return { path: [] };
  if (value.startsWith("$run.input.")) return { path: value.slice(11).split(".") };
  const match = /^\$tasks\.([A-Za-z0-9_-]+)\.output(?:\.(.+))?$/.exec(value);
  if (match) return { task: match[1]!, path: match[2]?.split(".") ?? [] };
  if (value.startsWith("$run.") || value.startsWith("$tasks.")) throw new DagmarError("invalid_input_reference", `Invalid input reference ${value}`);
  return undefined;
}
