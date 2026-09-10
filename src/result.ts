import { readFileSync } from "node:fs";
import { Ajv } from "ajv";
import type { JsonSchema, TaskResult } from "./types.js";
import { DagmarError } from "./types.js";

const ajv = new Ajv({ allErrors: true, strict: false, validateFormats: false });
const envelope = ajv.compile(JSON.parse(readFileSync(new URL("../result.schema.json", import.meta.url), "utf8")));

export function validateResult(value: unknown, outputSchema?: JsonSchema): TaskResult {
  if (!envelope(value) || !json(value)) throw new DagmarError("result_validation_failed", "Executor did not return a valid TaskResult");
  const result = value as TaskResult;
  if (outputSchema !== undefined) {
    let validate;
    try { validate = ajv.compile(outputSchema); }
    catch { throw new DagmarError("invalid_output_schema", "Task outputSchema is invalid"); }
    if (!validate(result.output)) throw new DagmarError("output_validation_failed", "Task output does not match outputSchema");
  }
  return result;
}

export function validateOutputSchema(value: unknown): asserts value is JsonSchema {
  if (typeof value !== "boolean" && (!value || typeof value !== "object" || Array.isArray(value))) throw new DagmarError("invalid_output_schema", "outputSchema is invalid");
  try { ajv.compile(value as JsonSchema); }
  catch { throw new DagmarError("invalid_output_schema", "outputSchema is invalid"); }
}

function json(value: unknown, seen = new Set<object>()): boolean {
  if (value === null || typeof value === "string" || typeof value === "boolean") return true;
  if (typeof value === "number") return Number.isFinite(value);
  if (!value || typeof value !== "object" || seen.has(value)) return false;
  if (!Array.isArray(value) && Object.getPrototypeOf(value) !== Object.prototype) return false;
  seen.add(value);
  const ok = (Array.isArray(value) ? value : Object.values(value)).every((item) => json(item, seen));
  seen.delete(value);
  return ok;
}
