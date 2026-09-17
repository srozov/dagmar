import { readFileSync } from "node:fs";
import { Ajv } from "ajv";
import type { Json, JsonSchema, TaskResult } from "./types.js";
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

// Shape-only check: is this value structurally a TaskResult envelope (without applying
// outputSchema and without throwing)? Used by the ACP executor to distinguish a
// conversational reply from a final answer. A positive shape check is still followed by
// validateResult(parsed, outputSchema), so a *final* answer whose `output` violates the
// schema still fails the task — we do not collapse shape and schema validation, which
// would silently swallow a real schema violation as "just more chat".
export function isEnvelope(value: unknown): boolean {
  return envelope(value) === true && json(value);
}

export function validateOutputSchema(value: unknown): asserts value is JsonSchema {
  if (typeof value !== "boolean" && (!value || typeof value !== "object" || Array.isArray(value))) throw new DagmarError("invalid_output_schema", "outputSchema is invalid");
  try { ajv.compile(value as JsonSchema); }
  catch { throw new DagmarError("invalid_output_schema", "outputSchema is invalid"); }
}

// Compile a gate's optional JSON Schema into an InteractionRequest.validate function. Without a
// schema, the validator accepts any value (gate has no inherent shape). With a schema, the value
// must conform or the answer is rejected as gate_validation_failed. The returned function matches
// the InteractionRequest.validate contract: return the accepted value or throw.
export function gateValidator(schema?: JsonSchema): (response: Json) => Json {
  if (schema === undefined) return (value) => value;
  let validate: ReturnType<typeof ajv.compile>;
  try { validate = ajv.compile(schema); }
  catch { throw new DagmarError("invalid_gate_schema", "Gate schema is invalid"); }
  return (value: Json): Json => {
    if (!validate(value)) throw new DagmarError("gate_validation_failed", "Gate response does not match schema");
    return value;
  };
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
