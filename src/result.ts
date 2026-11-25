import { readFileSync } from "node:fs";
import { Ajv, type ValidateFunction } from "ajv";
import type { Json, JsonSchema, TaskResult } from "./types.js";
import { DagmarError } from "./types.js";

const ajv = new Ajv({ allErrors: true, strict: false, validateFormats: false });
const envelope = ajv.compile(JSON.parse(readFileSync(new URL("../result.schema.json", import.meta.url), "utf8")));
// Bound on the schema-violation detail appended to output_validation_failed, so a very wrong
// output does not flood the repair prompt or the attempt error.
const SCHEMA_ERRORS_MAX = 500;

export function validateResult(value: unknown, outputSchema?: JsonSchema): TaskResult {
  if (!envelope(value) || !json(value)) throw new DagmarError("result_validation_failed", "Executor did not return a valid TaskResult");
  const result = value as TaskResult;
  if (outputSchema !== undefined) {
    let validate;
    try { validate = compile(outputSchema); }
    catch { throw new DagmarError("invalid_output_schema", "Task outputSchema is invalid"); }
    if (!validate(result.output)) {
      // Name every violation (Ajv runs with allErrors) by instance path, so an agent's repair
      // turn and the attempt error say which part of the output failed, not only that it did.
      const detail = ajv.errorsText(validate.errors, { dataVar: "output", separator: "; " });
      const bounded = detail.length > SCHEMA_ERRORS_MAX ? `${detail.slice(0, SCHEMA_ERRORS_MAX)}…` : detail;
      throw new DagmarError("output_validation_failed", `Task output does not match outputSchema: ${bounded}`);
    }
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
  try { compile(value as JsonSchema); }
  catch { throw new DagmarError("invalid_output_schema", "outputSchema is invalid"); }
}

// Compile a gate's optional JSON Schema into an InteractionRequest.validate function. Without a
// schema, the validator accepts any value (gate has no inherent shape). With a schema, the value
// must conform or the answer is rejected as gate_validation_failed. The returned function matches
// the InteractionRequest.validate contract: return the accepted value or throw.
export function gateValidator(schema?: JsonSchema): (response: Json) => Json {
  if (schema === undefined) return (value) => value;
  let validate: ValidateFunction;
  try { validate = compile(schema); }
  catch { throw new DagmarError("invalid_gate_schema", "Gate schema is invalid"); }
  return (value: Json): Json => {
    if (!validate(value)) throw new DagmarError("gate_validation_failed", "Gate response does not match schema");
    return value;
  };
}

// Ajv never frees what it compiled, and workflows are read again on every step, so each distinct schema is
// compiled once, found again by its text. Ajv's own record of the schema is dropped: it would refuse a
// second schema with the same `$id` as a duplicate. Nothing is evicted: each edited version of a schema
// stays compiled until the daemon restarts, which is bounded by how often workflow files change.
const compiled = new Map<string, ValidateFunction>();
function compile(schema: JsonSchema): ValidateFunction {
  const text = JSON.stringify(schema);
  let validate = compiled.get(text);
  if (!validate) {
    try { validate = ajv.compile(schema); }
    finally { if (typeof schema === "object") ajv.removeSchema(schema); }
    compiled.set(text, validate);
  }
  return validate;
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
