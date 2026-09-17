import type { Json, JsonObject, JsonSchema, TaskError, TaskResult, TranscriptInput } from "../types.js";

export type Settlement = { result: TaskResult } | { error: TaskError };
export interface Execution { done: Promise<Settlement>; cancel(): Promise<void> }
export interface InteractionRequest { kind: "permission" | "input" | "turn"; method: "session/request_permission" | "elicitation/create" | "turn/next"; request: Json; validate(response: Json): Json }
export interface Hooks { transcript(record: TranscriptInput): Promise<number>; session(id: string): Promise<void>; interact(request: InteractionRequest): Promise<Json> }
interface BaseRequest { runId: string; taskRunId: string; taskId: string; profile: string; cwd: string; env: NodeJS.ProcessEnv; inputs: JsonObject; outputSchema?: JsonSchema }
export interface ProcessRequest extends BaseRequest { type: "process"; run: [string, ...string[]] }
export interface AcpRequest extends BaseRequest { type: "acp"; run: [string, ...string[]]; prompt: string; loadSessionId?: string; interactive?: boolean }
export interface Executor<T extends ProcessRequest | AcpRequest> { start(request: T, hooks: Hooks): Promise<Execution> }
