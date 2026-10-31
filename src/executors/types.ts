import type { Json, JsonObject, JsonSchema, TaskError, TaskResult, TranscriptInput } from "../types.js";

// `retryable`: the attempt failed before doing any work, so the scheduler may run it again (the executor's `retry`).
export type Settlement = { result: TaskResult } | { error: TaskError; retryable?: true };
export interface Execution { done: Promise<Settlement>; cancel(): Promise<void> }
export interface InteractionRequest { kind: "permission" | "input" | "turn" | "gate"; method: "session/request_permission" | "elicitation/create" | "turn/next" | "gate/answer"; request: Json; validate(response: Json): Json }
export interface Hooks { transcript(record: TranscriptInput): Promise<number>; session(id: string): Promise<void>; interact(request: InteractionRequest): Promise<Json> }
interface BaseRequest { runId: string; taskRunId: string; taskId: string; profile: string; cwd: string; env: NodeJS.ProcessEnv; inputs: JsonObject; outputSchema?: JsonSchema }
export interface ProcessRequest extends BaseRequest { type: "process"; run: [string, ...string[]] }
export interface AcpRequest extends BaseRequest { type: "acp"; run: [string, ...string[]]; prompt: string; loadSessionId?: string; interactive?: boolean; model?: string; mode?: string; loadSettleMs?: number }
export interface Executor<T extends ProcessRequest | AcpRequest> { start(request: T, hooks: Hooks): Promise<Execution> }
