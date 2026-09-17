export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type JsonObject = { [key: string]: Json };
export type JsonSchema = boolean | Record<string, unknown>;
export type ExecutorType = "process" | "acp" | "gate";
export type RunStatus = "running" | "waiting" | "completed" | "blocked" | "cancelled";
export type AttemptStatus = "running" | "awaiting_permission" | "awaiting_input" | "completed" | "blocked" | "failed" | "cancelled";
export type TaskState = "pending" | "ready" | AttemptStatus | "blocked_by_dependency";

export interface TaskResult { outcome: "completed" | "blocked"; message: string; output: Json }
export interface TaskError { code: string; message: string; data?: Json }
export interface ExecutorProfile { type: ExecutorType; cwd: string; env: Record<string, string>; run?: [string, ...string[]] }
export interface Config { workflowDir: string; storageDir: string; listen: { host: "127.0.0.1" | "::1"; port: number }; executors: Record<string, ExecutorProfile> }
export interface TaskDef { executor?: string; dependsOn: string[]; inputs: JsonObject; prompt?: string; run?: [string, ...string[]]; outputSchema?: JsonSchema; interactive?: boolean; session?: { mode: "fresh" } | { mode: "continue"; from: string }; gate?: { prompt: string; schema?: JsonSchema } }
export interface Workflow { id: string; tasks: Record<string, TaskDef> }
export interface WorkflowErrorView { file: string; code: string; message: string }
export interface RunRow { id: string; workflowId: string; input: Json; status: RunStatus; startedAt: string; updatedAt: string; endedAt: string | null }
export interface AttemptRow { id: string; workflowRunId: string; taskId: string; attempt: number; executorProfile: string; executorType: ExecutorType; status: AttemptStatus; result: TaskResult | null; error: TaskError | null; acpSessionId: string | null; startedAt: string; updatedAt: string; endedAt: string | null }
export interface RunView extends RunRow { sequence: number; tasks: Record<string, { dependsOn: string[]; executor: string; state: TaskState; attempts: Omit<AttemptRow, "workflowRunId" | "taskId" | "executorProfile" | "executorType">[] }> }
export interface Interaction { id: string; workflowRunId: string; taskRunId: string; kind: "permission" | "input" | "turn" | "gate"; method: "session/request_permission" | "elicitation/create" | "turn/next" | "gate/answer"; request: Json; createdAt: string }
export type TranscriptInput =
  | { type: "lifecycle"; direction: "internal"; event: string; data?: Json }
  | { type: "stdio"; direction: "to_executor" | "from_executor"; stream: "stdin" | "stdout" | "stderr"; data: string }
  | { type: "acp"; direction: "to_executor" | "from_executor"; message: Json; raw?: string };
export type TranscriptRecord = TranscriptInput & { timestamp: string };
export type EventType = "workflow.status_changed" | "task.status_changed" | "interaction.changed" | "transcript.appended";
export interface DagmarEvent { sequence: number; timestamp: string; type: EventType; workflowRunId: string; taskRunId?: string; data: JsonObject }

export class DagmarError extends Error {
  constructor(readonly code: string, message: string, readonly data?: Json) { super(message); this.name = "DagmarError"; }
}
