import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { Readable, Writable } from "node:stream";
import { setTimeout as delay } from "node:timers/promises";
import { client, CreateElicitationRequest, methods, ndJsonStream, PROTOCOL_VERSION, RequestError, type ClientConnection, type ClientContext, type CreateElicitationResponse, type RequestPermissionRequest, type RequestPermissionResponse, type SessionNotification } from "@agentclientprotocol/sdk";
import { validateResult } from "../result.js";
import type { Json, JsonObject, TaskError } from "../types.js";
import type { AcpRequest, Execution, Executor, Hooks, Settlement } from "./types.js";

export const RESULT_INSTRUCTION = 'Respond with exactly one JSON object and no surrounding prose or Markdown. The object must contain exactly three fields: "outcome" ("completed" or "blocked"), "message" (a non-empty string), and "output" (any JSON value).';

export class AcpExecutor implements Executor<AcpRequest> {
  async start(request: AcpRequest, hooks: Hooks): Promise<Execution> {
    let child: ChildProcessWithoutNullStreams;
    try {
      child = spawn(request.run[0], request.run.slice(1), {
        cwd: request.cwd,
        env: request.env,
        shell: false,
        detached: true,
        stdio: ["pipe", "pipe", "pipe"],
      });
    } catch (error) {
      return finished(errorValue("acp_spawn_failed", error));
    }
    return new Attempt(child, request, hooks).execution();
  }
}

class Attempt {
  private connection?: ClientConnection;
  private agent?: ClientContext;
  private sessionId?: string;
  private supportsClose = false;
  private text = "";
  private cancelled = false;
  private cancelPromise?: Promise<void>;
  private cleanupPromise?: Promise<void>;
  private stderrDone: Promise<void> = Promise.resolve();
  private readonly closed: Promise<void>;
  private readonly done: Promise<Settlement>;

  constructor(
    private readonly child: ChildProcessWithoutNullStreams,
    private readonly request: AcpRequest,
    private readonly hooks: Hooks,
  ) {
    this.closed = new Promise((resolve) => child.once("close", resolve));
    this.done = this.run().catch((error) =>
      this.cancelled ? errorValue("acp_cancelled", error) : { error: classifyFailure(error) },
    );
  }

  execution(): Execution {
    return { done: this.done, cancel: () => this.cancel() };
  }

  private async run(): Promise<Settlement> {
    try {
      await this.hooks.transcript({
        type: "lifecycle",
        direction: "internal",
        event: "acp_process_started",
        data: { pid: this.child.pid ?? null },
      });
      this.child.stderr.setEncoding("utf8");
      this.stderrDone = (async () => {
        for await (const chunk of this.child.stderr) {
          await this.hooks.transcript({
            type: "stdio",
            direction: "from_executor",
            stream: "stderr",
            data: String(chunk),
          });
        }
      })();
      const outgoing = tap("to_executor", this.hooks);
      const incoming = tap("from_executor", this.hooks);
      const pipe = outgoing.readable.pipeTo(
        Writable.toWeb(this.child.stdin) as WritableStream<Uint8Array>,
      );
      const stream = ndJsonStream(
        outgoing.writable,
        (Readable.toWeb(this.child.stdout) as ReadableStream<Uint8Array>).pipeThrough(incoming),
      );
      const app = client({ name: "dagmar" });
      app.onNotification(methods.client.session.update, ({ params }) => this.update(params));
      app.onRequest(methods.client.session.requestPermission, ({ params }) =>
        this.permission(params),
      );
      app.onRequest(methods.client.elicitation.create, ({ params }) => this.elicitation(params));
      this.connection = app.connect(stream);
      this.agent = this.connection.agent;
      void pipe.catch((error) => this.connection?.close(error));
      const initialized = await this.agent.request(methods.agent.initialize, {
        protocolVersion: PROTOCOL_VERSION,
        clientCapabilities: { elicitation: { form: {} } },
        clientInfo: { name: "dagmar", version: "0.0.0" },
      });
      if (initialized.protocolVersion !== PROTOCOL_VERSION) {
        throw new Error("ACP protocol version is unsupported");
      }
      this.supportsClose = initialized.agentCapabilities?.sessionCapabilities?.close != null;
      const session = await this.agent.request(methods.agent.session.new, {
        cwd: this.request.cwd,
        mcpServers: [],
      });
      this.sessionId = session.sessionId;
      await this.hooks.session(session.sessionId);
      const prompt = `${this.request.prompt}\n\nInputs:\n${JSON.stringify(this.request.inputs)}\n\n${RESULT_INSTRUCTION}`;
      const response = await this.agent.request(methods.agent.session.prompt, {
        sessionId: session.sessionId,
        prompt: [{ type: "text", text: prompt }],
      });
      if (this.cancelled) throw new Error("ACP attempt was cancelled");
      if (response.stopReason !== "end_turn") {
        throw new Error(`ACP prompt stopped with ${response.stopReason}`);
      }
      let parsed: unknown;
      try {
        parsed = JSON.parse(this.text);
      } catch {
        throw new Error("ACP response must be exactly one JSON value");
      }
      const result = validateResult(parsed, this.request.outputSchema);
      await this.hooks.transcript({
        type: "lifecycle",
        direction: "internal",
        event: "acp_turn_completed",
        data: { stopReason: response.stopReason },
      });
      if (this.cancelled) throw new Error("ACP attempt was cancelled");
      return { result };
    } finally {
      await this.cleanup(false);
    }
  }

  private update(params: SessionNotification): void {
    if (this.sessionId && params.sessionId !== this.sessionId) {
      throw new Error("ACP update used the wrong session");
    }
    const update = params.update;
    if (update.sessionUpdate === "agent_message_chunk" && update.content.type === "text") {
      this.text += update.content.text;
    }
  }

  private async permission(request: RequestPermissionRequest): Promise<RequestPermissionResponse> {
    this.sameSession(request.sessionId);
    return (await this.hooks.interact({
      kind: "permission",
      method: "session/request_permission",
      request: request as unknown as Json,
      validate: (value) => permission(value, request),
    })) as unknown as RequestPermissionResponse;
  }

  private async elicitation(request: CreateElicitationRequest): Promise<CreateElicitationResponse> {
    if (!CreateElicitationRequest.isForm(request)) {
      throw new Error("ACP URL elicitation is unsupported");
    }
    if ("sessionId" in request) this.sameSession(request.sessionId);
    return (await this.hooks.interact({
      kind: "input",
      method: "elicitation/create",
      request: request as unknown as Json,
      validate: elicitation,
    })) as unknown as CreateElicitationResponse;
  }

  private sameSession(id: string): void {
    if (!this.sessionId || id !== this.sessionId) {
      throw new Error("ACP interaction used the wrong session");
    }
  }

  private cancel(): Promise<void> {
    return (this.cancelPromise ??= (async () => {
      this.cancelled = true;
      try {
        if (this.sessionId && this.agent && !this.connection?.signal.aborted) {
          await this.agent.notify(methods.agent.session.cancel, { sessionId: this.sessionId });
        }
      } catch {}
      await this.cleanup(true);
      await this.done;
    })());
  }

  private cleanup(cancelling: boolean): Promise<void> {
    return (this.cleanupPromise ??= (async () => {
      if (
        !cancelling &&
        this.supportsClose &&
        this.sessionId &&
        this.agent &&
        !this.connection?.signal.aborted
      ) {
        await Promise.race([
          this.agent
            .request(methods.agent.session.close, { sessionId: this.sessionId })
            .catch(() => undefined),
          delay(500, undefined, { ref: false }),
        ]);
      }
      this.connection?.close();
      this.child.stdin.end();
      await terminate(this.child, this.closed);
      await this.stderrDone.catch(() => undefined);
      try {
        await this.hooks.transcript({
          type: "lifecycle",
          direction: "internal",
          event: "acp_process_exited",
          data: { code: this.child.exitCode, signal: this.child.signalCode },
        });
      } catch {}
    })());
  }
}

function tap(
  direction: "to_executor" | "from_executor",
  hooks: Hooks,
): TransformStream<Uint8Array, Uint8Array> {
  const decoder = new TextDecoder();
  let buffer = "";
  const lines = async () => {
    for (;;) {
      const at = buffer.indexOf("\n");
      if (at < 0) return;
      const raw = buffer.slice(0, at).replace(/\r$/, "");
      buffer = buffer.slice(at + 1);
      if (!raw.trim()) continue;
      let message: Json;
      try {
        message = JSON.parse(raw);
      } catch {
        throw new Error("ACP emitted invalid NDJSON");
      }
      await hooks.transcript({ type: "acp", direction, message, raw });
    }
  };
  return new TransformStream({
    async transform(chunk, controller) {
      buffer += decoder.decode(chunk, { stream: true });
      await lines();
      controller.enqueue(chunk);
    },
    async flush() {
      buffer += decoder.decode();
      if (buffer.trim()) throw new Error("ACP ended with incomplete NDJSON");
    },
  });
}

function permission(value: Json, request: RequestPermissionRequest): Json {
  const v = object(value);
  const outcome = object(v.outcome);
  if (outcome.outcome === "cancelled") return value;
  if (
    outcome.outcome !== "selected" ||
    typeof outcome.optionId !== "string" ||
    !request.options.some((x) => x.optionId === outcome.optionId)
  ) {
    throw new Error("Invalid permission response");
  }
  return value;
}

function elicitation(value: Json): Json {
  const v = object(value);
  if (v.action === "decline" || v.action === "cancel") return value;
  if (
    v.action !== "accept" ||
    (v.content !== undefined &&
      (!v.content || typeof v.content !== "object" || Array.isArray(v.content)))
  ) {
    throw new Error("Invalid elicitation response");
  }
  return value;
}

function object(value: Json | undefined): JsonObject {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Expected object");
  }
  return value;
}

async function terminate(
  child: ChildProcessWithoutNullStreams,
  closed: Promise<void>,
): Promise<void> {
  if (child.pid) signal(child.pid, "SIGTERM");
  const didTimeout = await Promise.race([
    closed.then(() => false),
    delay(2000, true, { ref: false }),
  ]);
  if (didTimeout) {
    if (child.pid) signal(child.pid, "SIGKILL");
    await closed;
  }
}

function signal(pid: number, value: NodeJS.Signals): void {
  try {
    process.kill(-pid, value);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
  }
}

function errorValue(code: string, error: unknown): { error: TaskError } {
  return {
    error: {
      code,
      message: error instanceof Error ? error.message : "ACP execution failed",
    },
  };
}
// JSON-RPC error code the ACP agent returns from session/new when it requires authentication.
const AUTH_REQUIRED = -32000;
// dagmar performs no ACP `authenticate` handshake, so an agent that gates session creation on
// authentication surfaces here. Map it to a clear, actionable code instead of a generic failure;
// the daemon keeps running and the attempt simply settles as failed.
function classifyFailure(error: unknown): TaskError {
  const code = error instanceof RequestError ? error.code : (typeof error === "object" && error !== null && "code" in error ? (error as { code: unknown }).code : undefined);
  if (code === AUTH_REQUIRED) return { code: "acp_authentication_required", message: "ACP agent requires authentication; authenticate the configured adapter out-of-band before running (dagmar performs no ACP authentication)." };
  return { code: "acp_failed", message: error instanceof Error ? error.message : "ACP execution failed" };
}
function finished(value: Settlement): Execution { return { done: Promise.resolve(value), cancel: async () => undefined }; }
