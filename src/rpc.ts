import { randomUUID } from "node:crypto";
import { WebSocket, WebSocketServer } from "ws";
import type { AddressInfo } from "node:net";
import type { DagmarEvent, Json, JsonObject } from "./types.js";
import { DagmarError } from "./types.js";

type Handler = (params: JsonObject, signal: AbortSignal) => Json | Promise<Json>;
type Subscription = { socket: WebSocket; workflowRunId?: string; paused: boolean; queued: DagmarEvent[] };

export class EventBus {
  private sequence = 0;
  private readonly events: DagmarEvent[] = [];
  private readonly subscriptions = new Map<string, Subscription>();

  get current(): number {
    return this.sequence;
  }

  publish(input: Omit<DagmarEvent, "sequence" | "timestamp">): DagmarEvent {
    const event = { ...input, sequence: ++this.sequence, timestamp: new Date().toISOString() };
    this.events.push(event);
    if (this.events.length > 1000) this.events.shift();
    for (const sub of this.subscriptions.values()) {
      if (!sub.workflowRunId || sub.workflowRunId === event.workflowRunId) {
        if (sub.paused) sub.queued.push(event);
        else notify(sub.socket, event);
      }
    }
    return event;
  }

  subscribe(socket: WebSocket, params: JsonObject): { subscriptionId: string; currentSequence: number; activate(): void } {
    const after = params.afterSequence;
    if (after !== undefined && (!Number.isSafeInteger(after) || (after as number) < 0)) {
      throw new DagmarError("cursor_unavailable", "Event cursor is invalid");
    }
    const oldest = this.events[0]?.sequence ?? this.sequence + 1;
    if (typeof after === "number" && (after > this.sequence || after < oldest - 1)) {
      throw new DagmarError("cursor_unavailable", "Event cursor is unavailable");
    }
    const workflowRunId = params.workflowRunId;
    if (workflowRunId !== undefined && typeof workflowRunId !== "string") {
      throw new DagmarError("invalid_params", "workflowRunId must be a string");
    }
    const id = `sub_${randomUUID()}`;
    const replay = this.events.filter(
      (x) =>
        x.sequence > (typeof after === "number" ? after : this.sequence) &&
        (!workflowRunId || x.workflowRunId === workflowRunId),
    );
    const sub: Subscription = {
      socket,
      ...(workflowRunId ? { workflowRunId } : {}),
      paused: true,
      queued: [],
    };
    this.subscriptions.set(id, sub);
    return {
      subscriptionId: id,
      currentSequence: this.sequence,
      activate: () => {
        if (!sub.paused) return;
        sub.paused = false;
        for (const event of [...replay, ...sub.queued]) notify(socket, event);
        sub.queued.length = 0;
      },
    };
  }

  unsubscribe(id: string, socket?: WebSocket): boolean {
    const sub = this.subscriptions.get(id);
    if (!sub || (socket && sub.socket !== socket)) return false;
    this.subscriptions.delete(id);
    return true;
  }

  remove(socket: WebSocket): void {
    for (const [id, sub] of this.subscriptions) {
      if (sub.socket === socket) this.subscriptions.delete(id);
    }
  }
}

export class RpcServer {
  private server?: WebSocketServer;
  private readonly sockets = new Set<WebSocket>();
  private readonly work = new Set<Promise<void>>();

  constructor(
    private readonly host: string,
    private readonly port: number,
    private readonly handlers: Record<string, Handler>,
    private readonly events: EventBus,
  ) {}

  async start(): Promise<{ host: string; port: number }> {
    this.server = new WebSocketServer({ host: this.host, port: this.port });
    this.server.on("connection", (socket) => this.accept(socket));
    this.server.on("error", (error) => process.emitWarning(error.message));
    await new Promise<void>((resolve, reject) => {
      this.server!.once("listening", resolve);
      this.server!.once("error", reject);
    });
    const address = this.server.address() as AddressInfo;
    return { host: address.address, port: address.port };
  }

  async stop(): Promise<void> {
    for (const socket of this.sockets) socket.close();
    if (this.server) {
      await new Promise<void>((resolve) => this.server!.close(() => resolve()));
    }
    await Promise.all(this.work);
  }

  private accept(socket: WebSocket): void {
    this.sockets.add(socket);
    const controller = new AbortController();
    socket.on("message", (data) => {
      const job = this.message(socket, String(data), controller.signal).finally(() => this.work.delete(job));
      this.work.add(job);
    });
    socket.on("error", () => socket.terminate());
    socket.on("close", () => {
      controller.abort();
      this.events.remove(socket);
      this.sockets.delete(socket);
    });
  }
  private async message(socket: WebSocket, raw: string, signal: AbortSignal): Promise<void> {
    let request: Record<string, unknown>;
    try {
      request = JSON.parse(raw);
    } catch {
      sendError(socket, null, -32700, "Parse error");
      return;
    }
    const id = request.id as string | number | null | undefined;
    if (
      request.jsonrpc !== "2.0" ||
      typeof request.method !== "string" ||
      (id !== undefined && id !== null && typeof id !== "string" && typeof id !== "number")
    ) {
      sendError(socket, id ?? null, -32600, "Invalid Request");
      return;
    }
    if (id === undefined) return;
    const params = request.params === undefined ? {} : request.params;
    if (!params || typeof params !== "object" || Array.isArray(params)) {
      sendError(socket, id, -32602, "Invalid params");
      return;
    }
    try {
      if (request.method === "events.subscribe") {
        const sub = this.events.subscribe(socket, params as JsonObject);
        send(socket, {
          jsonrpc: "2.0",
          id,
          result: { subscriptionId: sub.subscriptionId, currentSequence: sub.currentSequence },
        });
        sub.activate();
        return;
      }
      if (request.method === "events.unsubscribe") {
        const subscriptionId = (params as JsonObject).subscriptionId;
        if (typeof subscriptionId !== "string" || !this.events.unsubscribe(subscriptionId, socket)) {
          throw new DagmarError("invalid_params", "Subscription was not found");
        }
        send(socket, { jsonrpc: "2.0", id, result: { unsubscribed: true } });
        return;
      }
      const handler = this.handlers[request.method];
      if (!handler) {
        sendError(socket, id, -32601, "Method not found");
        return;
      }
      send(socket, { jsonrpc: "2.0", id, result: await handler(params as JsonObject, signal) });
    } catch (error) {
      if (error instanceof DagmarError) {
        sendError(socket, id, -32000, error.message, {
          code: error.code,
          ...(error.data === undefined ? {} : { data: error.data }),
        });
      } else {
        sendError(socket, id, -32603, "Internal error");
      }
    }
  }
}

export class RpcClient {
  private readonly socket: WebSocket;
  private next = 1;
  private readonly pending = new Map<number, { resolve(value: Json): void; reject(error: Error): void }>();
  private readonly listeners = new Set<(event: DagmarEvent) => void>();
  readonly closed: Promise<void>;

  constructor(url: string) {
    this.socket = new WebSocket(url);
    this.closed = new Promise((resolve) => this.socket.once("close", resolve));
    this.socket.on("message", (data) => this.message(String(data)));
    this.socket.on("error", (error) => {
      for (const item of this.pending.values()) item.reject(error);
      this.pending.clear();
    });
  }

  async connect(): Promise<void> {
    if (this.socket.readyState === WebSocket.OPEN) return;
    await new Promise<void>((resolve, reject) => {
      this.socket.once("open", resolve);
      this.socket.once("error", reject);
    });
  }

  request(method: string, params: JsonObject = {}): Promise<Json> {
    const id = this.next++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      send(this.socket, { jsonrpc: "2.0", id, method, params });
    });
  }
  onEvent(listener: (event: DagmarEvent) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  close(): void {
    this.socket.close();
  }

  private message(raw: string): void {
    let value: Record<string, unknown>;
    try {
      value = JSON.parse(raw);
    } catch {
      return;
    }
    if (value.method === "event") {
      const event = (value.params as { sequence?: unknown }) ?? {};
      if (typeof event.sequence === "number") {
        for (const listener of this.listeners) listener(event as DagmarEvent);
      }
      return;
    }
    if (typeof value.id !== "number") return;
    const pending = this.pending.get(value.id);
    if (!pending) return;
    this.pending.delete(value.id);
    if (value.error && typeof value.error === "object") {
      const error = value.error as { message?: unknown; data?: unknown };
      const code =
        typeof (error.data as { code?: unknown })?.code === "string"
          ? (error.data as { code: string }).code
          : "rpc_error";
      const message = typeof error.message === "string" ? error.message : "RPC failed";
      pending.reject(new DagmarError(code, message));
    } else {
      pending.resolve(value.result as Json);
    }
  }
}

function notify(socket: WebSocket, event: DagmarEvent): void {
  send(socket, { jsonrpc: "2.0", method: "event", params: event });
}

function sendError(
  socket: WebSocket,
  id: unknown,
  code: number,
  message: string,
  data?: Json,
): void {
  send(socket, {
    jsonrpc: "2.0",
    id,
    error: { code, message, ...(data === undefined ? {} : { data }) },
  });
}

function send(socket: WebSocket, value: unknown): void {
  if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(value));
}
