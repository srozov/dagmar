import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { isAbsolute, join } from "node:path";
import { parseDocument } from "yaml";
import type { Config } from "./types.js";
import { DagmarError } from "./types.js";

export const DEFAULT_CONFIG = join(homedir(), ".config/dagmar/config.yaml");

export async function loadConfig(path = DEFAULT_CONFIG): Promise<Config> {
  let text: string;
  try { text = await readFile(path, "utf8"); }
  catch { throw new DagmarError("config_read_failed", `Cannot read config ${path}`); }
  const doc = parseDocument(text, { uniqueKeys: true });
  if (doc.errors.length) throw new DagmarError("config_invalid", "Config is not valid YAML");
  const root = object(doc.toJS({ maxAliasCount: 100 }), "config");
  keys(root, ["workflowDir", "storageDir", "listen"], "config");
  const workflowDir = absolute(root.workflowDir, "workflowDir");
  const storageDir = absolute(root.storageDir, "storageDir");
  const listen = object(root.listen, "listen");
  keys(listen, ["host", "port"], "listen");
  if (listen.host !== "127.0.0.1" && listen.host !== "::1") fail("listen.host must be loopback");
  if (!Number.isInteger(listen.port) || (listen.port as number) < 1 || (listen.port as number) > 65535) fail("listen.port is invalid");
  return { workflowDir, storageDir, listen: { host: listen.host, port: listen.port as number } };
}

function object(value: unknown, name: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) fail(`${name} must be an object`);
  return value as Record<string, unknown>;
}
function keys(value: Record<string, unknown>, allowed: string[], name: string): void {
  const bad = Object.keys(value).find((key) => !allowed.includes(key));
  if (bad) fail(`${name} contains unknown field ${bad}`);
}
function absolute(value: unknown, name: string): string {
  if (typeof value !== "string" || !isAbsolute(value)) fail(`${name} must be an absolute path`);
  return value;
}
function fail(message: string): never { throw new DagmarError("config_invalid", message); }
