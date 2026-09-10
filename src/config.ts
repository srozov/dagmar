import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { isAbsolute, join } from "node:path";
import { parseDocument } from "yaml";
import type { Config, ExecutorProfile } from "./types.js";
import { DagmarError } from "./types.js";

export const DEFAULT_CONFIG = join(homedir(), ".config/dagmar/config.yaml");

export async function loadConfig(path = DEFAULT_CONFIG): Promise<Config> {
  let text: string;
  try { text = await readFile(path, "utf8"); }
  catch { throw new DagmarError("config_read_failed", `Cannot read config ${path}`); }
  const doc = parseDocument(text, { uniqueKeys: true });
  if (doc.errors.length) throw new DagmarError("config_invalid", "Config is not valid YAML");
  const root = object(doc.toJS({ maxAliasCount: 100 }), "config");
  keys(root, ["workflowDir", "storageDir", "listen", "executors"], "config");
  const workflowDir = absolute(root.workflowDir, "workflowDir");
  const storageDir = absolute(root.storageDir, "storageDir");
  const listen = object(root.listen, "listen");
  keys(listen, ["host", "port"], "listen");
  if (listen.host !== "127.0.0.1" && listen.host !== "::1") fail("listen.host must be loopback");
  if (!Number.isInteger(listen.port) || (listen.port as number) < 1 || (listen.port as number) > 65535) fail("listen.port is invalid");
  const rawProfiles = object(root.executors, "executors");
  const executors: Record<string, ExecutorProfile> = {};
  for (const [name, raw] of Object.entries(rawProfiles)) {
    if (!/^[A-Za-z0-9][A-Za-z0-9_-]*$/.test(name)) fail(`Invalid executor name ${name}`);
    const p = object(raw, `executors.${name}`);
    if (p.type !== "process" && p.type !== "acp") fail(`Invalid executor type for ${name}`);
    keys(p, p.type === "acp" ? ["type", "cwd", "run", "env"] : ["type", "cwd", "env"], `executors.${name}`);
    const env: Record<string, string> = {};
    for (const [key, value] of Object.entries(p.env === undefined ? {} : object(p.env, `${name}.env`))) {
      if (!key || key.includes("=") || typeof value !== "string") fail(`Invalid environment for ${name}`);
      env[key] = value;
    }
    const profile: ExecutorProfile = { type: p.type, cwd: absolute(p.cwd, `${name}.cwd`), env };
    if (p.type === "acp") profile.run = argv(p.run, `${name}.run`);
    executors[name] = profile;
  }
  return { workflowDir, storageDir, listen: { host: listen.host, port: listen.port as number }, executors };
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
function argv(value: unknown, name: string): [string, ...string[]] {
  if (!Array.isArray(value) || !value.length || value.some((x) => typeof x !== "string" || !x)) fail(`${name} must be a non-empty string array`);
  return value as [string, ...string[]];
}
function fail(message: string): never { throw new DagmarError("config_invalid", message); }
