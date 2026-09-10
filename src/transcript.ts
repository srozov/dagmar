import { mkdir, open, readFile, truncate } from "node:fs/promises";
import { dirname, join } from "node:path";
import type { TranscriptInput, TranscriptRecord } from "./types.js";
import { DagmarError } from "./types.js";

export class Transcripts {
  private readonly tails = new Map<string, Promise<unknown>>();
  private readonly counts = new Map<string, number>();
  constructor(private readonly storageDir: string, private readonly appended?: (runId: string, taskRunId: string, line: number) => void) {}
  path(runId: string, taskRunId: string): string { safe(runId); safe(taskRunId); return join(this.storageDir, "runs", runId, "tasks", taskRunId, "transcript.jsonl"); }

  append(runId: string, taskRunId: string, input: TranscriptInput): Promise<number> {
    const path = this.path(runId, taskRunId);
    const job = (this.tails.get(path) ?? Promise.resolve()).then(async () => {
      await mkdir(dirname(path), { recursive: true });
      let count = this.counts.get(path);
      if (count === undefined) count = await repair(path);
      const record: TranscriptRecord = { ...input, timestamp: new Date().toISOString() };
      const handle = await open(path, "a");
      try { await handle.appendFile(`${JSON.stringify(record)}\n`); await handle.sync(); } finally { await handle.close(); }
      count += 1; this.counts.set(path, count); this.appended?.(runId, taskRunId, count); return count;
    });
    this.tails.set(path, job.catch(() => undefined));
    return job;
  }

  async read(runId: string, taskRunId: string, afterLine = 0): Promise<{ records: TranscriptRecord[]; nextLine: number }> {
    if (!Number.isSafeInteger(afterLine) || afterLine < 0) throw new DagmarError("invalid_params", "afterLine must be a non-negative integer");
    let text = ""; try { text = await readFile(this.path(runId, taskRunId), "utf8"); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
    const lines = text.split("\n"); if (lines.at(-1) === "") lines.pop(); else lines.pop();
    const records = lines.slice(afterLine).map((line) => { try { return JSON.parse(line) as TranscriptRecord; } catch { throw new DagmarError("transcript_corrupt", "Transcript contains invalid JSON"); } });
    return { records, nextLine: lines.length };
  }
}

async function repair(path: string): Promise<number> {
  let data: Buffer; try { data = await readFile(path); } catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return 0; throw error; }
  const newline = data.lastIndexOf(10); const size = newline < 0 ? 0 : newline + 1;
  if (size !== data.length) await truncate(path, size);
  return data.subarray(0, size).reduce((n, byte) => n + (byte === 10 ? 1 : 0), 0);
}
function safe(value: string): void { if (!/^[A-Za-z0-9_-]+$/.test(value)) throw new DagmarError("invalid_id", "Invalid transcript identifier"); }
