import { spawn } from "child_process";
import type { ExecutionResources, ExecutionResult, LocalProcessSpec, RunnerBackend } from "./types.js";

const MAX_CAPTURE_BYTES = 1024 * 1024;

function appendLimited(chunks: Buffer[], chunk: Buffer, state: { bytes: number; truncated: boolean }): void {
  if (state.truncated) return;
  const next = state.bytes + chunk.byteLength;
  if (next > MAX_CAPTURE_BYTES) {
    const keep = Math.max(0, MAX_CAPTURE_BYTES - state.bytes);
    if (keep > 0) chunks.push(chunk.subarray(0, keep));
    state.bytes = MAX_CAPTURE_BYTES;
    state.truncated = true;
    return;
  }
  chunks.push(chunk);
  state.bytes = next;
}

export class LocalProcessRunner implements RunnerBackend<"local_process"> {
  readonly kind = "local_process" as const;

  async execute(spec: LocalProcessSpec, _resources: ExecutionResources): Promise<ExecutionResult> {
    const [command, ...args] = spec.argv;
    if (!command) throw new Error("local_process argv must be non-empty");
    const startedAt = new Date().toISOString();

    const child = spawn(command, args, {
      cwd: spec.cwd,
      env: { ...process.env, ...spec.env },
      stdio: ["ignore", "pipe", "pipe"] as const
    });

    const stdoutChunks: Buffer[] = [];
    const stderrChunks: Buffer[] = [];
    const stdoutState = { bytes: 0, truncated: false };
    const stderrState = { bytes: 0, truncated: false };

    child.stdout.on("data", (chunk: Buffer) => appendLimited(stdoutChunks, chunk, stdoutState));
    child.stderr.on("data", (chunk: Buffer) => appendLimited(stderrChunks, chunk, stderrState));

    const exitCode = await new Promise<number>((resolve, reject) => {
      child.on("error", reject);
      child.on("close", (code: number | null) => resolve(code ?? 0));
    });

    const finishedAt = new Date().toISOString();

    const stdout = Buffer.concat(stdoutChunks).toString("utf8") + (stdoutState.truncated ? "\n[stdout truncated]\n" : "");
    const stderr = Buffer.concat(stderrChunks).toString("utf8") + (stderrState.truncated ? "\n[stderr truncated]\n" : "");

    return { exitCode, stdout, stderr, startedAt, finishedAt };
  }
}
