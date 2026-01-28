import { spawn } from "child_process";
import type { DockerSpec, ExecutionResources, ExecutionResult, RunnerBackend } from "./types.js";

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

export class DockerRunner implements RunnerBackend<"docker"> {
  readonly kind = "docker" as const;

  async execute(spec: DockerSpec, resources: ExecutionResources): Promise<ExecutionResult> {
    if (!spec.image) throw new Error("docker image must be non-empty");
    if (!spec.argv.length) throw new Error("docker argv must be non-empty");

    const startedAt = new Date().toISOString();
    const args: string[] = ["run", "--rm"];

    if (spec.containerName) {
      args.push("--name", spec.containerName);
    }

    args.push("--network", spec.network ?? "none");

    if (spec.readOnlyRootFs ?? true) {
      args.push("--read-only");
      args.push("--tmpfs", "/tmp:rw,noexec,nosuid,size=64m");
    }

    for (const t of spec.tmpfs ?? []) {
      args.push("--tmpfs", `${t.containerPath}${t.options ? `:${t.options}` : ""}`);
    }

    for (const [k, v] of Object.entries(spec.env ?? {})) {
      args.push("--env", `${k}=${v}`);
    }

    for (const m of spec.mounts ?? []) {
      const mode = m.readOnly ? "ro" : "rw";
      args.push("--volume", `${m.hostPath}:${m.containerPath}:${mode}`);
    }

    if (spec.user) {
      args.push("--user", spec.user);
    }

    if (spec.workdir) {
      args.push("--workdir", spec.workdir);
    }

    args.push(spec.image, ...spec.argv);

    const child = spawn("docker", args, {
      stdio: ["ignore", "pipe", "pipe"] as const
    });

    const stdoutChunks: Buffer[] = [];
    const stderrChunks: Buffer[] = [];
    const stdoutState = { bytes: 0, truncated: false };
    const stderrState = { bytes: 0, truncated: false };

    child.stdout.on("data", (chunk: Buffer) => appendLimited(stdoutChunks, chunk, stdoutState));
    child.stderr.on("data", (chunk: Buffer) => appendLimited(stderrChunks, chunk, stderrState));

    let timedOut = false;
    const timeoutMs = Math.max(0, Math.floor(resources.runtimeSeconds * 1000));
    const timeout =
      timeoutMs > 0
        ? setTimeout(() => {
            timedOut = true;
            if (spec.containerName) {
              const rm = spawn("docker", ["rm", "-f", spec.containerName], { stdio: "ignore" });
              rm.unref();
            }
            child.kill("SIGKILL");
          }, timeoutMs)
        : null;

    const exitCode = await new Promise<number>((resolve, reject) => {
      child.on("error", reject);
      child.on("close", (code: number | null) => resolve(code ?? 0));
    }).finally(() => {
      if (timeout) clearTimeout(timeout);
    });

    const finishedAt = new Date().toISOString();

    const stdout =
      Buffer.concat(stdoutChunks).toString("utf8") +
      (stdoutState.truncated ? "\n[stdout truncated]\n" : "") +
      (timedOut ? "\n[timeout]\n" : "");

    const stderr =
      Buffer.concat(stderrChunks).toString("utf8") +
      (stderrState.truncated ? "\n[stderr truncated]\n" : "") +
      (timedOut ? "\n[timeout]\n" : "");

    return { exitCode, stdout, stderr, startedAt, finishedAt };
  }
}

