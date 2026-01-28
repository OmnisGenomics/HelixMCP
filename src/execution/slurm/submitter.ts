import { spawnSync } from "child_process";

export interface SlurmSubmitResult {
  slurmJobId: string;
  stdout: string;
  stderr: string;
}

export interface SlurmSubmitter {
  submit(scriptPath: string): Promise<SlurmSubmitResult>;
}

function parseSbatchJobId(stdout: string): string | null {
  const trimmed = stdout.trim();
  if (!trimmed) return null;

  const parsable = trimmed.split(/\s+/)[0] ?? "";
  if (/^\d+/.test(parsable)) {
    const first = parsable.split(";")[0];
    return first ? first : null;
  }

  const m = /Submitted batch job\s+(\d+)/.exec(trimmed);
  return m && m[1] ? m[1] : null;
}

export class SbatchSubmitter implements SlurmSubmitter {
  async submit(scriptPath: string): Promise<SlurmSubmitResult> {
    const res = spawnSync("sbatch", ["--parsable", scriptPath], { stdio: ["ignore", "pipe", "pipe"] });
    const stdout = res.stdout ? res.stdout.toString("utf8") : "";
    const stderr = res.stderr ? res.stderr.toString("utf8") : "";

    if (res.error) {
      throw res.error;
    }
    if (res.status !== 0) {
      throw new Error(`sbatch failed (exit ${res.status})${stderr ? `: ${stderr}` : ""}`);
    }

    const jobId = parseSbatchJobId(stdout) ?? parseSbatchJobId(stderr);
    if (!jobId) {
      throw new Error(`unable to parse sbatch job id from output: ${stdout || stderr}`);
    }

    return { slurmJobId: jobId, stdout, stderr };
  }
}
