import { spawnSync } from "child_process";

export type SlurmNormalizedState = "queued" | "running" | "succeeded" | "failed" | "unknown";

export interface SlurmSchedulerInfo {
  source: "sacct" | "squeue";
  stateRaw: string;
  normalizedState: SlurmNormalizedState;
  exitCode: number | null;
}

export interface SlurmSchedulerQueryResult {
  info: SlurmSchedulerInfo | null;
  warnings: string[];
}

export interface SlurmScheduler {
  query(jobId: string): Promise<SlurmSchedulerQueryResult>;
}

function normalizeSlurmState(stateRaw: string, exitCode: number | null): SlurmNormalizedState {
  const s = String(stateRaw).trim().toUpperCase();
  if (!s) return "unknown";

  if (s.includes("PENDING") || s === "PD" || s.includes("CONFIGURING")) return "queued";
  if (s.includes("RUNNING") || s === "R" || s.includes("COMPLETING")) return "running";

  if (s.includes("COMPLETED")) {
    if (exitCode === null) return "unknown";
    return exitCode === 0 ? "succeeded" : "failed";
  }

  if (
    s.includes("FAILED") ||
    s.includes("CANCELLED") ||
    s.includes("TIMEOUT") ||
    s.includes("NODE_FAIL") ||
    s.includes("OUT_OF_MEMORY") ||
    s.includes("PREEMPTED")
  ) {
    return "failed";
  }

  return "unknown";
}

function parseExitCodeField(value: string): number | null {
  const trimmed = value.trim();
  if (!trimmed) return null;
  const first = trimmed.split(":")[0] ?? "";
  const n = Number.parseInt(first, 10);
  return Number.isInteger(n) ? n : null;
}

function trySacct(jobId: string): { info: SlurmSchedulerInfo | null; warnings: string[] } {
  const res = spawnSync("sacct", ["-j", jobId, "--noheader", "--parsable2", "-o", "JobIDRaw,State,ExitCode"], {
    stdio: ["ignore", "pipe", "pipe"]
  });

  const stdout = res.stdout ? res.stdout.toString("utf8") : "";
  const stderr = res.stderr ? res.stderr.toString("utf8") : "";

  if (res.error) {
    return { info: null, warnings: [`sacct unavailable: ${res.error.message}`] };
  }
  if (res.status !== 0) {
    return { info: null, warnings: [`sacct failed (exit ${res.status})${stderr ? `: ${stderr.trim()}` : ""}`] };
  }

  const lines = stdout
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l.length > 0);

  if (!lines.length) {
    return { info: null, warnings: ["sacct returned no rows"] };
  }

  const parsed = lines
    .map((l) => {
      const [job, state, exit] = l.split("|");
      return { job: job ?? "", state: state ?? "", exit: exit ?? "" };
    })
    .filter((r) => r.job.length > 0);

  const primary =
    parsed.find((r) => r.job === jobId) ??
    parsed.find((r) => r.job.startsWith(jobId) && !r.job.includes(".")) ??
    parsed[0]!;

  const exitCode = parseExitCodeField(primary.exit);
  const normalizedState = normalizeSlurmState(primary.state, exitCode);
  return {
    info: { source: "sacct", stateRaw: primary.state, normalizedState, exitCode },
    warnings: []
  };
}

function trySqueue(jobId: string): { info: SlurmSchedulerInfo | null; warnings: string[] } {
  const res = spawnSync("squeue", ["-j", jobId, "-h", "-o", "%T"], { stdio: ["ignore", "pipe", "pipe"] });
  const stdout = res.stdout ? res.stdout.toString("utf8") : "";
  const stderr = res.stderr ? res.stderr.toString("utf8") : "";

  if (res.error) {
    return { info: null, warnings: [`squeue unavailable: ${res.error.message}`] };
  }
  if (res.status !== 0) {
    return { info: null, warnings: [`squeue failed (exit ${res.status})${stderr ? `: ${stderr.trim()}` : ""}`] };
  }

  const state = stdout.trim();
  if (!state) return { info: null, warnings: ["squeue returned no rows"] };

  const normalizedState = normalizeSlurmState(state, null);
  return { info: { source: "squeue", stateRaw: state, normalizedState, exitCode: null }, warnings: [] };
}

export class SystemSlurmScheduler implements SlurmScheduler {
  async query(jobId: string): Promise<SlurmSchedulerQueryResult> {
    const warnings: string[] = [];

    const sacct = trySacct(jobId);
    warnings.push(...sacct.warnings);
    if (sacct.info) return { info: sacct.info, warnings };

    const squeue = trySqueue(jobId);
    warnings.push(...squeue.warnings);
    if (squeue.info) return { info: squeue.info, warnings };

    return { info: null, warnings };
  }
}

