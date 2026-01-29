import { spawnSync } from "child_process";
import type { ToolContext } from "./types.js";

export type Backend = "docker" | "slurm";

function isSbatchAvailable(): boolean {
  const res = spawnSync("sbatch", ["--version"], { stdio: ["ignore", "ignore", "ignore"], timeout: 1000 });
  if (res.error) return false;
  return res.status === 0;
}

function hasUsableSlurmDefaults(ctx: ToolContext): boolean {
  if (!ctx.policy.hasSlurmConfig()) return false;
  const slurm = (ctx.policy.snapshot() as any)?.slurm as
    | { partitions_allowlist?: unknown; accounts_allowlist?: unknown }
    | undefined;

  return (
    Array.isArray(slurm?.partitions_allowlist) &&
    slurm.partitions_allowlist.length > 0 &&
    Array.isArray(slurm?.accounts_allowlist) &&
    slurm.accounts_allowlist.length > 0
  );
}

export function resolveBackend(requested: Backend | undefined, ctx: ToolContext): Backend {
  if (requested) return requested;
  if (hasUsableSlurmDefaults(ctx) && isSbatchAvailable()) return "slurm";
  return "docker";
}
