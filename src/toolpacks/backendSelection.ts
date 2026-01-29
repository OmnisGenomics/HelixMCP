import { spawnSync } from "child_process";
import type { ToolContext } from "./types.js";

export type Backend = "docker" | "slurm";

function isSbatchAvailable(): boolean {
  const res = spawnSync("sbatch", ["--version"], { stdio: ["ignore", "ignore", "ignore"], timeout: 1000 });
  if (res.error) return false;
  return res.status === 0;
}

export function resolveBackend(requested: Backend | undefined, ctx: ToolContext): Backend {
  if (requested) return requested;
  if (ctx.policy.hasSlurmConfig() && isSbatchAvailable()) return "slurm";
  return "docker";
}

