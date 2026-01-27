import type { ExecutionResources, ExecutionResult, SlurmSpec, RunnerBackend } from "./types.js";

export class SlurmRunnerStub implements RunnerBackend<"slurm"> {
  readonly kind = "slurm" as const;

  async execute(_spec: SlurmSpec, _resources: ExecutionResources): Promise<ExecutionResult> {
    throw new Error("slurm runner not implemented (stub)");
  }
}

