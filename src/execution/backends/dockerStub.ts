import type { ExecutionResources, ExecutionResult, DockerSpec, RunnerBackend } from "./types.js";

export class DockerRunnerStub implements RunnerBackend<"docker"> {
  readonly kind = "docker" as const;

  async execute(_spec: DockerSpec, _resources: ExecutionResources): Promise<ExecutionResult> {
    throw new Error("docker runner not implemented (stub)");
  }
}

