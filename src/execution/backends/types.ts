export interface ExecutionResources {
  threads: number;
  runtimeSeconds: number;
  memoryMb?: number;
}

export interface ExecutionResult {
  exitCode: number;
  stdout: string;
  stderr: string;
  startedAt: string;
  finishedAt: string;
}

export interface LocalProcessSpec {
  kind: "local_process";
  argv: string[];
  cwd?: string;
  env?: Record<string, string>;
}

export interface DockerSpec {
  kind: "docker";
  image: string;
  argv: string[];
  workdir?: string;
}

export interface SlurmSpec {
  kind: "slurm";
  script: string;
  partition?: string;
}

export type ExecutionSpec = LocalProcessSpec | DockerSpec | SlurmSpec;

export interface RunnerBackend<K extends ExecutionSpec["kind"] = ExecutionSpec["kind"]> {
  kind: K;
  execute(spec: Extract<ExecutionSpec, { kind: K }>, resources: ExecutionResources): Promise<ExecutionResult>;
}

