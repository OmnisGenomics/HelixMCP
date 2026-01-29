import type { ArtifactRecord } from "../core/artifact.js";
import type { JsonObject } from "../core/json.js";
import type { RunId } from "../core/ids.js";
import type { PolicyEngine } from "../policy/policy.js";
import { simulateAlignReads, simulateQcFastq, type OutputArtifactSpec } from "./inSilico.js";

export interface ExecutionJob {
  runId: RunId;
  toolName: string;
  resources: {
    threads: number;
    runtimeSeconds: number;
  };
  canonicalParams: JsonObject;
  inputs: Record<string, ArtifactRecord | null>;
}

export interface ExecutionOutcome {
  outputs: OutputArtifactSpec[];
  metrics: JsonObject;
  exec?: {
    backend: "docker" | "in_silico" | "local_process";
    exitCode: number;
    stdout: string;
    stderr: string;
    startedAt: string;
    finishedAt: string;
  };
}

export interface ExecutionService {
  execute(job: ExecutionJob): Promise<ExecutionOutcome>;
}

export class DefaultExecutionService implements ExecutionService {
  constructor(private readonly deps: { policy: PolicyEngine }) {}

  async execute(job: ExecutionJob): Promise<ExecutionOutcome> {
    // Enforcement at the execution boundary (defense-in-depth).
    const threads = this.deps.policy.enforceThreads(job.toolName, job.resources.threads);
    this.deps.policy.enforceRuntimeSeconds(job.resources.runtimeSeconds);

    if (job.toolName === "simulate_qc_fastq") {
      const reads1 = job.inputs["reads_1"];
      if (!reads1) throw new Error("missing input reads_1");
      const reads2 = job.inputs["reads_2"] ?? null;
      const res = simulateQcFastq({ toolName: job.toolName, reads1, reads2, threads });
      return {
        outputs: res.outputs,
        metrics: { qc: res.qc }
      };
    }

    if (job.toolName === "simulate_align_reads") {
      const reads1 = job.inputs["reads_1"];
      if (!reads1) throw new Error("missing input reads_1");
      const reads2 = job.inputs["reads_2"] ?? null;
      const referenceAlias = String((job.canonicalParams.reference as any)?.alias ?? "");
      const sort = Boolean((job.canonicalParams as any).sort);
      const markDuplicates = Boolean((job.canonicalParams as any).mark_duplicates);
      const res = simulateAlignReads({
        toolName: job.toolName,
        reads1,
        reads2,
        referenceAlias,
        threads,
        sort,
        markDuplicates
      });
      return {
        outputs: res.outputs,
        metrics: { qc: res.qc }
      };
    }

    throw new Error(`unsupported tool for execution: ${job.toolName}`);
  }
}
