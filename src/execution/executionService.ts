import type { ArtifactRecord } from "../core/artifact.js";
import type { JsonObject } from "../core/json.js";
import type { RunId } from "../core/ids.js";
import type { PolicyEngine } from "../policy/policy.js";
import { simulateAlignReads, simulateQcFastq, type OutputArtifactSpec } from "./inSilico.js";
import type { RunnerBackend } from "./backends/types.js";
import { DockerRunner } from "./backends/dockerRunner.js";
import { createRunWorkspace } from "./workspace.js";
import type { LocalObjectStore } from "../artifacts/localObjectStore.js";
import { promises as fs } from "fs";

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
  private readonly docker: RunnerBackend<"docker">;

  constructor(
    private readonly deps: {
      policy: PolicyEngine;
      objects: LocalObjectStore;
      workspaceRootDir: string;
      docker?: RunnerBackend<"docker">;
    }
  ) {
    this.docker = deps.docker ?? new DockerRunner();
  }

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

    if (job.toolName === "seqkit_stats") {
      const input = job.inputs["input"];
      if (!input) throw new Error("missing input");

      const image = String((job.canonicalParams as any)?.docker?.image ?? "");
      this.deps.policy.assertDockerImageAllowed(image);

      const ws = await createRunWorkspace(this.deps.workspaceRootDir, job.runId);
      const inputPath = ws.inPath("input");
      await this.deps.objects.materializeToPath(input.artifactId, inputPath);

      const uid = typeof (process as any).getuid === "function" ? (process as any).getuid() : null;
      const gid = typeof (process as any).getgid === "function" ? (process as any).getgid() : null;
      const user = uid !== null && gid !== null ? `${uid}:${gid}` : undefined;

      const dockerSpec: Parameters<RunnerBackend<"docker">["execute"]>[0] = {
        kind: "docker",
        image,
        argv: ["seqkit", "stats", "-T", "/work/in/input"],
        workdir: "/work",
        network: this.deps.policy.dockerNetworkMode(),
        readOnlyRootFs: true,
        containerName: `helixmcp_${job.runId}`,
        mounts: [{ hostPath: ws.rootDir, containerPath: "/work", readOnly: false }],
        env: {
          OMP_NUM_THREADS: String(threads),
          OPENBLAS_NUM_THREADS: String(threads),
          MKL_NUM_THREADS: String(threads),
          NUMEXPR_NUM_THREADS: String(threads)
        }
      };
      if (user) dockerSpec.user = user;

      const result = await this.docker.execute(dockerSpec, { threads, runtimeSeconds: job.resources.runtimeSeconds });

      if (result.exitCode !== 0) {
        throw new Error(`seqkit stats failed (exit ${result.exitCode})`);
      }

      const outPath = ws.outPath("seqkit_stats.tsv");
      await fs.writeFile(outPath, result.stdout, "utf8");

      const { metrics, raw } = parseSeqkitStatsTsv(result.stdout);

      return {
        outputs: [
          {
            role: "report",
            type: "TSV",
            label: "seqkit_stats.tsv",
            contentText: raw
          }
        ],
        metrics: { stats: metrics },
        exec: { backend: "docker", ...result }
      };
    }

    throw new Error(`unsupported tool for execution: ${job.toolName}`);
  }
}

function parseSeqkitStatsTsv(tsv: string): { metrics: JsonObject; raw: string } {
  const lines = tsv
    .split(/\r?\n/)
    .map((l) => l.trimEnd())
    .filter((l) => l.length > 0);
  if (lines.length < 2) {
    return { metrics: {}, raw: tsv };
  }

  const headers = lines[0]!.split("\t").map((h) => h.trim());
  const values = lines[1]!.split("\t");

  const record: Record<string, string> = {};
  for (let i = 0; i < headers.length; i++) {
    const k = headers[i];
    if (!k) continue;
    record[k] = String(values[i] ?? "");
  }

  const numeric = (key: string): number | null => {
    const v = record[key];
    if (v === undefined) return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  };

  const metrics: JsonObject = {
    file: record["file"] ?? null,
    format: record["format"] ?? null,
    type: record["type"] ?? null,
    num_seqs: numeric("num_seqs"),
    sum_len: numeric("sum_len"),
    min_len: numeric("min_len"),
    avg_len: numeric("avg_len"),
    max_len: numeric("max_len"),
    raw: record
  };

  return { metrics, raw: lines.join("\n") + "\n" };
}
