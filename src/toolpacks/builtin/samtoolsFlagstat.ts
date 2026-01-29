import { ErrorCode, McpError } from "@modelcontextprotocol/sdk/types.js";
import { promises as fs } from "fs";
import type { ArtifactId, ProjectId, RunId } from "../../core/ids.js";
import type { JsonObject } from "../../core/json.js";
import { zSamtoolsFlagstatInputV2, zSamtoolsFlagstatOutputV2 } from "../../mcp/toolSchemas.js";
import type { PreparedToolRun, ToolContext, ToolDefinition, ToolExecutionResult } from "../types.js";
import { executeDockerPlan, type DockerExecutionPlan } from "../docker/executeDocker.js";
import type { ToolRun } from "../../runs/toolRun.js";
import { createRunWorkspace } from "../../execution/workspace.js";
import { executeSlurmPlan, type SlurmExecutionPlan } from "../slurm/executeSlurm.js";

const SAMTOOLS_DOCKER_IMAGE =
  "quay.io/biocontainers/samtools@sha256:bf80e07e650becfd084db1abde0fe932b50f990a07fa56421ea647b552b5a406";

const SAMTOOLS_APPTAINER_IMAGE =
  "docker://quay.io/biocontainers/samtools@sha256:bf80e07e650becfd084db1abde0fe932b50f990a07fa56421ea647b552b5a406";

type FlagstatCount = { passed: number; failed: number };

function parseSamtoolsFlagstat(stdout: string): {
  metrics: {
    total: FlagstatCount | null;
    secondary: FlagstatCount | null;
    supplementary: FlagstatCount | null;
    duplicates: FlagstatCount | null;
    mapped: FlagstatCount | null;
    paired_in_sequencing: FlagstatCount | null;
    read1: FlagstatCount | null;
    read2: FlagstatCount | null;
    properly_paired: FlagstatCount | null;
    with_itself_and_mate_mapped: FlagstatCount | null;
    singletons: FlagstatCount | null;
    with_mate_mapped_to_different_chr: FlagstatCount | null;
    with_mate_mapped_to_different_chr_mapq5: FlagstatCount | null;
  };
  rawLines: string[];
  rawText: string;
} {
  const rawLines = stdout
    .split(/\r?\n/)
    .map((l) => l.trimEnd())
    .filter((l) => l.length > 0);

  const empty: {
    total: FlagstatCount | null;
    secondary: FlagstatCount | null;
    supplementary: FlagstatCount | null;
    duplicates: FlagstatCount | null;
    mapped: FlagstatCount | null;
    paired_in_sequencing: FlagstatCount | null;
    read1: FlagstatCount | null;
    read2: FlagstatCount | null;
    properly_paired: FlagstatCount | null;
    with_itself_and_mate_mapped: FlagstatCount | null;
    singletons: FlagstatCount | null;
    with_mate_mapped_to_different_chr: FlagstatCount | null;
    with_mate_mapped_to_different_chr_mapq5: FlagstatCount | null;
  } = {
    total: null,
    secondary: null,
    supplementary: null,
    duplicates: null,
    mapped: null,
    paired_in_sequencing: null,
    read1: null,
    read2: null,
    properly_paired: null,
    with_itself_and_mate_mapped: null,
    singletons: null,
    with_mate_mapped_to_different_chr: null,
    with_mate_mapped_to_different_chr_mapq5: null
  };

  const parseCounts = (line: string): FlagstatCount | null => {
    const m = /^(\d+)\s+\+\s+(\d+)\s+/.exec(line);
    if (!m) return null;
    return { passed: Number(m[1]), failed: Number(m[2]) };
  };

  const out: typeof empty = { ...empty };

  for (const line of rawLines) {
    const counts = parseCounts(line);
    if (!counts) continue;

    if (line.includes(" in total")) out.total = counts;
    else if (/\ssecondary\b/.test(line)) out.secondary = counts;
    else if (/\ssupplementary\b/.test(line)) out.supplementary = counts;
    else if (/\sduplicates\b/.test(line)) out.duplicates = counts;
    else if (/\smapped\b/.test(line) && !line.includes("mate mapped")) out.mapped = counts;
    else if (line.includes(" paired in sequencing")) out.paired_in_sequencing = counts;
    else if (/\sread1\b/.test(line)) out.read1 = counts;
    else if (/\sread2\b/.test(line)) out.read2 = counts;
    else if (line.includes(" properly paired")) out.properly_paired = counts;
    else if (line.includes(" with itself and mate mapped")) out.with_itself_and_mate_mapped = counts;
    else if (line.includes(" singletons")) out.singletons = counts;
    else if (line.includes(" with mate mapped to a different chr (mapQ>=5)")) out.with_mate_mapped_to_different_chr_mapq5 = counts;
    else if (line.includes(" with mate mapped to a different chr")) out.with_mate_mapped_to_different_chr = counts;
  }

  return { metrics: out, rawLines, rawText: rawLines.join("\n") + "\n" };
}

type Args = typeof zSamtoolsFlagstatInputV2._output;

export const samtoolsFlagstatTool: ToolDefinition<Args, DockerExecutionPlan | SlurmExecutionPlan> = {
  toolName: "samtools_flagstat",
  contractVersion: "v2",
  planKind: "hybrid",
  description:
    'Run samtools flagstat on a BAM artifact (deterministic, policy-gated). Use backend="docker" for immediate execution or backend="slurm" for queued Slurm execution (collect via slurm_job_collect).',
  inputSchema: zSamtoolsFlagstatInputV2,
  outputSchema: zSamtoolsFlagstatOutputV2,
  declaredOutputs: [{ role: "report", type: "TEXT", label: "samtools_flagstat.txt", srcRelpath: "out/samtools_flagstat.txt" }],

  async canonicalize(args: Args, ctx: ToolContext): Promise<PreparedToolRun<DockerExecutionPlan | SlurmExecutionPlan>> {
    const projectId = args.project_id as ProjectId;
    const backend = args.backend ?? ctx.policy.defaultBackend();

    const bam = await ctx.artifacts.getArtifact(args.bam_artifact_id as ArtifactId);
    if (!bam) {
      throw new McpError(ErrorCode.InvalidParams, `unknown bam_artifact_id: ${args.bam_artifact_id}`);
    }
    if (bam.projectId !== projectId) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied cross-project artifact: ${bam.artifactId}`);
    }
    if (bam.type !== "BAM") {
      throw new McpError(ErrorCode.InvalidParams, `bam_artifact_id must be type BAM (got ${bam.type})`);
    }

    if (backend === "slurm") {
      const slurm = (ctx.policy.snapshot() as any)?.slurm as
        | { partitions_allowlist?: string[]; accounts_allowlist?: string[] }
        | undefined;
      const partition = Array.isArray(slurm?.partitions_allowlist) ? (slurm?.partitions_allowlist?.[0] ?? null) : null;
      const account = Array.isArray(slurm?.accounts_allowlist) ? (slurm?.accounts_allowlist?.[0] ?? null) : null;
      if (!partition || !account) {
        throw new McpError(
          ErrorCode.InvalidRequest,
          `policy denied slurm backend (missing slurm.partitions_allowlist/accounts_allowlist); configure policy or use backend="docker"`
        );
      }

      const plan: SlurmExecutionPlan = {
        version: 1,
        resources: { time_limit_seconds: 900, cpus: 1, mem_mb: 2048, gpus: null, gpu_type: null },
        placement: { partition, account, qos: null, constraint: null },
        execution: {
          kind: "container",
          container: { engine: "apptainer", image: SAMTOOLS_APPTAINER_IMAGE, network_mode: "none", readonly_rootfs: true },
          command: {
            argv: [
              "sh",
              "-c",
              "set -euo pipefail; samtools flagstat /work/in/input.bam > /work/out/samtools_flagstat.txt"
            ],
            workdir: "/work",
            env: {}
          }
        },
        io: {
          inputs: [
            {
              role: "bam",
              artifact_id: bam.artifactId,
              checksum_sha256: bam.checksumSha256,
              dest_relpath: "in/input.bam"
            }
          ],
          outputs: [
            {
              role: "report",
              src_relpath: "out/samtools_flagstat.txt",
              type: "TEXT",
              label: "samtools_flagstat.txt"
            }
          ]
        }
      };

      const canonicalParams: JsonObject = {
        project_id: projectId,
        backend: "slurm",
        bam: {
          checksum_sha256: bam.checksumSha256,
          type: bam.type,
          size_bytes: bam.sizeBytes.toString()
        },
        slurm: {
          spec_version: 1,
          slurm_script_version: "slurm_script_v1",
          resources: plan.resources,
          placement: plan.placement,
          execution: plan.execution,
          io: {
            inputs: plan.io.inputs.map((i) => ({
              role: i.role,
              artifact_id: i.artifact_id,
              checksum_sha256: i.checksum_sha256,
              dest_relpath: i.dest_relpath
            })),
            outputs: plan.io.outputs.map((o) => ({
              role: o.role,
              src_relpath: o.src_relpath,
              type: o.type,
              label: o.label
            }))
          }
        }
      };

      return {
        projectId,
        canonicalParams,
        toolVersion: "slurm_script_v1",
        plan,
        selectedPlanKind: "slurm",
        inputsToLink: [{ artifactId: bam.artifactId, role: "bam" }]
      };
    }

    const threads = ctx.policy.enforceThreads("samtools_flagstat", 1);
    const runtimeSeconds = ctx.policy.maxRuntimeSeconds();

    const canonicalParams: JsonObject = {
      project_id: projectId,
      backend: "docker",
      bam: {
        checksum_sha256: bam.checksumSha256,
        type: bam.type,
        size_bytes: bam.sizeBytes.toString()
      },
      threads,
      docker: {
        image: SAMTOOLS_DOCKER_IMAGE,
        network_mode: ctx.policy.dockerNetworkMode(),
        argv: ["samtools", "flagstat", "/work/in/input.bam"]
      }
    };

    const docker: DockerExecutionPlan = {
      image: SAMTOOLS_DOCKER_IMAGE,
      argv: ["samtools", "flagstat", "/work/in/input.bam"],
      workdir: "/work",
      inputs: [{ role: "bam", artifact: bam, destName: "input.bam" }],
      resources: { threads, runtimeSeconds }
    };

    return {
      projectId,
      canonicalParams,
      toolVersion: SAMTOOLS_DOCKER_IMAGE,
      plan: docker,
      selectedPlanKind: "docker",
      inputsToLink: [{ artifactId: bam.artifactId, role: "bam" }]
    };
  },

  async run(args: {
    runId: RunId;
    toolRun: ToolRun;
    prepared: PreparedToolRun<DockerExecutionPlan | SlurmExecutionPlan>;
    ctx: ToolContext;
  }): Promise<ToolExecutionResult> {
    const { runId, toolRun, prepared, ctx } = args;

    if (prepared.selectedPlanKind === "slurm") {
      const submitter = ctx.slurmSubmitter;
      if (!submitter) {
        throw new McpError(ErrorCode.InvalidRequest, `slurm submitter not configured`);
      }

      const submit = await executeSlurmPlan({
        policy: ctx.policy,
        artifacts: ctx.artifacts,
        runsDir: ctx.runsDir,
        runId,
        projectId: prepared.projectId as ProjectId,
        toolRun,
        plan: prepared.plan as SlurmExecutionPlan,
        submitter
      });

      return {
        summary: "slurm submitted",
        result: { slurm_job_id: submit.slurmJobId, slurm_script_artifact_id: submit.slurmScriptArtifactId }
      };
    }

    await toolRun.event("exec.plan", `backend=docker image=${SAMTOOLS_DOCKER_IMAGE}`, null);

    const result = await executeDockerPlan({
      policy: ctx.policy,
      runsDir: ctx.runsDir,
      runId,
      toolName: "samtools_flagstat",
      plan: prepared.plan as DockerExecutionPlan,
      materializeToPath: async (artifactId: string, destPath: string) =>
        ctx.artifacts.materializeToPath(artifactId as any, destPath)
    });

    await toolRun.event("exec.result", `exit=${result.exitCode}`, {
      backend: "docker",
      started_at: result.startedAt,
      finished_at: result.finishedAt,
      stderr: result.stderr
    });

    if (result.exitCode !== 0) {
      throw new Error(`samtools flagstat failed (exit ${result.exitCode})`);
    }

    const ws = await createRunWorkspace(ctx.runsDir, runId);
    const wsReportPath = ws.outPath("samtools_flagstat.txt");
    await fs.writeFile(wsReportPath, result.stdout, "utf8");

    const { metrics, rawLines, rawText } = parseSamtoolsFlagstat(result.stdout);

    const reportArtifactId = await toolRun.createOutputArtifact({
      type: "TEXT",
      label: "samtools_flagstat.txt",
      contentText: rawText,
      role: "report"
    });

    return {
      summary: "samtools flagstat ok",
      result: {
        report_artifact_id: reportArtifactId,
        flagstat: { ...metrics, raw_lines: rawLines }
      }
    };
  }
};
