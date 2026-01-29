import { ErrorCode, McpError } from "@modelcontextprotocol/sdk/types.js";
import type { ArtifactId, ProjectId, RunId } from "../../core/ids.js";
import type { JsonObject } from "../../core/json.js";
import { zSamtoolsFlagstatInput, zSlurmSubmitOutput } from "../../mcp/toolSchemas.js";
import type { PreparedToolRun, ToolContext, ToolDefinition, ToolExecutionResult } from "../types.js";
import type { ToolRun } from "../../runs/toolRun.js";
import { executeSlurmPlan, type SlurmExecutionPlan } from "../slurm/executeSlurm.js";

const SAMTOOLS_APPTAINER_IMAGE =
  "docker://quay.io/biocontainers/samtools@sha256:bf80e07e650becfd084db1abde0fe932b50f990a07fa56421ea647b552b5a406";

type Args = typeof zSamtoolsFlagstatInput._output;

export const samtoolsFlagstatSlurmTool: ToolDefinition<Args, SlurmExecutionPlan> = {
  toolName: "samtools_flagstat_slurm",
  contractVersion: "v1",
  planKind: "slurm",
  description:
    "Submit samtools flagstat as a deterministic, policy-gated Slurm job (apptainer-only, network none). Use slurm_job_collect to collect outputs.",
  inputSchema: zSamtoolsFlagstatInput,
  outputSchema: zSlurmSubmitOutput,
  declaredOutputs: [{ role: "report", type: "TEXT", label: "samtools_flagstat.txt", srcRelpath: "out/samtools_flagstat.txt" }],

  async canonicalize(args: Args, ctx: ToolContext): Promise<PreparedToolRun<SlurmExecutionPlan>> {
    const projectId = args.project_id as ProjectId;

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

    const slurm = (ctx.policy.snapshot() as any)?.slurm as
      | { partitions_allowlist?: string[]; accounts_allowlist?: string[] }
      | undefined;
    const partition = Array.isArray(slurm?.partitions_allowlist) ? (slurm?.partitions_allowlist?.[0] ?? null) : null;
    const account = Array.isArray(slurm?.accounts_allowlist) ? (slurm?.accounts_allowlist?.[0] ?? null) : null;
    if (!partition || !account) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        `policy denied slurm toolpack (missing slurm.partitions_allowlist/accounts_allowlist); configure policy or use slurm_submit`
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
  },

  async run(args: {
    runId: RunId;
    toolRun: ToolRun;
    prepared: PreparedToolRun<SlurmExecutionPlan>;
    ctx: ToolContext;
  }): Promise<ToolExecutionResult> {
    const { runId, toolRun, prepared, ctx } = args;

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
      plan: prepared.plan,
      submitter
    });

    return {
      summary: "slurm submitted",
      result: { slurm_job_id: submit.slurmJobId, slurm_script_artifact_id: submit.slurmScriptArtifactId }
    };
  }
};
