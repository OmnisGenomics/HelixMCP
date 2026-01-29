import { ErrorCode, McpError } from "@modelcontextprotocol/sdk/types.js";
import { promises as fs } from "fs";
import type { ArtifactId, ProjectId, RunId } from "../../core/ids.js";
import type { ArtifactService } from "../../artifacts/artifactService.js";
import type { PolicyEngine } from "../../policy/policy.js";
import { createRunWorkspace, safeJoin } from "../../execution/workspace.js";
import { renderSlurmScriptV1, type SlurmJobSpecV1 } from "../../execution/slurm/slurmScriptV1.js";
import type { SlurmSubmitter } from "../../execution/slurm/submitter.js";
import type { ToolRun } from "../../runs/toolRun.js";

export type SlurmExecutionPlan = SlurmJobSpecV1;

export interface SlurmExecutionOutcome {
  slurmJobId: string;
  slurmScript: string;
  slurmScriptArtifactId: ArtifactId;
}

export async function executeSlurmPlan(input: {
  policy: PolicyEngine;
  artifacts: ArtifactService;
  runsDir: string;
  runId: RunId;
  projectId: ProjectId;
  toolRun: ToolRun;
  plan: SlurmExecutionPlan;
  submitter: SlurmSubmitter;
}): Promise<SlurmExecutionOutcome> {
  const { policy, artifacts, runsDir, runId, projectId, toolRun, plan, submitter } = input;

  if (plan.version !== 1) {
    throw new McpError(ErrorCode.InvalidParams, `slurm plan version must be 1`);
  }
  if (plan.execution.kind !== "container") {
    throw new McpError(ErrorCode.InvalidParams, `slurm execution.kind must be "container"`);
  }
  if (plan.execution.container.engine !== "apptainer") {
    throw new McpError(ErrorCode.InvalidParams, `slurm container.engine must be "apptainer"`);
  }
  if (plan.execution.container.network_mode !== "none") {
    throw new McpError(ErrorCode.InvalidParams, `slurm container.network_mode must be "none"`);
  }
  if (plan.execution.container.readonly_rootfs !== true) {
    throw new McpError(ErrorCode.InvalidParams, `slurm container.readonly_rootfs must be true`);
  }

  policy.assertSlurmPartitionAllowed(plan.placement.partition);
  policy.assertSlurmAccountAllowed(plan.placement.account);
  policy.assertSlurmQosAllowed(plan.placement.qos ?? null);
  policy.assertSlurmConstraintAllowed(plan.placement.constraint ?? null);
  policy.enforceSlurmResources({
    timeLimitSeconds: plan.resources.time_limit_seconds,
    cpus: plan.resources.cpus,
    memMb: plan.resources.mem_mb,
    gpus: plan.resources.gpus ?? null,
    gpuType: plan.resources.gpu_type ?? null
  });
  policy.assertSlurmNetworkNone(plan.execution.container.network_mode);
  policy.assertSlurmApptainerImageAllowed(plan.execution.container.image);

  await toolRun.event("slurm.submit.plan", `partition=${plan.placement.partition} account=${plan.placement.account}`, {
    partition: plan.placement.partition,
    account: plan.placement.account,
    qos: plan.placement.qos ?? null,
    constraint: plan.placement.constraint ?? null,
    resources: plan.resources,
    image: plan.execution.container.image
  });

  const ws = await createRunWorkspace(runsDir, runId);

  for (const inp of plan.io.inputs) {
    const artifactId = inp.artifact_id as ArtifactId;
    const artifact = await artifacts.getArtifact(artifactId);
    if (!artifact) {
      throw new McpError(ErrorCode.InvalidParams, `unknown artifact_id in slurm input: ${artifactId}`);
    }
    if (artifact.projectId !== projectId) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied cross-project artifact: ${artifactId}`);
    }
    if (artifact.checksumSha256 !== inp.checksum_sha256) {
      throw new McpError(
        ErrorCode.InvalidParams,
        `checksum mismatch for artifact ${artifactId} (expected ${inp.checksum_sha256}, got ${artifact.checksumSha256})`
      );
    }

    const destPath = safeJoin(ws.rootDir, inp.dest_relpath);
    await artifacts.materializeToPath(artifactId, destPath);
  }

  const script = renderSlurmScriptV1({
    workspaceRootDir: ws.rootDir,
    jobName: `helixmcp_${runId}`,
    spec: plan
  });

  const scriptPath = ws.metaPath("slurm_script.sbatch");
  await fs.writeFile(scriptPath, script, "utf8");

  const scriptArtifactId = await toolRun.createOutputArtifact({
    type: "TEXT",
    label: "slurm_script.sbatch",
    contentText: script,
    role: "slurm_script"
  });

  await toolRun.event("slurm.submit.script_artifact", `artifact=${scriptArtifactId}`, {
    slurm_script_artifact_id: scriptArtifactId
  });

  const submit = await submitter.submit(scriptPath);
  await fs.writeFile(ws.metaPath("slurm_job_id.txt"), submit.slurmJobId + "\n", "utf8");

  await toolRun.event("slurm.submit.ok", `job_id=${submit.slurmJobId}`, { slurm_job_id: submit.slurmJobId });

  return {
    slurmJobId: submit.slurmJobId,
    slurmScript: script,
    slurmScriptArtifactId: scriptArtifactId
  };
}

