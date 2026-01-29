import { ErrorCode, McpError } from "@modelcontextprotocol/sdk/types.js";
import { promises as fs } from "fs";
import type { ArtifactId, ProjectId, RunId } from "../../core/ids.js";
import type { JsonObject } from "../../core/json.js";
import { sha256Prefixed, stableJsonStringify } from "../../core/canonicalJson.js";
import { zMultiqcInput, zMultiqcOutputV1 } from "../../mcp/toolSchemas.js";
import type { PreparedToolRun, ToolContext, ToolDefinition, ToolExecutionResult } from "../types.js";
import { resolveBackend } from "../backendSelection.js";
import { executeDockerPlan, type DockerExecutionPlan } from "../docker/executeDocker.js";
import type { ToolRun } from "../../runs/toolRun.js";
import { createRunWorkspace } from "../../execution/workspace.js";
import { executeSlurmPlan, type SlurmExecutionPlan } from "../slurm/executeSlurm.js";

const MULTIQC_DOCKER_IMAGE =
  "quay.io/biocontainers/multiqc@sha256:ecafca93ba3346775b773bbfd6ff920ecfc259f554777576c15d3139c678311b";

const MULTIQC_APPTAINER_IMAGE =
  "docker://quay.io/biocontainers/multiqc@sha256:ecafca93ba3346775b773bbfd6ff920ecfc259f554777576c15d3139c678311b";

async function assertRegularFileNoSymlink(filePath: string, context: string): Promise<void> {
  const st = await fs.lstat(filePath);
  if (st.isSymbolicLink()) throw new Error(`policy denied symlinked file for ${context}`);
  if (!st.isFile()) throw new Error(`expected file for ${context}: ${filePath}`);
}

function multiqcScript(): string {
  const pyZip = [
    "python3 - <<'PY'",
    "import pathlib, zipfile",
    "base = pathlib.Path('/work/out/multiqc_data')",
    "out = pathlib.Path('/work/out/multiqc_data.zip')",
    "with zipfile.ZipFile(out, 'w', compression=zipfile.ZIP_DEFLATED) as z:",
    "  for p in sorted(base.rglob('*')):",
    "    if p.is_file():",
    "      z.write(p, p.relative_to(base).as_posix())",
    "PY"
  ].join("\n");

  return ["set -eu", "multiqc /work/in -o /work/out", pyZip].join("; ");
}

function pad4(n: number): string {
  return String(n).padStart(4, "0");
}

function parseMultiqcGeneralStatsText(text: string): { samples: number | null; rawLines: string[] | null } {
  const rawLines = text
    .split(/\r?\n/)
    .map((l) => l.trimEnd())
    .filter((l) => l.length > 0);
  if (rawLines.length < 2) return { samples: null, rawLines: rawLines.length ? rawLines.slice(0, 50) : null };
  // Header + rows
  const samples = Math.max(0, rawLines.length - 1);
  return { samples, rawLines: rawLines.slice(0, 50) };
}

type Args = typeof zMultiqcInput._output;

export const multiqcTool: ToolDefinition<Args, DockerExecutionPlan | SlurmExecutionPlan> = {
  abiVersion: "v1",
  toolName: "multiqc",
  contractVersion: "v1",
  planKind: "hybrid",
  description: 'Run MultiQC over FastQC ZIP artifacts (Docker or queued Slurm). Produces an HTML report and a data ZIP bundle.',
  inputSchema: zMultiqcInput,
  outputSchema: zMultiqcOutputV1,
  declaredOutputs: [
    { role: "multiqc_html", type: "HTML", label: "multiqc_report.html", srcRelpath: "out/multiqc_report.html" },
    { role: "multiqc_data_zip", type: "ZIP", label: "multiqc_data.zip", srcRelpath: "out/multiqc_data.zip" }
  ],

  async canonicalize(args: Args, ctx: ToolContext): Promise<PreparedToolRun<DockerExecutionPlan | SlurmExecutionPlan>> {
    const projectId = args.project_id as ProjectId;
    const backend = resolveBackend(args.backend, ctx);
    const threads = ctx.policy.enforceThreads("multiqc", args.threads);
    const runtimeSeconds = ctx.policy.maxRuntimeSeconds();

    const ids = args.fastqc_zip_artifact_ids as unknown as string[];
    const seen = new Set<string>();
    for (const id of ids) {
      if (seen.has(id)) {
        throw new McpError(ErrorCode.InvalidParams, `fastqc_zip_artifact_ids must not contain duplicates: ${id}`);
      }
      seen.add(id);
    }

    const artifacts = [];
    for (const id of ids) {
      const art = await ctx.artifacts.getArtifact(id as ArtifactId);
      if (!art) {
        throw new McpError(ErrorCode.InvalidParams, `unknown fastqc_zip_artifact_id: ${id}`);
      }
      if (art.projectId !== projectId) {
        throw new McpError(ErrorCode.InvalidRequest, `policy denied cross-project artifact: ${art.artifactId}`);
      }
      if (art.type !== "ZIP") {
        throw new McpError(ErrorCode.InvalidParams, `fastqc_zip_artifact_ids must be type ZIP (got ${art.type} for ${art.artifactId})`);
      }
      artifacts.push(art);
    }

    const sorted = [...artifacts].sort((a, b) => {
      const c = a.checksumSha256.localeCompare(b.checksumSha256);
      if (c !== 0) return c;
      if (a.sizeBytes < b.sizeBytes) return -1;
      if (a.sizeBytes > b.sizeBytes) return 1;
      return a.artifactId.localeCompare(b.artifactId);
    });

    const inputsMeta = sorted.map((a) => ({
      checksum_sha256: a.checksumSha256,
      type: a.type,
      size_bytes: a.sizeBytes.toString()
    }));
    const inputsDigest = sha256Prefixed(stableJsonStringify(inputsMeta));

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

      const planInputs = sorted.map((a, idx) => ({
        role: "fastqc_zip",
        artifact_id: a.artifactId,
        checksum_sha256: a.checksumSha256,
        // MultiQC's FastQC module matching is filename-sensitive; preserve the "*_fastqc.zip" suffix.
        dest_relpath: `in/fastqc_${pad4(idx + 1)}_fastqc.zip`
      }));

      const plan: SlurmExecutionPlan = {
        version: 1,
        resources: { time_limit_seconds: 3600, cpus: threads, mem_mb: 4096, gpus: null, gpu_type: null },
        placement: { partition, account, qos: null, constraint: null },
        execution: {
          kind: "container",
          container: { engine: "apptainer", image: MULTIQC_APPTAINER_IMAGE, network_mode: "none", readonly_rootfs: true },
          command: { argv: ["sh", "-c", multiqcScript()], workdir: "/work", env: {} }
        },
        io: {
          inputs: planInputs,
          outputs: [
            { role: "multiqc_html", src_relpath: "out/multiqc_report.html", type: "HTML", label: "multiqc_report.html" },
            { role: "multiqc_data_zip", src_relpath: "out/multiqc_data.zip", type: "ZIP", label: "multiqc_data.zip" }
          ]
        }
      };

      const canonicalParams: JsonObject = {
        project_id: projectId,
        backend: "slurm",
        inputs_count: inputsMeta.length,
        inputs: inputsMeta,
        inputs_digest_sha256: inputsDigest,
        threads,
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
        inputsToLink: sorted.map((a) => ({ artifactId: a.artifactId, role: "fastqc_zip" }))
      };
    }

    const planInputs = sorted.map((a, idx) => ({
      role: "fastqc_zip",
      artifact: a,
      // MultiQC's FastQC module matching is filename-sensitive; preserve the "*_fastqc.zip" suffix.
      destName: `fastqc_${pad4(idx + 1)}_fastqc.zip`
    }));

    const plan: DockerExecutionPlan = {
      image: MULTIQC_DOCKER_IMAGE,
      argv: ["sh", "-c", multiqcScript()],
      workdir: "/work",
      inputs: planInputs,
      resources: { threads, runtimeSeconds }
    };

    const canonicalParams: JsonObject = {
      project_id: projectId,
      backend: "docker",
      inputs_count: inputsMeta.length,
      inputs: inputsMeta,
      inputs_digest_sha256: inputsDigest,
      threads,
      docker: {
        image: MULTIQC_DOCKER_IMAGE,
        network_mode: ctx.policy.dockerNetworkMode(),
        argv: plan.argv,
        input_filenames: planInputs.map((i) => i.destName)
      }
    };

    return {
      projectId,
      canonicalParams,
      toolVersion: MULTIQC_DOCKER_IMAGE,
      plan,
      selectedPlanKind: "docker",
      inputsToLink: sorted.map((a) => ({ artifactId: a.artifactId, role: "fastqc_zip" }))
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

    await toolRun.event("exec.plan", `backend=docker image=${MULTIQC_DOCKER_IMAGE}`, null);

    const result = await executeDockerPlan({
      policy: ctx.policy,
      runsDir: ctx.runsDir,
      runId,
      toolName: "multiqc",
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
      throw new Error(`multiqc failed (exit ${result.exitCode})`);
    }

    const ws = await createRunWorkspace(ctx.runsDir, runId);
    const htmlPath = ws.outPath("multiqc_report.html");
    const zipPath = ws.outPath("multiqc_data.zip");

    await assertRegularFileNoSymlink(htmlPath, "multiqc_html");
    await assertRegularFileNoSymlink(zipPath, "multiqc_data_zip");

    const generalStatsPath = ws.outPath("multiqc_data/multiqc_general_stats.txt");
    let generalStatsText: string | null = null;
    try {
      await assertRegularFileNoSymlink(generalStatsPath, "multiqc_general_stats");
      generalStatsText = await fs.readFile(generalStatsPath, "utf8");
    } catch {
      generalStatsText = null;
    }

    const [htmlArtifact, zipArtifact] = await Promise.all([
      ctx.artifacts.importArtifact({
        projectId: prepared.projectId as ProjectId,
        source: { kind: "local_path", path: htmlPath },
        typeHint: "HTML",
        label: "multiqc_report.html",
        createdByRunId: runId,
        maxBytes: null
      }),
      ctx.artifacts.importArtifact({
        projectId: prepared.projectId as ProjectId,
        source: { kind: "local_path", path: zipPath },
        typeHint: "ZIP",
        label: "multiqc_data.zip",
        createdByRunId: runId,
        maxBytes: null
      })
    ]);

    await toolRun.linkOutput(htmlArtifact.artifactId, "multiqc_html");
    await toolRun.linkOutput(zipArtifact.artifactId, "multiqc_data_zip");

    const parsed = generalStatsText ? parseMultiqcGeneralStatsText(generalStatsText) : { samples: null, rawLines: null };

    return {
      summary: "multiqc ok",
      result: {
        multiqc_html_artifact_id: htmlArtifact.artifactId,
        multiqc_data_zip_artifact_id: zipArtifact.artifactId,
        metrics: {
          samples: parsed.samples,
          raw_general_stats_lines: parsed.rawLines
        }
      }
    };
  }
};
