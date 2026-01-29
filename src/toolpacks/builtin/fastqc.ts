import { ErrorCode, McpError } from "@modelcontextprotocol/sdk/types.js";
import { promises as fs } from "fs";
import type { ArtifactId, ProjectId, RunId } from "../../core/ids.js";
import type { JsonObject } from "../../core/json.js";
import { zFastqcInput, zFastqcOutputV1 } from "../../mcp/toolSchemas.js";
import type { PreparedToolRun, ToolContext, ToolDefinition, ToolExecutionResult } from "../types.js";
import { resolveBackend } from "../backendSelection.js";
import { executeDockerPlan, type DockerExecutionPlan } from "../docker/executeDocker.js";
import type { ToolRun } from "../../runs/toolRun.js";
import { createRunWorkspace } from "../../execution/workspace.js";
import { executeSlurmPlan, type SlurmExecutionPlan } from "../slurm/executeSlurm.js";

const FASTQC_DOCKER_IMAGE =
  "quay.io/biocontainers/fastqc@sha256:e194048df39c3145d9b4e0a14f4da20b59d59250465b6f2a9cb698445fd45900";

const FASTQC_APPTAINER_IMAGE =
  "docker://quay.io/biocontainers/fastqc@sha256:e194048df39c3145d9b4e0a14f4da20b59d59250465b6f2a9cb698445fd45900";

type FastqcModuleStatus = "PASS" | "WARN" | "FAIL";

function parseFastqcSummaryText(text: string): { pass: number; warn: number; fail: number; modules: Record<string, FastqcModuleStatus>; rawLines: string[] } {
  const rawLines = text
    .split(/\r?\n/)
    .map((l) => l.trimEnd())
    .filter((l) => l.length > 0);

  const modules: Record<string, FastqcModuleStatus> = {};
  let pass = 0;
  let warn = 0;
  let fail = 0;

  for (const line of rawLines) {
    const cols = line.split("\t");
    const status = (cols[0] ?? "").trim();
    const moduleName = (cols[1] ?? "").trim();
    if (!moduleName) continue;
    if (status === "PASS" || status === "WARN" || status === "FAIL") {
      modules[moduleName] = status;
      if (status === "PASS") pass += 1;
      else if (status === "WARN") warn += 1;
      else fail += 1;
    }
  }

  return { pass, warn, fail, modules, rawLines };
}

async function assertRegularFileNoSymlink(filePath: string, context: string): Promise<void> {
  const st = await fs.lstat(filePath);
  if (st.isSymbolicLink()) throw new Error(`policy denied symlinked file for ${context}`);
  if (!st.isFile()) throw new Error(`expected file for ${context}: ${filePath}`);
}

function fastqcScript(threads: number): string {
  return [
    "set -eu",
    "rm -rf /work/out/tmp_fastqc",
    "mkdir -p /work/out/tmp_fastqc",
    `fastqc --threads ${threads} --extract -o /work/out/tmp_fastqc /work/in/reads.fastq.gz`,
    "mv /work/out/tmp_fastqc/*_fastqc.html /work/out/fastqc.html",
    "mv /work/out/tmp_fastqc/*_fastqc.zip /work/out/fastqc.zip",
    "cp /work/out/tmp_fastqc/*_fastqc/summary.txt /work/out/summary.txt"
  ].join("; ");
}

type Args = typeof zFastqcInput._output;

export const fastqcTool: ToolDefinition<Args, DockerExecutionPlan | SlurmExecutionPlan> = {
  abiVersion: "v1",
  toolName: "fastqc",
  contractVersion: "v1",
  planKind: "hybrid",
  description: 'Run FastQC on a FASTQ_GZ artifact (Docker or queued Slurm). For paired-end data, run twice and aggregate with "multiqc".',
  inputSchema: zFastqcInput,
  outputSchema: zFastqcOutputV1,
  declaredOutputs: [
    { role: "fastqc_html", type: "HTML", label: "fastqc.html", srcRelpath: "out/fastqc.html" },
    { role: "fastqc_zip", type: "ZIP", label: "fastqc.zip", srcRelpath: "out/fastqc.zip" }
  ],

  async canonicalize(args: Args, ctx: ToolContext): Promise<PreparedToolRun<DockerExecutionPlan | SlurmExecutionPlan>> {
    const projectId = args.project_id as ProjectId;
    const backend = resolveBackend(args.backend, ctx);
    const threads = ctx.policy.enforceThreads("fastqc", args.threads);
    const runtimeSeconds = ctx.policy.maxRuntimeSeconds();

    const reads = await ctx.artifacts.getArtifact(args.reads_artifact_id as ArtifactId);
    if (!reads) {
      throw new McpError(ErrorCode.InvalidParams, `unknown reads_artifact_id: ${args.reads_artifact_id}`);
    }
    if (reads.projectId !== projectId) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied cross-project artifact: ${reads.artifactId}`);
    }
    if (reads.type !== "FASTQ_GZ") {
      throw new McpError(ErrorCode.InvalidParams, `reads_artifact_id must be type FASTQ_GZ (got ${reads.type})`);
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
        resources: { time_limit_seconds: 3600, cpus: threads, mem_mb: 4096, gpus: null, gpu_type: null },
        placement: { partition, account, qos: null, constraint: null },
        execution: {
          kind: "container",
          container: { engine: "apptainer", image: FASTQC_APPTAINER_IMAGE, network_mode: "none", readonly_rootfs: true },
          command: { argv: ["sh", "-c", fastqcScript(threads)], workdir: "/work", env: {} }
        },
        io: {
          inputs: [
            {
              role: "reads",
              artifact_id: reads.artifactId,
              checksum_sha256: reads.checksumSha256,
              dest_relpath: "in/reads.fastq.gz"
            }
          ],
          outputs: [
            { role: "fastqc_html", src_relpath: "out/fastqc.html", type: "HTML", label: "fastqc.html" },
            { role: "fastqc_zip", src_relpath: "out/fastqc.zip", type: "ZIP", label: "fastqc.zip" }
          ]
        }
      };

      const canonicalParams: JsonObject = {
        project_id: projectId,
        backend: "slurm",
        reads: {
          checksum_sha256: reads.checksumSha256,
          type: reads.type,
          size_bytes: reads.sizeBytes.toString()
        },
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
        inputsToLink: [{ artifactId: reads.artifactId, role: "reads" }]
      };
    }

    const plan: DockerExecutionPlan = {
      image: FASTQC_DOCKER_IMAGE,
      argv: ["sh", "-c", fastqcScript(threads)],
      workdir: "/work",
      inputs: [{ role: "reads", artifact: reads, destName: "reads.fastq.gz" }],
      resources: { threads, runtimeSeconds }
    };

    const canonicalParams: JsonObject = {
      project_id: projectId,
      backend: "docker",
      reads: {
        checksum_sha256: reads.checksumSha256,
        type: reads.type,
        size_bytes: reads.sizeBytes.toString()
      },
      threads,
      docker: {
        image: FASTQC_DOCKER_IMAGE,
        network_mode: ctx.policy.dockerNetworkMode(),
        argv: plan.argv
      }
    };

    return {
      projectId,
      canonicalParams,
      toolVersion: FASTQC_DOCKER_IMAGE,
      plan,
      selectedPlanKind: "docker",
      inputsToLink: [{ artifactId: reads.artifactId, role: "reads" }]
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

    await toolRun.event("exec.plan", `backend=docker image=${FASTQC_DOCKER_IMAGE}`, null);

    const result = await executeDockerPlan({
      policy: ctx.policy,
      runsDir: ctx.runsDir,
      runId,
      toolName: "fastqc",
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
      throw new Error(`fastqc failed (exit ${result.exitCode})`);
    }

    const ws = await createRunWorkspace(ctx.runsDir, runId);
    const htmlPath = ws.outPath("fastqc.html");
    const zipPath = ws.outPath("fastqc.zip");
    const summaryPath = ws.outPath("summary.txt");

    await assertRegularFileNoSymlink(htmlPath, "fastqc_html");
    await assertRegularFileNoSymlink(zipPath, "fastqc_zip");
    await assertRegularFileNoSymlink(summaryPath, "fastqc summary");

    const [htmlArtifact, zipArtifact, summaryText] = await Promise.all([
      ctx.artifacts.importArtifact({
        projectId: prepared.projectId as ProjectId,
        source: { kind: "local_path", path: htmlPath },
        typeHint: "HTML",
        label: "fastqc.html",
        createdByRunId: runId,
        maxBytes: null
      }),
      ctx.artifacts.importArtifact({
        projectId: prepared.projectId as ProjectId,
        source: { kind: "local_path", path: zipPath },
        typeHint: "ZIP",
        label: "fastqc.zip",
        createdByRunId: runId,
        maxBytes: null
      }),
      fs.readFile(summaryPath, "utf8")
    ]);

    await toolRun.linkOutput(htmlArtifact.artifactId, "fastqc_html");
    await toolRun.linkOutput(zipArtifact.artifactId, "fastqc_zip");

    const metricsParsed = parseFastqcSummaryText(summaryText);

    return {
      summary: "fastqc ok",
      result: {
        fastqc_html_artifact_id: htmlArtifact.artifactId,
        fastqc_zip_artifact_id: zipArtifact.artifactId,
        metrics: {
          pass: metricsParsed.pass,
          warn: metricsParsed.warn,
          fail: metricsParsed.fail,
          modules: metricsParsed.modules,
          raw_lines: metricsParsed.rawLines
        }
      }
    };
  }
};

