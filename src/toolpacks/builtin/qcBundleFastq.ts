import { ErrorCode, McpError } from "@modelcontextprotocol/sdk/types.js";
import { promises as fs } from "fs";
import type { ArtifactId, ProjectId, RunId } from "../../core/ids.js";
import type { JsonObject } from "../../core/json.js";
import { zQcBundleFastqInput, zQcBundleFastqOutputV1 } from "../../mcp/toolSchemas.js";
import type { PreparedToolRun, ToolContext, ToolDefinition, ToolExecutionResult } from "../types.js";
import { stableJsonStringify, sha256Prefixed } from "../../core/canonicalJson.js";
import { executeDockerPlan, type DockerExecutionPlan } from "../docker/executeDocker.js";
import { ToolRun } from "../../runs/toolRun.js";
import { createRunWorkspace } from "../../execution/workspace.js";
import { envSnapshot } from "../../mcp/envSnapshot.js";
import { deriveRunId } from "../../runs/runIdentity.js";
import type { SlurmExecutionPlan } from "../slurm/executeSlurm.js";
import { fastqcTool } from "./fastqc.js";
import { multiqcTool } from "./multiqc.js";
import { resolveBackend } from "../backendSelection.js";

const FASTQC_DOCKER_IMAGE =
  "quay.io/biocontainers/fastqc@sha256:e194048df39c3145d9b4e0a14f4da20b59d59250465b6f2a9cb698445fd45900";

const MULTIQC_DOCKER_IMAGE =
  "quay.io/biocontainers/multiqc@sha256:ecafca93ba3346775b773bbfd6ff920ecfc259f554777576c15d3139c678311b";

const MULTIQC_APPTAINER_IMAGE =
  "docker://quay.io/biocontainers/multiqc@sha256:ecafca93ba3346775b773bbfd6ff920ecfc259f554777576c15d3139c678311b";

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

type BundlePhaseIntent = "submit_fastqc" | "submit_multiqc" | "finalize";
type BundlePhase = "fastqc_submitted" | "multiqc_submitted" | "complete";

function fastqcBundleScript(inputPaths: string[], threads: number): string {
  const args = inputPaths.map((p) => p.replace(/"/g, "")).join(" ");
  return [
    "set -eu",
    "rm -rf /work/out/tmp_fastqc",
    "mkdir -p /work/out/tmp_fastqc",
    `fastqc --threads ${threads} --extract -o /work/out/tmp_fastqc ${args}`,
    // fail closed: require one extracted summary per input name
    "test -f /work/out/tmp_fastqc/reads_1_fastqc/summary.txt",
    "cp /work/out/tmp_fastqc/reads_1_fastqc/summary.txt /work/out/summary_reads_1.txt",
    inputPaths.includes("/work/in/reads_2.fastq.gz")
      ? "test -f /work/out/tmp_fastqc/reads_2_fastqc/summary.txt && cp /work/out/tmp_fastqc/reads_2_fastqc/summary.txt /work/out/summary_reads_2.txt"
      : "true"
  ].join("; ");
}

function multiqcOverFastqcTmpScript(): string {
  const pyZip = [
    "python - <<'PY'",
    "import pathlib, zipfile",
    "base = pathlib.Path('/work/out/multiqc_data')",
    "out = pathlib.Path('/work/out/multiqc_data.zip')",
    "with zipfile.ZipFile(out, 'w', compression=zipfile.ZIP_DEFLATED) as z:",
    "  for p in base.rglob('*'):",
    "    if p.is_file():",
    "      z.write(p, p.relative_to(base))",
    "PY"
  ].join("\n");

  return ["set -eu", "multiqc /work/out/tmp_fastqc -o /work/out", pyZip].join("; ");
}

function renderBundleMarkdown(input: {
  runId: RunId;
  reads1: { checksumSha256: string; sizeBytes: string };
  reads2: { checksumSha256: string; sizeBytes: string } | null;
  backend: "docker" | "slurm";
  phase: BundlePhase;
  expectedCollectRunIds: RunId[];
  fastqcRunIds: RunId[];
  multiqcRunId: RunId | null;
  fastqc1: { pass: number; warn: number; fail: number } | null;
  fastqc2: { pass: number; warn: number; fail: number } | null;
  fastqcArtifacts: {
    reads_1: { html: ArtifactId | null; zip: ArtifactId | null };
    reads_2: { html: ArtifactId | null; zip: ArtifactId | null } | null;
  };
  multiqcArtifacts: { html: ArtifactId | null; zip: ArtifactId | null } | null;
  dockerImages: { fastqc: string; multiqc: string };
}): string {
  const lines: string[] = [];
  lines.push("# HelixMCP FASTQ QC Bundle");
  lines.push("");
  lines.push(`- provenance_run_id: ${input.runId}`);
  lines.push(`- backend: ${input.backend}`);
  lines.push(`- phase: ${input.phase}`);
  lines.push("");
  lines.push("## Inputs");
  lines.push(`- reads_1: ${input.reads1.checksumSha256} (${input.reads1.sizeBytes} bytes)`);
  if (input.reads2) {
    lines.push(`- reads_2: ${input.reads2.checksumSha256} (${input.reads2.sizeBytes} bytes)`);
  }
  lines.push("");
  lines.push("## Next Action");
  if (input.expectedCollectRunIds.length > 0) {
    lines.push("- collect these run_ids:");
    for (const id of input.expectedCollectRunIds) lines.push(`  - ${id}`);
  } else {
    lines.push("- none");
  }
  lines.push("");
  lines.push("## FastQC Summary");
  lines.push(`- reads_1 run_id: ${input.fastqcRunIds[0] ?? "unknown"}`);
  lines.push(`- reads_1 html: ${input.fastqcArtifacts.reads_1.html ?? "pending"}`);
  lines.push(`- reads_1 zip: ${input.fastqcArtifacts.reads_1.zip ?? "pending"}`);
  if (input.fastqc1) lines.push(`- reads_1 metrics: PASS=${input.fastqc1.pass} WARN=${input.fastqc1.warn} FAIL=${input.fastqc1.fail}`);
  if (input.fastqcArtifacts.reads_2) {
    lines.push(`- reads_2 run_id: ${input.fastqcRunIds[1] ?? "unknown"}`);
    lines.push(`- reads_2 html: ${input.fastqcArtifacts.reads_2.html ?? "pending"}`);
    lines.push(`- reads_2 zip: ${input.fastqcArtifacts.reads_2.zip ?? "pending"}`);
    if (input.fastqc2) lines.push(`- reads_2 metrics: PASS=${input.fastqc2.pass} WARN=${input.fastqc2.warn} FAIL=${input.fastqc2.fail}`);
  }
  lines.push("");
  lines.push("## MultiQC Outputs");
  lines.push(`- multiqc run_id: ${input.multiqcRunId ?? "pending"}`);
  lines.push(`- multiqc_html_artifact_id: ${input.multiqcArtifacts?.html ?? "pending"}`);
  lines.push(`- multiqc_data_zip_artifact_id: ${input.multiqcArtifacts?.zip ?? "pending"}`);
  lines.push("");
  lines.push("## Tool Versions (Docker images)");
  lines.push(`- fastqc: ${input.dockerImages.fastqc}`);
  lines.push(`- multiqc: ${input.dockerImages.multiqc}`);
  lines.push("");
  return lines.join("\n");
}

type Args = typeof zQcBundleFastqInput._output;

type ResolvedRunOutputs = {
  htmlArtifactId: ArtifactId | null;
  zipArtifactId: ArtifactId | null;
};

async function getRunOutputArtifactIdByRole(ctx: ToolContext, runId: RunId, role: string): Promise<ArtifactId | null> {
  const outputs = await ctx.store.listRunOutputs(runId as any);
  const match = outputs.find((o) => o.role === role);
  return match ? (match.artifactId as ArtifactId) : null;
}

async function getFastqcOutputs(ctx: ToolContext, runId: RunId): Promise<ResolvedRunOutputs> {
  const html = await getRunOutputArtifactIdByRole(ctx, runId, "fastqc_html");
  const zip = await getRunOutputArtifactIdByRole(ctx, runId, "fastqc_zip");
  return { htmlArtifactId: html, zipArtifactId: zip };
}

async function getMultiqcOutputs(ctx: ToolContext, runId: RunId): Promise<ResolvedRunOutputs> {
  const html = await getRunOutputArtifactIdByRole(ctx, runId, "multiqc_html");
  const zip = await getRunOutputArtifactIdByRole(ctx, runId, "multiqc_data_zip");
  return { htmlArtifactId: html, zipArtifactId: zip };
}

function normalizeCanonicalParams(input: JsonObject): JsonObject {
  return JSON.parse(stableJsonStringify(input)) as JsonObject;
}

function makeGraph(input: {
  fastqcRunIds: RunId[];
  multiqcRunId: RunId | null;
}): { graphDigest: `sha256:${string}`; nodes: Array<{ run_id: RunId; tool_name: string; contract_version: string }>; edges: Array<{ from_run_id: RunId; to_run_id: RunId; kind: string }> } {
  const nodes: Array<{ run_id: RunId; tool_name: string; contract_version: string }> = [];
  const edges: Array<{ from_run_id: RunId; to_run_id: RunId; kind: string }> = [];

  for (const id of input.fastqcRunIds) nodes.push({ run_id: id, tool_name: "fastqc", contract_version: "v1" });
  if (input.multiqcRunId) nodes.push({ run_id: input.multiqcRunId, tool_name: "multiqc", contract_version: "v1" });
  if (input.multiqcRunId) {
    for (const fq of input.fastqcRunIds) edges.push({ from_run_id: fq, to_run_id: input.multiqcRunId, kind: "input" });
  }

  const graphJson = stableJsonStringify({ nodes, edges });
  const graphDigest = sha256Prefixed(graphJson);
  return { graphDigest, nodes, edges };
}

async function ensureSlurmSubrunSubmitted(input: {
  ctx: ToolContext;
  projectId: ProjectId;
  toolName: string;
  contractVersion: string;
  toolVersion: string;
  canonicalParams: JsonObject;
  runId: RunId;
  paramsHash: `sha256:${string}`;
  prepared: PreparedToolRun<any>;
  runFn: (args: { runId: RunId; toolRun: ToolRun; prepared: PreparedToolRun<any>; ctx: ToolContext }) => Promise<ToolExecutionResult>;
}): Promise<void> {
  const { ctx, projectId, toolName, contractVersion, toolVersion, canonicalParams, runId, paramsHash, prepared, runFn } = input;

  const existing = await ctx.store.getRun(runId);
  if (existing?.resultJson) return;

  const toolRun = new ToolRun(
    { store: ctx.store, artifacts: ctx.artifacts },
    {
      runId,
      projectId,
      toolName,
      contractVersion,
      toolVersion,
      paramsHash,
      canonicalParams,
      policyHash: ctx.policy.policyHash,
      requestedBy: null,
      policySnapshot: ctx.policy.snapshot() as JsonObject,
      environment: envSnapshot()
    }
  );

  await toolRun.start("queued");
  for (const inp of prepared.inputsToLink) {
    await toolRun.linkInput(inp.artifactId, inp.role);
  }

  const res = await runFn({ runId, toolRun, prepared, ctx });
  await toolRun.checkpointQueued(res.result, res.summary);
}

function buildVirtualSlurmPlan(ctx: ToolContext, projectId: ProjectId, reads: Array<{ role: string; artifact_id: ArtifactId; checksum_sha256: string; dest_relpath: string }>): SlurmExecutionPlan {
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

  return {
    version: 1,
    resources: { time_limit_seconds: 60, cpus: 1, mem_mb: 256, gpus: null, gpu_type: null },
    placement: { partition, account, qos: null, constraint: null },
    execution: {
      kind: "container",
      container: { engine: "apptainer", image: MULTIQC_APPTAINER_IMAGE, network_mode: "none", readonly_rootfs: true },
      command: { argv: ["sh", "-c", "true"], workdir: "/work", env: {} }
    },
    io: {
      inputs: reads.map((r) => ({
        role: r.role,
        artifact_id: r.artifact_id,
        checksum_sha256: r.checksum_sha256,
        dest_relpath: r.dest_relpath
      })),
      outputs: [
        { role: "multiqc_html", src_relpath: "out/multiqc_report.html", type: "HTML", label: "multiqc_report.html" },
        { role: "multiqc_data_zip", src_relpath: "out/multiqc_data.zip", type: "ZIP", label: "multiqc_data.zip" },
        { role: "bundle_report", src_relpath: "out/qc_bundle_report.md", type: "MD", label: "qc_bundle_report.md" }
      ]
    }
  };
}

export const qcBundleFastqTool: ToolDefinition<Args, DockerExecutionPlan | SlurmExecutionPlan> = {
  abiVersion: "v1",
  toolName: "qc_bundle_fastq",
  contractVersion: "v1",
  planKind: "hybrid",
  description:
    'Run a deterministic FASTQ QC bundle (FastQC + MultiQC). For backend="slurm", uses an explicit multi-step state machine with submit/collect/finalize. Produces MultiQC outputs and a Markdown bundle receipt.',
  inputSchema: zQcBundleFastqInput,
  outputSchema: zQcBundleFastqOutputV1,
  declaredOutputs: [
    { role: "multiqc_html", type: "HTML", label: "multiqc_report.html", srcRelpath: "out/multiqc_report.html" },
    { role: "multiqc_data_zip", type: "ZIP", label: "multiqc_data.zip", srcRelpath: "out/multiqc_data.zip" },
    { role: "bundle_report", type: "MD", label: "qc_bundle_report.md", srcRelpath: "out/qc_bundle_report.md" }
  ],

  async canonicalize(args: Args, ctx: ToolContext): Promise<PreparedToolRun<DockerExecutionPlan | SlurmExecutionPlan>> {
    const projectId = args.project_id as ProjectId;
    const backend = resolveBackend(args.backend, ctx);

    const threadsFastqc = ctx.policy.enforceThreads("fastqc", args.threads_fastqc);
    const threadsMultiqc = ctx.policy.enforceThreads("multiqc", args.threads_multiqc);
    const runtimeSeconds = ctx.policy.maxRuntimeSeconds();

    const reads1 = await ctx.artifacts.getArtifact(args.reads_1_artifact_id as ArtifactId);
    if (!reads1) {
      throw new McpError(ErrorCode.InvalidParams, `unknown reads_1_artifact_id: ${args.reads_1_artifact_id}`);
    }
    if (reads1.projectId !== projectId) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied cross-project artifact: ${reads1.artifactId}`);
    }
    if (reads1.type !== "FASTQ_GZ") {
      throw new McpError(ErrorCode.InvalidParams, `reads_1_artifact_id must be type FASTQ_GZ (got ${reads1.type})`);
    }

    let reads2: Awaited<ReturnType<ToolContext["artifacts"]["getArtifact"]>> | null = null;
    if (args.reads_2_artifact_id) {
      reads2 = await ctx.artifacts.getArtifact(args.reads_2_artifact_id as ArtifactId);
      if (!reads2) {
        throw new McpError(ErrorCode.InvalidParams, `unknown reads_2_artifact_id: ${args.reads_2_artifact_id}`);
      }
      if (reads2.projectId !== projectId) {
        throw new McpError(ErrorCode.InvalidRequest, `policy denied cross-project artifact: ${reads2.artifactId}`);
      }
      if (reads2.type !== "FASTQ_GZ") {
        throw new McpError(ErrorCode.InvalidParams, `reads_2_artifact_id must be type FASTQ_GZ (got ${reads2.type})`);
      }
    }

    if (backend === "slurm") {
      // Determine phase based only on verifiable DB truth (run status + linked artifacts).
      const fqPrepared1 = await fastqcTool.canonicalize(
        { project_id: projectId, reads_artifact_id: reads1.artifactId, backend: "slurm", threads: threadsFastqc } as any,
        ctx
      );
      fqPrepared1.canonicalParams = normalizeCanonicalParams(fqPrepared1.canonicalParams);
      const fq1 = deriveRunId({
        toolName: "fastqc",
        contractVersion: "v1",
        policyHash: ctx.policy.policyHash,
        canonicalParams: fqPrepared1.canonicalParams
      });

      let fq2: { runId: RunId; paramsHash: `sha256:${string}`; prepared: PreparedToolRun<any> } | null = null;
      let fqPrepared2: PreparedToolRun<any> | null = null;
      if (reads2) {
        fqPrepared2 = await fastqcTool.canonicalize(
          { project_id: projectId, reads_artifact_id: reads2.artifactId, backend: "slurm", threads: threadsFastqc } as any,
          ctx
        );
        fqPrepared2.canonicalParams = normalizeCanonicalParams(fqPrepared2.canonicalParams);
        const fq2id = deriveRunId({
          toolName: "fastqc",
          contractVersion: "v1",
          policyHash: ctx.policy.policyHash,
          canonicalParams: fqPrepared2.canonicalParams
        });
        fq2 = { runId: fq2id.runId, paramsHash: fq2id.paramsHash, prepared: fqPrepared2 };
      }

      const fastqcRunIds: RunId[] = [fq1.runId, ...(fq2 ? [fq2.runId] : [])];

      const fq1Run = await ctx.store.getRun(fq1.runId);
      const fq1Out = await getFastqcOutputs(ctx, fq1.runId);
      const fq1Ready = fq1Run?.status === "succeeded" && !!fq1Out.zipArtifactId;

      let fq2Ready = true;
      let fq2Run: any = null;
      let fq2Out: ResolvedRunOutputs | null = null;
      if (fq2) {
        fq2Run = await ctx.store.getRun(fq2.runId);
        fq2Out = await getFastqcOutputs(ctx, fq2.runId);
        fq2Ready = fq2Run?.status === "succeeded" && !!fq2Out.zipArtifactId;
      }

      let multiqcRunId: RunId | null = null;
      let multiqcParamsHash: `sha256:${string}` | null = null;
      let multiqcPrepared: PreparedToolRun<any> | null = null;
      let multiqcReady = false;
      let multiqcOut: ResolvedRunOutputs | null = null;

      if (fq1Ready && fq2Ready) {
        const zipIds = [fq1Out.zipArtifactId!, ...(fq2Out?.zipArtifactId ? [fq2Out.zipArtifactId] : [])];
        multiqcPrepared = await multiqcTool.canonicalize(
          { project_id: projectId, fastqc_zip_artifact_ids: zipIds, backend: "slurm", threads: threadsMultiqc } as any,
          ctx
        );
        multiqcPrepared.canonicalParams = normalizeCanonicalParams(multiqcPrepared.canonicalParams);
        const mq = deriveRunId({
          toolName: "multiqc",
          contractVersion: "v1",
          policyHash: ctx.policy.policyHash,
          canonicalParams: multiqcPrepared.canonicalParams
        });
        multiqcRunId = mq.runId;
        multiqcParamsHash = mq.paramsHash;

        const mqRun = await ctx.store.getRun(mq.runId);
        multiqcOut = await getMultiqcOutputs(ctx, mq.runId);
        multiqcReady = mqRun?.status === "succeeded" && !!multiqcOut.htmlArtifactId && !!multiqcOut.zipArtifactId;
      }

      const phaseIntent: BundlePhaseIntent = !fq1Ready || !fq2Ready ? "submit_fastqc" : !multiqcReady ? "submit_multiqc" : "finalize";
      const graph = makeGraph({ fastqcRunIds, multiqcRunId });

      const canonicalParams: JsonObject = {
        project_id: projectId,
        backend: "slurm",
        reads: {
          reads_1: { artifact_id: reads1.artifactId, checksum_sha256: reads1.checksumSha256, type: reads1.type, size_bytes: reads1.sizeBytes.toString() },
          reads_2: reads2
            ? { artifact_id: reads2.artifactId, checksum_sha256: reads2.checksumSha256, type: reads2.type, size_bytes: reads2.sizeBytes.toString() }
            : null
        },
        threads: { fastqc: threadsFastqc, multiqc: threadsMultiqc },
        phase_intent: phaseIntent,
        fastqc_run_ids: fastqcRunIds,
        multiqc_run_id: multiqcRunId,
        graph_digest_sha256: graph.graphDigest
      };

      const slurmInputs = [
        { role: "reads_1", artifact_id: reads1.artifactId, checksum_sha256: reads1.checksumSha256, dest_relpath: "in/reads_1.fastq.gz" },
        ...(reads2 ? [{ role: "reads_2", artifact_id: reads2.artifactId, checksum_sha256: reads2.checksumSha256, dest_relpath: "in/reads_2.fastq.gz" }] : [])
      ];
      const slurmPlan = buildVirtualSlurmPlan(ctx, projectId, slurmInputs);

      const selectedPlanKind = phaseIntent === "finalize" ? ("docker" as const) : ("slurm" as const);
      const plan: DockerExecutionPlan | SlurmExecutionPlan =
        selectedPlanKind === "slurm"
          ? slurmPlan
          : {
              image: MULTIQC_DOCKER_IMAGE,
              argv: ["sh", "-c", "true"],
              workdir: "/work",
              inputs: [
                { role: "reads_1", artifact: reads1, destName: "reads_1.fastq.gz" },
                ...(reads2 ? [{ role: "reads_2", artifact: reads2, destName: "reads_2.fastq.gz" } as const] : [])
              ],
              resources: { threads: 1, runtimeSeconds: 1 }
            };

      return {
        projectId,
        canonicalParams,
        toolVersion: "orchestrator_v1",
        plan,
        selectedPlanKind,
        inputsToLink: [
          { artifactId: reads1.artifactId, role: "reads_1" },
          ...(reads2 ? [{ artifactId: reads2.artifactId, role: "reads_2" } as const] : [])
        ]
      };
    }

    const inputPaths = ["/work/in/reads_1.fastq.gz"];
    if (reads2) inputPaths.push("/work/in/reads_2.fastq.gz");

    const plan: DockerExecutionPlan = {
      image: FASTQC_DOCKER_IMAGE,
      argv: ["sh", "-c", fastqcBundleScript(inputPaths, threadsFastqc)],
      workdir: "/work",
      inputs: [
        { role: "reads_1", artifact: reads1, destName: "reads_1.fastq.gz" },
        ...(reads2 ? [{ role: "reads_2", artifact: reads2, destName: "reads_2.fastq.gz" } as const] : [])
      ],
      resources: { threads: threadsFastqc, runtimeSeconds }
    };

    const graph = makeGraph({ fastqcRunIds: [], multiqcRunId: null });
    const canonicalParams: JsonObject = {
      project_id: projectId,
      backend: "docker",
      reads: {
        reads_1: { artifact_id: reads1.artifactId, checksum_sha256: reads1.checksumSha256, type: reads1.type, size_bytes: reads1.sizeBytes.toString() },
        reads_2: reads2
          ? { artifact_id: reads2.artifactId, checksum_sha256: reads2.checksumSha256, type: reads2.type, size_bytes: reads2.sizeBytes.toString() }
          : null
      },
      threads: { fastqc: threadsFastqc, multiqc: threadsMultiqc },
      phase_intent: "finalize",
      graph_digest_sha256: graph.graphDigest,
      docker: {
        fastqc: { image: FASTQC_DOCKER_IMAGE, argv: plan.argv, network_mode: ctx.policy.dockerNetworkMode() },
        multiqc: { image: MULTIQC_DOCKER_IMAGE, argv: ["sh", "-c", multiqcOverFastqcTmpScript()], network_mode: ctx.policy.dockerNetworkMode() }
      }
    };

    return {
      projectId,
      canonicalParams,
      toolVersion: `fastqc=${FASTQC_DOCKER_IMAGE} multiqc=${MULTIQC_DOCKER_IMAGE}`,
      plan,
      selectedPlanKind: "docker",
      inputsToLink: [
        { artifactId: reads1.artifactId, role: "reads_1" },
        ...(reads2 ? [{ artifactId: reads2.artifactId, role: "reads_2" } as const] : [])
      ]
    };
  },

  async run(args: {
    runId: RunId;
    toolRun: ToolRun;
    prepared: PreparedToolRun<DockerExecutionPlan | SlurmExecutionPlan>;
    ctx: ToolContext;
  }): Promise<ToolExecutionResult> {
    const { runId, toolRun, prepared, ctx } = args;

    const canonical = prepared.canonicalParams as any;
    const backend = canonical.backend as "docker" | "slurm";
    const phaseIntent = canonical.phase_intent as BundlePhaseIntent;

    if (backend === "docker") {
      await toolRun.event("exec.plan", `backend=docker images=[fastqc,multiqc]`, null);

      const fastqcOutcome = await executeDockerPlan({
        policy: ctx.policy,
        runsDir: ctx.runsDir,
        runId,
        toolName: "fastqc",
        plan: prepared.plan as DockerExecutionPlan,
        materializeToPath: async (artifactId: string, destPath: string) => ctx.artifacts.materializeToPath(artifactId as any, destPath)
      });
      await toolRun.event("exec.step", `fastqc exit=${fastqcOutcome.exitCode}`, {
        started_at: fastqcOutcome.startedAt,
        finished_at: fastqcOutcome.finishedAt,
        stderr: fastqcOutcome.stderr
      });
      if (fastqcOutcome.exitCode !== 0) {
        throw new Error(`qc_bundle_fastq fastqc failed (exit ${fastqcOutcome.exitCode})`);
      }

      const multiqcPlan: DockerExecutionPlan = {
        image: MULTIQC_DOCKER_IMAGE,
        argv: ["sh", "-c", multiqcOverFastqcTmpScript()],
        workdir: "/work",
        inputs: [],
        resources: { threads: canonical.threads?.multiqc ?? 1, runtimeSeconds: ctx.policy.maxRuntimeSeconds() }
      };

      const multiqcOutcome = await executeDockerPlan({
        policy: ctx.policy,
        runsDir: ctx.runsDir,
        runId,
        toolName: "multiqc",
        plan: multiqcPlan,
        materializeToPath: async () => {
          throw new Error("unexpected materializeToPath for multiqc step (no inputs)");
        }
      });
      await toolRun.event("exec.step", `multiqc exit=${multiqcOutcome.exitCode}`, {
        started_at: multiqcOutcome.startedAt,
        finished_at: multiqcOutcome.finishedAt,
        stderr: multiqcOutcome.stderr
      });
      if (multiqcOutcome.exitCode !== 0) {
        throw new Error(`qc_bundle_fastq multiqc failed (exit ${multiqcOutcome.exitCode})`);
      }

      const ws = await createRunWorkspace(ctx.runsDir, runId);
      const htmlPath = ws.outPath("multiqc_report.html");
      const zipPath = ws.outPath("multiqc_data.zip");
      const summary1Path = ws.outPath("summary_reads_1.txt");
      const summary2Path = ws.outPath("summary_reads_2.txt");

      await assertRegularFileNoSymlink(htmlPath, "multiqc_html");
      await assertRegularFileNoSymlink(zipPath, "multiqc_data_zip");
      await assertRegularFileNoSymlink(summary1Path, "fastqc summary reads_1");
      const hasReads2 = await fs
        .stat(summary2Path)
        .then((st) => st.isFile())
        .catch(() => false);
      if (hasReads2) await assertRegularFileNoSymlink(summary2Path, "fastqc summary reads_2");

      const [multiqcHtml, multiqcZip, summary1Text, summary2Text] = await Promise.all([
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
        }),
        fs.readFile(summary1Path, "utf8"),
        hasReads2 ? fs.readFile(summary2Path, "utf8") : Promise.resolve(null)
      ]);

      await toolRun.linkOutput(multiqcHtml.artifactId, "multiqc_html");
      await toolRun.linkOutput(multiqcZip.artifactId, "multiqc_data_zip");

      const fastqc1 = parseFastqcSummaryText(summary1Text);
      const fastqc2 = summary2Text ? parseFastqcSummaryText(summary2Text) : null;

      const reads1 = await ctx.artifacts.getArtifact((prepared.inputsToLink[0] as any).artifactId as ArtifactId);
      const reads2 = hasReads2 && prepared.inputsToLink.length > 1 ? await ctx.artifacts.getArtifact((prepared.inputsToLink[1] as any).artifactId as ArtifactId) : null;
      if (!reads1) throw new Error("qc_bundle_fastq missing reads_1 artifact at runtime");

      const report = renderBundleMarkdown({
        runId,
        backend: "docker",
        phase: "complete",
        expectedCollectRunIds: [],
        fastqcRunIds: [],
        multiqcRunId: null,
        reads1: { checksumSha256: reads1.checksumSha256, sizeBytes: reads1.sizeBytes.toString() },
        reads2: reads2 ? { checksumSha256: reads2.checksumSha256, sizeBytes: reads2.sizeBytes.toString() } : null,
        fastqc1: { pass: fastqc1.pass, warn: fastqc1.warn, fail: fastqc1.fail },
        fastqc2: fastqc2 ? { pass: fastqc2.pass, warn: fastqc2.warn, fail: fastqc2.fail } : null,
        fastqcArtifacts: {
          reads_1: { html: null, zip: null },
          reads_2: reads2 ? { html: null, zip: null } : null
        },
        multiqcArtifacts: { html: multiqcHtml.artifactId, zip: multiqcZip.artifactId },
        dockerImages: { fastqc: FASTQC_DOCKER_IMAGE, multiqc: MULTIQC_DOCKER_IMAGE }
      });

      const reportArtifactId = await toolRun.createOutputArtifact({
        type: "MD",
        label: "qc_bundle_report.md",
        contentText: report,
        role: "bundle_report"
      });

      return {
        summary: "qc bundle ok",
        result: {
          phase: "complete",
          expected_collect_run_ids: [],
          fastqc_run_ids: [],
          multiqc_run_id: null,
          bundle_report_artifact_id: reportArtifactId,
          fastqc: {
            reads_1: { run_id: runId, fastqc_html_artifact_id: null, fastqc_zip_artifact_id: null, metrics: { pass: fastqc1.pass, warn: fastqc1.warn, fail: fastqc1.fail } },
            reads_2: reads2 ? { run_id: runId, fastqc_html_artifact_id: null, fastqc_zip_artifact_id: null, metrics: fastqc2 ? { pass: fastqc2.pass, warn: fastqc2.warn, fail: fastqc2.fail } : null } : null
          },
          multiqc: { run_id: runId, multiqc_html_artifact_id: multiqcHtml.artifactId, multiqc_data_zip_artifact_id: multiqcZip.artifactId },
          graph: { graph_digest_sha256: canonical.graph_digest_sha256, nodes: [], edges: [] }
        }
      };
    }

    // backend=slurm multi-step state machine based on DB truth.
    const readsBlock = canonical.reads as any;
    const reads1Id = readsBlock.reads_1.artifact_id as ArtifactId;
    const reads2Id = readsBlock.reads_2?.artifact_id ? (readsBlock.reads_2.artifact_id as ArtifactId) : null;
    const threadsFastqc = canonical.threads.fastqc as number;
    const threadsMultiqc = canonical.threads.multiqc as number;

    const reads1 = await ctx.artifacts.getArtifact(reads1Id);
    if (!reads1) throw new Error("qc_bundle_fastq missing reads_1 artifact");
    const reads2 = reads2Id ? await ctx.artifacts.getArtifact(reads2Id) : null;

    const fqPrepared1 = await fastqcTool.canonicalize(
      { project_id: prepared.projectId, reads_artifact_id: reads1Id, backend: "slurm", threads: threadsFastqc } as any,
      ctx
    );
    fqPrepared1.canonicalParams = normalizeCanonicalParams(fqPrepared1.canonicalParams);
    const fq1 = deriveRunId({
      toolName: "fastqc",
      contractVersion: "v1",
      policyHash: ctx.policy.policyHash,
      canonicalParams: fqPrepared1.canonicalParams
    });
    const fastqcRunIds: RunId[] = [fq1.runId];

    let fqPrepared2: PreparedToolRun<any> | null = null;
    let fq2: { runId: RunId; paramsHash: `sha256:${string}` } | null = null;
    if (reads2Id) {
      fqPrepared2 = await fastqcTool.canonicalize(
        { project_id: prepared.projectId, reads_artifact_id: reads2Id, backend: "slurm", threads: threadsFastqc } as any,
        ctx
      );
      fqPrepared2.canonicalParams = normalizeCanonicalParams(fqPrepared2.canonicalParams);
      const fq2id = deriveRunId({
        toolName: "fastqc",
        contractVersion: "v1",
        policyHash: ctx.policy.policyHash,
        canonicalParams: fqPrepared2.canonicalParams
      });
      fq2 = { runId: fq2id.runId, paramsHash: fq2id.paramsHash };
      fastqcRunIds.push(fq2.runId);
    }

    const fq1Run = await ctx.store.getRun(fq1.runId);
    const fq1Out = await getFastqcOutputs(ctx, fq1.runId);
    const fq1Ready = fq1Run?.status === "succeeded" && !!fq1Out.zipArtifactId;

    let fq2Ready = true;
    let fq2Run: any = null;
    let fq2Out: ResolvedRunOutputs | null = null;
    if (fq2 && fqPrepared2) {
      fq2Run = await ctx.store.getRun(fq2.runId);
      fq2Out = await getFastqcOutputs(ctx, fq2.runId);
      fq2Ready = fq2Run?.status === "succeeded" && !!fq2Out.zipArtifactId;
    }

    if (phaseIntent === "submit_fastqc") {
      if (!ctx.slurmSubmitter) throw new McpError(ErrorCode.InvalidRequest, `slurm submitter not configured`);

      if (!fq1Run || fq1Run.status === "queued" || fq1Run.status === "running") {
        // if missing, submit; if already queued/running, no-op
        if (!fq1Run) {
          await ensureSlurmSubrunSubmitted({
            ctx,
            projectId: prepared.projectId as ProjectId,
            toolName: "fastqc",
            contractVersion: "v1",
            toolVersion: fqPrepared1.toolVersion,
            canonicalParams: fqPrepared1.canonicalParams,
            runId: fq1.runId,
            paramsHash: fq1.paramsHash,
            prepared: fqPrepared1,
            runFn: fastqcTool.run as any
          });
        }
      }
      if (fq2 && fqPrepared2) {
        if (!fq2Run) {
          await ensureSlurmSubrunSubmitted({
            ctx,
            projectId: prepared.projectId as ProjectId,
            toolName: "fastqc",
            contractVersion: "v1",
            toolVersion: fqPrepared2.toolVersion,
            canonicalParams: fqPrepared2.canonicalParams,
            runId: fq2.runId,
            paramsHash: fq2.paramsHash,
            prepared: fqPrepared2,
            runFn: fastqcTool.run as any
          });
        }
      }

      const expected: RunId[] = [];
      const fq1After = await ctx.store.getRun(fq1.runId);
      if (!fq1After || fq1After.status === "queued" || fq1After.status === "running") expected.push(fq1.runId);
      if (fq2) {
        const fq2After = await ctx.store.getRun(fq2.runId);
        if (!fq2After || fq2After.status === "queued" || fq2After.status === "running") expected.push(fq2.runId);
      }

      const graph = makeGraph({ fastqcRunIds, multiqcRunId: null });
      const report = renderBundleMarkdown({
        runId,
        backend: "slurm",
        phase: "fastqc_submitted",
        expectedCollectRunIds: expected,
        fastqcRunIds,
        multiqcRunId: null,
        reads1: { checksumSha256: reads1.checksumSha256, sizeBytes: reads1.sizeBytes.toString() },
        reads2: reads2 ? { checksumSha256: reads2.checksumSha256, sizeBytes: reads2.sizeBytes.toString() } : null,
        fastqc1: null,
        fastqc2: null,
        fastqcArtifacts: {
          reads_1: { html: fq1Out.htmlArtifactId, zip: fq1Out.zipArtifactId },
          reads_2: fq2Out ? { html: fq2Out.htmlArtifactId, zip: fq2Out.zipArtifactId } : null
        },
        multiqcArtifacts: null,
        dockerImages: { fastqc: FASTQC_DOCKER_IMAGE, multiqc: MULTIQC_DOCKER_IMAGE }
      });

      const reportArtifactId = await toolRun.createOutputArtifact({
        type: "MD",
        label: "qc_bundle_report.md",
        contentText: report,
        role: "bundle_report"
      });

      const result: JsonObject = {
        phase: "fastqc_submitted",
        expected_collect_run_ids: expected,
        bundle_report_artifact_id: reportArtifactId,
        fastqc_run_ids: fastqcRunIds,
        multiqc_run_id: null,
        fastqc: {
          reads_1: { run_id: fq1.runId, fastqc_html_artifact_id: fq1Out.htmlArtifactId, fastqc_zip_artifact_id: fq1Out.zipArtifactId, metrics: null },
          reads_2: fq2 ? { run_id: fq2.runId, fastqc_html_artifact_id: fq2Out?.htmlArtifactId ?? null, fastqc_zip_artifact_id: fq2Out?.zipArtifactId ?? null, metrics: null } : null
        },
        multiqc: null,
        graph: { graph_digest_sha256: graph.graphDigest, nodes: graph.nodes, edges: graph.edges }
      };

      return { summary: "fastqc submitted", result };
    }

    if (phaseIntent === "submit_multiqc") {
      if (!fq1Ready || !fq2Ready) {
        throw new Error(`qc_bundle_fastq cannot submit multiqc before fastqc outputs are available`);
      }
      const zipIds = [fq1Out.zipArtifactId!, ...(fq2Out?.zipArtifactId ? [fq2Out.zipArtifactId] : [])];

      const mqPrepared = await multiqcTool.canonicalize(
        { project_id: prepared.projectId, fastqc_zip_artifact_ids: zipIds, backend: "slurm", threads: threadsMultiqc } as any,
        ctx
      );
      mqPrepared.canonicalParams = normalizeCanonicalParams(mqPrepared.canonicalParams);
      const mq = deriveRunId({
        toolName: "multiqc",
        contractVersion: "v1",
        policyHash: ctx.policy.policyHash,
        canonicalParams: mqPrepared.canonicalParams
      });

      const mqRun = await ctx.store.getRun(mq.runId);
      if (!mqRun) {
        await ensureSlurmSubrunSubmitted({
          ctx,
          projectId: prepared.projectId as ProjectId,
          toolName: "multiqc",
          contractVersion: "v1",
          toolVersion: mqPrepared.toolVersion,
          canonicalParams: mqPrepared.canonicalParams,
          runId: mq.runId,
          paramsHash: mq.paramsHash,
          prepared: mqPrepared,
          runFn: multiqcTool.run as any
        });
      }

      const expected: RunId[] = [];
      const mqAfter = await ctx.store.getRun(mq.runId);
      if (!mqAfter || mqAfter.status === "queued" || mqAfter.status === "running") expected.push(mq.runId);

      const graph = makeGraph({ fastqcRunIds, multiqcRunId: mq.runId });
      const report = renderBundleMarkdown({
        runId,
        backend: "slurm",
        phase: "multiqc_submitted",
        expectedCollectRunIds: expected,
        fastqcRunIds,
        multiqcRunId: mq.runId,
        reads1: { checksumSha256: reads1.checksumSha256, sizeBytes: reads1.sizeBytes.toString() },
        reads2: reads2 ? { checksumSha256: reads2.checksumSha256, sizeBytes: reads2.sizeBytes.toString() } : null,
        fastqc1: null,
        fastqc2: null,
        fastqcArtifacts: {
          reads_1: { html: fq1Out.htmlArtifactId, zip: fq1Out.zipArtifactId },
          reads_2: fq2Out ? { html: fq2Out.htmlArtifactId, zip: fq2Out.zipArtifactId } : null
        },
        multiqcArtifacts: null,
        dockerImages: { fastqc: FASTQC_DOCKER_IMAGE, multiqc: MULTIQC_DOCKER_IMAGE }
      });

      const reportArtifactId = await toolRun.createOutputArtifact({
        type: "MD",
        label: "qc_bundle_report.md",
        contentText: report,
        role: "bundle_report"
      });

      const result: JsonObject = {
        phase: "multiqc_submitted",
        expected_collect_run_ids: expected,
        bundle_report_artifact_id: reportArtifactId,
        fastqc_run_ids: fastqcRunIds,
        multiqc_run_id: mq.runId,
        fastqc: {
          reads_1: { run_id: fq1.runId, fastqc_html_artifact_id: fq1Out.htmlArtifactId, fastqc_zip_artifact_id: fq1Out.zipArtifactId, metrics: null },
          reads_2: fq2 ? { run_id: fq2.runId, fastqc_html_artifact_id: fq2Out?.htmlArtifactId ?? null, fastqc_zip_artifact_id: fq2Out?.zipArtifactId ?? null, metrics: null } : null
        },
        multiqc: { run_id: mq.runId, multiqc_html_artifact_id: null, multiqc_data_zip_artifact_id: null },
        graph: { graph_digest_sha256: graph.graphDigest, nodes: graph.nodes, edges: graph.edges }
      };

      return { summary: "multiqc submitted", result };
    }

    // phaseIntent=finalize
    if (!fq1Ready || !fq2Ready) {
      throw new Error(`qc_bundle_fastq cannot finalize before fastqc outputs are available`);
    }
    const zipIds = [fq1Out.zipArtifactId!, ...(fq2Out?.zipArtifactId ? [fq2Out.zipArtifactId] : [])];
    const mqPrepared = await multiqcTool.canonicalize(
      { project_id: prepared.projectId, fastqc_zip_artifact_ids: zipIds, backend: "slurm", threads: threadsMultiqc } as any,
      ctx
    );
    mqPrepared.canonicalParams = normalizeCanonicalParams(mqPrepared.canonicalParams);
    const mq = deriveRunId({
      toolName: "multiqc",
      contractVersion: "v1",
      policyHash: ctx.policy.policyHash,
      canonicalParams: mqPrepared.canonicalParams
    });
    const mqRun = await ctx.store.getRun(mq.runId);
    if (mqRun?.status !== "succeeded") {
      throw new Error(`qc_bundle_fastq cannot finalize before multiqc run is succeeded: ${mq.runId}`);
    }
    const mqOut = await getMultiqcOutputs(ctx, mq.runId);
    if (!mqOut.htmlArtifactId || !mqOut.zipArtifactId) {
      throw new Error(`qc_bundle_fastq cannot finalize before multiqc outputs are linked: ${mq.runId}`);
    }

    await toolRun.linkOutput(mqOut.htmlArtifactId, "multiqc_html");
    await toolRun.linkOutput(mqOut.zipArtifactId, "multiqc_data_zip");

    const graph = makeGraph({ fastqcRunIds, multiqcRunId: mq.runId });
    const report = renderBundleMarkdown({
      runId,
      backend: "slurm",
      phase: "complete",
      expectedCollectRunIds: [],
      fastqcRunIds,
      multiqcRunId: mq.runId,
      reads1: { checksumSha256: reads1.checksumSha256, sizeBytes: reads1.sizeBytes.toString() },
      reads2: reads2 ? { checksumSha256: reads2.checksumSha256, sizeBytes: reads2.sizeBytes.toString() } : null,
      fastqc1: null,
      fastqc2: null,
      fastqcArtifacts: {
        reads_1: { html: fq1Out.htmlArtifactId, zip: fq1Out.zipArtifactId },
        reads_2: fq2Out ? { html: fq2Out.htmlArtifactId, zip: fq2Out.zipArtifactId } : null
      },
      multiqcArtifacts: { html: mqOut.htmlArtifactId, zip: mqOut.zipArtifactId },
      dockerImages: { fastqc: FASTQC_DOCKER_IMAGE, multiqc: MULTIQC_DOCKER_IMAGE }
    });

    const reportArtifactId = await toolRun.createOutputArtifact({
      type: "MD",
      label: "qc_bundle_report.md",
      contentText: report,
      role: "bundle_report"
    });

    return {
      summary: "qc bundle finalized",
      result: {
        phase: "complete",
        expected_collect_run_ids: [],
        bundle_report_artifact_id: reportArtifactId,
        fastqc_run_ids: fastqcRunIds,
        multiqc_run_id: mq.runId,
        fastqc: {
          reads_1: { run_id: fq1.runId, fastqc_html_artifact_id: fq1Out.htmlArtifactId, fastqc_zip_artifact_id: fq1Out.zipArtifactId, metrics: null },
          reads_2: fq2 ? { run_id: fq2.runId, fastqc_html_artifact_id: fq2Out?.htmlArtifactId ?? null, fastqc_zip_artifact_id: fq2Out?.zipArtifactId ?? null, metrics: null } : null
        },
        multiqc: { run_id: mq.runId, multiqc_html_artifact_id: mqOut.htmlArtifactId, multiqc_data_zip_artifact_id: mqOut.zipArtifactId },
        graph: { graph_digest_sha256: graph.graphDigest, nodes: graph.nodes, edges: graph.edges }
      }
    };
  }
};
