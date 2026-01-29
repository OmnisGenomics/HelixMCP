import { ErrorCode, McpError } from "@modelcontextprotocol/sdk/types.js";
import { promises as fs } from "fs";
import type { ArtifactRecord } from "../../core/artifact.js";
import type { ArtifactId, ProjectId, RunId } from "../../core/ids.js";
import type { JsonObject } from "../../core/json.js";
import { zSeqkitStatsInput, zSeqkitStatsOutput } from "../../mcp/toolSchemas.js";
import type { ToolDefinition, ToolExecutionResult, ToolContext, PreparedToolRun } from "../types.js";
import { executeDockerPlan, type DockerExecutionPlan } from "../docker/executeDocker.js";
import type { ToolRun } from "../../runs/toolRun.js";
import { createRunWorkspace } from "../../execution/workspace.js";

const SEQKIT_IMAGE =
  "quay.io/biocontainers/seqkit@sha256:67c9a1cfeafbccfd43bbd1fbb80646c9faa06a50b22c8ea758c3c84268b6765d";

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

type Args = typeof zSeqkitStatsInput._output;

export const seqkitStatsTool: ToolDefinition<Args, DockerExecutionPlan> = {
  toolName: "seqkit_stats",
  contractVersion: "v1",
  planKind: "docker",
  description: "Run seqkit stats in Docker on a FASTA/FASTQ artifact (deterministic, policy-gated).",
  inputSchema: zSeqkitStatsInput,
  outputSchema: zSeqkitStatsOutput,
  declaredOutputs: [{ role: "report", type: "TSV", label: "seqkit_stats.tsv" }],

  async canonicalize(args: Args, ctx: ToolContext): Promise<PreparedToolRun<DockerExecutionPlan>> {
    const projectId = args.project_id as ProjectId;
    const threads = ctx.policy.enforceThreads("seqkit_stats", args.threads);
    const runtimeSeconds = ctx.policy.maxRuntimeSeconds();

    const input = await ctx.artifacts.getArtifact(args.input_artifact_id as ArtifactId);
    if (!input) {
      throw new McpError(ErrorCode.InvalidParams, `unknown input_artifact_id: ${args.input_artifact_id}`);
    }
    if (input.projectId !== projectId) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied cross-project artifact: ${input.artifactId}`);
    }

    const canonicalParams: JsonObject = {
      project_id: projectId,
      input: {
        checksum_sha256: input.checksumSha256,
        type: input.type,
        size_bytes: input.sizeBytes.toString()
      },
      threads,
      docker: {
        image: SEQKIT_IMAGE,
        network_mode: ctx.policy.dockerNetworkMode(),
        argv: ["seqkit", "stats", "-T", "/work/in/input"]
      }
    };

    const docker: DockerExecutionPlan = {
      image: SEQKIT_IMAGE,
      argv: ["seqkit", "stats", "-T", "/work/in/input"],
      workdir: "/work",
      inputs: [{ role: "input", artifact: input, destName: "input" }],
      resources: { threads, runtimeSeconds }
    };

    return {
      projectId,
      canonicalParams,
      toolVersion: SEQKIT_IMAGE,
      plan: docker,
      inputsToLink: [{ artifactId: input.artifactId, role: "input" }]
    };
  },

  async run(args: {
    runId: RunId;
    toolRun: ToolRun;
    prepared: PreparedToolRun<DockerExecutionPlan>;
    ctx: ToolContext;
  }): Promise<ToolExecutionResult> {
    const { runId, toolRun, prepared, ctx } = args;

    await toolRun.event("exec.plan", `backend=docker image=${SEQKIT_IMAGE}`, null);

    const result = await executeDockerPlan({
      policy: ctx.policy,
      runsDir: ctx.runsDir,
      runId,
      toolName: "seqkit_stats",
      plan: prepared.plan,
      materializeToPath: async (artifactId: string, destPath: string) => ctx.artifacts.materializeToPath(artifactId as any, destPath)
    });

    await toolRun.event("exec.result", `exit=${result.exitCode}`, {
      backend: "docker",
      started_at: result.startedAt,
      finished_at: result.finishedAt,
      stderr: result.stderr
    });

    if (result.exitCode !== 0) {
      throw new Error(`seqkit stats failed (exit ${result.exitCode})`);
    }

    const ws = await createRunWorkspace(ctx.runsDir, runId);
    const wsReportPath = ws.outPath("seqkit_stats.tsv");
    await fs.writeFile(wsReportPath, result.stdout, "utf8");

    const { metrics, raw } = parseSeqkitStatsTsv(result.stdout);

    const reportArtifactId = await toolRun.createOutputArtifact({
      type: "TSV",
      label: "seqkit_stats.tsv",
      contentText: raw,
      role: "report"
    });

    return {
      summary: "seqkit stats ok",
      result: {
        report_artifact_id: reportArtifactId,
        stats: metrics
      }
    };
  }
};
