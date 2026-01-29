import { ErrorCode, McpError } from "@modelcontextprotocol/sdk/types.js";
import { promises as fs } from "fs";
import type { ArtifactRecord } from "../../core/artifact.js";
import type { ArtifactId, ProjectId, RunId } from "../../core/ids.js";
import type { JsonObject } from "../../core/json.js";
import { zSamtoolsFlagstatInput, zSamtoolsFlagstatOutput } from "../../mcp/toolSchemas.js";
import type { PreparedToolRun, ToolContext, ToolDefinition, ToolExecutionResult } from "../types.js";
import { executeDockerPlan, type DockerExecutionPlan } from "../docker/executeDocker.js";
import type { ToolRun } from "../../runs/toolRun.js";
import { createRunWorkspace } from "../../execution/workspace.js";

const SAMTOOLS_IMAGE =
  "quay.io/biocontainers/samtools@sha256:bf80e07e650becfd084db1abde0fe932b50f990a07fa56421ea647b552b5a406";

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

type Args = typeof zSamtoolsFlagstatInput._output;

interface ExecPlan {
  runtimeSeconds: number;
  bam: ArtifactRecord;
  docker: DockerExecutionPlan;
}

export const samtoolsFlagstatTool: ToolDefinition<Args, ExecPlan> = {
  toolName: "samtools_flagstat",
  contractVersion: "v1",
  planKind: "docker",
  description: "Run samtools flagstat in Docker on a BAM artifact (deterministic, policy-gated).",
  inputSchema: zSamtoolsFlagstatInput,
  outputSchema: zSamtoolsFlagstatOutput,
  declaredOutputs: [{ role: "report", type: "TEXT", label: "samtools_flagstat.txt" }],

  async canonicalize(args: Args, ctx: ToolContext): Promise<PreparedToolRun<ExecPlan>> {
    const projectId = args.project_id as ProjectId;
    const threads = ctx.policy.enforceThreads("samtools_flagstat", 1);
    const runtimeSeconds = ctx.policy.maxRuntimeSeconds();

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

    const canonicalParams: JsonObject = {
      project_id: projectId,
      bam: {
        checksum_sha256: bam.checksumSha256,
        type: bam.type,
        size_bytes: bam.sizeBytes.toString()
      },
      threads,
      docker: {
        image: SAMTOOLS_IMAGE,
        network_mode: ctx.policy.dockerNetworkMode(),
        argv: ["samtools", "flagstat", "/work/in/input.bam"]
      }
    };

    const docker: DockerExecutionPlan = {
      image: SAMTOOLS_IMAGE,
      argv: ["samtools", "flagstat", "/work/in/input.bam"],
      workdir: "/work",
      inputs: [{ role: "bam", artifact: bam, destName: "input.bam" }],
      resources: { threads, runtimeSeconds }
    };

    return {
      projectId,
      canonicalParams,
      toolVersion: SAMTOOLS_IMAGE,
      plan: { runtimeSeconds, bam, docker },
      inputsToLink: [{ artifactId: bam.artifactId, role: "bam" }]
    };
  },

  async run(args: {
    runId: RunId;
    toolRun: ToolRun;
    prepared: PreparedToolRun<ExecPlan>;
    ctx: ToolContext;
  }): Promise<ToolExecutionResult> {
    const { runId, toolRun, prepared, ctx } = args;

    await toolRun.event("exec.plan", `backend=docker image=${SAMTOOLS_IMAGE}`, null);

    const result = await executeDockerPlan({
      policy: ctx.policy,
      runsDir: ctx.runsDir,
      runId,
      toolName: "samtools_flagstat",
      plan: prepared.plan.docker,
      materializeToPath: async (artifactId: string, destPath: string) => ctx.artifacts.materializeToPath(artifactId as any, destPath)
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
