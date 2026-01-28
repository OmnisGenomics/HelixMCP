import { ErrorCode, McpError } from "@modelcontextprotocol/sdk/types.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { promises as fs } from "fs";
import type { ArtifactRecord } from "../core/artifact.js";
import type { ArtifactType } from "../core/artifact.js";
import type { ArtifactId, ProjectId, RunId } from "../core/ids.js";
import type { JsonObject } from "../core/json.js";
import { sha256Prefixed } from "../core/canonicalJson.js";
import type { ArtifactService } from "../artifacts/artifactService.js";
import type { PostgresStore } from "../store/postgresStore.js";
import type { PolicyEngine } from "../policy/policy.js";
import type { ExecutionService } from "../execution/executionService.js";
import { ToolRun, requestedByFromExtra } from "../runs/toolRun.js";
import { deriveRunId } from "../runs/runIdentity.js";
import { ImportTooLargeError } from "../artifacts/localObjectStore.js";
import {
  zArtifactGetInput,
  zArtifactGetOutput,
  zArtifactImportInput,
  zArtifactImportOutput,
  zArtifactListInput,
  zArtifactListOutput,
  zArtifactPreviewTextInput,
  zArtifactPreviewTextOutput,
  zExportNextflowInput,
  zExportNextflowOutput,
  zSeqkitStatsInput,
  zSeqkitStatsOutput,
  zSamtoolsFlagstatInput,
  zSamtoolsFlagstatOutput,
  zSimulateAlignReadsInput,
  zSimulateAlignReadsOutput,
  zSimulateQcFastqInput,
  zSimulateQcFastqOutput
} from "./toolSchemas.js";

export interface GatewayDeps {
  policy: PolicyEngine;
  store: PostgresStore;
  artifacts: ArtifactService;
  execution: ExecutionService;
}

function toArtifactSummary(a: ArtifactRecord): JsonObject {
  return {
    artifact_id: a.artifactId,
    project_id: a.projectId,
    type: a.type,
    mime_type: a.mimeType,
    size_bytes: a.sizeBytes.toString(),
    checksum_sha256: a.checksumSha256,
    label: a.label,
    created_at: a.createdAt,
    created_by_run_id: a.createdByRunId,
    metadata: a.metadata
  };
}

function envSnapshot(): JsonObject {
  return {
    node: process.version,
    mode: process.env.DATABASE_URL ? "postgres" : "pg-mem",
    object_store: process.env.OBJECT_STORE_DIR ?? "var/objects"
  };
}

export function createGatewayServer(deps: GatewayDeps): McpServer {
  const mcp = new McpServer({
    name: "helixmcp-biomcp-fabric-gateway",
    version: "0.3.0"
  });

  const SEQKIT_IMAGE =
    "quay.io/biocontainers/seqkit@sha256:67c9a1cfeafbccfd43bbd1fbb80646c9faa06a50b22c8ea758c3c84268b6765d";
  const SAMTOOLS_IMAGE =
    "quay.io/biocontainers/samtools@sha256:bf80e07e650becfd084db1abde0fe932b50f990a07fa56421ea647b552b5a406";

  mcp.registerTool(
    "artifact_import",
    {
      description: "Import bytes into the Artifact store (in silico, policy-gated).",
      inputSchema: zArtifactImportInput,
      outputSchema: zArtifactImportOutput
    },
	    async (args, extra) => {
	      const toolName = "artifact_import";
	      const contractVersion = "v1";
	      const projectId = args.project_id as ProjectId;
	      let toolRun: ToolRun | null = null;
	      let started = false;

	      try {
	        deps.policy.assertToolAllowed(toolName);
	        deps.policy.assertImportSourceAllowed(args.source.kind);
	        const sourceKind = args.source.kind;
	        const safeLocalPath =
	          sourceKind === "local_path" ? await deps.policy.validateLocalPathImport(args.source.path) : null;
	        const localPathSizeBytes = safeLocalPath ? (await fs.stat(safeLocalPath)).size : null;

	        const canonicalSource: JsonObject =
	          sourceKind === "inline_text"
	            ? {
	                kind: "inline_text",
	                text_sha256: sha256Prefixed(Buffer.from(args.source.text, "utf8")),
	                text_bytes: Buffer.byteLength(args.source.text, "utf8")
	              }
	            : {
	                kind: "local_path",
	                path: safeLocalPath,
	                size_bytes: localPathSizeBytes
	              };

	        const canonicalParams: JsonObject = {
	          project_id: projectId,
	          type_hint: (args.type_hint ?? null) as ArtifactType | null,
	          label: args.label ?? null,
	          source: canonicalSource
	        };

        const { runId, paramsHash } = deriveRunId({
          toolName,
          contractVersion,
          policyHash: deps.policy.policyHash,
          canonicalParams
        });

        const existing = await deps.store.getRun(runId);
        if (existing?.status === "succeeded" && existing.resultJson) {
          return {
            content: [{ type: "text", text: `Replayed ${toolName} (${runId})` }],
            structuredContent: existing.resultJson
          };
        }

	        toolRun = new ToolRun(
	          { store: deps.store, artifacts: deps.artifacts },
	          {
	            runId,
	            projectId,
	            toolName,
	            contractVersion,
	            toolVersion: "v1",
	            paramsHash,
	            canonicalParams,
	            policyHash: deps.policy.policyHash,
	            requestedBy: requestedByFromExtra(extra),
	            policySnapshot: deps.policy.snapshot() as JsonObject,
	            environment: envSnapshot()
	          }
	        );

	        await toolRun.start();
	        started = true;

	        const safeSource =
	          sourceKind === "local_path"
	            ? { kind: "local_path" as const, path: safeLocalPath ?? args.source.path }
	            : { kind: "inline_text" as const, text: args.source.text };

	        const artifact = await deps.artifacts.importArtifact({
	          projectId,
	          source: safeSource,
	          typeHint: (args.type_hint ?? null) as ArtifactType | null,
	          label: args.label ?? null,
	          createdByRunId: toolRun.runId,
	          maxBytes: deps.policy.maxImportBytes()
	        });

	        await toolRun.linkOutput(artifact.artifactId, "artifact");
	        const structured = await toolRun.finishSuccess({ artifact: toArtifactSummary(artifact) }, "imported");

	        return {
	          content: [{ type: "text", text: `Imported ${artifact.artifactId}` }],
	          structuredContent: structured
	        };
	      } catch (e) {
	        if (e instanceof ImportTooLargeError) {
	          if (toolRun && started) await toolRun.finishBlocked(e.message);
	          throw new McpError(ErrorCode.InvalidRequest, e.message);
	        }
	        if (e instanceof McpError) {
	          if (toolRun && started) {
	            if (e.code === ErrorCode.InvalidRequest) await toolRun.finishBlocked(e.message);
	            else await toolRun.finishFailure(e.message);
	          }
	          throw e;
	        }
	        if (e instanceof Error) {
	          if (toolRun && started) await toolRun.finishFailure(e.message);
	          throw e;
	        }
	        if (toolRun && started) await toolRun.finishFailure("unknown error");
	        throw e;
	      }
	    }
	  );

  mcp.registerTool(
    "artifact_get",
    {
      description: "Fetch Artifact metadata by ID.",
      inputSchema: zArtifactGetInput,
      outputSchema: zArtifactGetOutput
    },
    async (args, extra) => {
      const toolName = "artifact_get";
      const contractVersion = "v1";
      let toolRun: ToolRun | null = null;
      let started = false;

      try {
        deps.policy.assertToolAllowed(toolName);

        const artifact = await deps.artifacts.getArtifact(args.artifact_id as ArtifactId);
        if (!artifact) throw new McpError(ErrorCode.InvalidParams, `unknown artifact_id: ${args.artifact_id}`);

        const canonicalParams: JsonObject = {
          artifact: {
            checksum_sha256: artifact.checksumSha256,
            type: artifact.type,
            size_bytes: artifact.sizeBytes.toString()
          }
        };

        const { runId, paramsHash } = deriveRunId({
          toolName,
          contractVersion,
          policyHash: deps.policy.policyHash,
          canonicalParams
        });

        const existing = await deps.store.getRun(runId);
        if (existing?.status === "succeeded" && existing.resultJson) {
          return {
            content: [{ type: "text", text: `Replayed ${toolName} (${runId})` }],
            structuredContent: existing.resultJson
          };
        }

        toolRun = new ToolRun(
          { store: deps.store, artifacts: deps.artifacts },
          {
            runId,
            projectId: artifact.projectId,
            toolName,
            contractVersion,
            toolVersion: "v1",
            paramsHash,
            canonicalParams,
            policyHash: deps.policy.policyHash,
            requestedBy: requestedByFromExtra(extra),
            policySnapshot: deps.policy.snapshot() as JsonObject,
            environment: envSnapshot()
          }
        );

        await toolRun.start();
        started = true;
        await toolRun.linkInput(artifact.artifactId, "artifact");

        const structured = await toolRun.finishSuccess({ artifact: toArtifactSummary(artifact) }, "ok");
        return {
          content: [{ type: "text", text: `Artifact ${artifact.artifactId}` }],
          structuredContent: structured
        };
      } catch (e) {
        if (e instanceof McpError) {
          if (toolRun && started) {
            if (e.code === ErrorCode.InvalidRequest) await toolRun.finishBlocked(e.message);
            else await toolRun.finishFailure(e.message);
          }
          throw e;
        }
        if (e instanceof Error) {
          if (toolRun && started) await toolRun.finishFailure(e.message);
          throw e;
        }
        if (toolRun && started) await toolRun.finishFailure("unknown error");
        throw e;
      }
    }
  );

  mcp.registerTool(
    "artifact_list",
    {
      description: "List Artifacts for a project.",
      inputSchema: zArtifactListInput,
      outputSchema: zArtifactListOutput
    },
    async (args, extra) => {
      const toolName = "artifact_list";
      const contractVersion = "v1";
      const projectId = args.project_id as ProjectId;
      let toolRun: ToolRun | null = null;
      let started = false;

      try {
        deps.policy.assertToolAllowed(toolName);

        const snapshot = await deps.store.getArtifactListSnapshot(projectId);
        const canonicalParams: JsonObject = {
          project_id: projectId,
          limit: args.limit,
          as_of_created_at: snapshot.asOfCreatedAt,
          artifact_count: snapshot.artifactCount
        };
        const { runId, paramsHash } = deriveRunId({
          toolName,
          contractVersion,
          policyHash: deps.policy.policyHash,
          canonicalParams
        });

        const existing = await deps.store.getRun(runId);
        if (existing?.status === "succeeded" && existing.resultJson) {
          return {
            content: [{ type: "text", text: `Replayed ${toolName} (${runId})` }],
            structuredContent: existing.resultJson
          };
        }

        toolRun = new ToolRun(
          { store: deps.store, artifacts: deps.artifacts },
          {
            runId,
            projectId,
            toolName,
            contractVersion,
            toolVersion: "v1",
            paramsHash,
            canonicalParams,
            policyHash: deps.policy.policyHash,
            requestedBy: requestedByFromExtra(extra),
            policySnapshot: deps.policy.snapshot() as JsonObject,
            environment: envSnapshot()
          }
        );

        await toolRun.start();
        started = true;
        const artifacts = await deps.artifacts.listArtifacts(projectId, args.limit, snapshot.asOfCreatedAt);
        const structured = await toolRun.finishSuccess(
          { as_of_created_at: snapshot.asOfCreatedAt, artifact_count: snapshot.artifactCount, artifacts: artifacts.map(toArtifactSummary) },
          `count=${artifacts.length}`
        );

        return {
          content: [{ type: "text", text: `Artifacts: ${artifacts.length}` }],
          structuredContent: structured
        };
      } catch (e) {
        if (e instanceof McpError) {
          if (toolRun && started) {
            if (e.code === ErrorCode.InvalidRequest) await toolRun.finishBlocked(e.message);
            else await toolRun.finishFailure(e.message);
          }
          throw e;
        }
        if (e instanceof Error) {
          if (toolRun && started) await toolRun.finishFailure(e.message);
          throw e;
        }
        if (toolRun && started) await toolRun.finishFailure("unknown error");
        throw e;
      }
    }
  );

  mcp.registerTool(
    "artifact_preview_text",
    {
      description: "Preview an Artifact as UTF-8 text (policy-gated by artifact access).",
      inputSchema: zArtifactPreviewTextInput,
      outputSchema: zArtifactPreviewTextOutput
    },
    async (args, extra) => {
      const toolName = "artifact_preview_text";
      const contractVersion = "v1";
      let toolRun: ToolRun | null = null;
      let started = false;

      try {
        deps.policy.assertToolAllowed(toolName);

        const artifact = await deps.artifacts.getArtifact(args.artifact_id as ArtifactId);
        if (!artifact) throw new McpError(ErrorCode.InvalidParams, `unknown artifact_id: ${args.artifact_id}`);

        const caps = deps.policy.previewCaps();
        const effectiveMaxBytes = Math.min(args.max_bytes, caps.maxBytes);
        const effectiveMaxLines = Math.min(args.max_lines, caps.maxLines);

        const canonicalParams: JsonObject = {
          artifact_id: artifact.artifactId,
          max_bytes: effectiveMaxBytes,
          max_lines: effectiveMaxLines
        };

        const { runId, paramsHash } = deriveRunId({
          toolName,
          contractVersion,
          policyHash: deps.policy.policyHash,
          canonicalParams
        });

        const existing = await deps.store.getRun(runId);
        if (existing?.status === "succeeded" && existing.resultJson) {
          return {
            content: [{ type: "text", text: `Replayed ${toolName} (${runId})` }],
            structuredContent: existing.resultJson
          };
        }

        toolRun = new ToolRun(
          { store: deps.store, artifacts: deps.artifacts },
          {
            runId,
            projectId: artifact.projectId,
            toolName,
            contractVersion,
            toolVersion: "v1",
            paramsHash,
            canonicalParams,
            policyHash: deps.policy.policyHash,
            requestedBy: requestedByFromExtra(extra),
            policySnapshot: deps.policy.snapshot() as JsonObject,
            environment: envSnapshot()
          }
        );

        await toolRun.start();
        started = true;
        await toolRun.linkInput(artifact.artifactId, "artifact");

        const preview = await deps.artifacts.previewText(artifact.artifactId, {
          maxBytes: effectiveMaxBytes,
          maxLines: effectiveMaxLines
        });

        const structured = await toolRun.finishSuccess(
          {
            artifact_id: artifact.artifactId,
            preview: preview.preview,
            truncated: preview.truncated
          },
          preview.truncated ? "preview(truncated)" : "preview"
        );

        return {
          content: [{ type: "text", text: preview.preview }],
          structuredContent: structured
        };
      } catch (e) {
        if (e instanceof McpError) {
          if (toolRun && started) {
            if (e.code === ErrorCode.InvalidRequest) await toolRun.finishBlocked(e.message);
            else await toolRun.finishFailure(e.message);
          }
          throw e;
        }
        if (e instanceof Error) {
          if (toolRun && started) await toolRun.finishFailure(e.message);
          throw e;
        }
        if (toolRun && started) await toolRun.finishFailure("unknown error");
        throw e;
      }
    }
  );

  mcp.registerTool(
    "simulate_qc_fastq",
    {
      description: "Simulate QC metrics for read artifacts (deterministic, in silico).",
      inputSchema: zSimulateQcFastqInput,
      outputSchema: zSimulateQcFastqOutput
    },
    async (args, extra) => {
      const toolName = "simulate_qc_fastq";
      const contractVersion = "v1";
      const projectId = args.project_id as ProjectId;
      let toolRun: ToolRun | null = null;
      let started = false;

      try {
        deps.policy.assertToolAllowed(toolName);
        const threads = deps.policy.enforceThreads(toolName, args.threads);

        const reads1 = await deps.artifacts.getArtifact(args.reads_1 as ArtifactId);
        if (!reads1) throw new McpError(ErrorCode.InvalidParams, `unknown reads_1 artifact_id: ${args.reads_1}`);

        let reads2: ArtifactRecord | null = null;
        if (args.reads_2) {
          reads2 = await deps.artifacts.getArtifact(args.reads_2 as ArtifactId);
          if (!reads2) throw new McpError(ErrorCode.InvalidParams, `unknown reads_2 artifact_id: ${args.reads_2}`);
        }

	        const canonicalParams: JsonObject = {
	          project_id: projectId,
	          reads_1: {
	            checksum_sha256: reads1.checksumSha256,
	            type: reads1.type,
	            size_bytes: reads1.sizeBytes.toString()
	          },
	          reads_2: reads2
	            ? {
	                checksum_sha256: reads2.checksumSha256,
	                type: reads2.type,
	                size_bytes: reads2.sizeBytes.toString()
	              }
	            : null,
	          threads
	        };

        const { runId, paramsHash } = deriveRunId({
          toolName,
          contractVersion,
          policyHash: deps.policy.policyHash,
          canonicalParams
        });

        const existing = await deps.store.getRun(runId);
        if (existing?.status === "succeeded" && existing.resultJson) {
          return {
            content: [{ type: "text", text: `Replayed ${toolName} (${runId})` }],
            structuredContent: existing.resultJson
          };
        }

        toolRun = new ToolRun(
          { store: deps.store, artifacts: deps.artifacts },
          {
            runId,
            projectId,
            toolName,
            contractVersion,
            toolVersion: "sim-v1",
            paramsHash,
            canonicalParams,
            policyHash: deps.policy.policyHash,
            requestedBy: requestedByFromExtra(extra),
            policySnapshot: deps.policy.snapshot() as JsonObject,
            environment: envSnapshot()
          }
        );

        await toolRun.start();
	        started = true;
	        await toolRun.linkInput(reads1.artifactId, "reads_1");
	        if (reads2) await toolRun.linkInput(reads2.artifactId, "reads_2");
	        await toolRun.event("simulate.qc", `threads=${threads}`, null);

	        const outcome = await deps.execution.execute({
	          runId,
	          toolName,
	          resources: { threads, runtimeSeconds: deps.policy.maxRuntimeSeconds() },
	          canonicalParams,
	          inputs: {
	            reads_1: reads1,
	            reads_2: reads2
	          }
	        });

	        const outputIds: Record<string, ArtifactId> = {};
	        for (const out of outcome.outputs) {
	          const id = await toolRun.createOutputArtifact({
	            type: out.type,
	            label: out.label,
	            contentText: out.contentText,
	            role: out.role
	          });
	          outputIds[out.role] = id;
	        }

	        const qc = (outcome.metrics as any).qc as unknown;
	        const reportArtifactId = outputIds["report"];
	        if (!reportArtifactId) throw new Error("missing report output");

	        const structured = await toolRun.finishSuccess({ qc: qc as JsonObject, report_artifact_id: reportArtifactId }, "qc ok");

	        return {
	          content: [{ type: "text", text: `QC complete (run ${structured.provenance_run_id as string})` }],
	          structuredContent: structured
	        };
      } catch (e) {
        if (e instanceof McpError) {
          if (toolRun && started) {
            if (e.code === ErrorCode.InvalidRequest) await toolRun.finishBlocked(e.message);
            else await toolRun.finishFailure(e.message);
          }
          throw e;
        }
        if (e instanceof Error) {
          if (toolRun && started) await toolRun.finishFailure(e.message);
          throw e;
        }
        if (toolRun && started) await toolRun.finishFailure("unknown error");
        throw new Error("unknown error");
      }
    }
  );

  mcp.registerTool(
    "simulate_align_reads",
    {
      description: "Simulate alignment outputs for read artifacts (deterministic, in silico).",
      inputSchema: zSimulateAlignReadsInput,
      outputSchema: zSimulateAlignReadsOutput
    },
    async (args, extra) => {
      const toolName = "simulate_align_reads";
      const contractVersion = "v1";
      const projectId = args.project_id as ProjectId;
      let toolRun: ToolRun | null = null;
      let started = false;

      try {
        deps.policy.assertToolAllowed(toolName);
        const threads = deps.policy.enforceThreads(toolName, args.threads);

        const reads1 = await deps.artifacts.getArtifact(args.reads_1 as ArtifactId);
        if (!reads1) throw new McpError(ErrorCode.InvalidParams, `unknown reads_1 artifact_id: ${args.reads_1}`);

        let reads2: ArtifactRecord | null = null;
        if (args.reads_2) {
          reads2 = await deps.artifacts.getArtifact(args.reads_2 as ArtifactId);
          if (!reads2) throw new McpError(ErrorCode.InvalidParams, `unknown reads_2 artifact_id: ${args.reads_2}`);
        }

	        const canonicalParams: JsonObject = {
	          project_id: projectId,
	          reads_1: {
	            checksum_sha256: reads1.checksumSha256,
	            type: reads1.type,
	            size_bytes: reads1.sizeBytes.toString()
	          },
	          reads_2: reads2
	            ? {
	                checksum_sha256: reads2.checksumSha256,
	                type: reads2.type,
	                size_bytes: reads2.sizeBytes.toString()
	              }
	            : null,
	          reference: { alias: args.reference.alias },
	          read_group: args.read_group
	            ? {
	                id: args.read_group.id,
	                sm: args.read_group.sm,
	                ...(args.read_group.pl ? { pl: args.read_group.pl } : {})
	              }
	            : null,
	          threads,
	          sort: args.sort,
	          mark_duplicates: args.mark_duplicates
	        };

        const { runId, paramsHash } = deriveRunId({
          toolName,
          contractVersion,
          policyHash: deps.policy.policyHash,
          canonicalParams
        });

        const existing = await deps.store.getRun(runId);
        if (existing?.status === "succeeded" && existing.resultJson) {
          return {
            content: [{ type: "text", text: `Replayed ${toolName} (${runId})` }],
            structuredContent: existing.resultJson
          };
        }

        toolRun = new ToolRun(
          { store: deps.store, artifacts: deps.artifacts },
          {
            runId,
            projectId,
            toolName,
            contractVersion,
            toolVersion: "sim-v1",
            paramsHash,
            canonicalParams,
            policyHash: deps.policy.policyHash,
            requestedBy: requestedByFromExtra(extra),
            policySnapshot: deps.policy.snapshot() as JsonObject,
            environment: envSnapshot()
          }
        );

        await toolRun.start();
	        started = true;
	        await toolRun.linkInput(reads1.artifactId, "reads_1");
	        if (reads2) await toolRun.linkInput(reads2.artifactId, "reads_2");
	        await toolRun.event("simulate.align", `reference=${args.reference.alias} threads=${threads}`, null);

	        const outcome = await deps.execution.execute({
	          runId,
	          toolName,
	          resources: { threads, runtimeSeconds: deps.policy.maxRuntimeSeconds() },
	          canonicalParams,
	          inputs: {
	            reads_1: reads1,
	            reads_2: reads2
	          }
	        });

	        const outputIds: Record<string, ArtifactId> = {};
	        for (const out of outcome.outputs) {
	          const id = await toolRun.createOutputArtifact({
	            type: out.type,
	            label: out.label,
	            contentText: out.contentText,
	            role: out.role
	          });
	          outputIds[out.role] = id;
	        }

	        const qc = (outcome.metrics as any).qc as unknown;
	        const bamSortedId = outputIds["bam_sorted"];
	        const baiId = outputIds["bai"];
	        if (!bamSortedId || !baiId) throw new Error("missing alignment outputs");

	        const structured = await toolRun.finishSuccess(
	          {
	            bam_sorted: bamSortedId,
	            bai: baiId,
	            qc: qc as JsonObject
	          },
	          "align ok"
	        );

	        return {
	          content: [{ type: "text", text: `Alignment complete (run ${structured.provenance_run_id as string})` }],
	          structuredContent: structured
	        };
      } catch (e) {
        if (e instanceof McpError) {
          if (toolRun && started) {
            if (e.code === ErrorCode.InvalidRequest) await toolRun.finishBlocked(e.message);
            else await toolRun.finishFailure(e.message);
          }
          throw e;
        }
        if (e instanceof Error) {
          if (toolRun && started) await toolRun.finishFailure(e.message);
          throw e;
        }
        if (toolRun && started) await toolRun.finishFailure("unknown error");
        throw new Error("unknown error");
      }
    }
  );

  mcp.registerTool(
    "seqkit_stats",
    {
      description: "Run seqkit stats in Docker on a sequence artifact (deterministic, policy-gated).",
      inputSchema: zSeqkitStatsInput,
      outputSchema: zSeqkitStatsOutput
    },
    async (args, extra) => {
      const toolName = "seqkit_stats";
      const contractVersion = "v1";
      const projectId = args.project_id as ProjectId;
      let toolRun: ToolRun | null = null;
      let started = false;

      try {
        deps.policy.assertToolAllowed(toolName);
        const threads = deps.policy.enforceThreads(toolName, args.threads);

        const input = await deps.artifacts.getArtifact(args.input_artifact_id as ArtifactId);
        if (!input) {
          throw new McpError(ErrorCode.InvalidParams, `unknown input_artifact_id: ${args.input_artifact_id}`);
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
            network_mode: deps.policy.dockerNetworkMode(),
            argv: ["seqkit", "stats", "-T", "/work/in/input"]
          }
        };

        const { runId, paramsHash } = deriveRunId({
          toolName,
          contractVersion,
          policyHash: deps.policy.policyHash,
          canonicalParams
        });

        const existing = await deps.store.getRun(runId);
        if (existing?.status === "succeeded" && existing.resultJson) {
          return {
            content: [{ type: "text", text: `Replayed ${toolName} (${runId})` }],
            structuredContent: existing.resultJson
          };
        }

        toolRun = new ToolRun(
          { store: deps.store, artifacts: deps.artifacts },
          {
            runId,
            projectId,
            toolName,
            contractVersion,
            toolVersion: SEQKIT_IMAGE,
            paramsHash,
            canonicalParams,
            policyHash: deps.policy.policyHash,
            requestedBy: requestedByFromExtra(extra),
            policySnapshot: deps.policy.snapshot() as JsonObject,
            environment: envSnapshot()
          }
        );

        await toolRun.start();
        started = true;
        await toolRun.linkInput(input.artifactId, "input");
        await toolRun.event("exec.plan", `backend=docker image=${SEQKIT_IMAGE}`, null);

        const outcome = await deps.execution.execute({
          runId,
          toolName,
          resources: { threads, runtimeSeconds: deps.policy.maxRuntimeSeconds() },
          canonicalParams,
          inputs: {
            input
          }
        });

        if (outcome.exec) {
          await toolRun.event("exec.result", `exit=${outcome.exec.exitCode}`, {
            backend: outcome.exec.backend,
            started_at: outcome.exec.startedAt,
            finished_at: outcome.exec.finishedAt,
            stderr: outcome.exec.stderr
          });
        }

        const outputIds: Record<string, ArtifactId> = {};
        for (const out of outcome.outputs) {
          const id = await toolRun.createOutputArtifact({
            type: out.type,
            label: out.label,
            contentText: out.contentText,
            role: out.role
          });
          outputIds[out.role] = id;
        }

        const reportArtifactId = outputIds["report"];
        if (!reportArtifactId) throw new Error("missing report output");

        const stats = (outcome.metrics as any).stats as unknown;
        const structured = await toolRun.finishSuccess(
          { report_artifact_id: reportArtifactId, stats: stats as JsonObject },
          "seqkit stats ok"
        );

        return {
          content: [{ type: "text", text: `seqkit stats complete (run ${structured.provenance_run_id as string})` }],
          structuredContent: structured
        };
      } catch (e) {
        if (e instanceof McpError) {
          if (toolRun && started) {
            if (e.code === ErrorCode.InvalidRequest) await toolRun.finishBlocked(e.message);
            else await toolRun.finishFailure(e.message);
          }
          throw e;
        }
        if (e instanceof Error) {
          if (toolRun && started) await toolRun.finishFailure(e.message);
          throw e;
        }
        if (toolRun && started) await toolRun.finishFailure("unknown error");
        throw new Error("unknown error");
      }
    }
  );

  mcp.registerTool(
    "samtools_flagstat",
    {
      description: "Run samtools flagstat in Docker on a BAM artifact (deterministic, policy-gated).",
      inputSchema: zSamtoolsFlagstatInput,
      outputSchema: zSamtoolsFlagstatOutput
    },
    async (args, extra) => {
      const toolName = "samtools_flagstat";
      const contractVersion = "v1";
      const projectId = args.project_id as ProjectId;
      let toolRun: ToolRun | null = null;
      let started = false;

      try {
        deps.policy.assertToolAllowed(toolName);
        const threads = deps.policy.enforceThreads(toolName, 1);

        const bam = await deps.artifacts.getArtifact(args.bam_artifact_id as ArtifactId);
        if (!bam) {
          throw new McpError(ErrorCode.InvalidParams, `unknown bam_artifact_id: ${args.bam_artifact_id}`);
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
            network_mode: deps.policy.dockerNetworkMode(),
            argv: ["samtools", "flagstat", "/work/in/input.bam"]
          }
        };

        const { runId, paramsHash } = deriveRunId({
          toolName,
          contractVersion,
          policyHash: deps.policy.policyHash,
          canonicalParams
        });

        const existing = await deps.store.getRun(runId);
        if (existing?.status === "succeeded" && existing.resultJson) {
          return {
            content: [{ type: "text", text: `Replayed ${toolName} (${runId})` }],
            structuredContent: existing.resultJson
          };
        }

        toolRun = new ToolRun(
          { store: deps.store, artifacts: deps.artifacts },
          {
            runId,
            projectId,
            toolName,
            contractVersion,
            toolVersion: SAMTOOLS_IMAGE,
            paramsHash,
            canonicalParams,
            policyHash: deps.policy.policyHash,
            requestedBy: requestedByFromExtra(extra),
            policySnapshot: deps.policy.snapshot() as JsonObject,
            environment: envSnapshot()
          }
        );

        await toolRun.start();
        started = true;
        await toolRun.linkInput(bam.artifactId, "bam");
        await toolRun.event("exec.plan", `backend=docker image=${SAMTOOLS_IMAGE}`, null);

        const outcome = await deps.execution.execute({
          runId,
          toolName,
          resources: { threads, runtimeSeconds: deps.policy.maxRuntimeSeconds() },
          canonicalParams,
          inputs: {
            bam
          }
        });

        if (outcome.exec) {
          await toolRun.event("exec.result", `exit=${outcome.exec.exitCode}`, {
            backend: outcome.exec.backend,
            started_at: outcome.exec.startedAt,
            finished_at: outcome.exec.finishedAt,
            stderr: outcome.exec.stderr
          });
        }

        const outputIds: Record<string, ArtifactId> = {};
        for (const out of outcome.outputs) {
          const id = await toolRun.createOutputArtifact({
            type: out.type,
            label: out.label,
            contentText: out.contentText,
            role: out.role
          });
          outputIds[out.role] = id;
        }

        const reportArtifactId = outputIds["report"];
        if (!reportArtifactId) throw new Error("missing report output");

        const flagstat = (outcome.metrics as any).flagstat as unknown;
        const structured = await toolRun.finishSuccess(
          { report_artifact_id: reportArtifactId, flagstat: flagstat as JsonObject },
          "samtools flagstat ok"
        );

        return {
          content: [{ type: "text", text: `samtools flagstat complete (run ${structured.provenance_run_id as string})` }],
          structuredContent: structured
        };
      } catch (e) {
        if (e instanceof McpError) {
          if (toolRun && started) {
            if (e.code === ErrorCode.InvalidRequest) await toolRun.finishBlocked(e.message);
            else await toolRun.finishFailure(e.message);
          }
          throw e;
        }
        if (e instanceof Error) {
          if (toolRun && started) await toolRun.finishFailure(e.message);
          throw e;
        }
        if (toolRun && started) await toolRun.finishFailure("unknown error");
        throw new Error("unknown error");
      }
    }
  );

  mcp.registerTool(
    "export_nextflow",
    {
      description: "Export a run graph as a Nextflow stub (in silico, diff-friendly).",
      inputSchema: zExportNextflowInput,
      outputSchema: zExportNextflowOutput
    },
    async (args, extra) => {
      const toolName = "export_nextflow";
      const contractVersion = "v1";
      let toolRun: ToolRun | null = null;
      let started = false;

      try {
        deps.policy.assertToolAllowed(toolName);

        const targetRun = await deps.store.getRun(args.run_id as RunId);
        if (!targetRun) throw new McpError(ErrorCode.InvalidParams, `unknown run_id: ${args.run_id}`);

        const canonicalParams: JsonObject = {
          exported_run_id: targetRun.runId
        };

        const { runId, paramsHash } = deriveRunId({
          toolName,
          contractVersion,
          policyHash: deps.policy.policyHash,
          canonicalParams
        });

        const existing = await deps.store.getRun(runId);
        if (existing?.status === "succeeded" && existing.resultJson) {
          return {
            content: [{ type: "text", text: `Replayed ${toolName} (${runId})` }],
            structuredContent: existing.resultJson
          };
        }

        toolRun = new ToolRun(
          { store: deps.store, artifacts: deps.artifacts },
          {
            runId,
            projectId: targetRun.projectId,
            toolName,
            contractVersion,
            toolVersion: "v1",
            paramsHash,
            canonicalParams,
            policyHash: deps.policy.policyHash,
            requestedBy: requestedByFromExtra(extra),
            policySnapshot: deps.policy.snapshot() as JsonObject,
            environment: envSnapshot()
          }
        );

        await toolRun.start();
        started = true;

        const inputs = await deps.store.listRunInputs(targetRun.runId);
        const outputs = await deps.store.listRunOutputs(targetRun.runId);

        const nf = `// Nextflow export (stub) for run ${targetRun.runId}\n// Tool: ${targetRun.toolName}\n\nparams.run_id = '${targetRun.runId}'\n\nworkflow {\n  // Inputs\n${inputs
          .map((i) => `  // - ${i.role}: ${i.artifactId}`)
          .join("\n")}\n\n  // Outputs\n${outputs
          .map((o) => `  // - ${o.role}: ${o.artifactId}`)
          .join("\n")}\n\n  // TODO: map artifact IDs to materialized paths in your runner\n}\n`;

        const nfArtifactId = await toolRun.createOutputArtifact({
          type: "TEXT",
          label: "main.nf",
          contentText: nf,
          role: "nextflow"
        });

        const structured = await toolRun.finishSuccess(
          {
            exported_run_id: targetRun.runId,
            nextflow_script_artifact_id: nfArtifactId
          },
          "exported"
        );

        return {
          content: [{ type: "text", text: `Exported Nextflow stub for ${targetRun.runId}` }],
          structuredContent: structured
        };
      } catch (e) {
        if (e instanceof McpError) {
          if (toolRun && started) {
            if (e.code === ErrorCode.InvalidRequest) await toolRun.finishBlocked(e.message);
            else await toolRun.finishFailure(e.message);
          }
          throw e;
        }
        if (e instanceof Error) {
          if (toolRun && started) await toolRun.finishFailure(e.message);
          throw e;
        }
        if (toolRun && started) await toolRun.finishFailure("unknown error");
        throw new Error("unknown error");
      }
    }
  );

  return mcp;
}
