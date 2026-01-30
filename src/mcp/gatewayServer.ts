import { ErrorCode, McpError } from "@modelcontextprotocol/sdk/types.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { promises as fs } from "fs";
import path from "path";
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
import { createRunWorkspace, safeJoin } from "../execution/workspace.js";
import { renderSlurmScriptV1, type SlurmJobSpecV1 } from "../execution/slurm/slurmScriptV1.js";
import type { SlurmSubmitter } from "../execution/slurm/submitter.js";
import { SbatchSubmitter } from "../execution/slurm/submitter.js";
import type { SlurmScheduler } from "../execution/slurm/scheduler.js";
import { SystemSlurmScheduler } from "../execution/slurm/scheduler.js";
import { envSnapshot } from "./envSnapshot.js";
import { registerToolDefinitions } from "../toolpacks/register.js";
import { builtinToolDefinitions } from "../toolpacks/builtin/index.js";
import {
  zArtifactGetInput,
  zArtifactGetOutput,
  zArtifactImportInput,
  zArtifactImportOutput,
  zArtifactListInput,
  zArtifactListOutput,
  zArtifactPreviewTextInput,
  zArtifactPreviewTextOutput,
  zDockerJobGetInput,
  zDockerJobGetOutput,
  zExportNextflowInput,
  zExportNextflowOutput,
  zSlurmJobCollectInput,
  zSlurmJobCollectOutput,
  zSlurmJobGetInput,
  zSlurmJobGetOutput,
  zSlurmSubmitInput,
  zSlurmSubmitOutput,
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
  runsDir: string;
  slurmSubmitter?: SlurmSubmitter;
  slurmScheduler?: SlurmScheduler;
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

export function createGatewayServer(deps: GatewayDeps): McpServer {
  const mcp = new McpServer({
    name: "helixmcp-biomcp-fabric-gateway",
    version: "0.9.0"
  });

  const slurmSubmitter = deps.slurmSubmitter ?? new SbatchSubmitter();
  const slurmScheduler = deps.slurmScheduler ?? new SystemSlurmScheduler();

	  const ARTIFACT_TYPE_SET = new Set<ArtifactType>([
	    "FASTQ_GZ",
	    "BAM",
	    "BAI",
	    "VCF",
	    "H5AD",
	    "TSV",
	    "CSV",
	    "JSON",
	    "ZIP",
	    "MD",
	    "TEXT",
	    "HTML",
	    "PDF",
	    "LOG",
	    "UNKNOWN"
	  ]);

  function requireArtifactType(value: string): ArtifactType {
    if (ARTIFACT_TYPE_SET.has(value as ArtifactType)) return value as ArtifactType;
    throw new McpError(ErrorCode.InvalidParams, `unknown artifact type: ${value}`);
  }

  async function assertRegularFileNoSymlink(filePath: string, context: string): Promise<void> {
    const st = await fs.lstat(filePath);
    if (st.isSymbolicLink()) throw new McpError(ErrorCode.InvalidRequest, `policy denied symlinked file for ${context}`);
    if (!st.isFile()) throw new McpError(ErrorCode.InvalidRequest, `expected file for ${context}: ${filePath}`);
  }

  registerToolDefinitions(
    mcp,
    { policy: deps.policy, store: deps.store, artifacts: deps.artifacts, runsDir: deps.runsDir, slurmSubmitter },
    builtinToolDefinitions
  );

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

        await toolRun.start("queued");
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

        await toolRun.start("queued");
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

        await toolRun.start("queued");
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
    "slurm_submit",
    {
      description: "Submit a deterministic, policy-gated Slurm job spec (apptainer-only, network none).",
      inputSchema: zSlurmSubmitInput,
      outputSchema: zSlurmSubmitOutput
    },
    async (args, extra) => {
      const toolName = "slurm_submit";
      const contractVersion = "v1";
      const projectId = args.project_id as ProjectId;
      const jobSpec = args.job_spec as unknown as SlurmJobSpecV1;
      let toolRun: ToolRun | null = null;
      let started = false;

      try {
        deps.policy.assertToolAllowed(toolName);

        const env = jobSpec.execution.command.env ?? {};
        const envSorted: Record<string, string> = {};
        for (const k of Object.keys(env).sort()) envSorted[k] = String(env[k] ?? "");

        const inputsSorted = [...jobSpec.io.inputs].sort((a, b) =>
          a.role.localeCompare(b.role) || a.dest_relpath.localeCompare(b.dest_relpath)
        );
        const outputsSorted = [...jobSpec.io.outputs].sort((a, b) =>
          a.role.localeCompare(b.role) || a.src_relpath.localeCompare(b.src_relpath)
        );

        const canonicalParams: JsonObject = {
          project_id: projectId,
          slurm: {
            spec_version: 1,
            slurm_script_version: "slurm_script_v1",
            resources: {
              time_limit_seconds: jobSpec.resources.time_limit_seconds,
              cpus: jobSpec.resources.cpus,
              mem_mb: jobSpec.resources.mem_mb,
              gpus: jobSpec.resources.gpus ?? null,
              gpu_type: jobSpec.resources.gpu_type ?? null
            },
            placement: {
              partition: jobSpec.placement.partition,
              account: jobSpec.placement.account,
              qos: jobSpec.placement.qos ?? null,
              constraint: jobSpec.placement.constraint ?? null
            },
            execution: {
              kind: "container",
              container: {
                engine: "apptainer",
                image: jobSpec.execution.container.image,
                network_mode: "none",
                readonly_rootfs: true
              },
              command: {
                argv: [...jobSpec.execution.command.argv],
                workdir: jobSpec.execution.command.workdir ?? "/work",
                env: envSorted
              }
            },
            io: {
              inputs: inputsSorted.map((i) => ({
                role: i.role,
                artifact_id: i.artifact_id,
                checksum_sha256: i.checksum_sha256,
                dest_relpath: i.dest_relpath
              })),
              outputs: outputsSorted.map((o) => ({
                role: o.role,
                src_relpath: o.src_relpath,
                type: o.type,
                label: o.label
              }))
            }
          }
        };

        const { runId, paramsHash } = deriveRunId({
          toolName,
          contractVersion,
          policyHash: deps.policy.policyHash,
          canonicalParams
        });

        const existing = await deps.store.getRun(runId);
        if (existing?.resultJson) {
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
            toolVersion: "slurm_script_v1",
            paramsHash,
            canonicalParams,
            policyHash: deps.policy.policyHash,
            requestedBy: requestedByFromExtra(extra),
            policySnapshot: deps.policy.snapshot() as JsonObject,
            environment: envSnapshot()
          }
        );

        await toolRun.start("queued");
        started = true;

        deps.policy.assertSlurmPartitionAllowed(jobSpec.placement.partition);
        deps.policy.assertSlurmAccountAllowed(jobSpec.placement.account);
        deps.policy.assertSlurmQosAllowed(jobSpec.placement.qos ?? null);
        deps.policy.assertSlurmConstraintAllowed(jobSpec.placement.constraint ?? null);
        deps.policy.enforceSlurmResources({
          timeLimitSeconds: jobSpec.resources.time_limit_seconds,
          cpus: jobSpec.resources.cpus,
          memMb: jobSpec.resources.mem_mb,
          gpus: jobSpec.resources.gpus ?? null,
          gpuType: jobSpec.resources.gpu_type ?? null
        });
        deps.policy.assertSlurmNetworkNone(jobSpec.execution.container.network_mode);
        deps.policy.assertSlurmApptainerImageAllowed(jobSpec.execution.container.image);

        await toolRun.event("slurm.submit.plan", `partition=${jobSpec.placement.partition} account=${jobSpec.placement.account}`, {
          partition: jobSpec.placement.partition,
          account: jobSpec.placement.account,
          qos: jobSpec.placement.qos ?? null,
          constraint: jobSpec.placement.constraint ?? null,
          resources: jobSpec.resources,
          image: jobSpec.execution.container.image
        });

        const ws = await createRunWorkspace(deps.runsDir, runId);

        for (const input of inputsSorted) {
          const artifactId = input.artifact_id as ArtifactId;
          const artifact = await deps.artifacts.getArtifact(artifactId);
          if (!artifact) {
            throw new McpError(ErrorCode.InvalidParams, `unknown artifact_id in slurm input: ${artifactId}`);
          }
          if (artifact.projectId !== projectId) {
            throw new McpError(ErrorCode.InvalidRequest, `policy denied cross-project artifact: ${artifactId}`);
          }
          if (artifact.checksumSha256 !== input.checksum_sha256) {
            throw new McpError(
              ErrorCode.InvalidParams,
              `checksum mismatch for artifact ${artifactId} (expected ${input.checksum_sha256}, got ${artifact.checksumSha256})`
            );
          }

          await toolRun.linkInput(artifactId, input.role);
          const destPath = safeJoin(ws.rootDir, input.dest_relpath);
          await deps.artifacts.materializeToPath(artifactId, destPath);
        }

        const script = renderSlurmScriptV1({
          workspaceRootDir: ws.rootDir,
          jobName: `helixmcp_${runId}`,
          spec: jobSpec
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

        const submit = await slurmSubmitter.submit(scriptPath);
        await fs.writeFile(ws.metaPath("slurm_job_id.txt"), submit.slurmJobId + "\n", "utf8");

        await toolRun.event("slurm.submit.ok", `job_id=${submit.slurmJobId}`, { slurm_job_id: submit.slurmJobId });

        const structured = await toolRun.checkpointQueued(
          { slurm_job_id: submit.slurmJobId, slurm_script_artifact_id: scriptArtifactId },
          "slurm submitted"
        );

        return {
          content: [{ type: "text", text: `slurm submitted (run ${structured.provenance_run_id as string})` }],
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
    "docker_job_get",
    {
      description: "Get current state for a Docker-backed run (DB-only; no external calls).",
      inputSchema: zDockerJobGetInput,
      outputSchema: zDockerJobGetOutput
    },
    async (args, extra) => {
      const toolName = "docker_job_get";
      const contractVersion = "v1";
      const targetRunId = args.run_id as RunId;
      let toolRun: ToolRun | null = null;
      let started = false;

      try {
        deps.policy.assertToolAllowed(toolName);

        const targetRun = await deps.store.getRun(targetRunId);
        if (!targetRun) throw new McpError(ErrorCode.InvalidParams, `unknown run_id: ${targetRunId}`);

        const targetParams = await deps.store.getParamSet(targetRun.paramsHash);
        const dockerSpec = (targetParams as any)?.docker as unknown;
        if (!dockerSpec || typeof dockerSpec !== "object") {
          throw new McpError(
            ErrorCode.InvalidRequest,
            `target run does not look like a docker-backed run: ${targetRunId} (use slurm_job_get for Slurm runs)`
          );
        }

        const outputs = await deps.store.listRunOutputs(targetRunId);
        const artifactsByRole: Record<string, string> = {};
        const roleSeen = new Set<string>();
        const warnings: string[] = [];
        for (const o of outputs) {
          if (roleSeen.has(o.role)) {
            warnings.push(`multiple artifacts linked for role=${o.role}; using first`);
            continue;
          }
          roleSeen.add(o.role);
          artifactsByRole[o.role] = o.artifactId;
        }

        const dbStatus = targetRun.status;
        const dbExitCode = targetRun.exitCode ?? null;
        const dbStartedAt = targetRun.startedAt ?? null;
        const dbFinishedAt = targetRun.finishedAt ?? null;

        let state: "queued" | "running" | "succeeded" | "failed" | "blocked" | "unknown" = dbStatus;
        let exitCode: number | null = dbExitCode;

        if (dbFinishedAt && dbExitCode !== null) {
          state = dbExitCode === 0 ? "succeeded" : "failed";
        }
        if (dbStatus === "blocked") state = "blocked";

        const canonicalParams: JsonObject = {
          target_run_id: targetRunId,
          observation: {
            state,
            exit_code: exitCode,
            db: { status: dbStatus, exit_code: dbExitCode, started_at: dbStartedAt, finished_at: dbFinishedAt },
            artifacts_by_role: artifactsByRole
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

        const structured = await toolRun.finishSuccess(
          {
            target_run_id: targetRunId,
            source: "db_only",
            warnings,
            state,
            exit_code: exitCode,
            artifacts_by_role: artifactsByRole,
            db: { status: dbStatus, exit_code: dbExitCode, started_at: dbStartedAt, finished_at: dbFinishedAt }
          },
          "docker job get ok"
        );

        return {
          content: [{ type: "text", text: `docker job state: ${state}` }],
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
    "slurm_job_get",
    {
      description: "Get current state for a Slurm-submitted run (workspace-first; optional scheduler query via policy).",
      inputSchema: zSlurmJobGetInput,
      outputSchema: zSlurmJobGetOutput
    },
    async (args, extra) => {
      const toolName = "slurm_job_get";
      const contractVersion = "v1";
      const targetRunId = args.run_id as RunId;
      let toolRun: ToolRun | null = null;
      let started = false;

      try {
        deps.policy.assertToolAllowed(toolName);

        const targetRun = await deps.store.getRun(targetRunId);
        if (!targetRun) throw new McpError(ErrorCode.InvalidParams, `unknown run_id: ${targetRunId}`);

        const targetParams = await deps.store.getParamSet(targetRun.paramsHash);
        const slurmOutputs = (((targetParams as any)?.slurm as any)?.io as any)?.outputs;
        if (!Array.isArray(slurmOutputs)) {
          throw new McpError(ErrorCode.InvalidRequest, `target run does not look like a slurm-submitted run: ${targetRunId}`);
        }

        const readOptionalText = async (filePath: string, context: string): Promise<string | null> => {
          try {
            await assertRegularFileNoSymlink(filePath, context);
            return (await fs.readFile(filePath, "utf8")).trim();
          } catch {
            return null;
          }
        };

        const hasRegularFile = async (filePath: string, context: string): Promise<boolean> => {
          try {
            await assertRegularFileNoSymlink(filePath, context);
            return true;
          } catch {
            return false;
          }
        };

        const wsRootDir = path.resolve(deps.runsDir, targetRunId);
        const jobIdPath = safeJoin(wsRootDir, path.join("meta", "slurm_job_id.txt"));
        const exitCodePath = safeJoin(wsRootDir, path.join("meta", "exit_code.txt"));
        const stdoutPath = safeJoin(wsRootDir, path.join("meta", "stdout.txt"));
        const stderrPath = safeJoin(wsRootDir, path.join("meta", "stderr.txt"));

        const jobIdFromFile = await readOptionalText(jobIdPath, "slurm_job_id");
        const jobIdFromResult =
          typeof (targetRun.resultJson as any)?.slurm_job_id === "string" ? ((targetRun.resultJson as any).slurm_job_id as string) : null;
        const slurmJobId = jobIdFromFile ?? jobIdFromResult;

        const exitCodeText = await readOptionalText(exitCodePath, "exit_code");
        const workspaceExitCode =
          exitCodeText && Number.isInteger(Number.parseInt(exitCodeText, 10)) ? Number.parseInt(exitCodeText, 10) : null;

        const hasStdout = await hasRegularFile(stdoutPath, "stdout");
        const hasStderr = await hasRegularFile(stderrPath, "stderr");

        const warnings: string[] = [];
        let schedulerInfo: { source: "sacct" | "squeue"; state_raw: string; exit_code: number | null } | null = null;
        let schedulerNormalized: "queued" | "running" | "succeeded" | "failed" | "unknown" | null = null;
        let source: "workspace_only" | "workspace+sacct" | "workspace+squeue" = "workspace_only";

        if (!deps.policy.allowSlurmSchedulerQueries()) {
          warnings.push("scheduler query skipped: disabled by policy (slurm.allow_scheduler_queries=false)");
        } else if (!slurmJobId) {
          warnings.push("scheduler query skipped: missing slurm_job_id");
        } else {
          const queried = await slurmScheduler.query(slurmJobId);
          warnings.push(...queried.warnings);
          if (queried.info) {
            schedulerInfo = {
              source: queried.info.source,
              state_raw: queried.info.stateRaw,
              exit_code: queried.info.exitCode
            };
            schedulerNormalized = queried.info.normalizedState;
            source = queried.info.source === "sacct" ? "workspace+sacct" : "workspace+squeue";
          } else {
            warnings.push("scheduler query skipped: no scheduler state available");
          }
        }

        const outputs = await deps.store.listRunOutputs(targetRunId);
        const artifactsByRole: Record<string, string> = {};
        const roleSeen = new Set<string>();
        for (const o of outputs) {
          if (roleSeen.has(o.role)) {
            warnings.push(`multiple artifacts linked for role=${o.role}; using first`);
            continue;
          }
          roleSeen.add(o.role);
          artifactsByRole[o.role] = o.artifactId;
        }

        const dbStatus = targetRun.status;
        const dbExitCode = targetRun.exitCode ?? null;
        const dbFinishedAt = targetRun.finishedAt ?? null;

        let state: "queued" | "running" | "succeeded" | "failed" | "blocked" | "unknown" = "unknown";
        let exitCode: number | null = null;

        if (workspaceExitCode !== null) {
          exitCode = workspaceExitCode;
          state = workspaceExitCode === 0 ? "succeeded" : "failed";
        } else if (schedulerNormalized && schedulerNormalized !== "unknown") {
          if (schedulerInfo && schedulerInfo.exit_code !== null) exitCode = schedulerInfo.exit_code;
          if (schedulerNormalized === "queued") state = "queued";
          else if (schedulerNormalized === "running") state = "running";
          else if (schedulerNormalized === "succeeded") state = "succeeded";
          else if (schedulerNormalized === "failed") state = "failed";
        } else if (dbStatus === "queued" || dbStatus === "running" || dbStatus === "succeeded" || dbStatus === "failed" || dbStatus === "blocked") {
          state = dbStatus;
          exitCode = dbExitCode;
        }

        const canonicalParams: JsonObject = {
          target_run_id: targetRunId,
          observation: {
            slurm_job_id: slurmJobId,
            state,
            exit_code: exitCode,
            source,
            db: { status: dbStatus, exit_code: dbExitCode, finished_at: dbFinishedAt },
            workspace: {
              has_stdout: hasStdout,
              has_stderr: hasStderr,
              has_exit_code: workspaceExitCode !== null,
              exit_code: workspaceExitCode
            },
            scheduler: schedulerInfo
              ? { source: schedulerInfo.source, state_raw: schedulerInfo.state_raw, exit_code: schedulerInfo.exit_code }
              : null,
            artifacts_by_role: artifactsByRole
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

        const structured = await toolRun.finishSuccess(
          {
            target_run_id: targetRunId,
            slurm_job_id: slurmJobId,
            source,
            warnings,
            state,
            exit_code: exitCode,
            artifacts_by_role: artifactsByRole,
            db: { status: dbStatus, exit_code: dbExitCode, finished_at: dbFinishedAt },
            workspace: {
              has_stdout: hasStdout,
              has_stderr: hasStderr,
              has_exit_code: workspaceExitCode !== null,
              exit_code: workspaceExitCode
            },
            scheduler: schedulerInfo
              ? { source: schedulerInfo.source, state_raw: schedulerInfo.state_raw, exit_code: schedulerInfo.exit_code }
              : null
          },
          "slurm job get ok"
        );

        return {
          content: [{ type: "text", text: `slurm job state: ${state}` }],
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
    "slurm_job_collect",
    {
      description: "Collect outputs for a previously submitted slurm_submit run and finalize its status.",
      inputSchema: zSlurmJobCollectInput,
      outputSchema: zSlurmJobCollectOutput
    },
    async (args, extra) => {
      const toolName = "slurm_job_collect";
      const contractVersion = "v1";
      const targetRunId = args.run_id as RunId;
      let toolRun: ToolRun | null = null;
      let started = false;

      try {
        deps.policy.assertToolAllowed(toolName);

        const targetRun = await deps.store.getRun(targetRunId);
        if (!targetRun) throw new McpError(ErrorCode.InvalidParams, `unknown run_id: ${targetRunId}`);

        const targetParams = await deps.store.getParamSet(targetRun.paramsHash);
        const slurmOutputs = (((targetParams as any)?.slurm as any)?.io as any)?.outputs;
        if (!Array.isArray(slurmOutputs)) {
          throw new McpError(ErrorCode.InvalidRequest, `target run does not look like a slurm_submit run: ${targetRunId}`);
        }

        const canonicalParams: JsonObject = {
          target_run_id: targetRunId
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
        await toolRun.event("collect.plan", `target_run_id=${targetRunId}`, null);

        const finalized =
          (targetRun.status === "succeeded" || targetRun.status === "failed") &&
          typeof targetRun.finishedAt === "string" &&
          targetRun.exitCode !== null;
        if (finalized) {
          const existingOutputs = await deps.store.listRunOutputs(targetRunId);
          existingOutputs.sort((a, b) => a.role.localeCompare(b.role) || a.artifactId.localeCompare(b.artifactId));

          const byRoleExisting = new Map<string, ArtifactId>();
          for (const o of existingOutputs) {
            if (!byRoleExisting.has(o.role)) byRoleExisting.set(o.role, o.artifactId);
          }

          const artifactsByRole: Record<string, ArtifactId> = {};
          for (const out of slurmOutputs as Array<any>) {
            const role = String(out.role ?? "");
            if (!role) continue;
            const existing = byRoleExisting.get(role);
            if (existing) artifactsByRole[role] = existing;
          }

          for (const role of ["stdout", "stderr"] as const) {
            const existing = byRoleExisting.get(role);
            if (existing) artifactsByRole[role] = existing;
          }

          const slurmScript = byRoleExisting.get("slurm_script");
          if (slurmScript) artifactsByRole["slurm_script"] = slurmScript;

          const structured = await toolRun.finishSuccess(
            { target_run_id: targetRunId, exit_code: targetRun.exitCode, artifacts_by_role: artifactsByRole },
            "slurm collect (idempotent)"
          );

          return {
            content: [{ type: "text", text: `slurm already finalized (target ${targetRunId})` }],
            structuredContent: structured
          };
        }

        await deps.store.addRunEvent(targetRunId, "slurm.collect.start", "collect started", { collector_run_id: runId });

        const ws = await createRunWorkspace(deps.runsDir, targetRunId);
        const existingOutputs = await deps.store.listRunOutputs(targetRunId);
        existingOutputs.sort((a, b) => a.role.localeCompare(b.role) || a.artifactId.localeCompare(b.artifactId));
        const byRoleExisting = new Map<string, ArtifactId>();
        for (const o of existingOutputs) {
          if (!byRoleExisting.has(o.role)) byRoleExisting.set(o.role, o.artifactId);
        }

        const artifactsByRole: Record<string, ArtifactId> = {};
        const maxOutputBytes = deps.policy.slurmMaxCollectOutputBytes();
        const maxLogBytes = deps.policy.slurmMaxCollectLogBytes();

        for (const out of slurmOutputs as Array<any>) {
          const role = String(out.role ?? "");
          const srcRel = String(out.src_relpath ?? "");
          const typeStr = String(out.type ?? "");
          const label = String(out.label ?? "");
          if (!role || !srcRel || !typeStr || !label) {
            throw new McpError(ErrorCode.InvalidRequest, `invalid slurm output spec in target canonical params`);
          }

          const already = byRoleExisting.get(role);
          if (already) {
            artifactsByRole[role] = already;
            continue;
          }

          const type = requireArtifactType(typeStr);
          const srcPath = safeJoin(ws.rootDir, srcRel);
          await assertRegularFileNoSymlink(srcPath, `output role=${role}`);

          const artifact = await deps.artifacts.importArtifact({
            projectId: targetRun.projectId,
            source: { kind: "local_path", path: srcPath },
            typeHint: type,
            label,
            createdByRunId: targetRunId,
            maxBytes: maxOutputBytes
          });

          await deps.store.addRunOutput(targetRunId, artifact.artifactId, role);
          byRoleExisting.set(role, artifact.artifactId);
          artifactsByRole[role] = artifact.artifactId;
        }

        const stdoutPath = safeJoin(ws.rootDir, path.join("meta", "stdout.txt"));
        const stderrPath = safeJoin(ws.rootDir, path.join("meta", "stderr.txt"));

        for (const [role, p] of [
          ["stdout", stdoutPath],
          ["stderr", stderrPath]
        ] as const) {
          if (byRoleExisting.has(role)) {
            artifactsByRole[role] = byRoleExisting.get(role)!;
            continue;
          }
          try {
            await assertRegularFileNoSymlink(p, role);
          } catch {
            continue;
          }

          const artifact = await deps.artifacts.importArtifact({
            projectId: targetRun.projectId,
            source: { kind: "local_path", path: p },
            typeHint: "LOG",
            label: `${role}.txt`,
            createdByRunId: targetRunId,
            maxBytes: maxLogBytes
          });
          await deps.store.addRunOutput(targetRunId, artifact.artifactId, role);
          byRoleExisting.set(role, artifact.artifactId);
          artifactsByRole[role] = artifact.artifactId;
        }

        const slurmScript = byRoleExisting.get("slurm_script");
        if (slurmScript) artifactsByRole["slurm_script"] = slurmScript;

        const exitCodePath = safeJoin(ws.rootDir, path.join("meta", "exit_code.txt"));
        await assertRegularFileNoSymlink(exitCodePath, "exit_code");
        const exitCodeRaw = (await fs.readFile(exitCodePath, "utf8")).trim();
        const exitCode = Number.parseInt(exitCodeRaw, 10);
        if (!Number.isInteger(exitCode)) {
          throw new McpError(ErrorCode.InvalidRequest, `invalid exit_code.txt content: ${exitCodeRaw}`);
        }

        const finishedAt = new Date().toISOString();
        const status = exitCode === 0 ? "succeeded" : "failed";
        const error = exitCode === 0 ? null : `slurm job exit_code=${exitCode}`;

        await deps.store.updateRun(targetRunId, {
          status,
          finishedAt,
          exitCode,
          error
        });

        await deps.store.addRunEvent(targetRunId, "slurm.collect.outputs_registered", "outputs registered", {
          artifacts_by_role: artifactsByRole
        });
        await deps.store.addRunEvent(
          targetRunId,
          status === "succeeded" ? "slurm.collect.done" : "slurm.collect.failed",
          status === "succeeded" ? "collect done" : "collect failed",
          { exit_code: exitCode }
        );

        const structured = await toolRun.finishSuccess(
          { target_run_id: targetRunId, exit_code: exitCode, artifacts_by_role: artifactsByRole },
          "slurm collect ok"
        );

        return {
          content: [{ type: "text", text: `slurm collected (target ${targetRunId})` }],
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
