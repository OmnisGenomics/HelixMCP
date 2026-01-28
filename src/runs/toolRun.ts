import { ErrorCode, McpError } from "@modelcontextprotocol/sdk/types.js";
import type { ArtifactType } from "../core/artifact.js";
import type { ArtifactId, ProjectId, RunId } from "../core/ids.js";
import type { JsonObject } from "../core/json.js";
import type { RunStatus } from "../core/run.js";
import type { ArtifactService } from "../artifacts/artifactService.js";
import type { PostgresStore } from "../store/postgresStore.js";

export class ToolRun {
  readonly runId: RunId;
  private readonly logLines: string[] = [];

  constructor(
    private readonly deps: {
      store: PostgresStore;
      artifacts: ArtifactService;
    },
    private readonly info: {
      runId: RunId;
      projectId: ProjectId;
      toolName: string;
      contractVersion: string;
      toolVersion: string;
      paramsHash: `sha256:${string}`;
      canonicalParams: JsonObject;
      policyHash: `sha256:${string}`;
      requestedBy: string | null;
      policySnapshot: JsonObject | null;
      environment: JsonObject | null;
    }
  ) {
    this.runId = info.runId;
  }

  async start(initialStatus: Extract<RunStatus, "queued" | "running"> = "running"): Promise<void> {
    await this.deps.store.createRun({
      runId: this.runId,
      projectId: this.info.projectId,
      toolName: this.info.toolName,
      contractVersion: this.info.contractVersion,
      toolVersion: this.info.toolVersion,
      paramsHash: this.info.paramsHash,
      canonicalParams: this.info.canonicalParams,
      policyHash: this.info.policyHash,
      status: initialStatus,
      requestedBy: this.info.requestedBy,
      policySnapshot: this.info.policySnapshot,
      environment: this.info.environment
    });

    const now = new Date().toISOString();
    await this.deps.store.updateRun(this.runId, { startedAt: now });
    await this.event("run.started", `tool=${this.info.toolName}`, {
      now,
      params_hash: this.info.paramsHash,
      policy_hash: this.info.policyHash
    });
  }

  async event(kind: string, message: string, data: JsonObject | null): Promise<void> {
    const line = JSON.stringify({ ts: new Date().toISOString(), kind, message, data });
    this.logLines.push(line);
    await this.deps.store.addRunEvent(this.runId, kind, message, data);
  }

  async linkInput(artifactId: ArtifactId, role: string): Promise<void> {
    await this.deps.store.addRunInput(this.runId, artifactId, role);
  }

  async linkOutput(artifactId: ArtifactId, role: string): Promise<void> {
    await this.deps.store.addRunOutput(this.runId, artifactId, role);
  }

  async createOutputArtifact(input: { type: ArtifactType; label: string; contentText: string; role: string }): Promise<ArtifactId> {
    const artifact = await this.deps.artifacts.importArtifact({
      projectId: this.info.projectId,
      source: { kind: "inline_text", text: input.contentText },
      typeHint: input.type,
      label: input.label,
      createdByRunId: this.runId,
      maxBytes: null
    });

    await this.linkOutput(artifact.artifactId, input.role);
    return artifact.artifactId;
  }

  async finishSuccess(result: JsonObject, summary: string): Promise<JsonObject> {
    return this.finish("succeeded", null, summary, result);
  }

  async checkpointQueued(result: JsonObject, summary: string): Promise<JsonObject> {
    return this.checkpoint("queued", summary, result);
  }

  async finishBlocked(reason: string): Promise<void> {
    await this.finish("blocked", reason, `blocked: ${reason}`, null);
  }

  async finishFailure(errorMessage: string): Promise<void> {
    await this.finish("failed", errorMessage, `failed: ${errorMessage}`, null);
  }

  private async checkpoint(status: Extract<RunStatus, "queued" | "running">, summary: string, result: JsonObject): Promise<JsonObject> {
    await this.event(`run.${status}`, summary, null);

    const logArtifactId = await this.createOutputArtifact({
      type: "LOG",
      label: `${this.info.toolName}.log`,
      contentText: this.logLines.join("\n") + "\n",
      role: "log"
    });

    const resultWithProvenance: JsonObject = {
      ...result,
      provenance_run_id: this.runId,
      log_artifact_id: logArtifactId
    };

    await this.deps.store.updateRun(this.runId, {
      status,
      error: null,
      logArtifactId,
      resultJson: resultWithProvenance
    });

    return resultWithProvenance;
  }

  private async finish(status: RunStatus, error: string | null, finalMessage: string, result: JsonObject | null): Promise<JsonObject> {
    await this.event(`run.${status}`, finalMessage, error ? { error } : null);

    const logArtifactId = await this.createOutputArtifact({
      type: "LOG",
      label: `${this.info.toolName}.log`,
      contentText: this.logLines.join("\n") + "\n",
      role: "log"
    });

    const now = new Date().toISOString();
    const resultWithProvenance: JsonObject | null = result
      ? {
          ...result,
          provenance_run_id: this.runId,
          log_artifact_id: logArtifactId
        }
      : null;

    await this.deps.store.updateRun(this.runId, {
      status,
      finishedAt: now,
      error,
      logArtifactId,
      resultJson: resultWithProvenance
    });

    if (!resultWithProvenance) {
      return {
        provenance_run_id: this.runId,
        log_artifact_id: logArtifactId
      };
    }
    return resultWithProvenance;
  }
}

export function requestedByFromExtra(extra: {
  authInfo?: { clientId: string; extra?: Record<string, unknown> } | undefined;
  sessionId?: string | undefined;
}): string | null {
  const extraObj = extra.authInfo?.extra;
  const maybeSubject =
    typeof extraObj === "object" &&
    extraObj !== null &&
    typeof (extraObj as Record<string, unknown>)["subject"] === "string"
      ? ((extraObj as Record<string, unknown>)["subject"] as string)
      : null;

  return maybeSubject ?? extra.authInfo?.clientId ?? extra.sessionId ?? null;
}

export function requireProjectId(projectId: ProjectId | null, context: string): ProjectId {
  if (!projectId) {
    throw new McpError(ErrorCode.InvalidParams, `missing project context for ${context}`);
  }
  return projectId;
}
