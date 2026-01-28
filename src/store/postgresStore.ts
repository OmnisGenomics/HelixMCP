import type { Kysely, Selectable } from "kysely";
import type { ArtifactRecord, ArtifactType } from "../core/artifact.js";
import type { JsonObject } from "../core/json.js";
import type { RunRecord, RunStatus } from "../core/run.js";
import type { ArtifactId, ProjectId, RunId } from "../core/ids.js";
import type { DB } from "../db/types.js";

function toIso(value: unknown): string {
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "string") return value;
  return new Date(String(value)).toISOString();
}

function toIsoOrNull(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  return toIso(value);
}

export class PostgresStore {
  constructor(private readonly db: Kysely<DB>) {}

  async getArtifactListSnapshot(projectId: ProjectId): Promise<{ artifactCount: string; asOfCreatedAt: string | null }> {
    const row = await this.db
      .selectFrom("artifacts")
      .select(({ fn }) => [fn.countAll<string>().as("artifact_count"), fn.max("created_at").as("max_created_at")])
      .where("project_id", "=", projectId)
      .where("type", "!=", "LOG")
      .executeTakeFirstOrThrow();

    return {
      artifactCount: String(row.artifact_count),
      asOfCreatedAt: row.max_created_at ? toIso(row.max_created_at) : null
    };
  }

  async ensureProject(projectId: ProjectId, name: string | null = null): Promise<void> {
    await this.db
      .insertInto("projects")
      .values({ project_id: projectId, name, metadata: {} })
      .onConflict((oc) => oc.column("project_id").doNothing())
      .execute();
  }

  async createArtifact(input: {
    artifactId: ArtifactId;
    projectId: ProjectId;
    type: ArtifactType;
    uri: string;
    mimeType: string;
    sizeBytes: bigint;
    checksumSha256: `sha256:${string}`;
    label: string | null;
    createdByRunId?: RunId | null;
    metadata: JsonObject;
  }): Promise<ArtifactRecord> {
    await this.ensureProject(input.projectId);

    await this.db
      .insertInto("artifacts")
      .values({
        artifact_id: input.artifactId,
        project_id: input.projectId,
        type: input.type,
        uri: input.uri,
        mime_type: input.mimeType,
        size_bytes: input.sizeBytes.toString(),
        checksum_sha256: input.checksumSha256,
        label: input.label,
        created_by_run_id: input.createdByRunId ?? null,
        metadata: input.metadata
      })
      .execute();

    const row = await this.db
      .selectFrom("artifacts")
      .selectAll()
      .where("artifact_id", "=", input.artifactId)
      .executeTakeFirstOrThrow();

    return {
      artifactId: row.artifact_id as ArtifactId,
      projectId: row.project_id as ProjectId,
      type: row.type as ArtifactType,
      uri: row.uri,
      mimeType: row.mime_type,
      sizeBytes: BigInt(row.size_bytes),
      checksumSha256: row.checksum_sha256 as `sha256:${string}`,
      label: row.label,
      createdAt: toIso((row as unknown as { created_at: unknown }).created_at),
      createdByRunId: (row.created_by_run_id as RunId | null) ?? null,
      metadata: (row.metadata ?? {}) as JsonObject
    };
  }

  async getArtifact(artifactId: ArtifactId): Promise<ArtifactRecord | null> {
    const row = await this.db
      .selectFrom("artifacts")
      .selectAll()
      .where("artifact_id", "=", artifactId)
      .executeTakeFirst();

    if (!row) return null;

    return {
      artifactId: row.artifact_id as ArtifactId,
      projectId: row.project_id as ProjectId,
      type: row.type as ArtifactType,
      uri: row.uri,
      mimeType: row.mime_type,
      sizeBytes: BigInt(row.size_bytes),
      checksumSha256: row.checksum_sha256 as `sha256:${string}`,
      label: row.label,
      createdAt: toIso((row as unknown as { created_at: unknown }).created_at),
      createdByRunId: (row.created_by_run_id as RunId | null) ?? null,
      metadata: (row.metadata ?? {}) as JsonObject
    };
  }

  async listArtifacts(projectId: ProjectId, limit: number, asOfCreatedAt?: string | null): Promise<ArtifactRecord[]> {
    if (asOfCreatedAt === null) return [];

    let q = this.db
      .selectFrom("artifacts")
      .selectAll()
      .where("project_id", "=", projectId)
      .where("type", "!=", "LOG");

    if (asOfCreatedAt !== undefined) {
      q = q.where("created_at", "<=", asOfCreatedAt);
    }

    const rows = await q.orderBy("created_at", "desc").orderBy("artifact_id", "desc").limit(limit).execute();

    return rows.map((row) => ({
      artifactId: row.artifact_id as ArtifactId,
      projectId: row.project_id as ProjectId,
      type: row.type as ArtifactType,
      uri: row.uri,
      mimeType: row.mime_type,
      sizeBytes: BigInt(row.size_bytes),
      checksumSha256: row.checksum_sha256 as `sha256:${string}`,
      label: row.label,
      createdAt: toIso((row as unknown as { created_at: unknown }).created_at),
      createdByRunId: (row.created_by_run_id as RunId | null) ?? null,
      metadata: (row.metadata ?? {}) as JsonObject
    }));
  }

  async ensureParamSet(paramsHash: `sha256:${string}`, canonicalParams: JsonObject): Promise<void> {
    await this.db
      .insertInto("param_sets")
      .values({ params_hash: paramsHash, canonical_json: canonicalParams })
      .onConflict((oc) => oc.column("params_hash").doNothing())
      .execute();
  }

  async getParamSet(paramsHash: `sha256:${string}`): Promise<JsonObject | null> {
    const row = await this.db
      .selectFrom("param_sets")
      .select(["canonical_json"])
      .where("params_hash", "=", paramsHash)
      .executeTakeFirst();
    return row ? ((row.canonical_json ?? null) as JsonObject) : null;
  }

  async createRun(input: {
    runId: RunId;
    projectId: ProjectId;
    toolName: string;
    contractVersion: string;
    toolVersion: string | null;
    paramsHash: `sha256:${string}`;
    canonicalParams: JsonObject;
    policyHash: `sha256:${string}`;
    status: RunStatus;
    requestedBy: string | null;
    policySnapshot: JsonObject | null;
    environment: JsonObject | null;
  }): Promise<RunRecord> {
    await this.ensureProject(input.projectId);
    await this.ensureParamSet(input.paramsHash, input.canonicalParams);

    await this.db
      .insertInto("runs")
      .values({
        run_id: input.runId,
        project_id: input.projectId,
        tool_name: input.toolName,
        contract_version: input.contractVersion,
        tool_version: input.toolVersion,
        params_hash: input.paramsHash,
        policy_hash: input.policyHash,
        status: input.status,
        requested_by: input.requestedBy,
        policy_snapshot: input.policySnapshot ?? null,
        environment: input.environment ?? null
      })
      .onConflict((oc) => oc.column("run_id").doNothing())
      .execute();

    const row = await this.db
      .selectFrom("runs")
      .selectAll()
      .where("run_id", "=", input.runId)
      .executeTakeFirstOrThrow();

    return this.mapRun(row);
  }

  async getRun(runId: RunId): Promise<RunRecord | null> {
    const row = await this.db.selectFrom("runs").selectAll().where("run_id", "=", runId).executeTakeFirst();
    return row ? this.mapRun(row) : null;
  }

  async updateRun(
    runId: RunId,
    patch: Partial<Pick<RunRecord, "status" | "startedAt" | "finishedAt" | "exitCode" | "error" | "logArtifactId" | "resultJson">>
  ): Promise<void> {
    const updates: Record<string, unknown> = {};
    if (patch.status) updates.status = patch.status;
    if (patch.startedAt !== undefined) updates.started_at = patch.startedAt;
    if (patch.finishedAt !== undefined) updates.finished_at = patch.finishedAt;
    if (patch.exitCode !== undefined) updates.exit_code = patch.exitCode;
    if (patch.error !== undefined) updates.error = patch.error;
    if (patch.logArtifactId !== undefined) updates.log_artifact_id = patch.logArtifactId;
    if (patch.resultJson !== undefined) updates.result_json = patch.resultJson;

    if (Object.keys(updates).length === 0) return;

    await this.db.updateTable("runs").set(updates).where("run_id", "=", runId).execute();
  }

  async addRunInput(runId: RunId, artifactId: ArtifactId, role: string): Promise<void> {
    await this.db
      .insertInto("run_inputs")
      .values({ run_id: runId, artifact_id: artifactId, role })
      .onConflict((oc) => oc.columns(["run_id", "artifact_id", "role"]).doNothing())
      .execute();
  }

  async addRunOutput(runId: RunId, artifactId: ArtifactId, role: string): Promise<void> {
    await this.db
      .insertInto("run_outputs")
      .values({ run_id: runId, artifact_id: artifactId, role })
      .onConflict((oc) => oc.columns(["run_id", "artifact_id", "role"]).doNothing())
      .execute();
  }

  async listRunInputs(runId: RunId): Promise<Array<{ artifactId: ArtifactId; role: string }>> {
    const rows = await this.db.selectFrom("run_inputs").selectAll().where("run_id", "=", runId).execute();
    return rows.map((r) => ({ artifactId: r.artifact_id as ArtifactId, role: r.role }));
  }

  async listRunOutputs(runId: RunId): Promise<Array<{ artifactId: ArtifactId; role: string }>> {
    const rows = await this.db.selectFrom("run_outputs").selectAll().where("run_id", "=", runId).execute();
    return rows.map((r) => ({ artifactId: r.artifact_id as ArtifactId, role: r.role }));
  }

  async addRunEvent(runId: RunId, kind: string, message: string | null, data: JsonObject | null): Promise<void> {
    await this.db
      .insertInto("run_events")
      .values({
        run_id: runId,
        kind,
        message,
        data: data ?? null
      })
      .execute();
  }

  private mapRun(row: Selectable<DB["runs"]>): RunRecord {
    return {
      runId: row.run_id as RunId,
      projectId: row.project_id as ProjectId,
      toolName: row.tool_name,
      contractVersion: row.contract_version,
      toolVersion: row.tool_version,
      paramsHash: row.params_hash as `sha256:${string}`,
      policyHash: row.policy_hash as `sha256:${string}`,
      status: row.status as RunStatus,
      requestedBy: row.requested_by,
      createdAt: toIso((row as unknown as { created_at: unknown }).created_at),
      startedAt: toIsoOrNull((row as unknown as { started_at: unknown }).started_at),
      finishedAt: toIsoOrNull((row as unknown as { finished_at: unknown }).finished_at),
      policySnapshot: row.policy_snapshot ? (row.policy_snapshot as JsonObject) : null,
      environment: row.environment ? (row.environment as JsonObject) : null,
      exitCode: row.exit_code,
      error: row.error,
      resultJson: row.result_json ? (row.result_json as JsonObject) : null,
      logArtifactId: row.log_artifact_id as ArtifactId | null
    };
  }
}
