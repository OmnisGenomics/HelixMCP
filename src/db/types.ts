import type { ColumnType, Generated, JSONColumnType } from "kysely";

type OptionalNullable<T> = ColumnType<T | null, T | null | undefined, T | null>;
type JsonObject = Record<string, unknown>;
type Json = JSONColumnType<JsonObject, JsonObject, JsonObject>;
type JsonNullable = JSONColumnType<JsonObject | null, JsonObject | null | undefined, JsonObject | null>;

export interface ProjectsTable {
  project_id: string;
  name: OptionalNullable<string>;
  created_at: Generated<string>;
  metadata: Json;
}

export interface ArtifactsTable {
  artifact_id: string;
  project_id: string;
  type: string;
  uri: string;
  mime_type: string;
  size_bytes: string; // pg returns bigint as string by default
  checksum_sha256: string;
  label: OptionalNullable<string>;
  created_by_run_id: OptionalNullable<string>;
  created_at: Generated<string>;
  metadata: Json;
}

export interface ParamSetsTable {
  params_hash: string;
  canonical_json: Json;
  created_at: Generated<string>;
}

export interface RunsTable {
  run_id: string;
  project_id: string;
  tool_name: string;
  contract_version: string;
  tool_version: OptionalNullable<string>;
  params_hash: string;
  policy_hash: string;
  status: string;
  requested_by: OptionalNullable<string>;
  created_at: Generated<string>;
  started_at: OptionalNullable<string>;
  finished_at: OptionalNullable<string>;
  policy_snapshot: JsonNullable;
  environment: JsonNullable;
  exit_code: ColumnType<number | null, number | null | undefined, number | null>;
  error: OptionalNullable<string>;
  result_json: JsonNullable;
  log_artifact_id: OptionalNullable<string>;
}

export interface RunArtifactsTable {
  run_id: string;
  artifact_id: string;
  role: string;
}

export interface RunEventsTable {
  event_id: Generated<string>;
  run_id: string;
  ts: Generated<string>;
  kind: string;
  message: OptionalNullable<string>;
  data: JsonNullable;
}

export interface DB {
  projects: ProjectsTable;
  artifacts: ArtifactsTable;
  param_sets: ParamSetsTable;
  runs: RunsTable;
  run_inputs: RunArtifactsTable;
  run_outputs: RunArtifactsTable;
  run_events: RunEventsTable;
}
