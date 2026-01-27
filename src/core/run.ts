import type { ArtifactId, ProjectId, RunId } from "./ids.js";
import type { JsonObject } from "./json.js";

export type RunStatus = "queued" | "running" | "succeeded" | "failed" | "blocked";

export interface RunRecord {
  runId: RunId;
  projectId: ProjectId;
  toolName: string;
  contractVersion: string;
  toolVersion: string | null;
  paramsHash: `sha256:${string}`;
  policyHash: `sha256:${string}`;
  status: RunStatus;
  requestedBy: string | null;
  createdAt: string;
  startedAt: string | null;
  finishedAt: string | null;
  policySnapshot: JsonObject | null;
  environment: JsonObject | null;
  exitCode: number | null;
  error: string | null;
  resultJson: JsonObject | null;
  logArtifactId: ArtifactId | null;
}
