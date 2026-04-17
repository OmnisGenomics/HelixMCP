import type { ArtifactId, ProjectId, RunId } from "./ids.js";
import type { JsonObject } from "./json.js";

export const RUN_STATUSES = ["queued", "running", "succeeded", "failed", "blocked"] as const;

export type RunStatus = (typeof RUN_STATUSES)[number];

export const RUN_TERMINAL_STATUSES = ["succeeded", "failed", "blocked"] as const satisfies readonly RunStatus[];

export const RUN_STATUS_TRANSITIONS: Record<RunStatus, readonly RunStatus[]> = {
  queued: ["queued", "running", "succeeded", "failed", "blocked"],
  running: ["running", "succeeded", "failed", "blocked"],
  succeeded: ["succeeded"],
  failed: ["failed"],
  blocked: ["blocked"]
};

export function canTransitionRunStatus(from: RunStatus, to: RunStatus): boolean {
  return RUN_STATUS_TRANSITIONS[from].includes(to);
}

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
