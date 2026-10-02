import * as z from "zod/v4";
import { zArtifactId, zProvenance, zSha256, zStudioBridgeInfo } from "./toolSchemas.js";

const nonblank = z.string().min(1).regex(/\S/);
const uniqueStrings = z.array(nonblank).refine((items) => new Set(items).size === items.length, "duplicate values");
const task = z.object({
  status: z.enum(["completed", "blocked", "skipped"]),
  assistance_needed: z.boolean(),
  notes: nonblank
}).strict();

// Matches Helix's existing workflow_answers_v1 contract; no invented observations.
export const zWorkflowAnswers = z.object({
  schema: z.literal("helix.workflow_evaluation.answers.v1"),
  tasks: z.object({
    compare_candidates: task, inspect_assumptions: task,
    explain_decision: task, verify_package: task
  }).strict(),
  selected_candidate_id: nonblank.nullable(),
  alternatives_considered: uniqueStrings,
  decision_rationale: z.string(),
  assumptions: uniqueStrings,
  change_explanation: nonblank.nullable(),
  intent_to_export_seconds: z.number().nonnegative().nullable(),
  manual_handoffs_removed: z.number().int().nonnegative().nullable(),
  return_visit: z.boolean(),
  would_use_again: z.boolean().nullable()
}).strict();

export const zStudioPilotReviewState = z.object({
  visible: z.boolean(), status: z.string(), integrity_verified: z.boolean(),
  candidate_count: z.number().int().nonnegative(),
  manifest_sha256: z.string().regex(/^[a-f0-9]{64}$/).nullable(),
  observation_state: z.enum(["pending", "started", "finished"]),
  export_enabled: z.boolean(), exported_path: z.string().nullable()
}).strict();

export const zStudioPilotReviewStateInput = z.object({
  bridge_file: z.string().min(1).optional()
}).strict();
export const zStudioPilotReviewOutput = zProvenance.extend({
  bridge: zStudioBridgeInfo, pilot_review: zStudioPilotReviewState,
  log_artifact_id: zArtifactId
});
export const zStudioPilotReviewOpenInput = zStudioPilotReviewStateInput.extend({
  package_artifact_id: zArtifactId, baseline_artifact_id: zArtifactId.optional()
});
export const zStudioPilotReviewStartInput = zStudioPilotReviewStateInput.extend({
  study_id: z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$/),
  participant_id: z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$/),
  role: z.enum(["reviewer", "author"]).default("reviewer")
});
export const zStudioPilotReviewFillInput = zStudioPilotReviewStateInput.extend({ answers: zWorkflowAnswers });
export const zStudioPilotReviewViewInput = zStudioPilotReviewStateInput.extend({
  tab_index: z.number().int().min(0).max(3).default(0),
  width: z.number().int().min(680).max(7680).optional(),
  height: z.number().int().min(480).max(4320).optional(),
  scroll_y: z.number().int().nonnegative().optional()
}).refine((value) => (value.width === undefined) === (value.height === undefined), "provide both width and height");
export const zStudioPilotReviewExportOutput = zStudioPilotReviewOutput.extend({
  observation_artifact_id: zArtifactId, observation_checksum_sha256: zSha256
});
