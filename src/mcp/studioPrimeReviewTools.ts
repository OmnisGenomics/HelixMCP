import * as z from "zod/v4";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { JsonObject } from "../core/json.js";
import { callStudioBridge } from "./studioBridgeClient.js";
import { zArtifactId, zStudioBridgeInfo, zProvenance } from "./toolSchemas.js";
import type { StudioInteractiveRunner } from "./studioReviewTools.js";

const runId = z.string().min(1).max(160).regex(/^[A-Za-z0-9_.-]+$/);
const zAlignment = z.object({
  coordinate_system: z.literal("declared_reference_boundary_alignment/v1"),
  columns: z.array(z.union([
    z.object({kind:z.literal("reference"), offset:z.number().int().nonnegative()}).strict(),
    z.object({kind:z.literal("insertion"), boundary:z.number().int().nonnegative(), index:z.number().int().nonnegative()}).strict()
  ])),
  tracks:z.object({Reference:z.string(),Selected:z.string().nullable(),Previous:z.string().nullable()}).strict()
}).strict();
export const zPrimeReviewInput = z.object({
  bridge_file: z.string().min(1).optional(), action: z.enum(["open", "select", "get_state"]),
  run_id: runId.optional(), compare_run_id: runId.nullable().optional(), full_sequence: z.boolean().optional(),
  scroll_y: z.number().int().nonnegative().optional()
}).strict();
export const zPrimeReviewState = z.object({
  schema: z.enum(["helix.studio.prime_results_review/v1", "helix.studio.prime_results_review/v2"]), status: z.enum(["empty", "ready", "error"]),
  visible: z.boolean(), run_ids: z.array(z.string()), selected_run_id: z.string().nullable(),
  compare_run_id: z.string().nullable(), previous_design: z.boolean(), full_sequence: z.boolean(),
  scroll_y: z.number().int().nonnegative(), scroll_max: z.number().int().nonnegative(),
  comparison: z.object({ same_reference: z.boolean(), sequence_comparable: z.boolean(),
    different_offsets: z.array(z.number().int().nonnegative()).nullable(), reasons: z.array(z.string()), model_changed: z.boolean(),
    edited_sequences_equal:z.boolean().nullable().optional(), edit_definitions_equal:z.boolean().nullable().optional(),
    alignment_difference_columns:z.array(z.number().int().nonnegative()).nullable().optional(),
    alignment:zAlignment.nullable().optional()
  }).strict().nullable().optional(), categories: z.array(z.array(z.string())).optional(),
  selected_compatibility: z.record(z.string(), z.unknown()).nullable().optional(),
  previous_compatibility: z.record(z.string(), z.unknown()).nullable().optional(),
  alignment: z.string().optional(), integrity: z.string().optional(), error: z.string().optional()
}).strict();
export const zPrimeReviewOutput = zProvenance.extend({
  bridge: zStudioBridgeInfo, prime_review: zPrimeReviewState, log_artifact_id: zArtifactId
});

export function registerStudioPrimeReviewTools(mcp: McpServer, runInteractive: StudioInteractiveRunner): void {
  mcp.registerTool("studio_prime_results_review", {
    description: "Open Prime Results Review, select registered execution IDs for comparison, or inspect its displayed sequence alignment and compatibility. Rechecks bundle integrity on open/select. Read-only evidence review; never changes source, run results or approval.",
    inputSchema: zPrimeReviewInput, outputSchema: zPrimeReviewOutput,
    annotations: { destructiveHint: false, openWorldHint: false, idempotentHint: false }
  }, (args, extra) => runInteractive("studio_prime_results_review", JSON.parse(JSON.stringify(args)) as JsonObject, extra, async (run) => {
    const command = "prime_review_" + args.action;
    const params: JsonObject = {};
    if (args.run_id !== undefined) params.run_id = args.run_id;
    if (args.compare_run_id !== undefined) params.compare_run_id = args.compare_run_id;
    if (args.full_sequence !== undefined) params.full_sequence = args.full_sequence;
    if (args.scroll_y !== undefined) params.scroll_y = args.scroll_y;
    await run.event("studio.command", command, params);
    const raw = await callStudioBridge(command, params, { ...(args.bridge_file ? { bridgeFile: args.bridge_file } : {}), timeoutMs: 30000 });
    return { bridge: JSON.parse(JSON.stringify(zStudioBridgeInfo.parse(raw.bridge))),
      prime_review: JSON.parse(JSON.stringify(zPrimeReviewState.parse(raw.prime_review))) } as JsonObject;
  }));
}
