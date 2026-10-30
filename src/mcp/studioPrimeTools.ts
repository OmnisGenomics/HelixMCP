import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import * as z from "zod/v4";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { ArtifactService } from "../artifacts/artifactService.js";
import type { ArtifactId } from "../core/ids.js";
import type { JsonObject } from "../core/json.js";
import { createRunWorkspace } from "../execution/workspace.js";
import { callStudioBridge } from "./studioBridgeClient.js";
import { zArtifactId, zStudioBridgeInfo, zProvenance } from "./toolSchemas.js";
import type { StudioInteractiveRunner } from "./studioReviewTools.js";

const field = z.string().max(10000);
export const zStudioPrimeInput = z.object({
  bridge_file: z.string().min(1).optional(),
  action: z.enum(["get_state", "wizard_open", "wizard_target", "wizard_fill", "wizard_navigate", "wizard_commit", "run", "evidence"]),
  mode: z.enum(["new", "edit"]).optional(),
  target_artifact_id: zArtifactId.optional(),
  synthetic: z.literal(true).optional(),
  stage: z.number().int().min(0).max(3).optional(),
  inputs: z.object({ spacer: field.optional(), pbs: field.optional(), rtt: field.optional(),
    offset: field.optional(), reference: field.optional(), alternative: field.optional(),
    edit_kind: z.enum(["SUBSTITUTION", "INSERTION", "DELETION", "REPLACEMENT"]).optional(), inserted_sequence: field.optional(),
    deletion_length: field.optional(), replacement_length: field.optional(), replacement_sequence: field.optional() }).strict().optional()
}).strict();
const zPrimeResult = z.object({
  bridge: zStudioBridgeInfo,
  prime: z.object({ schema: z.literal("helix.studio.prime_automation/v1"),
    wizard: z.record(z.string(), z.unknown()).nullable(),
    run_id: z.number().int(), run_in_progress: z.boolean(),
    prediction: z.record(z.string(), z.unknown()).nullable(), previous_design: z.boolean(),
    intent_id: z.string().nullable(), bundle: z.string().nullable(), verification: z.string()
  }).strict()
}).strict();
export const zStudioPrimeOutput = zProvenance.extend({ ...zPrimeResult.shape, log_artifact_id: zArtifactId });

export function registerStudioPrimeTools(mcp: McpServer,
  deps: { artifacts: ArtifactService; runsDir: string }, runInteractive: StudioInteractiveRunner): void {
  mcp.registerTool("studio_prime_workflow", {
    description: "Drive the actual Prime experiment wizard, run its draft, inspect RTT compatibility and verify evidence in live Studio. Synthetic in-silico fixtures only; no approval or biological accuracy claim.",
    inputSchema: zStudioPrimeInput, outputSchema: zStudioPrimeOutput,
    annotations: { openWorldHint: false, destructiveHint: false, idempotentHint: false }
  }, (args, extra) => runInteractive("studio_prime_workflow", JSON.parse(JSON.stringify(args)) as JsonObject, extra, async (run) => {
    const params: JsonObject = {};
    if (args.action === "wizard_open") params.mode = args.mode ?? "edit";
    if (args.action === "wizard_fill") {
      if (!args.inputs) throw new Error("inputs are required");
      params.inputs = JSON.parse(JSON.stringify(args.inputs)) as JsonObject;
    }
    if (args.action === "wizard_navigate") {
      if (args.stage === undefined) throw new Error("stage is required");
      params.stage = args.stage;
    }
    if (args.action === "wizard_target") {
      if (!args.target_artifact_id || args.synthetic !== true) throw new Error("An explicitly synthetic target artifact is required");
      const artifact = await deps.artifacts.getArtifact(args.target_artifact_id as ArtifactId);
      if (!artifact || !["TEXT", "UNKNOWN"].includes(artifact.type) || artifact.sizeBytes > 100000) {
        throw new Error("Target fixture must be a small text/FASTA artifact");
      }
      const workspace = await createRunWorkspace(deps.runsDir, run.runId);
      const file = workspace.inPath("synthetic_target.fa");
      await deps.artifacts.materializeToPath(artifact.artifactId, file);
      const hash = createHash("sha256");
      for await (const chunk of createReadStream(file)) hash.update(chunk);
      if ("sha256:" + hash.digest("hex") !== artifact.checksumSha256) throw new Error("Target artifact checksum mismatch");
      await run.linkInput(artifact.artifactId, "synthetic_target");
      params.path = file; params.synthetic = true;
    }
    const command = "prime_" + args.action;
    await run.event("studio.command", command, params);
    const raw = await callStudioBridge(command, params,
      { ...(args.bridge_file ? { bridgeFile: args.bridge_file } : {}), timeoutMs: 30000 });
    return JSON.parse(JSON.stringify(zPrimeResult.parse({ bridge: raw.bridge, prime: raw.prime }))) as JsonObject;
  }));
}
