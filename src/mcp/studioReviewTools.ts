import { createReadStream } from "node:fs";
import { createHash } from "node:crypto";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { ErrorCode, McpError } from "@modelcontextprotocol/sdk/types.js";
import type { ArtifactService } from "../artifacts/artifactService.js";
import type { ArtifactId, ProjectId } from "../core/ids.js";
import type { JsonObject } from "../core/json.js";
import { createRunWorkspace } from "../execution/workspace.js";
import type { ToolRun } from "../runs/toolRun.js";
import { callStudioBridge } from "./studioBridgeClient.js";
import { zStudioBridgeInfo } from "./toolSchemas.js";
import {
  zStudioPilotReviewExportOutput, zStudioPilotReviewFillInput,
  zStudioPilotReviewOpenInput, zStudioPilotReviewOutput,
  zStudioPilotReviewStartInput, zStudioPilotReviewState, zStudioPilotReviewStateInput,
  zStudioPilotReviewViewInput
} from "./studioReviewSchemas.js";

export type StudioToolExtra = {
  authInfo?: { clientId: string; extra?: Record<string, unknown> } | undefined;
  sessionId?: string | undefined;
};
export type StudioInteractiveRunner = (
  name: string, params: JsonObject, extra: StudioToolExtra,
  execute: (run: ToolRun) => Promise<JsonObject>
) => Promise<{ content: Array<{ type: "text"; text: string }>; structuredContent: JsonObject }>;

export function registerStudioReviewTools(
  mcp: McpServer,
  deps: { artifacts: ArtifactService; runsDir: string; projectId: ProjectId },
  runInteractive: StudioInteractiveRunner
): void {
  const options = (bridgeFile?: string) => bridgeFile
    ? { bridgeFile, timeoutMs: 30_000 } : { timeoutMs: 30_000 };
  const json = (value: unknown): JsonObject => JSON.parse(JSON.stringify(value)) as JsonObject;
  const output = (raw: JsonObject): JsonObject => ({
    bridge: json(zStudioBridgeInfo.parse(raw.bridge)),
    pilot_review: json(zStudioPilotReviewState.parse(raw.pilot_review))
  });
  const command = (name: string, bridgeCommand: string, params: JsonObject, bridgeFile: string | undefined, extra: StudioToolExtra) =>
    runInteractive(name, { bridge_file: bridgeFile ?? null, ...params }, extra, async (run) => {
      await run.event("studio.command", bridgeCommand, params);
      return output(await callStudioBridge(bridgeCommand, params, options(bridgeFile)));
    });
  const local = { openWorldHint: false, destructiveHint: false, idempotentHint: false };

  mcp.registerTool("studio_pilot_review_get_state", {
    description: "Inspect the existing pilot reviewer window without opening it. Workflow observation is separate from approval; in silico only.",
    inputSchema: zStudioPilotReviewStateInput, outputSchema: zStudioPilotReviewOutput,
    annotations: { ...local, readOnlyHint: true }
  }, (args, extra) => command("studio_pilot_review_get_state", "pilot_review_state", {}, args.bridge_file, extra));

  mcp.registerTool("studio_pilot_review_open", {
    description: "Open a previously imported ZIP artifact in live Studio. Verify package integrity and inspect modeled candidates, alternatives, assumptions and limits. No human task is auto-completed.",
    inputSchema: zStudioPilotReviewOpenInput, outputSchema: zStudioPilotReviewOutput,
    annotations: { ...local, readOnlyHint: false }
  }, (args, extra) => runInteractive("studio_pilot_review_open", json(args), extra, async (run) => {
    const workspace = await createRunWorkspace(deps.runsDir, run.runId);
    async function materialize(id: string, filename: string, role: string): Promise<string> {
      const artifact = await deps.artifacts.getArtifact(id as ArtifactId);
      if (!artifact || artifact.type !== "ZIP") {
        throw new McpError(ErrorCode.InvalidParams, "pilot review requires an imported ZIP artifact");
      }
      const dest = workspace.inPath(filename);
      await deps.artifacts.materializeToPath(artifact.artifactId, dest);
      const digest = createHash("sha256");
      for await (const chunk of createReadStream(dest)) digest.update(chunk);
      if ("sha256:" + digest.digest("hex") !== artifact.checksumSha256) {
        throw new McpError(ErrorCode.InvalidRequest, "pilot ZIP artifact checksum mismatch");
      }
      await run.linkInput(artifact.artifactId, role);
      return dest;
    }
    const params: JsonObject = { package: await materialize(args.package_artifact_id, "pilot_package.zip", "package") };
    if (args.baseline_artifact_id) params.baseline = await materialize(args.baseline_artifact_id, "baseline.zip", "baseline");
    await run.event("studio.command", "pilot_review_open", params);
    return output(await callStudioBridge("pilot_review_open", params, options(args.bridge_file)));
  }));

  mcp.registerTool("studio_pilot_review_start", {
    description: "Start actual observation timing and bind the verified manifests. Use anonymous codes. For GUI tests, use an explicitly automated study and participant code.",
    inputSchema: zStudioPilotReviewStartInput, outputSchema: zStudioPilotReviewOutput,
    annotations: { ...local, readOnlyHint: false }
  }, (args, extra) => command("studio_pilot_review_start", "pilot_review_start", {
    study_id: args.study_id, participant_id: args.participant_id, role: args.role
  }, args.bridge_file, extra));

  mcp.registerTool("studio_pilot_review_fill", {
    description: "Populate the actual review form with v1 workflow answers. Record observed completed, blocked or skipped tasks and rationale. Automated answers must say they are fixtures, not human observations.",
    inputSchema: zStudioPilotReviewFillInput, outputSchema: zStudioPilotReviewOutput,
    annotations: { ...local, readOnlyHint: false }
  }, (args, extra) => command("studio_pilot_review_fill", "pilot_review_fill", { answers: json(args.answers) }, args.bridge_file, extra));

  mcp.registerTool("studio_pilot_review_view", {
    description: "Show reviewer tab 0 comparison, 1 verified source, 2 prior package, or 3 observation. Resize or scroll the actual window for screenshot checks.",
    inputSchema: zStudioPilotReviewViewInput, outputSchema: zStudioPilotReviewOutput,
    annotations: { ...local, readOnlyHint: false }
  }, (args, extra) => {
    const { bridge_file, ...params } = args;
    return command("studio_pilot_review_view", "pilot_review_view", json(params), bridge_file, extra);
  });

  mcp.registerTool("studio_pilot_review_export", {
    description: "Reverify the bound packages, finish the form observation and register its JSON as an artifact. Writes a new gateway-managed output, never inside the package. This is workflow evidence, not decision approval.",
    inputSchema: zStudioPilotReviewStateInput, outputSchema: zStudioPilotReviewExportOutput,
    annotations: { ...local, readOnlyHint: false }
  }, (args, extra) => runInteractive("studio_pilot_review_export", json(args), extra, async (run) => {
    const workspace = await createRunWorkspace(deps.runsDir, run.runId);
    const dest = workspace.outPath("workflow_observation.v1.json");
    await run.event("studio.command", "pilot_review_export", { path: dest });
    const raw = await callStudioBridge("pilot_review_export", { path: dest }, options(args.bridge_file));
    const state = zStudioPilotReviewState.parse(raw.pilot_review);
    if (state.observation_state !== "finished" || state.exported_path !== dest) {
      throw new Error("Studio did not confirm the requested observation export");
    }
    const artifact = await deps.artifacts.importArtifact({
      projectId: deps.projectId, source: { kind: "local_path", path: dest },
      typeHint: "JSON", label: "workflow_observation.v1.json", createdByRunId: run.runId,
      maxBytes: null
    });
    await run.linkOutput(artifact.artifactId, "observation");
    return { ...output(raw), observation_artifact_id: artifact.artifactId, observation_checksum_sha256: artifact.checksumSha256 };
  }));
}
