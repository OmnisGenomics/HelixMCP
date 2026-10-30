import { createHash } from "node:crypto";
import { createReadStream, promises as fs } from "node:fs";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import * as z from "zod/v4";
import type { ArtifactService } from "../artifacts/artifactService.js";
import type { ProjectId } from "../core/ids.js";
import type { JsonObject } from "../core/json.js";
import { createRunWorkspace } from "../execution/workspace.js";
import { callStudioBridge } from "./studioBridgeClient.js";
import { zArtifactId, zProvenance, zSha256, zStudioBridgeInfo } from "./toolSchemas.js";
import type { StudioInteractiveRunner } from "./studioReviewTools.js";

export const zPrimeExportInput = z.object({ bridge_file: z.string().min(1).optional() }).strict();
export const zPrimeExportOperation = z.object({
  schema: z.literal("helix.studio.prime_comparison_export/v1"),
  status: z.enum(["idle", "running", "complete", "failed"]),
  selected_run_id: z.string().optional(), previous_run_id: z.string().optional(), error: z.string().optional(),
  receipt: z.object({ schema:z.literal("helix.prime_comparison_export/v1"), path:z.string(),
    sha256:z.string().regex(/^[a-f0-9]{64}$/), size_bytes:z.number().int().nonnegative() }).strict().optional()
}).strict();
export const zPrimeExportOutput = zProvenance.extend({
  bridge:zStudioBridgeInfo, comparison_export:zPrimeExportOperation,
  comparison_artifact_id:zArtifactId, comparison_checksum_sha256:zSha256, log_artifact_id:zArtifactId
});

export function registerStudioPrimeExportTools(mcp: McpServer,
  deps:{artifacts:ArtifactService;runsDir:string;projectId:ProjectId}, runInteractive:StudioInteractiveRunner):void {
  mcp.registerTool("studio_prime_comparison_export", {
    description:"Export the two selected recorded Prime runs as a verified portable ZIP containing JSON, HTML and unchanged source bundles. Uses a new gateway-managed output and registers it as an artifact. Integrity is separate from approval or calibration.",
    inputSchema:zPrimeExportInput,outputSchema:zPrimeExportOutput,
    annotations:{destructiveHint:false,openWorldHint:false,idempotentHint:false}
  },(args,extra)=>runInteractive("studio_prime_comparison_export",{bridge_file:args.bridge_file??null},extra,async(run)=>{
    const workspace=await createRunWorkspace(deps.runsDir,run.runId);
    const dest=workspace.outPath("prime_comparison.v1.zip");
    const options={...(args.bridge_file?{bridgeFile:args.bridge_file}:{}),timeoutMs:30000};
    await run.event("studio.command","prime_comparison_export",{path:dest});
    let raw=await callStudioBridge("prime_comparison_export",{path:dest},options);
    let state=zPrimeExportOperation.parse(raw.comparison_export);
    const deadline=Date.now()+60000;
    while(state.status==="running" && Date.now()<deadline){
      await new Promise(resolve=>setTimeout(resolve,100));
      raw=await callStudioBridge("prime_comparison_export_state",{},options);
      state=zPrimeExportOperation.parse(raw.comparison_export);
    }
    if(state.status!=="complete" || !state.receipt || state.receipt.path!==dest){
      throw new Error(state.error??"Studio did not confirm completion of the requested comparison export");
    }
    const info=await fs.lstat(dest);
    if(!info.isFile() || info.isSymbolicLink() || info.size>128*1024*1024 || info.size!==state.receipt.size_bytes){
      throw new Error("Comparison export is not the expected bounded regular file");
    }
    const digest=createHash("sha256");
    for await(const chunk of createReadStream(dest))digest.update(chunk);
    const checksum="sha256:"+digest.digest("hex");
    if(checksum!=="sha256:"+state.receipt.sha256)throw new Error("Comparison export checksum changed after verification");
    await run.event("studio.comparison_export.verified","Portable comparison verified by Studio",{
      selected_run_id:state.selected_run_id??null,previous_run_id:state.previous_run_id??null,checksum_sha256:checksum
    });
    const artifact=await deps.artifacts.importArtifact({projectId:deps.projectId,source:{kind:"local_path",path:dest},typeHint:"ZIP",
      label:"prime_comparison.v1.zip",createdByRunId:run.runId,maxBytes:BigInt(128*1024*1024)});
    if(artifact.checksumSha256!==checksum)throw new Error("Comparison artifact checksum changed during registration");
    await run.linkOutput(artifact.artifactId,"prime_comparison");
    return {bridge:JSON.parse(JSON.stringify(zStudioBridgeInfo.parse(raw.bridge))),comparison_export:JSON.parse(JSON.stringify(state)),
      comparison_artifact_id:artifact.artifactId,comparison_checksum_sha256:artifact.checksumSha256} as JsonObject;
  }));
}
