import {createHash} from "node:crypto";
import {createReadStream,promises as fs} from "node:fs";
import type {McpServer} from "@modelcontextprotocol/sdk/server/mcp.js";
import * as z from "zod/v4";
import type {ArtifactService} from "../artifacts/artifactService.js";
import type {ProjectId} from "../core/ids.js";
import type {JsonObject} from "../core/json.js";
import {createRunWorkspace} from "../execution/workspace.js";
import {callStudioBridge} from "./studioBridgeClient.js";
import {zArtifactId,zProvenance,zSha256,zStudioBridgeInfo} from "./toolSchemas.js";
import type {StudioInteractiveRunner} from "./studioReviewTools.js";

export const zPrimeValidationInput=z.object({bridge_file:z.string().min(1).optional()}).strict();
export const zPrimeValidationOperation=z.object({
  schema:z.literal("helix.studio.prime_validation_benchmark/v1"),status:z.enum(["idle","running","complete","failed"]),
  error:z.string().optional(),session_unchanged:z.boolean().optional(),
  receipt:z.object({schema:z.literal("helix.prime_validation_export/v1"),path:z.string(),sha256:z.string().regex(/^[a-f0-9]{64}$/),
    size_bytes:z.number().int().nonnegative(),status:z.enum(["PASS","FAIL"]),case_count:z.number().int().min(1).max(512),
    passed_count:z.number().int().nonnegative(),failed_case_ids:z.array(z.string())}).strict().optional()
}).strict();
export const zPrimeValidationOutput=zProvenance.extend({bridge:zStudioBridgeInfo,validation:zPrimeValidationOperation,
  validation_artifact_id:zArtifactId,validation_checksum_sha256:zSha256,log_artifact_id:zArtifactId});

export function registerStudioPrimeValidationTools(mcp:McpServer,
  deps:{artifacts:ArtifactService;runsDir:string;projectId:ProjectId},runInteractive:StudioInteractiveRunner):void{
  mcp.registerTool("studio_prime_validation_benchmark",{
    description:"Run the frozen synthetic Prime sequence-validation suite in live Studio without changing the experiment. Checks an independent token oracle, deterministic replay and historical export verification. Registers a PASS or FAIL evidence ZIP; does not establish biological accuracy, probability calibration or approval.",
    inputSchema:zPrimeValidationInput,outputSchema:zPrimeValidationOutput,
    annotations:{destructiveHint:false,openWorldHint:false,idempotentHint:false}
  },(args,extra)=>runInteractive("studio_prime_validation_benchmark",{bridge_file:args.bridge_file??null},extra,async(run)=>{
    const workspace=await createRunWorkspace(deps.runsDir,run.runId),dest=workspace.outPath("prime_validation.v1.zip");
    const options={...(args.bridge_file?{bridgeFile:args.bridge_file}:{}),timeoutMs:30000};
    await run.event("studio.command","prime_validation_benchmark",{path:dest});
    let raw=await callStudioBridge("prime_validation_benchmark",{path:dest},options);
    let state=zPrimeValidationOperation.parse(raw.prime_validation);const deadline=Date.now()+120000;
    while(state.status==="running"&&Date.now()<deadline){
      await new Promise(r=>setTimeout(r,100));raw=await callStudioBridge("prime_validation_benchmark_state",{},options);
      state=zPrimeValidationOperation.parse(raw.prime_validation);
    }
    if(state.status!=="complete"||!state.receipt||state.receipt.path!==dest)throw new Error(state.error??"Studio did not complete the benchmark");
    const info=await fs.lstat(dest);
    if(!info.isFile()||info.isSymbolicLink()||info.size>64*1024*1024||info.size!==state.receipt.size_bytes)throw new Error("Unexpected validation archive");
    const hash=createHash("sha256");for await(const chunk of createReadStream(dest))hash.update(chunk);
    const checksum="sha256:"+hash.digest("hex");if(checksum!=="sha256:"+state.receipt.sha256)throw new Error("Validation checksum changed");
    const artifact=await deps.artifacts.importArtifact({projectId:deps.projectId,source:{kind:"local_path",path:dest},typeHint:"ZIP",
      label:"prime_validation.v1.zip",createdByRunId:run.runId,maxBytes:BigInt(64*1024*1024)});
    if(artifact.checksumSha256!==checksum)throw new Error("Validation artifact changed during registration");
    await run.linkOutput(artifact.artifactId,"prime_validation");
    await run.event("studio.prime_validation.complete",state.receipt.status,{checksum_sha256:checksum,failed_case_ids:state.receipt.failed_case_ids});
    // A failed benchmark is preserved as evidence, not discarded as a transport error.
    return {bridge:JSON.parse(JSON.stringify(zStudioBridgeInfo.parse(raw.bridge))),validation:JSON.parse(JSON.stringify(state)),
      validation_artifact_id:artifact.artifactId,validation_checksum_sha256:artifact.checksumSha256} as JsonObject;
  }));
}
