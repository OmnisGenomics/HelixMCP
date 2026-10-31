import * as z from "zod/v4";
import type {McpServer} from "@modelcontextprotocol/sdk/server/mcp.js";
import type {JsonObject} from "../core/json.js";
import {callStudioBridge} from "./studioBridgeClient.js";
import {zArtifactId,zStudioBridgeInfo,zProvenance} from "./toolSchemas.js";
import type {StudioInteractiveRunner} from "./studioReviewTools.js";

export const zPrimeAssistantInput=z.object({
  bridge_file:z.string().min(1).optional(),action:z.enum(["generate","select","get_state"]),
  pbs_lengths:z.array(z.number().int().min(1).max(100)).min(1).max(16).optional(),
  rtt_lengths:z.array(z.number().int().min(1).max(512)).min(1).max(16).optional(),
  candidate_id:z.string().regex(/^primecand_[a-f0-9]{64}$/).optional()
}).strict();
export const zPrimeAssistantState=z.object({
  schema:z.literal("helix.studio.prime_design_assistant/v1"),status:z.enum(["idle","running","ready","empty","error"]),
  message:z.string(),report:z.record(z.string(),z.unknown()).nullable(),selection:z.record(z.string(),z.unknown()).nullable()
}).strict();
export const zPrimeAssistantOutput=zProvenance.extend({bridge:zStudioBridgeInfo,design_assistant:zPrimeAssistantState,log_artifact_id:zArtifactId});

export function registerStudioPrimeAssistantTools(mcp:McpServer,runInteractive:StudioInteractiveRunner):void{
  mcp.registerTool("studio_prime_design_assistant",{
    description:"Use the actual Prime wizard assistant: generate compatible candidates from the retained target and desired edit with explicit length choices, inspect its search, or explicitly select a candidate to populate editable wizard sequences. Deterministic positional ordering, not a success ranking. Synthetic in-silico modeling only; no calibration or approval claim.",
    inputSchema:zPrimeAssistantInput,outputSchema:zPrimeAssistantOutput,
    annotations:{openWorldHint:false,destructiveHint:false,idempotentHint:false}
  },(args,extra)=>runInteractive("studio_prime_design_assistant",JSON.parse(JSON.stringify(args)) as JsonObject,extra,async(run)=>{
    const params:JsonObject={};
    if(args.action==="generate"){
      if(!args.pbs_lengths||!args.rtt_lengths||args.candidate_id!==undefined)throw new Error("Generation requires only explicit PBS and RTT length lists");
      params.pbs_lengths=args.pbs_lengths;params.rtt_lengths=args.rtt_lengths;
    }else if(args.action==="select"){
      if(!args.candidate_id||args.pbs_lengths!==undefined||args.rtt_lengths!==undefined)throw new Error("Selection requires only a current candidate_id");
      params.candidate_id=args.candidate_id;
    }else if(args.candidate_id!==undefined||args.pbs_lengths!==undefined||args.rtt_lengths!==undefined)throw new Error("State inspection accepts no design overrides");
    const command="prime_assistant_"+args.action;
    await run.event("studio.command",command,params);
    const raw=await callStudioBridge(command,params,{...(args.bridge_file?{bridgeFile:args.bridge_file}:{}),timeoutMs:30000});
    return {bridge:JSON.parse(JSON.stringify(zStudioBridgeInfo.parse(raw.bridge))),
      design_assistant:JSON.parse(JSON.stringify(zPrimeAssistantState.parse(raw.design_assistant)))} as JsonObject;
  }));
}
