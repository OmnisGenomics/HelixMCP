import {it,expect} from "vitest";
import {readFile} from "node:fs/promises";
import {Ajv2020} from "ajv/dist/2020.js";
import {zPrimeAssistantInput,zPrimeAssistantOutput} from "../src/mcp/studioPrimeAssistantTools.js";

it("keeps bounded assistant inputs aligned and rejects arbitrary design/approval overrides",async()=>{
  const validate=new Ajv2020({strict:false}).compile(JSON.parse(await readFile("contracts/tools/studio_prime_design_assistant.v1.schema.json","utf8")));
  for(const [input,valid] of [
    [{action:"get_state"},true],[{action:"generate",pbs_lengths:[5],rtt_lengths:[16]},true],
    [{action:"select",candidate_id:"primecand_"+"a".repeat(64)},true],
    [{action:"generate",pbs_lengths:[true],rtt_lengths:[16]},false],
    [{action:"generate",pbs_lengths:[101],rtt_lengths:[16]},false],
    [{action:"generate",pbs_lengths:[],rtt_lengths:[16]},false],
    [{action:"generate",sequence:"ACGT"},false],[{action:"select",candidate_id:"unbound"},false],
    [{action:"get_state",approved:true},false]
  ] as const){expect(zPrimeAssistantInput.safeParse(input).success).toBe(valid);expect(validate(input)).toBe(valid);}
});

it("publishes strict assistant state without claiming calibration",async()=>{
  const validate=new Ajv2020({strict:false}).compile(JSON.parse(await readFile("contracts/tools/studio_prime_design_assistant.out.v1.schema.json","utf8")));
  const output={provenance_run_id:"run_01ARZ3NDEKTSV4RRFFQ69G5FAV",log_artifact_id:"art_01ARZ3NDEKTSV4RRFFQ69G5FAV",
    bridge:{bridge_file:"bridge.json",host:"127.0.0.1",port:1234,pid:12,session_id:"test",started_at:"2026-10-30"},
    design_assistant:{schema:"helix.studio.prime_design_assistant/v1",status:"empty",message:"No compatible candidates",report:{candidate_count:0},selection:null}};
  expect(zPrimeAssistantOutput.safeParse(output).success).toBe(true);expect(validate(output)).toBe(true);
  output.design_assistant.status="approved";
  expect(zPrimeAssistantOutput.safeParse(output).success).toBe(false);expect(validate(output)).toBe(false);
});
