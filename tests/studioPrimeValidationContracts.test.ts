import {it,expect} from "vitest";
import {readFile} from "node:fs/promises";
import {Ajv2020} from "ajv/dist/2020.js";
import {zPrimeValidationInput,zPrimeValidationOutput} from "../src/mcp/studioPrimeValidationTools.js";

it("keeps bounded validation inputs aligned and refuses source/path/approval overrides",async()=>{
  const validate=new Ajv2020({strict:false}).compile(JSON.parse(await readFile("contracts/tools/studio_prime_validation_benchmark.v1.schema.json","utf8")));
  for(const [input,valid] of [[{},true],[{bridge_file:"bridge.json"},true],[{path:"any.zip"},false],[{approve:true},false],[{cases:[]},false]] as const){
    expect(zPrimeValidationInput.safeParse(input).success).toBe(valid);expect(validate(input)).toBe(valid);
  }
});

it("preserves completed FAIL evidence with strict published output schemas",async()=>{
  const validate=new Ajv2020({strict:false}).compile(JSON.parse(await readFile("contracts/tools/studio_prime_validation_benchmark.out.v1.schema.json","utf8")));
  const output={provenance_run_id:"run_01ARZ3NDEKTSV4RRFFQ69G5FAV",log_artifact_id:"art_01ARZ3NDEKTSV4RRFFQ69G5FAV",
    validation_artifact_id:"art_01ARZ3NDEKTSV4RRFFQ69G5FAV",validation_checksum_sha256:"sha256:"+"a".repeat(64),
    bridge:{bridge_file:"bridge.json",host:"127.0.0.1",port:1234,pid:12,session_id:"test",started_at:"2026-10-30"},
    validation:{schema:"helix.studio.prime_validation_benchmark/v1",status:"complete",session_unchanged:true,
      receipt:{schema:"helix.prime_validation_export/v1",path:"managed.zip",sha256:"a".repeat(64),size_bytes:100,
        status:"FAIL",case_count:236,passed_count:235,failed_case_ids:["sentinel-substitution-1"]}}};
  expect(zPrimeValidationOutput.safeParse(output).success).toBe(true);expect(validate(output)).toBe(true);
  output.validation.receipt.case_count=10000;
  expect(zPrimeValidationOutput.safeParse(output).success).toBe(false);expect(validate(output)).toBe(false);
});
