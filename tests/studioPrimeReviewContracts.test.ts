import { it, expect } from "vitest";
import { readFile } from "node:fs/promises";
import { Ajv2020 } from "ajv/dist/2020.js";
import { zPrimeReviewInput, zPrimeReviewOutput } from "../src/mcp/studioPrimeReviewTools.js";

it("validates bounded review operations with matching Zod and JSON schemas", async () => {
  const validate = new Ajv2020({ strict:false }).compile(JSON.parse(await readFile("contracts/tools/studio_prime_results_review.v1.schema.json","utf8")));
  for (const [input, valid] of [
    [{action:"open"},true], [{action:"select",run_id:"run_123",compare_run_id:null,full_sequence:true},true],
    [{action:"get_state"},true], [{action:"run"},false], [{action:"select",run_id:"../outside"},false],
    [{action:"open",path:"/arbitrary"},false], [{action:"open",full_sequence:1},false],
    [{action:"select",scroll_y:-1},false], [{action:"select",scroll_y:true},false]
  ] as const) {
    expect(zPrimeReviewInput.safeParse(input).success).toBe(valid);
    expect(validate(input)).toBe(valid);
  }
});

it("accepts declared insertion comparisons in both runtime and published output contracts", async () => {
  const validate = new Ajv2020({strict:false}).compile(JSON.parse(await readFile("contracts/tools/studio_prime_results_review.out.v1.schema.json","utf8")));
  const output = {provenance_run_id:"run_01ARZ3NDEKTSV4RRFFQ69G5FAV",log_artifact_id:"art_01ARZ3NDEKTSV4RRFFQ69G5FAV",
    bridge:{bridge_file:"bridge.json",host:"127.0.0.1",port:1234,pid:123,session_id:"test",started_at:"2026-10-30"},
    prime_review:{schema:"helix.studio.prime_results_review/v2",status:"ready",visible:true,run_ids:["a","b"],selected_run_id:"a",compare_run_id:"b",
      previous_design:false,full_sequence:true,scroll_y:0,scroll_max:100,
      comparison:{same_reference:true,sequence_comparable:true,different_offsets:null,reasons:[],model_changed:false,
        edited_sequences_equal:true,edit_definitions_equal:false,alignment_difference_columns:[1,3],
        alignment:{coordinate_system:"declared_reference_boundary_alignment/v1",columns:[{kind:"insertion",boundary:1,index:0}],tracks:{Reference:"-",Selected:"A",Previous:"-"}}}}};
  expect(zPrimeReviewOutput.safeParse(output).success).toBe(true);
  expect(validate(output)).toBe(true);
  const bad=structuredClone(output);bad.prime_review.comparison.alignment.columns[0]!.boundary=-1;
  expect(zPrimeReviewOutput.safeParse(bad).success).toBe(false);
  expect(validate(bad)).toBe(false);
});
