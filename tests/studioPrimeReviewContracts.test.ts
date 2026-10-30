import { it, expect } from "vitest";
import { readFile } from "node:fs/promises";
import { Ajv2020 } from "ajv/dist/2020.js";
import { zPrimeReviewInput } from "../src/mcp/studioPrimeReviewTools.js";

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
