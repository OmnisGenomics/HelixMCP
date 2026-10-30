import { it, expect } from "vitest";
import { readFile } from "node:fs/promises";
import { Ajv2020 } from "ajv/dist/2020.js";
import { zPrimeExportInput } from "../src/mcp/studioPrimeExportTools.js";

it("allows only a live bridge reference, never caller-selected export paths or approval",async()=>{
  const validate=new Ajv2020({strict:false}).compile(JSON.parse(await readFile("contracts/tools/studio_prime_comparison_export.v1.schema.json","utf8")));
  for(const [value,valid] of [[{},true],[{bridge_file:"bridge.json"},true],[{path:"/overwrite"},false],[{run_id:"arbitrary"},false],[{approved:true},false]] as const){
    expect(zPrimeExportInput.safeParse(value).success).toBe(valid);
    expect(validate(value)).toBe(valid);
  }
});
