import { describe, it, expect } from "vitest";
import { readFile } from "node:fs/promises";
import { Ajv2020 } from "ajv/dist/2020.js";
import { zStudioPrimeInput } from "../src/mcp/studioPrimeTools.js";

describe("Prime workflow contract", () => {
  it("keeps JSON Schema and Zod validation aligned", async () => {
    const schema = JSON.parse(await readFile("contracts/tools/studio_prime_workflow.v1.schema.json", "utf8"));
    const validate = new Ajv2020({ strict: false, validateFormats: false }).compile(schema);
    for (const [input, valid] of [
      [{ action: "get_state" }, true],
      [{ action: "wizard_navigate", stage: 3 }, true],
      [{ action: "wizard_fill", inputs: { rtt: "ACGT" } }, true],
      [{ action: "wizard_fill", inputs: { edit_kind:"INSERTION", inserted_sequence:"AG" } }, true],
      [{ action: "wizard_fill", inputs: { edit_kind:"DELETION", deletion_length:"2" } }, true],
      [{ action: "wizard_fill", inputs: { edit_kind:"REPLACEMENT" } }, false],
      [{ action: "eval", code: "arbitrary" }, false],
      [{ action: "wizard_navigate", stage: 4 }, false],
      [{ action: "wizard_navigate", stage: true }, false],
      [{ action: "wizard_fill", inputs: { arbitrary: "x" } }, false],
      [{ action: "wizard_target", path: "/arbitrary" }, false],
      [{ action: "wizard_target", synthetic: false }, false]
    ] as const) {
      expect(zStudioPrimeInput.safeParse(input).success).toBe(valid);
      expect(validate(input)).toBe(valid);
    }
  });
});
