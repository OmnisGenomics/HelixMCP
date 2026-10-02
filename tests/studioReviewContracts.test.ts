import { describe, expect, it } from "vitest";
import { readFile, readdir } from "node:fs/promises";
import { Ajv2020 } from "ajv/dist/2020.js";
import * as schemas from "../src/mcp/studioReviewSchemas.js";
import { zStudioCaptureScreenshotInput } from "../src/mcp/toolSchemas.js";

export const answers = {
  schema: "helix.workflow_evaluation.answers.v1",
  tasks: Object.fromEntries(["compare_candidates", "inspect_assumptions", "explain_decision", "verify_package"].map((task) => [task, {status:"skipped",assistance_needed:false,notes:"Automated contract fixture; not a human observation."}])),
  selected_candidate_id: null, alternatives_considered: [], decision_rationale: "", assumptions: [],
  change_explanation: null, intent_to_export_seconds: null, manual_handoffs_removed: null,
  return_visit: false, would_use_again: null
};

async function validators() {
  const ajv = new Ajv2020({ strict: false, validateFormats: false });
  ajv.addSchema(JSON.parse(await readFile("contracts/common.schema.json", "utf8")));
  for (const name of (await readdir("contracts/tools")).filter((file) => file.startsWith("studio_") && file.endsWith(".schema.json"))) {
    ajv.addSchema(JSON.parse(await readFile(`contracts/tools/${name}`, "utf8")));
  }
  return ajv;
}

describe("pilot reviewer contracts", () => {
  it("keeps published and runtime parameters aligned", async () => {
    const inputs = {
      studio_pilot_review_get_state: schemas.zStudioPilotReviewStateInput,
      studio_pilot_review_open: schemas.zStudioPilotReviewOpenInput,
      studio_pilot_review_start: schemas.zStudioPilotReviewStartInput,
      studio_pilot_review_fill: schemas.zStudioPilotReviewFillInput,
      studio_pilot_review_view: schemas.zStudioPilotReviewViewInput,
      studio_pilot_review_export: schemas.zStudioPilotReviewStateInput,
      studio_capture_screenshot: zStudioCaptureScreenshotInput
    };
    for (const [name, runtime] of Object.entries(inputs)) {
      const published = JSON.parse(await readFile(`contracts/tools/${name}.v1.schema.json`, "utf8"));
      expect(Object.keys(published.properties).sort()).toEqual(Object.keys(runtime.shape).sort());
    }
  });

  it("rejects invalid observations identically before contacting Studio", async () => {
    const ajv = await validators();
    const validate = ajv.getSchema("helixmcp:tool:studio_pilot_review_fill:v1")!;
    const samples: Array<[unknown, boolean]> = [
      [{answers}, true],
      [{answers:{...answers,intent_to_export_seconds:0,manual_handoffs_removed:0,would_use_again:false}},true],
      [{answers:{...answers,intent_to_export_seconds:-1}},false],
      [{answers:{...answers,manual_handoffs_removed:1.5}},false],
      [{answers:{...answers,alternatives_considered:["A","A"]}},false],
      [{answers:{...answers,assumptions:[" "]}},false],
      [{answers:{...answers,tasks:{...answers.tasks,verify_package:{status:"approved",assistance_needed:false,notes:"Invalid fixture"}}}},false],
      [{answers:{...answers,approved:true}},false],
      [{answers,arbitrary_command:"delete"},false]
    ];
    for (const [sample, accepted] of samples) {
      expect(schemas.zStudioPilotReviewFillInput.safeParse(sample).success).toBe(accepted);
      expect(validate(sample), JSON.stringify(validate.errors)).toBe(accepted);
    }
  });

  it("requires paired dimensions and imported artifacts instead of source paths", async () => {
    const ajv = await validators();
    for (const sample of [{width:740}, {height:620}, {width:740,height:620}, {tab_index:99}]) {
      expect(ajv.getSchema("helixmcp:tool:studio_pilot_review_view:v1")!(sample)).toBe(schemas.zStudioPilotReviewViewInput.safeParse(sample).success);
    }
    const sample={package:"/tmp/raw.zip"};
    expect(schemas.zStudioPilotReviewOpenInput.safeParse(sample).success).toBe(false);
    expect(ajv.getSchema("helixmcp:tool:studio_pilot_review_open:v1")!(sample)).toBe(false);
  });
});
