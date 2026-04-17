import { describe, expect, it } from "vitest";
import { readdir, readFile } from "fs/promises";
import path from "path";
import { Ajv2020 } from "ajv/dist/2020.js";

import { PolicyEngine } from "../src/policy/policy.js";
import { builtinToolDefinitions } from "../src/toolpacks/builtin/index.js";
import { zArtifactListInput } from "../src/mcp/toolSchemas.js";

async function readJson(filePath: string): Promise<Record<string, unknown>> {
  return JSON.parse(await readFile(filePath, "utf8")) as Record<string, unknown>;
}

async function listContractFiles(dir: string): Promise<string[]> {
  return (await readdir(dir)).filter((f) => f.endsWith(".schema.json")).sort();
}

function expectedToolSchemaId(fileName: string): string | null {
  const out = /^(.+)\.out\.(v\d+)\.schema\.json$/.exec(fileName);
  if (out) return `helixmcp:tool:${out[1]}:out:${out[2]}`;

  const input = /^(.+)\.(v\d+)\.schema\.json$/.exec(fileName);
  if (input) return `helixmcp:tool:${input[1]}:${input[2]}`;

  return null;
}

describe("published JSON contracts", () => {
  it("compile and use file-aligned tool schema ids", async () => {
    const ajv = new Ajv2020({ allErrors: true, strict: false, validateFormats: false });
    const schemaPaths = [
      path.resolve("contracts/common.schema.json"),
      path.resolve("contracts/slurm_job_spec.v1.schema.json"),
      path.resolve("contracts/bundles/bundle_manifest.v1.schema.json"),
      ...(await listContractFiles(path.resolve("contracts/tools"))).map((f) => path.resolve("contracts/tools", f))
    ];

    const schemas = await Promise.all(schemaPaths.map((p) => readJson(p)));
    for (const schema of schemas) {
      ajv.addSchema(schema);
    }

    for (const [i, schema] of schemas.entries()) {
      expect(() => ajv.compile(schema), schemaPaths[i]).not.toThrow();
    }

    for (const fileName of await listContractFiles(path.resolve("contracts/tools"))) {
      const schema = await readJson(path.resolve("contracts/tools", fileName));
      expect(schema.$id, fileName).toBe(expectedToolSchemaId(fileName));
    }
  });

  it("covers default policy tools and builtin toolpack contract versions", async () => {
    const contractFiles = new Set(await listContractFiles(path.resolve("contracts/tools")));
    const policy = await PolicyEngine.loadFromFile(path.resolve("policies/default.policy.yaml"));
    const policyTools = policy.snapshot().tool_allowlist;

    expect(Array.isArray(policyTools)).toBe(true);
    for (const toolName of policyTools as string[]) {
      expect(
        [...contractFiles].some((fileName) => fileName.startsWith(`${toolName}.`) && !fileName.includes(".out.")),
        `missing published input contract for default policy tool ${toolName}`
      ).toBe(true);
    }

    for (const tool of builtinToolDefinitions) {
      expect(
        contractFiles.has(`${tool.toolName}.${tool.contractVersion}.schema.json`),
        `missing published input contract for builtin toolpack ${tool.toolName}.${tool.contractVersion}`
      ).toBe(true);
    }
  });

  it("keeps artifact_list input contract aligned with runtime top-level parameters", async () => {
    const schema = await readJson(path.resolve("contracts/tools/artifact_list.v1.schema.json"));
    const publishedKeys = Object.keys((schema.properties ?? {}) as Record<string, unknown>).sort();
    const runtimeKeys = Object.keys(zArtifactListInput.shape).sort();

    expect(publishedKeys).toEqual(runtimeKeys);
  });
});
