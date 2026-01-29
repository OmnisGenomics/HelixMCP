import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { ErrorCode, McpError } from "@modelcontextprotocol/sdk/types.js";
import type { JsonObject } from "../core/json.js";
import type { ProjectId } from "../core/ids.js";
import { stableJsonStringify } from "../core/canonicalJson.js";
import { deriveRunId } from "../runs/runIdentity.js";
import { requestedByFromExtra, ToolRun } from "../runs/toolRun.js";
import { envSnapshot } from "../mcp/envSnapshot.js";
import type { ArtifactType } from "../core/artifact.js";
import type { ToolContext, ToolDefinition } from "./types.js";

const TOOL_NAME_RE = /^[a-z][a-z0-9_]*$/;
const CONTRACT_VERSION_RE = /^v\d+$/;

const ARTIFACT_TYPES: Set<ArtifactType> = new Set<ArtifactType>([
  "FASTQ_GZ",
  "BAM",
  "BAI",
  "VCF",
  "H5AD",
  "TSV",
  "CSV",
  "JSON",
  "TEXT",
  "HTML",
  "PDF",
  "LOG",
  "UNKNOWN"
]);

function assertJsonSafe(value: unknown, context: string): void {
  const seen = new Set<unknown>();

  const walk = (v: unknown, path: string): void => {
    if (v === null) return;
    const t = typeof v;
    if (t === "string") return;
    if (t === "boolean") return;
    if (t === "number") {
      if (!Number.isFinite(v)) throw new Error(`${context}: non-finite number at ${path}`);
      return;
    }
    if (t === "bigint") throw new Error(`${context}: bigint is not JSON-safe at ${path}`);
    if (t === "undefined") throw new Error(`${context}: undefined is not JSON-safe at ${path}`);
    if (t === "function") throw new Error(`${context}: function is not JSON-safe at ${path}`);
    if (t === "symbol") throw new Error(`${context}: symbol is not JSON-safe at ${path}`);

    if (Array.isArray(v)) {
      if (seen.has(v)) throw new Error(`${context}: circular reference at ${path}`);
      seen.add(v);
      for (let i = 0; i < v.length; i++) walk(v[i], `${path}[${i}]`);
      return;
    }

    if (t === "object") {
      if (seen.has(v)) throw new Error(`${context}: circular reference at ${path}`);
      seen.add(v);
      for (const [k, vv] of Object.entries(v as Record<string, unknown>)) {
        if (typeof k !== "string" || k.length === 0) throw new Error(`${context}: invalid key at ${path}`);
        walk(vv, `${path}.${k}`);
      }
      return;
    }

    throw new Error(`${context}: unsupported value type at ${path}: ${t}`);
  };

  walk(value, "$");
}

function validateToolDefinitions(tools: Array<ToolDefinition<any, any>>): void {
  const toolNames = new Set<string>();
  for (const tool of tools) {
    if (!tool.toolName || typeof tool.toolName !== "string") {
      throw new Error(`toolpack: missing toolName`);
    }
    if (tool.toolName.length > 128 || !TOOL_NAME_RE.test(tool.toolName)) {
      throw new Error(`toolpack: invalid toolName: ${tool.toolName}`);
    }
    if (toolNames.has(tool.toolName)) {
      throw new Error(`toolpack: duplicate toolName: ${tool.toolName}`);
    }
    toolNames.add(tool.toolName);

    if (!tool.contractVersion || typeof tool.contractVersion !== "string" || !CONTRACT_VERSION_RE.test(tool.contractVersion)) {
      throw new Error(`toolpack:${tool.toolName}: invalid contractVersion: ${tool.contractVersion}`);
    }

    if (tool.planKind !== "docker" && tool.planKind !== "slurm") {
      throw new Error(`toolpack:${tool.toolName}: invalid planKind: ${String((tool as any).planKind)}`);
    }

    if (!Array.isArray(tool.declaredOutputs)) {
      throw new Error(`toolpack:${tool.toolName}: declaredOutputs must be an array`);
    }
    const roles = new Set<string>();
    for (const out of tool.declaredOutputs) {
      if (!out || typeof out !== "object") throw new Error(`toolpack:${tool.toolName}: invalid declaredOutputs entry`);
      if (!out.role || typeof out.role !== "string") throw new Error(`toolpack:${tool.toolName}: output role must be a string`);
      if (out.role === "log") throw new Error(`toolpack:${tool.toolName}: output role "log" is reserved`);
      if (out.role.length > 64) throw new Error(`toolpack:${tool.toolName}: output role too long: ${out.role}`);
      if (roles.has(out.role)) throw new Error(`toolpack:${tool.toolName}: duplicate output role: ${out.role}`);
      roles.add(out.role);

      if (!ARTIFACT_TYPES.has(out.type)) {
        throw new Error(`toolpack:${tool.toolName}: invalid output type for role ${out.role}: ${String(out.type)}`);
      }
      if (typeof out.label !== "string" || out.label.trim().length === 0 || out.label.length > 256) {
        throw new Error(`toolpack:${tool.toolName}: invalid output label for role ${out.role}`);
      }
    }
  }
}

async function assertDeclaredOutputsSatisfied(ctx: ToolContext, tool: ToolDefinition<any, any>, runId: string): Promise<void> {
  const outputs = await ctx.store.listRunOutputs(runId as any);
  const byRole = new Map<string, string[]>();
  for (const o of outputs) {
    const list = byRole.get(o.role) ?? [];
    list.push(o.artifactId);
    byRole.set(o.role, list);
  }

  const allowedRoles = new Set<string>(tool.declaredOutputs.map((o) => o.role));
  for (const role of byRole.keys()) {
    if (!allowedRoles.has(role)) {
      throw new Error(`toolpack:${tool.toolName}: unexpected output role linked: ${role}`);
    }
  }

  for (const expected of tool.declaredOutputs) {
    const ids = byRole.get(expected.role) ?? [];
    if (ids.length !== 1) {
      throw new Error(`toolpack:${tool.toolName}: expected exactly 1 output for role ${expected.role}, got ${ids.length}`);
    }
    const art = await ctx.artifacts.getArtifact(ids[0] as any);
    if (!art) throw new Error(`toolpack:${tool.toolName}: missing output artifact for role ${expected.role}: ${ids[0]}`);
    if (art.type !== expected.type) {
      throw new Error(
        `toolpack:${tool.toolName}: output type mismatch for role ${expected.role} (expected ${expected.type}, got ${art.type})`
      );
    }
    if (expected.label && art.label !== expected.label) {
      throw new Error(
        `toolpack:${tool.toolName}: output label mismatch for role ${expected.role} (expected ${expected.label}, got ${art.label})`
      );
    }
  }
}

function canonicalizeToolpackCanonicalParams(input: JsonObject, context: string): JsonObject {
  const canonicalJson = stableJsonStringify(input);
  const parsed = JSON.parse(canonicalJson) as unknown;

  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error(`${context}: canonicalParams must be a JSON object`);
  }

  return parsed as JsonObject;
}

function assertDockerPlanInputsMatchLinkedInputs(toolName: string, prepared: { plan: unknown; inputsToLink: unknown }): void {
  const plan = prepared.plan as any;
  if (!plan || typeof plan !== "object") {
    throw new Error(`toolpack:${toolName}: docker plan must be an object`);
  }

  const planInputs = plan.inputs as unknown;
  if (!Array.isArray(planInputs)) {
    throw new Error(`toolpack:${toolName}: docker plan.inputs must be an array`);
  }

  const linked = prepared.inputsToLink as any;
  if (!Array.isArray(linked)) {
    throw new Error(`toolpack:${toolName}: inputsToLink must be an array`);
  }

  const planKeys = new Set<string>();
  for (const inp of planInputs) {
    const role = inp?.role;
    const artifactId = inp?.artifact?.artifactId;
    if (typeof role !== "string" || role.trim().length === 0) {
      throw new Error(`toolpack:${toolName}: docker plan.inputs role must be a string`);
    }
    if (typeof artifactId !== "string" || artifactId.trim().length === 0) {
      throw new Error(`toolpack:${toolName}: docker plan.inputs artifactId must be a string`);
    }
    const key = `${role}\u0000${artifactId}`;
    if (planKeys.has(key)) {
      throw new Error(`toolpack:${toolName}: docker plan.inputs contains duplicate role+artifact: ${role} ${artifactId}`);
    }
    planKeys.add(key);
  }

  const linkedKeys = new Set<string>();
  for (const inp of linked) {
    const role = inp?.role;
    const artifactId = inp?.artifactId;
    if (typeof role !== "string" || role.trim().length === 0) {
      throw new Error(`toolpack:${toolName}: inputsToLink role must be a string`);
    }
    if (typeof artifactId !== "string" || artifactId.trim().length === 0) {
      throw new Error(`toolpack:${toolName}: inputsToLink artifactId must be a string`);
    }
    const key = `${role}\u0000${artifactId}`;
    if (linkedKeys.has(key)) {
      throw new Error(`toolpack:${toolName}: inputsToLink contains duplicate role+artifact: ${role} ${artifactId}`);
    }
    linkedKeys.add(key);
  }

  const missing: string[] = [];
  for (const k of linkedKeys) {
    if (!planKeys.has(k)) missing.push(k);
  }
  const extra: string[] = [];
  for (const k of planKeys) {
    if (!linkedKeys.has(k)) extra.push(k);
  }

  if (missing.length || extra.length) {
    const fmt = (k: string): string => {
      const [role, artifactId] = k.split("\u0000");
      return `${role} ${artifactId}`;
    };
    throw new Error(
      `toolpack:${toolName}: docker plan inputs must exactly match inputsToLink` +
        (missing.length ? ` (missing: ${missing.slice(0, 5).map(fmt).join(", ")})` : "") +
        (extra.length ? ` (extra: ${extra.slice(0, 5).map(fmt).join(", ")})` : "")
    );
  }
}

export function registerToolDefinitions(mcp: McpServer, ctx: ToolContext, tools: Array<ToolDefinition<any, any>>): void {
  // Fail closed: refuse to start if any toolpack is structurally invalid.
  validateToolDefinitions(tools);

  for (const tool of tools) {
    mcp.registerTool(
      tool.toolName,
      {
        description: tool.description,
        inputSchema: tool.inputSchema,
        outputSchema: tool.outputSchema
      },
      async (args: any, extra: any) => {
        const toolName = tool.toolName;
        const contractVersion = tool.contractVersion;
        let toolRun: ToolRun | null = null;
        let started = false;

        try {
          ctx.policy.assertToolAllowed(toolName);

          const prepared = await tool.canonicalize(args, ctx);
          assertJsonSafe(prepared.canonicalParams, `toolpack:${toolName}: canonicalParams`);
          prepared.canonicalParams = canonicalizeToolpackCanonicalParams(prepared.canonicalParams, `toolpack:${toolName}: canonicalParams`);

          const { runId, paramsHash } = deriveRunId({
            toolName,
            contractVersion,
            policyHash: ctx.policy.policyHash,
            canonicalParams: prepared.canonicalParams
          });

          const existing = await ctx.store.getRun(runId);
          if (existing?.status === "succeeded" && existing.resultJson) {
            return {
              content: [{ type: "text", text: `Replayed ${toolName} (${runId})` }],
              structuredContent: existing.resultJson
            };
          }

          toolRun = new ToolRun(
            { store: ctx.store, artifacts: ctx.artifacts },
            {
              runId,
              projectId: prepared.projectId as ProjectId,
              toolName,
              contractVersion,
              toolVersion: prepared.toolVersion,
              paramsHash,
              canonicalParams: prepared.canonicalParams,
              policyHash: ctx.policy.policyHash,
              requestedBy: requestedByFromExtra(extra),
              policySnapshot: ctx.policy.snapshot() as JsonObject,
              environment: envSnapshot()
            }
          );

          await toolRun.start();
          started = true;

          for (const inp of prepared.inputsToLink) {
            await toolRun.linkInput(inp.artifactId, inp.role);
          }

          if (tool.planKind === "docker") {
            assertDockerPlanInputsMatchLinkedInputs(toolName, prepared);
          }

          const res = await tool.run({ runId, toolRun, prepared, ctx });
          await assertDeclaredOutputsSatisfied(ctx, tool, runId);
          const structured = await toolRun.finishSuccess(res.result, res.summary);

          return {
            content: [{ type: "text", text: `${toolName} complete (run ${structured.provenance_run_id as string})` }],
            structuredContent: structured
          };
        } catch (e) {
          if (e instanceof McpError) {
            if (toolRun && started) {
              if (e.code === ErrorCode.InvalidRequest) await toolRun.finishBlocked(e.message);
              else await toolRun.finishFailure(e.message);
            }
            throw e;
          }
          if (e instanceof Error) {
            if (toolRun && started) await toolRun.finishFailure(e.message);
            throw e;
          }
          if (toolRun && started) await toolRun.finishFailure("unknown error");
          throw new Error("unknown error");
        }
      }
    );
  }
}
