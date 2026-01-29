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
const RESERVED_OUTPUT_ROLES = new Set(["log", "stdout", "stderr", "slurm_script"]);

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
    const slurmRelpaths = new Set<string>();
    for (const out of tool.declaredOutputs) {
      if (!out || typeof out !== "object") throw new Error(`toolpack:${tool.toolName}: invalid declaredOutputs entry`);
      if (!out.role || typeof out.role !== "string") throw new Error(`toolpack:${tool.toolName}: output role must be a string`);
      if (RESERVED_OUTPUT_ROLES.has(out.role)) {
        throw new Error(`toolpack:${tool.toolName}: output role "${out.role}" is reserved`);
      }
      if (out.role.length > 64) throw new Error(`toolpack:${tool.toolName}: output role too long: ${out.role}`);
      if (roles.has(out.role)) throw new Error(`toolpack:${tool.toolName}: duplicate output role: ${out.role}`);
      roles.add(out.role);

      if (!ARTIFACT_TYPES.has(out.type)) {
        throw new Error(`toolpack:${tool.toolName}: invalid output type for role ${out.role}: ${String(out.type)}`);
      }
      if (typeof out.label !== "string" || out.label.trim().length === 0 || out.label.length > 256) {
        throw new Error(`toolpack:${tool.toolName}: invalid output label for role ${out.role}`);
      }

      if (tool.planKind === "slurm") {
        if (typeof out.srcRelpath !== "string" || out.srcRelpath.trim().length === 0 || out.srcRelpath.length > 256) {
          throw new Error(`toolpack:${tool.toolName}: slurm outputs must declare srcRelpath for role ${out.role}`);
        }
        const rel = out.srcRelpath.trim();
        if (rel !== out.srcRelpath) {
          throw new Error(`toolpack:${tool.toolName}: slurm output srcRelpath must not include leading/trailing whitespace`);
        }
        if (rel.startsWith("/") || rel.includes("..")) {
          throw new Error(`toolpack:${tool.toolName}: slurm output srcRelpath must be a safe relative path: ${rel}`);
        }
        if (slurmRelpaths.has(rel)) {
          throw new Error(`toolpack:${tool.toolName}: duplicate slurm output srcRelpath: ${rel}`);
        }
        slurmRelpaths.add(rel);
      } else {
        if (out.srcRelpath !== undefined) {
          throw new Error(`toolpack:${tool.toolName}: srcRelpath is only valid for slurm toolpacks (role ${out.role})`);
        }
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
  const destNames = new Set<string>();
  for (const inp of planInputs) {
    const role = inp?.role;
    const artifactId = inp?.artifact?.artifactId;
    const destName = inp?.destName;
    if (typeof role !== "string" || role.trim().length === 0) {
      throw new Error(`toolpack:${toolName}: docker plan.inputs role must be a string`);
    }
    if (role !== role.trim()) {
      throw new Error(`toolpack:${toolName}: docker plan.inputs role must not include leading/trailing whitespace`);
    }
    if (typeof artifactId !== "string" || artifactId.trim().length === 0) {
      throw new Error(`toolpack:${toolName}: docker plan.inputs artifactId must be a string`);
    }
    if (typeof destName !== "string" || destName.trim().length === 0) {
      throw new Error(`toolpack:${toolName}: docker plan.inputs destName must be a string`);
    }
    if (destName !== destName.trim()) {
      throw new Error(`toolpack:${toolName}: docker plan.inputs destName must not include leading/trailing whitespace`);
    }
    if (destNames.has(destName)) {
      throw new Error(`toolpack:${toolName}: docker plan.inputs destName collision: ${destName}`);
    }
    destNames.add(destName);
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
    if (role !== role.trim()) {
      throw new Error(`toolpack:${toolName}: inputsToLink role must not include leading/trailing whitespace`);
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

function assertSlurmPlanInputsMatchLinkedInputs(toolName: string, prepared: { plan: unknown; inputsToLink: unknown }): void {
  const plan = prepared.plan as any;
  if (!plan || typeof plan !== "object") {
    throw new Error(`toolpack:${toolName}: slurm plan must be an object`);
  }

  const planInputs = plan.io?.inputs as unknown;
  if (!Array.isArray(planInputs)) {
    throw new Error(`toolpack:${toolName}: slurm plan.io.inputs must be an array`);
  }

  const linked = prepared.inputsToLink as any;
  if (!Array.isArray(linked)) {
    throw new Error(`toolpack:${toolName}: inputsToLink must be an array`);
  }

  const planKeys = new Set<string>();
  const destRelpaths = new Set<string>();
  for (const inp of planInputs) {
    const role = inp?.role;
    const artifactId = inp?.artifact_id;
    const checksum = inp?.checksum_sha256;
    const destRelpath = inp?.dest_relpath;

    if (typeof role !== "string" || role.trim().length === 0) {
      throw new Error(`toolpack:${toolName}: slurm plan.io.inputs role must be a string`);
    }
    if (role !== role.trim()) {
      throw new Error(`toolpack:${toolName}: slurm plan.io.inputs role must not include leading/trailing whitespace`);
    }

    if (typeof artifactId !== "string" || artifactId.trim().length === 0) {
      throw new Error(`toolpack:${toolName}: slurm plan.io.inputs artifact_id must be a string`);
    }
    if (artifactId !== artifactId.trim()) {
      throw new Error(`toolpack:${toolName}: slurm plan.io.inputs artifact_id must not include leading/trailing whitespace`);
    }

    if (typeof checksum !== "string" || checksum.trim().length === 0) {
      throw new Error(`toolpack:${toolName}: slurm plan.io.inputs checksum_sha256 must be a string`);
    }
    if (checksum !== checksum.trim()) {
      throw new Error(`toolpack:${toolName}: slurm plan.io.inputs checksum_sha256 must not include leading/trailing whitespace`);
    }

    if (typeof destRelpath !== "string" || destRelpath.trim().length === 0) {
      throw new Error(`toolpack:${toolName}: slurm plan.io.inputs dest_relpath must be a string`);
    }
    if (destRelpath !== destRelpath.trim()) {
      throw new Error(`toolpack:${toolName}: slurm plan.io.inputs dest_relpath must not include leading/trailing whitespace`);
    }
    if (destRelpath.startsWith("/") || destRelpath.includes("..")) {
      throw new Error(`toolpack:${toolName}: slurm plan.io.inputs dest_relpath must be a safe relative path: ${destRelpath}`);
    }
    if (!destRelpath.startsWith("in/")) {
      throw new Error(`toolpack:${toolName}: slurm plan.io.inputs dest_relpath must start with "in/": ${destRelpath}`);
    }
    if (destRelpaths.has(destRelpath)) {
      throw new Error(`toolpack:${toolName}: slurm plan.io.inputs dest_relpath collision: ${destRelpath}`);
    }
    destRelpaths.add(destRelpath);

    const key = `${role}\u0000${artifactId}`;
    if (planKeys.has(key)) {
      throw new Error(`toolpack:${toolName}: slurm plan.io.inputs contains duplicate role+artifact: ${role} ${artifactId}`);
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
    if (role !== role.trim()) {
      throw new Error(`toolpack:${toolName}: inputsToLink role must not include leading/trailing whitespace`);
    }
    if (typeof artifactId !== "string" || artifactId.trim().length === 0) {
      throw new Error(`toolpack:${toolName}: inputsToLink artifactId must be a string`);
    }
    if (artifactId !== artifactId.trim()) {
      throw new Error(`toolpack:${toolName}: inputsToLink artifactId must not include leading/trailing whitespace`);
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
      `toolpack:${toolName}: slurm plan inputs must exactly match inputsToLink` +
        (missing.length ? ` (missing: ${missing.slice(0, 5).map(fmt).join(", ")})` : "") +
        (extra.length ? ` (extra: ${extra.slice(0, 5).map(fmt).join(", ")})` : "")
    );
  }
}

function assertSlurmPlanOutputsMatchDeclaredOutputs(tool: ToolDefinition<any, any>, prepared: { plan: unknown }): void {
  const plan = prepared.plan as any;
  if (!plan || typeof plan !== "object") {
    throw new Error(`toolpack:${tool.toolName}: slurm plan must be an object`);
  }

  const planOutputs = plan.io?.outputs as unknown;
  if (!Array.isArray(planOutputs)) {
    throw new Error(`toolpack:${tool.toolName}: slurm plan.io.outputs must be an array`);
  }

  const byRole = new Map<string, { srcRelpath: string; type: string; label: string }>();
  const srcRelpaths = new Set<string>();
  for (const out of planOutputs) {
    const role = out?.role;
    const srcRelpath = out?.src_relpath;
    const type = out?.type;
    const label = out?.label;

    if (typeof role !== "string" || role.trim().length === 0) {
      throw new Error(`toolpack:${tool.toolName}: slurm plan.io.outputs role must be a string`);
    }
    if (role !== role.trim()) {
      throw new Error(`toolpack:${tool.toolName}: slurm plan.io.outputs role must not include leading/trailing whitespace`);
    }
    if (RESERVED_OUTPUT_ROLES.has(role)) {
      throw new Error(`toolpack:${tool.toolName}: slurm plan.io.outputs role "${role}" is reserved`);
    }

    if (typeof srcRelpath !== "string" || srcRelpath.trim().length === 0) {
      throw new Error(`toolpack:${tool.toolName}: slurm plan.io.outputs src_relpath must be a string`);
    }
    if (srcRelpath !== srcRelpath.trim()) {
      throw new Error(`toolpack:${tool.toolName}: slurm plan.io.outputs src_relpath must not include leading/trailing whitespace`);
    }
    if (srcRelpath.startsWith("/") || srcRelpath.includes("..")) {
      throw new Error(`toolpack:${tool.toolName}: slurm plan.io.outputs src_relpath must be a safe relative path: ${srcRelpath}`);
    }
    if (!srcRelpath.startsWith("out/")) {
      throw new Error(`toolpack:${tool.toolName}: slurm plan.io.outputs src_relpath must start with "out/": ${srcRelpath}`);
    }
    if (srcRelpaths.has(srcRelpath)) {
      throw new Error(`toolpack:${tool.toolName}: slurm plan.io.outputs src_relpath collision: ${srcRelpath}`);
    }
    srcRelpaths.add(srcRelpath);

    if (typeof type !== "string" || type.trim().length === 0) {
      throw new Error(`toolpack:${tool.toolName}: slurm plan.io.outputs type must be a string`);
    }
    if (!ARTIFACT_TYPES.has(type as ArtifactType)) {
      throw new Error(`toolpack:${tool.toolName}: slurm plan.io.outputs invalid type for role ${role}: ${String(type)}`);
    }

    if (typeof label !== "string" || label.trim().length === 0 || label.length > 256) {
      throw new Error(`toolpack:${tool.toolName}: slurm plan.io.outputs label must be a string`);
    }
    if (label !== label.trim()) {
      throw new Error(`toolpack:${tool.toolName}: slurm plan.io.outputs label must not include leading/trailing whitespace`);
    }

    if (byRole.has(role)) {
      throw new Error(`toolpack:${tool.toolName}: slurm plan.io.outputs duplicate role: ${role}`);
    }
    byRole.set(role, { srcRelpath, type, label });
  }

  const expectedRoles = new Set(tool.declaredOutputs.map((o) => o.role));
  const planRoles = new Set(byRole.keys());

  if (expectedRoles.size !== planRoles.size) {
    throw new Error(`toolpack:${tool.toolName}: slurm plan.io.outputs must match declaredOutputs exactly`);
  }
  for (const r of expectedRoles) {
    if (!planRoles.has(r)) {
      throw new Error(`toolpack:${tool.toolName}: slurm plan.io.outputs missing declared role: ${r}`);
    }
  }
  for (const r of planRoles) {
    if (!expectedRoles.has(r)) {
      throw new Error(`toolpack:${tool.toolName}: slurm plan.io.outputs includes undeclared role: ${r}`);
    }
  }

  for (const decl of tool.declaredOutputs) {
    const expectedRel = decl.srcRelpath;
    if (typeof expectedRel !== "string" || expectedRel.trim().length === 0) {
      throw new Error(`toolpack:${tool.toolName}: slurm toolpack declaredOutputs missing srcRelpath for role ${decl.role}`);
    }
    const po = byRole.get(decl.role);
    if (!po) throw new Error(`toolpack:${tool.toolName}: slurm plan.io.outputs missing role: ${decl.role}`);

    if (po.type !== decl.type) {
      throw new Error(
        `toolpack:${tool.toolName}: slurm output type mismatch for role ${decl.role} (expected ${decl.type}, got ${po.type})`
      );
    }
    if (po.label !== decl.label) {
      throw new Error(
        `toolpack:${tool.toolName}: slurm output label mismatch for role ${decl.role} (expected ${decl.label}, got ${po.label})`
      );
    }
    if (po.srcRelpath !== expectedRel) {
      throw new Error(
        `toolpack:${tool.toolName}: slurm output src_relpath mismatch for role ${decl.role} (expected ${expectedRel}, got ${po.srcRelpath})`
      );
    }
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
          if (existing?.resultJson && (tool.planKind === "slurm" || existing.status === "succeeded")) {
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

          if (tool.planKind === "docker") {
            assertDockerPlanInputsMatchLinkedInputs(toolName, prepared);
          } else {
            assertSlurmPlanInputsMatchLinkedInputs(toolName, prepared);
            assertSlurmPlanOutputsMatchDeclaredOutputs(tool, prepared);
          }

          for (const inp of prepared.inputsToLink) {
            await toolRun.linkInput(inp.artifactId, inp.role);
          }

          const res = await tool.run({ runId, toolRun, prepared, ctx });
          let structured: JsonObject;
          if (tool.planKind === "docker") {
            await assertDeclaredOutputsSatisfied(ctx, tool, runId);
            structured = await toolRun.finishSuccess(res.result, res.summary);
          } else {
            structured = await toolRun.checkpointQueued(res.result, res.summary);
          }

          const verb = tool.planKind === "slurm" ? "queued" : "complete";

          return {
            content: [{ type: "text", text: `${toolName} ${verb} (run ${structured.provenance_run_id as string})` }],
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
