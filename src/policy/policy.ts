import { promises as fs } from "fs";
import path from "path";
import YAML from "yaml";
import { McpError, ErrorCode } from "@modelcontextprotocol/sdk/types.js";
import { sha256Prefixed, stableJsonStringify } from "../core/canonicalJson.js";

export interface PolicyConfig {
  version: number;
  tool_allowlist: string[];
  quotas: {
    max_threads: number;
    max_runtime_seconds: number;
    max_import_bytes: number;
    max_preview_bytes?: number;
    max_preview_lines?: number;
  };
  imports: {
    allow_source_kinds: Array<"inline_text" | "local_path">;
    local_path_prefix_allowlist: string[];
    deny_symlinks?: boolean;
  };
  tools?: Record<string, { max_threads?: number }>;
}

function isPolicyConfig(value: unknown): value is PolicyConfig {
  if (!value || typeof value !== "object") return false;
  const obj = value as Record<string, unknown>;
  return typeof obj.version === "number" && Array.isArray(obj.tool_allowlist);
}

export class PolicyEngine {
  readonly policyHash: `sha256:${string}`;

  constructor(private readonly policy: PolicyConfig) {
    this.policyHash = sha256Prefixed(stableJsonStringify(policy));
  }

  static async loadFromFile(filePath: string): Promise<PolicyEngine> {
    const raw = await fs.readFile(filePath, "utf8");
    const parsed = YAML.parse(raw) as unknown;
    if (!isPolicyConfig(parsed)) {
      throw new Error(`invalid policy at ${filePath}`);
    }
    return new PolicyEngine(parsed);
  }

  snapshot(): Record<string, unknown> {
    return JSON.parse(JSON.stringify(this.policy)) as Record<string, unknown>;
  }

  previewCaps(): { maxBytes: number; maxLines: number } {
    return {
      maxBytes: this.policy.quotas.max_preview_bytes ?? 8192,
      maxLines: this.policy.quotas.max_preview_lines ?? 200
    };
  }

  assertToolAllowed(toolName: string): void {
    if (!this.policy.tool_allowlist.includes(toolName)) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied tool: ${toolName}`);
    }
  }

  enforceThreads(toolName: string, requestedThreads: number | undefined): number {
    const maxGlobal = this.policy.quotas.max_threads;
    const maxTool = this.policy.tools?.[toolName]?.max_threads ?? maxGlobal;
    const threads = requestedThreads ?? 1;
    if (!Number.isInteger(threads) || threads < 1) {
      throw new McpError(ErrorCode.InvalidParams, `threads must be an integer >= 1`);
    }
    if (threads > maxTool) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        `policy denied threads=${threads} (max ${maxTool}) for tool ${toolName}`
      );
    }
    return threads;
  }

  maxImportBytes(): bigint {
    return BigInt(this.policy.quotas.max_import_bytes);
  }

  maxRuntimeSeconds(): number {
    return this.policy.quotas.max_runtime_seconds;
  }

  enforceRuntimeSeconds(requested: number): number {
    const max = this.policy.quotas.max_runtime_seconds;
    if (!Number.isInteger(requested) || requested < 1) {
      throw new McpError(ErrorCode.InvalidParams, `runtimeSeconds must be an integer >= 1`);
    }
    if (requested > max) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied runtimeSeconds=${requested} (max ${max})`);
    }
    return requested;
  }

  assertImportSourceAllowed(sourceKind: "inline_text" | "local_path"): void {
    if (!this.policy.imports.allow_source_kinds.includes(sourceKind)) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied import source kind: ${sourceKind}`);
    }
  }

  async validateLocalPathImport(sourcePath: string): Promise<string> {
    const resolved = path.resolve(sourcePath);
    const real = await fs.realpath(resolved);

    const denySymlinks = this.policy.imports.deny_symlinks ?? true;
    if (denySymlinks && real !== resolved) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied symlinked path: ${resolved}`);
    }

    const allow = await Promise.all(
      this.policy.imports.local_path_prefix_allowlist.map(async (prefix) => {
        const resolvedPrefix = path.resolve(prefix);
        let realPrefix = resolvedPrefix;
        try {
          realPrefix = await fs.realpath(resolvedPrefix);
        } catch {
          // ignore
        }
        return real === realPrefix || real.startsWith(realPrefix + path.sep);
      })
    );

    if (!allow.some(Boolean)) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied local_path outside allowlist: ${real}`);
    }

    const st = await fs.lstat(real);
    if (!st.isFile()) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied local_path (not a file): ${real}`);
    }

    if (denySymlinks && st.isSymbolicLink()) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied symlink: ${real}`);
    }

    return real;
  }
}
