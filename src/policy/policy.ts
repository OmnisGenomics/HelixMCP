import { promises as fs } from "fs";
import path from "path";
import YAML from "yaml";
import { McpError, ErrorCode } from "@modelcontextprotocol/sdk/types.js";
import { sha256Prefixed, stableJsonStringify } from "../core/canonicalJson.js";

export interface PolicyConfig {
  version: number;
  runtime?: {
    instance_id: string;
  };
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
  docker?: {
    network_mode?: "none" | "bridge";
    image_allowlist: string[];
  };
  slurm?: {
    partitions_allowlist: string[];
    accounts_allowlist: string[];
    qos_allowlist?: string[];
    constraints_allowlist?: string[];
    max_time_limit_seconds: number;
    max_cpus: number;
    max_mem_mb: number;
    max_gpus: number;
    max_collect_output_bytes: number;
    max_collect_log_bytes: number;
    gpu_types_allowlist?: string[];
    apptainer: {
      image_allowlist: string[];
    };
    network_mode_required: "none";
  };
  tools?: Record<string, { max_threads?: number }>;
}

function isPolicyConfig(value: unknown): value is PolicyConfig {
  if (!value || typeof value !== "object") return false;
  const obj = value as Record<string, unknown>;
  return typeof obj.version === "number" && Array.isArray(obj.tool_allowlist);
}

function expandEnvToken(value: string): string | null {
  const trimmed = value.trim();

  const m1 = /^\$\{([A-Z0-9_]+)\}$/.exec(trimmed);
  if (m1) {
    const varName = m1[1];
    if (!varName) return null;
    const v = process.env[varName]?.trim();
    return v ? v : null;
  }

  const m2 = /^\$([A-Z0-9_]+)$/.exec(trimmed);
  if (m2) {
    const varName = m2[1];
    if (!varName) return null;
    const v = process.env[varName]?.trim();
    return v ? v : null;
  }

  return value;
}

function expandPolicyEnv(policy: PolicyConfig): PolicyConfig {
  const prefixes = policy.imports.local_path_prefix_allowlist
    .map((p) => expandEnvToken(p))
    .filter((p): p is string => typeof p === "string" && p.trim().length > 0);

  return {
    ...policy,
    imports: {
      ...policy.imports,
      local_path_prefix_allowlist: prefixes
    }
  };
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
    return new PolicyEngine(expandPolicyEnv(parsed));
  }

  snapshot(): Record<string, unknown> {
    return JSON.parse(JSON.stringify(this.policy)) as Record<string, unknown>;
  }

  runtimeInstanceId(): string | null {
    const raw = this.policy.runtime?.instance_id;
    if (typeof raw !== "string") return null;
    const trimmed = raw.trim();
    return trimmed.length > 0 ? trimmed : null;
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

  dockerNetworkMode(): "none" | "bridge" {
    return this.policy.docker?.network_mode ?? "none";
  }

  assertDockerImageAllowed(image: string): void {
    const allowlist = this.policy.docker?.image_allowlist ?? [];
    if (!allowlist.length) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied docker image (no allowlist configured): ${image}`);
    }
    if (!allowlist.includes(image)) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied docker image: ${image}`);
    }
  }

  private requireSlurm(): NonNullable<PolicyConfig["slurm"]> {
    if (!this.policy.slurm) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied slurm (no slurm config)`);
    }
    return this.policy.slurm;
  }

  assertSlurmPartitionAllowed(partition: string): void {
    const slurm = this.requireSlurm();
    if (!slurm.partitions_allowlist.includes(partition)) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied slurm partition: ${partition}`);
    }
  }

  assertSlurmAccountAllowed(account: string): void {
    const slurm = this.requireSlurm();
    if (!slurm.accounts_allowlist.includes(account)) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied slurm account: ${account}`);
    }
  }

  assertSlurmQosAllowed(qos: string | null | undefined): void {
    if (qos === null || qos === undefined) return;
    const slurm = this.requireSlurm();
    const allowlist = slurm.qos_allowlist ?? [];
    if (!allowlist.includes(qos)) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied slurm qos: ${qos}`);
    }
  }

  assertSlurmConstraintAllowed(constraint: string | null | undefined): void {
    if (constraint === null || constraint === undefined) return;
    const slurm = this.requireSlurm();
    const allowlist = slurm.constraints_allowlist ?? [];
    if (!allowlist.includes(constraint)) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied slurm constraint: ${constraint}`);
    }
  }

  enforceSlurmResources(input: {
    timeLimitSeconds: number;
    cpus: number;
    memMb: number;
    gpus: number | null;
    gpuType: string | null;
  }): void {
    const slurm = this.requireSlurm();

    if (input.timeLimitSeconds > slurm.max_time_limit_seconds) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        `policy denied slurm time_limit_seconds=${input.timeLimitSeconds} (max ${slurm.max_time_limit_seconds})`
      );
    }
    if (input.cpus > slurm.max_cpus) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied slurm cpus=${input.cpus} (max ${slurm.max_cpus})`);
    }
    if (input.memMb > slurm.max_mem_mb) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied slurm mem_mb=${input.memMb} (max ${slurm.max_mem_mb})`);
    }

    if (input.gpus !== null) {
      if (input.gpus > slurm.max_gpus) {
        throw new McpError(
          ErrorCode.InvalidRequest,
          `policy denied slurm gpus=${input.gpus} (max ${slurm.max_gpus})`
        );
      }
      if (input.gpuType !== null) {
        const allow = slurm.gpu_types_allowlist ?? [];
        if (!allow.includes(input.gpuType)) {
          throw new McpError(ErrorCode.InvalidRequest, `policy denied slurm gpu_type: ${input.gpuType}`);
        }
      }
    }
  }

  assertSlurmApptainerImageAllowed(image: string): void {
    const slurm = this.requireSlurm();
    const allowlist = slurm.apptainer.image_allowlist ?? [];
    if (!allowlist.length) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied apptainer image (no allowlist configured): ${image}`);
    }
    if (!allowlist.includes(image)) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied apptainer image: ${image}`);
    }
  }

  assertSlurmNetworkNone(networkMode: string): void {
    const slurm = this.requireSlurm();
    if (slurm.network_mode_required !== "none") {
      throw new McpError(ErrorCode.InvalidRequest, `policy invalid slurm.network_mode_required (must be none)`);
    }
    if (networkMode !== "none") {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied slurm network_mode: ${networkMode}`);
    }
  }

  slurmMaxCollectOutputBytes(): bigint {
    const slurm = this.requireSlurm();
    const value = slurm.max_collect_output_bytes;
    if (!Number.isInteger(value) || value < 1) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        `policy denied slurm collect outputs (no max_collect_output_bytes configured)`
      );
    }
    return BigInt(value);
  }

  slurmMaxCollectLogBytes(): bigint {
    const slurm = this.requireSlurm();
    const value = slurm.max_collect_log_bytes;
    if (!Number.isInteger(value) || value < 1) {
      throw new McpError(ErrorCode.InvalidRequest, `policy denied slurm collect logs (no max_collect_log_bytes configured)`);
    }
    return BigInt(value);
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
