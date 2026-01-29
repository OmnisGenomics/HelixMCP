import { describe, it, expect } from "vitest";
import { mkdtemp, writeFile, symlink, rm } from "fs/promises";
import os from "os";
import path from "path";
import { McpError, ErrorCode } from "@modelcontextprotocol/sdk/types.js";
import { PolicyEngine } from "../src/policy/policy.js";

describe("PolicyEngine", () => {
  it("loads default policy and enforces allowlists", async () => {
    const policy = await PolicyEngine.loadFromFile(path.resolve("policies/default.policy.yaml"));
    expect(() => policy.assertToolAllowed("artifact_import")).not.toThrow();
    expect(() => policy.assertToolAllowed("definitely_not_allowed")).toThrow(McpError);
  });

  it("enforces docker image allowlist", async () => {
    const policy = await PolicyEngine.loadFromFile(path.resolve("policies/default.policy.yaml"));
    expect(() =>
      policy.assertDockerImageAllowed(
        "quay.io/biocontainers/seqkit@sha256:67c9a1cfeafbccfd43bbd1fbb80646c9faa06a50b22c8ea758c3c84268b6765d"
      )
    ).not.toThrow();
    expect(() => policy.assertDockerImageAllowed("docker.io/library/alpine:latest")).toThrow(McpError);
  });

  it("enforces pinned docker image digests and network none", async () => {
    const policy = await PolicyEngine.loadFromFile(path.resolve("policies/default.policy.yaml"));
    expect(() =>
      policy.assertDockerImagePinned(
        "quay.io/biocontainers/seqkit@sha256:67c9a1cfeafbccfd43bbd1fbb80646c9faa06a50b22c8ea758c3c84268b6765d"
      )
    ).not.toThrow();
    expect(() => policy.assertDockerImagePinned("docker.io/library/alpine:latest")).toThrow(McpError);
    expect(() => policy.assertDockerNetworkNone()).not.toThrow();

    const bridgePolicy = new PolicyEngine({
      version: 1,
      runtime: { instance_id: "local" },
      tool_allowlist: [],
      quotas: { max_threads: 1, max_runtime_seconds: 1, max_import_bytes: 1 },
      imports: { allow_source_kinds: ["inline_text"], local_path_prefix_allowlist: [], deny_symlinks: true },
      docker: { network_mode: "bridge", image_allowlist: [] }
    });
    expect(() => bridgePolicy.assertDockerNetworkNone()).toThrow(McpError);
  });

  it("enforces local_path prefix allowlist", async () => {
    const policy = await PolicyEngine.loadFromFile(path.resolve("policies/default.policy.yaml"));
    const dir = await mkdtemp(path.join(os.tmpdir(), "helixmcp-policy-"));
    try {
      const okPath = path.join(dir, "ok.txt");
      await writeFile(okPath, "ok\n");
      await expect(policy.validateLocalPathImport(okPath)).resolves.toBeDefined();

      try {
        await policy.validateLocalPathImport("/etc/passwd");
        throw new Error("expected throw");
      } catch (e) {
        expect(e).toBeInstanceOf(McpError);
        const err = e as McpError;
        expect(err.code).toBe(ErrorCode.InvalidRequest);
      }

      const linkPath = path.join(dir, "passwd.link");
      await symlink("/etc/passwd", linkPath);
      await expect(policy.validateLocalPathImport(linkPath)).rejects.toBeInstanceOf(McpError);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
});
