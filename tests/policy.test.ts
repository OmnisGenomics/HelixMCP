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
