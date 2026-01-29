import { describe, it, expect } from "vitest";
import { mkdtemp, rm } from "fs/promises";
import os from "os";
import path from "path";

import { newDb } from "pg-mem";
import * as pg from "pg";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { CallToolResultSchema } from "@modelcontextprotocol/sdk/types.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import * as z from "zod/v4";

import { applySqlFile } from "../src/db/bootstrap.js";
import { createDb } from "../src/db/connection.js";
import { PostgresStore } from "../src/store/postgresStore.js";
import { LocalObjectStore } from "../src/artifacts/localObjectStore.js";
import { ArtifactService } from "../src/artifacts/artifactService.js";
import { PolicyEngine } from "../src/policy/policy.js";
import { registerToolDefinitions } from "../src/toolpacks/register.js";
import { deriveRunId } from "../src/runs/runIdentity.js";

describe("toolpacks", () => {
  it("fails closed on invalid tool definitions", () => {
    const mcp = new McpServer({ name: "helixmcp-test", version: "0.0.0" });
    expect(() =>
      registerToolDefinitions(
        mcp,
        {} as any,
        [
          {
            toolName: "NotValid",
            contractVersion: "v1",
            planKind: "docker",
            description: "bad",
            inputSchema: z.object({}),
            outputSchema: z.object({}),
            declaredOutputs: [],
            canonicalize: async () => {
              throw new Error("unreachable");
            },
            run: async () => {
              throw new Error("unreachable");
            }
          }
        ] as any
      )
    ).toThrow();
  });

  it("rejects tool success if declared outputs are missing", async () => {
    const tmpDir = await mkdtemp(path.join(os.tmpdir(), "helixmcp-toolpacks-"));
    let pool: pg.Pool | null = null;
    let client: Client | null = null;
    let clientTransport: InMemoryTransport | null = null;
    let serverTransport: InMemoryTransport | null = null;

    try {
      const mem = newDb({ autoCreateForeignKeyIndices: true });
      const adapter = mem.adapters.createPg();
      pool = new adapter.Pool() as unknown as pg.Pool;
      await applySqlFile(pool, path.resolve("db/schema.sql"));

      const db = createDb(pool);
      const store = new PostgresStore(db);
      const objects = new LocalObjectStore(path.join(tmpDir, "objects"));
      const artifacts = new ArtifactService(store, objects);
      const runsDir = path.join(tmpDir, "runs");

      const policy = new PolicyEngine({
        version: 1,
        runtime: { instance_id: "local" },
        tool_allowlist: ["dummy_tool"],
        quotas: { max_threads: 1, max_runtime_seconds: 10, max_import_bytes: 1 },
        imports: { allow_source_kinds: ["inline_text"], local_path_prefix_allowlist: [], deny_symlinks: true },
        docker: {
          network_mode: "none",
          image_allowlist: ["example.com/dummy@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
        }
      });

      const mcp = new McpServer({ name: "helixmcp-test", version: "0.0.0" });

      const dummyTool = {
        toolName: "dummy_tool",
        contractVersion: "v1",
        planKind: "docker" as const,
        description: "Deliberately missing declared outputs (test only).",
        inputSchema: z.object({ project_id: z.string() }),
        outputSchema: z.object({ provenance_run_id: z.string(), log_artifact_id: z.string() }),
        declaredOutputs: [{ role: "report", type: "TEXT", label: "report.txt" }],
        canonicalize: async (args: any) => {
          return {
            projectId: args.project_id,
            canonicalParams: { project_id: args.project_id, stable: true },
            toolVersion: "v1",
            plan: {
              image: "example.com/dummy@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
              argv: ["true"],
              workdir: "/work",
              inputs: [],
              resources: { threads: 1, runtimeSeconds: 10 }
            },
            inputsToLink: []
          };
        },
        run: async ({ toolRun }: any) => {
          await toolRun.event("dummy", "did nothing", null);
          return { summary: "ok", result: {} };
        }
      };

      registerToolDefinitions(mcp, { policy, store, artifacts, runsDir }, [dummyTool] as any);

      [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
      await mcp.connect(serverTransport);

      client = new Client({ name: "helixmcp-test-client", version: "0.0.0" });
      await client.connect(clientTransport);

      const projectId = "proj_01HZZZZZZZZZZZZZZZZZZZZZZZ";
      const res = await client.request(
        { method: "tools/call", params: { name: "dummy_tool", arguments: { project_id: projectId } } },
        CallToolResultSchema
      );
      expect(res.isError).toBe(true);

      const { runId } = deriveRunId({
        toolName: "dummy_tool",
        contractVersion: "v1",
        policyHash: policy.policyHash,
        canonicalParams: { project_id: projectId, stable: true }
      });

      const run = await store.getRun(runId);
      expect(run?.status).toBe("failed");
    } finally {
      if (clientTransport) await clientTransport.close();
      if (serverTransport) await serverTransport.close();
      if (pool) await pool.end();
      await rm(tmpDir, { recursive: true, force: true });
    }
  });

  it("fails closed on docker plan input destName collisions", async () => {
    const tmpDir = await mkdtemp(path.join(os.tmpdir(), "helixmcp-toolpacks-"));
    let pool: pg.Pool | null = null;
    let client: Client | null = null;
    let clientTransport: InMemoryTransport | null = null;
    let serverTransport: InMemoryTransport | null = null;

    try {
      const mem = newDb({ autoCreateForeignKeyIndices: true });
      const adapter = mem.adapters.createPg();
      pool = new adapter.Pool() as unknown as pg.Pool;
      await applySqlFile(pool, path.resolve("db/schema.sql"));

      const db = createDb(pool);
      const store = new PostgresStore(db);
      const objects = new LocalObjectStore(path.join(tmpDir, "objects"));
      const artifacts = new ArtifactService(store, objects);
      const runsDir = path.join(tmpDir, "runs");

      const policy = new PolicyEngine({
        version: 1,
        runtime: { instance_id: "local" },
        tool_allowlist: ["collision_tool"],
        quotas: { max_threads: 1, max_runtime_seconds: 10, max_import_bytes: 1024 },
        imports: { allow_source_kinds: ["inline_text"], local_path_prefix_allowlist: [], deny_symlinks: true },
        docker: {
          network_mode: "none",
          image_allowlist: ["example.com/dummy@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
        }
      });

      const projectId = "proj_01HZZZZZZZZZZZZZZZZZZZZZZZ";
      const a = await artifacts.importArtifact({
        projectId: projectId as any,
        source: { kind: "inline_text", text: "a\n" },
        typeHint: "TEXT",
        label: "a.txt",
        createdByRunId: null,
        maxBytes: null
      });
      const b = await artifacts.importArtifact({
        projectId: projectId as any,
        source: { kind: "inline_text", text: "b\n" },
        typeHint: "TEXT",
        label: "b.txt",
        createdByRunId: null,
        maxBytes: null
      });

      const mcp = new McpServer({ name: "helixmcp-test", version: "0.0.0" });

      const collisionTool = {
        toolName: "collision_tool",
        contractVersion: "v1",
        planKind: "docker" as const,
        description: "Deliberately collides plan.inputs destName (test only).",
        inputSchema: z.object({ project_id: z.string(), a_id: z.string(), b_id: z.string() }),
        outputSchema: z.object({}),
        declaredOutputs: [],
        canonicalize: async (args: any, ctx: any) => {
          const ar = await ctx.artifacts.getArtifact(args.a_id);
          const br = await ctx.artifacts.getArtifact(args.b_id);
          if (!ar || !br) throw new Error("missing test artifacts");
          return {
            projectId: args.project_id,
            canonicalParams: { project_id: args.project_id, a: ar.checksumSha256, b: br.checksumSha256 },
            toolVersion: "v1",
            plan: {
              image: "example.com/dummy@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
              argv: ["true"],
              workdir: "/work",
              inputs: [
                { role: "a", artifact: ar, destName: "input" },
                { role: "b", artifact: br, destName: "input" }
              ],
              resources: { threads: 1, runtimeSeconds: 10 }
            },
            inputsToLink: [
              { artifactId: ar.artifactId, role: "a" },
              { artifactId: br.artifactId, role: "b" }
            ]
          };
        },
        run: async () => {
          throw new Error("unreachable");
        }
      };

      registerToolDefinitions(mcp, { policy, store, artifacts, runsDir }, [collisionTool] as any);

      [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
      await mcp.connect(serverTransport);

      client = new Client({ name: "helixmcp-test-client", version: "0.0.0" });
      await client.connect(clientTransport);

      const res = await client.request(
        {
          method: "tools/call",
          params: { name: "collision_tool", arguments: { project_id: projectId, a_id: a.artifactId, b_id: b.artifactId } }
        },
        CallToolResultSchema
      );

      expect(res.isError).toBe(true);
    } finally {
      if (clientTransport) await clientTransport.close();
      if (serverTransport) await serverTransport.close();
      if (pool) await pool.end();
      await rm(tmpDir, { recursive: true, force: true });
    }
  });
});
