import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { mkdtemp, rm } from "fs/promises";
import os from "os";
import path from "path";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { CallToolResultSchema, ListToolsResultSchema } from "@modelcontextprotocol/sdk/types.js";

import { newDb } from "pg-mem";
import * as pg from "pg";

import { applySqlFile } from "../src/db/bootstrap.js";
import { createDb } from "../src/db/connection.js";
import { PostgresStore } from "../src/store/postgresStore.js";
import { LocalObjectStore } from "../src/artifacts/localObjectStore.js";
import { ArtifactService } from "../src/artifacts/artifactService.js";
import { PolicyEngine } from "../src/policy/policy.js";
import { createGatewayServer } from "../src/mcp/gatewayServer.js";
import { newProjectId } from "../src/core/ids.js";
import { DefaultExecutionService } from "../src/execution/executionService.js";

describe("gateway (in-memory)", () => {
  let tmpDir: string;
  let pool: pg.Pool;
  let store: PostgresStore;
  let client: Client;
  let serverTransport: InMemoryTransport;
  let clientTransport: InMemoryTransport;
  let server: ReturnType<typeof createGatewayServer>;

  beforeAll(async () => {
    tmpDir = await mkdtemp(path.join(os.tmpdir(), "helixmcp-"));

    const mem = newDb({ autoCreateForeignKeyIndices: true });
    const adapter = mem.adapters.createPg();
    pool = new adapter.Pool() as unknown as pg.Pool;
    await applySqlFile(pool, path.resolve("db/schema.sql"));

    const db = createDb(pool);
    store = new PostgresStore(db);
    const objects = new LocalObjectStore(path.join(tmpDir, "objects"));
    const artifacts = new ArtifactService(store, objects);
    const policy = await PolicyEngine.loadFromFile(path.resolve("policies/default.policy.yaml"));
    const execution = new DefaultExecutionService(policy);

    server = createGatewayServer({ policy, store, artifacts, execution });

    [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    await server.connect(serverTransport);

    client = new Client({ name: "helixmcp-test-client", version: "0.0.0" });
    await client.connect(clientTransport);
  });

  afterAll(async () => {
    await clientTransport.close();
    await serverTransport.close();
    await pool.end();
    await rm(tmpDir, { recursive: true, force: true });
  });

  it("lists tools", async () => {
    const result = await client.request({ method: "tools/list", params: {} }, ListToolsResultSchema);
    const names = new Set(result.tools.map((t) => t.name));
    expect(names.has("artifact_import")).toBe(true);
    expect(names.has("simulate_align_reads")).toBe(true);
  });

  it("imports an artifact and previews it", async () => {
    const projectId = newProjectId();

    const imported = await client.request(
      {
        method: "tools/call",
        params: {
          name: "artifact_import",
          arguments: {
            project_id: projectId,
            type_hint: "TEXT",
            label: "hello.txt",
            source: { kind: "inline_text", text: "hello\nworld\n" }
          }
        }
      },
      CallToolResultSchema
    );
    if (imported.isError) {
      throw new Error(
        `artifact_import failed: ${imported.content
          .map((c) => (c.type === "text" ? c.text : c.type))
          .join("\n")}`
      );
    }

    const importedSc = imported.structuredContent as any;
    expect(importedSc.provenance_run_id).toMatch(/^run_/);
    expect(importedSc.artifact.artifact_id).toMatch(/^art_/);
    expect(importedSc.artifact.project_id).toBe(projectId);

    const artifactId = importedSc.artifact.artifact_id;

    const preview = await client.request(
      {
        method: "tools/call",
        params: { name: "artifact_preview_text", arguments: { artifact_id: artifactId, max_bytes: 64, max_lines: 10 } }
      },
      CallToolResultSchema
    );
    const previewSc = preview.structuredContent as any;
    expect(previewSc.preview).toContain("hello");
    expect(previewSc.preview).toContain("world");
  });

  it(
    "runs simulated alignment and exports nextflow",
    async () => {
      const projectId = newProjectId();
      const reads = await client.request(
        {
          method: "tools/call",
          params: {
            name: "artifact_import",
            arguments: {
              project_id: projectId,
              type_hint: "FASTQ_GZ",
              label: "reads_1.fastq.gz",
              source: { kind: "inline_text", text: "@r1\nACGT\n+\n!!!!\n" }
            }
          }
        },
        CallToolResultSchema
      );
      if (reads.isError) {
        throw new Error(
          `artifact_import failed: ${reads.content
            .map((c) => (c.type === "text" ? c.text : c.type))
            .join("\n")}`
        );
      }
      const readsId = (reads.structuredContent as any).artifact.artifact_id as string;

      const aligned = await client.request(
        {
          method: "tools/call",
          params: {
            name: "simulate_align_reads",
            arguments: {
              project_id: projectId,
              reads_1: readsId,
              reference: { alias: "hg38" },
              threads: 4,
              sort: true,
              mark_duplicates: false
            }
          }
        },
        CallToolResultSchema
      );

      const alignedSc = aligned.structuredContent as any;
      expect(alignedSc.provenance_run_id).toMatch(/^run_/);
      expect(alignedSc.bam_sorted).toMatch(/^art_/);

      const exported = await client.request(
        {
          method: "tools/call",
          params: { name: "export_nextflow", arguments: { run_id: alignedSc.provenance_run_id } }
        },
        CallToolResultSchema
      );
      const exportedSc = exported.structuredContent as any;
      expect(exportedSc.exported_run_id).toBe(alignedSc.provenance_run_id);
      expect(exportedSc.nextflow_script_artifact_id).toMatch(/^art_/);
    },
    20_000
  );

  it(
    "derives deterministic run_id and replays",
    async () => {
      const projectId = newProjectId();
      const reads = await client.request(
        {
          method: "tools/call",
          params: {
            name: "artifact_import",
            arguments: {
              project_id: projectId,
              type_hint: "FASTQ_GZ",
              label: "reads_1.fastq.gz",
              source: { kind: "inline_text", text: "@r1\nACGT\n+\n!!!!\n" }
            }
          }
        },
        CallToolResultSchema
      );
      if (reads.isError) {
        throw new Error(
          `artifact_import failed: ${reads.content
            .map((c) => (c.type === "text" ? c.text : c.type))
            .join("\n")}`
        );
      }
      const readsId = (reads.structuredContent as any).artifact.artifact_id as string;

      const aligned1 = await client.request(
        {
          method: "tools/call",
          params: {
            name: "simulate_align_reads",
            arguments: {
              project_id: projectId,
              reads_1: readsId,
              reference: { alias: "hg38" },
              threads: 4,
              sort: true,
              mark_duplicates: false
            }
          }
        },
        CallToolResultSchema
      );
      if (aligned1.isError) {
        throw new Error(
          `simulate_align_reads failed: ${aligned1.content
            .map((c) => (c.type === "text" ? c.text : c.type))
            .join("\n")}`
        );
      }
      const sc1 = aligned1.structuredContent as any;

      const count1 = Number((await pool.query("SELECT COUNT(*) AS c FROM param_sets")).rows[0]?.c);
      const run1 = await store.getRun(sc1.provenance_run_id);
      expect(run1?.paramsHash).toMatch(/^sha256:/);
      expect(run1?.policyHash).toMatch(/^sha256:/);
      expect(run1?.resultJson).toBeTruthy();

      const aligned2 = await client.request(
        {
          method: "tools/call",
          params: {
            name: "simulate_align_reads",
            arguments: {
              project_id: projectId,
              reads_1: readsId,
              reference: { alias: "hg38" },
              threads: 4,
              sort: true,
              mark_duplicates: false
            }
          }
        },
        CallToolResultSchema
      );
      if (aligned2.isError) {
        throw new Error(
          `simulate_align_reads failed: ${aligned2.content
            .map((c) => (c.type === "text" ? c.text : c.type))
            .join("\n")}`
        );
      }
      const sc2 = aligned2.structuredContent as any;

      const count2 = Number((await pool.query("SELECT COUNT(*) AS c FROM param_sets")).rows[0]?.c);

      expect(aligned2.content[0]?.type).toBe("text");
      expect((aligned2.content[0] as any).text).toContain("Replayed");
      expect(sc2).toEqual(sc1);
      expect(count2).toBe(count1);
    },
    20_000
  );
});
