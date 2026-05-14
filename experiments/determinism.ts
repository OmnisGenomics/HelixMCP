import { mkdtemp, rm } from "fs/promises";
import os from "os";
import path from "path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { CallToolResultSchema } from "@modelcontextprotocol/sdk/types.js";
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

async function callTool(client: Client, name: string, args: Record<string, unknown>, timeoutMs = 60_000) {
  return client.request(
    { method: "tools/call", params: { name, arguments: args } },
    CallToolResultSchema,
    { timeout: timeoutMs }
  );
}

async function main() {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "helixmcp-determinism-"));
  console.log("Determinism experiment tmpDir:", tmpDir);

  const mem = newDb({ autoCreateForeignKeyIndices: true });
  const adapter = mem.adapters.createPg();
  const pool = new adapter.Pool() as unknown as pg.Pool;
  await applySqlFile(pool, path.resolve("db/schema.sql"));

  const db = createDb(pool);
  const store = new PostgresStore(db);
  const objects = new LocalObjectStore(path.join(tmpDir, "objects"));
  const artifacts = new ArtifactService(store, objects);
  const policy = await PolicyEngine.loadFromFile(path.resolve("policies/default.policy.yaml"));
  const runsDir = path.join(tmpDir, "runs");
  const execution = new DefaultExecutionService({ policy });

  const server = createGatewayServer({ policy, store, artifacts, execution, runsDir });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);

  const client = new Client({ name: "helixmcp-determinism-client", version: "0.0.0" });
  await client.connect(clientTransport);

  const projectId = newProjectId();
  console.log("Project ID:", projectId);

  // Import a FASTQ artifact
  const fastqImport = await callTool(client, "artifact_import", {
    project_id: projectId,
    type_hint: "FASTQ_GZ",
    label: "reads_1.fastq.gz",
    source: { kind: "inline_text", text: "@r1\nACGTACGTACGTACGTACGTACGTACGTACGTACGT\n+\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n" }
  });
  if (fastqImport.isError) throw new Error("fastq import failed");
  const readsId = (fastqImport.structuredContent as any).artifact.artifact_id;

  // Run 1: simulate_align_reads
  console.log("\n[Run 1] simulate_align_reads...");
  const align1 = await callTool(client, "simulate_align_reads", {
    project_id: projectId,
    reads_1: readsId,
    reference: { alias: "hg38" },
    threads: 4,
    sort: true,
    mark_duplicates: false
  });
  const sc1 = align1.structuredContent as any;
  console.log("Run 1 run_id:", sc1.provenance_run_id);
  console.log("Run 1 bam_sorted:", sc1.bam_sorted);

  // Count param_sets after run 1
  const count1 = Number((await pool.query("SELECT COUNT(*) AS c FROM param_sets")).rows[0]?.c);
  console.log("Param sets after run 1:", count1);

  // Run 2: identical call should replay
  console.log("\n[Run 2] Identical simulate_align_reads (should replay)...");
  const align2 = await callTool(client, "simulate_align_reads", {
    project_id: projectId,
    reads_1: readsId,
    reference: { alias: "hg38" },
    threads: 4,
    sort: true,
    mark_duplicates: false
  });
  const sc2 = align2.structuredContent as any;
  console.log("Run 2 run_id:", sc2.provenance_run_id);

  // Check text content for "Replayed"
  const textContent = (align2.content[0] as any)?.text ?? "";
  console.log("Run 2 text:", textContent);

  const count2 = Number((await pool.query("SELECT COUNT(*) AS c FROM param_sets")).rows[0]?.c);
  console.log("Param sets after run 2:", count2);

  // Assertions
  if (sc2.provenance_run_id !== sc1.provenance_run_id) {
    throw new Error(`run_id mismatch: ${sc2.provenance_run_id} !== ${sc1.provenance_run_id}`);
  }
  if (sc2.bam_sorted !== sc1.bam_sorted) {
    throw new Error(`bam_sorted mismatch: ${sc2.bam_sorted} !== ${sc1.bam_sorted}`);
  }
  if (count2 !== count1) {
    throw new Error(`param_sets count changed: ${count2} !== ${count1}`);
  }
  if (!textContent.includes("Replayed")) {
    throw new Error("Expected 'Replayed' in response text");
  }

  console.log("\n✅ Determinism experiment PASSED: identical inputs produced identical outputs with replay.");

  // Cleanup
  await clientTransport.close();
  await serverTransport.close();
  await pool.end();
  await rm(tmpDir, { recursive: true, force: true });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
