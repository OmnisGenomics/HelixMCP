import { mkdtemp, rm, readFile } from "fs/promises";
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
import { exportBundleToDir } from "../src/bundle/bundleExport.js";
import { verifyBundleDir } from "../src/bundle/bundleVerify.js";

async function callTool(client: Client, name: string, args: Record<string, unknown>, timeoutMs = 60_000) {
  return client.request(
    { method: "tools/call", params: { name, arguments: args } },
    CallToolResultSchema,
    { timeout: timeoutMs }
  );
}

async function main() {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "helixmcp-experiment-"));
  console.log("Experiment tmpDir:", tmpDir);

  const mem = newDb({ autoCreateForeignKeyIndices: true });
  const adapter = mem.adapters.createPg();
  const pool = new adapter.Pool() as unknown as pg.Pool;
  await applySqlFile(pool, path.resolve("db/schema.sql"));

  const db = createDb(pool);
  const store = new PostgresStore(db);
  const objectsDir = path.join(tmpDir, "objects");
  const objects = new LocalObjectStore(objectsDir);
  const artifacts = new ArtifactService(store, objects);
  const policy = await PolicyEngine.loadFromFile(path.resolve("policies/default.policy.yaml"));
  const runsDir = path.join(tmpDir, "runs");
  const execution = new DefaultExecutionService({ policy });

  const server = createGatewayServer({ policy, store, artifacts, execution, runsDir });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);

  const client = new Client({ name: "helixmcp-experiment-client", version: "0.0.0" });
  await client.connect(clientTransport);

  const projectId = newProjectId();
  console.log("Project ID:", projectId);

  // Experiment 1: Artifact import inline text
  console.log("\n[Experiment 1] Import inline text artifact...");
  const importRes = await callTool(client, "artifact_import", {
    project_id: projectId,
    type_hint: "TEXT",
    label: "sample_manifest.txt",
    source: { kind: "inline_text", text: "sample_id,condition\nS1,control\nS2,treatment\n" }
  });
  if (importRes.isError) throw new Error("artifact_import failed");
  const artifactId = (importRes.structuredContent as any).artifact.artifact_id;
  console.log("Artifact ID:", artifactId);

  // Experiment 2: List artifacts
  console.log("\n[Experiment 2] List artifacts...");
  const listRes = await callTool(client, "artifact_list", { project_id: projectId });
  console.log("Artifacts:", JSON.stringify((listRes.structuredContent as any).artifacts, null, 2));

  // Experiment 3: Preview text artifact
  console.log("\n[Experiment 3] Preview text artifact...");
  const previewRes = await callTool(client, "artifact_preview_text", { artifact_id: artifactId });
  console.log("Preview:", JSON.stringify(previewRes.structuredContent, null, 2));

  // Experiment 4: Simulate QC on FASTQ
  console.log("\n[Experiment 4] Simulate QC on FASTQ...");
  const fastqImport = await callTool(client, "artifact_import", {
    project_id: projectId,
    type_hint: "FASTQ_GZ",
    label: "reads_1.fastq.gz",
    source: { kind: "inline_text", text: "@r1\nACGTACGTACGTACGTACGTACGTACGTACGTACGT\n+\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n" }
  });
  if (fastqImport.isError) throw new Error("fastq import failed");
  const readsId = (fastqImport.structuredContent as any).artifact.artifact_id;

  const qcRes = await callTool(client, "simulate_qc_fastq", {
    project_id: projectId,
    reads_1: readsId,
    threads: 2
  });
  console.log("QC Result:", JSON.stringify(qcRes.structuredContent, null, 2));

  // Experiment 5: Simulate alignment
  console.log("\n[Experiment 5] Simulate alignment...");
  const alignRes = await callTool(client, "simulate_align_reads", {
    project_id: projectId,
    reads_1: readsId,
    reference: { alias: "hg38" },
    threads: 4,
    sort: true,
    mark_duplicates: false
  });
  console.log("Alignment Result:", JSON.stringify(alignRes.structuredContent, null, 2));

  // Experiment 6: Export Nextflow stub
  console.log("\n[Experiment 6] Export Nextflow stub...");
  const runId = (alignRes.structuredContent as any).provenance_run_id;
  const exportRes = await callTool(client, "export_nextflow", {
    run_id: runId
  });
  console.log("Export Result:", JSON.stringify(exportRes.structuredContent, null, 2));

  // Experiment 7: Offline bundle export and verify
  console.log("\n[Experiment 7] Offline bundle export and verify...");
  const bundleDir = path.join(tmpDir, "bundle");
  const bundleRes = await exportBundleToDir(
    { rootRunId: runId, outDir: bundleDir, includeBlobs: "all", maxBlobBytes: 1000000n, verifyAfterWrite: true },
    { store, objectStoreDir: objectsDir }
  );
  console.log("Bundle manifest SHA256:", bundleRes.manifestSha256);
  await verifyBundleDir(bundleDir);
  console.log("Bundle verify: PASS");

  // Show manifest snippet
  const manifest = JSON.parse(await readFile(path.join(bundleDir, "manifest.json"), "utf-8"));
  console.log("Bundle root_run_id:", manifest.root_run_id);
  console.log("Bundle files:", manifest.files.map((f: any) => f.path));

  // Cleanup
  await clientTransport.close();
  await serverTransport.close();
  await pool.end();
  await rm(tmpDir, { recursive: true, force: true });
  console.log("\nAll experiments completed. Cleaned up tmpDir.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
