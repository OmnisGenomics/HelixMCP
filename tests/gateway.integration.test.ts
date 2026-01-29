import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { mkdtemp, rm, writeFile } from "fs/promises";
import os from "os";
import path from "path";
import { execSync, spawnSync } from "child_process";
import { gzipSync } from "zlib";

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
import { deriveRunId } from "../src/runs/runIdentity.js";

const DOCKER_ENABLED = process.env.HELIXMCP_TEST_DOCKER === "1";
const DOCKER_AVAILABLE =
  DOCKER_ENABLED &&
  (() => {
    try {
      execSync("docker version", { stdio: "ignore" });
      return true;
    } catch {
      return false;
    }
  })();

const SAMTOOLS_IMAGE =
  "quay.io/biocontainers/samtools@sha256:bf80e07e650becfd084db1abde0fe932b50f990a07fa56421ea647b552b5a406";

describe.sequential("gateway (in-memory)", () => {
  let tmpDir: string;
  let pool: pg.Pool;
  let store: PostgresStore;
  let policy: PolicyEngine;
  let client: Client;
  let serverTransport: InMemoryTransport;
  let clientTransport: InMemoryTransport;
  let server: ReturnType<typeof createGatewayServer>;

  async function callTool(name: string, args: Record<string, unknown>, timeoutMs = 60_000) {
    return client.request(
      { method: "tools/call", params: { name, arguments: args } },
      CallToolResultSchema,
      { timeout: timeoutMs }
    );
  }

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
    policy = await PolicyEngine.loadFromFile(path.resolve("policies/default.policy.yaml"));
    const runsDir = path.join(tmpDir, "runs");
    const execution = new DefaultExecutionService({ policy });

    server = createGatewayServer({ policy, store, artifacts, execution, runsDir });

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

  it("lists artifacts deterministically with snapshot replay", async () => {
    const projectId = newProjectId();

    const a1 = await client.request(
      {
        method: "tools/call",
        params: {
          name: "artifact_import",
          arguments: {
            project_id: projectId,
            type_hint: "TEXT",
            label: "a.txt",
            source: { kind: "inline_text", text: "a\n" }
          }
        }
      },
      CallToolResultSchema
    );
    if (a1.isError) {
      throw new Error(
        `artifact_import failed: ${a1.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`
      );
    }

    const list1 = await client.request(
      { method: "tools/call", params: { name: "artifact_list", arguments: { project_id: projectId, limit: 100 } } },
      CallToolResultSchema
    );
    if (list1.isError) {
      throw new Error(
        `artifact_list failed: ${list1.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`
      );
    }

    const sc1 = list1.structuredContent as any;
    expect(sc1.artifact_count).toBe("1");
    expect(sc1.as_of_created_at).toBeTypeOf("string");
    expect(sc1.artifacts).toHaveLength(1);

    const list2 = await client.request(
      { method: "tools/call", params: { name: "artifact_list", arguments: { project_id: projectId, limit: 100 } } },
      CallToolResultSchema
    );
    if (list2.isError) {
      throw new Error(
        `artifact_list failed: ${list2.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`
      );
    }
    const sc2 = list2.structuredContent as any;
    expect(list2.content[0]?.type).toBe("text");
    expect((list2.content[0] as any).text).toContain("Replayed");
    expect(sc2).toEqual(sc1);

    const a2 = await client.request(
      {
        method: "tools/call",
        params: {
          name: "artifact_import",
          arguments: {
            project_id: projectId,
            type_hint: "TEXT",
            label: "b.txt",
            source: { kind: "inline_text", text: "b\n" }
          }
        }
      },
      CallToolResultSchema
    );
    if (a2.isError) {
      throw new Error(
        `artifact_import failed: ${a2.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`
      );
    }

    const list3 = await client.request(
      { method: "tools/call", params: { name: "artifact_list", arguments: { project_id: projectId, limit: 100 } } },
      CallToolResultSchema
    );
    if (list3.isError) {
      throw new Error(
        `artifact_list failed: ${list3.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`
      );
    }

    const sc3 = list3.structuredContent as any;
    expect((list3.content[0] as any).text).not.toContain("Replayed");
    expect(sc3.provenance_run_id).not.toBe(sc1.provenance_run_id);
    expect(sc3.artifact_count).toBe("2");
    expect(sc3.artifacts).toHaveLength(2);
  });

  it("reports docker job state from the DB", async () => {
    const projectId = newProjectId();

    const canonicalParams = {
      project_id: projectId,
      docker: {
        image: "example.com/dummy@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        network_mode: "none",
        argv: ["true"]
      }
    };

    const { runId, paramsHash } = deriveRunId({
      toolName: "dummy_docker_run",
      contractVersion: "v1",
      policyHash: policy.policyHash,
      canonicalParams
    });

    await store.createRun({
      runId,
      projectId,
      toolName: "dummy_docker_run",
      contractVersion: "v1",
      toolVersion: "v1",
      paramsHash,
      canonicalParams: canonicalParams as any,
      policyHash: policy.policyHash,
      status: "running",
      requestedBy: null,
      policySnapshot: policy.snapshot() as any,
      environment: null
    });

    const get1 = await client.request(
      { method: "tools/call", params: { name: "docker_job_get", arguments: { run_id: runId } } },
      CallToolResultSchema
    );
    if (get1.isError) {
      throw new Error(`docker_job_get failed: ${get1.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`);
    }
    const sc1 = get1.structuredContent as any;
    expect(sc1.target_run_id).toBe(runId);
    expect(sc1.source).toBe("db_only");
    expect(sc1.state).toBe("running");
    expect(sc1.exit_code).toBeNull();

    await store.updateRun(runId as any, { status: "succeeded", finishedAt: new Date().toISOString(), exitCode: 0 });

    const get2 = await client.request(
      { method: "tools/call", params: { name: "docker_job_get", arguments: { run_id: runId } } },
      CallToolResultSchema
    );
    if (get2.isError) {
      throw new Error(`docker_job_get failed: ${get2.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`);
    }
    const sc2 = get2.structuredContent as any;
    expect(sc2.target_run_id).toBe(runId);
    expect(sc2.state).toBe("succeeded");
    expect(sc2.exit_code).toBe(0);
  });

  it.runIf(DOCKER_AVAILABLE)(
    "runs seqkit stats via docker and replays",
    async () => {
      const projectId = newProjectId();

      const fasta = await client.request(
        {
          method: "tools/call",
          params: {
            name: "artifact_import",
            arguments: {
              project_id: projectId,
              type_hint: "TEXT",
              label: "tiny.fasta",
              source: { kind: "inline_text", text: ">seq1\nACGTACGT\n>seq2\nTTTT\n" }
            }
          }
        },
        CallToolResultSchema
      );
      if (fasta.isError) {
        throw new Error(
          `artifact_import failed: ${fasta.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`
        );
      }
      const fastaId = (fasta.structuredContent as any).artifact.artifact_id as string;

      const stats1 = await client.request(
        {
          method: "tools/call",
          params: {
            name: "seqkit_stats",
            arguments: { project_id: projectId, input_artifact_id: fastaId, threads: 2 }
          }
        },
        CallToolResultSchema
      );
      if (stats1.isError) {
        throw new Error(
          `seqkit_stats failed: ${stats1.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`
        );
      }

      const sc1 = stats1.structuredContent as any;
      expect(sc1.provenance_run_id).toMatch(/^run_/);
      expect(sc1.report_artifact_id).toMatch(/^art_/);
      expect(sc1.stats.num_seqs).toBe(2);
      expect(sc1.stats.sum_len).toBe(12);
      expect(sc1.stats.min_len).toBe(4);
      expect(sc1.stats.avg_len).toBe(6);
      expect(sc1.stats.max_len).toBe(8);

      const report = await client.request(
        { method: "tools/call", params: { name: "artifact_get", arguments: { artifact_id: sc1.report_artifact_id } } },
        CallToolResultSchema
      );
      if (report.isError) {
        throw new Error(
          `artifact_get failed: ${report.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`
        );
      }
      const reportSc = report.structuredContent as any;
      expect(reportSc.artifact.type).toBe("TSV");
      expect(reportSc.artifact.created_by_run_id).toBe(sc1.provenance_run_id);

      const count1 = Number((await pool.query("SELECT COUNT(*) AS c FROM param_sets")).rows[0]?.c);

      const stats2 = await client.request(
        {
          method: "tools/call",
          params: {
            name: "seqkit_stats",
            arguments: { project_id: projectId, input_artifact_id: fastaId, threads: 2 }
          }
        },
        CallToolResultSchema
      );
      if (stats2.isError) {
        throw new Error(
          `seqkit_stats failed: ${stats2.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`
        );
      }

      const sc2 = stats2.structuredContent as any;
      const count2 = Number((await pool.query("SELECT COUNT(*) AS c FROM param_sets")).rows[0]?.c);

      expect((stats2.content[0] as any).text).toContain("Replayed");
      expect(sc2).toEqual(sc1);
      expect(count2).toBe(count1);
    },
    90_000
  );

  it.runIf(DOCKER_AVAILABLE)(
    "runs fastqc + multiqc via docker and replays",
    async () => {
      const projectId = newProjectId();

      const fastqText = ["@r1", "ACGT", "+", "!!!!", ""].join("\n");
      const gzPath = path.join(tmpDir, "reads.fastq.gz");
      await writeFile(gzPath, gzipSync(Buffer.from(fastqText, "utf8")));

      const imported = await callTool(
        "artifact_import",
        {
          project_id: projectId,
          type_hint: "FASTQ_GZ",
          label: "reads.fastq.gz",
          source: { kind: "local_path", path: gzPath }
        },
        240_000
      );
      if (imported.isError) {
        throw new Error(
          `artifact_import failed: ${imported.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`
        );
      }
      const readsId = (imported.structuredContent as any).artifact.artifact_id as string;

      const fastqc1 = await callTool(
        "fastqc",
        { project_id: projectId, reads_artifact_id: readsId, backend: "docker" },
        240_000
      );
      if (fastqc1.isError) {
        throw new Error(`fastqc failed: ${fastqc1.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`);
      }
      const f1 = fastqc1.structuredContent as any;
      expect(f1.provenance_run_id).toMatch(/^run_/);
      expect(f1.fastqc_html_artifact_id).toMatch(/^art_/);
      expect(f1.fastqc_zip_artifact_id).toMatch(/^art_/);
      expect(f1.metrics.pass).toBeTypeOf("number");
      expect(f1.metrics.warn).toBeTypeOf("number");
      expect(f1.metrics.fail).toBeTypeOf("number");

      const fastqc2 = await callTool(
        "fastqc",
        { project_id: projectId, reads_artifact_id: readsId, backend: "docker" },
        240_000
      );
      if (fastqc2.isError) {
        throw new Error(`fastqc failed: ${fastqc2.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`);
      }
      const f2 = fastqc2.structuredContent as any;
      expect((fastqc2.content[0] as any).text).toContain("Replayed");
      expect(f2.provenance_run_id).toBe(f1.provenance_run_id);
      expect(f2.fastqc_html_artifact_id).toBe(f1.fastqc_html_artifact_id);
      expect(f2.fastqc_zip_artifact_id).toBe(f1.fastqc_zip_artifact_id);

      const multiqc1 = await callTool(
        "multiqc",
        { project_id: projectId, fastqc_zip_artifact_ids: [f1.fastqc_zip_artifact_id], backend: "docker" },
        240_000
      );
      if (multiqc1.isError) {
        throw new Error(`multiqc failed: ${multiqc1.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`);
      }
      const m1 = multiqc1.structuredContent as any;
      expect(m1.provenance_run_id).toMatch(/^run_/);
      expect(m1.multiqc_html_artifact_id).toMatch(/^art_/);
      expect(m1.multiqc_data_zip_artifact_id).toMatch(/^art_/);

      const multiqc2 = await callTool(
        "multiqc",
        { project_id: projectId, fastqc_zip_artifact_ids: [f1.fastqc_zip_artifact_id], backend: "docker" },
        240_000
      );
      if (multiqc2.isError) {
        throw new Error(`multiqc failed: ${multiqc2.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`);
      }
      const m2 = multiqc2.structuredContent as any;
      expect((multiqc2.content[0] as any).text).toContain("Replayed");
      expect(m2.provenance_run_id).toBe(m1.provenance_run_id);
      expect(m2.multiqc_html_artifact_id).toBe(m1.multiqc_html_artifact_id);
      expect(m2.multiqc_data_zip_artifact_id).toBe(m1.multiqc_data_zip_artifact_id);
    },
    300_000
  );

  it.runIf(DOCKER_AVAILABLE)(
    "runs qc_bundle_fastq via docker and replays",
    async () => {
      const projectId = newProjectId();

      const fastqText = ["@r1", "ACGT", "+", "!!!!", ""].join("\n");
      const gzPath = path.join(tmpDir, "bundle_reads.fastq.gz");
      await writeFile(gzPath, gzipSync(Buffer.from(fastqText, "utf8")));

      const imported = await callTool(
        "artifact_import",
        {
          project_id: projectId,
          type_hint: "FASTQ_GZ",
          label: "reads.fastq.gz",
          source: { kind: "local_path", path: gzPath }
        },
        240_000
      );
      if (imported.isError) {
        throw new Error(
          `artifact_import failed: ${imported.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`
        );
      }
      const readsId = (imported.structuredContent as any).artifact.artifact_id as string;

      const bundle1 = await callTool(
        "qc_bundle_fastq",
        { project_id: projectId, reads_1_artifact_id: readsId, backend: "docker" },
        240_000
      );
      if (bundle1.isError) {
        throw new Error(
          `qc_bundle_fastq failed: ${bundle1.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`
        );
      }
      const b1 = bundle1.structuredContent as any;
      expect(b1.provenance_run_id).toMatch(/^run_/);
      expect(b1.bundle_report_artifact_id).toMatch(/^art_/);
      expect(b1.multiqc.run_id).toMatch(/^run_/);
      expect(b1.multiqc.multiqc_html_artifact_id).toMatch(/^art_/);
      expect(b1.multiqc.multiqc_data_zip_artifact_id).toMatch(/^art_/);
      expect(b1.fastqc.reads_1.metrics.pass).toBeTypeOf("number");
      expect(b1.fastqc.reads_1.metrics.warn).toBeTypeOf("number");
      expect(b1.fastqc.reads_1.metrics.fail).toBeTypeOf("number");

      const bundle2 = await callTool(
        "qc_bundle_fastq",
        { project_id: projectId, reads_1_artifact_id: readsId, backend: "docker" },
        240_000
      );
      if (bundle2.isError) {
        throw new Error(
          `qc_bundle_fastq failed: ${bundle2.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`
        );
      }
      const b2 = bundle2.structuredContent as any;
      expect((bundle2.content[0] as any).text).toContain("Replayed");
      expect(b2.provenance_run_id).toBe(b1.provenance_run_id);
      expect(b2.bundle_report_artifact_id).toBe(b1.bundle_report_artifact_id);
      expect(b2.multiqc.multiqc_html_artifact_id).toBe(b1.multiqc.multiqc_html_artifact_id);
      expect(b2.multiqc.multiqc_data_zip_artifact_id).toBe(b1.multiqc.multiqc_data_zip_artifact_id);
    },
    300_000
  );

  it.runIf(DOCKER_AVAILABLE)(
    "runs samtools flagstat via docker and replays",
    async () => {
      const projectId = newProjectId();

      const samPath = path.join(tmpDir, "input.sam");
      const bamPath = path.join(tmpDir, "input.bam");

      const sam = [
        "@HD\tVN:1.6\tSO:unsorted",
        "@SQ\tSN:chr1\tLN:1000",
        "r1\t0\tchr1\t1\t60\t4M\t*\t0\t0\tACGT\t!!!!"
      ].join("\n");

      await writeFile(samPath, sam + "\n", "utf8");

      const docker = spawnSync(
        "docker",
        [
          "run",
          "--rm",
          "--network",
          "none",
          "-v",
          `${tmpDir}:/work`,
          "-w",
          "/work",
          SAMTOOLS_IMAGE,
          "samtools",
          "view",
          "-bS",
          "/work/input.sam"
        ],
        { stdio: ["ignore", "pipe", "pipe"] }
      );
      if (docker.error) throw docker.error;
      if (docker.status !== 0) {
        const stderr = docker.stderr ? docker.stderr.toString("utf8") : "";
        throw new Error(`docker samtools view failed (exit ${docker.status})${stderr ? `: ${stderr}` : ""}`);
      }
      if (!docker.stdout || typeof docker.stdout === "string") {
        throw new Error("docker samtools view produced no BAM bytes");
      }
      const bamBytes = docker.stdout;
      await writeFile(bamPath, bamBytes);

      const imported = await callTool(
        "artifact_import",
        {
          project_id: projectId,
          type_hint: "BAM",
          label: "input.bam",
          source: { kind: "local_path", path: bamPath }
        },
        240_000
      );
      if (imported.isError) {
        throw new Error(
          `artifact_import failed: ${imported.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`
        );
      }
      const bamId = (imported.structuredContent as any).artifact.artifact_id as string;

      const flag1 = await callTool(
        "samtools_flagstat",
        { project_id: projectId, bam_artifact_id: bamId, backend: "docker" },
        240_000
      );
      if (flag1.isError) {
        throw new Error(
          `samtools_flagstat failed: ${flag1.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`
        );
      }

      const sc1 = flag1.structuredContent as any;
      expect(sc1.provenance_run_id).toMatch(/^run_/);
      expect(sc1.report_artifact_id).toMatch(/^art_/);
      expect(sc1.flagstat.total.passed).toBe(1);
      expect(sc1.flagstat.mapped.passed).toBe(1);

      const report = await client.request(
        { method: "tools/call", params: { name: "artifact_get", arguments: { artifact_id: sc1.report_artifact_id } } },
        CallToolResultSchema
      );
      if (report.isError) {
        throw new Error(
          `artifact_get failed: ${report.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`
        );
      }
      const reportSc = report.structuredContent as any;
      expect(reportSc.artifact.type).toBe("TEXT");
      expect(reportSc.artifact.created_by_run_id).toBe(sc1.provenance_run_id);

      const count1 = Number((await pool.query("SELECT COUNT(*) AS c FROM param_sets")).rows[0]?.c);

      const flag2 = await callTool(
        "samtools_flagstat",
        { project_id: projectId, bam_artifact_id: bamId, backend: "docker" },
        240_000
      );
      if (flag2.isError) {
        throw new Error(
          `samtools_flagstat failed: ${flag2.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`
        );
      }
      const sc2 = flag2.structuredContent as any;
      const count2 = Number((await pool.query("SELECT COUNT(*) AS c FROM param_sets")).rows[0]?.c);

      expect((flag2.content[0] as any).text).toContain("Replayed");
      expect(sc2).toEqual(sc1);
      expect(count2).toBe(count1);
    },
    300_000
  );

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
