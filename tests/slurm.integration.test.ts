import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { mkdtemp, rm, writeFile, readFile } from "fs/promises";
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
import { PolicyEngine, type PolicyConfig } from "../src/policy/policy.js";
import { createGatewayServer } from "../src/mcp/gatewayServer.js";
import { newProjectId } from "../src/core/ids.js";
import { DefaultExecutionService } from "../src/execution/executionService.js";
import type { SlurmScheduler } from "../src/execution/slurm/scheduler.js";

const APPTAINER_SAMTOOLS_IMAGE =
  "docker://quay.io/biocontainers/samtools@sha256:bf80e07e650becfd084db1abde0fe932b50f990a07fa56421ea647b552b5a406";

describe.sequential("slurm (stubbed)", () => {
  let tmpDir: string;
  let runsDir: string;
  let pool: pg.Pool;
  let store: PostgresStore;
  let client: Client;
  let serverTransport: InMemoryTransport;
  let clientTransport: InMemoryTransport;
  let submitCalls = 0;
  let schedulerCalls = 0;
  let schedulerMode: "unavailable" | "running" = "running";

  beforeAll(async () => {
    tmpDir = await mkdtemp(path.join(os.tmpdir(), "helixmcp-slurm-"));
    runsDir = path.join(tmpDir, "runs");

    const mem = newDb({ autoCreateForeignKeyIndices: true });
    const adapter = mem.adapters.createPg();
    pool = new adapter.Pool() as unknown as pg.Pool;
    await applySqlFile(pool, path.resolve("db/schema.sql"));

    const db = createDb(pool);
    store = new PostgresStore(db);
    const objects = new LocalObjectStore(path.join(tmpDir, "objects"));
    const artifacts = new ArtifactService(store, objects);

    const policyConfig: PolicyConfig = {
      version: 1,
      runtime: { instance_id: "test" },
      tool_allowlist: ["artifact_import", "artifact_get", "slurm_submit", "slurm_job_get", "slurm_job_collect"],
      quotas: {
        max_threads: 16,
        max_runtime_seconds: 3600,
        max_import_bytes: 1024 * 1024,
        max_preview_bytes: 8192,
        max_preview_lines: 200
      },
      imports: {
        allow_source_kinds: ["inline_text"],
        local_path_prefix_allowlist: [],
        deny_symlinks: true
      },
      docker: {
        network_mode: "none",
        image_allowlist: []
      },
      slurm: {
        partitions_allowlist: ["short"],
        accounts_allowlist: ["bio"],
        qos_allowlist: [],
        constraints_allowlist: [],
        allow_scheduler_queries: true,
        max_time_limit_seconds: 7200,
        max_cpus: 32,
        max_mem_mb: 262144,
        max_gpus: 2,
        max_collect_output_bytes: 1024 * 1024,
        max_collect_log_bytes: 1024 * 1024,
        gpu_types_allowlist: [],
        apptainer: {
          image_allowlist: [APPTAINER_SAMTOOLS_IMAGE]
        },
        network_mode_required: "none"
      }
    };
    const policy = new PolicyEngine(policyConfig);

    const execution = new DefaultExecutionService({ policy });
    const slurmScheduler: SlurmScheduler = {
      query: async (_jobId: string) => {
        schedulerCalls += 1;
        if (schedulerMode === "unavailable") {
          return { info: null, warnings: ["sacct unavailable: fake"] };
        }
        return {
          info: { source: "squeue", stateRaw: "RUNNING", normalizedState: "running", exitCode: null },
          warnings: []
        };
      }
    };
    const server = createGatewayServer({
      policy,
      store,
      artifacts,
      execution,
      runsDir,
      slurmSubmitter: {
        submit: async (_scriptPath: string) => {
          submitCalls += 1;
          return { slurmJobId: "12345", stdout: "12345\n", stderr: "" };
        }
      },
      slurmScheduler
    });

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

  it("submits and collects a slurm job deterministically without resubmitting", async () => {
    const projectId = newProjectId();

    const imported = await client.request(
      {
        method: "tools/call",
        params: {
          name: "artifact_import",
          arguments: {
            project_id: projectId,
            type_hint: "BAM",
            label: "input.bam",
            source: { kind: "inline_text", text: "FAKEBAM\n" }
          }
        }
      },
      CallToolResultSchema
    );
    if (imported.isError) {
      throw new Error(
        `artifact_import failed: ${imported.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`
      );
    }

    const importedSc = imported.structuredContent as any;
    const bamId = importedSc.artifact.artifact_id as string;
    const bamChecksum = importedSc.artifact.checksum_sha256 as string;

    const jobSpec = {
      version: 1,
      resources: { time_limit_seconds: 60, cpus: 1, mem_mb: 512, gpus: null, gpu_type: null },
      placement: { partition: "short", account: "bio", qos: null, constraint: null },
      execution: {
        kind: "container",
        container: { engine: "apptainer", image: APPTAINER_SAMTOOLS_IMAGE, network_mode: "none", readonly_rootfs: true },
        command: {
          argv: ["sh", "-c", "set -euo pipefail; samtools flagstat /work/in/input.bam > /work/out/samtools_flagstat.txt"],
          workdir: "/work",
          env: {}
        }
      },
      io: {
        inputs: [{ role: "bam", artifact_id: bamId, checksum_sha256: bamChecksum, dest_relpath: "in/input.bam" }],
        outputs: [{ role: "report", src_relpath: "out/samtools_flagstat.txt", type: "TEXT", label: "samtools_flagstat.txt" }]
      }
    };

    const submit1 = await client.request(
      { method: "tools/call", params: { name: "slurm_submit", arguments: { project_id: projectId, job_spec: jobSpec } } },
      CallToolResultSchema
    );
    if (submit1.isError) {
      throw new Error(
        `slurm_submit failed: ${submit1.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`
      );
    }
    expect(submitCalls).toBe(1);

    const sc1 = submit1.structuredContent as any;
    expect(sc1.provenance_run_id).toMatch(/^run_/);
    expect(sc1.slurm_job_id).toBe("12345");
    expect(sc1.slurm_script_artifact_id).toMatch(/^art_/);

    const targetRunId = sc1.provenance_run_id as string;
    const targetRun = await store.getRun(targetRunId as any);
    expect(targetRun?.status).toBe("queued");
    expect(targetRun?.finishedAt).toBeNull();

    schedulerMode = "unavailable";
    const get1 = await client.request(
      { method: "tools/call", params: { name: "slurm_job_get", arguments: { run_id: targetRunId } } },
      CallToolResultSchema
    );
    if (get1.isError) {
      throw new Error(`slurm_job_get failed: ${get1.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`);
    }
    const getSc1 = get1.structuredContent as any;
    expect(getSc1.target_run_id).toBe(targetRunId);
    expect(getSc1.source).toBe("workspace_only");
    expect(getSc1.warnings.join("\n")).toContain("sacct unavailable");
    expect(getSc1.state).toBe("queued");

    schedulerMode = "running";
    const get2 = await client.request(
      { method: "tools/call", params: { name: "slurm_job_get", arguments: { run_id: targetRunId } } },
      CallToolResultSchema
    );
    if (get2.isError) {
      throw new Error(`slurm_job_get failed: ${get2.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`);
    }
    const getSc2 = get2.structuredContent as any;
    expect(getSc2.target_run_id).toBe(targetRunId);
    expect(getSc2.source).toBe("workspace+squeue");
    expect(getSc2.state).toBe("running");

    const materialized = await readFile(path.join(runsDir, targetRunId, "in", "input.bam"), "utf8");
    expect(materialized).toBe("FAKEBAM\n");

    await writeFile(path.join(runsDir, targetRunId, "out", "samtools_flagstat.txt"), "1 + 0 in total\n1 + 0 mapped\n", "utf8");
    await writeFile(path.join(runsDir, targetRunId, "meta", "stdout.txt"), "ok\n", "utf8");
    await writeFile(path.join(runsDir, targetRunId, "meta", "stderr.txt"), "", "utf8");
    await writeFile(path.join(runsDir, targetRunId, "meta", "exit_code.txt"), "0\n", "utf8");

    const get3 = await client.request(
      { method: "tools/call", params: { name: "slurm_job_get", arguments: { run_id: targetRunId } } },
      CallToolResultSchema
    );
    if (get3.isError) {
      throw new Error(`slurm_job_get failed: ${get3.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`);
    }
    const getSc3 = get3.structuredContent as any;
    expect(getSc3.target_run_id).toBe(targetRunId);
    expect(getSc3.state).toBe("succeeded");
    expect(getSc3.exit_code).toBe(0);

    const collect1 = await client.request(
      { method: "tools/call", params: { name: "slurm_job_collect", arguments: { run_id: targetRunId } } },
      CallToolResultSchema
    );
    if (collect1.isError) {
      throw new Error(
        `slurm_job_collect failed: ${collect1.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`
      );
    }

    const collectSc1 = collect1.structuredContent as any;
    expect(collectSc1.target_run_id).toBe(targetRunId);
    expect(collectSc1.exit_code).toBe(0);
    expect(collectSc1.artifacts_by_role.report).toMatch(/^art_/);

    const updated = await store.getRun(targetRunId as any);
    expect(updated?.status).toBe("succeeded");
    expect(updated?.exitCode).toBe(0);
    expect(updated?.finishedAt).toBeTypeOf("string");

    const reportId = collectSc1.artifacts_by_role.report as string;
    const report = await store.getArtifact(reportId as any);
    expect(report?.createdByRunId).toBe(targetRunId);

    const submit2 = await client.request(
      { method: "tools/call", params: { name: "slurm_submit", arguments: { project_id: projectId, job_spec: jobSpec } } },
      CallToolResultSchema
    );
    if (submit2.isError) {
      throw new Error(
        `slurm_submit failed: ${submit2.content.map((c) => (c.type === "text" ? c.text : c.type)).join("\n")}`
      );
    }

    expect(submitCalls).toBe(1);
    expect((submit2.content[0] as any).text).toContain("Replayed");
    expect(submit2.structuredContent).toEqual(sc1);
  });
});
