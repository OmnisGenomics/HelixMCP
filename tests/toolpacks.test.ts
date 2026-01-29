import { describe, it, expect } from "vitest";
import { mkdtemp, rm, readFile } from "fs/promises";
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
import { executeSlurmPlan, type SlurmExecutionPlan } from "../src/toolpacks/slurm/executeSlurm.js";

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
            selectedPlanKind: "docker",
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
            selectedPlanKind: "docker",
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

  it("submits a slurm toolpack deterministically and replays without resubmitting", async () => {
    const tmpDir = await mkdtemp(path.join(os.tmpdir(), "helixmcp-toolpacks-"));
    let pool: pg.Pool | null = null;
    let client: Client | null = null;
    let clientTransport: InMemoryTransport | null = null;
    let serverTransport: InMemoryTransport | null = null;
    let submitCalls = 0;

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

      const APPTAINER_IMAGE =
        "docker://quay.io/biocontainers/samtools@sha256:bf80e07e650becfd084db1abde0fe932b50f990a07fa56421ea647b552b5a406";

      const policy = new PolicyEngine({
        version: 1,
        runtime: { instance_id: "local" },
        tool_allowlist: ["slurm_tool"],
        quotas: { max_threads: 4, max_runtime_seconds: 10, max_import_bytes: 1024 * 1024 },
        imports: { allow_source_kinds: ["inline_text"], local_path_prefix_allowlist: [], deny_symlinks: true },
        docker: { network_mode: "none", image_allowlist: [] },
        slurm: {
          partitions_allowlist: ["short"],
          accounts_allowlist: ["bio"],
          qos_allowlist: [],
          constraints_allowlist: [],
          max_time_limit_seconds: 3600,
          max_cpus: 4,
          max_mem_mb: 8192,
          max_gpus: 0,
          max_collect_output_bytes: 1024 * 1024,
          max_collect_log_bytes: 1024 * 1024,
          gpu_types_allowlist: [],
          apptainer: { image_allowlist: [APPTAINER_IMAGE] },
          network_mode_required: "none"
        }
      });

      const slurmSubmitter = {
        submit: async (_scriptPath: string) => {
          submitCalls += 1;
          return { slurmJobId: "99999", stdout: "99999\n", stderr: "" };
        }
      };

      const projectId = "proj_01HZZZZZZZZZZZZZZZZZZZZZZZ";
      const input = await artifacts.importArtifact({
        projectId: projectId as any,
        source: { kind: "inline_text", text: "FAKEBAM\n" },
        typeHint: "BAM",
        label: "input.bam",
        createdByRunId: null,
        maxBytes: null
      });

      const mcp = new McpServer({ name: "helixmcp-test", version: "0.0.0" });

      const slurmTool = {
        toolName: "slurm_tool",
        contractVersion: "v1",
        planKind: "slurm" as const,
        description: "Submit a Slurm plan via toolpacks (test only).",
        inputSchema: z.object({ project_id: z.string(), bam_artifact_id: z.string() }),
        outputSchema: z.object({}),
        declaredOutputs: [{ role: "report", type: "TEXT", label: "report.txt", srcRelpath: "out/report.txt" }],
        canonicalize: async (args: any, ctx: any) => {
          const bam = await ctx.artifacts.getArtifact(args.bam_artifact_id);
          if (!bam) throw new Error("missing bam");
          return {
            projectId: args.project_id,
            canonicalParams: {
              project_id: args.project_id,
              slurm: {
                spec_version: 1,
                slurm_script_version: "slurm_script_v1",
                resources: { time_limit_seconds: 60, cpus: 1, mem_mb: 512, gpus: null, gpu_type: null },
                placement: { partition: "short", account: "bio", qos: null, constraint: null },
                execution: {
                  kind: "container",
                  container: { engine: "apptainer", image: APPTAINER_IMAGE, network_mode: "none", readonly_rootfs: true },
                  command: { argv: ["samtools", "flagstat", "/work/in/input.bam"], workdir: "/work", env: {} }
                },
                io: {
                  inputs: [
                    {
                      role: "bam",
                      artifact_id: bam.artifactId,
                      checksum_sha256: bam.checksumSha256,
                      dest_relpath: "in/input.bam"
                    }
                  ],
                  outputs: [{ role: "report", src_relpath: "out/report.txt", type: "TEXT", label: "report.txt" }]
                }
              }
            },
            toolVersion: "slurm_script_v1",
            plan: {
              version: 1,
              resources: { time_limit_seconds: 60, cpus: 1, mem_mb: 512, gpus: null, gpu_type: null },
              placement: { partition: "short", account: "bio", qos: null, constraint: null },
              execution: {
                kind: "container",
                container: { engine: "apptainer", image: APPTAINER_IMAGE, network_mode: "none", readonly_rootfs: true },
                command: { argv: ["samtools", "flagstat", "/work/in/input.bam"], workdir: "/work", env: {} }
              },
              io: {
                inputs: [
                  {
                    role: "bam",
                    artifact_id: bam.artifactId,
                    checksum_sha256: bam.checksumSha256,
                    dest_relpath: "in/input.bam"
                  }
                ],
                outputs: [{ role: "report", src_relpath: "out/report.txt", type: "TEXT", label: "report.txt" }]
              }
            } satisfies SlurmExecutionPlan,
            selectedPlanKind: "slurm",
            inputsToLink: [{ artifactId: bam.artifactId, role: "bam" }]
          };
        },
        run: async ({ runId, toolRun, prepared, ctx }: any) => {
          if (!ctx.slurmSubmitter) throw new Error("missing slurmSubmitter");
          const submit = await executeSlurmPlan({
            policy: ctx.policy,
            artifacts: ctx.artifacts,
            runsDir: ctx.runsDir,
            runId,
            projectId: prepared.projectId,
            toolRun,
            plan: prepared.plan,
            submitter: ctx.slurmSubmitter
          });
          return {
            summary: "slurm submitted",
            result: { slurm_job_id: submit.slurmJobId, slurm_script_artifact_id: submit.slurmScriptArtifactId }
          };
        }
      };

      registerToolDefinitions(mcp, { policy, store, artifacts, runsDir, slurmSubmitter }, [slurmTool] as any);

      [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
      await mcp.connect(serverTransport);

      client = new Client({ name: "helixmcp-test-client", version: "0.0.0" });
      await client.connect(clientTransport);

      const submit1 = await client.request(
        {
          method: "tools/call",
          params: { name: "slurm_tool", arguments: { project_id: projectId, bam_artifact_id: input.artifactId } }
        },
        CallToolResultSchema
      );
      expect(submit1.isError).not.toBe(true);
      expect(submitCalls).toBe(1);
      const sc1 = submit1.structuredContent as any;
      expect(sc1.provenance_run_id).toMatch(/^run_/);
      expect(sc1.slurm_job_id).toBe("99999");
      expect(sc1.slurm_script_artifact_id).toMatch(/^art_/);

      const targetRunId = sc1.provenance_run_id as string;
      const materialized = await readFile(path.join(runsDir, targetRunId, "in", "input.bam"), "utf8");
      expect(materialized).toBe("FAKEBAM\n");

      const submit2 = await client.request(
        {
          method: "tools/call",
          params: { name: "slurm_tool", arguments: { project_id: projectId, bam_artifact_id: input.artifactId } }
        },
        CallToolResultSchema
      );
      expect(submit2.isError).not.toBe(true);
      expect(submitCalls).toBe(1);
      expect((submit2.content[0] as any).text).toContain("Replayed");
      expect(submit2.structuredContent).toEqual(sc1);
    } finally {
      if (clientTransport) await clientTransport.close();
      if (serverTransport) await serverTransport.close();
      if (pool) await pool.end();
      await rm(tmpDir, { recursive: true, force: true });
    }
  });

  it("submits a hybrid toolpack in slurm mode deterministically and replays without resubmitting", async () => {
    const tmpDir = await mkdtemp(path.join(os.tmpdir(), "helixmcp-toolpacks-"));
    let pool: pg.Pool | null = null;
    let client: Client | null = null;
    let clientTransport: InMemoryTransport | null = null;
    let serverTransport: InMemoryTransport | null = null;
    let submitCalls = 0;

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

      const APPTAINER_IMAGE =
        "docker://quay.io/biocontainers/samtools@sha256:bf80e07e650becfd084db1abde0fe932b50f990a07fa56421ea647b552b5a406";

      const policy = new PolicyEngine({
        version: 1,
        runtime: { instance_id: "local" },
        tool_allowlist: ["hybrid_slurm_tool"],
        quotas: { max_threads: 4, max_runtime_seconds: 10, max_import_bytes: 1024 * 1024 },
        imports: { allow_source_kinds: ["inline_text"], local_path_prefix_allowlist: [], deny_symlinks: true },
        docker: { network_mode: "none", image_allowlist: [] },
        slurm: {
          partitions_allowlist: ["short"],
          accounts_allowlist: ["bio"],
          qos_allowlist: [],
          constraints_allowlist: [],
          max_time_limit_seconds: 3600,
          max_cpus: 4,
          max_mem_mb: 8192,
          max_gpus: 0,
          max_collect_output_bytes: 1024 * 1024,
          max_collect_log_bytes: 1024 * 1024,
          gpu_types_allowlist: [],
          apptainer: { image_allowlist: [APPTAINER_IMAGE] },
          network_mode_required: "none"
        }
      });

      const slurmSubmitter = {
        submit: async (_scriptPath: string) => {
          submitCalls += 1;
          return { slurmJobId: "424242", stdout: "424242\n", stderr: "" };
        }
      };

      const projectId = "proj_01HZZZZZZZZZZZZZZZZZZZZZZZ";
      const input = await artifacts.importArtifact({
        projectId: projectId as any,
        source: { kind: "inline_text", text: "FAKEBAM\n" },
        typeHint: "BAM",
        label: "input.bam",
        createdByRunId: null,
        maxBytes: null
      });

      const mcp = new McpServer({ name: "helixmcp-test", version: "0.0.0" });

      const hybridTool = {
        toolName: "hybrid_slurm_tool",
        contractVersion: "v1",
        planKind: "hybrid" as const,
        description: "Hybrid toolpack (test only): slurm submit path",
        inputSchema: z.object({ project_id: z.string(), bam_artifact_id: z.string() }),
        outputSchema: z.object({}),
        declaredOutputs: [{ role: "report", type: "TEXT", label: "report.txt", srcRelpath: "out/report.txt" }],
        canonicalize: async (args: any, ctx: any) => {
          const bam = await ctx.artifacts.getArtifact(args.bam_artifact_id);
          if (!bam) throw new Error("missing bam");

          const plan: SlurmExecutionPlan = {
            version: 1,
            resources: { time_limit_seconds: 60, cpus: 1, mem_mb: 512, gpus: null, gpu_type: null },
            placement: { partition: "short", account: "bio", qos: null, constraint: null },
            execution: {
              kind: "container",
              container: { engine: "apptainer", image: APPTAINER_IMAGE, network_mode: "none", readonly_rootfs: true },
              command: { argv: ["samtools", "flagstat", "/work/in/input.bam"], workdir: "/work", env: {} }
            },
            io: {
              inputs: [
                { role: "bam", artifact_id: bam.artifactId, checksum_sha256: bam.checksumSha256, dest_relpath: "in/input.bam" }
              ],
              outputs: [{ role: "report", src_relpath: "out/report.txt", type: "TEXT", label: "report.txt" }]
            }
          };

          return {
            projectId: args.project_id,
            canonicalParams: { project_id: args.project_id, backend: "slurm", slurm: { spec_version: 1 } },
            toolVersion: "slurm_script_v1",
            plan,
            selectedPlanKind: "slurm",
            inputsToLink: [{ artifactId: bam.artifactId, role: "bam" }]
          };
        },
        run: async ({ runId, toolRun, prepared, ctx }: any) => {
          if (!ctx.slurmSubmitter) throw new Error("missing slurmSubmitter");
          const submit = await executeSlurmPlan({
            policy: ctx.policy,
            artifacts: ctx.artifacts,
            runsDir: ctx.runsDir,
            runId,
            projectId: prepared.projectId,
            toolRun,
            plan: prepared.plan,
            submitter: ctx.slurmSubmitter
          });
          return {
            summary: "slurm submitted",
            result: { slurm_job_id: submit.slurmJobId, slurm_script_artifact_id: submit.slurmScriptArtifactId }
          };
        }
      };

      registerToolDefinitions(mcp, { policy, store, artifacts, runsDir, slurmSubmitter }, [hybridTool] as any);

      [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
      await mcp.connect(serverTransport);

      client = new Client({ name: "helixmcp-test-client", version: "0.0.0" });
      await client.connect(clientTransport);

      const submit1 = await client.request(
        {
          method: "tools/call",
          params: { name: "hybrid_slurm_tool", arguments: { project_id: projectId, bam_artifact_id: input.artifactId } }
        },
        CallToolResultSchema
      );
      expect(submit1.isError).not.toBe(true);
      expect(submitCalls).toBe(1);

      const submit2 = await client.request(
        {
          method: "tools/call",
          params: { name: "hybrid_slurm_tool", arguments: { project_id: projectId, bam_artifact_id: input.artifactId } }
        },
        CallToolResultSchema
      );
      expect(submit2.isError).not.toBe(true);
      expect(submitCalls).toBe(1);
      expect((submit2.content[0] as any).text).toContain("Replayed");
      expect(submit2.structuredContent).toEqual(submit1.structuredContent);
    } finally {
      if (clientTransport) await clientTransport.close();
      if (serverTransport) await serverTransport.close();
      if (pool) await pool.end();
      await rm(tmpDir, { recursive: true, force: true });
    }
  });

  it("fails closed when slurm plan outputs do not match declaredOutputs", async () => {
    const tmpDir = await mkdtemp(path.join(os.tmpdir(), "helixmcp-toolpacks-"));
    let pool: pg.Pool | null = null;
    let client: Client | null = null;
    let clientTransport: InMemoryTransport | null = null;
    let serverTransport: InMemoryTransport | null = null;
    let submitCalls = 0;
    let runCalled = false;

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

      const APPTAINER_IMAGE =
        "docker://quay.io/biocontainers/samtools@sha256:bf80e07e650becfd084db1abde0fe932b50f990a07fa56421ea647b552b5a406";

      const policy = new PolicyEngine({
        version: 1,
        runtime: { instance_id: "local" },
        tool_allowlist: ["mismatch_slurm_tool"],
        quotas: { max_threads: 4, max_runtime_seconds: 10, max_import_bytes: 1024 * 1024 },
        imports: { allow_source_kinds: ["inline_text"], local_path_prefix_allowlist: [], deny_symlinks: true },
        docker: { network_mode: "none", image_allowlist: [] },
        slurm: {
          partitions_allowlist: ["short"],
          accounts_allowlist: ["bio"],
          qos_allowlist: [],
          constraints_allowlist: [],
          max_time_limit_seconds: 3600,
          max_cpus: 4,
          max_mem_mb: 8192,
          max_gpus: 0,
          max_collect_output_bytes: 1024 * 1024,
          max_collect_log_bytes: 1024 * 1024,
          gpu_types_allowlist: [],
          apptainer: { image_allowlist: [APPTAINER_IMAGE] },
          network_mode_required: "none"
        }
      });

      const slurmSubmitter = {
        submit: async (_scriptPath: string) => {
          submitCalls += 1;
          return { slurmJobId: "11111", stdout: "11111\n", stderr: "" };
        }
      };

      const projectId = "proj_01HZZZZZZZZZZZZZZZZZZZZZZZ";
      const input = await artifacts.importArtifact({
        projectId: projectId as any,
        source: { kind: "inline_text", text: "FAKEBAM\n" },
        typeHint: "BAM",
        label: "input.bam",
        createdByRunId: null,
        maxBytes: null
      });

      const mcp = new McpServer({ name: "helixmcp-test", version: "0.0.0" });

      const mismatchTool = {
        toolName: "mismatch_slurm_tool",
        contractVersion: "v1",
        planKind: "slurm" as const,
        description: "Mismatch plan outputs vs declaredOutputs (test only).",
        inputSchema: z.object({ project_id: z.string(), bam_artifact_id: z.string() }),
        outputSchema: z.object({}),
        declaredOutputs: [{ role: "report", type: "TEXT", label: "report.txt", srcRelpath: "out/report.txt" }],
        canonicalize: async (args: any, ctx: any) => {
          const bam = await ctx.artifacts.getArtifact(args.bam_artifact_id);
          if (!bam) throw new Error("missing bam");
          return {
            projectId: args.project_id,
            canonicalParams: { project_id: args.project_id, stable: true },
            toolVersion: "slurm_script_v1",
            plan: {
              version: 1,
              resources: { time_limit_seconds: 60, cpus: 1, mem_mb: 512, gpus: null, gpu_type: null },
              placement: { partition: "short", account: "bio", qos: null, constraint: null },
              execution: {
                kind: "container",
                container: { engine: "apptainer", image: APPTAINER_IMAGE, network_mode: "none", readonly_rootfs: true },
                command: { argv: ["samtools", "flagstat", "/work/in/input.bam"], workdir: "/work", env: {} }
              },
              io: {
                inputs: [
                  {
                    role: "bam",
                    artifact_id: bam.artifactId,
                    checksum_sha256: bam.checksumSha256,
                    dest_relpath: "in/input.bam"
                  }
                ],
                // Mismatch: src_relpath differs from declaredOutputs.srcRelpath.
                outputs: [{ role: "report", src_relpath: "out/NOT_report.txt", type: "TEXT", label: "report.txt" }]
              }
            } satisfies SlurmExecutionPlan,
            selectedPlanKind: "slurm",
            inputsToLink: [{ artifactId: bam.artifactId, role: "bam" }]
          };
        },
        run: async () => {
          runCalled = true;
          throw new Error("unreachable");
        }
      };

      registerToolDefinitions(mcp, { policy, store, artifacts, runsDir, slurmSubmitter }, [mismatchTool] as any);

      [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
      await mcp.connect(serverTransport);

      client = new Client({ name: "helixmcp-test-client", version: "0.0.0" });
      await client.connect(clientTransport);

      const res = await client.request(
        {
          method: "tools/call",
          params: { name: "mismatch_slurm_tool", arguments: { project_id: projectId, bam_artifact_id: input.artifactId } }
        },
        CallToolResultSchema
      );

      expect(res.isError).toBe(true);
      expect(runCalled).toBe(false);
      expect(submitCalls).toBe(0);
    } finally {
      if (clientTransport) await clientTransport.close();
      if (serverTransport) await serverTransport.close();
      if (pool) await pool.end();
      await rm(tmpDir, { recursive: true, force: true });
    }
  });
});
