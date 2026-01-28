import * as pg from "pg";
import { newDb } from "pg-mem";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { createDb, createPgPool } from "./db/connection.js";
import { applySqlFile } from "./db/bootstrap.js";
import { LocalObjectStore } from "./artifacts/localObjectStore.js";
import { ArtifactService } from "./artifacts/artifactService.js";
import { createGatewayServer } from "./mcp/gatewayServer.js";
import { PolicyEngine } from "./policy/policy.js";
import { PostgresStore } from "./store/postgresStore.js";
import { DefaultExecutionService } from "./execution/executionService.js";

async function createPool(): Promise<pg.Pool> {
  const url = process.env.DATABASE_URL;
  if (url) return createPgPool(url);

  const mem = newDb({ autoCreateForeignKeyIndices: true });
  const adapter = mem.adapters.createPg();
  return new adapter.Pool() as unknown as pg.Pool;
}

async function main(): Promise<void> {
  const policyPath = process.env.GATEWAY_POLICY_PATH ?? "policies/default.policy.yaml";
  const objectStoreDir = process.env.OBJECT_STORE_DIR ?? "var/objects";
  const runsDir = process.env.RUNS_DIR ?? "var/runs";
  const autoSchema = (process.env.AUTO_SCHEMA ?? "true").toLowerCase() !== "false";

  const policy = await PolicyEngine.loadFromFile(policyPath);
  if (process.env.DATABASE_URL) {
    const instanceId = policy.runtimeInstanceId();
    if (!instanceId) {
      throw new Error(`policy runtime.instance_id is required when DATABASE_URL is set (to avoid run_id collisions)`);
    }
  }
  const pool = await createPool();
  if (!process.env.DATABASE_URL || autoSchema) {
    await applySqlFile(pool, "db/schema.sql");
  }

  const db = createDb(pool);
  const store = new PostgresStore(db);
  const objects = new LocalObjectStore(objectStoreDir);
  const artifacts = new ArtifactService(store, objects);
  const execution = new DefaultExecutionService({ policy, objects, workspaceRootDir: runsDir });

  const server = createGatewayServer({ policy, store, artifacts, execution, runsDir });
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("helixmcp gateway ready");
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
