import { describe, expect, it } from "vitest";
import path from "path";

import { newDb } from "pg-mem";
import * as pg from "pg";

import { applySqlFile } from "../src/db/bootstrap.js";
import { createDb } from "../src/db/connection.js";
import { newProjectId, type RunId } from "../src/core/ids.js";
import {
  canTransitionRunStatus,
  RUN_STATUSES,
  RUN_TERMINAL_STATUSES,
  type RunStatus
} from "../src/core/run.js";
import { deriveRunId } from "../src/runs/runIdentity.js";
import { PostgresStore } from "../src/store/postgresStore.js";

const POLICY_HASH = `sha256:${"a".repeat(64)}` as const;

async function withStore<T>(fn: (store: PostgresStore) => Promise<T>): Promise<T> {
  const mem = newDb({ autoCreateForeignKeyIndices: true });
  const adapter = mem.adapters.createPg();
  const pool = new adapter.Pool() as unknown as pg.Pool;

  try {
    await applySqlFile(pool, path.resolve("db/schema.sql"));
    return await fn(new PostgresStore(createDb(pool)));
  } finally {
    await pool.end();
  }
}

async function createRun(store: PostgresStore, status: RunStatus, label: string): Promise<RunId> {
  const projectId = newProjectId();
  const canonicalParams = { project_id: projectId, label, status };
  const { runId, paramsHash } = deriveRunId({
    toolName: "lifecycle_test",
    contractVersion: "v1",
    policyHash: POLICY_HASH,
    canonicalParams
  });

  await store.createRun({
    runId,
    projectId,
    toolName: "lifecycle_test",
    contractVersion: "v1",
    toolVersion: "v1",
    paramsHash,
    canonicalParams,
    policyHash: POLICY_HASH,
    status,
    requestedBy: null,
    policySnapshot: null,
    environment: null
  });

  return runId;
}

describe("run lifecycle", () => {
  it("keeps the documented status transition matrix explicit", () => {
    const expected: Record<RunStatus, readonly RunStatus[]> = {
      queued: ["queued", "running", "succeeded", "failed", "blocked"],
      running: ["running", "succeeded", "failed", "blocked"],
      succeeded: ["succeeded"],
      failed: ["failed"],
      blocked: ["blocked"]
    };

    for (const from of RUN_STATUSES) {
      for (const to of RUN_STATUSES) {
        expect(canTransitionRunStatus(from, to), `${from} -> ${to}`).toBe(expected[from].includes(to));
      }
    }

    expect(RUN_TERMINAL_STATUSES).toEqual(["succeeded", "failed", "blocked"]);
  });

  it("fails closed when PostgresStore receives invalid status transitions", async () => {
    await withStore(async (store) => {
      const runningRunId = await createRun(store, "running", "running-to-queued");

      await expect(store.updateRun(runningRunId, { status: "queued", error: "invalid" })).rejects.toThrow(
        /invalid run status transition: running -> queued/
      );

      const runningRun = await store.getRun(runningRunId);
      expect(runningRun?.status).toBe("running");
      expect(runningRun?.error).toBeNull();

      const succeededRunId = await createRun(store, "succeeded", "terminal-to-failed");

      await expect(store.updateRun(succeededRunId, { status: "failed", exitCode: 1 })).rejects.toThrow(
        /invalid run status transition: succeeded -> failed/
      );

      const succeededRun = await store.getRun(succeededRunId);
      expect(succeededRun?.status).toBe("succeeded");
      expect(succeededRun?.exitCode).toBeNull();
    });
  });
});
