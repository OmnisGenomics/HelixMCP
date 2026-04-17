import { describe, expect, it } from "vitest";
import { mkdtemp, rm } from "fs/promises";
import os from "os";
import path from "path";

import { newDb } from "pg-mem";
import * as pg from "pg";

import { ArtifactService } from "../src/artifacts/artifactService.js";
import { ImportTooLargeError, LocalObjectStore } from "../src/artifacts/localObjectStore.js";
import { newProjectId } from "../src/core/ids.js";
import { applySqlFile } from "../src/db/bootstrap.js";
import { createDb } from "../src/db/connection.js";
import { PostgresStore } from "../src/store/postgresStore.js";

describe("ArtifactService", () => {
  it("enforces maxBytes for inline_text imports", async () => {
    const tmpDir = await mkdtemp(path.join(os.tmpdir(), "helixmcp-artifacts-"));
    let pool: pg.Pool | null = null;

    try {
      const mem = newDb({ autoCreateForeignKeyIndices: true });
      const adapter = mem.adapters.createPg();
      pool = new adapter.Pool() as unknown as pg.Pool;
      await applySqlFile(pool, path.resolve("db/schema.sql"));

      const db = createDb(pool);
      const store = new PostgresStore(db);
      const objects = new LocalObjectStore(path.join(tmpDir, "objects"));
      const artifacts = new ArtifactService(store, objects);
      const projectId = newProjectId();

      await expect(
        artifacts.importArtifact({
          projectId,
          source: { kind: "inline_text", text: "four" },
          typeHint: "TEXT",
          label: "too-large.txt",
          createdByRunId: null,
          maxBytes: 3n
        })
      ).rejects.toBeInstanceOf(ImportTooLargeError);

      const countAfterDeniedImport = Number((await pool.query("SELECT COUNT(*) AS c FROM artifacts")).rows[0]?.c);
      expect(countAfterDeniedImport).toBe(0);

      const ok = await artifacts.importArtifact({
        projectId,
        source: { kind: "inline_text", text: "ok" },
        typeHint: "TEXT",
        label: "ok.txt",
        createdByRunId: null,
        maxBytes: 2n
      });

      expect(ok.sizeBytes).toBe(2n);
      await db.destroy();
    } finally {
      if (pool) await pool.end();
      await rm(tmpDir, { recursive: true, force: true });
    }
  });
});
