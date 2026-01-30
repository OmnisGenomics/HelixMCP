import { describe, it, expect } from "vitest";
import { mkdtemp, rm, readFile } from "fs/promises";
import os from "os";
import path from "path";
import { execFile } from "child_process";
import { promisify } from "util";

import { newDb } from "pg-mem";
import * as pg from "pg";

import { applySqlFile } from "../src/db/bootstrap.js";
import { createDb } from "../src/db/connection.js";
import { PostgresStore } from "../src/store/postgresStore.js";
import { LocalObjectStore } from "../src/artifacts/localObjectStore.js";
import { stableJsonStringify, sha256Prefixed } from "../src/core/canonicalJson.js";
import { exportBundleToDir } from "../src/bundle/bundleExport.js";
import { verifyBundleDir } from "../src/bundle/bundleVerify.js";
import { bundleDirToDeterministicTar } from "../src/bundle/bundleTar.js";

const execFileAsync = promisify(execFile);

describe("bundle_export", () => {
  it("exports a deterministic bundle dir and verifies offline", async () => {
    const tmpDir = await mkdtemp(path.join(os.tmpdir(), "helixmcp-bundle-"));
    let pool: pg.Pool | null = null;

    try {
      const mem = newDb({ autoCreateForeignKeyIndices: true });
      const adapter = mem.adapters.createPg();
      pool = new adapter.Pool() as unknown as pg.Pool;
      await applySqlFile(pool, path.resolve("db/schema.sql"));

      const db = createDb(pool);
      const store = new PostgresStore(db);

      const objectsDir = path.join(tmpDir, "objects");
      const objects = new LocalObjectStore(objectsDir);

      const projectId = "proj_test" as any;
      const runId = "run_01TESTBUNDLEEXPORT00000000000000" as any;
      const inputArtifactId = "art_input" as any;
      const reportArtifactId = "art_report" as any;
      const logArtifactId = "art_log" as any;

      const inputPut = await objects.putInlineText(inputArtifactId, "synthetic fastq bytes\n");
      const reportPut = await objects.putInlineText(reportArtifactId, "# report\nok\n");
      const logPut = await objects.putInlineText(logArtifactId, "log\n");

      await store.createArtifact({
        artifactId: inputArtifactId,
        projectId,
        type: "FASTQ_GZ",
        uri: inputPut.uri,
        mimeType: "application/gzip",
        sizeBytes: inputPut.sizeBytes,
        checksumSha256: inputPut.checksumSha256,
        label: "reads.fastq.gz",
        createdByRunId: null,
        metadata: {}
      });

      await store.createArtifact({
        artifactId: reportArtifactId,
        projectId,
        type: "MD",
        uri: reportPut.uri,
        mimeType: "text/markdown",
        sizeBytes: reportPut.sizeBytes,
        checksumSha256: reportPut.checksumSha256,
        label: "qc_bundle_report.md",
        createdByRunId: runId,
        metadata: {}
      });

      await store.createArtifact({
        artifactId: logArtifactId,
        projectId,
        type: "LOG",
        uri: logPut.uri,
        mimeType: "text/plain",
        sizeBytes: logPut.sizeBytes,
        checksumSha256: logPut.checksumSha256,
        label: "log.txt",
        createdByRunId: runId,
        metadata: {}
      });

      const canonicalParams = { project_id: projectId, synthetic: true };
      const paramsHash = sha256Prefixed(stableJsonStringify(canonicalParams));
      const policyHash = "sha256:" + "a".repeat(64);

      await store.createRun({
        runId,
        projectId,
        toolName: "qc_bundle_fastq",
        contractVersion: "v1",
        toolVersion: "test",
        paramsHash,
        canonicalParams: canonicalParams as any,
        policyHash: policyHash as any,
        status: "succeeded",
        requestedBy: null,
        policySnapshot: { version: 1, runtime: { instance_id: "local" } } as any,
        environment: { mode: "pg-mem" } as any
      });

      await store.addRunInput(runId, inputArtifactId, "reads");
      await store.addRunOutput(runId, reportArtifactId, "bundle_report");
      await store.updateRun(runId, { logArtifactId, resultJson: { ok: true } as any, exitCode: 0, finishedAt: new Date(0).toISOString() });
      await store.addRunEvent(runId, "run.succeeded", "ok", { synthetic: true } as any);

      const outDir1 = path.join(tmpDir, "out1");
      const outDir2 = path.join(tmpDir, "out2");

      const res1 = await exportBundleToDir(
        { rootRunId: runId, outDir: outDir1, includeBlobs: "all", maxBlobBytes: 1000000n, verifyAfterWrite: true },
        { store, objectStoreDir: objectsDir }
      );
      const res2 = await exportBundleToDir(
        { rootRunId: runId, outDir: outDir2, includeBlobs: "all", maxBlobBytes: 1000000n, verifyAfterWrite: true },
        { store, objectStoreDir: objectsDir }
      );

      expect(res1.manifestSha256).toEqual(res2.manifestSha256);
      expect(await readFile(path.join(outDir1, "manifest.json"), "utf8")).toEqual(await readFile(path.join(outDir2, "manifest.json"), "utf8"));

      await verifyBundleDir(outDir1);
      await verifyBundleDir(outDir2);

      await db.destroy();
    } finally {
      const p = pool;
      if (p) await new Promise<void>((resolve) => p.end(() => resolve()));
      await rm(tmpDir, { recursive: true, force: true });
    }
  });

  it("produces deterministic tar bytes for the same bundle dir", async () => {
    const tmpDir = await mkdtemp(path.join(os.tmpdir(), "helixmcp-bundle-tar-"));
    try {
      await execFileAsync("tar", ["--version"]);
    } catch {
      await rm(tmpDir, { recursive: true, force: true });
      return;
    }

    try {
      const bundleDir = path.join(tmpDir, "bundle");
      await fsMkdirp(bundleDir);
      await writeUtf8(bundleDir, "runs.json", "[]");
      await writeUtf8(bundleDir, "param_sets.json", "[]");
      await writeUtf8(bundleDir, "policy_snapshots.json", "[]");
      await writeUtf8(bundleDir, "environments.json", "[]");
      await writeUtf8(bundleDir, "artifacts.json", "[]");
      await writeUtf8(bundleDir, "artifact_graph.json", "{}");
      await writeUtf8(bundleDir, "events.ndjson", "\n");
      await writeUtf8(bundleDir, "manifest.json", "{\"bundle_version\":1,\"root_run_id\":\"run_x\",\"include_blobs\":\"none\",\"max_blob_bytes\":\"0\",\"files\":[]}");
      await writeUtf8(bundleDir, "manifest.sha256", "sha256:00  manifest.json\n");
      await writeUtf8(bundleDir, "bundle.sha256", "sha256:00  manifest.json\n");

      const tar1 = path.join(tmpDir, "b1.tar");
      const tar2 = path.join(tmpDir, "b2.tar");
      const { tarSha256: s1 } = await bundleDirToDeterministicTar({ bundleDir, outTarPath: tar1 });
      const { tarSha256: s2 } = await bundleDirToDeterministicTar({ bundleDir, outTarPath: tar2 });
      expect(s1).toEqual(s2);
    } finally {
      await rm(tmpDir, { recursive: true, force: true });
    }
  });
});

async function fsMkdirp(p: string): Promise<void> {
  const { mkdir } = await import("fs/promises");
  await mkdir(p, { recursive: true });
}

async function writeUtf8(root: string, rel: string, text: string): Promise<void> {
  const { writeFile, mkdir } = await import("fs/promises");
  const full = path.join(root, rel);
  await mkdir(path.dirname(full), { recursive: true });
  await writeFile(full, text, "utf8");
}
