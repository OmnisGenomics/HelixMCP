import * as pg from "pg";
import { promises as fs } from "fs";
import os from "os";
import path from "path";
import { createDb, createPgPool } from "../src/db/connection.js";
import { PostgresStore } from "../src/store/postgresStore.js";
import { exportBundleToDir } from "../src/bundle/bundleExport.js";
import { bundleDirToDeterministicTar } from "../src/bundle/bundleTar.js";
import type { IncludeBlobsMode } from "../src/bundle/manifest.js";
import type { RunId } from "../src/core/ids.js";

function usage(): string {
  return [
    "usage:",
    "  tsx scripts/bundle_export.ts --run-id <run_id> --out <dir|.tar> [--include-blobs none|selected|all] [--max-bytes <n>] [--verify-after-write]",
    "",
    "env:",
    "  DATABASE_URL (required)",
    "  OBJECT_STORE_DIR (optional, used to assert file:// URIs are contained)",
    ""
  ].join("\n");
}

function parseArgs(argv: string[]): Record<string, string | boolean> {
  const out: Record<string, string | boolean> = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a) continue;
    if (!a.startsWith("--")) throw new Error(`unexpected arg: ${a}`);
    const key = a.slice(2);
    if (key === "verify-after-write" || key === "help") {
      out[key] = true;
      continue;
    }
    const next = argv[i + 1];
    if (!next || next.startsWith("--")) throw new Error(`missing value for --${key}`);
    out[key] = next;
    i++;
  }
  return out;
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    process.stdout.write(usage());
    return;
  }

  const runId = args["run-id"];
  const outPath = args.out;
  if (typeof runId !== "string" || typeof outPath !== "string") {
    throw new Error(`--run-id and --out are required\n\n${usage()}`);
  }
  if (!runId.startsWith("run_")) throw new Error(`invalid --run-id: ${runId}`);

  const includeBlobs = (typeof args["include-blobs"] === "string" ? args["include-blobs"] : "none") as IncludeBlobsMode;
  if (!["none", "selected", "all"].includes(includeBlobs)) throw new Error(`invalid --include-blobs: ${includeBlobs}`);

  const maxBytesRaw = typeof args["max-bytes"] === "string" ? args["max-bytes"] : "100000000";
  if (!/^[0-9]+$/.test(maxBytesRaw)) throw new Error(`invalid --max-bytes: ${maxBytesRaw}`);
  const maxBlobBytes = BigInt(maxBytesRaw);

  const verifyAfterWrite = Boolean(args["verify-after-write"]);

  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) throw new Error(`DATABASE_URL is required`);

  const pool = createPgPool(databaseUrl);
  const db = createDb(pool);
  const store = new PostgresStore(db);

  const objectStoreDir = process.env.OBJECT_STORE_DIR ?? null;
  try {
    if (outPath.endsWith(".tar")) {
      const tmpRoot = await fs.mkdtemp(path.join(os.tmpdir(), "helixmcp_bundle_export_"));
      try {
        const res = await exportBundleToDir(
          { rootRunId: runId as RunId, outDir: tmpRoot, includeBlobs, maxBlobBytes, verifyAfterWrite },
          { store, objectStoreDir }
        );

        const outTar = path.resolve(outPath);
        const outTarDir = path.dirname(outTar);
        await fs.mkdir(outTarDir, { recursive: true });

        const { tarSha256 } = await bundleDirToDeterministicTar({ bundleDir: tmpRoot, outTarPath: outTar });
        const digestPath = `${outTar}.sha256`;
        const digestLines = [`${res.manifestSha256}  manifest.json`, `${tarSha256}  ${path.basename(outTar)}`].join("\n") + "\n";
        await fs.writeFile(digestPath, digestLines, "utf8");

        process.stdout.write(`${tarSha256}  ${outTar}\n`);
        if (res.warnings.length) {
          for (const w of res.warnings) process.stderr.write(`warning: ${w}\n`);
        }
      } finally {
        await fs.rm(tmpRoot, { recursive: true, force: true });
      }
    } else {
      const outDir = path.resolve(outPath);
      const res = await exportBundleToDir(
        { rootRunId: runId as RunId, outDir, includeBlobs, maxBlobBytes, verifyAfterWrite },
        { store, objectStoreDir }
      );
      process.stdout.write(`${res.manifestSha256}  ${res.manifestPath}\n`);
      if (res.warnings.length) {
        for (const w of res.warnings) process.stderr.write(`warning: ${w}\n`);
      }
    }
  } finally {
    await db.destroy();
    await new Promise<void>((resolve) => pool.end(() => resolve()));
  }
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : err);
  process.exitCode = 1;
});
