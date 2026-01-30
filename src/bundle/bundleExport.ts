import { createHash } from "crypto";
import { promises as fs } from "fs";
import path from "path";
import { stableJsonStringify, sha256Prefixed } from "../core/canonicalJson.js";
import type { ArtifactId, RunId } from "../core/ids.js";
import type { JsonObject } from "../core/json.js";
import { PostgresStore, type RunEventRecord } from "../store/postgresStore.js";
import type { ArtifactRecord } from "../core/artifact.js";
import type { RunRecord } from "../core/run.js";
import type { BundleFileEntry, BundleManifestV1, IncludeBlobsMode } from "./manifest.js";

export interface ExportBundleOptions {
  rootRunId: RunId;
  outDir: string;
  includeBlobs: IncludeBlobsMode;
  maxBlobBytes: bigint;
  verifyAfterWrite: boolean;
}

export interface ExportBundleDeps {
  store: PostgresStore;
  objectStoreDir?: string | null;
}

export interface ExportBundleResult {
  manifestPath: string;
  manifestSha256: `sha256:${string}`;
  bundleSha256Path: string;
  warnings: string[];
}

function requireRelativePath(p: string, label: string): string {
  const trimmed = p.trim();
  if (trimmed.length === 0) throw new Error(`${label} must be non-empty`);
  if (path.isAbsolute(trimmed)) throw new Error(`${label} must be a relative path`);
  const normalized = path.posix.normalize(trimmed);
  if (normalized.startsWith("../") || normalized === "..") throw new Error(`${label} must not contain '..' segments`);
  return normalized;
}

async function sha256File(filePath: string): Promise<{ sha256: `sha256:${string}`; sizeBytes: bigint }> {
  const hash = createHash("sha256");
  const fd = await fs.open(filePath, "r");
  try {
    const buf = Buffer.alloc(1024 * 1024);
    let total = 0n;
    for (;;) {
      const { bytesRead } = await fd.read(buf, 0, buf.length, null);
      if (bytesRead === 0) break;
      total += BigInt(bytesRead);
      hash.update(buf.subarray(0, bytesRead));
    }
    return { sha256: `sha256:${hash.digest("hex")}` as const, sizeBytes: total };
  } finally {
    await fd.close();
  }
}

function extractGraphRunIds(run: RunRecord): RunId[] {
  const graph = run.resultJson?.graph;
  if (!graph || typeof graph !== "object") return [];
  const nodes = (graph as { nodes?: unknown }).nodes;
  if (!Array.isArray(nodes)) return [];
  const out: RunId[] = [];
  for (const n of nodes) {
    if (!n || typeof n !== "object") continue;
    const runId = (n as { run_id?: unknown }).run_id;
    if (typeof runId === "string" && runId.startsWith("run_")) out.push(runId as RunId);
  }
  return Array.from(new Set(out)).sort();
}

function blobSourcePathForArtifact(a: ArtifactRecord, deps: ExportBundleDeps): string {
  if (!a.uri.startsWith("file://")) {
    throw new Error(`artifact ${a.artifactId} has unsupported uri scheme (expected file://)`);
  }
  const filePath = new URL(a.uri).pathname;
  const decoded = decodeURIComponent(filePath);
  const objectStoreDir = deps.objectStoreDir?.trim();
  if (objectStoreDir) {
    const rel = path.relative(path.resolve(objectStoreDir), path.resolve(decoded));
    if (rel.startsWith("..") || path.isAbsolute(rel)) {
      throw new Error(`artifact ${a.artifactId} uri is outside OBJECT_STORE_DIR`);
    }
  }
  return decoded;
}

function chooseBlobArtifactIds(opts: ExportBundleOptions, rootRunId: RunId, graph: ExportGraph): Set<ArtifactId> {
  if (opts.includeBlobs === "none") return new Set();
  if (opts.includeBlobs === "all") return new Set(graph.artifacts.map((a) => a.artifactId));

  const root = graph.runs.find((r) => r.run.runId === rootRunId);
  if (!root) return new Set();

  const out = new Set<ArtifactId>();
  for (const { artifactId } of root.inputs) out.add(artifactId);
  for (const { artifactId } of root.outputs) out.add(artifactId);
  if (root.run.logArtifactId) out.add(root.run.logArtifactId);
  return out;
}

interface ExportGraphRun {
  run: RunRecord;
  canonicalParams: JsonObject | null;
  events: RunEventRecord[];
  inputs: Array<{ artifactId: ArtifactId; role: string }>;
  outputs: Array<{ artifactId: ArtifactId; role: string }>;
}

interface ExportGraph {
  runs: ExportGraphRun[];
  artifacts: ArtifactRecord[];
}

async function loadExportGraph(store: PostgresStore, rootRunId: RunId): Promise<ExportGraph> {
  const root = await store.getRun(rootRunId);
  if (!root) throw new Error(`unknown run_id: ${rootRunId}`);

  const runIds = Array.from(new Set<RunId>([rootRunId, ...extractGraphRunIds(root)])).sort();
  const runs = runIds.length === 1 ? [root] : await store.listRuns(runIds);

  const runById = new Map(runs.map((r) => [r.runId, r]));
  for (const id of runIds) {
    if (!runById.has(id)) throw new Error(`run referenced in graph not found: ${id}`);
  }

  const graphRuns: ExportGraphRun[] = [];
  const artifactIdSet = new Set<ArtifactId>();

  for (const run of runs.sort((a, b) => a.runId.localeCompare(b.runId))) {
    const canonicalParams = await store.getParamSet(run.paramsHash);
    const events = await store.listRunEvents(run.runId);
    const inputs = (await store.listRunInputs(run.runId)).slice().sort((a, b) => {
      const ar = a.role.localeCompare(b.role);
      if (ar !== 0) return ar;
      return a.artifactId.localeCompare(b.artifactId);
    });
    const outputs = (await store.listRunOutputs(run.runId)).slice().sort((a, b) => {
      const ar = a.role.localeCompare(b.role);
      if (ar !== 0) return ar;
      return a.artifactId.localeCompare(b.artifactId);
    });

    for (const io of inputs) artifactIdSet.add(io.artifactId);
    for (const io of outputs) artifactIdSet.add(io.artifactId);
    if (run.logArtifactId) artifactIdSet.add(run.logArtifactId);

    graphRuns.push({ run, canonicalParams, events, inputs, outputs });
  }

  const artifacts = await store.listArtifactsById(Array.from(artifactIdSet).sort());
  const artifactById = new Map(artifacts.map((a) => [a.artifactId, a]));
  for (const id of artifactIdSet) {
    if (!artifactById.has(id)) throw new Error(`run references missing artifact_id: ${id}`);
  }

  return { runs: graphRuns, artifacts };
}

async function writeFileUtf8(outDir: string, relPath: string, content: string): Promise<void> {
  const safeRel = requireRelativePath(relPath, "bundle file path");
  const full = path.join(outDir, safeRel);
  await fs.mkdir(path.dirname(full), { recursive: true });
  await fs.writeFile(full, content, "utf8");
}

async function writeFileBytes(outDir: string, relPath: string, bytes: Buffer): Promise<void> {
  const safeRel = requireRelativePath(relPath, "bundle file path");
  const full = path.join(outDir, safeRel);
  await fs.mkdir(path.dirname(full), { recursive: true });
  await fs.writeFile(full, bytes);
}

export async function exportBundleToDir(opts: ExportBundleOptions, deps: ExportBundleDeps): Promise<ExportBundleResult> {
  const warnings: string[] = [];
  await fs.mkdir(opts.outDir, { recursive: true });

  const graph = await loadExportGraph(deps.store, opts.rootRunId);

  const runsJson = graph.runs.map((r) => r.run);
  const paramSetsJson = graph.runs
    .map((r) => ({ params_hash: r.run.paramsHash, canonical_json: r.canonicalParams }))
    .sort((a, b) => a.params_hash.localeCompare(b.params_hash));
  const policySnapshotsJson = graph.runs
    .map((r) => ({ run_id: r.run.runId, policy_snapshot: r.run.policySnapshot }))
    .sort((a, b) => a.run_id.localeCompare(b.run_id));
  const environmentsJson = graph.runs
    .map((r) => ({ run_id: r.run.runId, environment: r.run.environment }))
    .sort((a, b) => a.run_id.localeCompare(b.run_id));

  const artifactsJson = graph.artifacts.map((a) => a).sort((a, b) => a.artifactId.localeCompare(b.artifactId));

  const artifactGraphJson: JsonObject = {
    root_run_id: opts.rootRunId,
    runs: graph.runs.map((r) => ({
      run_id: r.run.runId,
      inputs: r.inputs.map((i) => ({ role: i.role, artifact_id: i.artifactId })),
      outputs: r.outputs.map((o) => ({ role: o.role, artifact_id: o.artifactId }))
    }))
  };

  const allEvents: Array<JsonObject & { run_id: string; event_id: string; created_at: string; kind: string; message: string | null; data: JsonObject | null }> =
    [];
  for (const r of graph.runs) {
    for (const e of r.events) {
      allEvents.push({
        run_id: r.run.runId,
        event_id: e.eventId,
        created_at: e.createdAt,
        kind: e.kind,
        message: e.message,
        data: e.data
      });
    }
  }
  allEvents.sort((a, b) => {
    const ar = a.run_id.localeCompare(b.run_id);
    if (ar !== 0) return ar;
    return BigInt(a.event_id) < BigInt(b.event_id) ? -1 : BigInt(a.event_id) > BigInt(b.event_id) ? 1 : 0;
  });

  await writeFileUtf8(opts.outDir, "runs.json", stableJsonStringify(runsJson));
  await writeFileUtf8(opts.outDir, "param_sets.json", stableJsonStringify(paramSetsJson));
  await writeFileUtf8(opts.outDir, "policy_snapshots.json", stableJsonStringify(policySnapshotsJson));
  await writeFileUtf8(opts.outDir, "environments.json", stableJsonStringify(environmentsJson));
  await writeFileUtf8(opts.outDir, "artifacts.json", stableJsonStringify(artifactsJson));
  await writeFileUtf8(opts.outDir, "artifact_graph.json", stableJsonStringify(artifactGraphJson));

  const eventsNdjson = allEvents.map((e) => stableJsonStringify(e)).join("\n") + "\n";
  await writeFileUtf8(opts.outDir, "events.ndjson", eventsNdjson);

  const blobArtifactIds = chooseBlobArtifactIds(opts, opts.rootRunId, graph);
  for (const a of graph.artifacts) {
    if (!blobArtifactIds.has(a.artifactId)) continue;
    const hex = a.checksumSha256.replace(/^sha256:/, "");
    const rel = requireRelativePath(path.posix.join("blobs", hex), "blob path");
    if (a.sizeBytes > opts.maxBlobBytes) {
      warnings.push(`skipped blob for ${a.artifactId} (size=${a.sizeBytes.toString()} exceeds max_blob_bytes=${opts.maxBlobBytes.toString()})`);
      continue;
    }
    const source = blobSourcePathForArtifact(a, deps);
    const dest = path.join(opts.outDir, rel);
    await fs.mkdir(path.dirname(dest), { recursive: true });
    await fs.copyFile(source, dest);
  }

  const contentRelPaths = ["runs.json", "param_sets.json", "policy_snapshots.json", "environments.json", "artifacts.json", "artifact_graph.json", "events.ndjson"];
  const blobPaths: string[] = [];
  if (opts.includeBlobs !== "none") {
    for (const a of graph.artifacts) {
      if (!blobArtifactIds.has(a.artifactId)) continue;
      const hex = a.checksumSha256.replace(/^sha256:/, "");
      const rel = path.posix.join("blobs", hex);
      const full = path.join(opts.outDir, rel);
      try {
        await fs.stat(full);
        blobPaths.push(rel);
      } catch {
        // skipped
      }
    }
  }

  const fileEntries: BundleFileEntry[] = [];
  for (const rel of [...contentRelPaths, ...blobPaths].sort()) {
    const full = path.join(opts.outDir, rel);
    const { sha256, sizeBytes } = await sha256File(full);
    fileEntries.push({ path: rel, sha256, size_bytes: sizeBytes.toString() });
  }

  const manifest: BundleManifestV1 = {
    bundle_version: 1,
    root_run_id: opts.rootRunId,
    include_blobs: opts.includeBlobs,
    max_blob_bytes: opts.maxBlobBytes.toString(),
    ...(warnings.length ? { warnings } : {}),
    files: fileEntries
  };

  const manifestBytes = Buffer.from(stableJsonStringify(manifest), "utf8");
  await writeFileBytes(opts.outDir, "manifest.json", manifestBytes);

  const manifestSha256 = sha256Prefixed(manifestBytes);
  await writeFileUtf8(opts.outDir, "manifest.sha256", `${manifestSha256}  manifest.json\n`);
  await writeFileUtf8(opts.outDir, "bundle.sha256", `${manifestSha256}  manifest.json\n`);

  if (opts.verifyAfterWrite) {
    const { verifyBundleDir } = await import("./bundleVerify.js");
    await verifyBundleDir(opts.outDir);
  }

  return {
    manifestPath: path.join(opts.outDir, "manifest.json"),
    manifestSha256,
    bundleSha256Path: path.join(opts.outDir, "bundle.sha256"),
    warnings
  };
}
