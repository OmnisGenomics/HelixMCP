import { createHash } from "crypto";
import { promises as fs } from "fs";
import path from "path";
import { stableJsonStringify } from "../core/canonicalJson.js";
import type { BundleManifestV1 } from "./manifest.js";

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

function parseManifest(json: string): BundleManifestV1 {
  const parsed = JSON.parse(json) as unknown;
  const canonical = stableJsonStringify(parsed);
  return JSON.parse(canonical) as BundleManifestV1;
}

export async function verifyBundleDir(bundleDir: string): Promise<void> {
  const manifestPath = path.join(bundleDir, "manifest.json");
  const manifestText = await fs.readFile(manifestPath, "utf8");
  const manifest = parseManifest(manifestText);

  if (manifest.bundle_version !== 1) throw new Error(`unsupported bundle_version: ${String(manifest.bundle_version)}`);

  const entries = manifest.files ?? [];
  if (!Array.isArray(entries) || entries.length === 0) throw new Error("manifest.files is missing or empty");

  for (const entry of entries) {
    const rel = entry.path;
    if (typeof rel !== "string" || rel.length === 0) throw new Error("invalid manifest.files[*].path");
    const full = path.join(bundleDir, rel);
    const { sha256, sizeBytes } = await sha256File(full);
    if (sha256 !== entry.sha256) throw new Error(`sha256 mismatch for ${rel} (expected ${entry.sha256}, got ${sha256})`);
    if (sizeBytes.toString() !== entry.size_bytes) {
      throw new Error(`size mismatch for ${rel} (expected ${entry.size_bytes}, got ${sizeBytes.toString()})`);
    }
  }

  const manifestDigest = await sha256File(manifestPath);
  const shaLine = `${manifestDigest.sha256}  manifest.json\n`;
  const manifestShaPath = path.join(bundleDir, "manifest.sha256");
  const onDisk = await fs.readFile(manifestShaPath, "utf8");
  if (onDisk !== shaLine) throw new Error(`manifest.sha256 does not match manifest.json digest`);
}

