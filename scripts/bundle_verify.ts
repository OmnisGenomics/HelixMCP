import { createHash } from "crypto";
import { promises as fs } from "fs";
import os from "os";
import path from "path";
import { execFile } from "child_process";
import { promisify } from "util";
import { verifyBundleDir } from "../src/bundle/bundleVerify.js";

const execFileAsync = promisify(execFile);

function usage(): string {
  return [
    "usage:",
    "  tsx scripts/bundle_verify.ts --bundle <dir|.tar>",
    "",
    "notes:",
    "  - For .tar bundles, this verifies extracted contents and (if present) <bundle>.sha256",
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
    if (key === "help") {
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

async function sha256File(filePath: string): Promise<`sha256:${string}`> {
  const hash = createHash("sha256");
  const fd = await fs.open(filePath, "r");
  try {
    const buf = Buffer.alloc(1024 * 1024);
    for (;;) {
      const { bytesRead } = await fd.read(buf, 0, buf.length, null);
      if (bytesRead === 0) break;
      hash.update(buf.subarray(0, bytesRead));
    }
    return `sha256:${hash.digest("hex")}` as const;
  } finally {
    await fd.close();
  }
}

async function verifyTarDigest(tarPath: string, extractedDir: string): Promise<void> {
  const digestPath = `${tarPath}.sha256`;
  try {
    const text = await fs.readFile(digestPath, "utf8");
    const lines = text.split(/\r?\n/).filter(Boolean);
    const tarSha = await sha256File(tarPath);
    const manifestSha = (await fs.readFile(path.join(extractedDir, "manifest.sha256"), "utf8")).trimEnd();
    const expected = new Set([`${tarSha}  ${path.basename(tarPath)}`, manifestSha]);
    for (const line of lines) {
      if (!expected.has(line)) throw new Error(`${digestPath} contains unexpected line: ${line}`);
    }
    for (const e of expected) {
      if (!lines.includes(e)) throw new Error(`${digestPath} missing expected line: ${e}`);
    }
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") return;
    throw err;
  }
}

async function verifyTarBundle(tarPath: string): Promise<void> {
  await execFileAsync("tar", ["-tf", tarPath]);
  const tmpRoot = await fs.mkdtemp(path.join(os.tmpdir(), "helixmcp_bundle_verify_"));
  try {
    await execFileAsync("tar", ["-xf", tarPath, "-C", tmpRoot]);
    await verifyBundleDir(tmpRoot);
    await verifyTarDigest(tarPath, tmpRoot);
  } finally {
    await fs.rm(tmpRoot, { recursive: true, force: true });
  }
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    process.stdout.write(usage());
    return;
  }
  const bundle = args.bundle;
  if (typeof bundle !== "string") throw new Error(`--bundle is required\n\n${usage()}`);
  const full = path.resolve(bundle);
  if (full.endsWith(".tar")) {
    await verifyTarBundle(full);
  } else {
    await verifyBundleDir(full);
  }
  process.stdout.write("ok\n");
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : err);
  process.exitCode = 1;
});

