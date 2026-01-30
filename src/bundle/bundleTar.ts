import { createHash } from "crypto";
import { promises as fs } from "fs";
import { execFile } from "child_process";
import path from "path";
import { promisify } from "util";

const execFileAsync = promisify(execFile);

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

export async function bundleDirToDeterministicTar(input: { bundleDir: string; outTarPath: string }): Promise<{
  tarSha256: `sha256:${string}`;
}> {
  await execFileAsync("tar", ["--version"]);
  await fs.mkdir(path.dirname(input.outTarPath), { recursive: true });

  // Deterministic tar: sorted entries, fixed mtime, numeric owner/group.
  await execFileAsync("tar", [
    "--sort=name",
    "--mtime=UTC 1970-01-01",
    "--owner=0",
    "--group=0",
    "--numeric-owner",
    "-cf",
    input.outTarPath,
    "-C",
    input.bundleDir,
    "."
  ]);

  return { tarSha256: await sha256File(input.outTarPath) };
}

