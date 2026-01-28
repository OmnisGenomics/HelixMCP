import { createHash } from "crypto";
import { createReadStream, createWriteStream, promises as fs } from "fs";
import path from "path";
import { Transform } from "stream";
import { pipeline } from "stream/promises";
import type { ArtifactId } from "../core/ids.js";

export interface PutResult {
  uri: string;
  sizeBytes: bigint;
  checksumSha256: `sha256:${string}`;
}

export class ImportTooLargeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ImportTooLargeError";
  }
}

export class LocalObjectStore {
  constructor(private readonly rootDir: string) {}

  async init(): Promise<void> {
    await fs.mkdir(this.rootDir, { recursive: true });
  }

  private objectPath(artifactId: ArtifactId): string {
    return path.join(this.rootDir, artifactId);
  }

  async putInlineText(artifactId: ArtifactId, text: string): Promise<PutResult> {
    await this.init();
    const bytes = Buffer.from(text, "utf8");
    const checksumSha256 = `sha256:${createHash("sha256").update(bytes).digest("hex")}` as const;
    const objectPath = this.objectPath(artifactId);
    await fs.writeFile(objectPath, bytes);
    return {
      uri: `file://${objectPath}`,
      sizeBytes: BigInt(bytes.byteLength),
      checksumSha256
    };
  }

  async putFromLocalPath(artifactId: ArtifactId, sourcePath: string, maxBytes: bigint | null): Promise<PutResult> {
    await this.init();
    const destinationPath = this.objectPath(artifactId);
    const hash = createHash("sha256");
    let total = 0n;

    const limiter = new Transform({
      transform(chunk: Buffer, _enc, cb) {
        total += BigInt(chunk.byteLength);
        if (maxBytes !== null && total > maxBytes) {
          cb(new ImportTooLargeError(`import exceeds max_bytes=${maxBytes.toString()}`));
          return;
        }
        hash.update(chunk);
        cb(null, chunk);
      }
    });

    await pipeline(createReadStream(sourcePath), limiter, createWriteStream(destinationPath));

    const checksumSha256 = `sha256:${hash.digest("hex")}` as const;
    return {
      uri: `file://${destinationPath}`,
      sizeBytes: total,
      checksumSha256
    };
  }

  async readTextPreview(artifactId: ArtifactId, opts: { maxBytes: number; maxLines: number }): Promise<{ preview: string; truncated: boolean }> {
    const objectPath = this.objectPath(artifactId);
    const fd = await fs.open(objectPath, "r");
    try {
      const buffer = Buffer.alloc(opts.maxBytes);
      const { bytesRead } = await fd.read(buffer, 0, opts.maxBytes, 0);
      const slice = buffer.subarray(0, bytesRead);
      const text = slice.toString("utf8");

      const lines = text.split(/\r?\n/);
      const limited = lines.slice(0, opts.maxLines).join("\n");

      const stats = await fd.stat();
      const truncatedByBytes = BigInt(stats.size) > BigInt(bytesRead);
      const truncatedByLines = lines.length > opts.maxLines;

      return { preview: limited, truncated: truncatedByBytes || truncatedByLines };
    } finally {
      await fd.close();
    }
  }

  async materializeToPath(artifactId: ArtifactId, destPath: string): Promise<void> {
    const sourcePath = this.objectPath(artifactId);
    await fs.mkdir(path.dirname(destPath), { recursive: true });
    await fs.copyFile(sourcePath, destPath);
  }
}
