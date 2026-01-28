import { promises as fs } from "fs";
import path from "path";
import type { RunId } from "../core/ids.js";

export interface RunWorkspace {
  rootDir: string;
  inDir: string;
  outDir: string;
  metaDir: string;
  inPath(name: string): string;
  outPath(name: string): string;
  metaPath(name: string): string;
}

function safeJoin(baseDir: string, name: string): string {
  const joined = path.join(baseDir, name);
  const rel = path.relative(baseDir, joined);
  if (rel.startsWith("..") || path.isAbsolute(rel)) {
    throw new Error(`unsafe workspace path: ${name}`);
  }
  return joined;
}

export { safeJoin };

export async function createRunWorkspace(rootDir: string, runId: RunId): Promise<RunWorkspace> {
  const root = path.resolve(rootDir, runId);
  const inDir = path.join(root, "in");
  const outDir = path.join(root, "out");
  const metaDir = path.join(root, "meta");

  await fs.mkdir(inDir, { recursive: true });
  await fs.mkdir(outDir, { recursive: true });
  await fs.mkdir(metaDir, { recursive: true });

  return {
    rootDir: root,
    inDir,
    outDir,
    metaDir,
    inPath: (name: string) => safeJoin(inDir, name),
    outPath: (name: string) => safeJoin(outDir, name),
    metaPath: (name: string) => safeJoin(metaDir, name)
  };
}
