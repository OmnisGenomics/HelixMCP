import { ulid } from "ulid";
import { createHash } from "crypto";
import { encodeCrockfordBase32_128bits } from "./canonicalJson.js";

export type ProjectId = `proj_${string}`;
export type ArtifactId = `art_${string}`;
export type RunId = `run_${string}`;

function prefixed(prefix: string): `${string}_${string}` {
  return `${prefix}_${ulid()}` as const;
}

export function newProjectId(): ProjectId {
  return prefixed("proj") as ProjectId;
}

export function newArtifactId(): ArtifactId {
  return prefixed("art") as ArtifactId;
}

export function newRunId(): RunId {
  return prefixed("run") as RunId;
}

export function deriveRunIdFromParts(parts: string[]): RunId {
  const h = createHash("sha256");
  for (const p of parts) h.update(p).update("|");
  const digest = h.digest();
  const first16 = digest.subarray(0, 16);
  return `run_${encodeCrockfordBase32_128bits(first16)}` as RunId;
}
