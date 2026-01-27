import type { ArtifactId, ProjectId, RunId } from "./ids.js";
import type { JsonObject } from "./json.js";

export type ArtifactType =
  | "FASTQ_GZ"
  | "BAM"
  | "BAI"
  | "VCF"
  | "H5AD"
  | "TSV"
  | "CSV"
  | "JSON"
  | "TEXT"
  | "HTML"
  | "PDF"
  | "LOG"
  | "UNKNOWN";

export interface ArtifactRecord {
  artifactId: ArtifactId;
  projectId: ProjectId;
  type: ArtifactType;
  uri: string;
  mimeType: string;
  sizeBytes: bigint;
  checksumSha256: `sha256:${string}`;
  label: string | null;
  createdAt: string;
  createdByRunId: RunId | null;
  metadata: JsonObject;
}
