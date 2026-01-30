export type IncludeBlobsMode = "none" | "selected" | "all";

export const BUNDLE_MANIFEST_V1_SCHEMA_ID = "helixmcp:bundle:manifest:v1" as const;

export interface BundleFileEntry {
  path: string;
  sha256: `sha256:${string}`;
  size_bytes: string;
}

export interface BundleManifestV1 {
  bundle_version: 1;
  root_run_id: string;
  include_blobs: IncludeBlobsMode;
  max_blob_bytes: string;
  warnings?: string[];
  files: BundleFileEntry[];
}
