import * as z from "zod/v4";

const ulid26 = "[0-9A-HJKMNP-TV-Z]{26}";

export const zProjectId = z.string().regex(new RegExp(`^proj_${ulid26}$`), "invalid project_id");
export const zRunId = z.string().regex(new RegExp(`^run_${ulid26}$`), "invalid run_id");
export const zArtifactId = z.string().regex(new RegExp(`^art_${ulid26}$`), "invalid artifact_id");
export const zSha256 = z.string().regex(/^sha256:[a-f0-9]{64}$/);

export const zArtifactType = z.enum([
  "FASTQ_GZ",
  "BAM",
  "BAI",
  "VCF",
  "H5AD",
  "TSV",
  "CSV",
  "JSON",
  "TEXT",
  "HTML",
  "PDF",
  "LOG",
  "UNKNOWN"
]);

export const zArtifactSummary = z.object({
  artifact_id: zArtifactId,
  project_id: zProjectId,
  type: zArtifactType,
  mime_type: z.string(),
  size_bytes: z.string(),
  checksum_sha256: zSha256,
  label: z.string().nullable(),
  created_at: z.string(),
  created_by_run_id: zRunId.nullable(),
  metadata: z.record(z.string(), z.unknown())
});

export const zProvenance = z.object({
  provenance_run_id: zRunId
});

export const zArtifactImportInput = z.object({
  project_id: zProjectId,
  type_hint: zArtifactType.optional(),
  label: z.string().min(1).max(256).optional(),
  source: z.discriminatedUnion("kind", [
    z.object({ kind: z.literal("local_path"), path: z.string().min(1) }),
    z.object({ kind: z.literal("inline_text"), text: z.string().max(1048576) })
  ])
});

export const zArtifactImportOutput = zProvenance.extend({
  artifact: zArtifactSummary,
  log_artifact_id: zArtifactId
});

export const zArtifactGetInput = z.object({
  artifact_id: zArtifactId
});

export const zArtifactGetOutput = zProvenance.extend({
  artifact: zArtifactSummary,
  log_artifact_id: zArtifactId
});

export const zArtifactListInput = z.object({
  project_id: zProjectId,
  limit: z.number().int().min(1).max(500).default(100)
});

export const zArtifactListOutput = zProvenance.extend({
  as_of_created_at: z.string().nullable(),
  artifact_count: z.string(),
  artifacts: z.array(zArtifactSummary),
  log_artifact_id: zArtifactId
});

export const zArtifactPreviewTextInput = z.object({
  artifact_id: zArtifactId,
  max_bytes: z.number().int().min(1).max(262144).default(8192),
  max_lines: z.number().int().min(1).max(5000).default(200)
});

export const zArtifactPreviewTextOutput = zProvenance.extend({
  artifact_id: zArtifactId,
  preview: z.string(),
  truncated: z.boolean(),
  log_artifact_id: zArtifactId
});

export const zSimulateQcFastqInput = z.object({
  project_id: zProjectId,
  reads_1: zArtifactId,
  reads_2: zArtifactId.optional(),
  threads: z.number().int().min(1).max(64).default(4)
});

export const zSimulateQcFastqOutput = zProvenance.extend({
  qc: z.object({
    reads_estimated: z.number(),
    q30_estimated: z.number(),
    gc_pct_estimated: z.number()
  }),
  report_artifact_id: zArtifactId,
  log_artifact_id: zArtifactId
});

export const zSimulateAlignReadsInput = z.object({
  project_id: zProjectId,
  reads_1: zArtifactId,
  reads_2: zArtifactId.optional(),
  reference: z.object({ alias: z.string().min(1).max(64) }),
  read_group: z
    .object({
      id: z.string().min(1).max(64),
      sm: z.string().min(1).max(64),
      pl: z.string().min(1).max(64).optional()
    })
    .optional(),
  threads: z.number().int().min(1).max(64).default(8),
  sort: z.boolean().default(true),
  mark_duplicates: z.boolean().default(false)
});

export const zSimulateAlignReadsOutput = zProvenance.extend({
  bam_sorted: zArtifactId,
  bai: zArtifactId,
  qc: z.object({
    mapped_pct: z.number(),
    duplication_pct: z.number(),
    insert_size_median: z.number()
  }),
  log_artifact_id: zArtifactId
});

export const zExportNextflowInput = z.object({
  run_id: zRunId
});

export const zExportNextflowOutput = zProvenance.extend({
  exported_run_id: zRunId,
  nextflow_script_artifact_id: zArtifactId,
  log_artifact_id: zArtifactId
});
