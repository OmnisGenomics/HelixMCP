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

export const zSeqkitStatsInput = z.object({
  project_id: zProjectId,
  input_artifact_id: zArtifactId,
  threads: z.number().int().min(1).max(64).default(2)
});

export const zSeqkitStatsOutput = zProvenance.extend({
  report_artifact_id: zArtifactId,
  stats: z.object({
    file: z.string().nullable(),
    format: z.string().nullable(),
    type: z.string().nullable(),
    num_seqs: z.number().nullable(),
    sum_len: z.number().nullable(),
    min_len: z.number().nullable(),
    avg_len: z.number().nullable(),
    max_len: z.number().nullable(),
    raw: z.record(z.string(), z.string())
  }),
  log_artifact_id: zArtifactId
});

export const zSamtoolsFlagstatInput = z.object({
  project_id: zProjectId,
  bam_artifact_id: zArtifactId
});

const zFlagstatCount = z.object({
  passed: z.number(),
  failed: z.number()
});

export const zSamtoolsFlagstatOutput = zProvenance.extend({
  report_artifact_id: zArtifactId,
  flagstat: z.object({
    total: zFlagstatCount.nullable(),
    secondary: zFlagstatCount.nullable(),
    supplementary: zFlagstatCount.nullable(),
    duplicates: zFlagstatCount.nullable(),
    mapped: zFlagstatCount.nullable(),
    paired_in_sequencing: zFlagstatCount.nullable(),
    read1: zFlagstatCount.nullable(),
    read2: zFlagstatCount.nullable(),
    properly_paired: zFlagstatCount.nullable(),
    with_itself_and_mate_mapped: zFlagstatCount.nullable(),
    singletons: zFlagstatCount.nullable(),
    with_mate_mapped_to_different_chr: zFlagstatCount.nullable(),
    with_mate_mapped_to_different_chr_mapq5: zFlagstatCount.nullable(),
    raw_lines: z.array(z.string())
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

export const zSlurmJobSpecV1 = z.object({
  version: z.literal(1),
  resources: z.object({
    time_limit_seconds: z.number().int().min(1).max(604800),
    cpus: z.number().int().min(1).max(256),
    mem_mb: z.number().int().min(1).max(2097152),
    gpus: z.number().int().min(1).max(16).nullable().optional(),
    gpu_type: z.string().min(1).max(64).nullable().optional()
  }),
  placement: z.object({
    partition: z.string().min(1).max(64),
    account: z.string().min(1).max(64),
    qos: z.string().min(1).max(64).nullable().optional(),
    constraint: z.string().min(1).max(256).nullable().optional()
  }),
  execution: z.object({
    kind: z.literal("container"),
    container: z.object({
      engine: z.literal("apptainer"),
      image: z.string().min(1).max(512),
      network_mode: z.literal("none"),
      readonly_rootfs: z.literal(true)
    }),
    command: z.object({
      argv: z.array(z.string().min(1)).min(1),
      workdir: z.string().min(1).default("/work"),
      env: z.record(z.string(), z.string())
    })
  }),
  io: z.object({
    inputs: z.array(
      z.object({
        role: z.string().min(1).max(64),
        artifact_id: zArtifactId,
        checksum_sha256: zSha256,
        dest_relpath: z.string().min(1).max(256)
      })
    ),
    outputs: z.array(
      z.object({
        role: z.string().min(1).max(64),
        src_relpath: z.string().min(1).max(256),
        type: z.string().min(1).max(32),
        label: z.string().min(1).max(256)
      })
    )
  })
});

export const zSlurmSubmitInput = z.object({
  project_id: zProjectId,
  job_spec: zSlurmJobSpecV1
});

export const zSlurmSubmitOutput = zProvenance.extend({
  slurm_job_id: z.string().min(1).max(64),
  slurm_script_artifact_id: zArtifactId,
  log_artifact_id: zArtifactId
});

export const zSlurmJobCollectInput = z.object({
  run_id: zRunId
});

export const zSlurmJobCollectOutput = zProvenance.extend({
  target_run_id: zRunId,
  exit_code: z.number().int(),
  artifacts_by_role: z.record(z.string(), zArtifactId),
  log_artifact_id: zArtifactId
});

export const zBackend = z.enum(["docker", "slurm"]);

export const zSamtoolsFlagstatInputV2 = zSamtoolsFlagstatInput.extend({
  backend: zBackend.optional()
});

export const zSamtoolsFlagstatOutputV2 = z.union([zSamtoolsFlagstatOutput, zSlurmSubmitOutput]);
