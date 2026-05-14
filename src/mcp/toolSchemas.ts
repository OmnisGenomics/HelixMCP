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
  "ZIP",
  "MD",
  "TEXT",
  "HTML",
  "PDF",
  "PNG",
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

export const zArtifactPreviewImageInput = z.object({
  artifact_id: zArtifactId
});

export const zArtifactPreviewImageOutput = zProvenance.extend({
  artifact_id: zArtifactId,
  format: z.literal("PNG"),
  width_px: z.number().int().positive(),
  height_px: z.number().int().positive(),
  bit_depth: z.number().int().min(1).max(16),
  color_type_code: z.number().int().min(0).max(6),
  color_type: z.enum(["grayscale", "rgb", "indexed", "grayscale_alpha", "rgba"]),
  channel_count: z.number().int().min(1).max(4),
  has_alpha: z.boolean(),
  interlaced: z.boolean(),
  compression_method: z.number().int().min(0).max(255),
  filter_method: z.number().int().min(0).max(255),
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

export const zFastqcInput = z.object({
  project_id: zProjectId,
  reads_artifact_id: zArtifactId,
  backend: zBackend.optional(),
  threads: z.number().int().min(1).max(64).default(2)
});

export const zFastqcOutputDocker = zProvenance.extend({
  fastqc_html_artifact_id: zArtifactId,
  fastqc_zip_artifact_id: zArtifactId,
  metrics: z.object({
    pass: z.number().int(),
    warn: z.number().int(),
    fail: z.number().int(),
    modules: z.record(z.string(), z.enum(["PASS", "WARN", "FAIL"])),
    raw_lines: z.array(z.string())
  }),
  log_artifact_id: zArtifactId
});

// NOTE: Avoid z.union here. MCP SDK schema plumbing is stricter than we want for hybrid tools.
// This schema is a permissive, single-object contract that accepts either the Docker-complete shape
// or the queued Slurm submit shape (or both) without crashing the server.
export const zFastqcOutputV1 = zProvenance.extend({
  // docker-complete fields
  fastqc_html_artifact_id: zArtifactId.optional(),
  fastqc_zip_artifact_id: zArtifactId.optional(),
  metrics: z
    .object({
      pass: z.number().int(),
      warn: z.number().int(),
      fail: z.number().int(),
      modules: z.record(z.string(), z.enum(["PASS", "WARN", "FAIL"])),
      raw_lines: z.array(z.string())
    })
    .optional(),

  // slurm-queued fields
  slurm_job_id: z.string().min(1).max(64).optional(),
  slurm_script_artifact_id: zArtifactId.optional(),

  log_artifact_id: zArtifactId
});

export const zMultiqcInput = z.object({
  project_id: zProjectId,
  fastqc_zip_artifact_ids: z.array(zArtifactId).min(1).max(1000),
  backend: zBackend.optional(),
  threads: z.number().int().min(1).max(64).default(2)
});

export const zMultiqcOutputDocker = zProvenance.extend({
  multiqc_html_artifact_id: zArtifactId,
  multiqc_data_zip_artifact_id: zArtifactId,
  metrics: z.object({
    samples: z.number().int().nullable(),
    raw_general_stats_lines: z.array(z.string()).nullable()
  }),
  log_artifact_id: zArtifactId
});

export const zMultiqcOutputV1 = zProvenance.extend({
  // docker-complete fields
  multiqc_html_artifact_id: zArtifactId.optional(),
  multiqc_data_zip_artifact_id: zArtifactId.optional(),
  metrics: z
    .object({
      samples: z.number().int().nullable(),
      raw_general_stats_lines: z.array(z.string()).nullable()
    })
    .optional(),

  // slurm-queued fields
  slurm_job_id: z.string().min(1).max(64).optional(),
  slurm_script_artifact_id: zArtifactId.optional(),

  log_artifact_id: zArtifactId
});

export const zQcBundleFastqInput = z.object({
  project_id: zProjectId,
  reads_1_artifact_id: zArtifactId,
  reads_2_artifact_id: zArtifactId.optional(),
  backend: zBackend.optional(),
  threads_fastqc: z.number().int().min(1).max(64).default(2),
  threads_multiqc: z.number().int().min(1).max(64).default(2)
});

export const zQcBundlePhase = z.enum(["fastqc_submitted", "multiqc_submitted", "complete"]);

export const zQcBundleFastqOutputV1 = zProvenance.extend({
  phase: zQcBundlePhase,
  expected_collect_run_ids: z.array(zRunId),
  bundle_report_artifact_id: zArtifactId,

  fastqc_run_ids: z.array(zRunId),
  multiqc_run_id: zRunId.nullable(),

  fastqc: z.object({
    reads_1: z.object({
      run_id: zRunId,
      fastqc_html_artifact_id: zArtifactId.nullable(),
      fastqc_zip_artifact_id: zArtifactId.nullable(),
      metrics: z
        .object({
          pass: z.number().int(),
          warn: z.number().int(),
          fail: z.number().int()
        })
        .nullable()
    }),
    reads_2: z
      .object({
        run_id: zRunId,
        fastqc_html_artifact_id: zArtifactId.nullable(),
        fastqc_zip_artifact_id: zArtifactId.nullable(),
        metrics: z
          .object({
            pass: z.number().int(),
            warn: z.number().int(),
            fail: z.number().int()
          })
          .nullable()
      })
      .nullable()
  }),

  multiqc: z
    .object({
      run_id: zRunId,
      multiqc_html_artifact_id: zArtifactId.nullable(),
      multiqc_data_zip_artifact_id: zArtifactId.nullable()
    })
    .nullable(),

  graph: z.object({
    graph_digest_sha256: zSha256,
    nodes: z.array(
      z.object({
        run_id: zRunId,
        tool_name: z.string().min(1),
        contract_version: z.string().min(1)
      })
    ),
    edges: z.array(
      z.object({
        from_run_id: zRunId,
        to_run_id: zRunId,
        kind: z.string().min(1)
      })
    )
  }),

  log_artifact_id: zArtifactId
});

export const zSamtoolsFlagstatInputV2 = zSamtoolsFlagstatInput.extend({
  backend: zBackend.optional()
});

export const zSamtoolsFlagstatOutputV2 = zProvenance.extend({
  // docker-complete fields
  report_artifact_id: zArtifactId.optional(),
  flagstat: z
    .object({
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
    })
    .optional(),

  // slurm-queued fields
  slurm_job_id: z.string().min(1).max(64).optional(),
  slurm_script_artifact_id: zArtifactId.optional(),

  log_artifact_id: zArtifactId
});

export const zSlurmJobGetInput = z.object({
  run_id: zRunId
});

export const zJobGetSource = z.enum(["workspace_only", "workspace+sacct", "workspace+squeue"]);
export const zRunState = z.enum(["queued", "running", "succeeded", "failed", "blocked", "unknown"]);

export const zSlurmJobGetOutput = zProvenance.extend({
  target_run_id: zRunId,
  slurm_job_id: z.string().min(1).max(64).nullable(),
  source: zJobGetSource,
  warnings: z.array(z.string()),
  state: zRunState,
  exit_code: z.number().int().nullable(),
  artifacts_by_role: z.record(z.string(), zArtifactId),
  db: z.object({
    status: zRunState,
    exit_code: z.number().int().nullable(),
    finished_at: z.string().nullable()
  }),
  workspace: z.object({
    has_stdout: z.boolean(),
    has_stderr: z.boolean(),
    has_exit_code: z.boolean(),
    exit_code: z.number().int().nullable()
  }),
  scheduler: z
    .object({
      source: z.enum(["sacct", "squeue"]).nullable(),
      state_raw: z.string().nullable(),
      exit_code: z.number().int().nullable()
    })
    .nullable(),
  log_artifact_id: zArtifactId
});

export const zDockerJobGetInput = z.object({
  run_id: zRunId
});

export const zDockerJobGetOutput = zProvenance.extend({
  target_run_id: zRunId,
  source: z.literal("db_only"),
  warnings: z.array(z.string()),
  state: zRunState,
  exit_code: z.number().int().nullable(),
  artifacts_by_role: z.record(z.string(), zArtifactId),
  db: z.object({
    status: zRunState,
    exit_code: z.number().int().nullable(),
    started_at: z.string().nullable(),
    finished_at: z.string().nullable()
  }),
  log_artifact_id: zArtifactId
});

export const zStudioBridgeInfo = z.object({
  bridge_file: z.string().min(1),
  host: z.string().min(1),
  port: z.number().int().min(1).max(65535),
  pid: z.number().int().positive(),
  session_id: z.string().min(1),
  started_at: z.string().min(1),
  project_root: z.string().min(1).optional()
});

export const zStudioVisualizationState = z.object({
  mode: z.enum(["explore", "compare", "verify"]).nullable(),
  editor_profile: z.enum(["spcas9", "hifi", "prime"]).nullable(),
  repair_profile: z.enum(["balanced", "precise", "indel_bias"]).nullable(),
  accessibility_profile: z.enum(["constrained", "baseline", "open"]).nullable(),
  selected_scale_id: z.enum(["sequence", "complex", "cell", "tissue", "population"]).nullable(),
  selected_outcome_label: z.string().nullable(),
  available_outcomes: z.array(z.string()),
  compare_loaded: z.boolean(),
  run_id: z.string().nullable(),
  run_label: z.string().nullable()
});

export const zStudioState = z.object({
  current_tab: z.string().nullable(),
  available_tabs: z.array(z.string()),
  active_run_id: z.string().nullable(),
  project_root: z.string().nullable(),
  visualization: zStudioVisualizationState.nullable()
});

export const zStudioScreenshot = z.object({
  path: z.string().min(1),
  artifact_id: zArtifactId,
  format: z.literal("PNG"),
  width_px: z.number().int().positive(),
  height_px: z.number().int().positive(),
  device_pixel_ratio: z.number().positive(),
  size_bytes: z.string(),
  checksum_sha256: zSha256,
  captured_at: z.string().min(1)
});

export const zStudioGetStateInput = z.object({
  bridge_file: z.string().min(1).optional()
});

export const zStudioGetStateOutput = zProvenance.extend({
  bridge: zStudioBridgeInfo,
  studio_state: zStudioState,
  log_artifact_id: zArtifactId
});

export const zStudioOpenTabInput = z.object({
  bridge_file: z.string().min(1).optional(),
  tab: z.string().min(1).max(64)
});

export const zStudioOpenTabOutput = zProvenance.extend({
  bridge: zStudioBridgeInfo,
  studio_state: zStudioState,
  log_artifact_id: zArtifactId
});

export const zStudioResetLayoutInput = z.object({
  bridge_file: z.string().min(1).optional()
});

export const zStudioResetLayoutOutput = zProvenance.extend({
  bridge: zStudioBridgeInfo,
  studio_state: zStudioState,
  log_artifact_id: zArtifactId
});

export const zStudioLoadEvsInput = z.object({
  bridge_file: z.string().min(1).optional(),
  path: z.string().min(1),
  role: z.enum(["primary", "compare"]).default("primary"),
  open_visualizations: z.boolean().default(true),
  set_compare_mode: z.boolean().default(true),
  apply_layout_preset: z.boolean().default(false),
  focus_sidebars: z.boolean().default(false),
  sync_lightcone: z.boolean().default(false)
});

export const zStudioLoadEvsOutput = zProvenance.extend({
  bridge: zStudioBridgeInfo,
  studio_state: zStudioState,
  log_artifact_id: zArtifactId
});

export const zStudioClearCompareInput = z.object({
  bridge_file: z.string().min(1).optional()
});

export const zStudioClearCompareOutput = zProvenance.extend({
  bridge: zStudioBridgeInfo,
  studio_state: zStudioState,
  log_artifact_id: zArtifactId
});

export const zStudioVisualizationEditInput = z.object({
  bridge_file: z.string().min(1).optional(),
  open_visualizations: z.boolean().default(true),
  mode: z.enum(["explore", "compare", "verify"]).optional(),
  editor_profile: z.enum(["spcas9", "hifi", "prime"]).optional(),
  repair_profile: z.enum(["balanced", "precise", "indel_bias"]).optional(),
  accessibility_profile: z.enum(["constrained", "baseline", "open"]).optional(),
  selected_scale_id: z.enum(["sequence", "complex", "cell", "tissue", "population"]).optional(),
  selected_outcome_label: z.string().min(1).optional()
});

export const zStudioVisualizationEditOutput = zProvenance.extend({
  bridge: zStudioBridgeInfo,
  studio_state: zStudioState,
  log_artifact_id: zArtifactId
});

export const zStudioCaptureScreenshotInput = z.object({
  bridge_file: z.string().min(1).optional(),
  path: z.string().min(1).optional(),
  tab: z.string().min(1).max(64).optional(),
  overwrite: z.boolean().default(false)
});

export const zStudioCaptureScreenshotOutput = zProvenance.extend({
  bridge: zStudioBridgeInfo,
  studio_state: zStudioState,
  screenshot: zStudioScreenshot,
  log_artifact_id: zArtifactId
});
