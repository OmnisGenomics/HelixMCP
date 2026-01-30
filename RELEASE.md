# HelixMCP v0.9.5

This release adds deterministic, offline-verifiable audit bundle export as a CLI.

What is included:
- `scripts/bundle_export.ts`: exports a run bundle (dir or deterministic `.tar`) with optional blob inclusion and per-blob size caps
- `scripts/bundle_verify.ts`: verifies bundle structure and content hashes offline (supports dir or `.tar`)
- Bundle manifest v1 contract: `contracts/bundles/bundle_manifest.v1.schema.json`

# HelixMCP v0.9.0

HelixMCP v0.9.0 freezes Toolpack ABI v1 and Events v1. All v1.x releases are additive-only. Toolpacks built against ABI v1 will continue to run unchanged.

# HelixMCP v0.8.1

This release adds a deterministic, audit-friendly FASTQ QC on-ramp built from toolpacks.

What is included:
- `MD` artifact type end-to-end (contracts + schemas + MIME + allowlists)
- FASTQ QC toolpacks shipped: `fastqc`, `multiqc`, `qc_bundle_fastq`
- `qc_bundle_fastq` supports explicit Slurm multi-step phases (A/B queued, C final) using DB truth only (no scheduler polling)
- Regression guard: Slurm phase A checkpoints queued without requiring MultiQC outputs; replay does not resubmit

# HelixMCP v0.8.0

This release adds read-only job state observation tools and locks the run lifecycle state machine.

What is included:
- `slurm_job_get`: workspace-first state; optional scheduler query (`sacct` → `squeue`) behind `slurm.allow_scheduler_queries` (default false)
- `docker_job_get`: DB-only state observation for Docker-backed runs (no external calls)
- Fail-closed run status transitions (no `running → queued`; terminal states never transition)
- Run lifecycle documented in `ARCHITECTURE.md`

# HelixMCP v0.7.0

This release adds hybrid toolpacks: a single tool can run on Docker or submit a Slurm job while preserving deterministic identity, policy-gated execution, and replay semantics.

What is included:
- Hybrid toolpack support (`ToolDefinition.planKind="hybrid"` with per-run `selectedPlanKind`)
- `samtools_flagstat` contract v2: adds `backend: "docker" | "slurm"` (policy default supported)
- GitHub Actions CI workflow (typecheck + tests) to support branch protection

# HelixMCP v0.6.1

This is a small documentation polish release.

What is included:
- README Slurm toolpack example: `samtools_flagstat_slurm`
- README links to `docs/slurm_cluster_smoke.md`

# HelixMCP v0.6.0

This release extends Toolpacks to the Slurm island: toolpacks can now submit deterministic Slurm plans (apptainer-only, network none) with replay semantics.

What is included:
- Slurm toolpack support (validators, queued checkpoint semantics, replay without resubmission)
- Slurm plan IO equality enforced against declared outputs (role/type/label/srcRelpath)
- Apptainer image must be `docker://...@sha256:...` (fail closed even if allowlisted)
- Built-in Slurm toolpack: `samtools_flagstat_slurm`
- Docker toolpack input hardening (trimmed roles, unique destName, no hidden IO)

# HelixMCP v0.5.0

This release introduces Toolpacks: a fail-closed, typed registry for scaling tools without turning the gateway into a “god file”.

What is included:
- Toolpack registration-time validation (name/version invariants, plan kind, declared outputs)
- JSON-safe canonical params enforcement (toolpacks must produce replayable, hashable params)
- Centralized Docker execution hardening for toolpacks (digest-pinned images, `network=none`, read-only rootfs, threads/runtime enforced at execution boundary)
- Built-in toolpacks migrated: `seqkit_stats`, `samtools_flagstat`

# HelixMCP v0.4.0

This release adds a deterministic Slurm “submit + collect” island (apptainer only, network none) while preserving trust-layer invariants.

What is included:
- `slurm_submit` tool (hashes a canonicalized `SlurmJobSpecV1` into `run_id`; stores a deterministic `slurm_script_v1` artifact; records Slurm job id as an event)
- `slurm_job_collect` tool (collects declared outputs into artifacts, enforces size caps, and finalizes the target run idempotently)
- Slurm policy surface: allowlists for partition/account/qos/constraint and apptainer image digests; hard `network_mode=none`
- `runtime.instance_id` included in `policyHash` and required when `DATABASE_URL` is set to avoid cross-gateway run collisions
- Stubbed integration test proving submit → collect → replay without resubmission

# HelixMCP v0.3.0

This release adds a second real, policy-gated Docker tool and reinforces the “contract-first execution” pattern.

What is included:
- Second real tool execution via Docker: `samtools_flagstat` (pinned image digest)
- BAM → report artifact + parsed structured metrics
- Deterministic replay still holds for the Docker execution path
- Docker integration test included (gated behind `HELIXMCP_TEST_DOCKER=1`)

# HelixMCP v0.2.0

This release crosses the line from “trust layer only” to “trust layer + real execution”.

What is included:
- First real tool execution via Docker: `seqkit_stats` (pinned image digest)
- Per-run workspace with input materialization and output capture into artifacts
- Docker policy hardening: image allowlist + network mode, enforced at execution boundary
- Deterministic replay still holds for the real execution path
- Docker integration test included (auto-skips if Docker is unavailable)

# HelixMCP v0.1.0

This release establishes the trust layer for a governed MCP gateway.

What is included:
- Deterministic `run_id` derivation and replay semantics
- Canonical parameter sets stored once and deduplicated
- Artifact integrity enforcement with content hashing
- Artifact previews with explicit size caps
- Policy-gated imports with path containment and symlink denial
- Idempotent run input/output linking
- Snapshot-based `artifact_list` replay for determinism
- Runner abstraction (local + docker/slurm stubs)
- Nextflow export stub for pipeline handoff

What is NOT included:
- No real bioinformatics tool execution yet
- No production-grade scheduler or HPC integration
- No multi-tenant auth or RBAC

Status:
This is an open-core foundation. The goal of v0.1.0 is to make trust, replay, and provenance boring and correct before adding execution.
