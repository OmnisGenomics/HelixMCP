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
