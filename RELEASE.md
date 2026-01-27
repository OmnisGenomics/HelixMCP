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
