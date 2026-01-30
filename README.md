# HelixMCP (BioMCP Fabric Gateway) — in silico

Governed, reproducible MCP gateway skeleton for bioinformatics compute + database tooling:

- Artifact-first data model (handles, metadata, previews)
- Policy-gated tool calls (allowlists + quotas)
- Run/provenance capture (inputs, outputs, logs, checksums)
- Docker execution backend (read-only rootfs, no network by default)
- MCP server surface for intent-level tools

This repository is intentionally **in silico** only: it focuses on deterministic execution, verification, and artifact/provenance handling as software.

## Quickstart (dev)

```bash
npm install
npm run dev
```

## Tests

```bash
npm test
HELIXMCP_TEST_DOCKER=1 npm test
```

## Hybrid toolpack example (Docker or Slurm)

- `samtools_flagstat` supports `backend: "docker" | "slurm"` (default via policy; falls back to `"docker"`).
- This is the same tool contract and deterministic `run_id`, with the backend selecting the execution fabric (immediate Docker vs queued Slurm).
- With `backend: "slurm"`, the tool checkpoints `queued` and returns a `run_id` plus Slurm metadata; use `slurm_job_collect` to ingest declared `out/` outputs as artifacts.
- Set `execution.default_backend: "slurm"` in policy to make Slurm the default (requires `slurm` policy config).
- For a cluster smoke test see `docs/slurm_cluster_smoke.md`.

## FASTQ QC bundle (Slurm multi-step)

The `qc_bundle_fastq` tool is deterministic and audit-friendly. On `backend: "slurm"` it never polls; it advances only when run state and artifacts are visible in Postgres.

1. Call `qc_bundle_fastq` (returns `phase="fastqc_submitted"` and `expected_collect_run_ids`).
2. Collect each FastQC run: call `slurm_job_collect` for every run id listed.
3. Call `qc_bundle_fastq` again (returns `phase="multiqc_submitted"` and the MultiQC run id to collect).
4. Collect the MultiQC run: call `slurm_job_collect` for that run id.
5. Call `qc_bundle_fastq` a final time to get `phase="complete"` and the final `bundle_report_artifact_id`.

## Audit bundle export (CLI)

Export and verify an offline audit bundle for a run (dir or deterministic `.tar`): see `docs/bundle_export.md`.

### Configuration

- `DATABASE_URL` (optional): if unset, gateway uses in-memory Postgres (`pg-mem`) for dev.
- `GATEWAY_POLICY_PATH` (default: `policies/default.policy.yaml`)
- `GATEWAY_IMPORT_ROOT` (optional): expands `${GATEWAY_IMPORT_ROOT}` in `local_path_prefix_allowlist` for `artifact_import` with `local_path`.
- `OBJECT_STORE_DIR` (default: `var/objects`)
- `RUNS_DIR` (default: `var/runs`): per-run workspaces for tool execution.
- `AUTO_SCHEMA` (default: `true`): apply `db/schema.sql` on startup (recommended for dev).

## Next steps

- Stand up Postgres + object store (see `instructions.md`)
- Implement real executors (Slurm/K8s adapters) behind the policy boundary
- For Slurm smoke test see `docs/slurm_cluster_smoke.md`

## License

Apache-2.0 (see `LICENSE`).

## Security

See `SECURITY.md`. Please report vulnerabilities via GitHub Security Advisories (private reporting).

## Stability

See `VERSIONING.md`, `TOOLPACK_ABI_V1.md`, and `EVENTS_V1.md` for the stability contract.
