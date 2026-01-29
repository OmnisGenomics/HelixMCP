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

## Slurm toolpack example

- `samtools_flagstat_slurm`: submits a deterministic, policy-gated Slurm plan (apptainer-only, `network=none`) and returns a `run_id` plus Slurm metadata.
- Use `slurm_job_collect` with that `run_id` to ingest declared `out/` outputs as artifacts.
- For a cluster smoke test see `docs/slurm_cluster_smoke.md`.

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
