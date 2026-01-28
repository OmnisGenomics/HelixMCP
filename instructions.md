# Working agreement (repo-local)

## Product invariants

1. **Everything is an Artifact**: tools accept/return `artifact_id`s; file paths are only allowed at controlled import boundaries.
2. **Every tool call is a Run**: capture normalized params, input/output checksums, logs, and environment snapshot.
3. **Policy-first execution**: tool calls are validated by JSON Schema and then gated by a YAML policy (allowlists, quotas, egress rules).
4. **Deterministic + replayable**: the same inputs + policy + tool version should reproduce identical outputs (or fail identically).
5. **Deterministic identity**: `run_id` is derived from `(toolName, contractVersion, policyHash, canonicalParamsHash)`; canonical params are stored once and referenced by hash.

## Repo layout (initial)

```text
contracts/          JSON Schemas for MCP tools (stable contracts)
db/                 Postgres schema + migrations
docs/               Architecture notes
policies/           Policy YAML (tool gating + quotas)
src/                Gateway implementation (TypeScript)
tests/              Unit tests (vitest + pg-mem)
```

## Development commands

```bash
npm install
npm run typecheck
npm test
npm run dev
```

## Local imports (in silico boundary)

`artifact_import` with `source.kind=local_path` is only permitted under allowlisted prefixes. The default policy allows:

- `/tmp`
- `var/import`
- `${GATEWAY_IMPORT_ROOT}` (expanded from the environment if set)

## Execution workspaces

Real tools run in a per-run workspace under `RUNS_DIR` (default: `var/runs`):

- `in/` materialized inputs (from `artifact_id`)
- `out/` tool outputs (captured and re-registered as new artifacts)
- `meta/` execution metadata

## Validation strategy

- Unit tests validate: schema validation, policy gating, deterministic ID/checksum behavior, and provenance row creation.
- `pg-mem` backs tests to keep the Postgres surface realistic without needing a live DB.
