# HelixMCP Architecture (opinionated)

HelixMCP is a governed MCP gateway focused on **deterministic execution**, **artifact integrity**, and **replayable provenance** for bioinformatics-style workloads (in silico).

This is not “another tool wrapper”. It is the platform layer that makes tool wrappers safe and usable in serious orgs.

## Artifact-first model (no file paths)

HelixMCP treats every non-trivial payload as an **Artifact**:

- Tools accept and return `artifact_id` handles.
- The assistant sees metadata + previews + reports, not raw blobs.
- File paths are only allowed at explicit **import boundaries** (`artifact_import`), and they are policy-gated.

Why this matters:

- Prevents accidental large-data slurping into chat.
- Makes data movement explicit and auditable.
- Enables provenance to refer to immutable content hashes instead of machine-local paths.

## Deterministic `run_id` (what’s hashed)

Every tool call is a Run with a deterministic identity:

`run_id = H(tool_name, contract_version, policy_hash, canonical_params_hash)`

Where:

- `policy_hash` is a hash of the loaded policy snapshot (after env expansion).
- `canonical_params_hash` is the hash of canonicalized input parameters (stable JSON).

Canonical parameters are stored once in `param_sets` and referenced by hash from the `runs` table. This is the core “boring and correct” foundation for replay and audit.

## Replay semantics (what replays and what doesn’t)

HelixMCP can short-circuit execution by returning the stored `result_json` for a previously succeeded run with the same `run_id`.

Two key rules:

1. Replay keys must be derived from **immutable inputs** (artifact checksums, pinned images, stable params).
2. Tools whose outputs depend on mutable state must define a **snapshot boundary** inside canonical params.

Example: `artifact_list` is snapshot-based. It includes `(as_of_created_at, artifact_count)` in canonical params and filters results to `created_at <= as_of_created_at` so replay is correct even if the project changes later.

## Execution boundary (policy before work runs)

HelixMCP enforces policy twice:

1. **Before execution**: tool allowlists, input validation, import boundaries.
2. **At the execution boundary**: threads/runtime quotas and backend-specific guards.

This “defense in depth” prevents accidental bypass via new tools or future composition.

### Docker execution (current)

The first real execution path uses Docker:

- Network mode is policy-controlled (default: `none`).
- Image is policy-allowlisted (pinned digest).
- Container root filesystem is read-only.
- A per-run workspace under `RUNS_DIR` is mounted at `/work`:
  - `in/` materialized inputs (copied from artifact objects)
  - `out/` tool outputs (captured and re-registered as new artifacts)
  - `meta/` execution metadata

The gateway never exposes arbitrary shell execution. Each tool is an explicit contract and maps to a concrete backend spec.

## Provenance and auditability

Runs persist:

- tool identity + contract version
- canonical params hash + policy hash
- input/output artifact links
- log artifact
- structured result JSON (the replay payload)

The intent is that every run is inspectable, replayable, and safe to export into pipeline systems later (e.g., Nextflow stubs).

## Relationship to BioinfoMCP (complement, not competition)

BioinfoMCP and other tool-wrapper projects solve “how to expose many tools as MCP servers”.

HelixMCP solves the missing layer for org-grade adoption:

- governed execution (policy gates + quotas)
- artifact lifecycle (IDs, checksums, previews)
- deterministic replay and provenance
- execution backends (Docker now; HPC schedulers later)

In practice: BioinfoMCP-style wrappers can plug into HelixMCP’s execution fabric and governance model, while HelixMCP keeps stable, intent-level contracts at the gateway boundary.

