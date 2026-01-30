# Events v1 (FROZEN)

Status: **FROZEN** as of HelixMCP v0.9.0.

This document defines the Event v1 envelope and the semantics for event kinds emitted into `run_events`. Consumers should rely on Events v1, not log text scraping.

## What “FROZEN” means

In all HelixMCP **v1.x** releases:

- The Event v1 envelope fields and meanings will not change in a breaking way.
- New event kinds may be added. Existing kinds will not be removed or repurposed.
- Event payloads may gain new fields, but existing fields will not change meaning.

## Event envelope (v1)

Events are stored as rows in `run_events`, and may also appear as JSONL lines in log artifacts.

**Stable fields (v1):**

- `run_id`: the emitting run id (stored as a DB column; may also be included when exporting events)
- `created_at`: RFC3339 timestamp (stored as the DB `ts` column)
- `kind`: stable kind string (see Kind enum)
- `message`: human readable short text (do not parse)
- `data`: optional structured JSON payload (object or null)

Event versioning:
- Event ABI is **v1** by this document. An explicit `event_version` field may be added in a future additive change, but consumers should treat this document as the version signal today.

HelixMCP records append-only events in Postgres in the `run_events` table for audit and debugging. Events are **not** used for run identity or replay.

## Storage schema

`db/schema.sql` defines:

- `event_id`: bigint (DB primary key)
- `run_id`: run identifier (FK to `runs`)
- `ts`: timestamp (`timestamptz`, DB default `NOW()`)
- `kind`: event kind (string)
- `message`: optional human-readable message
- `data`: optional JSON payload

## Log-line representation (JSONL)

Log artifacts may contain JSONL lines shaped like:

```json
{
  "ts": "2026-01-29T00:00:00.000Z",
  "kind": "exec.plan",
  "message": "backend=docker image=…",
  "data": { "any": "json" }
}
```

Notes:
- In Postgres, `run_id` is a separate column and `ts` is the DB timestamp.
- Treat Postgres as the source of truth.

## Kind conventions (v1)

Event kinds are **strings**, with these conventions:

- Lifecycle: `run.*`
- Execution planning/results: `exec.*`
- Slurm submission/collection: `slurm.*`
- Collection orchestration: `collect.*`

### Lifecycle kinds (emitted by `ToolRun`)

- `run.started`
- `run.queued`
- `run.running`
- `run.succeeded`
- `run.failed`
- `run.blocked`

### Common execution kinds

- `exec.plan`
- `exec.result`

### Slurm kinds

- `slurm.submit.plan`
- `slurm.submit.script_artifact`
- `slurm.submit.ok`
- `slurm.collect.start`
- `slurm.collect.outputs_registered`
- `slurm.collect.done`
- `slurm.collect.failed`

### Simulation-only kinds (in silico)

- `simulate.qc`
- `simulate.align`

## Mutation rules

- Events are append-only.
- `*_job_get` tools are **read-only** with respect to the target run: they must not mutate the target run. They create their own observation runs and events.

## Compatibility rules

- Adding a new `kind` is an additive change.
- Changing the meaning of an existing `kind` or its `data` shape is a breaking change and must be versioned.
