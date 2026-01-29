# Events v1 (run_events)

HelixMCP records append-only events in Postgres in the `run_events` table for audit and debugging. Events are **not** used for run identity or replay.

## Storage schema

`db/schema.sql` defines:

- `event_id`: bigint (DB primary key)
- `run_id`: run identifier (FK to `runs`)
- `ts`: timestamp (`timestamptz`, DB default `NOW()`)
- `kind`: event kind (string)
- `message`: optional human-readable message
- `data`: optional JSON payload

## Event envelope (stable)

An event is represented as:

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
- Log artifacts may also contain JSONL lines with the same shape; treat Postgres as the source of truth.

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

