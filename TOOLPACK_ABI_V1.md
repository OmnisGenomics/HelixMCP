# Toolpack ABI v1

This document defines the **Toolpack ABI v1** for HelixMCP. Toolpacks are TypeScript modules that register deterministic, policy-gated tools without turning the gateway into a monolith.

## Guarantees (what ABI v1 means)

- **Fail-closed startup**: the gateway refuses to start if any registered toolpack violates this ABI.
- **Deterministic run identity**: tool executions derive `run_id` from `(toolName, contractVersion, policyHash, canonicalParamsHash)`.
- **No hidden IO**: toolpacks can only use artifacts provided via the tool input / linked inputs.
- **Declared outputs are enforced**: tools must create exactly the declared output roles with the declared artifact types/labels.

## ABI versioning

Toolpacks must declare:

- `abiVersion: "v1"`

The gateway rejects any toolpack with an unknown/unsupported ABI version.

## Core interface (stable)

Toolpacks implement `ToolDefinition<TArgs, TPlan>` (see `src/toolpacks/types.ts`).

Key fields:

- `abiVersion`: `"v1"` (required)
- `toolName`: stable tool identifier (regex: `[a-z][a-z0-9_]*`)
- `contractVersion`: version of the tool’s MCP contract (regex: `vN`)
- `planKind`: `"docker" | "slurm" | "hybrid"`
- `declaredOutputs[]`: list of output roles + types + labels (and `srcRelpath` for Slurm)
- `canonicalize(args, ctx)`: returns canonical params + an execution plan
- `run(...)`: executes the plan (Docker) or submits/checkpoints (Slurm)

## Canonical params contract (stable)

`canonicalize()` must return a `canonicalParams` object that is:

- JSON-safe (no `undefined`, `bigint`, functions, non-finite numbers, cycles, etc.)
- Deterministic: only immutable inputs (artifact checksums, pinned image digests, stable flags)

The gateway will **canonicalize** toolpack-provided `canonicalParams` using stable JSON serialization before hashing/storing.

## Input linking contract (stable)

`canonicalize()` must also return `inputsToLink[]`:

- `{ artifactId, role }` pairs
- role strings are whitespace-strict

### Docker toolpacks

`plan.inputs[]` must match `inputsToLink[]` **exactly** (same `role` + `artifactId` set). Additional inputs are rejected.

Each Docker `plan.inputs[*].destName`:

- is required
- is whitespace-strict
- must be unique (prevents silent overwrite under `/work/in/`)

### Slurm toolpacks

`plan.io.inputs[]` must match `inputsToLink[]` **exactly** (same `role` + `artifactId` set). Additional inputs are rejected.

Each Slurm `dest_relpath`:

- is whitespace-strict
- must be a safe relative path and start with `in/`
- must be unique (prevents silent overwrite)

## Output contract (stable)

Toolpacks must declare outputs up front via `declaredOutputs[]`.

- Docker: enforcement is based on what the tool links to `run_outputs`.
- Slurm: `plan.io.outputs[]` must match `declaredOutputs[]` **exactly** (role, type, label, `srcRelpath`), so `slurm_job_collect` is unambiguous.

The special role `log` is reserved and is produced automatically by `ToolRun`.

## Replay semantics (stable)

- **Docker-backed execution**: replay short-circuits only on a previously `succeeded` run with `result_json`.
- **Slurm-backed execution**: replay short-circuits on any existing run with `result_json` (including `queued`) to prevent resubmission.

## Non-goals

- No arbitrary shell command tools.
- No direct filesystem reads outside the run workspace materialization.
- No implicit network access (policy decides).
- No dynamic, mutable-state tools without explicit snapshot boundaries in canonical params.

