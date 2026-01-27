# Architecture (gateway skeleton)

## Control plane

- **MCP server** (stdio): exposes intent-level tools and returns `structuredContent` for machine-readability.
- **Policy engine**: YAML allowlists + quotas gate each tool call before execution.
- **Deterministic runs**: `run_id` is derived from tool contract version + canonical params hash + policy hash; reruns replay stored results.
- **Provenance**: every tool call is recorded as a Run with inputs/outputs + log artifact and a stored `result_json`.

## Data plane

- **Artifacts**: immutable, typed handles backed by a local object store (dev) and recorded in Postgres.
- **Param sets**: canonical params are stored once in `param_sets` and referenced by hash from `runs`.
- **Execution**: current tools are deterministic, in silico simulations; real executors (Slurm/K8s) should plug in behind the same Run/Artifact boundary.

## Key files

- Postgres schema: `db/schema.sql`
- Policy DSL: `policies/default.policy.yaml`
- Tool contracts (JSON Schema): `contracts/`
- MCP gateway implementation: `src/index.ts`, `src/mcp/gatewayServer.ts`
