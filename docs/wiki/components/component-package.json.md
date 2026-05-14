---
page_id: component-component:package.json
page_type: component
generation_mode: inferred
freshness_status: new
updated_at: 2026-04-18T05:41:58.006Z
---

<details>
<summary>Build metadata</summary>

```json
{
  "freshnessKey": "39bfb4c9b8729a6ced51555d99c7d88427e75eac",
  "plannerReason": "Generated because the component was ranked as significant for repo navigation.",
  "changedPaths": [
    "src/index.ts",
    "package.json",
    "src/artifacts/artifactService.ts",
    "src/artifacts/localObjectStore.ts",
    "src/bundle/bundleExport.ts",
    "src/bundle/bundleTar.ts",
    "src/bundle/bundleVerify.ts",
    "src/bundle/manifest.ts",
    "src/core/artifact.ts"
  ],
  "dependencyPaths": [
    "src/index.ts",
    "package.json",
    "src/artifacts/artifactService.ts",
    "src/artifacts/localObjectStore.ts",
    "src/bundle/bundleExport.ts",
    "src/bundle/bundleTar.ts",
    "src/bundle/bundleVerify.ts",
    "src/bundle/manifest.ts",
    "src/core/artifact.ts"
  ],
  "dependencyEvidenceIds": [
    "component:package.json",
    "ingest:file:docs/architecture.md",
    "ingest:file:docs/bundle_export.md",
    "ingest:file:docs/slurm_cluster_smoke.md",
    "ingest:file:README.md",
    "ingest:file:tests/artifactService.test.ts",
    "ingest:file:tests/bundleExport.test.ts",
    "ingest:file:tests/contracts.test.ts",
    "ingest:file:tests/gateway.integration.test.ts",
    "ingest:file:tests/policy.test.ts",
    "ingest:file:tests/runLifecycle.test.ts",
    "ingest:file:tests/slurm.integration.test.ts",
    "ingest:file:tests/toolpacks.test.ts",
    "workflow:package.json"
  ],
  "evidenceIds": [
    "component:package.json",
    "ingest:file:docs/architecture.md",
    "ingest:file:docs/bundle_export.md",
    "ingest:file:docs/slurm_cluster_smoke.md",
    "ingest:file:README.md",
    "ingest:file:tests/artifactService.test.ts",
    "ingest:file:tests/bundleExport.test.ts",
    "ingest:file:tests/contracts.test.ts",
    "ingest:file:tests/gateway.integration.test.ts",
    "ingest:file:tests/policy.test.ts",
    "ingest:file:tests/runLifecycle.test.ts",
    "ingest:file:tests/slurm.integration.test.ts",
    "ingest:file:tests/toolpacks.test.ts",
    "workflow:package.json"
  ],
  "qualityWarnings": []
}

```
</details>

# helixmcp-biomcp-fabric

helixmcp-biomcp-fabric node component

## Related Pages

- [components](components.md)
- [workflows](workflows.md)
- [interfaces](interfaces.md)
- [dependencies](dependencies.md)

## Implementation Roles

### `src/index.ts`
Role classification: inferred execution boundary.
Proved signals:
- Matched an inferred entrypoint or entrypoint symbol in `src/index.ts`.
Why this role fits: These proved signals suggest this unit is a first-hop execution boundary that receives control and hands it into component logic.
Supporting implementation citations:
- `src/index.ts:18`

<details>
<summary>Related files:</summary>

- `src/index.ts`
</details>

<details>
<summary>Citations:</summary>

- `src/index.ts:18`
</details>

## Module Responsibilities

Insufficient evidence to infer module responsibilities confidently.

<details>
<summary>Supporting citations:</summary>

- none
</details>


## Key Symbols

Insufficient evidence to infer key symbol behavior confidently.

<details>
<summary>Supporting citations:</summary>

- none
</details>


## State Boundaries

Insufficient evidence to infer state boundaries confidently.

<details>
<summary>Supporting citations:</summary>

- none
</details>


## State Ownership and Handoffs

Insufficient evidence to infer state ownership and handoffs confidently.

<details>
<summary>Supporting citations:</summary>

- none
</details>


## Request Lifecycle

Insufficient evidence to infer a bounded request lifecycle confidently.

<details>
<summary>Supporting citations:</summary>

- none
</details>


## Responsibilities

helixmcp-biomcp-fabric node component

Type: application
Root path: `.`
Ecosystem: node

<details>
<summary>Related files:</summary>

- `package.json`
- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`
- `src/bundle/bundleExport.ts`
- `src/bundle/bundleTar.ts`
- `src/bundle/bundleVerify.ts`
- `src/bundle/manifest.ts`
- `src/core/artifact.ts`
</details>

<details>
<summary>Citations:</summary>

- `package.json`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `src/bundle/bundleExport.ts:203`
</details>

## Entrypoints and Runtime Surface

- `src/index.ts`

<details>
<summary>Related files:</summary>

- `src/index.ts`
</details>

<details>
<summary>Citations:</summary>

- `src/index.ts:18`
</details>

## Interfaces and Config

- none

## Dependencies and Relationships

- `component:docs` documents `component:package.json` (medium)
- `component:package.json` depends_on `component:external:node:@modelcontextprotocol/sdk` (high)
- `component:package.json` depends_on `component:external:node:@types/node` (high)
- `component:package.json` depends_on `component:external:node:@types/pg` (high)
- `component:package.json` depends_on `component:external:node:ajv` (high)
- `component:package.json` depends_on `component:external:node:kysely` (high)
- `component:package.json` depends_on `component:external:node:pg` (high)
- `component:package.json` depends_on `component:external:node:pg-mem` (high)
- `component:package.json` depends_on `component:external:node:tsx` (high)
- `component:package.json` depends_on `component:external:node:typescript` (high)
- `component:package.json` depends_on `component:external:node:ulid` (high)
- `component:package.json` depends_on `component:external:node:vitest` (high)
- `component:package.json` depends_on `component:external:node:yaml` (high)
- `component:package.json` depends_on `component:external:node:zod` (high)
- `component:tests` tests `component:package.json` (high)

<details>
<summary>Related files:</summary>

- `package.json`
- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`
- `src/bundle/bundleExport.ts`
- `src/bundle/bundleTar.ts`
- `src/bundle/bundleVerify.ts`
- `src/bundle/manifest.ts`
- `src/core/artifact.ts`
</details>

<details>
<summary>Citations:</summary>

- `package.json`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `src/bundle/bundleExport.ts:203`
</details>

## Why This Hotspot Matters

Insufficient evidence to explain this component as a hotspot confidently.

<details>
<summary>Supporting citations:</summary>

- none
</details>


## Operational Risk Surface

Insufficient evidence to infer operational risk surface confidently.

<details>
<summary>Supporting citations:</summary>

- none
</details>


## Invariants and Failure Handling

Insufficient evidence to infer invariants and failure handling confidently.

<details>
<summary>Supporting citations:</summary>

- none
</details>


## Where to Edit

Likely change entry files:
- `src/index.ts`
- `package.json`
- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`

Owned interfaces:
- none

Nearby verification surfaces:
- Run `pnpm build` (build) from `.`.
- Run `pnpm bundle:export` (bundle:export) from `.`.
- Run `pnpm bundle:verify` (bundle:verify) from `.`.
- Run `pnpm dev` (dev) from `.`.

<details>
<summary>Supporting citations:</summary>

- `src/index.ts:18`
- `package.json`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
</details>

<details>
<summary>Related files:</summary>

- `src/index.ts`
- `package.json`
- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`
</details>

<details>
<summary>Citations:</summary>

- `src/index.ts:18`
- `package.json`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
</details>

## Change Planning

Impacted areas:
- Downstream components likely affected: `@modelcontextprotocol/sdk`, `@types/node`, `@types/pg`, `ajv`.

Suggested verification steps:
- Run `pnpm build` (build) from `.`.
- Run `pnpm bundle:export` (bundle:export) from `.`.
- Run `pnpm bundle:verify` (bundle:verify) from `.`.
- Run `pnpm dev` (dev) from `.`.

<details>
<summary>Supporting citations:</summary>

- `package.json`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
</details>

<details>
<summary>Related files:</summary>

- `package.json`
- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`
- `src/bundle/bundleExport.ts`
</details>

<details>
<summary>Citations:</summary>

- `package.json`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
</details>

## Nearby Workflows

- `pnpm build` (build)
- `pnpm bundle:export` (bundle:export)
- `pnpm bundle:verify` (bundle:verify)
- `pnpm dev` (dev)
- `pnpm test` (test)
- `pnpm test:watch` (test:watch)
- `pnpm typecheck` (typecheck)

<details>
<summary>Related files:</summary>

- `package.json`
</details>

<details>
<summary>Citations:</summary>

- `package.json`
</details>

## Citations

<details>
<summary>Citations:</summary>

- `src/index.ts:18`
- `package.json`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `src/bundle/bundleExport.ts:203`
</details>
