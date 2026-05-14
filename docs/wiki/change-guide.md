---
page_id: change-guide
page_type: change-guide
generation_mode: inferred
freshness_status: new
updated_at: 2026-04-18T05:42:41.664Z
---

<details>
<summary>Build metadata</summary>

```json
{
  "freshnessKey": "b8813ec1682722e1e15c5741a63d0634b62fee52",
  "plannerReason": "Generated when deterministic critical-component, edit-surface, and validation evidence is strong enough to assemble a bounded change-oriented reader path.",
  "changedPaths": [
    "src/artifacts/artifactService.ts",
    "src/artifacts/localObjectStore.ts",
    "src/bundle/bundleExport.ts",
    "src/bundle/bundleTar.ts",
    "tests/artifactService.test.ts",
    "tests/bundleExport.test.ts",
    "tests/contracts.test.ts",
    "tests/gateway.integration.test.ts",
    "docs/architecture.md",
    "docs/bundle_export.md",
    "docs/slurm_cluster_smoke.md",
    "README.md",
    "package.json"
  ],
  "dependencyPaths": [
    "src/artifacts/artifactService.ts",
    "src/artifacts/localObjectStore.ts",
    "src/bundle/bundleExport.ts",
    "src/bundle/bundleTar.ts",
    "tests/artifactService.test.ts",
    "tests/bundleExport.test.ts",
    "tests/contracts.test.ts",
    "tests/gateway.integration.test.ts",
    "docs/architecture.md",
    "docs/bundle_export.md",
    "docs/slurm_cluster_smoke.md",
    "README.md",
    "package.json"
  ],
  "dependencyEvidenceIds": [
    "ingest:file:src/artifacts/artifactService.ts",
    "ingest:file:src/artifacts/localObjectStore.ts",
    "ingest:file:src/bundle/bundleExport.ts",
    "ingest:file:src/bundle/bundleTar.ts",
    "ingest:file:src/bundle/bundleVerify.ts",
    "ingest:file:src/bundle/manifest.ts",
    "ingest:file:src/core/artifact.ts",
    "ingest:file:src/core/canonicalJson.ts",
    "ingest:file:src/core/detectArtifactType.ts",
    "ingest:file:src/core/ids.ts",
    "ingest:file:tests/artifactService.test.ts",
    "ingest:file:tests/bundleExport.test.ts",
    "ingest:file:tests/contracts.test.ts",
    "ingest:file:tests/gateway.integration.test.ts",
    "ingest:file:tests/policy.test.ts",
    "ingest:file:tests/runLifecycle.test.ts",
    "ingest:file:tests/slurm.integration.test.ts",
    "ingest:file:tests/toolpacks.test.ts",
    "ingest:file:docs/architecture.md",
    "ingest:file:docs/bundle_export.md",
    "ingest:file:docs/slurm_cluster_smoke.md",
    "ingest:file:README.md",
    "workflow:package.json"
  ],
  "evidenceIds": [
    "ingest:file:src/artifacts/artifactService.ts",
    "ingest:file:src/artifacts/localObjectStore.ts",
    "ingest:file:src/bundle/bundleExport.ts",
    "ingest:file:src/bundle/bundleTar.ts",
    "ingest:file:src/bundle/bundleVerify.ts",
    "ingest:file:src/bundle/manifest.ts",
    "ingest:file:src/core/artifact.ts",
    "ingest:file:src/core/canonicalJson.ts",
    "ingest:file:src/core/detectArtifactType.ts",
    "ingest:file:src/core/ids.ts",
    "ingest:file:tests/artifactService.test.ts",
    "ingest:file:tests/bundleExport.test.ts",
    "ingest:file:tests/contracts.test.ts",
    "ingest:file:tests/gateway.integration.test.ts",
    "ingest:file:tests/policy.test.ts",
    "ingest:file:tests/runLifecycle.test.ts",
    "ingest:file:tests/slurm.integration.test.ts",
    "ingest:file:tests/toolpacks.test.ts",
    "ingest:file:docs/architecture.md",
    "ingest:file:docs/bundle_export.md",
    "ingest:file:docs/slurm_cluster_smoke.md",
    "ingest:file:README.md",
    "workflow:package.json"
  ],
  "qualityWarnings": []
}

```
</details>

# Change Guide

Task-first guide for making bounded changes in HelixMCP.

## Related Pages

- [components](components.md)
- [validation](validation.md)
- [playbook](playbook.md)
- [workflows](workflows.md)

## Change Priorities

1. `src`: Hotspot score 2619 with 446 inbound and 426 outbound inferred edges. Touches 26 inferred dependency edges.
2. `Tests`: Hotspot score 336 with 21 inbound and 90 outbound inferred edges. Touches 256 inferred dependency edges.
3. `Documentation`: Touches 230 inferred dependency edges.

<details>
<summary>Related files:</summary>

- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`
- `src/bundle/bundleExport.ts`
- `src/bundle/bundleTar.ts`
- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
- `tests/contracts.test.ts`
- `tests/gateway.integration.test.ts`
- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
- `README.md`
- `package.json`
</details>

<details>
<summary>Citations:</summary>

- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `src/bundle/bundleExport.ts:203`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `tests/contracts.test.ts:30`
- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
- `package.json`
</details>

## Where to Start Editing

- Start with `src` via `src/artifacts/artifactService.ts`, `src/artifacts/localObjectStore.ts`. Then read [src](components/src.md) for the bounded component guide.
- Start with `Tests` via `tests/artifactService.test.ts`, `tests/bundleExport.test.ts`. Then read [Tests](components/tests.md) for the bounded component guide.
- Start with `Documentation` via `docs/architecture.md`, `docs/bundle_export.md`. Then read [Documentation](components/docs.md) for the bounded component guide.

<details>
<summary>Related files:</summary>

- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`
- `src/bundle/bundleExport.ts`
- `src/bundle/bundleTar.ts`
- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
- `tests/contracts.test.ts`
- `tests/gateway.integration.test.ts`
- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
- `README.md`
- `package.json`
</details>

<details>
<summary>Citations:</summary>

- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `src/bundle/bundleExport.ts:203`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `tests/contracts.test.ts:30`
- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
- `package.json`
</details>

## Validation Order

1. Fast feedback: run `pnpm build` (build) from `.`.
1. Fast feedback: run `pnpm typecheck` (typecheck) from `.`.
2. Behavioral verification: run `pnpm bundle:verify` (bundle:verify) from `.`.
2. Behavioral verification: run `pnpm test` (test) from `.`.
3. Release-safety validation: run `pnpm build` (build) from `.`.

<details>
<summary>Related files:</summary>

- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`
- `src/bundle/bundleExport.ts`
- `src/bundle/bundleTar.ts`
- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
- `tests/contracts.test.ts`
- `tests/gateway.integration.test.ts`
- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
- `README.md`
- `package.json`
</details>

<details>
<summary>Citations:</summary>

- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `src/bundle/bundleExport.ts:203`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `tests/contracts.test.ts:30`
- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
- `package.json`
</details>

## Common Change Paths

### 1. Modify validation flow for `src`

Start here:
- Open `src/artifacts/artifactService.ts` first; it is the strongest workflow or owning file tied to the current validation path.
- Then cross-check [validation](validation.md) and [src](components/src.md) before changing the command order or scope.

Likely files:
- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`
- `package.json`

Risk boundary:
- Validation changes cover hotspot-heavy behavior for `src`, which currently carries score 2619.
- A weaker validation path can miss regressions that ripple into `src`, `Tests`, `Documentation`.
- Release-safety checks are part of the current confidence boundary, so removing or weakening them can raise publish or deploy risk.

Validate with:
- Run `pnpm build` (build) from `.`.
- Run `pnpm bundle:verify` (bundle:verify) from `.`.

<details>
<summary>Supporting citations:</summary>

- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `package.json`
</details>

<details>
<summary>Related files:</summary>

- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`
- `package.json`
</details>

<details>
<summary>Citations:</summary>

- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `package.json`
</details>

## Citations

<details>
<summary>Citations:</summary>

- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `src/bundle/bundleExport.ts:203`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `tests/contracts.test.ts:30`
- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
- `package.json`
</details>
