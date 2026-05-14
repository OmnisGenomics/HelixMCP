---
page_id: start-here
page_type: start-here
generation_mode: inferred
freshness_status: new
updated_at: 2026-04-18T05:42:41.666Z
---

<details>
<summary>Build metadata</summary>

```json
{
  "freshnessKey": "b08048f8e3afa01e8de2ac9fdbe229ea817aa389",
  "plannerReason": "Generated for service-like repositories when deterministic runtime/setup evidence is strong enough to separate startup guidance from validation guidance.",
  "changedPaths": [
    "package.json",
    "pnpm dev",
    "src/index.ts",
    "src/artifacts/artifactService.ts",
    "src/artifacts/localObjectStore.ts",
    "src/bundle/bundleExport.ts",
    "tests/artifactService.test.ts",
    "tests/bundleExport.test.ts",
    "tests/contracts.test.ts"
  ],
  "dependencyPaths": [
    "package.json",
    "pnpm dev",
    "src/index.ts",
    "src/artifacts/artifactService.ts",
    "src/artifacts/localObjectStore.ts",
    "src/bundle/bundleExport.ts",
    "tests/artifactService.test.ts",
    "tests/bundleExport.test.ts",
    "tests/contracts.test.ts"
  ],
  "dependencyEvidenceIds": [
    "workflow:package.json",
    "component:package.json",
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
    "ingest:file:tests/toolpacks.test.ts"
  ],
  "evidenceIds": [
    "workflow:package.json",
    "component:package.json",
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
    "ingest:file:tests/toolpacks.test.ts"
  ],
  "qualityWarnings": []
}

```
</details>

# Start Here

Startup-oriented guide for getting HelixMCP running the first time.

## Related Pages

- [runtime](runtime.md)
- [playbook](playbook.md)
- [workflows](workflows.md)
- [components](components.md)

## Startup Prerequisites

- Use package manager `pnpm`.
- Run from `.` before starting the main runtime path.
- Check `package.json` for setup prerequisites.

<details>
<summary>Related files:</summary>

- `package.json`
- `pnpm dev`
- `src/index.ts`
- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`
- `src/bundle/bundleExport.ts`
- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
- `tests/contracts.test.ts`
</details>

<details>
<summary>Citations:</summary>

- `package.json`
- `pnpm dev`
- `src/index.ts:18`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

## Primary Run Path

1. Start with `pnpm dev` (dev) from `.`.
2. Enter through `pnpm dev`, `src/index.ts`.
3. Hand off to `helixmcp-biomcp-fabric` as the primary runtime owner.

<details>
<summary>Related files:</summary>

- `package.json`
- `pnpm dev`
- `src/index.ts`
- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`
- `src/bundle/bundleExport.ts`
- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
- `tests/contracts.test.ts`
</details>

<details>
<summary>Citations:</summary>

- `package.json`
- `pnpm dev`
- `src/index.ts:18`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

## First Debugging Entrypoints

- Inspect helixmcp-biomcp-fabric at `.` via `src/index.ts`.
- Inspect src at `src`.
- Inspect Tests at `tests`.
- Re-run `pnpm dev` to reproduce the startup path quickly.

<details>
<summary>Related files:</summary>

- `package.json`
- `pnpm dev`
- `src/index.ts`
- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`
- `src/bundle/bundleExport.ts`
- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
- `tests/contracts.test.ts`
</details>

<details>
<summary>Citations:</summary>

- `package.json`
- `pnpm dev`
- `src/index.ts:18`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

## Citations

<details>
<summary>Citations:</summary>

- `package.json`
- `pnpm dev`
- `src/index.ts:18`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>
