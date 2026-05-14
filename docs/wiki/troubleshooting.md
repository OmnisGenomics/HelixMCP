---
page_id: troubleshooting
page_type: troubleshooting
generation_mode: inferred
freshness_status: new
updated_at: 2026-04-18T05:42:41.665Z
---

<details>
<summary>Build metadata</summary>

```json
{
  "freshnessKey": "336ca17f3333e9d138a36eecd0450d467c6df960",
  "plannerReason": "Generated when enough deterministic runtime, hotspot, and validation evidence exists to assemble a bounded troubleshooting guide.",
  "changedPaths": [
    "package.json",
    "src/artifacts/artifactService.ts",
    "src/artifacts/localObjectStore.ts",
    "src/bundle/bundleExport.ts",
    "tests/artifactService.test.ts",
    "tests/bundleExport.test.ts",
    "tests/contracts.test.ts"
  ],
  "dependencyPaths": [
    "package.json",
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

# Troubleshooting

Failure-first inspection guide for HelixMCP.

## Related Pages

- [playbook](playbook.md)
- [runtime](runtime.md)
- [components](components.md)
- [testing](testing.md)

## First Inspection Points

- Reproduce the failure through `pnpm dev` (dev) from `.`.
- Inspect helixmcp-biomcp-fabric at `.` via `src/index.ts`.
- Inspect src at `src`.
- Inspect Tests at `tests`.
- Inspect @modelcontextprotocol/sdk at `external/node/@modelcontextprotocol/sdk`.

<details>
<summary>Related files:</summary>

- `package.json`
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
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

## Likely Failure Boundaries

- src: score 2619; watch inbound 446, outbound 426, and 1 bridged subsystem boundary from `src`.
- Tests: score 336; watch inbound 21, outbound 90, and 1 bridged subsystem boundary from `tests`.
- @modelcontextprotocol/sdk: score 102; watch inbound 17, outbound 16, and 1 bridged subsystem boundary from `external/node/@modelcontextprotocol/sdk`.
- vitest: score 90; watch inbound 9, outbound 20, and 1 bridged subsystem boundary from `external/node/vitest`.

<details>
<summary>Related files:</summary>

- `package.json`
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
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

## Relevant Validation Commands

- Run `pnpm build` (build) from `.` after reproducing or patching the issue.
- Run `pnpm test` (test) from `.` after reproducing or patching the issue.
- Run `pnpm test:watch` (test:watch) from `.` after reproducing or patching the issue.
- Run `pnpm typecheck` (typecheck) from `.` after reproducing or patching the issue.
- If needed, re-run `pnpm dev` to verify the runtime path after the fix.

<details>
<summary>Related files:</summary>

- `package.json`
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
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

## Citations

<details>
<summary>Citations:</summary>

- `package.json`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>
