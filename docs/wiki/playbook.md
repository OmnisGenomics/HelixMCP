---
page_id: playbook
page_type: playbook
generation_mode: inferred
freshness_status: new
updated_at: 2026-04-18T05:42:41.574Z
---

<details>
<summary>Build metadata</summary>

```json
{
  "freshnessKey": "eda73ac7776acb5141f1766d92f04a5593e29ba8",
  "plannerReason": "Generated when enough workflow, runtime, and hotspot evidence exists to assemble an operational guide.",
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

# Playbook

Operational guide for validating and debugging HelixMCP.

## Related Pages

- [workflows](workflows.md)
- [testing](testing.md)
- [runtime](runtime.md)
- [components](components.md)

## Validation Order

1. Run `pnpm build` (build) from `.`.
2. Run `pnpm test` (test) from `.`.
3. Run `pnpm test:watch` (test:watch) from `.`.
4. Run `pnpm typecheck` (typecheck) from `.`.

<details>
<summary>Related files:</summary>

- `package.json`
</details>

<details>
<summary>Citations:</summary>

- `package.json`
</details>

## Debugging Entrypoints

- Start from workflow `pnpm dev` (dev).
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

## Change-Safety Notes

- src: score 2619; validate around inbound 446, outbound 426, and 1 bridged subsystem boundary.
- Tests: score 336; validate around inbound 21, outbound 90, and 1 bridged subsystem boundary.
- @modelcontextprotocol/sdk: score 102; validate around inbound 17, outbound 16, and 1 bridged subsystem boundary.

<details>
<summary>Related files:</summary>

- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`
- `src/bundle/bundleExport.ts`
- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
- `tests/contracts.test.ts`
- `package.json`
</details>

<details>
<summary>Citations:</summary>

- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `package.json`
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
