---
page_id: components
page_type: components
generation_mode: inferred
freshness_status: new
updated_at: 2026-04-18T05:42:41.554Z
---

<details>
<summary>Build metadata</summary>

```json
{
  "freshnessKey": "48bca8eb0f5de9a227a669e83fda26c56e1763cc",
  "plannerReason": "Service template selected because deterministic evidence suggests a runnable application or service surface. The generic runtime page is suppressed because start-here now covers startup orientation more directly, while playbook keeps validation guidance separate. The generic components navigation section is demoted to an appendix because change-guide plus component pages provide the stronger explanation-first edit path for this service-shaped repository. The generic components navigation section is demoted to an appendix because change-guide and component pages now provide the stronger explanation-first path for this repo shape.",
  "changedPaths": [
    "package.json",
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
    "README.md"
  ],
  "dependencyPaths": [
    "package.json",
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
    "README.md"
  ],
  "dependencyEvidenceIds": [
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
    "ingest:file:tests/toolpacks.test.ts",
    "ingest:file:docs/architecture.md",
    "ingest:file:docs/bundle_export.md",
    "ingest:file:docs/slurm_cluster_smoke.md",
    "ingest:file:README.md"
  ],
  "evidenceIds": [
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
    "ingest:file:tests/toolpacks.test.ts",
    "ingest:file:docs/architecture.md",
    "ingest:file:docs/bundle_export.md",
    "ingest:file:docs/slurm_cluster_smoke.md",
    "ingest:file:README.md"
  ],
  "qualityWarnings": []
}

```
</details>

# Components

Component inventory for HelixMCP.

## Related Pages

- [component-component:package.json](component-component:package.json.md)
- [component-component:src](component-component:src.md)
- [component-component:tests](component-component:tests.md)
- [component-component:docs](component-component:docs.md)
- [component-component:external:node:@modelcontextprotocol/sdk](component-component:external:node:@modelcontextprotocol/sdk.md)
- [component-component:external:node:@types/node](component-component:external:node:@types/node.md)
- [component-component:external:node:@types/pg](component-component:external:node:@types/pg.md)
- [component-component:external:node:ajv](component-component:external:node:ajv.md)
- [component-component:external:node:kysely](component-component:external:node:kysely.md)
- [component-component:external:node:pg](component-component:external:node:pg.md)
- [component-component:external:node:pg-mem](component-component:external:node:pg-mem.md)
- [component-component:external:node:tsx](component-component:external:node:tsx.md)

## Component Inventory

- helixmcp-biomcp-fabric (application) at `.` with 49 files.
- src (module) at `src` with 48 files.
- Tests (tests) at `tests` with 8 files.
- Documentation (docs) at `docs` with 4 files.
- @modelcontextprotocol/sdk (package) at `external/node/@modelcontextprotocol/sdk` with 1 files.
- @types/node (package) at `external/node/@types/node` with 1 files.
- @types/pg (package) at `external/node/@types/pg` with 1 files.
- ajv (package) at `external/node/ajv` with 1 files.
- kysely (package) at `external/node/kysely` with 1 files.
- pg (package) at `external/node/pg` with 1 files.
- pg-mem (package) at `external/node/pg-mem` with 1 files.
- tsx (package) at `external/node/tsx` with 1 files.

<details>
<summary>Related files:</summary>

- `package.json`
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
</details>

<details>
<summary>Citations:</summary>

- `package.json`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `docs/architecture.md`
- `docs/bundle_export.md`
</details>

## Component Navigation Appendix

- [component-component:package.json](components/component-package.json.md)
- [component-component:src](components/component-src.md)
- [component-component:tests](components/component-tests.md)
- [component-component:docs](components/component-docs.md)
- [component-component:external:node:@modelcontextprotocol/sdk](components/component-external-node-@modelcontextprotocol-sdk.md)
- [component-component:external:node:@types/node](components/component-external-node-@types-node.md)
- [component-component:external:node:@types/pg](components/component-external-node-@types-pg.md)
- [component-component:external:node:ajv](components/component-external-node-ajv.md)
- [component-component:external:node:kysely](components/component-external-node-kysely.md)
- [component-component:external:node:pg](components/component-external-node-pg.md)
- [component-component:external:node:pg-mem](components/component-external-node-pg-mem.md)
- [component-component:external:node:tsx](components/component-external-node-tsx.md)

## Citations

<details>
<summary>Citations:</summary>

- `package.json`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `docs/architecture.md`
- `docs/bundle_export.md`
</details>
