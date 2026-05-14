---
page_id: index
page_type: index
generation_mode: inferred
freshness_status: new
updated_at: 2026-04-18T05:41:58.193Z
---

<details>
<summary>Build metadata</summary>

```json
{
  "freshnessKey": "1ec577d0ba3be8f58c14895903efe0e247bdacec",
  "plannerReason": "Service template selected because deterministic evidence suggests a runnable application or service surface. The generic runtime page is suppressed because start-here now covers startup orientation more directly, while playbook keeps validation guidance separate. The generic components navigation section is demoted to an appendix because change-guide plus component pages provide the stronger explanation-first edit path for this service-shaped repository.",
  "changedPaths": [
    "docs/architecture.md",
    "docs/bundle_export.md",
    "docs/slurm_cluster_smoke.md",
    "README.md",
    "package.json",
    "src/artifacts/artifactService.ts",
    "src/artifacts/localObjectStore.ts",
    "src/bundle/bundleExport.ts",
    "src/bundle/bundleTar.ts",
    "tests/artifactService.test.ts",
    "tests/bundleExport.test.ts",
    "tests/contracts.test.ts",
    "tests/gateway.integration.test.ts"
  ],
  "dependencyPaths": [
    "docs/architecture.md",
    "docs/bundle_export.md",
    "docs/slurm_cluster_smoke.md",
    "README.md",
    "package.json",
    "src/artifacts/artifactService.ts",
    "src/artifacts/localObjectStore.ts",
    "src/bundle/bundleExport.ts",
    "src/bundle/bundleTar.ts",
    "tests/artifactService.test.ts",
    "tests/bundleExport.test.ts",
    "tests/contracts.test.ts",
    "tests/gateway.integration.test.ts"
  ],
  "dependencyEvidenceIds": [
    "component:external:node:@esbuild/aix-ppc64",
    "component:external:node:@esbuild/android-arm",
    "component:src",
    "component:tests",
    "component:docs",
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
    "component:package.json"
  ],
  "evidenceIds": [
    "component:external:node:@esbuild/aix-ppc64",
    "component:external:node:@esbuild/android-arm",
    "component:src",
    "component:tests",
    "component:docs",
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
    "component:package.json"
  ],
  "qualityWarnings": []
}

```
</details>

# HelixMCP Wiki

Purpose: HelixMCP is a repository indexed by RepoIntel MCP.

Documentation starts at docs/architecture.md, docs/bundle_export.md, docs/slurm_cluster_smoke.md.

Context:
- Version control: git
- Detected ecosystems: node
- Inventory: 111 files, 231 components, 7 workflows

## Related Pages

- [architecture](architecture.md)
- [start-here](start-here.md)
- [configuration](configuration.md)
- [playbook](playbook.md)
- [validation](validation.md)
- [change-guide](change-guide.md)
- [troubleshooting](troubleshooting.md)
- [components](components.md)
- [interfaces](interfaces.md)
- [dependencies](dependencies.md)
- [workflows](workflows.md)
- [testing](testing.md)
- [diagrams](diagrams.md)
- [glossary](glossary.md)

## Repository Overview

Purpose: HelixMCP is a repository indexed by RepoIntel MCP.

Documentation starts at docs/architecture.md, docs/bundle_export.md, docs/slurm_cluster_smoke.md.

Context:
- Version control: git
- Detected ecosystems: node
- Inventory: 111 files, 231 components, 7 workflows

Primary capabilities:
- build: `pnpm build`
- bundle:export: `pnpm bundle:export`
- bundle:verify: `pnpm bundle:verify`
- dev: `pnpm dev`

Major subsystem map:
- external: external groups 228 components using path structure plus graph-connected merges.
- src: src groups 2 components using path structure plus graph-connected merges.
- docs: docs groups 1 components under docs/ or related paths.

Suggested reading order:
1. Read [architecture](architecture.md) next for the subsystem view.
2. Read [start-here](start-here.md) next for the startup prerequisites, first run path, and initial debugging entrypoints.
3. Read [configuration](configuration.md) next for the required setup, tunable knobs, and risk-sensitive settings.
4. Read [playbook](playbook.md) next for the operational validation and debugging guide.
5. Read [validation](validation.md) next for the validation layers and the confidence they provide.
6. Read [change-guide](change-guide.md) next for the task-first change priorities, edit entrypoints, and verification order.
7. Read [troubleshooting](troubleshooting.md) next for the failure-first inspection points and validation commands.
8. Read [components](components.md) next for the important component inventory.
9. Read [diagrams](diagrams.md) next for the diagrams details.
10. Read [dependencies](dependencies.md) next for the dependencies details.
11. Read [workflows](workflows.md) next for the workflows details.

<details>
<summary>Related files:</summary>

- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
- `README.md`
- `package.json`
- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`
</details>

<details>
<summary>Citations:</summary>

- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
- `README.md`
- `package.json`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
</details>

## Critical Components

### 1. src
Why it matters: Hotspot score 2619 with 446 inbound and 426 outbound inferred edges. Touches 26 inferred dependency edges.

What it owns:
- Source module rooted at src.
- Owns files rooted at `src`.

<details>
<summary>Supporting citations:</summary>

- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `src/bundle/bundleExport.ts:203`
</details>

Ranking score: 2645.

### 2. Tests
Why it matters: Hotspot score 336 with 21 inbound and 90 outbound inferred edges. Touches 256 inferred dependency edges.

What it owns:
- Repository tests and fixtures.
- Owns files rooted at `tests`.

<details>
<summary>Supporting citations:</summary>

- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `tests/contracts.test.ts:30`
</details>

Ranking score: 592.

### 3. Documentation
Why it matters: Touches 230 inferred dependency edges.

What it owns:
- Repository documentation and wiki source files.
- Owns files rooted at `docs`.

<details>
<summary>Supporting citations:</summary>

- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
</details>

Ranking score: 230.

### 4. @modelcontextprotocol/sdk
Why it matters: Hotspot score 102 with 17 inbound and 16 outbound inferred edges. Shows up in 7 inferred workflows. Touches 36 inferred dependency edges.

What it owns:
- External node dependency inferred from package.json.
- Owns files rooted at `external/node/@modelcontextprotocol/sdk`.

<details>
<summary>Supporting citations:</summary>

- `package.json`
</details>

Ranking score: 152.

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

## Repository Summary

Purpose: HelixMCP is a repository indexed by RepoIntel MCP.

Documentation starts at docs/architecture.md, docs/bundle_export.md, docs/slurm_cluster_smoke.md.

Context:
- Version control: git
- Detected ecosystems: node
- Inventory: 111 files, 231 components, 7 workflows

Indexed revision: git:6f650d7aaeed36c066dd7e1543a16c388b8be729.
Indexed at: 2026-04-18T05:41:57.865Z.

<details>
<summary>Related files:</summary>

- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
- `README.md`
</details>

<details>
<summary>Citations:</summary>

- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
- `README.md`
</details>

## Repository Shape

- `.github/`
- `contracts/`
- `db/`
- `docs/`
- `policies/`
- `scripts/`
- `src/`
- `tests/`

Languages:
- json
- markdown
- typescript
- yaml

<details>
<summary>Related files:</summary>

- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
- `README.md`
</details>

<details>
<summary>Citations:</summary>

- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
- `README.md`
</details>

## Important Components

- helixmcp-biomcp-fabric (application) at `.`: helixmcp-biomcp-fabric node component
- src (module) at `src`: Source module rooted at src.
- Tests (tests) at `tests`: Repository tests and fixtures.
- Documentation (docs) at `docs`: Repository documentation and wiki source files.
- @modelcontextprotocol/sdk (package) at `external/node/@modelcontextprotocol/sdk`: External node dependency inferred from package.json.
- @types/node (package) at `external/node/@types/node`: External node dependency inferred from package.json.
- @types/pg (package) at `external/node/@types/pg`: External node dependency inferred from package.json.
- ajv (package) at `external/node/ajv`: External node dependency inferred from package.json.
- kysely (package) at `external/node/kysely`: External node dependency inferred from package.json.
- pg (package) at `external/node/pg`: External node dependency inferred from package.json.
- pg-mem (package) at `external/node/pg-mem`: External node dependency inferred from package.json.
- tsx (package) at `external/node/tsx`: External node dependency inferred from package.json.

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

## Citations

<details>
<summary>Citations:</summary>

- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
- `README.md`
- `package.json`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `src/bundle/bundleExport.ts:203`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `tests/contracts.test.ts:30`
</details>
