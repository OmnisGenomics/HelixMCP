---
page_id: dependencies
page_type: dependencies
generation_mode: inferred
freshness_status: new
updated_at: 2026-04-18T05:42:41.572Z
---

<details>
<summary>Build metadata</summary>

```json
{
  "freshnessKey": "6302c96f1c5915ef9adbab8cae2080c14e63d7a8",
  "plannerReason": "Service template selected because deterministic evidence suggests a runnable application or service surface. The generic runtime page is suppressed because start-here now covers startup orientation more directly, while playbook keeps validation guidance separate. The generic components navigation section is demoted to an appendix because change-guide plus component pages provide the stronger explanation-first edit path for this service-shaped repository.",
  "changedPaths": [
    "package.json",
    "package-lock.json",
    "src/artifacts/artifactService.ts",
    "src/artifacts/localObjectStore.ts",
    "tests/artifactService.test.ts",
    "tests/bundleExport.test.ts",
    "scripts/bundle_export.ts",
    "src/db/bootstrap.ts",
    "src/db/connection.ts",
    "src/index.ts",
    "tests/gateway.integration.test.ts",
    "tests/runLifecycle.test.ts",
    "tests/slurm.integration.test.ts",
    "src/mcp/toolSchemas.ts",
    "src/toolpacks/types.ts",
    "tests/toolpacks.test.ts",
    "docs/architecture.md",
    "docs/bundle_export.md"
  ],
  "dependencyPaths": [
    "package.json",
    "package-lock.json",
    "src/artifacts/artifactService.ts",
    "src/artifacts/localObjectStore.ts",
    "tests/artifactService.test.ts",
    "tests/bundleExport.test.ts",
    "scripts/bundle_export.ts",
    "src/db/bootstrap.ts",
    "src/db/connection.ts",
    "src/index.ts",
    "tests/gateway.integration.test.ts",
    "tests/runLifecycle.test.ts",
    "tests/slurm.integration.test.ts",
    "src/mcp/toolSchemas.ts",
    "src/toolpacks/types.ts",
    "tests/toolpacks.test.ts",
    "docs/architecture.md",
    "docs/bundle_export.md"
  ],
  "dependencyEvidenceIds": [
    "component:package.json",
    "ingest:file:scripts/bundle_export.ts",
    "ingest:file:src/db/bootstrap.ts",
    "ingest:file:src/db/connection.ts",
    "ingest:file:src/index.ts",
    "ingest:file:tests/artifactService.test.ts",
    "ingest:file:tests/bundleExport.test.ts",
    "ingest:file:tests/gateway.integration.test.ts",
    "ingest:file:tests/runLifecycle.test.ts",
    "ingest:file:tests/slurm.integration.test.ts",
    "ingest:file:tests/toolpacks.test.ts",
    "ingest:file:src/mcp/toolSchemas.ts",
    "ingest:file:src/toolpacks/types.ts",
    "ingest:file:tests/contracts.test.ts",
    "ingest:file:src/artifacts/localObjectStore.ts",
    "ingest:file:src/bundle/bundleExport.ts",
    "ingest:file:src/bundle/bundleTar.ts",
    "ingest:file:src/execution/backends/dockerRunner.ts",
    "ingest:file:scripts/bundle_verify.ts",
    "ingest:file:src/bundle/bundleVerify.ts",
    "ingest:file:src/core/ids.ts",
    "ingest:file:src/artifacts/artifactService.ts",
    "ingest:file:src/core/detectArtifactType.ts",
    "ingest:file:src/core/mimeType.ts",
    "ingest:file:src/store/postgresStore.ts",
    "ingest:file:src/execution/executionService.ts",
    "ingest:file:src/core/canonicalJson.ts",
    "ingest:file:src/policy/policy.ts",
    "ingest:file:src/execution/backends/localProcess.ts",
    "ingest:file:src/execution/deterministic.ts",
    "ingest:file:src/execution/inSilico.ts",
    "ingest:file:src/execution/slurm/scheduler.ts",
    "ingest:file:src/execution/slurm/slurmScriptV1.ts",
    "ingest:file:src/execution/slurm/submitter.ts",
    "ingest:file:src/execution/workspace.ts",
    "ingest:file:src/mcp/gatewayServer.ts",
    "ingest:file:src/mcp/envSnapshot.ts",
    "ingest:file:src/runs/runIdentity.ts",
    "ingest:file:src/runs/toolRun.ts",
    "ingest:file:src/toolpacks/register.ts",
    "ingest:file:src/toolpacks/builtin/seqkitStats.ts",
    "ingest:file:src/core/run.ts",
    "ingest:file:src/toolpacks/backendSelection.ts",
    "ingest:file:src/toolpacks/builtin/fastqc.ts",
    "ingest:file:src/toolpacks/docker/executeDocker.ts",
    "ingest:file:src/toolpacks/slurm/executeSlurm.ts",
    "ingest:file:src/toolpacks/builtin/multiqc.ts",
    "ingest:file:src/toolpacks/builtin/qcBundleFastq.ts",
    "ingest:file:src/toolpacks/builtin/samtoolsFlagstat.ts",
    "ingest:file:src/toolpacks/builtin/samtoolsFlagstatSlurm.ts",
    "ingest:file:docs/architecture.md",
    "ingest:file:docs/bundle_export.md",
    "ingest:file:docs/slurm_cluster_smoke.md",
    "ingest:file:README.md",
    "ingest:file:src/bundle/manifest.ts",
    "ingest:file:src/core/artifact.ts",
    "ingest:file:tests/policy.test.ts",
    "ingest:file:src/db/types.ts",
    "ingest:file:vitest.config.ts"
  ],
  "evidenceIds": [
    "component:package.json",
    "ingest:file:scripts/bundle_export.ts",
    "ingest:file:src/db/bootstrap.ts",
    "ingest:file:src/db/connection.ts",
    "ingest:file:src/index.ts",
    "ingest:file:tests/artifactService.test.ts",
    "ingest:file:tests/bundleExport.test.ts",
    "ingest:file:tests/gateway.integration.test.ts",
    "ingest:file:tests/runLifecycle.test.ts",
    "ingest:file:tests/slurm.integration.test.ts",
    "ingest:file:tests/toolpacks.test.ts",
    "ingest:file:src/mcp/toolSchemas.ts",
    "ingest:file:src/toolpacks/types.ts",
    "ingest:file:tests/contracts.test.ts",
    "ingest:file:src/artifacts/localObjectStore.ts",
    "ingest:file:src/bundle/bundleExport.ts",
    "ingest:file:src/bundle/bundleTar.ts",
    "ingest:file:src/execution/backends/dockerRunner.ts",
    "ingest:file:scripts/bundle_verify.ts",
    "ingest:file:src/bundle/bundleVerify.ts",
    "ingest:file:src/core/ids.ts",
    "ingest:file:src/artifacts/artifactService.ts",
    "ingest:file:src/core/detectArtifactType.ts",
    "ingest:file:src/core/mimeType.ts",
    "ingest:file:src/store/postgresStore.ts",
    "ingest:file:src/execution/executionService.ts",
    "ingest:file:src/core/canonicalJson.ts",
    "ingest:file:src/policy/policy.ts",
    "ingest:file:src/execution/backends/localProcess.ts",
    "ingest:file:src/execution/deterministic.ts",
    "ingest:file:src/execution/inSilico.ts",
    "ingest:file:src/execution/slurm/scheduler.ts",
    "ingest:file:src/execution/slurm/slurmScriptV1.ts",
    "ingest:file:src/execution/slurm/submitter.ts",
    "ingest:file:src/execution/workspace.ts",
    "ingest:file:src/mcp/gatewayServer.ts",
    "ingest:file:src/mcp/envSnapshot.ts",
    "ingest:file:src/runs/runIdentity.ts",
    "ingest:file:src/runs/toolRun.ts",
    "ingest:file:src/toolpacks/register.ts",
    "ingest:file:src/toolpacks/builtin/seqkitStats.ts",
    "ingest:file:src/core/run.ts",
    "ingest:file:src/toolpacks/backendSelection.ts",
    "ingest:file:src/toolpacks/builtin/fastqc.ts",
    "ingest:file:src/toolpacks/docker/executeDocker.ts",
    "ingest:file:src/toolpacks/slurm/executeSlurm.ts",
    "ingest:file:src/toolpacks/builtin/multiqc.ts",
    "ingest:file:src/toolpacks/builtin/qcBundleFastq.ts",
    "ingest:file:src/toolpacks/builtin/samtoolsFlagstat.ts",
    "ingest:file:src/toolpacks/builtin/samtoolsFlagstatSlurm.ts",
    "ingest:file:docs/architecture.md",
    "ingest:file:docs/bundle_export.md",
    "ingest:file:docs/slurm_cluster_smoke.md",
    "ingest:file:README.md",
    "ingest:file:src/bundle/manifest.ts",
    "ingest:file:src/core/artifact.ts",
    "ingest:file:tests/policy.test.ts",
    "ingest:file:src/db/types.ts",
    "ingest:file:vitest.config.ts"
  ],
  "qualityWarnings": []
}

```
</details>

# Dependencies

Dependency and relationship guide for HelixMCP.

## Related Pages

- [components](components.md)
- [diagrams](diagrams.md)

## Design-Shaping Dependencies

### pg
Architectural role: Data or messaging backbone

Repository touchpoints: Used by `helixmcp-biomcp-fabric`, `src`, `Tests` declared in `package.json` in runtime scope as standard dependencies and pinned in `package-lock.json`.

Why it matters: It anchors how repository-owned components persist state or exchange work.

<details>
<summary>Supporting citations:</summary>

- `package.json`
- `package-lock.json`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

### pg-mem
Architectural role: Data or messaging backbone

Repository touchpoints: Used by `helixmcp-biomcp-fabric`, `src`, `Tests` declared in `package.json` in development scope as standard dependencies and pinned in `package-lock.json`.

Why it matters: It anchors how repository-owned components persist state or exchange work.

<details>
<summary>Supporting citations:</summary>

- `package.json`
- `package-lock.json`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

### zod
Architectural role: Configuration or schema contract

Repository touchpoints: Used by `helixmcp-biomcp-fabric`, `src`, `Tests` declared in `package.json` in runtime scope as standard dependencies and pinned in `package-lock.json`.

Why it matters: It constrains configuration or payload shape across repository-owned components.

<details>
<summary>Supporting citations:</summary>

- `package.json`
- `package-lock.json`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

### ajv
Architectural role: Configuration or schema contract

Repository touchpoints: Used by `helixmcp-biomcp-fabric`, `Tests` declared in `package.json` in runtime scope as standard dependencies and pinned in `package-lock.json`.

Why it matters: It constrains configuration or payload shape across repository-owned components.

<details>
<summary>Supporting citations:</summary>

- `package.json`
- `package-lock.json`
- `src/artifacts/artifactService.ts:28`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

<details>
<summary>Related files:</summary>

- `package.json`
- `package-lock.json`
- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`
- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
</details>

<details>
<summary>Citations:</summary>

- `package.json`
- `package-lock.json`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

## Dependency Boundaries

### pg
Boundary role: Data or messaging backbone

Repository abstraction: Used directly across 2 repository components: `src`, `Tests`.

Replacement pressure: high

<details>
<summary>Supporting citations:</summary>

- `package.json`
- `package-lock.json`
- `scripts/bundle_export.ts:27`
- `src/db/bootstrap.ts:4`
- `src/db/connection.ts:9`
- `src/index.ts:18`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

### pg-mem
Boundary role: Data or messaging backbone

Repository abstraction: Used directly across 2 repository components: `src`, `Tests`.

Replacement pressure: high

<details>
<summary>Supporting citations:</summary>

- `package.json`
- `package-lock.json`
- `src/index.ts:18`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `tests/gateway.integration.test.ts:146`
- `tests/runLifecycle.test.ts:23`
- `tests/slurm.integration.test.ts:48`
</details>

### zod
Boundary role: Configuration or schema contract

Repository abstraction: Used directly across 2 repository components: `src`, `Tests`.

Replacement pressure: high

<details>
<summary>Supporting citations:</summary>

- `package.json`
- `package-lock.json`
- `src/mcp/toolSchemas.ts:3`
- `src/toolpacks/types.ts:38`
- `tests/toolpacks.test.ts:183`
</details>

<details>
<summary>Related files:</summary>

- `package.json`
- `package-lock.json`
- `scripts/bundle_export.ts`
- `src/db/bootstrap.ts`
- `src/db/connection.ts`
- `src/index.ts`
- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
- `tests/gateway.integration.test.ts`
- `tests/runLifecycle.test.ts`
- `tests/slurm.integration.test.ts`
- `src/mcp/toolSchemas.ts`
- `src/toolpacks/types.ts`
- `tests/toolpacks.test.ts`
</details>

<details>
<summary>Citations:</summary>

- `package.json`
- `package-lock.json`
- `scripts/bundle_export.ts:27`
- `src/db/bootstrap.ts:4`
- `src/db/connection.ts:9`
- `src/index.ts:18`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `tests/gateway.integration.test.ts:146`
- `tests/runLifecycle.test.ts:23`
- `tests/slurm.integration.test.ts:48`
- `src/mcp/toolSchemas.ts:3`
- `src/toolpacks/types.ts:38`
- `tests/toolpacks.test.ts:183`
</details>

## Dependency Inventory

- `symbol:scripts/bundle_export.ts:args:44` calls `symbol:scripts/bundle_export.ts:parseArgs:24` (high)
- `symbol:scripts/bundle_export.ts:args:44` calls `symbol:src/artifacts/localObjectStore.ts:slice:82` (low)
- `symbol:scripts/bundle_export.ts:db:70` calls `symbol:src/db/connection.ts:createDb:9` (low)
- `symbol:scripts/bundle_export.ts:key:30` calls `symbol:src/artifacts/localObjectStore.ts:slice:82` (low)
- `symbol:scripts/bundle_export.ts:main:43` calls `symbol:scripts/bundle_export.ts:parseArgs:24` (high)
- `symbol:scripts/bundle_export.ts:main:43` calls `symbol:scripts/bundle_export.ts:usage:12` (high)
- `symbol:scripts/bundle_export.ts:main:43` calls `symbol:src/artifacts/localObjectStore.ts:slice:82` (low)
- `symbol:scripts/bundle_export.ts:main:43` calls `symbol:src/bundle/bundleExport.ts:exportBundleToDir:175` (low)
- `symbol:scripts/bundle_export.ts:main:43` calls `symbol:src/bundle/bundleTar.ts:bundleDirToDeterministicTar:25` (low)
- `symbol:scripts/bundle_export.ts:main:43` calls `symbol:src/db/connection.ts:createDb:9` (low)
- `symbol:scripts/bundle_export.ts:main:43` calls `symbol:src/db/connection.ts:createPgPool:5` (low)
- `symbol:scripts/bundle_export.ts:main:43` calls `symbol:src/execution/backends/dockerRunner.ts:rm:83` (low)
- `symbol:scripts/bundle_export.ts:parseArgs:24` calls `symbol:src/artifacts/localObjectStore.ts:slice:82` (low)
- `symbol:scripts/bundle_export.ts:pool:69` calls `symbol:src/db/connection.ts:createPgPool:5` (low)
- `symbol:scripts/bundle_export.ts:res:101` calls `symbol:src/bundle/bundleExport.ts:exportBundleToDir:175` (low)
- `symbol:scripts/bundle_export.ts:res:78` calls `symbol:src/bundle/bundleExport.ts:exportBundleToDir:175` (low)
- `symbol:scripts/bundle_verify.ts:args:90` calls `symbol:scripts/bundle_verify.ts:parseArgs:22` (high)
- `symbol:scripts/bundle_verify.ts:args:90` calls `symbol:src/artifacts/localObjectStore.ts:slice:82` (low)
- `symbol:scripts/bundle_verify.ts:key:28` calls `symbol:src/artifacts/localObjectStore.ts:slice:82` (low)
- `symbol:scripts/bundle_verify.ts:main:89` calls `symbol:scripts/bundle_verify.ts:parseArgs:22` (high)
- `symbol:scripts/bundle_verify.ts:main:89` calls `symbol:scripts/bundle_verify.ts:usage:11` (high)
- `symbol:scripts/bundle_verify.ts:main:89` calls `symbol:scripts/bundle_verify.ts:verifyTarBundle:77` (high)
- `symbol:scripts/bundle_verify.ts:main:89` calls `symbol:src/artifacts/localObjectStore.ts:slice:82` (low)
- `symbol:scripts/bundle_verify.ts:main:89` calls `symbol:src/bundle/bundleVerify.ts:verifyBundleDir:31` (low)
- `symbol:scripts/bundle_verify.ts:parseArgs:22` calls `symbol:src/artifacts/localObjectStore.ts:slice:82` (low)
- `symbol:scripts/bundle_verify.ts:sha256File:41` calls `symbol:src/core/ids.ts:digest:28` (low)
- `symbol:scripts/bundle_verify.ts:tarSha:62` calls `symbol:scripts/bundle_verify.ts:sha256File:41` (high)
- `symbol:scripts/bundle_verify.ts:verifyTarBundle:77` calls `symbol:scripts/bundle_verify.ts:execFileAsync:9` (high)
- `symbol:scripts/bundle_verify.ts:verifyTarBundle:77` calls `symbol:scripts/bundle_verify.ts:verifyTarDigest:57` (high)
- `symbol:scripts/bundle_verify.ts:verifyTarBundle:77` calls `symbol:src/bundle/bundleVerify.ts:verifyBundleDir:31` (low)
- `symbol:scripts/bundle_verify.ts:verifyTarBundle:77` calls `symbol:src/execution/backends/dockerRunner.ts:rm:83` (low)
- `symbol:scripts/bundle_verify.ts:verifyTarDigest:57` calls `symbol:scripts/bundle_verify.ts:sha256File:41` (high)
- `symbol:src/artifacts/artifactService.ts:artifactId:28` calls `symbol:src/core/ids.ts:newArtifactId:17` (medium)
- `symbol:src/artifacts/artifactService.ts:ArtifactService:14` calls `symbol:src/core/detectArtifactType.ts:detectArtifactType:3` (medium)
- `symbol:src/artifacts/artifactService.ts:ArtifactService:14` calls `symbol:src/core/ids.ts:newArtifactId:17` (medium)
- `symbol:src/artifacts/artifactService.ts:ArtifactService:14` calls `symbol:src/core/mimeType.ts:mimeTypeForArtifactType:3` (medium)
- `symbol:src/artifacts/artifactService.ts:inferredType:29` calls `symbol:src/core/detectArtifactType.ts:detectArtifactType:3` (medium)
- `symbol:src/artifacts/artifactService.ts:mimeType:41` calls `symbol:src/core/mimeType.ts:mimeTypeForArtifactType:3` (medium)
- `symbol:src/artifacts/localObjectStore.ts:bytes:34` calls `symbol:src/store/postgresStore.ts:from:258` (medium)
- `symbol:src/artifacts/localObjectStore.ts:checksumSha256:38` calls `symbol:src/core/ids.ts:digest:28` (medium)
- `symbol:src/artifacts/localObjectStore.ts:checksumSha256:68` calls `symbol:src/core/ids.ts:digest:28` (medium)
- `symbol:src/artifacts/localObjectStore.ts:destinationPath:50` calls `symbol:src/artifacts/localObjectStore.ts:objectPath:39` (high)
- `symbol:src/artifacts/localObjectStore.ts:limited:86` calls `symbol:src/artifacts/localObjectStore.ts:slice:82` (high)
- `symbol:src/artifacts/localObjectStore.ts:LocalObjectStore:21` calls `symbol:src/artifacts/localObjectStore.ts:objectPath:39` (high)
- `symbol:src/artifacts/localObjectStore.ts:LocalObjectStore:21` calls `symbol:src/artifacts/localObjectStore.ts:slice:82` (high)
- `symbol:src/artifacts/localObjectStore.ts:LocalObjectStore:21` calls `symbol:src/core/ids.ts:digest:28` (medium)
- `symbol:src/artifacts/localObjectStore.ts:LocalObjectStore:21` calls `symbol:src/store/postgresStore.ts:from:258` (medium)
- `symbol:src/artifacts/localObjectStore.ts:objectPath:39` calls `symbol:src/artifacts/localObjectStore.ts:objectPath:77` (high)
- `symbol:src/artifacts/localObjectStore.ts:objectPath:77` calls `symbol:src/artifacts/localObjectStore.ts:objectPath:39` (high)
- `symbol:src/artifacts/localObjectStore.ts:sourcePath:99` calls `symbol:src/artifacts/localObjectStore.ts:objectPath:39` (high)
- `symbol:src/bundle/bundleExport.ts:artifacts:152` calls `symbol:src/execution/executionService.ts:sort:59` (medium)
- `symbol:src/bundle/bundleExport.ts:artifacts:152` calls `symbol:src/store/postgresStore.ts:from:258` (medium)
- `symbol:src/bundle/bundleExport.ts:artifactsJson:192` calls `symbol:src/execution/executionService.ts:sort:59` (medium)
- `symbol:src/bundle/bundleExport.ts:blobArtifactIds:233` calls `symbol:src/bundle/bundleExport.ts:chooseBlobArtifactIds:89` (high)
- `symbol:src/bundle/bundleExport.ts:environmentsJson:188` calls `symbol:src/execution/executionService.ts:sort:59` (medium)
- `symbol:src/bundle/bundleExport.ts:eventsNdjson:230` calls `symbol:src/core/canonicalJson.ts:stableJsonStringify:78` (medium)
- `symbol:src/bundle/bundleExport.ts:exportBundleToDir:175` calls `symbol:src/bundle/bundleExport.ts:blobSourcePathForArtifact:73` (high)
- `symbol:src/bundle/bundleExport.ts:exportBundleToDir:175` calls `symbol:src/bundle/bundleExport.ts:chooseBlobArtifactIds:89` (high)
- `symbol:src/bundle/bundleExport.ts:exportBundleToDir:175` calls `symbol:src/bundle/bundleExport.ts:loadExportGraph:116` (high)
- `symbol:src/bundle/bundleExport.ts:exportBundleToDir:175` calls `symbol:src/bundle/bundleExport.ts:requireRelativePath:32` (high)
- `symbol:src/bundle/bundleExport.ts:exportBundleToDir:175` calls `symbol:src/bundle/bundleExport.ts:sha256File:41` (high)
- `symbol:src/bundle/bundleExport.ts:exportBundleToDir:175` calls `symbol:src/bundle/bundleExport.ts:writeFileBytes:168` (high)
- `symbol:src/bundle/bundleExport.ts:exportBundleToDir:175` calls `symbol:src/bundle/bundleExport.ts:writeFileUtf8:161` (high)
- `symbol:src/bundle/bundleExport.ts:exportBundleToDir:175` calls `symbol:src/bundle/bundleVerify.ts:verifyBundleDir:31` (medium)
- `symbol:src/bundle/bundleExport.ts:exportBundleToDir:175` calls `symbol:src/core/canonicalJson.ts:sha256Prefixed:9` (medium)
- `symbol:src/bundle/bundleExport.ts:exportBundleToDir:175` calls `symbol:src/core/canonicalJson.ts:stableJsonStringify:78` (medium)
- `symbol:src/bundle/bundleExport.ts:exportBundleToDir:175` calls `symbol:src/execution/executionService.ts:sort:59` (medium)
- `symbol:src/bundle/bundleExport.ts:exportBundleToDir:175` calls `symbol:src/store/postgresStore.ts:from:258` (medium)
- `symbol:src/bundle/bundleExport.ts:extractGraphRunIds:59` calls `symbol:src/execution/executionService.ts:sort:59` (medium)
- `symbol:src/bundle/bundleExport.ts:extractGraphRunIds:59` calls `symbol:src/store/postgresStore.ts:from:258` (medium)
- `symbol:src/bundle/bundleExport.ts:graph:179` calls `symbol:src/bundle/bundleExport.ts:loadExportGraph:116` (high)
- `symbol:src/bundle/bundleExport.ts:inputs:134` calls `symbol:src/artifacts/localObjectStore.ts:slice:82` (medium)
- `symbol:src/bundle/bundleExport.ts:inputs:134` calls `symbol:src/execution/executionService.ts:sort:59` (medium)
- `symbol:src/bundle/bundleExport.ts:loadExportGraph:116` calls `symbol:src/artifacts/localObjectStore.ts:slice:82` (medium)
- `symbol:src/bundle/bundleExport.ts:loadExportGraph:116` calls `symbol:src/bundle/bundleExport.ts:extractGraphRunIds:59` (high)
- `symbol:src/bundle/bundleExport.ts:loadExportGraph:116` calls `symbol:src/execution/executionService.ts:sort:59` (medium)
- `symbol:src/bundle/bundleExport.ts:loadExportGraph:116` calls `symbol:src/store/postgresStore.ts:from:258` (medium)
- `symbol:src/bundle/bundleExport.ts:manifestBytes:281` calls `symbol:src/core/canonicalJson.ts:stableJsonStringify:78` (medium)
- `symbol:src/bundle/bundleExport.ts:manifestBytes:281` calls `symbol:src/store/postgresStore.ts:from:258` (medium)
- `symbol:src/bundle/bundleExport.ts:manifestSha256:284` calls `symbol:src/core/canonicalJson.ts:sha256Prefixed:9` (medium)
- `symbol:src/bundle/bundleExport.ts:outputs:139` calls `symbol:src/artifacts/localObjectStore.ts:slice:82` (medium)
- `symbol:src/bundle/bundleExport.ts:outputs:139` calls `symbol:src/execution/executionService.ts:sort:59` (medium)
- `symbol:src/bundle/bundleExport.ts:paramSetsJson:182` calls `symbol:src/execution/executionService.ts:sort:59` (medium)
- `symbol:src/bundle/bundleExport.ts:policySnapshotsJson:185` calls `symbol:src/execution/executionService.ts:sort:59` (medium)
- `symbol:src/bundle/bundleExport.ts:rel:237` calls `symbol:src/bundle/bundleExport.ts:requireRelativePath:32` (high)
- `symbol:src/bundle/bundleExport.ts:runIds:120` calls `symbol:src/bundle/bundleExport.ts:extractGraphRunIds:59` (high)
- `symbol:src/bundle/bundleExport.ts:runIds:120` calls `symbol:src/execution/executionService.ts:sort:59` (medium)
- `symbol:src/bundle/bundleExport.ts:runIds:120` calls `symbol:src/store/postgresStore.ts:from:258` (medium)
- `symbol:src/bundle/bundleExport.ts:safeRel:162` calls `symbol:src/bundle/bundleExport.ts:requireRelativePath:32` (high)
- `symbol:src/bundle/bundleExport.ts:safeRel:169` calls `symbol:src/bundle/bundleExport.ts:requireRelativePath:32` (high)
- `symbol:src/bundle/bundleExport.ts:sha256File:41` calls `symbol:src/core/ids.ts:digest:28` (medium)
- `symbol:src/bundle/bundleExport.ts:source:242` calls `symbol:src/bundle/bundleExport.ts:blobSourcePathForArtifact:73` (high)
- `symbol:src/bundle/bundleExport.ts:writeFileBytes:168` calls `symbol:src/bundle/bundleExport.ts:requireRelativePath:32` (high)
- `symbol:src/bundle/bundleExport.ts:writeFileUtf8:161` calls `symbol:src/bundle/bundleExport.ts:requireRelativePath:32` (high)
- `symbol:src/bundle/bundleTar.ts:bundleDirToDeterministicTar:25` calls `symbol:src/bundle/bundleTar.ts:execFileAsync:7` (high)
- `symbol:src/bundle/bundleTar.ts:bundleDirToDeterministicTar:25` calls `symbol:src/bundle/bundleTar.ts:sha256File:9` (high)
- `symbol:src/bundle/bundleTar.ts:sha256File:9` calls `symbol:src/core/ids.ts:digest:28` (medium)
- `symbol:src/bundle/bundleVerify.ts:canonical:27` calls `symbol:src/core/canonicalJson.ts:stableJsonStringify:78` (medium)
- `symbol:src/bundle/bundleVerify.ts:manifest:34` calls `symbol:src/bundle/bundleVerify.ts:parseManifest:25` (high)
- `symbol:src/bundle/bundleVerify.ts:manifestDigest:52` calls `symbol:src/bundle/bundleVerify.ts:sha256File:7` (high)
- `symbol:src/bundle/bundleVerify.ts:parseManifest:25` calls `symbol:src/core/canonicalJson.ts:stableJsonStringify:78` (medium)
- `symbol:src/bundle/bundleVerify.ts:sha256File:7` calls `symbol:src/core/ids.ts:digest:28` (medium)
- `symbol:src/bundle/bundleVerify.ts:verifyBundleDir:31` calls `symbol:src/bundle/bundleVerify.ts:parseManifest:25` (high)
- `symbol:src/bundle/bundleVerify.ts:verifyBundleDir:31` calls `symbol:src/bundle/bundleVerify.ts:sha256File:7` (high)
- `symbol:src/core/canonicalJson.ts:c:53` calls `symbol:src/core/canonicalJson.ts:canonicalizeJson:35` (high)
- `symbol:src/core/canonicalJson.ts:c:62` calls `symbol:src/core/canonicalJson.ts:canonicalizeJson:35` (high)
- `symbol:src/core/canonicalJson.ts:canonicalizeJson:35` calls `symbol:src/core/canonicalJson.ts:isPlainObject:27` (high)
- `symbol:src/core/canonicalJson.ts:canonicalizeJson:35` calls `symbol:src/core/canonicalJson.ts:keys:60` (high)
- `symbol:src/core/canonicalJson.ts:canonicalizeJson:35` calls `symbol:src/execution/executionService.ts:sort:59` (medium)
- `symbol:src/core/canonicalJson.ts:keys:60` calls `symbol:src/execution/executionService.ts:sort:59` (medium)
- `symbol:src/core/canonicalJson.ts:sha256Hex:5` calls `symbol:src/core/ids.ts:digest:28` (medium)
- `symbol:src/core/canonicalJson.ts:sha256Prefixed:9` calls `symbol:src/core/canonicalJson.ts:sha256Hex:5` (high)
- `symbol:src/core/canonicalJson.ts:stableJsonStringify:78` calls `symbol:src/core/canonicalJson.ts:canonicalizeJson:35` (high)
- `symbol:src/core/ids.ts:deriveRunIdFromParts:25` calls `symbol:src/core/canonicalJson.ts:encodeCrockfordBase32_128bits:13` (medium)
- `symbol:src/core/ids.ts:deriveRunIdFromParts:25` calls `symbol:src/core/ids.ts:digest:28` (high)
- `symbol:src/core/ids.ts:newArtifactId:17` calls `symbol:src/core/ids.ts:prefixed:9` (high)
- `symbol:src/core/ids.ts:newProjectId:13` calls `symbol:src/core/ids.ts:prefixed:9` (high)
- `symbol:src/core/ids.ts:newRunId:21` calls `symbol:src/core/ids.ts:prefixed:9` (high)
- `symbol:src/execution/backends/dockerRunner.ts:appendLimited:6` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/execution/backends/dockerRunner.ts:DockerRunner:20` calls `symbol:src/bundle/bundleVerify.ts:entries:38` (medium)
- `symbol:src/execution/backends/dockerRunner.ts:DockerRunner:20` calls `symbol:src/execution/backends/dockerRunner.ts:appendLimited:6` (high)
- `symbol:src/execution/backends/dockerRunner.ts:DockerRunner:20` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/execution/backends/dockerRunner.ts:keep:10` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/execution/backends/dockerRunner.ts:timeoutMs:77` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/execution/backends/localProcess.ts:appendLimited:6` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/execution/backends/localProcess.ts:keep:10` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/execution/backends/localProcess.ts:LocalProcessRunner:20` calls `symbol:src/execution/backends/localProcess.ts:appendLimited:6` (high)
- `symbol:src/execution/deterministic.ts:f:17` calls `symbol:src/execution/deterministic.ts:floatBetween:9` (high)
- `symbol:src/execution/deterministic.ts:floatBetween:9` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/execution/deterministic.ts:idx:10` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/execution/deterministic.ts:intBetween:16` calls `symbol:src/execution/deterministic.ts:floatBetween:9` (high)
- `symbol:src/execution/deterministic.ts:seedFrom:3` calls `symbol:src/core/ids.ts:digest:28` (medium)
- `symbol:src/execution/executionService.ts:DefaultExecutionService:35` calls `symbol:src/execution/inSilico.ts:simulateAlignReads:62` (medium)
- `symbol:src/execution/executionService.ts:DefaultExecutionService:35` calls `symbol:src/execution/inSilico.ts:simulateQcFastq:21` (medium)
- `symbol:src/execution/executionService.ts:res:47` calls `symbol:src/execution/inSilico.ts:simulateQcFastq:21` (medium)
- `symbol:src/execution/executionService.ts:res:61` calls `symbol:src/execution/inSilico.ts:simulateAlignReads:62` (medium)
- `symbol:src/execution/inSilico.ts:dupPct:82` calls `symbol:src/execution/deterministic.ts:floatBetween:9` (medium)
- `symbol:src/execution/inSilico.ts:gc:36` calls `symbol:src/execution/deterministic.ts:floatBetween:9` (medium)
- `symbol:src/execution/inSilico.ts:insertMedian:83` calls `symbol:src/execution/deterministic.ts:intBetween:16` (medium)
- `symbol:src/execution/inSilico.ts:mappedPct:81` calls `symbol:src/execution/deterministic.ts:floatBetween:9` (medium)
- `symbol:src/execution/inSilico.ts:q30:35` calls `symbol:src/execution/deterministic.ts:floatBetween:9` (medium)
- `symbol:src/execution/inSilico.ts:readsEstimated:34` calls `symbol:src/execution/deterministic.ts:intBetween:16` (medium)
- `symbol:src/execution/inSilico.ts:seed:27` calls `symbol:src/execution/deterministic.ts:seedFrom:3` (medium)
- `symbol:src/execution/inSilico.ts:seed:71` calls `symbol:src/execution/deterministic.ts:seedFrom:3` (medium)
- `symbol:src/execution/inSilico.ts:simulateAlignReads:62` calls `symbol:src/artifacts/localObjectStore.ts:slice:82` (medium)
- `symbol:src/execution/inSilico.ts:simulateAlignReads:62` calls `symbol:src/execution/deterministic.ts:floatBetween:9` (medium)
- `symbol:src/execution/inSilico.ts:simulateAlignReads:62` calls `symbol:src/execution/deterministic.ts:intBetween:16` (medium)
- `symbol:src/execution/inSilico.ts:simulateAlignReads:62` calls `symbol:src/execution/deterministic.ts:seedFrom:3` (medium)
- `symbol:src/execution/inSilico.ts:simulateQcFastq:21` calls `symbol:src/execution/deterministic.ts:floatBetween:9` (medium)
- `symbol:src/execution/inSilico.ts:simulateQcFastq:21` calls `symbol:src/execution/deterministic.ts:intBetween:16` (medium)
- `symbol:src/execution/inSilico.ts:simulateQcFastq:21` calls `symbol:src/execution/deterministic.ts:seedFrom:3` (medium)
- `symbol:src/execution/slurm/scheduler.ts:exitCode:91` calls `symbol:src/execution/slurm/scheduler.ts:parseExitCodeField:47` (high)
- `symbol:src/execution/slurm/scheduler.ts:normalizedState:114` calls `symbol:src/execution/slurm/scheduler.ts:normalizeSlurmState:21` (high)
- `symbol:src/execution/slurm/scheduler.ts:normalizedState:92` calls `symbol:src/execution/slurm/scheduler.ts:normalizeSlurmState:21` (high)
- `symbol:src/execution/slurm/scheduler.ts:sacct:122` calls `symbol:src/execution/slurm/scheduler.ts:trySacct:55` (high)
- `symbol:src/execution/slurm/scheduler.ts:squeue:126` calls `symbol:src/execution/slurm/scheduler.ts:trySqueue:99` (high)
- `symbol:src/execution/slurm/scheduler.ts:SystemSlurmScheduler:118` calls `symbol:src/execution/slurm/scheduler.ts:trySacct:55` (high)
- `symbol:src/execution/slurm/scheduler.ts:SystemSlurmScheduler:118` calls `symbol:src/execution/slurm/scheduler.ts:trySqueue:99` (high)
- `symbol:src/execution/slurm/scheduler.ts:trySacct:55` calls `symbol:src/execution/slurm/scheduler.ts:normalizeSlurmState:21` (high)
- `symbol:src/execution/slurm/scheduler.ts:trySacct:55` calls `symbol:src/execution/slurm/scheduler.ts:parseExitCodeField:47` (high)
- `symbol:src/execution/slurm/scheduler.ts:trySqueue:99` calls `symbol:src/execution/slurm/scheduler.ts:normalizeSlurmState:21` (high)
- `symbol:src/execution/slurm/slurmScriptV1.ts:cmdLine:143` calls `symbol:src/execution/slurm/slurmScriptV1.ts:bashSingleQuote:48` (high)
- `symbol:src/execution/slurm/slurmScriptV1.ts:envKeys:86` calls `symbol:src/core/canonicalJson.ts:keys:60` (medium)
- `symbol:src/execution/slurm/slurmScriptV1.ts:envKeys:86` calls `symbol:src/execution/executionService.ts:sort:59` (medium)
- `symbol:src/execution/slurm/slurmScriptV1.ts:jobName:82` calls `symbol:src/artifacts/localObjectStore.ts:slice:82` (medium)
- `symbol:src/execution/slurm/slurmScriptV1.ts:renderSlurmScriptV1:76` calls `symbol:src/artifacts/localObjectStore.ts:slice:82` (medium)
- `symbol:src/execution/slurm/slurmScriptV1.ts:renderSlurmScriptV1:76` calls `symbol:src/core/canonicalJson.ts:keys:60` (medium)
- `symbol:src/execution/slurm/slurmScriptV1.ts:renderSlurmScriptV1:76` calls `symbol:src/execution/executionService.ts:sort:59` (medium)
- `symbol:src/execution/slurm/slurmScriptV1.ts:renderSlurmScriptV1:76` calls `symbol:src/execution/slurm/slurmScriptV1.ts:assertEnvKey:70` (high)
- `symbol:src/execution/slurm/slurmScriptV1.ts:renderSlurmScriptV1:76` calls `symbol:src/execution/slurm/slurmScriptV1.ts:bashSingleQuote:48` (high)
- `symbol:src/execution/slurm/slurmScriptV1.ts:renderSlurmScriptV1:76` calls `symbol:src/execution/slurm/slurmScriptV1.ts:formatSlurmTimeLimit:52` (high)
- `symbol:src/execution/slurm/submitter.ts:jobId:40` calls `symbol:src/execution/slurm/submitter.ts:parseSbatchJobId:13` (high)
- `symbol:src/execution/slurm/submitter.ts:SbatchSubmitter:27` calls `symbol:src/execution/slurm/submitter.ts:parseSbatchJobId:13` (high)
- `symbol:src/execution/workspace.ts:createRunWorkspace:26` calls `symbol:src/execution/workspace.ts:safeJoin:15` (high)
- `symbol:src/index.ts:createPool:13` calls `symbol:src/db/connection.ts:createPgPool:5` (medium)
- `symbol:src/index.ts:db:40` calls `symbol:src/db/connection.ts:createDb:9` (medium)
- `symbol:src/index.ts:main:22` calls `symbol:src/db/bootstrap.ts:applySqlFile:4` (medium)
- `symbol:src/index.ts:main:22` calls `symbol:src/db/connection.ts:createDb:9` (medium)
- `symbol:src/index.ts:main:22` calls `symbol:src/index.ts:createPool:13` (high)
- `symbol:src/index.ts:main:22` calls `symbol:src/mcp/gatewayServer.ts:createGatewayServer:76` (medium)
- `symbol:src/index.ts:main:22` calls `symbol:src/mcp/gatewayServer.ts:error:1558` (medium)
- `symbol:src/index.ts:pool:35` calls `symbol:src/index.ts:createPool:13` (high)
- `symbol:src/index.ts:server:46` calls `symbol:src/mcp/gatewayServer.ts:createGatewayServer:76` (medium)
- `symbol:src/mcp/gatewayServer.ts:canonicalSource:142` calls `symbol:src/core/canonicalJson.ts:sha256Prefixed:9` (medium)
- `symbol:src/mcp/gatewayServer.ts:canonicalSource:142` calls `symbol:src/store/postgresStore.ts:from:258` (medium)
- `symbol:src/mcp/gatewayServer.ts:createGatewayServer:76` calls `symbol:src/core/canonicalJson.ts:keys:60` (medium)
- `symbol:src/mcp/gatewayServer.ts:createGatewayServer:76` calls `symbol:src/core/canonicalJson.ts:sha256Prefixed:9` (medium)
- `symbol:src/mcp/gatewayServer.ts:createGatewayServer:76` calls `symbol:src/execution/executionService.ts:sort:59` (medium)
- `symbol:src/mcp/gatewayServer.ts:createGatewayServer:76` calls `symbol:src/execution/slurm/slurmScriptV1.ts:renderSlurmScriptV1:76` (medium)
- `symbol:src/mcp/gatewayServer.ts:createGatewayServer:76` calls `symbol:src/execution/workspace.ts:createRunWorkspace:26` (medium)
- `symbol:src/mcp/gatewayServer.ts:createGatewayServer:76` calls `symbol:src/execution/workspace.ts:safeJoin:15` (medium)
- `symbol:src/mcp/gatewayServer.ts:createGatewayServer:76` calls `symbol:src/mcp/envSnapshot.ts:envSnapshot:3` (medium)
- `symbol:src/mcp/gatewayServer.ts:createGatewayServer:76` calls `symbol:src/mcp/gatewayServer.ts:assertRegularFileNoSymlink:108` (high)
- `symbol:src/mcp/gatewayServer.ts:createGatewayServer:76` calls `symbol:src/mcp/gatewayServer.ts:hasRegularFile:1176` (high)
- `symbol:src/mcp/gatewayServer.ts:createGatewayServer:76` calls `symbol:src/mcp/gatewayServer.ts:readOptionalText:1167` (high)
- `symbol:src/mcp/gatewayServer.ts:createGatewayServer:76` calls `symbol:src/mcp/gatewayServer.ts:requireArtifactType:103` (high)
- `symbol:src/mcp/gatewayServer.ts:createGatewayServer:76` calls `symbol:src/mcp/gatewayServer.ts:snapshot:343` (high)
- `symbol:src/mcp/gatewayServer.ts:createGatewayServer:76` calls `symbol:src/mcp/gatewayServer.ts:submit:974` (high)
- `symbol:src/mcp/gatewayServer.ts:createGatewayServer:76` calls `symbol:src/mcp/gatewayServer.ts:toArtifactSummary:61` (high)
- `symbol:src/mcp/gatewayServer.ts:createGatewayServer:76` calls `symbol:src/runs/runIdentity.ts:deriveRunId:16` (medium)
- `symbol:src/mcp/gatewayServer.ts:createGatewayServer:76` calls `symbol:src/runs/toolRun.ts:requestedByFromExtra:167` (medium)
- `symbol:src/mcp/gatewayServer.ts:createGatewayServer:76` calls `symbol:src/store/postgresStore.ts:from:258` (medium)
- `symbol:src/mcp/gatewayServer.ts:createGatewayServer:76` calls `symbol:src/toolpacks/register.ts:registerToolDefinitions:516` (medium)
- `symbol:src/mcp/gatewayServer.ts:destPath:950` calls `symbol:src/execution/workspace.ts:safeJoin:15` (medium)
- `symbol:src/mcp/gatewayServer.ts:exitCodePath:1187` calls `symbol:src/execution/workspace.ts:safeJoin:15` (medium)
- `symbol:src/mcp/gatewayServer.ts:exitCodePath:1548` calls `symbol:src/execution/workspace.ts:safeJoin:15` (medium)
- `symbol:src/mcp/gatewayServer.ts:exitCodeText:1196` calls `symbol:src/mcp/gatewayServer.ts:readOptionalText:1167` (high)
- `symbol:src/mcp/gatewayServer.ts:hasRegularFile:1176` calls `symbol:src/mcp/gatewayServer.ts:assertRegularFileNoSymlink:108` (high)
- `symbol:src/mcp/gatewayServer.ts:hasStderr:1201` calls `symbol:src/mcp/gatewayServer.ts:hasRegularFile:1176` (high)
- `symbol:src/mcp/gatewayServer.ts:hasStdout:1200` calls `symbol:src/mcp/gatewayServer.ts:hasRegularFile:1176` (high)
- `symbol:src/mcp/gatewayServer.ts:inputsSorted:817` calls `symbol:src/execution/executionService.ts:sort:59` (medium)
- `symbol:src/mcp/gatewayServer.ts:jobIdFromFile:1191` calls `symbol:src/mcp/gatewayServer.ts:readOptionalText:1167` (high)
- `symbol:src/mcp/gatewayServer.ts:jobIdPath:1186` calls `symbol:src/execution/workspace.ts:safeJoin:15` (medium)
- `symbol:src/mcp/gatewayServer.ts:outputsSorted:820` calls `symbol:src/execution/executionService.ts:sort:59` (medium)
- `symbol:src/mcp/gatewayServer.ts:readOptionalText:1167` calls `symbol:src/mcp/gatewayServer.ts:assertRegularFileNoSymlink:108` (high)
- `symbol:src/mcp/gatewayServer.ts:script:954` calls `symbol:src/execution/slurm/slurmScriptV1.ts:renderSlurmScriptV1:76` (medium)
- `symbol:src/mcp/gatewayServer.ts:srcPath:1498` calls `symbol:src/execution/workspace.ts:safeJoin:15` (medium)
- `symbol:src/mcp/gatewayServer.ts:stderrPath:1189` calls `symbol:src/execution/workspace.ts:safeJoin:15` (medium)
- `symbol:src/mcp/gatewayServer.ts:stderrPath:1516` calls `symbol:src/execution/workspace.ts:safeJoin:15` (medium)
- `symbol:src/mcp/gatewayServer.ts:stdoutPath:1188` calls `symbol:src/execution/workspace.ts:safeJoin:15` (medium)
- `symbol:src/mcp/gatewayServer.ts:stdoutPath:1515` calls `symbol:src/execution/workspace.ts:safeJoin:15` (medium)
- `symbol:src/mcp/gatewayServer.ts:structured:212` calls `symbol:src/mcp/gatewayServer.ts:toArtifactSummary:61` (high)
- `symbol:src/mcp/gatewayServer.ts:structured:303` calls `symbol:src/mcp/gatewayServer.ts:toArtifactSummary:61` (high)
- `symbol:src/mcp/gatewayServer.ts:type:1497` calls `symbol:src/mcp/gatewayServer.ts:requireArtifactType:103` (high)
- `symbol:src/mcp/gatewayServer.ts:ws:1470` calls `symbol:src/execution/workspace.ts:createRunWorkspace:26` (medium)
- `symbol:src/mcp/gatewayServer.ts:ws:931` calls `symbol:src/execution/workspace.ts:createRunWorkspace:26` (medium)
- `symbol:src/mcp/toolSchemas.ts:zArtifactImportInput:45` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/mcp/toolSchemas.ts:zArtifactListInput:69` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/mcp/toolSchemas.ts:zArtifactPreviewTextInput:81` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/mcp/toolSchemas.ts:zArtifactSummary:28` calls `symbol:src/toolpacks/builtin/seqkitStats.ts:record:26` (medium)
- `symbol:src/mcp/toolSchemas.ts:zDockerJobGetOutput:504` calls `symbol:src/toolpacks/builtin/seqkitStats.ts:record:26` (medium)
- `symbol:src/mcp/toolSchemas.ts:zFastqcInput:275` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/mcp/toolSchemas.ts:zFastqcOutputDocker:282` calls `symbol:src/toolpacks/builtin/seqkitStats.ts:record:26` (medium)
- `symbol:src/mcp/toolSchemas.ts:zFastqcOutputV1:298` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/mcp/toolSchemas.ts:zFastqcOutputV1:298` calls `symbol:src/toolpacks/builtin/seqkitStats.ts:record:26` (medium)
- `symbol:src/mcp/toolSchemas.ts:zMultiqcInput:319` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/mcp/toolSchemas.ts:zMultiqcOutputV1:336` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/mcp/toolSchemas.ts:zQcBundleFastqInput:354` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/mcp/toolSchemas.ts:zSamtoolsFlagstatOutputV2:435` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/mcp/toolSchemas.ts:zSeqkitStatsInput:139` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/mcp/toolSchemas.ts:zSeqkitStatsOutput:145` calls `symbol:src/toolpacks/builtin/seqkitStats.ts:record:26` (medium)
- `symbol:src/mcp/toolSchemas.ts:zSimulateAlignReadsInput:111` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/mcp/toolSchemas.ts:zSimulateQcFastqInput:94` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/mcp/toolSchemas.ts:zSlurmJobCollectOutput:266` calls `symbol:src/toolpacks/builtin/seqkitStats.ts:record:26` (medium)
- `symbol:src/mcp/toolSchemas.ts:zSlurmJobGetOutput:471` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/mcp/toolSchemas.ts:zSlurmJobGetOutput:471` calls `symbol:src/toolpacks/builtin/seqkitStats.ts:record:26` (medium)
- `symbol:src/mcp/toolSchemas.ts:zSlurmJobSpecV1:202` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/mcp/toolSchemas.ts:zSlurmJobSpecV1:202` calls `symbol:src/toolpacks/builtin/seqkitStats.ts:record:26` (medium)
- `symbol:src/mcp/toolSchemas.ts:zSlurmSubmitOutput:256` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/policy/policy.ts:expandPolicyEnv:81` calls `symbol:src/policy/policy.ts:expandEnvToken:59` (high)
- `symbol:src/policy/policy.ts:PolicyEngine:95` calls `symbol:src/core/canonicalJson.ts:sha256Prefixed:9` (medium)
- `symbol:src/policy/policy.ts:PolicyEngine:95` calls `symbol:src/core/canonicalJson.ts:stableJsonStringify:78` (medium)
- `symbol:src/policy/policy.ts:PolicyEngine:95` calls `symbol:src/policy/policy.ts:expandPolicyEnv:81` (high)
- `symbol:src/policy/policy.ts:PolicyEngine:95` calls `symbol:src/policy/policy.ts:isPolicyConfig:53` (high)
- `symbol:src/policy/policy.ts:prefixes:82` calls `symbol:src/policy/policy.ts:expandEnvToken:59` (high)
- `symbol:src/runs/runIdentity.ts:deriveCanonicalParamsHash:12` calls `symbol:src/core/canonicalJson.ts:sha256Prefixed:9` (medium)
- `symbol:src/runs/runIdentity.ts:deriveCanonicalParamsHash:12` calls `symbol:src/core/canonicalJson.ts:stableJsonStringify:78` (medium)
- `symbol:src/runs/runIdentity.ts:deriveRunId:16` calls `symbol:src/core/ids.ts:deriveRunIdFromParts:25` (medium)
- `symbol:src/runs/runIdentity.ts:deriveRunId:16` calls `symbol:src/runs/runIdentity.ts:deriveCanonicalParamsHash:12` (high)
- `symbol:src/runs/runIdentity.ts:paramsHash:17` calls `symbol:src/runs/runIdentity.ts:deriveCanonicalParamsHash:12` (high)
- `symbol:src/runs/runIdentity.ts:runId:18` calls `symbol:src/core/ids.ts:deriveRunIdFromParts:25` (medium)
- `symbol:src/runs/toolRun.ts:ToolRun:9` calls `symbol:tests/runLifecycle.test.ts:createRun:34` (low)
- `symbol:src/store/postgresStore.ts:PostgresStore:28` calls `symbol:src/core/canonicalJson.ts:keys:60` (medium)
- `symbol:src/store/postgresStore.ts:PostgresStore:28` calls `symbol:src/core/run.ts:canTransitionRunStatus:18` (medium)
- `symbol:src/store/postgresStore.ts:PostgresStore:28` calls `symbol:src/execution/executionService.ts:sort:59` (medium)
- `symbol:src/store/postgresStore.ts:PostgresStore:28` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/store/postgresStore.ts:PostgresStore:28` calls `symbol:src/store/postgresStore.ts:toIso:17` (high)
- `symbol:src/store/postgresStore.ts:PostgresStore:28` calls `symbol:src/store/postgresStore.ts:toIsoOrNull:23` (high)
- `symbol:src/store/postgresStore.ts:PostgresStore:28` calls `symbol:src/toolpacks/builtin/seqkitStats.ts:values:24` (medium)
- `symbol:src/store/postgresStore.ts:row:38` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/store/postgresStore.ts:toIsoOrNull:23` calls `symbol:src/store/postgresStore.ts:toIso:17` (high)
- `symbol:src/toolpacks/backendSelection.ts:hasUsableSlurmDefaults:12` calls `symbol:src/mcp/gatewayServer.ts:snapshot:343` (medium)
- `symbol:src/toolpacks/backendSelection.ts:resolveBackend:26` calls `symbol:src/toolpacks/backendSelection.ts:hasUsableSlurmDefaults:12` (high)
- `symbol:src/toolpacks/backendSelection.ts:resolveBackend:26` calls `symbol:src/toolpacks/backendSelection.ts:isSbatchAvailable:6` (high)
- `symbol:src/toolpacks/backendSelection.ts:slurm:14` calls `symbol:src/mcp/gatewayServer.ts:snapshot:343` (medium)
- `symbol:src/toolpacks/builtin/fastqc.ts:backend:97` calls `symbol:src/toolpacks/backendSelection.ts:resolveBackend:26` (medium)
- `symbol:src/toolpacks/builtin/fastqc.ts:fastqcTool:82` calls `symbol:scripts/bundle_export.ts:outPath:51` (low)
- `symbol:src/toolpacks/builtin/fastqc.ts:fastqcTool:82` calls `symbol:src/execution/workspace.ts:createRunWorkspace:26` (medium)
- `symbol:src/toolpacks/builtin/fastqc.ts:fastqcTool:82` calls `symbol:src/mcp/gatewayServer.ts:snapshot:343` (medium)
- `symbol:src/toolpacks/builtin/fastqc.ts:fastqcTool:82` calls `symbol:src/toolpacks/backendSelection.ts:resolveBackend:26` (medium)
- `symbol:src/toolpacks/builtin/fastqc.ts:fastqcTool:82` calls `symbol:src/toolpacks/builtin/fastqc.ts:assertRegularFileNoSymlink:48` (high)
- `symbol:src/toolpacks/builtin/fastqc.ts:fastqcTool:82` calls `symbol:src/toolpacks/builtin/fastqc.ts:fastqcScript:54` (high)
- `symbol:src/toolpacks/builtin/fastqc.ts:fastqcTool:82` calls `symbol:src/toolpacks/builtin/fastqc.ts:parseFastqcSummaryText:21` (high)
- `symbol:src/toolpacks/builtin/fastqc.ts:fastqcTool:82` calls `symbol:src/toolpacks/docker/executeDocker.ts:executeDockerPlan:27` (medium)
- `symbol:src/toolpacks/builtin/fastqc.ts:fastqcTool:82` calls `symbol:src/toolpacks/slurm/executeSlurm.ts:executeSlurmPlan:19` (medium)
- `symbol:src/toolpacks/builtin/fastqc.ts:htmlPath:281` calls `symbol:scripts/bundle_export.ts:outPath:51` (low)
- `symbol:src/toolpacks/builtin/fastqc.ts:metricsParsed:312` calls `symbol:src/toolpacks/builtin/fastqc.ts:parseFastqcSummaryText:21` (high)
- `symbol:src/toolpacks/builtin/fastqc.ts:plan:125` calls `symbol:src/toolpacks/builtin/fastqc.ts:fastqcScript:54` (high)
- `symbol:src/toolpacks/builtin/fastqc.ts:plan:192` calls `symbol:src/toolpacks/builtin/fastqc.ts:fastqcScript:54` (high)
- `symbol:src/toolpacks/builtin/fastqc.ts:result:259` calls `symbol:src/toolpacks/docker/executeDocker.ts:executeDockerPlan:27` (medium)
- `symbol:src/toolpacks/builtin/fastqc.ts:slurm:113` calls `symbol:src/mcp/gatewayServer.ts:snapshot:343` (medium)
- `symbol:src/toolpacks/builtin/fastqc.ts:submit:240` calls `symbol:src/toolpacks/slurm/executeSlurm.ts:executeSlurmPlan:19` (medium)
- `symbol:src/toolpacks/builtin/fastqc.ts:summaryPath:283` calls `symbol:scripts/bundle_export.ts:outPath:51` (low)
- `symbol:src/toolpacks/builtin/fastqc.ts:ws:280` calls `symbol:src/execution/workspace.ts:createRunWorkspace:26` (medium)
- `symbol:src/toolpacks/builtin/fastqc.ts:zipPath:282` calls `symbol:scripts/bundle_export.ts:outPath:51` (low)
- `symbol:src/toolpacks/builtin/multiqc.ts:backend:74` calls `symbol:src/toolpacks/backendSelection.ts:resolveBackend:26` (medium)
- `symbol:src/toolpacks/builtin/multiqc.ts:generalStatsPath:297` calls `symbol:scripts/bundle_export.ts:outPath:51` (low)
- `symbol:src/toolpacks/builtin/multiqc.ts:htmlPath:291` calls `symbol:scripts/bundle_export.ts:outPath:51` (low)
- `symbol:src/toolpacks/builtin/multiqc.ts:inputsDigest:115` calls `symbol:src/core/canonicalJson.ts:sha256Prefixed:9` (medium)
- `symbol:src/toolpacks/builtin/multiqc.ts:inputsDigest:115` calls `symbol:src/core/canonicalJson.ts:stableJsonStringify:78` (medium)
- `symbol:src/toolpacks/builtin/multiqc.ts:multiqcTool:59` calls `symbol:scripts/bundle_export.ts:outPath:51` (low)
- `symbol:src/toolpacks/builtin/multiqc.ts:multiqcTool:59` calls `symbol:src/core/canonicalJson.ts:sha256Prefixed:9` (medium)
- `symbol:src/toolpacks/builtin/multiqc.ts:multiqcTool:59` calls `symbol:src/core/canonicalJson.ts:stableJsonStringify:78` (medium)
- `symbol:src/toolpacks/builtin/multiqc.ts:multiqcTool:59` calls `symbol:src/execution/executionService.ts:sort:59` (medium)
- `symbol:src/toolpacks/builtin/multiqc.ts:multiqcTool:59` calls `symbol:src/execution/workspace.ts:createRunWorkspace:26` (medium)
- `symbol:src/toolpacks/builtin/multiqc.ts:multiqcTool:59` calls `symbol:src/mcp/gatewayServer.ts:snapshot:343` (medium)
- `symbol:src/toolpacks/builtin/multiqc.ts:multiqcTool:59` calls `symbol:src/toolpacks/backendSelection.ts:resolveBackend:26` (medium)
- `symbol:src/toolpacks/builtin/multiqc.ts:multiqcTool:59` calls `symbol:src/toolpacks/builtin/multiqc.ts:assertRegularFileNoSymlink:20` (high)
- `symbol:src/toolpacks/builtin/multiqc.ts:multiqcTool:59` calls `symbol:src/toolpacks/builtin/multiqc.ts:multiqcScript:26` (high)
- `symbol:src/toolpacks/builtin/multiqc.ts:multiqcTool:59` calls `symbol:src/toolpacks/builtin/multiqc.ts:pad4:42` (high)
- `symbol:src/toolpacks/builtin/multiqc.ts:multiqcTool:59` calls `symbol:src/toolpacks/builtin/multiqc.ts:parseMultiqcGeneralStatsText:46` (high)
- `symbol:src/toolpacks/builtin/multiqc.ts:multiqcTool:59` calls `symbol:src/toolpacks/docker/executeDocker.ts:executeDockerPlan:27` (medium)
- `symbol:src/toolpacks/builtin/multiqc.ts:multiqcTool:59` calls `symbol:src/toolpacks/slurm/executeSlurm.ts:executeSlurmPlan:19` (medium)
- `symbol:src/toolpacks/builtin/multiqc.ts:parsed:328` calls `symbol:src/toolpacks/builtin/multiqc.ts:parseMultiqcGeneralStatsText:46` (high)
- `symbol:src/toolpacks/builtin/multiqc.ts:parseMultiqcGeneralStatsText:46` calls `symbol:src/artifacts/localObjectStore.ts:slice:82` (medium)
- `symbol:src/toolpacks/builtin/multiqc.ts:parseMultiqcGeneralStatsText:46` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/toolpacks/builtin/multiqc.ts:plan:138` calls `symbol:src/toolpacks/builtin/multiqc.ts:multiqcScript:26` (high)
- `symbol:src/toolpacks/builtin/multiqc.ts:plan:203` calls `symbol:src/toolpacks/builtin/multiqc.ts:multiqcScript:26` (high)
- `symbol:src/toolpacks/builtin/multiqc.ts:planInputs:130` calls `symbol:src/toolpacks/builtin/multiqc.ts:pad4:42` (high)
- `symbol:src/toolpacks/builtin/multiqc.ts:planInputs:196` calls `symbol:src/toolpacks/builtin/multiqc.ts:pad4:42` (high)
- `symbol:src/toolpacks/builtin/multiqc.ts:result:269` calls `symbol:src/toolpacks/docker/executeDocker.ts:executeDockerPlan:27` (medium)
- `symbol:src/toolpacks/builtin/multiqc.ts:samples:53` calls `symbol:src/policy/policy.ts:max:333` (medium)
- `symbol:src/toolpacks/builtin/multiqc.ts:slurm:118` calls `symbol:src/mcp/gatewayServer.ts:snapshot:343` (medium)
- `symbol:src/toolpacks/builtin/multiqc.ts:sorted:102` calls `symbol:src/execution/executionService.ts:sort:59` (medium)
- `symbol:src/toolpacks/builtin/multiqc.ts:submit:250` calls `symbol:src/toolpacks/slurm/executeSlurm.ts:executeSlurmPlan:19` (medium)
- `symbol:src/toolpacks/builtin/multiqc.ts:ws:290` calls `symbol:src/execution/workspace.ts:createRunWorkspace:26` (medium)
- `symbol:src/toolpacks/builtin/multiqc.ts:zipPath:292` calls `symbol:scripts/bundle_export.ts:outPath:51` (low)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:backend:305` calls `symbol:src/toolpacks/backendSelection.ts:resolveBackend:26` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:buildVirtualSlurmPlan:250` calls `symbol:src/mcp/gatewayServer.ts:snapshot:343` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:canonicalParams:477` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:multiqcOverFastqcTmpScript:81` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:ensureSlurmSubrunSubmitted:207` calls `symbol:src/mcp/envSnapshot.ts:envSnapshot:3` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:ensureSlurmSubrunSubmitted:207` calls `symbol:src/mcp/gatewayServer.ts:snapshot:343` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:fastqc1:606` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:parseFastqcSummaryText:29` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:fastqc2:607` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:parseFastqcSummaryText:29` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:fastqcOutcome:523` calls `symbol:src/toolpacks/docker/executeDocker.ts:executeDockerPlan:27` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:fq1:343` calls `symbol:src/runs/runIdentity.ts:deriveRunId:16` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:fq1:673` calls `symbol:src/runs/runIdentity.ts:deriveRunId:16` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:fq1Out:370` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:getFastqcOutputs:173` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:fq1Out:700` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:getFastqcOutputs:173` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:fq2id:358` calls `symbol:src/runs/runIdentity.ts:deriveRunId:16` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:fq2id:689` calls `symbol:src/runs/runIdentity.ts:deriveRunId:16` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:getFastqcOutputs:173` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:getRunOutputArtifactIdByRole:167` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:getMultiqcOutputs:179` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:getRunOutputArtifactIdByRole:167` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:graph:410` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:makeGraph:189` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:graph:476` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:makeGraph:189` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:graph:757` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:makeGraph:189` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:graph:839` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:makeGraph:189` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:graph:911` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:makeGraph:189` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:graphDigest:203` calls `symbol:src/core/canonicalJson.ts:sha256Prefixed:9` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:graphJson:202` calls `symbol:src/core/canonicalJson.ts:stableJsonStringify:78` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:html:174` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:getRunOutputArtifactIdByRole:167` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:html:180` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:getRunOutputArtifactIdByRole:167` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:htmlPath:568` calls `symbol:scripts/bundle_export.ts:outPath:51` (low)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:makeGraph:189` calls `symbol:src/core/canonicalJson.ts:sha256Prefixed:9` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:makeGraph:189` calls `symbol:src/core/canonicalJson.ts:stableJsonStringify:78` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:mq:395` calls `symbol:src/runs/runIdentity.ts:deriveRunId:16` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:mq:812` calls `symbol:src/runs/runIdentity.ts:deriveRunId:16` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:mq:893` calls `symbol:src/runs/runIdentity.ts:deriveRunId:16` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:mqOut:903` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:getMultiqcOutputs:179` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:multiqcOutcome:548` calls `symbol:src/toolpacks/docker/executeDocker.ts:executeDockerPlan:27` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:multiqcPlan:540` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:multiqcOverFastqcTmpScript:81` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:normalizeCanonicalParams:185` calls `symbol:src/core/canonicalJson.ts:stableJsonStringify:78` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:plan:465` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:fastqcBundleScript:65` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:qcBundleFastqTool:288` calls `symbol:scripts/bundle_export.ts:outPath:51` (low)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:qcBundleFastqTool:288` calls `symbol:src/execution/workspace.ts:createRunWorkspace:26` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:qcBundleFastqTool:288` calls `symbol:src/runs/runIdentity.ts:deriveRunId:16` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:qcBundleFastqTool:288` calls `symbol:src/toolpacks/backendSelection.ts:resolveBackend:26` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:qcBundleFastqTool:288` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:assertRegularFileNoSymlink:56` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:qcBundleFastqTool:288` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:buildVirtualSlurmPlan:250` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:qcBundleFastqTool:288` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:ensureSlurmSubrunSubmitted:207` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:qcBundleFastqTool:288` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:fastqcBundleScript:65` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:qcBundleFastqTool:288` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:getFastqcOutputs:173` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:qcBundleFastqTool:288` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:getMultiqcOutputs:179` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:qcBundleFastqTool:288` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:makeGraph:189` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:qcBundleFastqTool:288` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:multiqcOverFastqcTmpScript:81` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:qcBundleFastqTool:288` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:normalizeCanonicalParams:185` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:qcBundleFastqTool:288` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:parseFastqcSummaryText:29` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:qcBundleFastqTool:288` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:renderBundleMarkdown:97` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:qcBundleFastqTool:288` calls `symbol:src/toolpacks/docker/executeDocker.ts:executeDockerPlan:27` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:report:613` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:renderBundleMarkdown:97` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:report:758` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:renderBundleMarkdown:97` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:report:840` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:renderBundleMarkdown:97` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:report:912` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:renderBundleMarkdown:97` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:slurm:251` calls `symbol:src/mcp/gatewayServer.ts:snapshot:343` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:slurmPlan:432` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:buildVirtualSlurmPlan:250` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:summary1Path:570` calls `symbol:scripts/bundle_export.ts:outPath:51` (low)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:summary2Path:571` calls `symbol:scripts/bundle_export.ts:outPath:51` (low)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:toolRun:224` calls `symbol:src/mcp/envSnapshot.ts:envSnapshot:3` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:toolRun:224` calls `symbol:src/mcp/gatewayServer.ts:snapshot:343` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:ws:567` calls `symbol:src/execution/workspace.ts:createRunWorkspace:26` (medium)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:zip:175` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:getRunOutputArtifactIdByRole:167` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:zip:181` calls `symbol:src/toolpacks/builtin/qcBundleFastq.ts:getRunOutputArtifactIdByRole:167` (high)
- `symbol:src/toolpacks/builtin/qcBundleFastq.ts:zipPath:569` calls `symbol:scripts/bundle_export.ts:outPath:51` (low)
- `symbol:src/toolpacks/builtin/samtoolsFlagstat.ts:counts:83` calls `symbol:src/toolpacks/builtin/samtoolsFlagstat.ts:parseCounts:74` (high)
- `symbol:src/toolpacks/builtin/samtoolsFlagstat.ts:parseSamtoolsFlagstat:20` calls `symbol:src/toolpacks/builtin/samtoolsFlagstat.ts:parseCounts:74` (high)
- `symbol:src/toolpacks/builtin/samtoolsFlagstat.ts:result:293` calls `symbol:src/toolpacks/docker/executeDocker.ts:executeDockerPlan:27` (medium)
- `symbol:src/toolpacks/builtin/samtoolsFlagstat.ts:samtoolsFlagstatTool:106` calls `symbol:scripts/bundle_export.ts:outPath:51` (low)
- `symbol:src/toolpacks/builtin/samtoolsFlagstat.ts:samtoolsFlagstatTool:106` calls `symbol:src/execution/workspace.ts:createRunWorkspace:26` (medium)
- `symbol:src/toolpacks/builtin/samtoolsFlagstat.ts:samtoolsFlagstatTool:106` calls `symbol:src/mcp/gatewayServer.ts:snapshot:343` (medium)
- `symbol:src/toolpacks/builtin/samtoolsFlagstat.ts:samtoolsFlagstatTool:106` calls `symbol:src/toolpacks/builtin/samtoolsFlagstat.ts:parseSamtoolsFlagstat:20` (high)
- `symbol:src/toolpacks/builtin/samtoolsFlagstat.ts:samtoolsFlagstatTool:106` calls `symbol:src/toolpacks/docker/executeDocker.ts:executeDockerPlan:27` (medium)
- `symbol:src/toolpacks/builtin/samtoolsFlagstat.ts:samtoolsFlagstatTool:106` calls `symbol:src/toolpacks/slurm/executeSlurm.ts:executeSlurmPlan:19` (medium)
- `symbol:src/toolpacks/builtin/samtoolsFlagstat.ts:slurm:133` calls `symbol:src/mcp/gatewayServer.ts:snapshot:343` (medium)
- `symbol:src/toolpacks/builtin/samtoolsFlagstat.ts:submit:274` calls `symbol:src/toolpacks/slurm/executeSlurm.ts:executeSlurmPlan:19` (medium)
- `symbol:src/toolpacks/builtin/samtoolsFlagstat.ts:ws:314` calls `symbol:src/execution/workspace.ts:createRunWorkspace:26` (medium)
- `symbol:src/toolpacks/builtin/samtoolsFlagstat.ts:wsReportPath:315` calls `symbol:scripts/bundle_export.ts:outPath:51` (low)
- `symbol:src/toolpacks/builtin/samtoolsFlagstatSlurm.ts:samtoolsFlagstatSlurmTool:14` calls `symbol:src/mcp/gatewayServer.ts:snapshot:343` (medium)
- `symbol:src/toolpacks/builtin/samtoolsFlagstatSlurm.ts:samtoolsFlagstatSlurmTool:14` calls `symbol:src/toolpacks/slurm/executeSlurm.ts:executeSlurmPlan:19` (medium)
- `symbol:src/toolpacks/builtin/samtoolsFlagstatSlurm.ts:slurm:39` calls `symbol:src/mcp/gatewayServer.ts:snapshot:343` (medium)
- `symbol:src/toolpacks/builtin/samtoolsFlagstatSlurm.ts:submit:141` calls `symbol:src/toolpacks/slurm/executeSlurm.ts:executeSlurmPlan:19` (medium)
- `symbol:src/toolpacks/builtin/seqkitStats.ts:metrics:40` calls `symbol:src/toolpacks/builtin/seqkitStats.ts:numeric:33` (high)
- `symbol:src/toolpacks/builtin/seqkitStats.ts:parseSeqkitStatsTsv:14` calls `symbol:src/toolpacks/builtin/seqkitStats.ts:numeric:33` (high)
- `symbol:src/toolpacks/builtin/seqkitStats.ts:result:123` calls `symbol:src/toolpacks/docker/executeDocker.ts:executeDockerPlan:27` (medium)
- `symbol:src/toolpacks/builtin/seqkitStats.ts:seqkitStatsTool:57` calls `symbol:scripts/bundle_export.ts:outPath:51` (low)
- `symbol:src/toolpacks/builtin/seqkitStats.ts:seqkitStatsTool:57` calls `symbol:src/execution/workspace.ts:createRunWorkspace:26` (medium)
- `symbol:src/toolpacks/builtin/seqkitStats.ts:seqkitStatsTool:57` calls `symbol:src/toolpacks/builtin/seqkitStats.ts:parseSeqkitStatsTsv:14` (high)
- `symbol:src/toolpacks/builtin/seqkitStats.ts:seqkitStatsTool:57` calls `symbol:src/toolpacks/docker/executeDocker.ts:executeDockerPlan:27` (medium)
- `symbol:src/toolpacks/builtin/seqkitStats.ts:ws:143` calls `symbol:src/execution/workspace.ts:createRunWorkspace:26` (medium)
- `symbol:src/toolpacks/builtin/seqkitStats.ts:wsReportPath:144` calls `symbol:scripts/bundle_export.ts:outPath:51` (low)
- `symbol:src/toolpacks/docker/executeDocker.ts:executeDockerPlan:27` calls `symbol:src/execution/workspace.ts:createRunWorkspace:26` (medium)
- `symbol:src/toolpacks/docker/executeDocker.ts:ws:43` calls `symbol:src/execution/workspace.ts:createRunWorkspace:26` (medium)
- `symbol:src/toolpacks/register.ts:assertDeclaredOutputsSatisfied:156` calls `symbol:src/core/canonicalJson.ts:keys:60` (medium)
- `symbol:src/toolpacks/register.ts:assertDockerPlanInputsMatchLinkedInputs:203` calls `symbol:src/artifacts/localObjectStore.ts:slice:82` (medium)
- `symbol:src/toolpacks/register.ts:assertJsonSafe:36` calls `symbol:src/bundle/bundleVerify.ts:entries:38` (medium)
- `symbol:src/toolpacks/register.ts:assertJsonSafe:36` calls `symbol:src/toolpacks/register.ts:walk:39` (high)
- `symbol:src/toolpacks/register.ts:assertSlurmPlanInputsMatchLinkedInputs:293` calls `symbol:src/artifacts/localObjectStore.ts:slice:82` (medium)
- `symbol:src/toolpacks/register.ts:assertSlurmPlanOutputsMatchDeclaredOutputs:407` calls `symbol:src/core/canonicalJson.ts:keys:60` (medium)
- `symbol:src/toolpacks/register.ts:canonicalizeToolpackCanonicalParams:192` calls `symbol:src/core/canonicalJson.ts:stableJsonStringify:78` (medium)
- `symbol:src/toolpacks/register.ts:canonicalJson:193` calls `symbol:src/core/canonicalJson.ts:stableJsonStringify:78` (medium)
- `symbol:src/toolpacks/register.ts:planRoles:474` calls `symbol:src/core/canonicalJson.ts:keys:60` (medium)
- `symbol:src/toolpacks/register.ts:registerToolDefinitions:516` calls `symbol:src/mcp/envSnapshot.ts:envSnapshot:3` (medium)
- `symbol:src/toolpacks/register.ts:registerToolDefinitions:516` calls `symbol:src/mcp/gatewayServer.ts:snapshot:343` (medium)
- `symbol:src/toolpacks/register.ts:registerToolDefinitions:516` calls `symbol:src/runs/runIdentity.ts:deriveRunId:16` (medium)
- `symbol:src/toolpacks/register.ts:registerToolDefinitions:516` calls `symbol:src/runs/toolRun.ts:requestedByFromExtra:167` (medium)
- `symbol:src/toolpacks/register.ts:registerToolDefinitions:516` calls `symbol:src/toolpacks/register.ts:assertDeclaredOutputsSatisfied:156` (high)
- `symbol:src/toolpacks/register.ts:registerToolDefinitions:516` calls `symbol:src/toolpacks/register.ts:assertDockerPlanInputsMatchLinkedInputs:203` (high)
- `symbol:src/toolpacks/register.ts:registerToolDefinitions:516` calls `symbol:src/toolpacks/register.ts:assertJsonSafe:36` (high)
- `symbol:src/toolpacks/register.ts:registerToolDefinitions:516` calls `symbol:src/toolpacks/register.ts:assertSlurmPlanInputsMatchLinkedInputs:293` (high)
- `symbol:src/toolpacks/register.ts:registerToolDefinitions:516` calls `symbol:src/toolpacks/register.ts:assertSlurmPlanOutputsMatchDeclaredOutputs:407` (high)
- `symbol:src/toolpacks/register.ts:registerToolDefinitions:516` calls `symbol:src/toolpacks/register.ts:canonicalizeToolpackCanonicalParams:192` (high)
- `symbol:src/toolpacks/register.ts:registerToolDefinitions:516` calls `symbol:src/toolpacks/register.ts:validateToolDefinitions:76` (high)
- `symbol:src/toolpacks/register.ts:registerToolDefinitions:516` calls `symbol:tests/toolpacks.test.ts:run:141` (low)
- `symbol:src/toolpacks/register.ts:res:594` calls `symbol:tests/toolpacks.test.ts:run:141` (low)
- `symbol:src/toolpacks/register.ts:validateToolDefinitions:76` calls `symbol:src/store/postgresStore.ts:from:258` (medium)
- `symbol:src/toolpacks/register.ts:walk:39` calls `symbol:src/bundle/bundleVerify.ts:entries:38` (medium)
- `symbol:src/toolpacks/slurm/executeSlurm.ts:destPath:88` calls `symbol:src/execution/workspace.ts:safeJoin:15` (medium)
- `symbol:src/toolpacks/slurm/executeSlurm.ts:executeSlurmPlan:19` calls `symbol:src/execution/slurm/slurmScriptV1.ts:renderSlurmScriptV1:76` (medium)
- `symbol:src/toolpacks/slurm/executeSlurm.ts:executeSlurmPlan:19` calls `symbol:src/execution/workspace.ts:createRunWorkspace:26` (medium)
- `symbol:src/toolpacks/slurm/executeSlurm.ts:executeSlurmPlan:19` calls `symbol:src/execution/workspace.ts:safeJoin:15` (medium)
- `symbol:src/toolpacks/slurm/executeSlurm.ts:executeSlurmPlan:19` calls `symbol:src/toolpacks/slurm/executeSlurm.ts:submit:112` (high)
- `symbol:src/toolpacks/slurm/executeSlurm.ts:script:92` calls `symbol:src/execution/slurm/slurmScriptV1.ts:renderSlurmScriptV1:76` (medium)
- `symbol:src/toolpacks/slurm/executeSlurm.ts:ws:70` calls `symbol:src/execution/workspace.ts:createRunWorkspace:26` (medium)
- `symbol:tests/artifactService.test.ts:db:27` calls `symbol:src/db/connection.ts:createDb:9` (low)
- `symbol:tests/artifactService.test.ts:projectId:31` calls `symbol:src/core/ids.ts:newProjectId:13` (low)
- `symbol:tests/bundleExport.test.ts:db:33` calls `symbol:src/db/connection.ts:createDb:9` (low)
- `symbol:tests/bundleExport.test.ts:paramsHash:89` calls `symbol:src/core/canonicalJson.ts:sha256Prefixed:9` (low)
- `symbol:tests/bundleExport.test.ts:paramsHash:89` calls `symbol:src/core/canonicalJson.ts:stableJsonStringify:78` (low)
- `symbol:tests/bundleExport.test.ts:res1:115` calls `symbol:src/bundle/bundleExport.ts:exportBundleToDir:175` (low)
- `symbol:tests/bundleExport.test.ts:res2:119` calls `symbol:src/bundle/bundleExport.ts:exportBundleToDir:175` (low)
- `symbol:tests/contracts.test.ts:contractFiles:54` calls `symbol:tests/contracts.test.ts:listContractFiles:14` (high)
- `symbol:tests/contracts.test.ts:listContractFiles:14` calls `symbol:src/execution/executionService.ts:sort:59` (low)
- `symbol:tests/contracts.test.ts:policyTools:56` calls `symbol:src/mcp/gatewayServer.ts:snapshot:343` (low)
- `symbol:tests/contracts.test.ts:publishedKeys:76` calls `symbol:src/core/canonicalJson.ts:keys:60` (low)
- `symbol:tests/contracts.test.ts:publishedKeys:76` calls `symbol:src/execution/executionService.ts:sort:59` (low)
- `symbol:tests/contracts.test.ts:runtimeKeys:77` calls `symbol:src/core/canonicalJson.ts:keys:60` (low)
- `symbol:tests/contracts.test.ts:runtimeKeys:77` calls `symbol:src/execution/executionService.ts:sort:59` (low)
- `symbol:tests/contracts.test.ts:schema:48` calls `symbol:tests/contracts.test.ts:readJson:10` (high)
- `symbol:tests/contracts.test.ts:schema:75` calls `symbol:tests/contracts.test.ts:readJson:10` (high)
- `symbol:tests/contracts.test.ts:schemaPaths:31` calls `symbol:tests/contracts.test.ts:listContractFiles:14` (high)
- `symbol:tests/contracts.test.ts:schemas:38` calls `symbol:tests/contracts.test.ts:readJson:10` (high)
- `symbol:tests/gateway.integration.test.ts:bundle1:501` calls `symbol:tests/gateway.integration.test.ts:callTool:51` (high)
- `symbol:tests/gateway.integration.test.ts:bundle2:521` calls `symbol:tests/gateway.integration.test.ts:callTool:51` (high)
- `symbol:tests/gateway.integration.test.ts:db:67` calls `symbol:src/db/connection.ts:createDb:9` (low)
- `symbol:tests/gateway.integration.test.ts:fastqc1:415` calls `symbol:tests/gateway.integration.test.ts:callTool:51` (high)
- `symbol:tests/gateway.integration.test.ts:fastqc2:431` calls `symbol:tests/gateway.integration.test.ts:callTool:51` (high)
- `symbol:tests/gateway.integration.test.ts:flag1:604` calls `symbol:tests/gateway.integration.test.ts:callTool:51` (high)
- `symbol:tests/gateway.integration.test.ts:flag2:636` calls `symbol:tests/gateway.integration.test.ts:callTool:51` (high)
- `symbol:tests/gateway.integration.test.ts:imported:398` calls `symbol:tests/gateway.integration.test.ts:callTool:51` (high)
- `symbol:tests/gateway.integration.test.ts:imported:484` calls `symbol:tests/gateway.integration.test.ts:callTool:51` (high)
- `symbol:tests/gateway.integration.test.ts:imported:587` calls `symbol:tests/gateway.integration.test.ts:callTool:51` (high)
- `symbol:tests/gateway.integration.test.ts:multiqc1:445` calls `symbol:tests/gateway.integration.test.ts:callTool:51` (high)
- `symbol:tests/gateway.integration.test.ts:multiqc2:458` calls `symbol:tests/gateway.integration.test.ts:callTool:51` (high)
- `symbol:tests/gateway.integration.test.ts:projectId:144` calls `symbol:src/core/ids.ts:newProjectId:13` (low)
- `symbol:tests/gateway.integration.test.ts:projectId:235` calls `symbol:src/core/ids.ts:newProjectId:13` (low)
- `symbol:tests/gateway.integration.test.ts:projectId:299` calls `symbol:src/core/ids.ts:newProjectId:13` (low)
- `symbol:tests/gateway.integration.test.ts:projectId:392` calls `symbol:src/core/ids.ts:newProjectId:13` (low)
- `symbol:tests/gateway.integration.test.ts:projectId:478` calls `symbol:src/core/ids.ts:newProjectId:13` (low)
- `symbol:tests/gateway.integration.test.ts:projectId:544` calls `symbol:src/core/ids.ts:newProjectId:13` (low)
- `symbol:tests/gateway.integration.test.ts:projectId:659` calls `symbol:src/core/ids.ts:newProjectId:13` (low)
- `symbol:tests/gateway.integration.test.ts:projectId:723` calls `symbol:src/core/ids.ts:newProjectId:13` (low)
- `symbol:tests/gateway.integration.test.ts:projectId:99` calls `symbol:src/core/ids.ts:newProjectId:13` (low)
- `symbol:tests/runLifecycle.test.ts:createRun:34` calls `symbol:src/core/ids.ts:newProjectId:13` (low)
- `symbol:tests/runLifecycle.test.ts:createRun:34` calls `symbol:src/runs/runIdentity.ts:deriveRunId:16` (low)
- `symbol:tests/runLifecycle.test.ts:projectId:35` calls `symbol:src/core/ids.ts:newProjectId:13` (low)
- `symbol:tests/runLifecycle.test.ts:runningRunId:83` calls `symbol:tests/runLifecycle.test.ts:createRun:34` (high)
- `symbol:tests/runLifecycle.test.ts:succeededRunId:93` calls `symbol:tests/runLifecycle.test.ts:createRun:34` (high)
- `symbol:tests/runLifecycle.test.ts:withStore:21` calls `symbol:src/db/bootstrap.ts:applySqlFile:4` (low)
- `symbol:tests/runLifecycle.test.ts:withStore:21` calls `symbol:src/db/connection.ts:createDb:9` (low)
- `symbol:tests/slurm.integration.test.ts:db:52` calls `symbol:src/db/connection.ts:createDb:9` (low)
- `symbol:tests/slurm.integration.test.ts:projectId:149` calls `symbol:src/core/ids.ts:newProjectId:13` (low)
- `symbol:tests/slurm.integration.test.ts:projectId:303` calls `symbol:src/core/ids.ts:newProjectId:13` (low)
- `symbol:tests/slurm.integration.test.ts:roles:350` calls `symbol:src/execution/executionService.ts:sort:59` (low)
- `symbol:tests/slurm.integration.test.ts:server:120` calls `symbol:src/mcp/gatewayServer.ts:createGatewayServer:76` (low)
- `symbol:tests/toolpacks.test.ts:db:164` calls `symbol:src/db/connection.ts:createDb:9` (low)
- `symbol:tests/toolpacks.test.ts:db:280` calls `symbol:src/db/connection.ts:createDb:9` (low)
- `symbol:tests/toolpacks.test.ts:db:475` calls `symbol:src/db/connection.ts:createDb:9` (low)
- `symbol:tests/toolpacks.test.ts:db:637` calls `symbol:src/db/connection.ts:createDb:9` (low)
- `symbol:tests/toolpacks.test.ts:db:68` calls `symbol:src/db/connection.ts:createDb:9` (low)
- `symbol:tests/toolpacks.test.ts:db:774` calls `symbol:src/db/connection.ts:createDb:9` (low)
- `symbol:tests/toolpacks.test.ts:db:869` calls `symbol:src/db/connection.ts:createDb:9` (low)
- `symbol:tests/toolpacks.test.ts:hybridTool:527` calls `symbol:src/toolpacks/slurm/executeSlurm.ts:executeSlurmPlan:19` (low)
- `symbol:tests/toolpacks.test.ts:r1:839` calls `symbol:src/runs/runIdentity.ts:deriveRunId:16` (low)
- `symbol:tests/toolpacks.test.ts:r2:845` calls `symbol:src/runs/runIdentity.ts:deriveRunId:16` (low)
- `symbol:tests/toolpacks.test.ts:slurmTool:332` calls `symbol:src/toolpacks/slurm/executeSlurm.ts:executeSlurmPlan:19` (low)
- `symbol:tests/toolpacks.test.ts:submit:399` calls `symbol:src/toolpacks/slurm/executeSlurm.ts:executeSlurmPlan:19` (low)
- `symbol:tests/toolpacks.test.ts:submit:568` calls `symbol:src/toolpacks/slurm/executeSlurm.ts:executeSlurmPlan:19` (low)
- `component:docs` documents `component:external:node:@esbuild/aix-ppc64` (medium)
- `component:docs` documents `component:external:node:@esbuild/android-arm` (medium)
- `component:docs` documents `component:external:node:@esbuild/android-arm64` (medium)
- `component:docs` documents `component:external:node:@esbuild/android-x64` (medium)
- `component:docs` documents `component:external:node:@esbuild/darwin-arm64` (medium)
- `component:docs` documents `component:external:node:@esbuild/darwin-x64` (medium)
- `component:docs` documents `component:external:node:@esbuild/freebsd-arm64` (medium)
- `component:docs` documents `component:external:node:@esbuild/freebsd-x64` (medium)
- `component:docs` documents `component:external:node:@esbuild/linux-arm` (medium)
- `component:docs` documents `component:external:node:@esbuild/linux-arm64` (medium)
- `component:docs` documents `component:external:node:@esbuild/linux-ia32` (medium)
- `component:docs` documents `component:external:node:@esbuild/linux-loong64` (medium)
- `component:docs` documents `component:external:node:@esbuild/linux-mips64el` (medium)
- `component:docs` documents `component:external:node:@esbuild/linux-ppc64` (medium)
- `component:docs` documents `component:external:node:@esbuild/linux-riscv64` (medium)
- `component:docs` documents `component:external:node:@esbuild/linux-s390x` (medium)
- `component:docs` documents `component:external:node:@esbuild/linux-x64` (medium)
- `component:docs` documents `component:external:node:@esbuild/netbsd-arm64` (medium)
- `component:docs` documents `component:external:node:@esbuild/netbsd-x64` (medium)
- `component:docs` documents `component:external:node:@esbuild/openbsd-arm64` (medium)
- `component:docs` documents `component:external:node:@esbuild/openbsd-x64` (medium)
- `component:docs` documents `component:external:node:@esbuild/openharmony-arm64` (medium)
- `component:docs` documents `component:external:node:@esbuild/sunos-x64` (medium)
- `component:docs` documents `component:external:node:@esbuild/win32-arm64` (medium)
- `component:docs` documents `component:external:node:@esbuild/win32-ia32` (medium)
- `component:docs` documents `component:external:node:@esbuild/win32-x64` (medium)
- `component:docs` documents `component:external:node:@hono/node-server` (medium)
- `component:docs` documents `component:external:node:@jridgewell/sourcemap-codec` (medium)
- `component:docs` documents `component:external:node:@modelcontextprotocol/sdk` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-android-arm-eabi` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-android-arm64` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-darwin-arm64` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-darwin-x64` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-freebsd-arm64` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-freebsd-x64` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-linux-arm-gnueabihf` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-linux-arm-musleabihf` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-linux-arm64-gnu` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-linux-arm64-musl` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-linux-loong64-gnu` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-linux-loong64-musl` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-linux-ppc64-gnu` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-linux-ppc64-musl` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-linux-riscv64-gnu` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-linux-riscv64-musl` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-linux-s390x-gnu` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-linux-x64-gnu` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-linux-x64-musl` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-openbsd-x64` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-openharmony-arm64` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-win32-arm64-msvc` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-win32-ia32-msvc` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-win32-x64-gnu` (medium)
- `component:docs` documents `component:external:node:@rollup/rollup-win32-x64-msvc` (medium)
- `component:docs` documents `component:external:node:@standard-schema/spec` (medium)
- `component:docs` documents `component:external:node:@types/chai` (medium)
- `component:docs` documents `component:external:node:@types/deep-eql` (medium)
- `component:docs` documents `component:external:node:@types/estree` (medium)
- `component:docs` documents `component:external:node:@types/node` (medium)
- `component:docs` documents `component:external:node:@types/pg` (medium)
- `component:docs` documents `component:external:node:@vitest/expect` (medium)
- `component:docs` documents `component:external:node:@vitest/mocker` (medium)
- `component:docs` documents `component:external:node:@vitest/pretty-format` (medium)
- `component:docs` documents `component:external:node:@vitest/runner` (medium)
- `component:docs` documents `component:external:node:@vitest/snapshot` (medium)
- `component:docs` documents `component:external:node:@vitest/spy` (medium)
- `component:docs` documents `component:external:node:@vitest/utils` (medium)
- `component:docs` documents `component:external:node:accepts` (medium)
- `component:docs` documents `component:external:node:ajv` (medium)
- `component:docs` documents `component:external:node:ajv-formats` (medium)
- `component:docs` documents `component:external:node:assertion-error` (medium)
- `component:docs` documents `component:external:node:body-parser` (medium)
- `component:docs` documents `component:external:node:bytes` (medium)
- `component:docs` documents `component:external:node:call-bind` (medium)
- `component:docs` documents `component:external:node:call-bind-apply-helpers` (medium)
- `component:docs` documents `component:external:node:call-bound` (medium)
- `component:docs` documents `component:external:node:chai` (medium)
- `component:docs` documents `component:external:node:commander` (medium)
- `component:docs` documents `component:external:node:content-disposition` (medium)
- `component:docs` documents `component:external:node:content-type` (medium)
- `component:docs` documents `component:external:node:cookie` (medium)
- `component:docs` documents `component:external:node:cookie-signature` (medium)
- `component:docs` documents `component:external:node:cors` (medium)
- `component:docs` documents `component:external:node:cross-spawn` (medium)
- `component:docs` documents `component:external:node:debug` (medium)
- `component:docs` documents `component:external:node:define-data-property` (medium)
- `component:docs` documents `component:external:node:depd` (medium)
- `component:docs` documents `component:external:node:discontinuous-range` (medium)
- `component:docs` documents `component:external:node:dunder-proto` (medium)
- `component:docs` documents `component:external:node:ee-first` (medium)
- `component:docs` documents `component:external:node:encodeurl` (medium)
- `component:docs` documents `component:external:node:es-define-property` (medium)
- `component:docs` documents `component:external:node:es-errors` (medium)
- `component:docs` documents `component:external:node:es-module-lexer` (medium)
- `component:docs` documents `component:external:node:es-object-atoms` (medium)
- `component:docs` documents `component:external:node:esbuild` (medium)
- `component:docs` documents `component:external:node:escape-html` (medium)
- `component:docs` documents `component:external:node:estree-walker` (medium)
- `component:docs` documents `component:external:node:etag` (medium)
- `component:docs` documents `component:external:node:eventsource` (medium)
- `component:docs` documents `component:external:node:eventsource-parser` (medium)
- `component:docs` documents `component:external:node:expect-type` (medium)
- `component:docs` documents `component:external:node:express` (medium)
- `component:docs` documents `component:external:node:express-rate-limit` (medium)
- `component:docs` documents `component:external:node:fast-deep-equal` (medium)
- `component:docs` documents `component:external:node:fast-uri` (medium)
- `component:docs` documents `component:external:node:fdir` (medium)
- `component:docs` documents `component:external:node:finalhandler` (medium)
- `component:docs` documents `component:external:node:forwarded` (medium)
- `component:docs` documents `component:external:node:fresh` (medium)
- `component:docs` documents `component:external:node:fsevents` (medium)
- `component:docs` documents `component:external:node:function-bind` (medium)
- `component:docs` documents `component:external:node:functional-red-black-tree` (medium)
- `component:docs` documents `component:external:node:get-intrinsic` (medium)
- `component:docs` documents `component:external:node:get-proto` (medium)
- `component:docs` documents `component:external:node:get-tsconfig` (medium)
- `component:docs` documents `component:external:node:gopd` (medium)
- `component:docs` documents `component:external:node:has-property-descriptors` (medium)
- `component:docs` documents `component:external:node:has-symbols` (medium)
- `component:docs` documents `component:external:node:hasown` (medium)
- `component:docs` documents `component:external:node:hono` (medium)
- `component:docs` documents `component:external:node:http-errors` (medium)
- `component:docs` documents `component:external:node:iconv-lite` (medium)
- `component:docs` documents `component:external:node:immutable` (medium)
- `component:docs` documents `component:external:node:inherits` (medium)
- `component:docs` documents `component:external:node:ipaddr.js` (medium)
- `component:docs` documents `component:external:node:is-promise` (medium)
- `component:docs` documents `component:external:node:isarray` (medium)
- `component:docs` documents `component:external:node:isexe` (medium)
- `component:docs` documents `component:external:node:jose` (medium)
- `component:docs` documents `component:external:node:json-schema-traverse` (medium)
- `component:docs` documents `component:external:node:json-schema-typed` (medium)
- `component:docs` documents `component:external:node:json-stable-stringify` (medium)
- `component:docs` documents `component:external:node:jsonify` (medium)
- `component:docs` documents `component:external:node:kysely` (medium)
- `component:docs` documents `component:external:node:lru-cache` (medium)
- `component:docs` documents `component:external:node:magic-string` (medium)
- `component:docs` documents `component:external:node:math-intrinsics` (medium)
- `component:docs` documents `component:external:node:media-typer` (medium)
- `component:docs` documents `component:external:node:merge-descriptors` (medium)
- `component:docs` documents `component:external:node:mime-db` (medium)
- `component:docs` documents `component:external:node:mime-types` (medium)
- `component:docs` documents `component:external:node:moment` (medium)
- `component:docs` documents `component:external:node:moo` (medium)
- `component:docs` documents `component:external:node:ms` (medium)
- `component:docs` documents `component:external:node:nanoid` (medium)
- `component:docs` documents `component:external:node:nearley` (medium)
- `component:docs` documents `component:external:node:negotiator` (medium)
- `component:docs` documents `component:external:node:object-assign` (medium)
- `component:docs` documents `component:external:node:object-hash` (medium)
- `component:docs` documents `component:external:node:object-inspect` (medium)
- `component:docs` documents `component:external:node:object-keys` (medium)
- `component:docs` documents `component:external:node:obug` (medium)
- `component:docs` documents `component:external:node:on-finished` (medium)
- `component:docs` documents `component:external:node:once` (medium)
- `component:docs` documents `component:external:node:parseurl` (medium)
- `component:docs` documents `component:external:node:path-key` (medium)
- `component:docs` documents `component:external:node:path-to-regexp` (medium)
- `component:docs` documents `component:external:node:pathe` (medium)
- `component:docs` documents `component:external:node:pg` (medium)
- `component:docs` documents `component:external:node:pg-cloudflare` (medium)
- `component:docs` documents `component:external:node:pg-connection-string` (medium)
- `component:docs` documents `component:external:node:pg-int8` (medium)
- `component:docs` documents `component:external:node:pg-mem` (medium)
- `component:docs` documents `component:external:node:pg-pool` (medium)
- `component:docs` documents `component:external:node:pg-protocol` (medium)
- `component:docs` documents `component:external:node:pg-types` (medium)
- `component:docs` documents `component:external:node:pgpass` (medium)
- `component:docs` documents `component:external:node:pgsql-ast-parser` (medium)
- `component:docs` documents `component:external:node:picocolors` (medium)
- `component:docs` documents `component:external:node:picomatch` (medium)
- `component:docs` documents `component:external:node:pkce-challenge` (medium)
- `component:docs` documents `component:external:node:postcss` (medium)
- `component:docs` documents `component:external:node:postgres-array` (medium)
- `component:docs` documents `component:external:node:postgres-bytea` (medium)
- `component:docs` documents `component:external:node:postgres-date` (medium)
- `component:docs` documents `component:external:node:postgres-interval` (medium)
- `component:docs` documents `component:external:node:proxy-addr` (medium)
- `component:docs` documents `component:external:node:qs` (medium)
- `component:docs` documents `component:external:node:railroad-diagrams` (medium)
- `component:docs` documents `component:external:node:randexp` (medium)
- `component:docs` documents `component:external:node:range-parser` (medium)
- `component:docs` documents `component:external:node:raw-body` (medium)
- `component:docs` documents `component:external:node:require-from-string` (medium)
- `component:docs` documents `component:external:node:resolve-pkg-maps` (medium)
- `component:docs` documents `component:external:node:ret` (medium)
- `component:docs` documents `component:external:node:rollup` (medium)
- `component:docs` documents `component:external:node:router` (medium)
- `component:docs` documents `component:external:node:safer-buffer` (medium)
- `component:docs` documents `component:external:node:send` (medium)
- `component:docs` documents `component:external:node:serve-static` (medium)
- `component:docs` documents `component:external:node:set-function-length` (medium)
- `component:docs` documents `component:external:node:setprototypeof` (medium)
- `component:docs` documents `component:external:node:shebang-command` (medium)
- `component:docs` documents `component:external:node:shebang-regex` (medium)
- `component:docs` documents `component:external:node:side-channel` (medium)
- `component:docs` documents `component:external:node:side-channel-list` (medium)
- `component:docs` documents `component:external:node:side-channel-map` (medium)
- `component:docs` documents `component:external:node:side-channel-weakmap` (medium)
- `component:docs` documents `component:external:node:siginfo` (medium)
- `component:docs` documents `component:external:node:source-map-js` (medium)
- `component:docs` documents `component:external:node:split2` (medium)
- `component:docs` documents `component:external:node:stackback` (medium)
- `component:docs` documents `component:external:node:statuses` (medium)
- `component:docs` documents `component:external:node:std-env` (medium)
- `component:docs` documents `component:external:node:tinybench` (medium)
- `component:docs` documents `component:external:node:tinyexec` (medium)
- `component:docs` documents `component:external:node:tinyglobby` (medium)
- `component:docs` documents `component:external:node:tinyrainbow` (medium)
- `component:docs` documents `component:external:node:toidentifier` (medium)
- `component:docs` documents `component:external:node:tsx` (medium)
- `component:docs` documents `component:external:node:type-is` (medium)
- `component:docs` documents `component:external:node:typescript` (medium)
- `component:docs` documents `component:external:node:ulid` (medium)
- `component:docs` documents `component:external:node:undici-types` (medium)
- `component:docs` documents `component:external:node:unpipe` (medium)
- `component:docs` documents `component:external:node:vary` (medium)
- `component:docs` documents `component:external:node:vite` (medium)
- `component:docs` documents `component:external:node:vitest` (medium)
- `component:docs` documents `component:external:node:which` (medium)
- `component:docs` documents `component:external:node:why-is-node-running` (medium)
- `component:docs` documents `component:external:node:wrappy` (medium)
- `component:docs` documents `component:external:node:xtend` (medium)
- `component:docs` documents `component:external:node:yallist` (medium)
- `component:docs` documents `component:external:node:yaml` (medium)
- `component:docs` documents `component:external:node:zod` (medium)
- `component:docs` documents `component:external:node:zod-to-json-schema` (medium)
- `component:docs` documents `component:package.json` (medium)
- `component:docs` documents `component:src` (medium)
- `component:docs` documents `component:tests` (medium)
- `repository` contains `component:external:node:@esbuild/aix-ppc64` (high)
- `repository` contains `component:external:node:@esbuild/android-arm` (high)
- `repository` contains `component:external:node:@esbuild/android-arm64` (high)
- `repository` contains `component:external:node:@esbuild/android-x64` (high)
- `repository` contains `component:external:node:@esbuild/darwin-arm64` (high)
- `repository` contains `component:external:node:@esbuild/darwin-x64` (high)
- `repository` contains `component:external:node:@esbuild/freebsd-arm64` (high)
- `repository` contains `component:external:node:@esbuild/freebsd-x64` (high)
- `repository` contains `component:external:node:@esbuild/linux-arm` (high)
- `repository` contains `component:external:node:@esbuild/linux-arm64` (high)
- `repository` contains `component:external:node:@esbuild/linux-ia32` (high)
- `repository` contains `component:external:node:@esbuild/linux-loong64` (high)
- `repository` contains `component:external:node:@esbuild/linux-mips64el` (high)
- `repository` contains `component:external:node:@esbuild/linux-ppc64` (high)
- `repository` contains `component:external:node:@esbuild/linux-riscv64` (high)
- `repository` contains `component:external:node:@esbuild/linux-s390x` (high)
- `repository` contains `component:external:node:@esbuild/linux-x64` (high)
- `repository` contains `component:external:node:@esbuild/netbsd-arm64` (high)
- `repository` contains `component:external:node:@esbuild/netbsd-x64` (high)
- `repository` contains `component:external:node:@esbuild/openbsd-arm64` (high)
- `repository` contains `component:external:node:@esbuild/openbsd-x64` (high)
- `repository` contains `component:external:node:@esbuild/openharmony-arm64` (high)
- `repository` contains `component:external:node:@esbuild/sunos-x64` (high)
- `repository` contains `component:external:node:@esbuild/win32-arm64` (high)
- `repository` contains `component:external:node:@esbuild/win32-ia32` (high)
- `repository` contains `component:external:node:@esbuild/win32-x64` (high)
- `repository` contains `component:external:node:@hono/node-server` (high)
- `repository` contains `component:external:node:@jridgewell/sourcemap-codec` (high)
- `component:external:node:@modelcontextprotocol/sdk` depends_on `component:external:node:@hono/node-server` (medium)
- `component:external:node:@modelcontextprotocol/sdk` depends_on `component:external:node:ajv-formats` (medium)
- `component:external:node:@modelcontextprotocol/sdk` depends_on `component:external:node:ajv` (medium)
- `component:external:node:@modelcontextprotocol/sdk` depends_on `component:external:node:content-type` (medium)
- `component:external:node:@modelcontextprotocol/sdk` depends_on `component:external:node:cors` (medium)
- `component:external:node:@modelcontextprotocol/sdk` depends_on `component:external:node:cross-spawn` (medium)
- `component:external:node:@modelcontextprotocol/sdk` depends_on `component:external:node:eventsource-parser` (medium)
- `component:external:node:@modelcontextprotocol/sdk` depends_on `component:external:node:eventsource` (medium)
- `component:external:node:@modelcontextprotocol/sdk` depends_on `component:external:node:express-rate-limit` (medium)
- `component:external:node:@modelcontextprotocol/sdk` depends_on `component:external:node:express` (medium)
- `component:external:node:@modelcontextprotocol/sdk` depends_on `component:external:node:jose` (medium)
- `component:external:node:@modelcontextprotocol/sdk` depends_on `component:external:node:json-schema-typed` (medium)
- `component:external:node:@modelcontextprotocol/sdk` depends_on `component:external:node:pkce-challenge` (medium)
- `component:external:node:@modelcontextprotocol/sdk` depends_on `component:external:node:raw-body` (medium)
- `component:external:node:@modelcontextprotocol/sdk` depends_on `component:external:node:zod-to-json-schema` (medium)
- `component:external:node:@modelcontextprotocol/sdk` depends_on `component:external:node:zod` (medium)
- `repository` contains `component:external:node:@modelcontextprotocol/sdk` (high)
- `repository` contains `component:external:node:@rollup/rollup-android-arm-eabi` (high)
- `repository` contains `component:external:node:@rollup/rollup-android-arm64` (high)
- `repository` contains `component:external:node:@rollup/rollup-darwin-arm64` (high)
- `repository` contains `component:external:node:@rollup/rollup-darwin-x64` (high)
- `repository` contains `component:external:node:@rollup/rollup-freebsd-arm64` (high)
- `repository` contains `component:external:node:@rollup/rollup-freebsd-x64` (high)
- `repository` contains `component:external:node:@rollup/rollup-linux-arm-gnueabihf` (high)
- `repository` contains `component:external:node:@rollup/rollup-linux-arm-musleabihf` (high)
- `repository` contains `component:external:node:@rollup/rollup-linux-arm64-gnu` (high)
- `repository` contains `component:external:node:@rollup/rollup-linux-arm64-musl` (high)
- `repository` contains `component:external:node:@rollup/rollup-linux-loong64-gnu` (high)
- `repository` contains `component:external:node:@rollup/rollup-linux-loong64-musl` (high)
- `repository` contains `component:external:node:@rollup/rollup-linux-ppc64-gnu` (high)
- `repository` contains `component:external:node:@rollup/rollup-linux-ppc64-musl` (high)
- `repository` contains `component:external:node:@rollup/rollup-linux-riscv64-gnu` (high)
- `repository` contains `component:external:node:@rollup/rollup-linux-riscv64-musl` (high)
- `repository` contains `component:external:node:@rollup/rollup-linux-s390x-gnu` (high)
- `repository` contains `component:external:node:@rollup/rollup-linux-x64-gnu` (high)
- `repository` contains `component:external:node:@rollup/rollup-linux-x64-musl` (high)
- `repository` contains `component:external:node:@rollup/rollup-openbsd-x64` (high)
- `repository` contains `component:external:node:@rollup/rollup-openharmony-arm64` (high)
- `repository` contains `component:external:node:@rollup/rollup-win32-arm64-msvc` (high)
- `repository` contains `component:external:node:@rollup/rollup-win32-ia32-msvc` (high)
- `repository` contains `component:external:node:@rollup/rollup-win32-x64-gnu` (high)
- `repository` contains `component:external:node:@rollup/rollup-win32-x64-msvc` (high)
- `repository` contains `component:external:node:@standard-schema/spec` (high)
- `component:external:node:@types/chai` depends_on `component:external:node:@types/deep-eql` (medium)
- `component:external:node:@types/chai` depends_on `component:external:node:assertion-error` (medium)
- `repository` contains `component:external:node:@types/chai` (high)
- `repository` contains `component:external:node:@types/deep-eql` (high)
- `repository` contains `component:external:node:@types/estree` (high)
- `component:external:node:@types/node` depends_on `component:external:node:undici-types` (medium)
- `repository` contains `component:external:node:@types/node` (high)
- `component:external:node:@types/pg` depends_on `component:external:node:@types/node` (medium)
- `component:external:node:@types/pg` depends_on `component:external:node:pg-protocol` (medium)
- `component:external:node:@types/pg` depends_on `component:external:node:pg-types` (medium)
- `repository` contains `component:external:node:@types/pg` (high)
- `component:external:node:@vitest/expect` depends_on `component:external:node:@standard-schema/spec` (medium)
- `component:external:node:@vitest/expect` depends_on `component:external:node:@types/chai` (medium)
- `component:external:node:@vitest/expect` depends_on `component:external:node:@vitest/spy` (medium)
- `component:external:node:@vitest/expect` depends_on `component:external:node:@vitest/utils` (medium)
- `component:external:node:@vitest/expect` depends_on `component:external:node:chai` (medium)
- `component:external:node:@vitest/expect` depends_on `component:external:node:tinyrainbow` (medium)
- `repository` contains `component:external:node:@vitest/expect` (high)
- `component:external:node:@vitest/mocker` depends_on `component:external:node:@vitest/spy` (medium)
- `component:external:node:@vitest/mocker` depends_on `component:external:node:estree-walker` (medium)
- `component:external:node:@vitest/mocker` depends_on `component:external:node:magic-string` (medium)
- `repository` contains `component:external:node:@vitest/mocker` (high)
- `component:external:node:@vitest/pretty-format` depends_on `component:external:node:tinyrainbow` (medium)
- `repository` contains `component:external:node:@vitest/pretty-format` (high)
- `component:external:node:@vitest/runner` depends_on `component:external:node:@vitest/utils` (medium)
- `component:external:node:@vitest/runner` depends_on `component:external:node:pathe` (medium)
- `repository` contains `component:external:node:@vitest/runner` (high)
- `component:external:node:@vitest/snapshot` depends_on `component:external:node:@vitest/pretty-format` (medium)
- `component:external:node:@vitest/snapshot` depends_on `component:external:node:magic-string` (medium)
- `component:external:node:@vitest/snapshot` depends_on `component:external:node:pathe` (medium)
- `repository` contains `component:external:node:@vitest/snapshot` (high)
- `repository` contains `component:external:node:@vitest/spy` (high)
- `component:external:node:@vitest/utils` depends_on `component:external:node:@vitest/pretty-format` (medium)
- `component:external:node:@vitest/utils` depends_on `component:external:node:tinyrainbow` (medium)
- `repository` contains `component:external:node:@vitest/utils` (high)
- `component:external:node:accepts` depends_on `component:external:node:mime-types` (medium)
- `component:external:node:accepts` depends_on `component:external:node:negotiator` (medium)
- `repository` contains `component:external:node:accepts` (high)
- `component:external:node:ajv` depends_on `component:external:node:fast-deep-equal` (medium)
- `component:external:node:ajv` depends_on `component:external:node:fast-uri` (medium)
- `component:external:node:ajv` depends_on `component:external:node:json-schema-traverse` (medium)
- `component:external:node:ajv` depends_on `component:external:node:require-from-string` (medium)
- `repository` contains `component:external:node:ajv` (high)
- `component:external:node:ajv-formats` depends_on `component:external:node:ajv` (medium)
- `repository` contains `component:external:node:ajv-formats` (high)
- `repository` contains `component:external:node:assertion-error` (high)
- `component:external:node:body-parser` depends_on `component:external:node:bytes` (medium)
- `component:external:node:body-parser` depends_on `component:external:node:content-type` (medium)
- `component:external:node:body-parser` depends_on `component:external:node:debug` (medium)
- `component:external:node:body-parser` depends_on `component:external:node:http-errors` (medium)
- `component:external:node:body-parser` depends_on `component:external:node:iconv-lite` (medium)
- `component:external:node:body-parser` depends_on `component:external:node:on-finished` (medium)
- `component:external:node:body-parser` depends_on `component:external:node:qs` (medium)
- `component:external:node:body-parser` depends_on `component:external:node:raw-body` (medium)
- `component:external:node:body-parser` depends_on `component:external:node:type-is` (medium)
- `repository` contains `component:external:node:body-parser` (high)
- `repository` contains `component:external:node:bytes` (high)
- `component:external:node:call-bind` depends_on `component:external:node:call-bind-apply-helpers` (medium)
- `component:external:node:call-bind` depends_on `component:external:node:es-define-property` (medium)
- `component:external:node:call-bind` depends_on `component:external:node:get-intrinsic` (medium)
- `component:external:node:call-bind` depends_on `component:external:node:set-function-length` (medium)
- `repository` contains `component:external:node:call-bind` (high)
- `component:external:node:call-bind-apply-helpers` depends_on `component:external:node:es-errors` (medium)
- `component:external:node:call-bind-apply-helpers` depends_on `component:external:node:function-bind` (medium)
- `repository` contains `component:external:node:call-bind-apply-helpers` (high)
- `component:external:node:call-bound` depends_on `component:external:node:call-bind-apply-helpers` (medium)
- `component:external:node:call-bound` depends_on `component:external:node:get-intrinsic` (medium)
- `repository` contains `component:external:node:call-bound` (high)
- `repository` contains `component:external:node:chai` (high)
- `repository` contains `component:external:node:commander` (high)
- `repository` contains `component:external:node:content-disposition` (high)
- `repository` contains `component:external:node:content-type` (high)
- `repository` contains `component:external:node:cookie` (high)
- `repository` contains `component:external:node:cookie-signature` (high)
- `component:external:node:cors` depends_on `component:external:node:object-assign` (medium)
- `component:external:node:cors` depends_on `component:external:node:vary` (medium)
- `repository` contains `component:external:node:cors` (high)
- `component:external:node:cross-spawn` depends_on `component:external:node:path-key` (medium)
- `component:external:node:cross-spawn` depends_on `component:external:node:shebang-command` (medium)
- `component:external:node:cross-spawn` depends_on `component:external:node:which` (medium)
- `repository` contains `component:external:node:cross-spawn` (high)
- `component:external:node:debug` depends_on `component:external:node:ms` (medium)
- `repository` contains `component:external:node:debug` (high)
- `component:external:node:define-data-property` depends_on `component:external:node:es-define-property` (medium)
- `component:external:node:define-data-property` depends_on `component:external:node:es-errors` (medium)
- `component:external:node:define-data-property` depends_on `component:external:node:gopd` (medium)
- `repository` contains `component:external:node:define-data-property` (high)
- `repository` contains `component:external:node:depd` (high)
- `repository` contains `component:external:node:discontinuous-range` (high)
- `component:external:node:dunder-proto` depends_on `component:external:node:call-bind-apply-helpers` (medium)
- `component:external:node:dunder-proto` depends_on `component:external:node:es-errors` (medium)
- `component:external:node:dunder-proto` depends_on `component:external:node:gopd` (medium)
- `repository` contains `component:external:node:dunder-proto` (high)
- `repository` contains `component:external:node:ee-first` (high)
- `repository` contains `component:external:node:encodeurl` (high)
- `repository` contains `component:external:node:es-define-property` (high)
- `repository` contains `component:external:node:es-errors` (high)
- `repository` contains `component:external:node:es-module-lexer` (high)
- `component:external:node:es-object-atoms` depends_on `component:external:node:es-errors` (medium)
- `repository` contains `component:external:node:es-object-atoms` (high)
- `repository` contains `component:external:node:esbuild` (high)
- `repository` contains `component:external:node:escape-html` (high)
- `component:external:node:estree-walker` depends_on `component:external:node:@types/estree` (medium)
- `repository` contains `component:external:node:estree-walker` (high)
- `repository` contains `component:external:node:etag` (high)
- `component:external:node:eventsource` depends_on `component:external:node:eventsource-parser` (medium)
- `repository` contains `component:external:node:eventsource` (high)
- `repository` contains `component:external:node:eventsource-parser` (high)
- `repository` contains `component:external:node:expect-type` (high)
- `component:external:node:express` depends_on `component:external:node:accepts` (medium)
- `component:external:node:express` depends_on `component:external:node:body-parser` (medium)
- `component:external:node:express` depends_on `component:external:node:content-disposition` (medium)
- `component:external:node:express` depends_on `component:external:node:content-type` (medium)
- `component:external:node:express` depends_on `component:external:node:cookie-signature` (medium)
- `component:external:node:express` depends_on `component:external:node:cookie` (medium)
- `component:external:node:express` depends_on `component:external:node:debug` (medium)
- `component:external:node:express` depends_on `component:external:node:depd` (medium)
- `component:external:node:express` depends_on `component:external:node:encodeurl` (medium)
- `component:external:node:express` depends_on `component:external:node:escape-html` (medium)
- `component:external:node:express` depends_on `component:external:node:etag` (medium)
- `component:external:node:express` depends_on `component:external:node:finalhandler` (medium)
- `component:external:node:express` depends_on `component:external:node:fresh` (medium)
- `component:external:node:express` depends_on `component:external:node:http-errors` (medium)
- `component:external:node:express` depends_on `component:external:node:merge-descriptors` (medium)
- `component:external:node:express` depends_on `component:external:node:mime-types` (medium)
- `component:external:node:express` depends_on `component:external:node:on-finished` (medium)
- `component:external:node:express` depends_on `component:external:node:once` (medium)
- `component:external:node:express` depends_on `component:external:node:parseurl` (medium)
- `component:external:node:express` depends_on `component:external:node:proxy-addr` (medium)
- `component:external:node:express` depends_on `component:external:node:qs` (medium)
- `component:external:node:express` depends_on `component:external:node:range-parser` (medium)
- `component:external:node:express` depends_on `component:external:node:router` (medium)
- `component:external:node:express` depends_on `component:external:node:send` (medium)
- `component:external:node:express` depends_on `component:external:node:serve-static` (medium)
- `component:external:node:express` depends_on `component:external:node:statuses` (medium)
- `component:external:node:express` depends_on `component:external:node:type-is` (medium)
- `component:external:node:express` depends_on `component:external:node:vary` (medium)
- `repository` contains `component:external:node:express` (high)
- `repository` contains `component:external:node:express-rate-limit` (high)
- `repository` contains `component:external:node:fast-deep-equal` (high)
- `repository` contains `component:external:node:fast-uri` (high)
- `repository` contains `component:external:node:fdir` (high)
- `component:external:node:finalhandler` depends_on `component:external:node:debug` (medium)
- `component:external:node:finalhandler` depends_on `component:external:node:encodeurl` (medium)
- `component:external:node:finalhandler` depends_on `component:external:node:escape-html` (medium)
- `component:external:node:finalhandler` depends_on `component:external:node:on-finished` (medium)
- `component:external:node:finalhandler` depends_on `component:external:node:parseurl` (medium)
- `component:external:node:finalhandler` depends_on `component:external:node:statuses` (medium)
- `repository` contains `component:external:node:finalhandler` (high)
- `repository` contains `component:external:node:forwarded` (high)
- `repository` contains `component:external:node:fresh` (high)
- `repository` contains `component:external:node:fsevents` (high)
- `repository` contains `component:external:node:function-bind` (high)
- `repository` contains `component:external:node:functional-red-black-tree` (high)
- `component:external:node:get-intrinsic` depends_on `component:external:node:call-bind-apply-helpers` (medium)
- `component:external:node:get-intrinsic` depends_on `component:external:node:es-define-property` (medium)
- `component:external:node:get-intrinsic` depends_on `component:external:node:es-errors` (medium)
- `component:external:node:get-intrinsic` depends_on `component:external:node:es-object-atoms` (medium)
- `component:external:node:get-intrinsic` depends_on `component:external:node:function-bind` (medium)
- `component:external:node:get-intrinsic` depends_on `component:external:node:get-proto` (medium)
- `component:external:node:get-intrinsic` depends_on `component:external:node:gopd` (medium)
- `component:external:node:get-intrinsic` depends_on `component:external:node:has-symbols` (medium)
- `component:external:node:get-intrinsic` depends_on `component:external:node:hasown` (medium)
- `component:external:node:get-intrinsic` depends_on `component:external:node:math-intrinsics` (medium)
- `repository` contains `component:external:node:get-intrinsic` (high)
- `component:external:node:get-proto` depends_on `component:external:node:dunder-proto` (medium)
- `component:external:node:get-proto` depends_on `component:external:node:es-object-atoms` (medium)
- `repository` contains `component:external:node:get-proto` (high)
- `component:external:node:get-tsconfig` depends_on `component:external:node:resolve-pkg-maps` (medium)
- `repository` contains `component:external:node:get-tsconfig` (high)
- `repository` contains `component:external:node:gopd` (high)
- `component:external:node:has-property-descriptors` depends_on `component:external:node:es-define-property` (medium)
- `repository` contains `component:external:node:has-property-descriptors` (high)
- `repository` contains `component:external:node:has-symbols` (high)
- `component:external:node:hasown` depends_on `component:external:node:function-bind` (medium)
- `repository` contains `component:external:node:hasown` (high)
- `repository` contains `component:external:node:hono` (high)
- `component:external:node:http-errors` depends_on `component:external:node:depd` (medium)
- `component:external:node:http-errors` depends_on `component:external:node:inherits` (medium)
- `component:external:node:http-errors` depends_on `component:external:node:setprototypeof` (medium)
- `component:external:node:http-errors` depends_on `component:external:node:statuses` (medium)
- `component:external:node:http-errors` depends_on `component:external:node:toidentifier` (medium)
- `repository` contains `component:external:node:http-errors` (high)
- `component:external:node:iconv-lite` depends_on `component:external:node:safer-buffer` (medium)
- `repository` contains `component:external:node:iconv-lite` (high)
- `repository` contains `component:external:node:immutable` (high)
- `repository` contains `component:external:node:inherits` (high)
- `repository` contains `component:external:node:ipaddr.js` (high)
- `repository` contains `component:external:node:is-promise` (high)
- `repository` contains `component:external:node:isarray` (high)
- `repository` contains `component:external:node:isexe` (high)
- `repository` contains `component:external:node:jose` (high)
- `repository` contains `component:external:node:json-schema-traverse` (high)
- `repository` contains `component:external:node:json-schema-typed` (high)
- `component:external:node:json-stable-stringify` depends_on `component:external:node:call-bind` (medium)
- `component:external:node:json-stable-stringify` depends_on `component:external:node:call-bound` (medium)
- `component:external:node:json-stable-stringify` depends_on `component:external:node:isarray` (medium)
- `component:external:node:json-stable-stringify` depends_on `component:external:node:jsonify` (medium)
- `component:external:node:json-stable-stringify` depends_on `component:external:node:object-keys` (medium)
- `repository` contains `component:external:node:json-stable-stringify` (high)
- `repository` contains `component:external:node:jsonify` (high)
- `repository` contains `component:external:node:kysely` (high)
- `component:external:node:lru-cache` depends_on `component:external:node:yallist` (medium)
- `repository` contains `component:external:node:lru-cache` (high)
- `component:external:node:magic-string` depends_on `component:external:node:@jridgewell/sourcemap-codec` (medium)
- `repository` contains `component:external:node:magic-string` (high)
- `repository` contains `component:external:node:math-intrinsics` (high)
- `repository` contains `component:external:node:media-typer` (high)
- `repository` contains `component:external:node:merge-descriptors` (high)
- `repository` contains `component:external:node:mime-db` (high)
- `component:external:node:mime-types` depends_on `component:external:node:mime-db` (medium)
- `repository` contains `component:external:node:mime-types` (high)
- `repository` contains `component:external:node:moment` (high)
- `repository` contains `component:external:node:moo` (high)
- `repository` contains `component:external:node:ms` (high)
- `repository` contains `component:external:node:nanoid` (high)
- `component:external:node:nearley` depends_on `component:external:node:commander` (medium)
- `component:external:node:nearley` depends_on `component:external:node:moo` (medium)
- `component:external:node:nearley` depends_on `component:external:node:railroad-diagrams` (medium)
- `component:external:node:nearley` depends_on `component:external:node:randexp` (medium)
- `repository` contains `component:external:node:nearley` (high)
- `repository` contains `component:external:node:negotiator` (high)
- `repository` contains `component:external:node:object-assign` (high)
- `repository` contains `component:external:node:object-hash` (high)
- `repository` contains `component:external:node:object-inspect` (high)
- `repository` contains `component:external:node:object-keys` (high)
- `repository` contains `component:external:node:obug` (high)
- `component:external:node:on-finished` depends_on `component:external:node:ee-first` (medium)
- `repository` contains `component:external:node:on-finished` (high)
- `component:external:node:once` depends_on `component:external:node:wrappy` (medium)
- `repository` contains `component:external:node:once` (high)
- `repository` contains `component:external:node:parseurl` (high)
- `repository` contains `component:external:node:path-key` (high)
- `repository` contains `component:external:node:path-to-regexp` (high)
- `repository` contains `component:external:node:pathe` (high)
- `component:external:node:pg` depends_on `component:external:node:pg-connection-string` (medium)
- `component:external:node:pg` depends_on `component:external:node:pg-pool` (medium)
- `component:external:node:pg` depends_on `component:external:node:pg-protocol` (medium)
- `component:external:node:pg` depends_on `component:external:node:pg-types` (medium)
- `component:external:node:pg` depends_on `component:external:node:pgpass` (medium)
- `repository` contains `component:external:node:pg` (high)
- `repository` contains `component:external:node:pg-cloudflare` (high)
- `repository` contains `component:external:node:pg-connection-string` (high)
- `repository` contains `component:external:node:pg-int8` (high)
- `component:external:node:pg-mem` depends_on `component:external:node:functional-red-black-tree` (medium)
- `component:external:node:pg-mem` depends_on `component:external:node:immutable` (medium)
- `component:external:node:pg-mem` depends_on `component:external:node:json-stable-stringify` (medium)
- `component:external:node:pg-mem` depends_on `component:external:node:lru-cache` (medium)
- `component:external:node:pg-mem` depends_on `component:external:node:moment` (medium)
- `component:external:node:pg-mem` depends_on `component:external:node:object-hash` (medium)
- `component:external:node:pg-mem` depends_on `component:external:node:pgsql-ast-parser` (medium)
- `repository` contains `component:external:node:pg-mem` (high)
- `repository` contains `component:external:node:pg-pool` (high)
- `repository` contains `component:external:node:pg-protocol` (high)
- `component:external:node:pg-types` depends_on `component:external:node:pg-int8` (medium)
- `component:external:node:pg-types` depends_on `component:external:node:postgres-array` (medium)
- `component:external:node:pg-types` depends_on `component:external:node:postgres-bytea` (medium)
- `component:external:node:pg-types` depends_on `component:external:node:postgres-date` (medium)
- `component:external:node:pg-types` depends_on `component:external:node:postgres-interval` (medium)
- `repository` contains `component:external:node:pg-types` (high)
- `component:external:node:pgpass` depends_on `component:external:node:split2` (medium)
- `repository` contains `component:external:node:pgpass` (high)
- `component:external:node:pgsql-ast-parser` depends_on `component:external:node:moo` (medium)
- `component:external:node:pgsql-ast-parser` depends_on `component:external:node:nearley` (medium)
- `repository` contains `component:external:node:pgsql-ast-parser` (high)
- `repository` contains `component:external:node:picocolors` (high)
- `repository` contains `component:external:node:picomatch` (high)
- `repository` contains `component:external:node:pkce-challenge` (high)
- `component:external:node:postcss` depends_on `component:external:node:nanoid` (medium)
- `component:external:node:postcss` depends_on `component:external:node:picocolors` (medium)
- `component:external:node:postcss` depends_on `component:external:node:source-map-js` (medium)
- `repository` contains `component:external:node:postcss` (high)
- `repository` contains `component:external:node:postgres-array` (high)
- `repository` contains `component:external:node:postgres-bytea` (high)
- `repository` contains `component:external:node:postgres-date` (high)
- `component:external:node:postgres-interval` depends_on `component:external:node:xtend` (medium)
- `repository` contains `component:external:node:postgres-interval` (high)
- `component:external:node:proxy-addr` depends_on `component:external:node:forwarded` (medium)
- `component:external:node:proxy-addr` depends_on `component:external:node:ipaddr.js` (medium)
- `repository` contains `component:external:node:proxy-addr` (high)
- `component:external:node:qs` depends_on `component:external:node:side-channel` (medium)
- `repository` contains `component:external:node:qs` (high)
- `repository` contains `component:external:node:railroad-diagrams` (high)
- `component:external:node:randexp` depends_on `component:external:node:discontinuous-range` (medium)
- `component:external:node:randexp` depends_on `component:external:node:ret` (medium)
- `repository` contains `component:external:node:randexp` (high)
- `repository` contains `component:external:node:range-parser` (high)
- `component:external:node:raw-body` depends_on `component:external:node:bytes` (medium)
- `component:external:node:raw-body` depends_on `component:external:node:http-errors` (medium)
- `component:external:node:raw-body` depends_on `component:external:node:iconv-lite` (medium)
- `component:external:node:raw-body` depends_on `component:external:node:unpipe` (medium)
- `repository` contains `component:external:node:raw-body` (high)
- `repository` contains `component:external:node:require-from-string` (high)
- `repository` contains `component:external:node:resolve-pkg-maps` (high)
- `repository` contains `component:external:node:ret` (high)
- `component:external:node:rollup` depends_on `component:external:node:@types/estree` (medium)
- `repository` contains `component:external:node:rollup` (high)
- `component:external:node:router` depends_on `component:external:node:debug` (medium)
- `component:external:node:router` depends_on `component:external:node:depd` (medium)
- `component:external:node:router` depends_on `component:external:node:is-promise` (medium)
- `component:external:node:router` depends_on `component:external:node:parseurl` (medium)
- `component:external:node:router` depends_on `component:external:node:path-to-regexp` (medium)
- `repository` contains `component:external:node:router` (high)
- `repository` contains `component:external:node:safer-buffer` (high)
- `component:external:node:send` depends_on `component:external:node:debug` (medium)
- `component:external:node:send` depends_on `component:external:node:encodeurl` (medium)
- `component:external:node:send` depends_on `component:external:node:escape-html` (medium)
- `component:external:node:send` depends_on `component:external:node:etag` (medium)
- `component:external:node:send` depends_on `component:external:node:fresh` (medium)
- `component:external:node:send` depends_on `component:external:node:http-errors` (medium)
- `component:external:node:send` depends_on `component:external:node:mime-types` (medium)
- `component:external:node:send` depends_on `component:external:node:ms` (medium)
- `component:external:node:send` depends_on `component:external:node:on-finished` (medium)
- `component:external:node:send` depends_on `component:external:node:range-parser` (medium)
- `component:external:node:send` depends_on `component:external:node:statuses` (medium)
- `repository` contains `component:external:node:send` (high)
- `component:external:node:serve-static` depends_on `component:external:node:encodeurl` (medium)
- `component:external:node:serve-static` depends_on `component:external:node:escape-html` (medium)
- `component:external:node:serve-static` depends_on `component:external:node:parseurl` (medium)
- `component:external:node:serve-static` depends_on `component:external:node:send` (medium)
- `repository` contains `component:external:node:serve-static` (high)
- `component:external:node:set-function-length` depends_on `component:external:node:define-data-property` (medium)
- `component:external:node:set-function-length` depends_on `component:external:node:es-errors` (medium)
- `component:external:node:set-function-length` depends_on `component:external:node:function-bind` (medium)
- `component:external:node:set-function-length` depends_on `component:external:node:get-intrinsic` (medium)
- `component:external:node:set-function-length` depends_on `component:external:node:gopd` (medium)
- `component:external:node:set-function-length` depends_on `component:external:node:has-property-descriptors` (medium)
- `repository` contains `component:external:node:set-function-length` (high)
- `repository` contains `component:external:node:setprototypeof` (high)
- `component:external:node:shebang-command` depends_on `component:external:node:shebang-regex` (medium)
- `repository` contains `component:external:node:shebang-command` (high)
- `repository` contains `component:external:node:shebang-regex` (high)
- `component:external:node:side-channel` depends_on `component:external:node:es-errors` (medium)
- `component:external:node:side-channel` depends_on `component:external:node:object-inspect` (medium)
- `component:external:node:side-channel` depends_on `component:external:node:side-channel-list` (medium)
- `component:external:node:side-channel` depends_on `component:external:node:side-channel-map` (medium)
- `component:external:node:side-channel` depends_on `component:external:node:side-channel-weakmap` (medium)
- `repository` contains `component:external:node:side-channel` (high)
- `component:external:node:side-channel-list` depends_on `component:external:node:es-errors` (medium)
- `component:external:node:side-channel-list` depends_on `component:external:node:object-inspect` (medium)
- `repository` contains `component:external:node:side-channel-list` (high)
- `component:external:node:side-channel-map` depends_on `component:external:node:call-bound` (medium)
- `component:external:node:side-channel-map` depends_on `component:external:node:es-errors` (medium)
- `component:external:node:side-channel-map` depends_on `component:external:node:get-intrinsic` (medium)
- `component:external:node:side-channel-map` depends_on `component:external:node:object-inspect` (medium)
- `repository` contains `component:external:node:side-channel-map` (high)
- `component:external:node:side-channel-weakmap` depends_on `component:external:node:call-bound` (medium)
- `component:external:node:side-channel-weakmap` depends_on `component:external:node:es-errors` (medium)
- `component:external:node:side-channel-weakmap` depends_on `component:external:node:get-intrinsic` (medium)
- `component:external:node:side-channel-weakmap` depends_on `component:external:node:object-inspect` (medium)
- `component:external:node:side-channel-weakmap` depends_on `component:external:node:side-channel-map` (medium)
- `repository` contains `component:external:node:side-channel-weakmap` (high)
- `repository` contains `component:external:node:siginfo` (high)
- `repository` contains `component:external:node:source-map-js` (high)
- `repository` contains `component:external:node:split2` (high)
- `repository` contains `component:external:node:stackback` (high)
- `repository` contains `component:external:node:statuses` (high)
- `repository` contains `component:external:node:std-env` (high)
- `repository` contains `component:external:node:tinybench` (high)
- `repository` contains `component:external:node:tinyexec` (high)
- `component:external:node:tinyglobby` depends_on `component:external:node:fdir` (medium)
- `component:external:node:tinyglobby` depends_on `component:external:node:picomatch` (medium)
- `repository` contains `component:external:node:tinyglobby` (high)
- `repository` contains `component:external:node:tinyrainbow` (high)
- `repository` contains `component:external:node:toidentifier` (high)
- `component:external:node:tsx` depends_on `component:external:node:esbuild` (medium)
- `component:external:node:tsx` depends_on `component:external:node:get-tsconfig` (medium)
- `repository` contains `component:external:node:tsx` (high)
- `component:external:node:type-is` depends_on `component:external:node:content-type` (medium)
- `component:external:node:type-is` depends_on `component:external:node:media-typer` (medium)
- `component:external:node:type-is` depends_on `component:external:node:mime-types` (medium)
- `repository` contains `component:external:node:type-is` (high)
- `repository` contains `component:external:node:typescript` (high)
- `repository` contains `component:external:node:ulid` (high)
- `repository` contains `component:external:node:undici-types` (high)
- `repository` contains `component:external:node:unpipe` (high)
- `repository` contains `component:external:node:vary` (high)
- `component:external:node:vite` depends_on `component:external:node:esbuild` (medium)
- `component:external:node:vite` depends_on `component:external:node:fdir` (medium)
- `component:external:node:vite` depends_on `component:external:node:picomatch` (medium)
- `component:external:node:vite` depends_on `component:external:node:postcss` (medium)
- `component:external:node:vite` depends_on `component:external:node:rollup` (medium)
- `component:external:node:vite` depends_on `component:external:node:tinyglobby` (medium)
- `repository` contains `component:external:node:vite` (high)
- `component:external:node:vitest` depends_on `component:external:node:@vitest/expect` (medium)
- `component:external:node:vitest` depends_on `component:external:node:@vitest/mocker` (medium)
- `component:external:node:vitest` depends_on `component:external:node:@vitest/pretty-format` (medium)
- `component:external:node:vitest` depends_on `component:external:node:@vitest/runner` (medium)
- `component:external:node:vitest` depends_on `component:external:node:@vitest/snapshot` (medium)
- `component:external:node:vitest` depends_on `component:external:node:@vitest/spy` (medium)
- `component:external:node:vitest` depends_on `component:external:node:@vitest/utils` (medium)
- `component:external:node:vitest` depends_on `component:external:node:es-module-lexer` (medium)
- `component:external:node:vitest` depends_on `component:external:node:expect-type` (medium)
- `component:external:node:vitest` depends_on `component:external:node:magic-string` (medium)
- `component:external:node:vitest` depends_on `component:external:node:obug` (medium)
- `component:external:node:vitest` depends_on `component:external:node:pathe` (medium)
- `component:external:node:vitest` depends_on `component:external:node:picomatch` (medium)
- `component:external:node:vitest` depends_on `component:external:node:std-env` (medium)
- `component:external:node:vitest` depends_on `component:external:node:tinybench` (medium)
- `component:external:node:vitest` depends_on `component:external:node:tinyexec` (medium)
- `component:external:node:vitest` depends_on `component:external:node:tinyglobby` (medium)
- `component:external:node:vitest` depends_on `component:external:node:tinyrainbow` (medium)
- `component:external:node:vitest` depends_on `component:external:node:vite` (medium)
- `component:external:node:vitest` depends_on `component:external:node:why-is-node-running` (medium)
- `repository` contains `component:external:node:vitest` (high)
- `component:external:node:which` depends_on `component:external:node:isexe` (medium)
- `repository` contains `component:external:node:which` (high)
- `component:external:node:why-is-node-running` depends_on `component:external:node:siginfo` (medium)
- `component:external:node:why-is-node-running` depends_on `component:external:node:stackback` (medium)
- `repository` contains `component:external:node:why-is-node-running` (high)
- `repository` contains `component:external:node:wrappy` (high)
- `repository` contains `component:external:node:xtend` (high)
- `repository` contains `component:external:node:yallist` (high)
- `repository` contains `component:external:node:yaml` (high)
- `repository` contains `component:external:node:zod` (high)
- `repository` contains `component:external:node:zod-to-json-schema` (high)
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
- `repository` contains `component:src` (high)
- `component:tests` tests `component:external:node:@esbuild/aix-ppc64` (high)
- `component:tests` tests `component:external:node:@esbuild/android-arm` (high)
- `component:tests` tests `component:external:node:@esbuild/android-arm64` (high)
- `component:tests` tests `component:external:node:@esbuild/android-x64` (high)
- `component:tests` tests `component:external:node:@esbuild/darwin-arm64` (high)
- `component:tests` tests `component:external:node:@esbuild/darwin-x64` (high)
- `component:tests` tests `component:external:node:@esbuild/freebsd-arm64` (high)
- `component:tests` tests `component:external:node:@esbuild/freebsd-x64` (high)
- `component:tests` tests `component:external:node:@esbuild/linux-arm` (high)
- `component:tests` tests `component:external:node:@esbuild/linux-arm64` (high)
- `component:tests` tests `component:external:node:@esbuild/linux-ia32` (high)
- `component:tests` tests `component:external:node:@esbuild/linux-loong64` (high)
- `component:tests` tests `component:external:node:@esbuild/linux-mips64el` (high)
- `component:tests` tests `component:external:node:@esbuild/linux-ppc64` (high)
- `component:tests` tests `component:external:node:@esbuild/linux-riscv64` (high)
- `component:tests` tests `component:external:node:@esbuild/linux-s390x` (high)
- `component:tests` tests `component:external:node:@esbuild/linux-x64` (high)
- `component:tests` tests `component:external:node:@esbuild/netbsd-arm64` (high)
- `component:tests` tests `component:external:node:@esbuild/netbsd-x64` (high)
- `component:tests` tests `component:external:node:@esbuild/openbsd-arm64` (high)
- `component:tests` tests `component:external:node:@esbuild/openbsd-x64` (high)
- `component:tests` tests `component:external:node:@esbuild/openharmony-arm64` (high)
- `component:tests` tests `component:external:node:@esbuild/sunos-x64` (high)
- `component:tests` tests `component:external:node:@esbuild/win32-arm64` (high)
- `component:tests` tests `component:external:node:@esbuild/win32-ia32` (high)
- `component:tests` tests `component:external:node:@esbuild/win32-x64` (high)
- `component:tests` tests `component:external:node:@hono/node-server` (high)
- `component:tests` tests `component:external:node:@jridgewell/sourcemap-codec` (high)
- `component:tests` tests `component:external:node:@modelcontextprotocol/sdk` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-android-arm-eabi` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-android-arm64` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-darwin-arm64` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-darwin-x64` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-freebsd-arm64` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-freebsd-x64` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-linux-arm-gnueabihf` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-linux-arm-musleabihf` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-linux-arm64-gnu` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-linux-arm64-musl` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-linux-loong64-gnu` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-linux-loong64-musl` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-linux-ppc64-gnu` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-linux-ppc64-musl` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-linux-riscv64-gnu` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-linux-riscv64-musl` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-linux-s390x-gnu` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-linux-x64-gnu` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-linux-x64-musl` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-openbsd-x64` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-openharmony-arm64` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-win32-arm64-msvc` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-win32-ia32-msvc` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-win32-x64-gnu` (high)
- `component:tests` tests `component:external:node:@rollup/rollup-win32-x64-msvc` (high)
- `component:tests` tests `component:external:node:@standard-schema/spec` (high)
- `component:tests` tests `component:external:node:@types/chai` (high)
- `component:tests` tests `component:external:node:@types/deep-eql` (high)
- `component:tests` tests `component:external:node:@types/estree` (high)
- `component:tests` tests `component:external:node:@types/node` (high)
- `component:tests` tests `component:external:node:@types/pg` (high)
- `component:tests` tests `component:external:node:@vitest/expect` (high)
- `component:tests` tests `component:external:node:@vitest/mocker` (high)
- `component:tests` tests `component:external:node:@vitest/pretty-format` (high)
- `component:tests` tests `component:external:node:@vitest/runner` (high)
- `component:tests` tests `component:external:node:@vitest/snapshot` (high)
- `component:tests` tests `component:external:node:@vitest/spy` (high)
- `component:tests` tests `component:external:node:@vitest/utils` (high)
- `component:tests` tests `component:external:node:accepts` (high)
- `component:tests` tests `component:external:node:ajv` (high)
- `component:tests` tests `component:external:node:ajv-formats` (high)
- `component:tests` tests `component:external:node:assertion-error` (high)
- `component:tests` tests `component:external:node:body-parser` (high)
- `component:tests` tests `component:external:node:bytes` (high)
- `component:tests` tests `component:external:node:call-bind` (high)
- `component:tests` tests `component:external:node:call-bind-apply-helpers` (high)
- `component:tests` tests `component:external:node:call-bound` (high)
- `component:tests` tests `component:external:node:chai` (high)
- `component:tests` tests `component:external:node:commander` (high)
- `component:tests` tests `component:external:node:content-disposition` (high)
- `component:tests` tests `component:external:node:content-type` (high)
- `component:tests` tests `component:external:node:cookie` (high)
- `component:tests` tests `component:external:node:cookie-signature` (high)
- `component:tests` tests `component:external:node:cors` (high)
- `component:tests` tests `component:external:node:cross-spawn` (high)
- `component:tests` tests `component:external:node:debug` (high)
- `component:tests` tests `component:external:node:define-data-property` (high)
- `component:tests` tests `component:external:node:depd` (high)
- `component:tests` tests `component:external:node:discontinuous-range` (high)
- `component:tests` tests `component:external:node:dunder-proto` (high)
- `component:tests` tests `component:external:node:ee-first` (high)
- `component:tests` tests `component:external:node:encodeurl` (high)
- `component:tests` tests `component:external:node:es-define-property` (high)
- `component:tests` tests `component:external:node:es-errors` (high)
- `component:tests` tests `component:external:node:es-module-lexer` (high)
- `component:tests` tests `component:external:node:es-object-atoms` (high)
- `component:tests` tests `component:external:node:esbuild` (high)
- `component:tests` tests `component:external:node:escape-html` (high)
- `component:tests` tests `component:external:node:estree-walker` (high)
- `component:tests` tests `component:external:node:etag` (high)
- `component:tests` tests `component:external:node:eventsource` (high)
- `component:tests` tests `component:external:node:eventsource-parser` (high)
- `component:tests` tests `component:external:node:expect-type` (high)
- `component:tests` tests `component:external:node:express` (high)
- `component:tests` tests `component:external:node:express-rate-limit` (high)
- `component:tests` tests `component:external:node:fast-deep-equal` (high)
- `component:tests` tests `component:external:node:fast-uri` (high)
- `component:tests` tests `component:external:node:fdir` (high)
- `component:tests` tests `component:external:node:finalhandler` (high)
- `component:tests` tests `component:external:node:forwarded` (high)
- `component:tests` tests `component:external:node:fresh` (high)
- `component:tests` tests `component:external:node:fsevents` (high)
- `component:tests` tests `component:external:node:function-bind` (high)
- `component:tests` tests `component:external:node:functional-red-black-tree` (high)
- `component:tests` tests `component:external:node:get-intrinsic` (high)
- `component:tests` tests `component:external:node:get-proto` (high)
- `component:tests` tests `component:external:node:get-tsconfig` (high)
- `component:tests` tests `component:external:node:gopd` (high)
- `component:tests` tests `component:external:node:has-property-descriptors` (high)
- `component:tests` tests `component:external:node:has-symbols` (high)
- `component:tests` tests `component:external:node:hasown` (high)
- `component:tests` tests `component:external:node:hono` (high)
- `component:tests` tests `component:external:node:http-errors` (high)
- `component:tests` tests `component:external:node:iconv-lite` (high)
- `component:tests` tests `component:external:node:immutable` (high)
- `component:tests` tests `component:external:node:inherits` (high)
- `component:tests` tests `component:external:node:ipaddr.js` (high)
- `component:tests` tests `component:external:node:is-promise` (high)
- `component:tests` tests `component:external:node:isarray` (high)
- `component:tests` tests `component:external:node:isexe` (high)
- `component:tests` tests `component:external:node:jose` (high)
- `component:tests` tests `component:external:node:json-schema-traverse` (high)
- `component:tests` tests `component:external:node:json-schema-typed` (high)
- `component:tests` tests `component:external:node:json-stable-stringify` (high)
- `component:tests` tests `component:external:node:jsonify` (high)
- `component:tests` tests `component:external:node:kysely` (high)
- `component:tests` tests `component:external:node:lru-cache` (high)
- `component:tests` tests `component:external:node:magic-string` (high)
- `component:tests` tests `component:external:node:math-intrinsics` (high)
- `component:tests` tests `component:external:node:media-typer` (high)
- `component:tests` tests `component:external:node:merge-descriptors` (high)
- `component:tests` tests `component:external:node:mime-db` (high)
- `component:tests` tests `component:external:node:mime-types` (high)
- `component:tests` tests `component:external:node:moment` (high)
- `component:tests` tests `component:external:node:moo` (high)
- `component:tests` tests `component:external:node:ms` (high)
- `component:tests` tests `component:external:node:nanoid` (high)
- `component:tests` tests `component:external:node:nearley` (high)
- `component:tests` tests `component:external:node:negotiator` (high)
- `component:tests` tests `component:external:node:object-assign` (high)
- `component:tests` tests `component:external:node:object-hash` (high)
- `component:tests` tests `component:external:node:object-inspect` (high)
- `component:tests` tests `component:external:node:object-keys` (high)
- `component:tests` tests `component:external:node:obug` (high)
- `component:tests` tests `component:external:node:on-finished` (high)
- `component:tests` tests `component:external:node:once` (high)
- `component:tests` tests `component:external:node:parseurl` (high)
- `component:tests` tests `component:external:node:path-key` (high)
- `component:tests` tests `component:external:node:path-to-regexp` (high)
- `component:tests` tests `component:external:node:pathe` (high)
- `component:tests` tests `component:external:node:pg` (high)
- `component:tests` tests `component:external:node:pg-cloudflare` (high)
- `component:tests` tests `component:external:node:pg-connection-string` (high)
- `component:tests` tests `component:external:node:pg-int8` (high)
- `component:tests` tests `component:external:node:pg-mem` (high)
- `component:tests` tests `component:external:node:pg-pool` (high)
- `component:tests` tests `component:external:node:pg-protocol` (high)
- `component:tests` tests `component:external:node:pg-types` (high)
- `component:tests` tests `component:external:node:pgpass` (high)
- `component:tests` tests `component:external:node:pgsql-ast-parser` (high)
- `component:tests` tests `component:external:node:picocolors` (high)
- `component:tests` tests `component:external:node:picomatch` (high)
- `component:tests` tests `component:external:node:pkce-challenge` (high)
- `component:tests` tests `component:external:node:postcss` (high)
- `component:tests` tests `component:external:node:postgres-array` (high)
- `component:tests` tests `component:external:node:postgres-bytea` (high)
- `component:tests` tests `component:external:node:postgres-date` (high)
- `component:tests` tests `component:external:node:postgres-interval` (high)
- `component:tests` tests `component:external:node:proxy-addr` (high)
- `component:tests` tests `component:external:node:qs` (high)
- `component:tests` tests `component:external:node:railroad-diagrams` (high)
- `component:tests` tests `component:external:node:randexp` (high)
- `component:tests` tests `component:external:node:range-parser` (high)
- `component:tests` tests `component:external:node:raw-body` (high)
- `component:tests` tests `component:external:node:require-from-string` (high)
- `component:tests` tests `component:external:node:resolve-pkg-maps` (high)
- `component:tests` tests `component:external:node:ret` (high)
- `component:tests` tests `component:external:node:rollup` (high)
- `component:tests` tests `component:external:node:router` (high)
- `component:tests` tests `component:external:node:safer-buffer` (high)
- `component:tests` tests `component:external:node:send` (high)
- `component:tests` tests `component:external:node:serve-static` (high)
- `component:tests` tests `component:external:node:set-function-length` (high)
- `component:tests` tests `component:external:node:setprototypeof` (high)
- `component:tests` tests `component:external:node:shebang-command` (high)
- `component:tests` tests `component:external:node:shebang-regex` (high)
- `component:tests` tests `component:external:node:side-channel` (high)
- `component:tests` tests `component:external:node:side-channel-list` (high)
- `component:tests` tests `component:external:node:side-channel-map` (high)
- `component:tests` tests `component:external:node:side-channel-weakmap` (high)
- `component:tests` tests `component:external:node:siginfo` (high)
- `component:tests` tests `component:external:node:source-map-js` (high)
- `component:tests` tests `component:external:node:split2` (high)
- `component:tests` tests `component:external:node:stackback` (high)
- `component:tests` tests `component:external:node:statuses` (high)
- `component:tests` tests `component:external:node:std-env` (high)
- `component:tests` tests `component:external:node:tinybench` (high)
- `component:tests` tests `component:external:node:tinyexec` (high)
- `component:tests` tests `component:external:node:tinyglobby` (high)
- `component:tests` tests `component:external:node:tinyrainbow` (high)
- `component:tests` tests `component:external:node:toidentifier` (high)
- `component:tests` tests `component:external:node:tsx` (high)
- `component:tests` tests `component:external:node:type-is` (high)
- `component:tests` tests `component:external:node:typescript` (high)
- `component:tests` tests `component:external:node:ulid` (high)
- `component:tests` tests `component:external:node:undici-types` (high)
- `component:tests` tests `component:external:node:unpipe` (high)
- `component:tests` tests `component:external:node:vary` (high)
- `component:tests` tests `component:external:node:vite` (high)
- `component:tests` tests `component:external:node:vitest` (high)
- `component:tests` tests `component:external:node:which` (high)
- `component:tests` tests `component:external:node:why-is-node-running` (high)
- `component:tests` tests `component:external:node:wrappy` (high)
- `component:tests` tests `component:external:node:xtend` (high)
- `component:tests` tests `component:external:node:yallist` (high)
- `component:tests` tests `component:external:node:yaml` (high)
- `component:tests` tests `component:external:node:zod` (high)
- `component:tests` tests `component:external:node:zod-to-json-schema` (high)
- `component:tests` tests `component:package.json` (high)
- `component:tests` tests `component:src` (high)
- `scripts/bundle_export.ts` depends_on `component:external:node:pg` (medium)
- `component:src` depends_on `component:external:node:ulid` (medium)
- `component:src` depends_on `component:external:node:pg` (medium)
- `component:src` depends_on `component:external:node:kysely` (medium)
- `component:src` depends_on `component:external:node:pg` (medium)
- `component:src` depends_on `component:external:node:kysely` (medium)
- `component:src` depends_on `component:external:node:@modelcontextprotocol/sdk` (medium)
- `component:src` depends_on `component:external:node:pg` (medium)
- `component:src` depends_on `component:external:node:pg-mem` (medium)
- `component:src` depends_on `component:external:node:@modelcontextprotocol/sdk` (medium)
- `component:src` depends_on `component:external:node:zod` (medium)
- `component:src` depends_on `component:external:node:@modelcontextprotocol/sdk` (medium)
- `component:src` depends_on `component:external:node:yaml` (medium)
- `component:src` depends_on `component:external:node:@modelcontextprotocol/sdk` (medium)
- `component:src` depends_on `component:external:node:kysely` (medium)
- `component:src` depends_on `component:external:node:@modelcontextprotocol/sdk` (medium)
- `component:src` depends_on `component:external:node:@modelcontextprotocol/sdk` (medium)
- `component:src` depends_on `component:external:node:@modelcontextprotocol/sdk` (medium)
- `component:src` depends_on `component:external:node:@modelcontextprotocol/sdk` (medium)
- `component:src` depends_on `component:external:node:@modelcontextprotocol/sdk` (medium)
- `component:src` depends_on `component:external:node:@modelcontextprotocol/sdk` (medium)
- `component:src` depends_on `component:external:node:@modelcontextprotocol/sdk` (medium)
- `component:src` depends_on `component:external:node:@modelcontextprotocol/sdk` (medium)
- `component:src` depends_on `component:external:node:zod` (medium)
- `component:tests` depends_on `component:external:node:pg` (medium)
- `component:tests` depends_on `component:external:node:pg-mem` (medium)
- `component:tests` depends_on `component:external:node:vitest` (medium)
- `component:tests` depends_on `component:external:node:pg` (medium)
- `component:tests` depends_on `component:external:node:pg-mem` (medium)
- `component:tests` depends_on `component:external:node:vitest` (medium)
- `component:tests` depends_on `component:external:node:ajv` (medium)
- `component:tests` depends_on `component:external:node:vitest` (medium)
- `component:tests` depends_on `component:external:node:@modelcontextprotocol/sdk` (medium)
- `component:tests` depends_on `component:external:node:pg` (medium)
- `component:tests` depends_on `component:external:node:pg-mem` (medium)
- `component:tests` depends_on `component:external:node:vitest` (medium)
- `component:tests` depends_on `component:external:node:@modelcontextprotocol/sdk` (medium)
- `component:tests` depends_on `component:external:node:vitest` (medium)
- `component:tests` depends_on `component:external:node:pg` (medium)
- `component:tests` depends_on `component:external:node:pg-mem` (medium)
- `component:tests` depends_on `component:external:node:vitest` (medium)
- `component:tests` depends_on `component:external:node:@modelcontextprotocol/sdk` (medium)
- `component:tests` depends_on `component:external:node:pg` (medium)
- `component:tests` depends_on `component:external:node:pg-mem` (medium)
- `component:tests` depends_on `component:external:node:vitest` (medium)
- `component:tests` depends_on `component:external:node:@modelcontextprotocol/sdk` (medium)
- `component:tests` depends_on `component:external:node:pg` (medium)
- `component:tests` depends_on `component:external:node:pg-mem` (medium)
- `component:tests` depends_on `component:external:node:vitest` (medium)
- `component:tests` depends_on `component:external:node:zod` (medium)
- `vitest.config.ts` depends_on `component:external:node:vitest` (medium)

<details>
<summary>Related files:</summary>

- `docs/architecture.md`
- `docs/bundle_export.md`
- `package.json`
- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`
- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
</details>

<details>
<summary>Citations:</summary>

- `docs/architecture.md`
- `docs/bundle_export.md`
- `package.json`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

## Navigation Guidance

- Follow component pages for detailed file lists.
- Use interfaces to inspect exposed configuration and manifests.
- Read diagrams for a simplified graph view.

## Citations

<details>
<summary>Citations:</summary>

- `package.json`
- `package-lock.json`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `scripts/bundle_export.ts:27`
- `src/db/bootstrap.ts:4`
- `src/db/connection.ts:9`
- `src/index.ts:18`
- `tests/gateway.integration.test.ts:146`
- `tests/runLifecycle.test.ts:23`
- `tests/slurm.integration.test.ts:48`
- `src/mcp/toolSchemas.ts:3`
- `src/toolpacks/types.ts:38`
- `tests/toolpacks.test.ts:183`
- `docs/architecture.md`
- `docs/bundle_export.md`
</details>
