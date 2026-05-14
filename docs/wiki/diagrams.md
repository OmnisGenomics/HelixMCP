---
page_id: diagrams
page_type: diagrams
generation_mode: inferred
freshness_status: new
updated_at: 2026-04-18T05:42:41.667Z
---

<details>
<summary>Build metadata</summary>

```json
{
  "freshnessKey": "cd3dd6dade0ca065796fd7841b9c8ffec6eac4cb",
  "plannerReason": "Generated to provide a compact architecture and dependency overview.",
  "changedPaths": [],
  "dependencyPaths": [],
  "dependencyEvidenceIds": [],
  "evidenceIds": [],
  "qualityWarnings": [
    "Diagrams has no citations."
  ]
}

```
</details>

# Diagrams

Generated 4 diagrams.

## Diagram Navigation

- [Component Overview](#component-overview) (component-overview; 9 nodes; 8 edges; omitted 223 nodes / 223 edges)
- [Dependency Graph](#dependency-graph) (dependency-graph; 15 nodes; 16 edges; omitted 0 nodes / 1511 edges)
- [Directory Map](#directory-map) (directory-map; 9 nodes; 8 edges)
- [Subsystem Clusters](#subsystem-clusters) (component-overview; 3 nodes; 1 edges; omitted 0 nodes / 839 edges)

## Related Pages

- [architecture](architecture.md)
- [dependencies](dependencies.md)
- [runtime](runtime.md)

## Component Overview

Shows the most prominent inferred components connected to the repository root.

Explained in:
- [Architecture Summary](architecture.md#architecture-summary)
- [Graph Hotspots](architecture.md#architecture-hotspots)
- [Design-Shaping Dependencies](dependencies.md#design-shaping-dependencies)

Interpretation note:
- Interpretation: use this view to see the main repository-owned components and their highest-level relationships before drilling into page-level details. Favor it when you need a fast inventory of the system surface.

Rendered surface:
- rendered nodes: 9, rendered edges: 8

Node mix:
- component: 8, repository: 1

Omitted surface:
- omitted nodes: 223
- omitted edges: 223

```mermaid
graph LR
  repository["HelixMCP"] --> component_docs["Documentation"]
  repository["HelixMCP"] --> component_external_node__esbuild_aix_ppc64["@esbuild/aix-ppc64"]
  repository["HelixMCP"] --> component_external_node__esbuild_android_arm["@esbuild/android-arm"]
  repository["HelixMCP"] --> component_external_node__esbuild_android_arm64["@esbuild/android-arm64"]
  repository["HelixMCP"] --> component_external_node__esbuild_android_x64["@esbuild/android-x64"]
  repository["HelixMCP"] --> component_external_node__esbuild_darwin_arm64["@esbuild/darwin-arm64"]
  repository["HelixMCP"] --> component_external_node__esbuild_darwin_x64["@esbuild/darwin-x64"]
  repository["HelixMCP"] --> component_external_node__esbuild_freebsd_arm64["@esbuild/freebsd-arm64"]

```

```dot
digraph RepoIntel {
  label="Component Overview";
  labelloc=t;
  rankdir=LR;
  node [shape=box];
  "repository" [label="HelixMCP", shape=box];
  "component:docs" [label="Documentation", shape=box];
  "component:external:node:@esbuild/aix-ppc64" [label="@esbuild/aix-ppc64", shape=box];
  "component:external:node:@esbuild/android-arm" [label="@esbuild/android-arm", shape=box];
  "component:external:node:@esbuild/android-arm64" [label="@esbuild/android-arm64", shape=box];
  "component:external:node:@esbuild/android-x64" [label="@esbuild/android-x64", shape=box];
  "component:external:node:@esbuild/darwin-arm64" [label="@esbuild/darwin-arm64", shape=box];
  "component:external:node:@esbuild/darwin-x64" [label="@esbuild/darwin-x64", shape=box];
  "component:external:node:@esbuild/freebsd-arm64" [label="@esbuild/freebsd-arm64", shape=box];
  "repository" -> "component:docs" [label="contains"];
  "repository" -> "component:external:node:@esbuild/aix-ppc64" [label="contains"];
  "repository" -> "component:external:node:@esbuild/android-arm" [label="contains"];
  "repository" -> "component:external:node:@esbuild/android-arm64" [label="contains"];
  "repository" -> "component:external:node:@esbuild/android-x64" [label="contains"];
  "repository" -> "component:external:node:@esbuild/darwin-arm64" [label="contains"];
  "repository" -> "component:external:node:@esbuild/darwin-x64" [label="contains"];
  "repository" -> "component:external:node:@esbuild/freebsd-arm64" [label="contains"];
}

```

Structured graph:
- nodes: 9
- edges: 8

Layout:
- direction: LR
- strategy: root-spoke

Simplification:
- simplified: yes
- rendered nodes: 9
- rendered edges: 8
- omitted nodes: 223
- omitted edges: 223
- Omitted 223 lower-priority components to keep the overview readable.
- Switched to a left-to-right root-spoke layout to keep the largest components scannable.

Why these edges:
- Repository contains HelixMCP as a prominent component.
- Repository contains HelixMCP as a prominent component.
- Repository contains HelixMCP as a prominent component.
- Repository contains HelixMCP as a prominent component.
- Repository contains HelixMCP as a prominent component.
- Repository contains HelixMCP as a prominent component.

## Dependency Graph

Shows a sampled set of dependency and call relationships across indexed entities.

Explained in:
- [Graph Hotspots](architecture.md#architecture-hotspots)
- [Design-Shaping Dependencies](dependencies.md#design-shaping-dependencies)
- [Navigation Guidance](dependencies.md#dependency-guidance)

Interpretation note:
- Interpretation: use this graph to spot concentrated dependency hubs and outward package pressure across the repository. Favor it when you need to reason about coupling, likely blast radius, or external dependency concentration.

Rendered surface:
- rendered nodes: 15, rendered edges: 16

Node mix:
- symbol: 15

Omitted surface:
- omitted nodes: 0
- omitted edges: 1511

```mermaid
graph LR
  symbol_scripts_bundle_export_ts_args_44 --> symbol_scripts_bundle_export_ts_parseArgs_24
  symbol_scripts_bundle_export_ts_args_44 --> symbol_src_artifacts_localObjectStore_ts_slice_82
  symbol_scripts_bundle_export_ts_db_70 --> symbol_src_db_connection_ts_createDb_9
  symbol_scripts_bundle_export_ts_key_30 --> symbol_src_artifacts_localObjectStore_ts_slice_82
  symbol_scripts_bundle_export_ts_main_43 --> symbol_scripts_bundle_export_ts_parseArgs_24
  symbol_scripts_bundle_export_ts_main_43 --> symbol_scripts_bundle_export_ts_usage_12
  symbol_scripts_bundle_export_ts_main_43 --> symbol_src_artifacts_localObjectStore_ts_slice_82
  symbol_scripts_bundle_export_ts_main_43 --> symbol_src_bundle_bundleExport_ts_exportBundleToDir_175
  symbol_scripts_bundle_export_ts_main_43 --> symbol_src_bundle_bundleTar_ts_bundleDirToDeterministicTar_25
  symbol_scripts_bundle_export_ts_main_43 --> symbol_src_db_connection_ts_createDb_9
  symbol_scripts_bundle_export_ts_main_43 --> symbol_src_db_connection_ts_createPgPool_5
  symbol_scripts_bundle_export_ts_main_43 --> symbol_src_execution_backends_dockerRunner_ts_rm_83
  symbol_scripts_bundle_export_ts_parseArgs_24 --> symbol_src_artifacts_localObjectStore_ts_slice_82
  symbol_scripts_bundle_export_ts_pool_69 --> symbol_src_db_connection_ts_createPgPool_5
  symbol_scripts_bundle_export_ts_res_101 --> symbol_src_bundle_bundleExport_ts_exportBundleToDir_175
  symbol_scripts_bundle_export_ts_res_78 --> symbol_src_bundle_bundleExport_ts_exportBundleToDir_175

```

```dot
digraph RepoIntel {
  label="Dependency Graph";
  labelloc=t;
  rankdir=LR;
  node [shape=box];
  "symbol:scripts/bundle_export.ts:args:44" [label="args", shape=box];
  "symbol:scripts/bundle_export.ts:parseArgs:24" [label="parseArgs", shape=box];
  "symbol:src/artifacts/localObjectStore.ts:slice:82" [label="slice", shape=box];
  "symbol:scripts/bundle_export.ts:db:70" [label="db", shape=box];
  "symbol:src/db/connection.ts:createDb:9" [label="createDb", shape=box];
  "symbol:scripts/bundle_export.ts:key:30" [label="key", shape=box];
  "symbol:scripts/bundle_export.ts:main:43" [label="main", shape=box];
  "symbol:scripts/bundle_export.ts:usage:12" [label="usage", shape=box];
  "symbol:src/bundle/bundleExport.ts:exportBundleToDir:175" [label="exportBundleToDir", shape=box];
  "symbol:src/bundle/bundleTar.ts:bundleDirToDeterministicTar:25" [label="bundleDirToDeterministicTar", shape=box];
  "symbol:src/db/connection.ts:createPgPool:5" [label="createPgPool", shape=box];
  "symbol:src/execution/backends/dockerRunner.ts:rm:83" [label="rm", shape=box];
  "symbol:scripts/bundle_export.ts:pool:69" [label="pool", shape=box];
  "symbol:scripts/bundle_export.ts:res:101" [label="res", shape=box];
  "symbol:scripts/bundle_export.ts:res:78" [label="res", shape=box];
  "symbol:scripts/bundle_export.ts:args:44" -> "symbol:scripts/bundle_export.ts:parseArgs:24" [label="calls"];
  "symbol:scripts/bundle_export.ts:args:44" -> "symbol:src/artifacts/localObjectStore.ts:slice:82" [label="calls"];
  "symbol:scripts/bundle_export.ts:db:70" -> "symbol:src/db/connection.ts:createDb:9" [label="calls"];
  "symbol:scripts/bundle_export.ts:key:30" -> "symbol:src/artifacts/localObjectStore.ts:slice:82" [label="calls"];
  "symbol:scripts/bundle_export.ts:main:43" -> "symbol:scripts/bundle_export.ts:parseArgs:24" [label="calls"];
  "symbol:scripts/bundle_export.ts:main:43" -> "symbol:scripts/bundle_export.ts:usage:12" [label="calls"];
  "symbol:scripts/bundle_export.ts:main:43" -> "symbol:src/artifacts/localObjectStore.ts:slice:82" [label="calls"];
  "symbol:scripts/bundle_export.ts:main:43" -> "symbol:src/bundle/bundleExport.ts:exportBundleToDir:175" [label="calls"];
  "symbol:scripts/bundle_export.ts:main:43" -> "symbol:src/bundle/bundleTar.ts:bundleDirToDeterministicTar:25" [label="calls"];
  "symbol:scripts/bundle_export.ts:main:43" -> "symbol:src/db/connection.ts:createDb:9" [label="calls"];
  "symbol:scripts/bundle_export.ts:main:43" -> "symbol:src/db/connection.ts:createPgPool:5" [label="calls"];
  "symbol:scripts/bundle_export.ts:main:43" -> "symbol:src/execution/backends/dockerRunner.ts:rm:83" [label="calls"];
  "symbol:scripts/bundle_export.ts:parseArgs:24" -> "symbol:src/artifacts/localObjectStore.ts:slice:82" [label="calls"];
  "symbol:scripts/bundle_export.ts:pool:69" -> "symbol:src/db/connection.ts:createPgPool:5" [label="calls"];
  "symbol:scripts/bundle_export.ts:res:101" -> "symbol:src/bundle/bundleExport.ts:exportBundleToDir:175" [label="calls"];
  "symbol:scripts/bundle_export.ts:res:78" -> "symbol:src/bundle/bundleExport.ts:exportBundleToDir:175" [label="calls"];
}

```

Structured graph:
- nodes: 15
- edges: 16

Layout:
- direction: LR
- strategy: edge-ranked

Simplification:
- simplified: yes
- rendered nodes: 15
- rendered edges: 16
- omitted nodes: 0
- omitted edges: 1511
- Omitted 1511 lower-priority dependency edges to avoid an unreadable graph.
- Kept a rank-ordered sample of stronger edges and switched to a left-to-right layout for denser graphs.

Why these edges:
- symbol:scripts/bundle_export.ts:args:44 calls symbol:scripts/bundle_export.ts:parseArgs:24 via scripts/bundle_export.ts.
- symbol:scripts/bundle_export.ts:args:44 calls symbol:src/artifacts/localObjectStore.ts:slice:82 via scripts/bundle_export.ts.
- symbol:scripts/bundle_export.ts:db:70 calls symbol:src/db/connection.ts:createDb:9 via scripts/bundle_export.ts.
- symbol:scripts/bundle_export.ts:key:30 calls symbol:src/artifacts/localObjectStore.ts:slice:82 via scripts/bundle_export.ts.
- symbol:scripts/bundle_export.ts:main:43 calls symbol:scripts/bundle_export.ts:parseArgs:24 via scripts/bundle_export.ts.
- symbol:scripts/bundle_export.ts:main:43 calls symbol:scripts/bundle_export.ts:usage:12 via scripts/bundle_export.ts.
- symbol:scripts/bundle_export.ts:main:43 calls symbol:src/artifacts/localObjectStore.ts:slice:82 via scripts/bundle_export.ts.
- symbol:scripts/bundle_export.ts:main:43 calls symbol:src/bundle/bundleExport.ts:exportBundleToDir:175 via scripts/bundle_export.ts.
- symbol:scripts/bundle_export.ts:main:43 calls symbol:src/bundle/bundleTar.ts:bundleDirToDeterministicTar:25 via scripts/bundle_export.ts.
- symbol:scripts/bundle_export.ts:main:43 calls symbol:src/db/connection.ts:createDb:9 via scripts/bundle_export.ts.

## Directory Map

Shows top-level directory layout to orient unfamiliar agents.

Interpretation note:
- Interpretation: use this map to orient yourself in the repository layout before reading code. Favor it when you need to connect top-level paths to the graph surfaces shown elsewhere.

Rendered surface:
- rendered nodes: 9, rendered edges: 8

Node mix:
- directory: 8, repository: 1

```mermaid
graph TD
  repository["HelixMCP"] --> _github[".github/"]
  repository["HelixMCP"] --> contracts["contracts/"]
  repository["HelixMCP"] --> db["db/"]
  repository["HelixMCP"] --> docs["docs/"]
  repository["HelixMCP"] --> policies["policies/"]
  repository["HelixMCP"] --> scripts["scripts/"]
  repository["HelixMCP"] --> src["src/"]
  repository["HelixMCP"] --> tests["tests/"]

```

```dot
digraph RepoIntel {
  label="Directory Map";
  labelloc=t;
  rankdir=TB;
  node [shape=box];
  "repository" [label="HelixMCP", shape=box];
  ".github" [label=".github/", shape=box];
  "contracts" [label="contracts/", shape=box];
  "db" [label="db/", shape=box];
  "docs" [label="docs/", shape=box];
  "policies" [label="policies/", shape=box];
  "scripts" [label="scripts/", shape=box];
  "src" [label="src/", shape=box];
  "tests" [label="tests/", shape=box];
  "repository" -> ".github" [label="contains"];
  "repository" -> "contracts" [label="contains"];
  "repository" -> "db" [label="contains"];
  "repository" -> "docs" [label="contains"];
  "repository" -> "policies" [label="contains"];
  "repository" -> "scripts" [label="contains"];
  "repository" -> "src" [label="contains"];
  "repository" -> "tests" [label="contains"];
}

```

Structured graph:
- nodes: 9
- edges: 8

Layout:
- direction: TD
- strategy: linear-map

Simplification:
- simplified: no
- rendered nodes: 9
- rendered edges: 8
- omitted nodes: 0
- omitted edges: 0

Why these edges:
- .github/ is a top-level directory under the repository root.
- contracts/ is a top-level directory under the repository root.
- db/ is a top-level directory under the repository root.
- docs/ is a top-level directory under the repository root.
- policies/ is a top-level directory under the repository root.
- scripts/ is a top-level directory under the repository root.
- src/ is a top-level directory under the repository root.
- tests/ is a top-level directory under the repository root.

## Subsystem Clusters

Shows a simplified subsystem graph grouped by dominant repository paths and graph-connected merges.

Explained in:
- [Subsystem Clusters](architecture.md#architecture-subsystems)
- [Architecture Summary](architecture.md#architecture-summary)

Interpretation note:
- Interpretation: use this clustering view to understand which source areas act like larger architectural slices and how strongly they connect. Favor it when you need a quick map of architectural boundaries instead of individual files or packages.

Rendered surface:
- rendered nodes: 3, rendered edges: 1

Node mix:
- subsystem: 3

Omitted surface:
- omitted nodes: 0
- omitted edges: 839

```mermaid
graph TD
  subgraph group_docs["docs/"]
    subsystem_docs["docs"]
  end
  subgraph group_package_json["package.json/"]
    subsystem_external["external"]
  end
  subgraph group_src["src/"]
    subsystem_src["src"]
  end

  subsystem_src --> subsystem_external

```

```dot
digraph RepoIntel {
  label="Subsystem Clusters";
  labelloc=t;
  rankdir=TB;
  node [shape=box];
  "subsystem:external" [label="external", shape=box];
  "subsystem:src" [label="src", shape=box];
  "subsystem:docs" [label="docs", shape=box];
  "subsystem:src" -> "subsystem:external" [label="depends_on", weight=49, penwidth=6];
}

```

Structured graph:
- nodes: 3
- edges: 1

Layout:
- direction: TD
- strategy: hierarchy-ranked

Simplification:
- simplified: yes
- rendered nodes: 3
- rendered edges: 1
- omitted nodes: 0
- omitted edges: 839
- Collapsed 839 additional subsystem edges from the rendered view.
- Grouped subsystem nodes by dominant path segment across 3 hierarchy buckets before rendering edges.

Why these edges:
- src depends_on external via src/core/ids.ts. 48 additional inferred edges reinforce this path. (49 inferred edges combined.)