---
page_id: architecture
page_type: architecture
generation_mode: inferred
freshness_status: new
updated_at: 2026-04-18T05:42:41.553Z
---

<details>
<summary>Build metadata</summary>

```json
{
  "freshnessKey": "5f6779c43bd875db379cd6c17fff43ecb228b429",
  "plannerReason": "Inferred architecture page summarizing repo shape and component structure.",
  "changedPaths": [
    "docs/architecture.md",
    "docs/bundle_export.md",
    "docs/slurm_cluster_smoke.md",
    "README.md",
    "src/toolpacks/builtin/qcBundleFastq.ts",
    "src/mcp/gatewayServer.ts",
    "src/bundle/bundleExport.ts",
    "src/core/canonicalJson.ts",
    "src/mcp/toolSchemas.ts",
    "package.json",
    "src/artifacts/artifactService.ts",
    "src/artifacts/localObjectStore.ts",
    "tests/artifactService.test.ts",
    "tests/bundleExport.test.ts",
    "tests/contracts.test.ts",
    "pnpm dev",
    "src/bundle/bundleTar.ts",
    "package-lock.json",
    "tests/gateway.integration.test.ts"
  ],
  "dependencyPaths": [
    "docs/architecture.md",
    "docs/bundle_export.md",
    "docs/slurm_cluster_smoke.md",
    "README.md",
    "src/toolpacks/builtin/qcBundleFastq.ts",
    "src/mcp/gatewayServer.ts",
    "src/bundle/bundleExport.ts",
    "src/core/canonicalJson.ts",
    "src/mcp/toolSchemas.ts",
    "package.json",
    "src/artifacts/artifactService.ts",
    "src/artifacts/localObjectStore.ts",
    "tests/artifactService.test.ts",
    "tests/bundleExport.test.ts",
    "tests/contracts.test.ts",
    "pnpm dev",
    "src/bundle/bundleTar.ts",
    "package-lock.json",
    "tests/gateway.integration.test.ts"
  ],
  "dependencyEvidenceIds": [
    "ingest:file:src/toolpacks/builtin/qcBundleFastq.ts",
    "ingest:file:src/toolpacks/backendSelection.ts",
    "ingest:file:src/mcp/gatewayServer.ts",
    "ingest:file:src/mcp/envSnapshot.ts",
    "ingest:file:src/toolpacks/docker/executeDocker.ts",
    "ingest:file:src/runs/runIdentity.ts",
    "ingest:file:src/core/canonicalJson.ts",
    "ingest:file:scripts/bundle_export.ts",
    "ingest:file:src/execution/workspace.ts",
    "ingest:file:src/index.ts",
    "ingest:file:src/store/postgresStore.ts",
    "ingest:file:src/execution/executionService.ts",
    "ingest:file:src/execution/slurm/slurmScriptV1.ts",
    "ingest:file:src/runs/toolRun.ts",
    "ingest:file:src/toolpacks/register.ts",
    "ingest:file:src/toolpacks/builtin/fastqc.ts",
    "ingest:file:src/toolpacks/builtin/multiqc.ts",
    "ingest:file:src/toolpacks/builtin/samtoolsFlagstat.ts",
    "ingest:file:src/toolpacks/builtin/samtoolsFlagstatSlurm.ts",
    "ingest:file:tests/contracts.test.ts",
    "ingest:file:tests/slurm.integration.test.ts",
    "ingest:file:src/bundle/bundleExport.ts",
    "ingest:file:src/bundle/bundleVerify.ts",
    "ingest:file:src/artifacts/localObjectStore.ts",
    "ingest:file:src/core/ids.ts",
    "ingest:file:tests/bundleExport.test.ts",
    "ingest:file:src/policy/policy.ts",
    "ingest:file:src/mcp/toolSchemas.ts",
    "ingest:file:src/toolpacks/builtin/seqkitStats.ts",
    "component:package.json",
    "ingest:file:src/artifacts/artifactService.ts",
    "ingest:file:src/bundle/bundleTar.ts",
    "ingest:file:src/bundle/manifest.ts",
    "ingest:file:src/core/artifact.ts",
    "ingest:file:src/core/detectArtifactType.ts",
    "ingest:file:tests/artifactService.test.ts",
    "ingest:file:tests/gateway.integration.test.ts",
    "ingest:file:tests/policy.test.ts",
    "ingest:file:tests/runLifecycle.test.ts",
    "ingest:file:tests/toolpacks.test.ts",
    "ingest:file:docs/architecture.md",
    "ingest:file:docs/bundle_export.md",
    "ingest:file:docs/slurm_cluster_smoke.md",
    "ingest:file:README.md",
    "workflow:package.json",
    "ingest:file:src/db/bootstrap.ts",
    "ingest:file:src/db/connection.ts",
    "ingest:file:src/db/types.ts",
    "ingest:file:src/toolpacks/slurm/executeSlurm.ts",
    "ingest:file:src/toolpacks/types.ts",
    "ingest:file:package.json",
    "ingest:file:tsconfig.json",
    "ingest:file:vitest.config.ts",
    "component:external:node:@esbuild/aix-ppc64",
    "component:external:node:@esbuild/android-arm",
    "component:external:node:@esbuild/android-arm64",
    "component:external:node:@esbuild/android-x64",
    "component:external:node:@esbuild/darwin-arm64",
    "component:external:node:@esbuild/darwin-x64",
    "component:external:node:@esbuild/freebsd-arm64",
    "component:external:node:@esbuild/freebsd-x64",
    "component:external:node:@esbuild/linux-arm",
    "component:external:node:@esbuild/linux-arm64",
    "component:external:node:@esbuild/linux-ia32",
    "component:external:node:@esbuild/linux-loong64",
    "component:external:node:@esbuild/linux-mips64el",
    "component:external:node:@esbuild/linux-ppc64",
    "component:external:node:@esbuild/linux-riscv64",
    "component:external:node:@esbuild/linux-s390x",
    "component:external:node:@esbuild/linux-x64",
    "component:external:node:@esbuild/netbsd-arm64",
    "component:external:node:@esbuild/netbsd-x64",
    "component:external:node:@esbuild/openbsd-arm64",
    "component:external:node:@esbuild/openbsd-x64",
    "component:external:node:@esbuild/openharmony-arm64",
    "component:external:node:@esbuild/sunos-x64",
    "component:external:node:@esbuild/win32-arm64",
    "component:external:node:@esbuild/win32-ia32",
    "component:external:node:@esbuild/win32-x64",
    "component:external:node:@hono/node-server",
    "component:external:node:@jridgewell/sourcemap-codec",
    "component:external:node:@modelcontextprotocol/sdk",
    "component:external:node:@rollup/rollup-android-arm-eabi",
    "component:external:node:@rollup/rollup-android-arm64",
    "component:external:node:@rollup/rollup-darwin-arm64",
    "component:external:node:@rollup/rollup-darwin-x64",
    "component:external:node:@rollup/rollup-freebsd-arm64",
    "component:external:node:@rollup/rollup-freebsd-x64",
    "component:external:node:@rollup/rollup-linux-arm-gnueabihf",
    "component:external:node:@rollup/rollup-linux-arm-musleabihf",
    "component:external:node:@rollup/rollup-linux-arm64-gnu",
    "component:external:node:@rollup/rollup-linux-arm64-musl",
    "component:external:node:@rollup/rollup-linux-loong64-gnu",
    "component:external:node:@rollup/rollup-linux-loong64-musl",
    "component:external:node:@rollup/rollup-linux-ppc64-gnu",
    "component:external:node:@rollup/rollup-linux-ppc64-musl",
    "component:external:node:@rollup/rollup-linux-riscv64-gnu",
    "component:external:node:@rollup/rollup-linux-riscv64-musl",
    "component:external:node:@rollup/rollup-linux-s390x-gnu",
    "component:external:node:@rollup/rollup-linux-x64-gnu",
    "component:external:node:@rollup/rollup-linux-x64-musl",
    "component:external:node:@rollup/rollup-openbsd-x64",
    "component:external:node:@rollup/rollup-openharmony-arm64",
    "component:external:node:@rollup/rollup-win32-arm64-msvc",
    "component:external:node:@rollup/rollup-win32-ia32-msvc",
    "component:external:node:@rollup/rollup-win32-x64-gnu",
    "component:external:node:@rollup/rollup-win32-x64-msvc",
    "component:external:node:@standard-schema/spec",
    "component:external:node:@types/chai",
    "component:external:node:@types/deep-eql",
    "component:external:node:@types/estree",
    "component:external:node:@types/node",
    "component:external:node:@types/pg",
    "component:external:node:@vitest/expect",
    "component:external:node:@vitest/mocker",
    "component:external:node:@vitest/pretty-format",
    "component:external:node:@vitest/runner",
    "component:external:node:@vitest/snapshot",
    "component:external:node:@vitest/spy",
    "component:external:node:@vitest/utils",
    "component:external:node:accepts",
    "component:external:node:ajv",
    "component:external:node:ajv-formats",
    "component:external:node:assertion-error",
    "component:external:node:body-parser",
    "component:external:node:bytes",
    "component:external:node:call-bind",
    "component:external:node:call-bind-apply-helpers",
    "component:external:node:call-bound",
    "component:external:node:chai",
    "component:external:node:commander",
    "component:external:node:content-disposition",
    "component:external:node:content-type",
    "component:external:node:cookie",
    "component:external:node:cookie-signature",
    "component:external:node:cors",
    "component:external:node:cross-spawn",
    "component:external:node:debug",
    "component:external:node:define-data-property",
    "component:external:node:depd",
    "component:external:node:discontinuous-range",
    "component:external:node:dunder-proto",
    "component:external:node:ee-first",
    "component:external:node:encodeurl",
    "component:external:node:es-define-property",
    "component:external:node:es-errors",
    "component:external:node:es-module-lexer",
    "component:external:node:es-object-atoms",
    "component:external:node:esbuild",
    "component:external:node:escape-html",
    "component:external:node:estree-walker",
    "component:external:node:etag",
    "component:external:node:eventsource",
    "component:external:node:eventsource-parser",
    "component:external:node:expect-type",
    "component:external:node:express",
    "component:external:node:express-rate-limit",
    "component:external:node:fast-deep-equal",
    "component:external:node:fast-uri",
    "component:external:node:fdir",
    "component:external:node:finalhandler",
    "component:external:node:forwarded",
    "component:external:node:fresh",
    "component:external:node:fsevents",
    "component:external:node:function-bind",
    "component:external:node:functional-red-black-tree",
    "component:external:node:get-intrinsic",
    "component:external:node:get-proto",
    "component:external:node:get-tsconfig",
    "component:external:node:gopd",
    "component:external:node:has-property-descriptors",
    "component:external:node:has-symbols",
    "component:external:node:hasown",
    "component:external:node:hono",
    "component:external:node:http-errors",
    "component:external:node:iconv-lite",
    "component:external:node:immutable",
    "component:external:node:inherits",
    "component:external:node:ipaddr.js",
    "component:external:node:is-promise",
    "component:external:node:isarray",
    "component:external:node:isexe",
    "component:external:node:jose",
    "component:external:node:json-schema-traverse",
    "component:external:node:json-schema-typed",
    "component:external:node:json-stable-stringify",
    "component:external:node:jsonify",
    "component:external:node:kysely",
    "component:external:node:lru-cache",
    "component:external:node:magic-string",
    "component:external:node:math-intrinsics",
    "component:external:node:media-typer",
    "component:external:node:merge-descriptors",
    "component:external:node:mime-db",
    "component:external:node:mime-types",
    "component:external:node:moment",
    "component:external:node:moo",
    "component:external:node:ms",
    "component:external:node:nanoid",
    "component:external:node:nearley",
    "component:external:node:negotiator",
    "component:external:node:object-assign",
    "component:external:node:object-hash",
    "component:external:node:object-inspect",
    "component:external:node:object-keys",
    "component:external:node:obug",
    "component:external:node:on-finished",
    "component:external:node:once",
    "component:external:node:parseurl",
    "component:external:node:path-key",
    "component:external:node:path-to-regexp",
    "component:external:node:pathe",
    "component:external:node:pg",
    "component:external:node:pg-cloudflare",
    "component:external:node:pg-connection-string",
    "component:external:node:pg-int8",
    "component:external:node:pg-mem",
    "component:external:node:pg-pool",
    "component:external:node:pg-protocol",
    "component:external:node:pg-types",
    "component:external:node:pgpass",
    "component:external:node:pgsql-ast-parser",
    "component:external:node:picocolors",
    "component:external:node:picomatch",
    "component:external:node:pkce-challenge",
    "component:external:node:postcss",
    "component:external:node:postgres-array",
    "component:external:node:postgres-bytea",
    "component:external:node:postgres-date",
    "component:external:node:postgres-interval",
    "component:external:node:proxy-addr",
    "component:external:node:qs",
    "component:external:node:railroad-diagrams",
    "component:external:node:randexp",
    "component:external:node:range-parser",
    "component:external:node:raw-body",
    "component:external:node:require-from-string",
    "component:external:node:resolve-pkg-maps",
    "component:external:node:ret",
    "component:external:node:rollup",
    "component:external:node:router",
    "component:external:node:safer-buffer",
    "component:external:node:send",
    "component:external:node:serve-static",
    "component:external:node:set-function-length",
    "component:external:node:setprototypeof",
    "component:external:node:shebang-command",
    "component:external:node:shebang-regex",
    "component:external:node:side-channel",
    "component:external:node:side-channel-list",
    "component:external:node:side-channel-map",
    "component:external:node:side-channel-weakmap",
    "component:external:node:siginfo",
    "component:external:node:source-map-js",
    "component:external:node:split2",
    "component:external:node:stackback",
    "component:external:node:statuses",
    "component:external:node:std-env",
    "component:external:node:tinybench",
    "component:external:node:tinyexec",
    "component:external:node:tinyglobby",
    "component:external:node:tinyrainbow",
    "component:external:node:toidentifier",
    "component:external:node:tsx",
    "component:external:node:type-is",
    "component:external:node:typescript",
    "component:external:node:ulid",
    "component:external:node:undici-types",
    "component:external:node:unpipe",
    "component:external:node:vary",
    "component:external:node:vite",
    "component:external:node:vitest",
    "component:external:node:which",
    "component:external:node:why-is-node-running",
    "component:external:node:wrappy",
    "component:external:node:xtend",
    "component:external:node:yallist",
    "component:external:node:yaml",
    "component:external:node:zod",
    "component:external:node:zod-to-json-schema",
    "component:src",
    "component:tests",
    "component:docs"
  ],
  "evidenceIds": [
    "ingest:file:src/toolpacks/builtin/qcBundleFastq.ts",
    "ingest:file:src/toolpacks/backendSelection.ts",
    "ingest:file:src/mcp/gatewayServer.ts",
    "ingest:file:src/mcp/envSnapshot.ts",
    "ingest:file:src/toolpacks/docker/executeDocker.ts",
    "ingest:file:src/runs/runIdentity.ts",
    "ingest:file:src/core/canonicalJson.ts",
    "ingest:file:scripts/bundle_export.ts",
    "ingest:file:src/execution/workspace.ts",
    "ingest:file:src/index.ts",
    "ingest:file:src/store/postgresStore.ts",
    "ingest:file:src/execution/executionService.ts",
    "ingest:file:src/execution/slurm/slurmScriptV1.ts",
    "ingest:file:src/runs/toolRun.ts",
    "ingest:file:src/toolpacks/register.ts",
    "ingest:file:src/toolpacks/builtin/fastqc.ts",
    "ingest:file:src/toolpacks/builtin/multiqc.ts",
    "ingest:file:src/toolpacks/builtin/samtoolsFlagstat.ts",
    "ingest:file:src/toolpacks/builtin/samtoolsFlagstatSlurm.ts",
    "ingest:file:tests/contracts.test.ts",
    "ingest:file:tests/slurm.integration.test.ts",
    "ingest:file:src/bundle/bundleExport.ts",
    "ingest:file:src/bundle/bundleVerify.ts",
    "ingest:file:src/artifacts/localObjectStore.ts",
    "ingest:file:src/core/ids.ts",
    "ingest:file:tests/bundleExport.test.ts",
    "ingest:file:src/policy/policy.ts",
    "ingest:file:src/mcp/toolSchemas.ts",
    "ingest:file:src/toolpacks/builtin/seqkitStats.ts",
    "component:package.json",
    "ingest:file:src/artifacts/artifactService.ts",
    "ingest:file:src/bundle/bundleTar.ts",
    "ingest:file:src/bundle/manifest.ts",
    "ingest:file:src/core/artifact.ts",
    "ingest:file:src/core/detectArtifactType.ts",
    "ingest:file:tests/artifactService.test.ts",
    "ingest:file:tests/gateway.integration.test.ts",
    "ingest:file:tests/policy.test.ts",
    "ingest:file:tests/runLifecycle.test.ts",
    "ingest:file:tests/toolpacks.test.ts",
    "ingest:file:docs/architecture.md",
    "ingest:file:docs/bundle_export.md",
    "ingest:file:docs/slurm_cluster_smoke.md",
    "ingest:file:README.md",
    "workflow:package.json",
    "ingest:file:src/db/bootstrap.ts",
    "ingest:file:src/db/connection.ts",
    "ingest:file:src/db/types.ts",
    "ingest:file:src/toolpacks/slurm/executeSlurm.ts",
    "ingest:file:src/toolpacks/types.ts",
    "ingest:file:package.json",
    "ingest:file:tsconfig.json",
    "ingest:file:vitest.config.ts",
    "component:external:node:@esbuild/aix-ppc64",
    "component:external:node:@esbuild/android-arm",
    "component:external:node:@esbuild/android-arm64",
    "component:external:node:@esbuild/android-x64",
    "component:external:node:@esbuild/darwin-arm64",
    "component:external:node:@esbuild/darwin-x64",
    "component:external:node:@esbuild/freebsd-arm64",
    "component:external:node:@esbuild/freebsd-x64",
    "component:external:node:@esbuild/linux-arm",
    "component:external:node:@esbuild/linux-arm64",
    "component:external:node:@esbuild/linux-ia32",
    "component:external:node:@esbuild/linux-loong64",
    "component:external:node:@esbuild/linux-mips64el",
    "component:external:node:@esbuild/linux-ppc64",
    "component:external:node:@esbuild/linux-riscv64",
    "component:external:node:@esbuild/linux-s390x",
    "component:external:node:@esbuild/linux-x64",
    "component:external:node:@esbuild/netbsd-arm64",
    "component:external:node:@esbuild/netbsd-x64",
    "component:external:node:@esbuild/openbsd-arm64",
    "component:external:node:@esbuild/openbsd-x64",
    "component:external:node:@esbuild/openharmony-arm64",
    "component:external:node:@esbuild/sunos-x64",
    "component:external:node:@esbuild/win32-arm64",
    "component:external:node:@esbuild/win32-ia32",
    "component:external:node:@esbuild/win32-x64",
    "component:external:node:@hono/node-server",
    "component:external:node:@jridgewell/sourcemap-codec",
    "component:external:node:@modelcontextprotocol/sdk",
    "component:external:node:@rollup/rollup-android-arm-eabi",
    "component:external:node:@rollup/rollup-android-arm64",
    "component:external:node:@rollup/rollup-darwin-arm64",
    "component:external:node:@rollup/rollup-darwin-x64",
    "component:external:node:@rollup/rollup-freebsd-arm64",
    "component:external:node:@rollup/rollup-freebsd-x64",
    "component:external:node:@rollup/rollup-linux-arm-gnueabihf",
    "component:external:node:@rollup/rollup-linux-arm-musleabihf",
    "component:external:node:@rollup/rollup-linux-arm64-gnu",
    "component:external:node:@rollup/rollup-linux-arm64-musl",
    "component:external:node:@rollup/rollup-linux-loong64-gnu",
    "component:external:node:@rollup/rollup-linux-loong64-musl",
    "component:external:node:@rollup/rollup-linux-ppc64-gnu",
    "component:external:node:@rollup/rollup-linux-ppc64-musl",
    "component:external:node:@rollup/rollup-linux-riscv64-gnu",
    "component:external:node:@rollup/rollup-linux-riscv64-musl",
    "component:external:node:@rollup/rollup-linux-s390x-gnu",
    "component:external:node:@rollup/rollup-linux-x64-gnu",
    "component:external:node:@rollup/rollup-linux-x64-musl",
    "component:external:node:@rollup/rollup-openbsd-x64",
    "component:external:node:@rollup/rollup-openharmony-arm64",
    "component:external:node:@rollup/rollup-win32-arm64-msvc",
    "component:external:node:@rollup/rollup-win32-ia32-msvc",
    "component:external:node:@rollup/rollup-win32-x64-gnu",
    "component:external:node:@rollup/rollup-win32-x64-msvc",
    "component:external:node:@standard-schema/spec",
    "component:external:node:@types/chai",
    "component:external:node:@types/deep-eql",
    "component:external:node:@types/estree",
    "component:external:node:@types/node",
    "component:external:node:@types/pg",
    "component:external:node:@vitest/expect",
    "component:external:node:@vitest/mocker",
    "component:external:node:@vitest/pretty-format",
    "component:external:node:@vitest/runner",
    "component:external:node:@vitest/snapshot",
    "component:external:node:@vitest/spy",
    "component:external:node:@vitest/utils",
    "component:external:node:accepts",
    "component:external:node:ajv",
    "component:external:node:ajv-formats",
    "component:external:node:assertion-error",
    "component:external:node:body-parser",
    "component:external:node:bytes",
    "component:external:node:call-bind",
    "component:external:node:call-bind-apply-helpers",
    "component:external:node:call-bound",
    "component:external:node:chai",
    "component:external:node:commander",
    "component:external:node:content-disposition",
    "component:external:node:content-type",
    "component:external:node:cookie",
    "component:external:node:cookie-signature",
    "component:external:node:cors",
    "component:external:node:cross-spawn",
    "component:external:node:debug",
    "component:external:node:define-data-property",
    "component:external:node:depd",
    "component:external:node:discontinuous-range",
    "component:external:node:dunder-proto",
    "component:external:node:ee-first",
    "component:external:node:encodeurl",
    "component:external:node:es-define-property",
    "component:external:node:es-errors",
    "component:external:node:es-module-lexer",
    "component:external:node:es-object-atoms",
    "component:external:node:esbuild",
    "component:external:node:escape-html",
    "component:external:node:estree-walker",
    "component:external:node:etag",
    "component:external:node:eventsource",
    "component:external:node:eventsource-parser",
    "component:external:node:expect-type",
    "component:external:node:express",
    "component:external:node:express-rate-limit",
    "component:external:node:fast-deep-equal",
    "component:external:node:fast-uri",
    "component:external:node:fdir",
    "component:external:node:finalhandler",
    "component:external:node:forwarded",
    "component:external:node:fresh",
    "component:external:node:fsevents",
    "component:external:node:function-bind",
    "component:external:node:functional-red-black-tree",
    "component:external:node:get-intrinsic",
    "component:external:node:get-proto",
    "component:external:node:get-tsconfig",
    "component:external:node:gopd",
    "component:external:node:has-property-descriptors",
    "component:external:node:has-symbols",
    "component:external:node:hasown",
    "component:external:node:hono",
    "component:external:node:http-errors",
    "component:external:node:iconv-lite",
    "component:external:node:immutable",
    "component:external:node:inherits",
    "component:external:node:ipaddr.js",
    "component:external:node:is-promise",
    "component:external:node:isarray",
    "component:external:node:isexe",
    "component:external:node:jose",
    "component:external:node:json-schema-traverse",
    "component:external:node:json-schema-typed",
    "component:external:node:json-stable-stringify",
    "component:external:node:jsonify",
    "component:external:node:kysely",
    "component:external:node:lru-cache",
    "component:external:node:magic-string",
    "component:external:node:math-intrinsics",
    "component:external:node:media-typer",
    "component:external:node:merge-descriptors",
    "component:external:node:mime-db",
    "component:external:node:mime-types",
    "component:external:node:moment",
    "component:external:node:moo",
    "component:external:node:ms",
    "component:external:node:nanoid",
    "component:external:node:nearley",
    "component:external:node:negotiator",
    "component:external:node:object-assign",
    "component:external:node:object-hash",
    "component:external:node:object-inspect",
    "component:external:node:object-keys",
    "component:external:node:obug",
    "component:external:node:on-finished",
    "component:external:node:once",
    "component:external:node:parseurl",
    "component:external:node:path-key",
    "component:external:node:path-to-regexp",
    "component:external:node:pathe",
    "component:external:node:pg",
    "component:external:node:pg-cloudflare",
    "component:external:node:pg-connection-string",
    "component:external:node:pg-int8",
    "component:external:node:pg-mem",
    "component:external:node:pg-pool",
    "component:external:node:pg-protocol",
    "component:external:node:pg-types",
    "component:external:node:pgpass",
    "component:external:node:pgsql-ast-parser",
    "component:external:node:picocolors",
    "component:external:node:picomatch",
    "component:external:node:pkce-challenge",
    "component:external:node:postcss",
    "component:external:node:postgres-array",
    "component:external:node:postgres-bytea",
    "component:external:node:postgres-date",
    "component:external:node:postgres-interval",
    "component:external:node:proxy-addr",
    "component:external:node:qs",
    "component:external:node:railroad-diagrams",
    "component:external:node:randexp",
    "component:external:node:range-parser",
    "component:external:node:raw-body",
    "component:external:node:require-from-string",
    "component:external:node:resolve-pkg-maps",
    "component:external:node:ret",
    "component:external:node:rollup",
    "component:external:node:router",
    "component:external:node:safer-buffer",
    "component:external:node:send",
    "component:external:node:serve-static",
    "component:external:node:set-function-length",
    "component:external:node:setprototypeof",
    "component:external:node:shebang-command",
    "component:external:node:shebang-regex",
    "component:external:node:side-channel",
    "component:external:node:side-channel-list",
    "component:external:node:side-channel-map",
    "component:external:node:side-channel-weakmap",
    "component:external:node:siginfo",
    "component:external:node:source-map-js",
    "component:external:node:split2",
    "component:external:node:stackback",
    "component:external:node:statuses",
    "component:external:node:std-env",
    "component:external:node:tinybench",
    "component:external:node:tinyexec",
    "component:external:node:tinyglobby",
    "component:external:node:tinyrainbow",
    "component:external:node:toidentifier",
    "component:external:node:tsx",
    "component:external:node:type-is",
    "component:external:node:typescript",
    "component:external:node:ulid",
    "component:external:node:undici-types",
    "component:external:node:unpipe",
    "component:external:node:vary",
    "component:external:node:vite",
    "component:external:node:vitest",
    "component:external:node:which",
    "component:external:node:why-is-node-running",
    "component:external:node:wrappy",
    "component:external:node:xtend",
    "component:external:node:yallist",
    "component:external:node:yaml",
    "component:external:node:zod",
    "component:external:node:zod-to-json-schema",
    "component:src",
    "component:tests",
    "component:docs"
  ],
  "qualityWarnings": []
}

```
</details>

# Architecture

High-level architecture for HelixMCP.

## Related Pages

- [components](components.md)
- [workflows](workflows.md)
- [dependencies](dependencies.md)

## Architecture Summary

Detected ecosystems:
- node

Top-level directories:
- `.github/`
- `contracts/`
- `db/`
- `docs/`
- `policies/`
- `scripts/`
- `src/`
- `tests/`

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

## How This Actually Works

Insufficient evidence to narrate one concrete implementation path through this repository.

<details>
<summary>Supporting citations:</summary>

- none
</details>


## Key Abstractions

### qcBundleFastqTool core implementation module
What it is: qcBundleFastqTool core implementation module acts as an inferred core implementation module built around `qcBundleFastqTool` in `src/toolpacks/builtin/qcBundleFastq.ts`.
What it controls: Controls a visible slice of src behavior instead of acting as passive inventory.
If you change it: Changing qcbundlefastqtool core implementation module can shift a central behavior boundary rather than only renaming an internal helper.

<details>
<summary>Supporting citations:</summary>

- `src/toolpacks/builtin/qcBundleFastq.ts:255`
- `src/toolpacks/builtin/qcBundleFastq.ts:160`
</details>

### createGatewayServer public orchestration module
What it is: createGatewayServer public orchestration module acts as an inferred public orchestration module built around `createGatewayServer`, `GatewayDeps` in `src/mcp/gatewayServer.ts`.
What it controls: Controls how src coordinates neighboring implementation units.
If you change it: Changing creategatewayserver public orchestration module can shift a central behavior boundary rather than only renaming an internal helper.

<details>
<summary>Supporting citations:</summary>

- `src/mcp/gatewayServer.ts:1491`
- `src/mcp/gatewayServer.ts:85`
</details>

### ExportBundleDeps core implementation module
What it is: ExportBundleDeps core implementation module acts as an inferred core implementation module built around `ExportBundleDeps`, `ExportBundleOptions` in `src/bundle/bundleExport.ts`.
What it controls: Controls a visible slice of src behavior instead of acting as passive inventory.
If you change it: Changing exportbundledeps core implementation module can shift a central behavior boundary rather than only renaming an internal helper.

<details>
<summary>Supporting citations:</summary>

- `src/bundle/bundleExport.ts:203`
- `src/bundle/bundleExport.ts:135`
</details>

### canonicalizeJson core implementation module
What it is: canonicalizeJson core implementation module acts as an inferred core implementation module built around `canonicalizeJson`, `encodeCrockfordBase32_128bits` in `src/core/canonicalJson.ts`.
What it controls: Controls a visible slice of src behavior instead of acting as passive inventory.
If you change it: Changing canonicalizejson core implementation module can shift a central behavior boundary rather than only renaming an internal helper.

<details>
<summary>Supporting citations:</summary>

- `src/core/canonicalJson.ts:53`
- `src/core/canonicalJson.ts:62`
</details>

### zArtifactGetInput core implementation module
What it is: zArtifactGetInput core implementation module acts as an inferred core implementation module built around `zArtifactGetInput`, `zArtifactGetOutput` in `src/mcp/toolSchemas.ts`.
What it controls: Controls a visible slice of src behavior instead of acting as passive inventory.
If you change it: Changing zartifactgetinput core implementation module can shift a central behavior boundary rather than only renaming an internal helper.

<details>
<summary>Supporting citations:</summary>

- `src/mcp/toolSchemas.ts:3`
- `src/mcp/toolSchemas.ts:60`
</details>

<details>
<summary>Related files:</summary>

- `src/toolpacks/builtin/qcBundleFastq.ts`
- `src/mcp/gatewayServer.ts`
- `src/bundle/bundleExport.ts`
- `src/core/canonicalJson.ts`
- `src/mcp/toolSchemas.ts`
</details>

<details>
<summary>Citations:</summary>

- `src/toolpacks/builtin/qcBundleFastq.ts:255`
- `src/toolpacks/builtin/qcBundleFastq.ts:160`
- `src/mcp/gatewayServer.ts:1491`
- `src/mcp/gatewayServer.ts:85`
- `src/bundle/bundleExport.ts:203`
- `src/bundle/bundleExport.ts:135`
- `src/core/canonicalJson.ts:53`
- `src/core/canonicalJson.ts:62`
- `src/mcp/toolSchemas.ts:3`
- `src/mcp/toolSchemas.ts:60`
</details>

## State Transitions and Recovery

Insufficient evidence to explain one bounded state path and its first recovery boundary confidently.

<details>
<summary>Supporting citations:</summary>

- none
</details>


## Subsystem Narratives

Diagram link: [Subsystem Clusters](diagrams.md#subsystem-clusters).

### external
Purpose: external groups 228 components using path structure plus graph-connected merges.

Responsibilities:
- External node dependency inferred from lockfile resolution.
- External node dependency inferred from package.json.
- helixmcp-biomcp-fabric node component
- Grouped around dominant path prefix `external`.

Key dependencies:
- @hono/node-server
- ajv-formats
- ajv
- content-type
- cors

Boundary notes:
- Mostly operates within its own inferred subsystem boundary.
- No strong adjacent subsystem boundary was inferred.
- Dominant paths: `package.json`, `src/artifacts/artifactService.ts`, `src/artifacts/localObjectStore.ts`.

### src
Purpose: src groups 2 components using path structure plus graph-connected merges.

Responsibilities:
- Source module rooted at src.
- Repository tests and fixtures.
- Grouped around dominant path prefix `src`.
- Merged `tests` into `src` because they attach most strongly through inferred dependency and call edges.

Key dependencies:
- @esbuild/aix-ppc64
- @esbuild/android-arm
- @esbuild/android-arm64
- @esbuild/android-x64
- @esbuild/darwin-arm64

Boundary notes:
- Crosses 49 external dependency edges into related subsystems.
- Most connected to `external`.
- Dominant paths: `src/artifacts/artifactService.ts`, `src/artifacts/localObjectStore.ts`, `src/bundle/bundleExport.ts`, `tests/artifactService.test.ts`, `tests/bundleExport.test.ts`, `tests/contracts.test.ts`.

### docs
Purpose: docs groups 1 components under docs/ or related paths.

Responsibilities:
- Repository documentation and wiki source files.
- Grouped around dominant path prefix `docs`.

Key dependencies:
- @esbuild/aix-ppc64
- @esbuild/android-arm
- @esbuild/android-arm64
- @esbuild/android-x64
- @esbuild/darwin-arm64

Boundary notes:
- Mostly operates within its own inferred subsystem boundary.
- No strong adjacent subsystem boundary was inferred.
- Dominant paths: `docs/architecture.md`, `docs/bundle_export.md`, `docs/slurm_cluster_smoke.md`.

<details>
<summary>Related files:</summary>

- `package.json`
- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`
- `src/bundle/bundleExport.ts`
- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
- `tests/contracts.test.ts`
- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
</details>

<details>
<summary>Citations:</summary>

- `package.json`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `src/bundle/bundleExport.ts:203`
- `tests/contracts.test.ts:30`
- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
</details>

## Execution Narrative

Stages:
1. Entry begins with `pnpm dev` (dev) from `.`.
2. Initial control lands in helixmcp-biomcp-fabric and quickly centers on helixmcp-biomcp-fabric as the primary execution owner.
3. Triggered by inferred dependency edges leaving src.
4. src hands off to `ulid`, `pg`, `kysely` in external through 49 inferred dependency edges.

Owning components:
- helixmcp-biomcp-fabric (application)
- Tests (tests)
- @modelcontextprotocol/sdk (package)

Handoffs:
- src -> external: src groups 2 components using path structure plus graph-connected merges. hands off into external groups 228 components using path structure plus graph-connected merges.
- src hands off to `ulid`, `pg`, `kysely` in external through 49 inferred dependency edges.

<details>
<summary>Supporting citations:</summary>

- `package.json`
- `pnpm dev`
- `src/artifacts/artifactService.ts`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

<details>
<summary>Related files:</summary>

- `package.json`
- `pnpm dev`
- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`
- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
- `src/bundle/bundleExport.ts`
- `tests/contracts.test.ts`
</details>

<details>
<summary>Citations:</summary>

- `package.json`
- `pnpm dev`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

## Code Path Slice

Starts at: `stableJsonStringify` (function) in `src/core/canonicalJson.ts`. Chosen because `stableJsonStringify` is an exported trigger inside hotspot component `src`.

Calls into: `canonicalizeJson` (function) in `src/core/canonicalJson.ts`.

Stops at: `isPlainObject` (function) in `src/core/canonicalJson.ts`, where no deeper unambiguous call step was inferred within the bounded slice.

<details>
<summary>Supporting citations:</summary>

- `src/core/canonicalJson.ts:78`
- `src/core/canonicalJson.ts:35`
- `src/core/canonicalJson.ts:27`
</details>

<details>
<summary>Related files:</summary>

- `src/core/canonicalJson.ts`
</details>

<details>
<summary>Citations:</summary>

- `src/core/canonicalJson.ts:78`
- `src/core/canonicalJson.ts:35`
- `src/core/canonicalJson.ts:27`
</details>

## State and Data Flow

Inputs:
- Input enters from workflow `pnpm dev` (dev) via `pnpm dev`.

Transformations:
- src acts as the primary transformation owner. It hands off into `Tests`, `ulid`, `pg`, `kysely` for the next transformation steps.
- Downstream transformation continues through `Tests`, `@esbuild/aix-ppc64`, `@esbuild/android-arm`, `@esbuild/android-arm64`.

Storage or sinks:
- `Tests`, `@esbuild/aix-ppc64`, `@esbuild/android-arm`, `@esbuild/android-arm64` behave as the final inferred sinks or terminal state holders in this path.

<details>
<summary>Supporting citations:</summary>

- `package.json`
- `pnpm dev`
- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`
- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

<details>
<summary>Related files:</summary>

- `package.json`
- `pnpm dev`
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
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

## Subsystem Interactions

Diagram links: [Subsystem Clusters](diagrams.md#subsystem-clusters) and [Dependency Graph](diagrams.md#dependency-graph).

### src -> external
Purpose: src groups 2 components using path structure plus graph-connected merges. hands off into external groups 228 components using path structure plus graph-connected merges.

Trigger: Triggered by inferred dependency edges leaving src.

Handoff: src hands off to `ulid`, `pg`, `kysely` in external through 49 inferred dependency edges.

<details>
<summary>Supporting citations:</summary>

- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `package.json`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

<details>
<summary>Related files:</summary>

- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`
- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
- `package.json`
- `src/bundle/bundleExport.ts`
- `tests/contracts.test.ts`
</details>

<details>
<summary>Citations:</summary>

- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `package.json`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

## Invariants and Risks

### src is an implicit compatibility boundary
Assumption: Assumption: src can change without breaking `external` is risky, because the component bridges those subsystems and carries hotspot score 2619.

<details>
<summary>Supporting citations:</summary>

- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `src/bundle/bundleExport.ts:203`
</details>

### src -> external is a cross-subsystem coupling hazard
Coupling hazard: Coupling hazard: src hands off to `ulid`, `pg`, `kysely` in external through 49 inferred dependency edges. Changes on either side are likely to ripple because src groups 2 components using path structure plus graph-connected merges. hands off into external groups 228 components using path structure plus graph-connected merges..

<details>
<summary>Supporting citations:</summary>

- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `package.json`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

### Operational changes tend to fail first at validation boundaries
Failure boundary: Failure boundary: verify high-coordination component `src` against `pnpm build` and `pnpm test` before and after changes, because those surfaces are the first deterministic checks tied to likely breakage.

<details>
<summary>Supporting citations:</summary>

- `package.json`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
</details>

<details>
<summary>Related files:</summary>

- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`
- `src/bundle/bundleExport.ts`
- `src/bundle/bundleTar.ts`
- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
- `package.json`
- `tests/contracts.test.ts`
</details>

<details>
<summary>Citations:</summary>

- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `src/bundle/bundleExport.ts:203`
- `package.json`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

## External Systems

### pg
Role: Data store or messaging integration

Interaction mode: Used by `Documentation`, `helixmcp-biomcp-fabric`, `Tests`, `src` via standard dependency declarations in runtime scope and pinned in `package-lock.json`.

<details>
<summary>Supporting citations:</summary>

- `package.json`
- `package-lock.json`
- `docs/architecture.md`
- `docs/bundle_export.md`
- `src/artifacts/artifactService.ts:28`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `src/artifacts/localObjectStore.ts:80`
</details>

### pg-mem
Role: Data store or messaging integration

Interaction mode: Used by `Documentation`, `helixmcp-biomcp-fabric`, `Tests`, `src` via standard dependency declarations in development scope and pinned in `package-lock.json`.

<details>
<summary>Supporting citations:</summary>

- `package.json`
- `package-lock.json`
- `docs/architecture.md`
- `docs/bundle_export.md`
- `src/artifacts/artifactService.ts:28`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `src/artifacts/localObjectStore.ts:80`
</details>

### @types/pg
Role: Data store or messaging integration

Interaction mode: Used by `Documentation`, `helixmcp-biomcp-fabric`, `Tests` via standard dependency declarations in development scope and pinned in `package-lock.json`.

<details>
<summary>Supporting citations:</summary>

- `package.json`
- `package-lock.json`
- `docs/architecture.md`
- `docs/bundle_export.md`
- `src/artifacts/artifactService.ts:28`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

### http-errors
Role: Protocol or client integration

Interaction mode: Used by `Documentation`, `Tests` via manifest dependency declarations and pinned in `package-lock.json`.

<details>
<summary>Supporting citations:</summary>

- `package-lock.json`
- `docs/architecture.md`
- `docs/bundle_export.md`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
</details>

<details>
<summary>Related files:</summary>

- `package.json`
- `package-lock.json`
- `docs/architecture.md`
- `docs/bundle_export.md`
- `src/artifacts/artifactService.ts`
- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
- `src/artifacts/localObjectStore.ts`
</details>

<details>
<summary>Citations:</summary>

- `package.json`
- `package-lock.json`
- `docs/architecture.md`
- `docs/bundle_export.md`
- `src/artifacts/artifactService.ts:28`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `src/artifacts/localObjectStore.ts:80`
</details>

## Subsystems and Components

- helixmcp-biomcp-fabric (application) depends on 13 known edges.
- src (module) depends on 23 known edges.
- Tests (tests) depends on 255 known edges.
- Documentation (docs) depends on 230 known edges.
- @modelcontextprotocol/sdk (package) depends on 16 known edges.
- @types/node (package) depends on 1 known edges.
- @types/pg (package) depends on 3 known edges.
- ajv (package) depends on 4 known edges.
- kysely (package) depends on 0 known edges.
- pg (package) depends on 5 known edges.
- pg-mem (package) depends on 7 known edges.
- tsx (package) depends on 2 known edges.

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
- `src/bundle/bundleExport.ts:203`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `tests/contracts.test.ts:30`
- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
</details>

## Subsystem Clusters

Diagram link: [Subsystem Clusters](diagrams.md#subsystem-clusters).

- external: external groups 228 components using path structure plus graph-connected merges. Strategy: hybrid. Internal edges: 273. External edges: 0. Dominant paths: `package.json`, `src/artifacts/artifactService.ts`, `src/artifacts/localObjectStore.ts`. Rationale: Grouped around dominant path prefix `external`. Merged `root` into `external` because they attach most strongly through inferred dependency and call edges.
- src: src groups 2 components using path structure plus graph-connected merges. Strategy: hybrid. Internal edges: 467. External edges: 49. Dominant paths: `src/artifacts/artifactService.ts`, `src/artifacts/localObjectStore.ts`, `src/bundle/bundleExport.ts`, `tests/artifactService.test.ts`, `tests/bundleExport.test.ts`, `tests/contracts.test.ts`. Rationale: Grouped around dominant path prefix `src`. Merged `tests` into `src` because they attach most strongly through inferred dependency and call edges. Most connected to `external` through inferred graph edges.
- docs: docs groups 1 components under docs/ or related paths. Strategy: path. Internal edges: 0. External edges: 0. Dominant paths: `docs/architecture.md`, `docs/bundle_export.md`, `docs/slurm_cluster_smoke.md`. Rationale: Grouped around dominant path prefix `docs`.

<details>
<summary>Related files:</summary>

- `package.json`
- `src/artifacts/artifactService.ts`
- `src/artifacts/localObjectStore.ts`
- `src/bundle/bundleExport.ts`
- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
- `tests/contracts.test.ts`
- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
</details>

<details>
<summary>Citations:</summary>

- `package.json`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `src/bundle/bundleExport.ts:203`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `tests/contracts.test.ts:30`
- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
</details>

## Graph Hotspots

Diagram links: [Dependency Graph](diagrams.md#dependency-graph) and [Component Overview](diagrams.md#component-overview).

- src (module) at `src`: score 2619. Inbound 446, outbound 426, calls 849, dependencies 23. Inbound edges: 446. Outbound edges: 426. Call edges: 849; dependency edges: 23. Bridges 1 subsystem boundaries: `external`.
- Tests (tests) at `tests`: score 336. Inbound 21, outbound 90, calls 85, dependencies 26. Inbound edges: 21. Outbound edges: 90. Call edges: 85; dependency edges: 26. Bridges 1 subsystem boundaries: `external`.
- @modelcontextprotocol/sdk (package) at `external/node/@modelcontextprotocol/sdk`: score 102. Inbound 17, outbound 16, calls 0, dependencies 33. Inbound edges: 17. Outbound edges: 16. Call edges: 0; dependency edges: 33. Bridges 1 subsystem boundaries: `src`.
- vitest (package) at `external/node/vitest`: score 90. Inbound 9, outbound 20, calls 0, dependencies 29. Inbound edges: 9. Outbound edges: 20. Call edges: 0; dependency edges: 29. Bridges 1 subsystem boundaries: `src`.
- express (package) at `external/node/express`: score 87. Inbound 1, outbound 28, calls 0, dependencies 29. Inbound edges: 1. Outbound edges: 28. Call edges: 0; dependency edges: 29. Mostly operates within a single subsystem boundary.
- pg (package) at `external/node/pg`: score 48. Inbound 10, outbound 5, calls 0, dependencies 15. Inbound edges: 10. Outbound edges: 5. Call edges: 0; dependency edges: 15. Bridges 1 subsystem boundaries: `src`.
- pg-mem (package) at `external/node/pg-mem`: score 48. Inbound 8, outbound 7, calls 0, dependencies 15. Inbound edges: 8. Outbound edges: 7. Call edges: 0; dependency edges: 15. Bridges 1 subsystem boundaries: `src`.
- get-intrinsic (package) at `external/node/get-intrinsic`: score 45. Inbound 5, outbound 10, calls 0, dependencies 15. Inbound edges: 5. Outbound edges: 10. Call edges: 0; dependency edges: 15. Mostly operates within a single subsystem boundary.

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

## Where to Read Next

- Start with `components.md` for ownership boundaries.
- Use `workflows.md` for build and test commands.
- Use `dependencies.md` to inspect inferred relationships.

## Citations

<details>
<summary>Citations:</summary>

- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
- `README.md`
- `src/toolpacks/builtin/qcBundleFastq.ts:255`
- `src/toolpacks/builtin/qcBundleFastq.ts:160`
- `src/mcp/gatewayServer.ts:1491`
- `src/mcp/gatewayServer.ts:85`
- `src/bundle/bundleExport.ts:203`
- `src/bundle/bundleExport.ts:135`
- `src/core/canonicalJson.ts:53`
- `src/core/canonicalJson.ts:62`
- `src/mcp/toolSchemas.ts:3`
- `src/mcp/toolSchemas.ts:60`
- `package.json`
- `src/artifacts/artifactService.ts:28`
- `src/artifacts/localObjectStore.ts:80`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `tests/contracts.test.ts:30`
- `pnpm dev`
- `src/core/canonicalJson.ts:78`
- `src/core/canonicalJson.ts:35`
- `src/core/canonicalJson.ts:27`
- `package-lock.json`
</details>
