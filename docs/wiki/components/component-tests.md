---
page_id: component-component:tests
page_type: component
generation_mode: inferred
freshness_status: new
updated_at: 2026-04-18T05:41:58.097Z
---

<details>
<summary>Build metadata</summary>

```json
{
  "freshnessKey": "4b97f78d05914258446769e0c984f59a9f1066a7",
  "plannerReason": "Generated because the component was ranked as significant for repo navigation.",
  "changedPaths": [
    "tests/gateway.integration.test.ts",
    "tests/runLifecycle.test.ts",
    "tests/contracts.test.ts",
    "tests/bundleExport.test.ts",
    "tests/artifactService.test.ts",
    "tests/policy.test.ts",
    "tests/slurm.integration.test.ts",
    "tests/toolpacks.test.ts",
    "docs/architecture.md",
    "docs/bundle_export.md",
    "docs/slurm_cluster_smoke.md",
    "README.md",
    "package.json"
  ],
  "dependencyPaths": [
    "tests/gateway.integration.test.ts",
    "tests/runLifecycle.test.ts",
    "tests/contracts.test.ts",
    "tests/bundleExport.test.ts",
    "tests/artifactService.test.ts",
    "tests/policy.test.ts",
    "tests/slurm.integration.test.ts",
    "tests/toolpacks.test.ts",
    "docs/architecture.md",
    "docs/bundle_export.md",
    "docs/slurm_cluster_smoke.md",
    "README.md",
    "package.json"
  ],
  "dependencyEvidenceIds": [
    "ingest:file:tests/gateway.integration.test.ts",
    "ingest:file:tests/runLifecycle.test.ts",
    "ingest:file:src/runs/toolRun.ts",
    "ingest:file:src/core/ids.ts",
    "ingest:file:src/runs/runIdentity.ts",
    "ingest:file:tests/contracts.test.ts",
    "ingest:file:tests/bundleExport.test.ts",
    "ingest:file:tests/artifactService.test.ts",
    "ingest:file:tests/policy.test.ts",
    "ingest:file:tests/slurm.integration.test.ts",
    "ingest:file:tests/toolpacks.test.ts",
    "ingest:file:docs/architecture.md",
    "ingest:file:docs/bundle_export.md",
    "ingest:file:docs/slurm_cluster_smoke.md",
    "ingest:file:README.md",
    "workflow:package.json"
  ],
  "evidenceIds": [
    "ingest:file:tests/gateway.integration.test.ts",
    "ingest:file:tests/runLifecycle.test.ts",
    "ingest:file:src/runs/toolRun.ts",
    "ingest:file:src/core/ids.ts",
    "ingest:file:src/runs/runIdentity.ts",
    "ingest:file:tests/contracts.test.ts",
    "ingest:file:tests/bundleExport.test.ts",
    "ingest:file:tests/artifactService.test.ts",
    "ingest:file:tests/policy.test.ts",
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

# Tests

Repository tests and fixtures.

## Related Pages

- [components](components.md)
- [workflows](workflows.md)
- [interfaces](interfaces.md)
- [dependencies](dependencies.md)

## Implementation Roles

Insufficient evidence to infer implementation roles confidently.

<details>
<summary>Supporting citations:</summary>

- none
</details>


## Module Responsibilities

Insufficient evidence to infer module responsibilities confidently.

<details>
<summary>Supporting citations:</summary>

- none
</details>


## Key Symbols

### `callTool` (function)
Behavior: Defines a visible implementation unit in `tests/gateway.integration.test.ts` without strong downstream call evidence.
Receives: Called by `bundle1`, `bundle2`, `fastqc1`.
Produces or triggers: No strong downstream trigger evidence was inferred.
Connected symbols:
- Callers: `bundle1`, `bundle2`, `fastqc1`.
Supporting implementation citations:
- `tests/gateway.integration.test.ts:51`
- `tests/gateway.integration.test.ts:501`

### `createRun` (function)
Behavior: Constructs or initializes an implementation boundary in `tests/runLifecycle.test.ts`. It directly calls `newProjectId`, `deriveRunId`.
Receives: Called by `ToolRun`, `runningRunId`, `succeededRunId`.
Produces or triggers: Triggers `newProjectId`, `deriveRunId`.
Connected symbols:
- Callers: `ToolRun`, `runningRunId`, `succeededRunId`.
- Callees: `newProjectId`, `deriveRunId`.
Supporting implementation citations:
- `tests/runLifecycle.test.ts:34`
- `src/runs/toolRun.ts:9`
- `src/core/ids.ts:13`

### `readJson` (function)
Behavior: Reads or loads data or dependencies for `tests/contracts.test.ts`.
Receives: Called by `schema`, `schema`, `schemas`.
Produces or triggers: No strong downstream trigger evidence was inferred.
Connected symbols:
- Callers: `schema`, `schema`, `schemas`.
Supporting implementation citations:
- `tests/contracts.test.ts:10`
- `tests/contracts.test.ts:48`

<details>
<summary>Related files:</summary>

- `tests/gateway.integration.test.ts`
- `tests/runLifecycle.test.ts`
- `tests/contracts.test.ts`
</details>

<details>
<summary>Citations:</summary>

- `tests/gateway.integration.test.ts:51`
- `tests/gateway.integration.test.ts:501`
- `tests/runLifecycle.test.ts:34`
- `src/runs/toolRun.ts:9`
- `src/core/ids.ts:13`
- `tests/contracts.test.ts:10`
- `tests/contracts.test.ts:48`
</details>

## State Boundaries

Validated at:
- none

Mutated in:
- Likely mutated in `createRun` in `tests/runLifecycle.test.ts`; this marks an inferred state-change boundary, not a formal dataflow proof.

Persisted or emitted through:
- Likely persisted or emitted through `writeUtf8` in `tests/bundleExport.test.ts`; this is inferred from persistence/emission naming and nearby implementation context. Recheck with Run `pnpm build` (build) from `.`.

<details>
<summary>Supporting citations:</summary>

- `tests/runLifecycle.test.ts:34`
- `tests/bundleExport.test.ts:177`
</details>

<details>
<summary>Related files:</summary>

- `tests/runLifecycle.test.ts`
- `tests/bundleExport.test.ts`
</details>

<details>
<summary>Citations:</summary>

- `tests/runLifecycle.test.ts:34`
- `tests/bundleExport.test.ts:177`
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

Repository tests and fixtures.

Type: tests
Root path: `tests`
Ecosystem: unknown

<details>
<summary>Related files:</summary>

- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
- `tests/contracts.test.ts`
- `tests/gateway.integration.test.ts`
- `tests/policy.test.ts`
- `tests/runLifecycle.test.ts`
- `tests/slurm.integration.test.ts`
- `tests/toolpacks.test.ts`
</details>

<details>
<summary>Citations:</summary>

- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `tests/contracts.test.ts:30`
- `tests/gateway.integration.test.ts:146`
</details>

## Entrypoints and Runtime Surface

- none

## Interfaces and Config

- none

## Dependencies and Relationships

- `component:docs` documents `component:tests` (medium)
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

<details>
<summary>Related files:</summary>

- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
- `tests/contracts.test.ts`
- `tests/gateway.integration.test.ts`
- `tests/policy.test.ts`
- `tests/runLifecycle.test.ts`
- `tests/slurm.integration.test.ts`
- `tests/toolpacks.test.ts`
</details>

<details>
<summary>Citations:</summary>

- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `tests/contracts.test.ts:30`
- `tests/gateway.integration.test.ts:146`
</details>

## Why This Hotspot Matters

Architectural role: Hotspot score 336 with 21 inbound and 90 outbound inferred edges marks `Tests` as a coordination-heavy component. It bridges `external`.

Main coupling surfaces:
- Coupled components: `Documentation`, `@esbuild/aix-ppc64`, `@esbuild/android-arm`, `@esbuild/android-arm64`.
- Call-heavy surface with 85 inferred call edges.
- Dependency-heavy surface with 26 inferred dependency edges.

Likely failure modes:
- Upstream breakage risk: 21 inbound edges suggest downstream callers depend on this boundary staying stable.
- Coordination risk: 90 outbound edges mean changes can ripple into neighboring components.
- Cross-subsystem regression risk: changes can disrupt handoffs across `external`.

<details>
<summary>Supporting citations:</summary>

- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `tests/contracts.test.ts:30`
- `tests/gateway.integration.test.ts:146`
- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
- `README.md`
</details>

<details>
<summary>Related files:</summary>

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

- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `tests/contracts.test.ts:30`
- `tests/gateway.integration.test.ts:146`
- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
- `README.md`
</details>

## Operational Risk Surface

Likely fault domains:
- External dependency boundaries: `@esbuild/aix-ppc64`, `@esbuild/android-arm`, `@esbuild/android-arm64`, `@esbuild/android-x64`.
- Cross-subsystem handoffs: `external`.

High-cost dependencies:
- `@esbuild/aix-ppc64` acts as a external dependency boundary.
- `@esbuild/android-arm` acts as a external dependency boundary.
- `@esbuild/android-arm64` acts as a external dependency boundary.
- `@esbuild/android-x64` acts as a external dependency boundary.

First validation checks:
- Run `pnpm build` (build) from `.`.
- Run `pnpm bundle:verify` (bundle:verify) from `.`.

<details>
<summary>Supporting citations:</summary>

- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `tests/contracts.test.ts:30`
- `package.json`
</details>

<details>
<summary>Related files:</summary>

- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
- `tests/contracts.test.ts`
- `package.json`
</details>

<details>
<summary>Citations:</summary>

- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `tests/contracts.test.ts:30`
- `package.json`
</details>

## Invariants and Failure Handling

Insufficient evidence to infer invariants and failure handling confidently.

<details>
<summary>Supporting citations:</summary>

- none
</details>


## Where to Edit

Likely change entry files:
- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
- `tests/contracts.test.ts`

Owned interfaces:
- none

Nearby verification surfaces:
- Validate with `pnpm test` (test) from `.`.
- Validate with `pnpm test:watch` (test:watch) from `.`.

<details>
<summary>Supporting citations:</summary>

- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `tests/contracts.test.ts:30`
- `package.json`
</details>

<details>
<summary>Related files:</summary>

- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
- `tests/contracts.test.ts`
- `package.json`
</details>

<details>
<summary>Citations:</summary>

- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `tests/contracts.test.ts:30`
- `package.json`
</details>

## Change Planning

Impacted areas:
- Downstream components likely affected: `@esbuild/aix-ppc64`, `@esbuild/android-arm`, `@esbuild/android-arm64`, `@esbuild/android-x64`.
- Cross-subsystem risk touches `external`.
- Hotspot score 336 with 21 inbound and 90 outbound edges suggests higher coordination risk.

Suggested verification steps:
- Validate with `pnpm test` (test) from `.`.
- Validate with `pnpm test:watch` (test:watch) from `.`.

<details>
<summary>Supporting citations:</summary>

- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `tests/contracts.test.ts:30`
- `package.json`
</details>

<details>
<summary>Related files:</summary>

- `tests/artifactService.test.ts`
- `tests/bundleExport.test.ts`
- `tests/contracts.test.ts`
- `tests/gateway.integration.test.ts`
- `package.json`
</details>

<details>
<summary>Citations:</summary>

- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `tests/contracts.test.ts:30`
- `package.json`
</details>

## Nearby Workflows

- none

## Citations

<details>
<summary>Citations:</summary>

- `tests/gateway.integration.test.ts:51`
- `tests/gateway.integration.test.ts:501`
- `tests/runLifecycle.test.ts:34`
- `src/runs/toolRun.ts:9`
- `src/core/ids.ts:13`
- `tests/contracts.test.ts:10`
- `tests/contracts.test.ts:48`
- `tests/bundleExport.test.ts:177`
- `tests/artifactService.test.ts:23`
- `tests/bundleExport.test.ts:29`
- `tests/contracts.test.ts:30`
- `tests/gateway.integration.test.ts:146`
- `docs/architecture.md`
- `docs/bundle_export.md`
- `docs/slurm_cluster_smoke.md`
- `README.md`
- `package.json`
</details>
