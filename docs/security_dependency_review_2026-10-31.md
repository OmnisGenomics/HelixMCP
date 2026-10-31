# MCP dependency remediation — 2026-10-31

Scope: the locked HelixMCP JavaScript dependency tree, including development
tools. Before remediation, npm audit reported 22 dependency findings (2 critical,
15 high, 5 moderate); production-only audit reported 12 (1 critical, 8 high,
3 moderate). These are dependency findings, not a count of independently
demonstrated exploitable application paths.

| Direct package | Before | After |
| --- | --- | --- |
| @modelcontextprotocol/sdk | 1.25.3 | 1.32.2 |
| ajv | 8.17.1 | 8.20.0 |
| kysely | 0.28.10 | 0.28.18 |
| yaml | 2.8.2 | 2.9.1 |
| zod | 4.3.6 | 4.6.7 |
| vitest (development) | 4.0.18 | 4.1.12 |
| pg-mem (development) | 3.0.5 | 3.0.14 |
| tsx (development) | 4.21.0 | 4.23.15 |

Compatible transitive fixes are captured in package-lock.json, including the
Express/Hono request handling and address/path parsing dependency chains.
No force upgrade, suppression or audit exception was used. Exact direct pins
remain explicit. Node 24.15.0 and npm 11.6.2 were used for locked installation;
the minimum Node line now reflects the test toolchain's 20.19 requirement.
The host's npm 9 incremental installer failed with an Arborist edgesOut error;
the preserved failed attempt did not qualify the final lock. npm 11 completed
installation and a fresh npm ci validated it.

## Applicability and boundaries

- SDK advisories include shared-server/transport response leakage and HTTP/OAuth
  input, authentication and redirect boundaries. The gateway's exercised path is
  one local stdio server per process, with separate server/transport instances in
  tests. HTTP/OAuth or multi-tenant deployment is not qualified by this update.
  [Upstream SDK advisory](https://github.com/modelcontextprotocol/typescript-sdk/security/advisories/GHSA-345p-7cg4-v4c7).
- AJV, YAML and Zod findings concern parser/validation resource or input handling;
  all are updated, including dependencies used for actual tool and policy inputs.
- Kysely JSON-path/literal advisories are addressed by the compatible patched
  dependency. Existing parameterized store/database tests passed; no additional
  database deployment or arbitrary SQL execution is authorized by this review.
- Vitest's affected UI/browser server is a development surface. The test command
  uses run mode; UI/browser serving is not enabled. The package and its transitive
  dependencies were patched regardless.
  [Upstream Vitest advisory](https://github.com/vitest-dev/vitest/security/advisories/GHSA-5xrq-8626-4rwp).

## Verification

Fresh locked install, TypeScript typecheck, build and all 47 enabled tests passed;
4 opt-in integration tests remained skipped. Production and complete npm audits
both reported **zero known findings**. Run `npm run security:audit` to repeat the
complete audit. A zero registry audit is time-scoped dependency evidence, not a
claim of complete source, service or operational security.

The native Windows Prime assistant proof also passed with the patched SDK:
166 stdio MCP calls, four generated edit types, candidate revision/comparison,
manual override and the 236-case validation benchmark. The standalone comparison
verified offline and prior generated evidence remained unchanged. Receipt:
`D:/Helix/artifacts/mcp-security-windows-proof-20261031/receipt.json`.

Raw before/after reports and test logs are preserved under
`D:/Helix/artifacts/mcp-security-*`. This review does not assert that unrelated
GitHub alerts, Python/native dependencies or optional service deployments are
cleared; those require their own exact inventories.

The final 2026-10-31 recheck again found zero known findings in both audits.
The patched gateway also passed against the frozen Windows candidate; see
[frozen Prime qualification](windows_frozen_prime_qualification_2026-10-31.md)
for the 163-call result and its distinct runtime and scientific boundaries.
