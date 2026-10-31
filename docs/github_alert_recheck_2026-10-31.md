# GitHub alert API recheck — 2026-10-31

## Outcome

Checked at `2026-10-31T05:18:29.939Z` against default-branch commit `7f7648e88c289e7f34e8198c0d4d43797412b820`.
All **102 original alerts remain open**. None has been marked `fixed`, dismissed,
or auto-dismissed. There are **zero new open alerts**. The explicit REST fixed-state
query returned zero records, and a separate GraphQL query confirmed 102 open alerts.

GitHub’s current SPDX SBOM still matches all **233 npm name/version pairs** from
236 lockfile installation entries. None of the 102 recorded vulnerable versions
is present in that graph. Each open alert’s reported vulnerable range was compared
with **every** current graph version of its package, including nested versions,
and with the exact direct requirement for package.json alerts. **Zero ranges
matched current versions.** Rollup is absent from the graph; its historical alert
is included rather than silently dropped.

Both refreshed full and production npm audits reported zero known findings.
The current evidence therefore supports stale GitHub alert records after graph
recovery, not an additional affected version in these 102 reported ranges.
GitHub’s internal reason for the delayed alert reevaluation remains unconfirmed.
This does not establish complete application or operational security.

## Investigation method and boundaries

- Compared original alert IDs with a newly paginated authenticated REST inventory.
- Verified the remote default branch and both manifest blobs against the local commit.
- Ran `npm run security:graph` against a newly exported GitHub SBOM: exit 0,
  no missing or unexpected npm pairs.
- Used npm’s installed semver 7.7.4 implementation, normalizing GitHub comma-separated
  bounds as conjunctions and including prerelease versions conservatively.
- Validated that all package.json alert packages still have exact direct requirements.
- Rechecked alert 117 with `Cache-Control: no-cache`: HTTP 200, server timestamp
  `2026-10-31T05:17:49Z`, still open at old body-parser 2.2.2.
- No browser actions, dependency submission, security-setting changes, new package
  upgrades, alert dismissal or outgoing support message occurred in this recheck.

An initial combined GraphQL query failed server-side with request ID
`DB04:1F41:357498F:B0748BD:6AE579FD`. One narrower read-only retry succeeded.
That failure is retained as diagnostic evidence; it does not establish the cause
of the stale alerts.

## Package reconciliation

Every row below has zero matching vulnerable ranges in the current graph.

| Package | Open alerts | Version recorded in alerts | Current GitHub graph version(s) |
| --- | ---: | --- | --- |
| @hono/node-server | 3 | 1.19.9 | 2.1.5 |
| @modelcontextprotocol/sdk | 12 | 1.25.3 | 1.32.2 |
| @vitest/mocker | 2 | 4.0.18 | 4.1.12 |
| ajv | 2 | 8.17.1 | 8.20.0 |
| body-parser | 2 | 2.2.2 | 2.4.0 |
| fast-uri | 8 | 3.1.0 | 3.1.9 |
| hono | 34 | 4.11.7 | 4.13.14 |
| immutable | 3 | 4.3.7 | 4.3.9 |
| kysely | 6 | 0.28.10 | 0.28.18 |
| moment | 1 | 2.30.1 | 2.31.0 |
| nanoid | 1 | 3.3.11 | 3.3.20 |
| path-to-regexp | 2 | 8.3.0 | 8.4.2 |
| picomatch | 1 | 4.0.3 | 4.0.7 |
| postcss | 3 | 8.5.6 | 8.5.32 |
| proxy-addr | 1 | 2.0.7 | 2.0.8 |
| qs | 3 | 6.14.1 | 6.17.1 |
| rollup | 1 | 4.57.0 | Absent |
| source-map-js | 1 | 1.2.1 | 1.2.2 |
| vite | 10 | 7.3.1 | 8.3.5 |
| vitest | 4 | 4.0.18 | 4.1.12 |
| yaml | 2 | 2.8.2 | 2.9.1 |

## Evidence identities

- Manifest blob: `8cffdb106aa5eadee1428ff2958eb4c6760b19fe`.
- Lockfile blob: `7d31c75cc5959f94b1272806611fc8e0e6ae823e`.
- SBOM exported: `2026-10-31T05:16:57Z`; SHA-256 `f7e98583ba91a56d4bc86a4a478056cdc8e62fc0469287c407a3020e3f58090f`.
- Full REST inventory SHA-256: `fa98afdac8a76fa3f3a5485df5c7768c69a364e375316eebc39637067e4dd33a`.
- Evidence directory: `D:/Helix/artifacts/mcp-alert-recheck-20261031T051654Z/`.
- `range-reconciliation.json` retains all 102 IDs, current versions, advisory
  ranges, matching results and GitHub state/timestamps.
- Other files retain the raw SBOM, REST pages, direct manifest, remote identities,
  HTTP headers, successful graph check, audits and GraphQL failure/retry.

## Remaining action

Alert clearance is **not verified**. Preserve the graph and alerts as observed.
If the discrepancy persists, the supported next escalation remains the browser’s
Refresh Dependabot alerts control or GitHub Support using this evidence.
Those actions were not performed under the requested API-only scope. This report
and the ledger below are ready for operator review; nothing was sent.

## Per-alert ledger

Current graph versions for each package are listed above. All rows remain open;
all have zero affected current versions and null fixed/dismissed timestamps.

| Alert | Package | Recorded version | Advisory | Reported vulnerable range |
| --- | --- | --- | --- | --- |
| [#1](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/1) | @modelcontextprotocol/sdk | 1.25.3 | GHSA-345p-7cg4-v4c7 | >= 1.10.0, <= 1.25.3 |
| [#2](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/2) | @modelcontextprotocol/sdk | 1.25.3 | GHSA-345p-7cg4-v4c7 | >= 1.10.0, <= 1.25.3 |
| [#3](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/3) | qs | 6.14.1 | GHSA-w7fw-mjwx-w883 | >= 6.7.0, <= 6.14.1 |
| [#4](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/4) | hono | 4.11.7 | GHSA-gq3j-xvxp-8hrf | < 4.11.10 |
| [#5](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/5) | ajv | 8.17.1 | GHSA-2g4f-4pwh-qvx6 | >= 7.0.0-alpha.0, < 8.18.0 |
| [#6](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/6) | ajv | 8.17.1 | GHSA-2g4f-4pwh-qvx6 | >= 7.0.0-alpha.0, < 8.18.0 |
| [#7](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/7) | rollup | 4.57.0 | GHSA-mw96-cpmx-2vgc | >= 4.0.0, < 4.59.0 |
| [#8](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/8) | hono | 4.11.7 | GHSA-p6xx-57qc-3wxr | < 4.12.4 |
| [#9](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/9) | hono | 4.11.7 | GHSA-q5qw-h33p-qvwr | < 4.12.4 |
| [#10](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/10) | hono | 4.11.7 | GHSA-5pq2-9x2x-5p6w | < 4.12.4 |
| [#11](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/11) | @hono/node-server | 1.19.9 | GHSA-wc8c-qw6v-h7f6 | < 1.19.10 |
| [#13](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/13) | immutable | 4.3.7 | GHSA-wf6x-7x77-mvgw | >= 4.0.0-rc.1, < 4.3.8 |
| [#14](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/14) | hono | 4.11.7 | GHSA-v8w9-8mx6-g223 | < 4.12.7 |
| [#17](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/17) | kysely | 0.28.10 | GHSA-wmrf-hv6w-mr66 | >= 0.26.0, <= 0.28.11 |
| [#18](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/18) | kysely | 0.28.10 | GHSA-wmrf-hv6w-mr66 | >= 0.26.0, <= 0.28.11 |
| [#19](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/19) | kysely | 0.28.10 | GHSA-8cpq-38p9-67gx | <= 0.28.13 |
| [#20](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/20) | kysely | 0.28.10 | GHSA-8cpq-38p9-67gx | <= 0.28.13 |
| [#22](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/22) | picomatch | 4.0.3 | GHSA-3v7f-55p6-f55p | >= 4.0.0, < 4.0.4 |
| [#23](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/23) | yaml | 2.8.2 | GHSA-48c2-rrv3-qjmp | >= 2.0.0, < 2.8.3 |
| [#24](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/24) | yaml | 2.8.2 | GHSA-48c2-rrv3-qjmp | >= 2.0.0, < 2.8.3 |
| [#25](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/25) | path-to-regexp | 8.3.0 | GHSA-j3q9-mxjg-w52f | >= 8.0.0, < 8.4.0 |
| [#26](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/26) | path-to-regexp | 8.3.0 | GHSA-27v5-c462-wpq7 | >= 8.0.0, < 8.4.0 |
| [#27](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/27) | vite | 7.3.1 | GHSA-v2wj-q39q-566r | >= 7.1.0, <= 7.3.1 |
| [#28](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/28) | vite | 7.3.1 | GHSA-4w7w-66w2-5vf9 | >= 7.0.0, <= 7.3.1 |
| [#29](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/29) | vite | 7.3.1 | GHSA-p9ff-h696-f583 | >= 7.0.0, <= 7.3.1 |
| [#30](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/30) | @hono/node-server | 1.19.9 | GHSA-92pp-h63x-v22m | < 1.19.13 |
| [#31](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/31) | hono | 4.11.7 | GHSA-xf4j-xp2r-rqqx | >= 4.0.0, <= 4.12.11 |
| [#32](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/32) | hono | 4.11.7 | GHSA-wmmm-f939-6g9c | < 4.12.12 |
| [#33](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/33) | hono | 4.11.7 | GHSA-xpcf-pg52-r92g | < 4.12.12 |
| [#34](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/34) | hono | 4.11.7 | GHSA-26pp-8wgv-hjvm | < 4.12.12 |
| [#35](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/35) | hono | 4.11.7 | GHSA-r5rp-j6wh-rvv4 | < 4.12.12 |
| [#36](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/36) | hono | 4.11.7 | GHSA-458j-xx4x-4375 | < 4.12.14 |
| [#37](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/37) | hono | 4.11.7 | GHSA-9vqf-7f2p-gf9v | < 4.12.16 |
| [#38](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/38) | hono | 4.11.7 | GHSA-69xw-7hcm-h432 | < 4.12.16 |
| [#40](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/40) | hono | 4.11.7 | GHSA-p77w-8qqv-26rm | < 4.12.18 |
| [#41](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/41) | hono | 4.11.7 | GHSA-hm8q-7f3q-5f36 | < 4.12.18 |
| [#42](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/42) | hono | 4.11.7 | GHSA-qp7p-654g-cw7p | < 4.12.18 |
| [#44](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/44) | kysely | 0.28.10 | GHSA-pv5w-4p9q-p3v2 | >= 0.26.0, < 0.28.17 |
| [#45](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/45) | kysely | 0.28.10 | GHSA-pv5w-4p9q-p3v2 | >= 0.26.0, < 0.28.17 |
| [#46](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/46) | qs | 6.14.1 | GHSA-q8mj-m7cp-5q26 | >= 6.11.1, <= 6.15.1 |
| [#49](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/49) | hono | 4.11.7 | GHSA-f577-qrjj-4474 | < 4.12.21 |
| [#50](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/50) | hono | 4.11.7 | GHSA-3hrh-pfw6-9m5x | < 4.12.21 |
| [#51](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/51) | hono | 4.11.7 | GHSA-xrhx-7g5j-rcj5 | < 4.12.21 |
| [#52](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/52) | hono | 4.11.7 | GHSA-2gcr-mfcq-wcc3 | < 4.12.21 |
| [#53](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/53) | vitest | 4.0.18 | GHSA-5xrq-8626-4rwp | >= 4.0.0, < 4.1.0 |
| [#54](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/54) | vitest | 4.0.18 | GHSA-5xrq-8626-4rwp | >= 4.0.0, < 4.1.0 |
| [#56](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/56) | vite | 7.3.1 | GHSA-v6wh-96g9-6wx3 | >= 7.0.0, <= 7.3.4 |
| [#57](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/57) | vite | 7.3.1 | GHSA-fx2h-pf6j-xcff | >= 7.0.0, <= 7.3.4 |
| [#58](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/58) | hono | 4.11.7 | GHSA-j6c9-x7qj-28xf | < 4.12.25 |
| [#59](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/59) | hono | 4.11.7 | GHSA-wwfh-h76j-fc44 | < 4.12.25 |
| [#60](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/60) | hono | 4.11.7 | GHSA-88fw-hqm2-52qc | < 4.12.25 |
| [#61](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/61) | hono | 4.11.7 | GHSA-rv63-4mwf-qqc2 | < 4.12.25 |
| [#62](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/62) | hono | 4.11.7 | GHSA-wgpf-jwqj-8h8p | < 4.12.25 |
| [#63](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/63) | body-parser | 2.2.2 | GHSA-v422-hmwv-36x6 | >= 2.0.0, < 2.3.0 |
| [#65](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/65) | hono | 4.11.7 | GHSA-xgm2-5f3f-mvvc | >= 4.3.3, < 4.12.27 |
| [#66](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/66) | hono | 4.11.7 | GHSA-w62v-xxxg-mg59 | >= 4.0.0, < 4.12.27 |
| [#67](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/67) | fast-uri | 3.1.0 | GHSA-4c8g-83qw-93j6 | >= 3.0.0, < 3.1.3 |
| [#68](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/68) | fast-uri | 3.1.0 | GHSA-v2hh-gcrm-f6hx | >= 3.0.0, <= 3.1.3 |
| [#71](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/71) | postcss | 8.5.6 | GHSA-6g55-p6wh-862q | <= 8.5.11 |
| [#72](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/72) | postcss | 8.5.6 | GHSA-r28c-9q8g-f849 | <= 8.5.17 |
| [#73](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/73) | hono | 4.11.7 | GHSA-8j4g-w8fx-2239 | < 4.12.34 |
| [#74](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/74) | fast-uri | 3.1.0 | GHSA-7p8r-x3mc-p8w7 | >= 3.0.0, < 3.1.5 |
| [#75](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/75) | hono | 4.11.7 | GHSA-79qm-7rj5-m7r9 | >= 4.7.0, < 4.12.34 |
| [#76](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/76) | hono | 4.11.7 | GHSA-f23p-vx2j-j53r | >= 3.8.0, < 4.12.34 |
| [#79](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/79) | postcss | 8.5.6 | GHSA-fxqj-rqcc-2cmp | <= 8.5.22 |
| [#80](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/80) | fast-uri | 3.1.0 | GHSA-q3j6-qgpj-74h6 | >= 3.0.0, <= 3.1.0 |
| [#81](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/81) | fast-uri | 3.1.0 | GHSA-v39h-62p7-jpjc | >= 3.0.0, <= 3.1.1 |
| [#82](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/82) | @hono/node-server | 1.19.9 | GHSA-frvp-7c67-39w9 | < 1.19.15 |
| [#84](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/84) | fast-uri | 3.1.0 | GHSA-f65p-4m7j-42xc | >= 3.0.0, < 3.1.6 |
| [#85](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/85) | fast-uri | 3.1.0 | GHSA-jqff-g426-hqxp | >= 3.0.0, < 3.1.6 |
| [#86](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/86) | immutable | 4.3.7 | GHSA-xvcm-6775-5m9r | >= 4.0.0-beta.1, < 4.3.9 |
| [#87](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/87) | nanoid | 3.3.11 | GHSA-xwg4-73v4-xw9w | < 3.3.12 |
| [#88](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/88) | qs | 6.14.1 | GHSA-4mjr-xmp4-gh2g | >= 2.2.5, < 6.16.0 |
| [#89](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/89) | hono | 4.11.7 | GHSA-g6gw-c38x-mqfc | < 4.13.5 |
| [#90](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/90) | hono | 4.11.7 | GHSA-crvj-82cr-hjcx | < 4.13.5 |
| [#91](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/91) | hono | 4.11.7 | GHSA-gqvv-2mrq-wpjv | < 4.13.5 |
| [#92](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/92) | @vitest/mocker | 4.0.18 | GHSA-82fw-gwwq-j7x9 | >= 2.1.0, < 4.1.11 |
| [#93](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/93) | vitest | 4.0.18 | GHSA-82fw-gwwq-j7x9 | >= 2.1.0, < 4.1.11 |
| [#94](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/94) | vitest | 4.0.18 | GHSA-82fw-gwwq-j7x9 | >= 2.1.0, < 4.1.11 |
| [#95](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/95) | immutable | 4.3.7 | GHSA-v56q-mh7h-f735 | >= 4.0.0-rc.1, < 4.3.9 |
| [#96](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/96) | fast-uri | 3.1.0 | GHSA-qw65-cvwx-89v3 | >= 3.0.0, < 3.1.7 |
| [#97](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/97) | moment | 2.30.1 | GHSA-4p3w-j4w9-5jqw | >= 2.29.2, < 2.31.0 |
| [#98](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/98) | hono | 4.11.7 | GHSA-hxh3-vqpv-xpqv | < 4.13.7 |
| [#99](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/99) | @modelcontextprotocol/sdk | 1.25.3 | GHSA-6qxp-vccf-f47h | >= 1.12.0, < 1.31.0 |
| [#100](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/100) | @modelcontextprotocol/sdk | 1.25.3 | GHSA-6qxp-vccf-f47h | >= 1.12.0, < 1.31.0 |
| [#101](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/101) | proxy-addr | 2.0.7 | GHSA-jqcg-44mw-7w3h | >= 1.1.0, < 2.0.8 |
| [#102](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/102) | source-map-js | 1.2.1 | GHSA-68fv-2mgg-jv7q | >= 1.0.0, < 1.2.2 |
| [#103](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/103) | @modelcontextprotocol/sdk | 1.25.3 | GHSA-22jm-h49p-29qw | >= 1.24.0, < 1.32.0 |
| [#104](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/104) | @modelcontextprotocol/sdk | 1.25.3 | GHSA-22jm-h49p-29qw | >= 1.24.0, < 1.32.0 |
| [#105](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/105) | @modelcontextprotocol/sdk | 1.25.3 | GHSA-6prh-2h8m-c8cw | < 1.32.0 |
| [#106](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/106) | @modelcontextprotocol/sdk | 1.25.3 | GHSA-6prh-2h8m-c8cw | < 1.32.0 |
| [#107](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/107) | @modelcontextprotocol/sdk | 1.25.3 | GHSA-rvq5-wwqv-78pq | >= 1.6.0, < 1.32.0 |
| [#108](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/108) | @modelcontextprotocol/sdk | 1.25.3 | GHSA-rvq5-wwqv-78pq | >= 1.6.0, < 1.32.0 |
| [#109](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/109) | @modelcontextprotocol/sdk | 1.25.3 | GHSA-46jr-4wx8-v453 | >= 1.3.0, < 1.32.0 |
| [#110](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/110) | @modelcontextprotocol/sdk | 1.25.3 | GHSA-46jr-4wx8-v453 | >= 1.3.0, < 1.32.0 |
| [#111](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/111) | vite | 7.3.1 | GHSA-698v-4r3x-3vcv | >= 7.0.0, <= 7.3.6 |
| [#112](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/112) | vite | 7.3.1 | GHSA-587h-7wmj-h5pm | >= 7.0.0, <= 7.3.6 |
| [#113](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/113) | vite | 7.3.1 | GHSA-r825-w8mm-5pxw | >= 7.0.0, <= 7.3.6 |
| [#114](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/114) | vite | 7.3.1 | GHSA-rq7h-c2jc-7f22 | >= 7.0.0, <= 7.3.6 |
| [#115](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/115) | vite | 7.3.1 | GHSA-9jrq-w75r-8gcw | >= 7.0.0, <= 7.3.6 |
| [#116](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/116) | @vitest/mocker | 4.0.18 | GHSA-qxg6-89mg-rqv8 | >= 2.1.0-beta.7, < 4.1.12 |
| [#117](https://github.com/OmnisGenomics/HelixMCP/security/dependabot/117) | body-parser | 2.2.2 | GHSA-27r3-jxmh-p828 | >= 2.0.0, < 2.4.0 |
