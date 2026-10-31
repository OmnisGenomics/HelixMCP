# Testing live Studio through HelixMCP

HelixMCP connects an MCP client to a running Omnis Helix Studio session through
Studio's local bridge. The gateway uses the existing MCP SDK and stdio transport;
no additional server is needed. Calls are policy-gated and logged as interactive
runs. Screenshots and exported observations enter the artifact store.

All examples use synthetic, in silico inputs. Automation proves software behavior;
it supplies no human review result, decision approval, or model accuracy claim.

## Build and connect

```bash
npm ci
npm run build
node dist/index.js
```

Run from the HelixMCP repository so the gateway can find its policy and database
schema. Without `DATABASE_URL`, the gateway uses its existing in-memory database.
Object bytes are saved under `OBJECT_STORE_DIR`; use a persistent database if
artifact and run records must survive gateway restarts.

Configure your MCP client's stdio server with `node` as the command,
`dist/index.js` as the argument, and this repository as its working directory.
If an existing HelixMCP connection is running, reconnect after rebuilding to
refresh its tool catalog. The gateway advertises version `1.4.0`.

Launch Studio from a Helix checkout with a display:

```bash
PYTHONPATH=src QT_QPA_PLATFORM=xcb \
OMNIS_HELIX_ENABLE_MCP_BRIDGE=1 OMNIS_HELIX_SKIP_WELCOME=1 \
python -m helix.studio.app --force-fullscreen
```

Pass `bridge_file` to the tools when using a custom bridge path. The gateway also
respects `HELIX_STUDIO_MCP_BRIDGE_FILE` and `OMNIS_HELIX_MCP_BRIDGE_FILE`.
The bridge must advertise a loopback host. Studio writes both its current and
legacy bridge filenames; the gateway retains legacy discovery compatibility.

## Prime wizard and simulation

`studio_prime_workflow` drives actual Qt widgets and the existing asynchronous
Run action. Its bounded `action` values are `get_state`, `wizard_open` (mode
`new` or `edit`), `wizard_target`, `wizard_fill`, `wizard_navigate` (stage 0–3),
`wizard_commit`, `run`, and `evidence`. Fill accepts only the six Prime design
fields. Target loading requires an imported small text/FASTA `target_artifact_id`
and `synthetic: true`; it materializes and checksum-checks that artifact in the
run workspace. There is no arbitrary code execution, source replacement, or
approval action. Studio still enforces evaluation access and wizard validation.

State includes wizard feedback, current run/prediction, previous-design status,
bundle location and integrity verification text. Open/commit are queued Qt
operations; poll state until the wizard opens/closes. Run returns immediately;
poll until `run_in_progress` is false and `run_id` advances. `evidence` opens the
Evidence panel and invokes its normal integrity verifier. A successful run does
not mean compatibility or evidence acceptance: inspect the prediction report.

With an isolated live Studio bridge (including native Windows), run:

```powershell
npx tsx scripts/studio_prime_smoke.ts --bridge-file D:\proof\bridge.json --outdir D:\proof\mcp-run
```

The output directory must be new. The proof creates a synthetic experiment using
the wizard, runs a supported RTT, verifies evidence, revises to an incompatible
RTT, reruns and checks null edited sequence plus preservation of prior evidence.
It uses an actual stdio MCP client and requires native screenshots. Use
`--reuse-session` explicitly to rerun against an existing Prime wizard session.
Start Studio with `PYTHONHASHSEED=0`; Windows native qualification used the real
`omnis-helix.exe` launcher and `QT_SCALE_FACTOR=1.5`. Automation verifies software
behavior; category frequencies are uncalibrated and no biological claim follows.

## Prime Results Review

`studio_prime_results_review` drives the dedicated scientific review page.
Its `action` is `open`, `select` or `get_state`. Select uses `run_id` and optional
nullable `compare_run_id` from the displayed registered-run catalog; arbitrary
paths are rejected. Optional `full_sequence` and `scroll_y` operate the actual
checkbox and scroll bar. State includes the displayed alignment, category rows,
reference compatibility/difference report, pending-design flag and scroll range.

Open/select reverify the selected bundles. Invalid evidence clears the result
display. Sequence differences are available only for matching reference identities
with resolved conditional sequences. An incompatible RTT has null differences.
The tool changes viewing state only; it never replaces the draft, creates a
simulation, alters evidence or grants approval. Edit current experiment in Studio
always refers to the active draft rather than a selected historical result.

From a new isolated native Studio session:

```powershell
npx tsx scripts/studio_prime_review_smoke.ts --bridge-file D:\proof\bridge.json --outdir D:\proof\review-run
```

The actual stdio client creates an experiment through the wizard, runs it,
revises its alternative allele and RTT together, inspects pending evidence,
reruns and compares the conditional sequences. It then tests an incompatible
RTT, category/footer scrolling, full-sequence mode, historical selection and
unchanged original bundle files. Native screenshots and a receipt are retained.
On a fresh Studio process reopened on that project, run the same script with
`--history-only` and a new output directory to verify all three registered runs
and their comparison without replacing its draft. Gateway stderr is retained
in `gateway.log`, including when a client attempt fails.

## Portable Prime comparison export

Select the two recorded executions with `studio_prime_results_review`, then call
`studio_prime_comparison_export` with only the optional `bridge_file`. It invokes
Studio's background exporter, waits for completion, checks the returned path,
size and SHA-256, and registers a ZIP artifact. The result includes
`comparison_artifact_id`, `comparison_checksum_sha256` and per-call provenance.
The gateway owns the output path; callers cannot overwrite arbitrary files.

The ZIP contains deterministic JSON, readable HTML and unchanged source bundles.
The Helix core verifier checks the ZIP inventory, embedded bundle integrity and
recomputed comparison/HTML offline. Integrity does not establish calibration,
authenticate origin or confer approval. Different-reference or unresolved
comparisons retain those limitations and never invent sequence differences.

The repeatable native proof is:

```powershell
npx tsx scripts/studio_prime_export_smoke.ts --bridge-file D:\proof\bridge.json --outdir D:\proof\mcp-run --project-root D:\proof\project --python D:\Helix\.venv\Scripts\python.exe
```

This negative-test harness requires an isolated sibling `project` fixture with
the `.prime-comparison-fixture` marker and the three synthetic runs produced by
the review proof. It verifies deterministic exports, registered artifact bytes,
offline verification and unresolved exports, then temporarily modifies one copied
input to test rejection. It restores the exact bytes in `finally` and checks all
source hashes. It never targets the original project. Native captures, operation
receipts and gateway stderr are retained.

Windows qualification on 2026-10-30 used the native launcher at 150% scaling.
The 16-call MCP proof passed with identical repeat ZIP checksums, 33 copied source
files unchanged, independent offline verification and rejected changed inputs.
The local gateway suite passed 41 tests; four Docker opt-in checks were skipped.

## Reviewer tools

| Tool | Behavior |
| --- | --- |
| `studio_pilot_review_get_state` | Inspect the reviewer without opening it; reports integrity, candidates, observation state, and export availability. |
| `studio_pilot_review_open` | Materialize and checksum-check `package_artifact_id`, plus optional `baseline_artifact_id`, then open the ZIPs in Studio. |
| `studio_pilot_review_start` | Enter anonymous `study_id` and `participant_id`, choose `role` (`reviewer` by default), and start actual observation timing. |
| `studio_pilot_review_fill` | Populate actual widgets with `answers` from the existing `helix.workflow_evaluation.answers.v1` contract. |
| `studio_pilot_review_view` | Show `tab_index` 0 comparison, 1 source text, 2 prior package, or 3 observation; optionally resize with paired `width`/`height` and set `scroll_y`. |
| `studio_pilot_review_export` | Reverify packages, finish the observation, and register a JSON artifact; returns `observation_artifact_id` and checksum. |

Import the package through `artifact_import` first. Local path imports must pass
the configured prefix and symlink policy. The open tool accepts ZIP artifact
identifiers, not arbitrary package paths. Its run links package and baseline
inputs. Export writes a new file in the gateway run workspace and links the
observation output; it accepts no caller-selected output path.

The form starts tasks as skipped and optional measurements as unknown. Verification
never auto-completes human tasks. Completed tasks require the corresponding
candidate alternatives, assumptions, and rationale; a completed explanation with
a baseline requires a change explanation. Studio blocks changed evidence and
retains input after validation or file errors. Export does not grant approval.

For automated tests, use an explicitly automated study and participant code and
label task notes and rationale as fixtures. Keep these records out of human-study
summaries. The gateway checks the answers structure; the live form enforces its
workflow semantics before export.

`studio_capture_screenshot` now accepts an optional `target`, including
`pilot_review` (visible dialog widget), `native` (active native window), and
`main`. Existing calls can omit it. Capture still registers a PNG artifact and
now returns optional target metadata, including native capture/fallback mode.
Use `artifact_preview_image` to inspect PNG dimensions and format. This tool
returns image metadata; the screenshot path contains the actual pixels.

## Repeatable live proof

Build the gateway, then run:

```bash
npx tsx scripts/studio_gui_smoke.ts \
  --helix-root /absolute/path/to/helix \
  --python /path/to/python-with-Studio-dependencies \
  --outdir /tmp/helix-mcp-gui-new
```

Use a new output directory and an X11 display. An isolated Xvfb display works;
some Wayland hosts block native XWayland capture. The proof rejects an offscreen
Qt platform and any screenshot fallback. It starts its own Studio and stdio
MCP gateway, isolates settings, bridge aliases, object storage and run workspaces,
and stops only its own processes. It uses in-memory Postgres even if the caller
has a `DATABASE_URL` configured.

The proof exercises MCP initialization and discovery, package import, candidate
inspection, form population, export and artifact retrieval. It captures desktop
and compact views at 1080×800 and 740×620, compares exported answers, checks the
observation schema/hash with Helix, verifies the source kit stayed unchanged, and
writes a Markdown check report. Observations and screenshots are test artifacts.

Input/output JSON contracts live under `contracts/tools/studio_pilot_review_*`.
`contracts/common.schema.json` includes the existing Helix answers contract
snapshot. Contract tests check schema/runtime agreement, including unknown values,
measured zero, duplicate alternatives, and paired dimensions. Gateway integration
tests cover artifact linking, incorrect types, invalid answers, policy denial,
and incomplete bridge responses. Docker tests remain separately opt-in.

## Prime synthetic validation benchmark (gateway 1.8.0)

`studio_prime_validation_benchmark` accepts only the optional `bridge_file`.
It starts the pinned Helix benchmark in a live Studio background worker, waits
for completion, and registers a checksum-checked ZIP in the managed artifact
store. Output includes `validation`, `validation_artifact_id` and
`validation_checksum_sha256`. The operation reports `session_unchanged` and a
PASS/FAIL receipt. A model FAIL remains a registered evidence artifact; execution
errors remain tool errors. The suite, expectations and output path cannot be
overridden through this tool. Busy simulations, modals and overlapping jobs are
refused. No approval or scientific calibration is implied.

Run the complete native Windows proof from the Helix checkout:

```powershell
./tools/prime_validation_benchmark.ps1 -OutputDirectory artifacts/prime-validation-new -McpRoot D:\HelixMCP
```

The wrapper builds this gateway and invokes `scripts/studio_prime_validation_smoke.ts`
using the real stdio SDK and `omnis-helix.exe` at 150% scaling. A NEW directory is
required. It verifies all 236 synthetic cases with both strands, repeatable
artifact digests and standalone offline verification, then runs the four edit
types through the actual wizard (plus-strand fixtures), results review and bundle
integrity checks. It preserves 44 historical run files and native screenshots,
checks the benchmark again with an active experiment, and closes only its own
isolated Studio project. Failed proof receipts/logs are retained. The oracle is
algorithmically separate, not independently authored or externally audited.

## Prime insertions and deletions (gateway 1.6.0)

Gateway 1.7.0 adds replacement authoring with `edit_kind: "REPLACEMENT"`,
`replacement_length` (reference interval length), `replacement_sequence` and the
existing zero-based `offset`. The wizard previews the retained reference and
explicit alternative, validates no-ops and lengths, and uses the runtime model
for compatibility. Review v4 accepts alignment v2 with required policy
`replacement_as_declared_delete_insert/v1`. Replacement alternatives occupy
insertion columns at the declared start and gap the replaced reference interval;
this does not infer within-interval correspondence. Managed exports use v4 ZIPs.

`scripts/studio_prime_replacement_smoke.ts --bridge-file /path/bridge.json --outdir /new/proof --python /path/python`
drives the live wizard-to-evidence journey, including invalid/no-op input,
length-changing revision, comparison, deterministic export, offline verification
and an incompatible RTT. Add `--history-only` after restarting the project to
verify read-only history and the same comparison export. All inputs are synthetic;
counts remain uncalibrated. Earlier tools and contracts remain supported.

Gateway 1.6.0 adds `edit_kind: "DELETION"` and `deletion_length` to wizard inputs;
`offset` is the zero-based start of the deleted retained-reference interval.
Studio validates and previews that interval. Review accepts v3 reports for
deletion/deletion or mixed comparisons; deleted reference positions are gaps,
with separate string and declared-edit equality. The managed export tool
registers comparison v3 ZIPs without adding caller-selected paths or approval.

Use `scripts/studio_prime_deletion_smoke.ts` with `--bridge-file`, a new
`--outdir`, and `--python` for the full native wizard-to-evidence proof. It checks
invalid/oversized numeric values, the exact interval preview, compatible deletion,
revision from two to three deleted bases, gapped review, byte-identical exports,
offline verification, unmodified first-run evidence and an incompatible RTT.
After restarting the project, `--history-only` checks its registered runs,
comparison and export without changing the active draft. All data are synthetic;
counts stay uncalibrated and no unsupported edited sequence is inferred.

`studio_prime_workflow` accepts optional `edit_kind` (`SUBSTITUTION` or
`INSERTION`) and `inserted_sequence` wizard fields. Insertion uses an explicit
zero-based reference boundary in `offset`; no source editing or sequence
generation occurs in the gateway. Existing substitution calls remain valid.
`studio_prime_results_review` accepts Studio v1/v2 reports. V2 comparison includes
declared reference/insertion columns, gapped tracks, separate string/edit equality
and alignment difference indices. Those indices are not reference offsets.
The managed comparison export tool is unchanged and registers v2 insertion ZIPs.

After launching a native Studio fixture with its bridge enabled:

```bash
npx tsx scripts/studio_prime_insertion_smoke.ts --bridge-file /path/bridge.json --outdir /new/proof --python /path/python
# Reopen the same isolated project, then verify saved history:
npx tsx scripts/studio_prime_insertion_smoke.ts --bridge-file /path/bridge.json --outdir /new/reopen-proof --python /path/python --history-only
```

The proof uses synthetic retained targets, actual wizard controls and Run action,
native screenshots, revision, different-length comparison, deterministic export,
offline verification and preserved historic evidence. Unsupported RTT resolves
to no edited sequence. Counts remain uncalibrated. These are software checks.
