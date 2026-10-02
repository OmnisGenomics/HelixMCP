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
refresh its tool catalog. The gateway advertises version `1.1.0`.

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
