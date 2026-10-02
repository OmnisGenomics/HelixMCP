/** Live Studio proof through the actual stdio MCP gateway. Automated fixtures only. */
import { promises as fs } from "node:fs";
import path from "node:path";
import { spawn, type ChildProcess } from "node:child_process";
import { createHash } from "node:crypto";
import { promisify } from "node:util";
import { execFile } from "node:child_process";
import { isDeepStrictEqual, parseArgs } from "node:util";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { CallToolResultSchema } from "@modelcontextprotocol/sdk/types.js";

const exec = promisify(execFile);
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const { values } = parseArgs({ options: {
  "helix-root": { type: "string" }, python: { type: "string", default: "python3" },
  outdir: { type: "string" }
} });
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function main(): Promise<void> {
  if (!values["helix-root"] || !values.outdir) throw new Error("--helix-root and --outdir are required");
  if (!process.env.DISPLAY || process.env.QT_QPA_PLATFORM === "offscreen") {
    throw new Error("Requires a live X11 display (Xvfb works), not an offscreen Qt platform");
  }
  const helix = path.resolve(values["helix-root"]);
  const outdir = path.resolve(values.outdir);
  const python = values.python!;
  await fs.mkdir(outdir, { recursive: false }); // New directory; preserve prior evidence.
  const runtime = path.join(outdir, "runtime");
  await fs.mkdir(runtime);
  const config = path.join(outdir, "configuration");
  await fs.mkdir(config);
  const bridgeFile = path.join(runtime, "studio-bridge.json");
  const studioEnv = { ...process.env, PYTHONPATH: path.join(helix, "src"),
    TMPDIR: runtime, XDG_CONFIG_HOME: config, QT_QPA_PLATFORM: "xcb",
    OMNIS_HELIX_ENABLE_MCP_BRIDGE: "1", OMNIS_HELIX_SKIP_WELCOME: "1",
    OMNIS_HELIX_MCP_BRIDGE_FILE: bridgeFile };
  const kit = path.join(outdir, "kit");
  await exec(python, ["-m", "helix.evaluation.workflow_cli", "prepare-demo", "--out-dir", kit], { env: studioEnv });
  const originalZip = await fs.readFile(path.join(kit, "pilot_package.zip"));
  const log = await fs.open(path.join(outdir, "studio.log"), "wx");
  let studio: ChildProcess | undefined;
  let startupError: Error | undefined;
  const client = new Client({ name: "helixmcp-live-gui-smoke", version: "1.0.0" });
  const mcpEnv = Object.fromEntries(Object.entries(process.env).filter((pair): pair is [string, string] => typeof pair[1] === "string"));
  Object.assign(mcpEnv, { DATABASE_URL: "", AUTO_SCHEMA: "true", GATEWAY_IMPORT_ROOT: outdir,
    GATEWAY_POLICY_PATH: path.join(root, "policies/default.policy.yaml"),
    OBJECT_STORE_DIR: path.join(outdir, "objects"), RUNS_DIR: path.join(outdir, "runs") });
  const transport = new StdioClientTransport({ command: process.execPath,
    args: [path.join(root, "dist/index.js")], cwd: root, env: mcpEnv, stderr: "pipe" });
  let calls = 0;
  const call = async (name: string, args: Record<string, unknown> = {}) => {
    const result = await client.request({ method: "tools/call", params: { name, arguments: args } }, CallToolResultSchema, { timeout: 45_000 });
    if (result.isError) throw new Error(`${name}: ${JSON.stringify(result.content)}`);
    if (!result.structuredContent) throw new Error(`${name} returned no structured result`);
    calls++;
    return result.structuredContent as any;
  };
  const studioCall = (name: string, args: Record<string, unknown> = {}) => call(name, { bridge_file: bridgeFile, ...args });
  const check = (condition: unknown, message: string) => { if (!condition) throw new Error(message); };
  const capture = async (name: string, tab: number, width: number, height: number, scroll = 0) => {
    await studioCall("studio_pilot_review_view", { tab_index: tab, width, height, scroll_y: scroll });
    await sleep(200);
    const shot = await studioCall("studio_capture_screenshot", { path: path.join(outdir, name), target: "native" });
    check(shot.screenshot.target?.class_name === "PilotReviewDialog" && shot.screenshot.target?.capture_mode === "native_window", "Expected actual native reviewer window capture");
    const preview = await call("artifact_preview_image", { artifact_id: shot.screenshot.artifact_id });
    check(preview.format === "PNG" || preview.image?.format === "PNG", "Screenshot artifact preview is not a PNG");
    console.log(`CAPTURE ${name}: ${shot.screenshot.width_px}x${shot.screenshot.height_px}, ${shot.screenshot.checksum_sha256}`);
  };
  try {
    studio = spawn(python, ["-m", "helix.studio.app", "--skip-welcome", "--force-fullscreen", helix], { cwd: helix, env: studioEnv, stdio: ["ignore", log.fd, log.fd] });
    studio.once("error", (error) => { startupError = error; });
    let ready = false;
    for (let attempt = 0; attempt < 180; attempt++) {
      if (startupError) throw startupError;
      if (studio.exitCode !== null) throw new Error("Studio exited before advertising its bridge; see studio.log");
      try { await fs.access(bridgeFile); ready = true; break; } catch { await sleep(250); }
    }
    check(ready, "Studio bridge did not appear within 45 seconds");
    await client.connect(transport);
    const listed = await client.listTools();
    check(listed.tools.some((tool) => tool.name === "studio_pilot_review_open"), "New reviewer tools not discovered over stdio");
    const state = await studioCall("studio_get_state");
    check(state.studio_state.project_root === helix, "Wrong Studio project");
    const pending = await studioCall("studio_pilot_review_get_state");
    check(!pending.pilot_review.visible && pending.pilot_review.observation_state === "pending", "State inspection unexpectedly opened a review");
    const imported = await call("artifact_import", { project_id: "proj_01ARZ3NDEKTSV4RRFFQ69G5FAV", type_hint: "ZIP", source: { kind: "local_path", path: path.join(kit, "pilot_package.zip") } });
    const opened = await studioCall("studio_pilot_review_open", { package_artifact_id: imported.artifact.artifact_id });
    check(opened.pilot_review.candidate_count === 3 && opened.pilot_review.integrity_verified, "Candidates not verified and displayed");
    check(!opened.pilot_review.export_enabled, "Pending review export unexpectedly enabled");
    await capture("comparison-desktop.png", 0, 1080, 800);
    await capture("comparison-compact.png", 0, 740, 620);
    await studioCall("studio_pilot_review_start", { study_id: "automated-mcp-smoke", participant_id: "automated-fixture", role: "reviewer" });
    const task = { status: "completed", assistance_needed: false, notes: "Automated stdio MCP fixture; not a human observation." };
    const answers = { schema: "helix.workflow_evaluation.answers.v1", tasks: {
      compare_candidates: task, inspect_assumptions: task, explain_decision: task, verify_package: task
    }, selected_candidate_id: "base_cbe_A", alternatives_considered: ["prime_peg_A", "prime_peg_B"],
      decision_rationale: "Automated fixture: base policy uses dominant mass, while prime uses intended mass. This policy selection remains unapproved.",
      assumptions: ["Fixed synthetic inputs; accuracy and probability calibration not established."],
      change_explanation: null, intent_to_export_seconds: null, manual_handoffs_removed: null,
      return_visit: false, would_use_again: null };
    await studioCall("studio_pilot_review_fill", { answers });
    await capture("observation-desktop.png", 3, 1080, 800, 220);
    await capture("observation-compact.png", 3, 740, 620, 100000);
    const exported = await studioCall("studio_pilot_review_export");
    check(exported.pilot_review.observation_state === "finished" && !exported.pilot_review.export_enabled, "Observation not finalized");
    const recordPath = exported.pilot_review.exported_path as string;
    const raw = await fs.readFile(recordPath);
    const record = JSON.parse(raw.toString("utf8"));
    check(isDeepStrictEqual(record.answers, answers), "Exported form answers differ from the automated fixture");
    check(record.package.manifest_sha256 === opened.pilot_review.manifest_sha256, "Record/package identity mismatch");
    check(exported.observation_checksum_sha256 === "sha256:" + createHash("sha256").update(raw).digest("hex"), "Observation artifact checksum mismatch");
    const artifact = await call("artifact_get", { artifact_id: exported.observation_artifact_id });
    check(artifact.artifact.checksum_sha256 === exported.observation_checksum_sha256, "Registered observation checksum mismatch");
    await exec(python, ["-c", "import json,sys; from helix.evaluation.workflow import summarize_evaluations; r=json.load(open(sys.argv[1])); assert summarize_evaluations([r])['n_records']==1", recordPath], { env: studioEnv });
    await exec(python, ["-m", "helix.evaluation.workflow_cli", "verify-demo", kit], { env: studioEnv });
    check(originalZip.equals(await fs.readFile(path.join(kit, "pilot_package.zip"))), "Source kit changed");
    await capture("exported.png", 3, 1080, 800, 220);
    await fs.writeFile(path.join(outdir, "README.md"), `# Live Studio MCP smoke\n\nPASS: stdio initialization, discovery, ${calls} successful policy-gated MCP calls, live xcb form, native desktop/compact screenshots registered as PNG artifacts, package-bound JSON observation registered as an artifact, v1 schema/hash validation, unchanged synthetic kit.\n\nAll task answers are automated fixtures. Human observations remain pending; exclude this study from human evaluation summaries.\n`);
    console.log(`PASS: ${calls} MCP calls through real stdio gateway and live Studio. Automated fixture only.`);
  } finally {
    await client.close().catch(() => {});
    await transport.close().catch(() => {});
    if (studio && studio.exitCode === null) {
      studio.kill("SIGTERM");
      for (let i = 0; i < 32 && studio.exitCode === null; i++) await sleep(250);
      if (studio.exitCode === null) studio.kill("SIGKILL");
    }
    await log.close();
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
