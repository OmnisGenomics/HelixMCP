import { describe, it, expect, beforeAll, afterAll, vi } from "vitest";
import { mkdtemp, rm, readFile, writeFile } from "fs/promises";
import os from "os";
import path from "path";
import * as net from "node:net";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { CallToolResultSchema } from "@modelcontextprotocol/sdk/types.js";

import { newDb } from "pg-mem";
import * as pg from "pg";

import { applySqlFile } from "../src/db/bootstrap.js";
import { createDb } from "../src/db/connection.js";
import { PostgresStore } from "../src/store/postgresStore.js";
import { LocalObjectStore } from "../src/artifacts/localObjectStore.js";
import { ArtifactService } from "../src/artifacts/artifactService.js";
import { PolicyEngine } from "../src/policy/policy.js";
import { createGatewayServer } from "../src/mcp/gatewayServer.js";
import { DefaultExecutionService } from "../src/execution/executionService.js";
import type { ArtifactId, ProjectId, RunId } from "../src/core/ids.js";

describe.sequential("studio bridge gateway tools", () => {
  let tmpDir: string;
  let pool: pg.Pool;
  let store: PostgresStore;
  let artifacts: ArtifactService;
  let policy: PolicyEngine;
  let client: Client;
  let serverTransport: InMemoryTransport;
  let clientTransport: InMemoryTransport;

  async function callTool(name: string, args: Record<string, unknown>, timeoutMs = 15_000) {
    return client.request(
      { method: "tools/call", params: { name, arguments: args } },
      CallToolResultSchema,
      { timeout: timeoutMs }
    );
  }

  beforeAll(async () => {
    tmpDir = await mkdtemp(path.join(os.tmpdir(), "helixmcp-studio-"));

    const mem = newDb({ autoCreateForeignKeyIndices: true });
    const adapter = mem.adapters.createPg();
    pool = new adapter.Pool() as unknown as pg.Pool;
    await applySqlFile(pool, path.resolve("db/schema.sql"));

    const db = createDb(pool);
    store = new PostgresStore(db);
    const objects = new LocalObjectStore(path.join(tmpDir, "objects"));
    artifacts = new ArtifactService(store, objects);
    policy = await PolicyEngine.loadFromFile(path.resolve("policies/default.policy.yaml"));
    const runsDir = path.join(tmpDir, "runs");
    const execution = new DefaultExecutionService({ policy });
    const server = createGatewayServer({ policy, store, artifacts, execution, runsDir });

    [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    await server.connect(serverTransport);

    client = new Client({ name: "helixmcp-studio-test-client", version: "0.0.0" });
    await client.connect(clientTransport);
  });

  afterAll(async () => {
    await clientTransport.close();
    await serverTransport.close();
    await pool.end();
    await rm(tmpDir, { recursive: true, force: true });
  });

  it("drives a local studio bridge through MCP tools", async () => {
    const bridgeFile = path.join(tmpDir, "studio-bridge.json");
    const screenshotPath = path.join(tmpDir, "studio-screenshot.png");
    const pngBytes = Buffer.from(
      "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4//8/AwAI/AL+KDgHAAAAAElFTkSuQmCC",
      "base64"
    );
    await writeFile(screenshotPath, pngBytes);
    const commands: Array<{ command: string; params: Record<string, unknown> }> = [];
    let currentState: Record<string, unknown> = {
      current_tab: "analysis",
      available_tabs: ["analysis", "visualizations", "lightcone"],
      active_run_id: null,
      project_root: tmpDir,
      visualization: {
        mode: "explore",
        editor_profile: "spcas9",
        repair_profile: "balanced",
        accessibility_profile: "baseline",
        selected_scale_id: "population",
        selected_outcome_label: null,
        available_outcomes: ["no_cut", "small_deletion"],
        compare_loaded: false,
        run_id: null,
        run_label: null
      }
    };

    const bridgeServer = net.createServer((socket) => {
      let buffer = "";
      socket.setEncoding("utf8");
      socket.on("data", (chunk) => {
        buffer += chunk;
        let newline = buffer.indexOf("\n");
        while (newline >= 0) {
          const line = buffer.slice(0, newline).trim();
          buffer = buffer.slice(newline + 1);
          newline = buffer.indexOf("\n");
          if (!line) continue;
          const request = JSON.parse(line) as { command: string; params: Record<string, unknown> };
          commands.push(request);
          if (request.command === "open_tab" && typeof request.params.tab === "string") {
            currentState = { ...currentState, current_tab: request.params.tab };
          }
          if (request.command === "load_evs") {
            if (request.params.role === "compare") {
              currentState = {
                ...currentState,
                current_tab: request.params.open_visualizations ? "visualizations" : currentState.current_tab,
                visualization: {
                  ...(currentState.visualization as Record<string, unknown>),
                  compare_loaded: true,
                  mode: request.params.set_compare_mode ? "compare" : (currentState.visualization as any).mode
                }
              };
            } else {
              currentState = {
                ...currentState,
                active_run_id: "evs_demo_v11",
                visualization: {
                  ...(currentState.visualization as Record<string, unknown>),
                  run_id: "evs_demo_v11",
                  run_label: "evs_demo_v11"
                }
              };
            }
          }
          if (request.command === "set_visualization_state") {
            currentState = {
              ...currentState,
              current_tab: request.params.open_visualizations ? "visualizations" : currentState.current_tab,
              visualization: {
                ...(currentState.visualization as Record<string, unknown>),
                mode: request.params.mode ?? (currentState.visualization as any).mode,
                editor_profile: request.params.editor_profile ?? (currentState.visualization as any).editor_profile,
                repair_profile: request.params.repair_profile ?? (currentState.visualization as any).repair_profile,
                accessibility_profile:
                  request.params.accessibility_profile ?? (currentState.visualization as any).accessibility_profile,
                selected_scale_id: request.params.selected_scale_id ?? (currentState.visualization as any).selected_scale_id,
                selected_outcome_label:
                  request.params.selected_outcome_label ?? (currentState.visualization as any).selected_outcome_label
              }
            };
          }
          if (request.command === "clear_compare") {
            currentState = {
              ...currentState,
              visualization: {
                ...(currentState.visualization as Record<string, unknown>),
                compare_loaded: false,
                mode: "explore"
              }
            };
          }
          if (request.command === "reset_layout") {
            currentState = { ...currentState, current_tab: "analysis" };
          }
          let screenshot: Record<string, unknown> | null = null;
          if (request.command === "capture_screenshot") {
            currentState = {
              ...currentState,
              current_tab: typeof request.params.tab === "string" ? request.params.tab : currentState.current_tab
            };
            screenshot = {
              path: typeof request.params.path === "string" ? request.params.path : screenshotPath,
              format: "PNG",
              width_px: 1,
              height_px: 1,
              device_pixel_ratio: 1,
              size_bytes: String(pngBytes.byteLength),
              checksum_sha256: "sha256:unused-by-gateway-test",
              captured_at: "2026-04-24T18:45:00Z"
            };
          }
          const response = {
            ok: true,
            command: request.command,
            bridge: bridgeInfo,
            state: currentState,
            ...(screenshot ? { screenshot } : {})
          };
          socket.write(`${JSON.stringify(response)}\n`);
        }
      });
    });

    await new Promise<void>((resolve, reject) => {
      bridgeServer.listen(0, "127.0.0.1", () => resolve());
      bridgeServer.once("error", reject);
    });

    const address = bridgeServer.address();
    if (address == null || typeof address === "string") {
      throw new Error("bridge server did not expose a TCP address");
    }

    const bridgeInfo = {
      bridge_file: bridgeFile,
      host: "127.0.0.1",
      port: address.port,
      pid: process.pid,
      session_id: "studio-test-session",
      started_at: "2026-04-24T18:40:00Z",
      project_root: tmpDir
    };
    await writeFile(bridgeFile, JSON.stringify(bridgeInfo), "utf8");

    try {
      const state = await callTool("studio_get_state", { bridge_file: bridgeFile });
      const stateSc = state.structuredContent as any;
      expect(stateSc.studio_state.current_tab).toBe("analysis");
      expect(stateSc.log_artifact_id).toMatch(/^art_/);

      const open = await callTool("studio_open_tab", { bridge_file: bridgeFile, tab: "visualizations" });
      const openSc = open.structuredContent as any;
      expect(openSc.studio_state.current_tab).toBe("visualizations");

      const compare = await callTool("studio_load_evs", {
        bridge_file: bridgeFile,
        path: "/tmp/compare.evs.json",
        role: "compare",
        open_visualizations: true,
        set_compare_mode: true
      });
      const compareSc = compare.structuredContent as any;
      expect(compareSc.studio_state.visualization.compare_loaded).toBe(true);
      expect(compareSc.studio_state.visualization.mode).toBe("compare");

      const edited = await callTool("studio_visualization_edit", {
        bridge_file: bridgeFile,
        mode: "compare",
        editor_profile: "hifi",
        repair_profile: "precise",
        accessibility_profile: "open",
        selected_scale_id: "cell",
        selected_outcome_label: "small_deletion"
      });
      const editedSc = edited.structuredContent as any;
      expect(editedSc.studio_state.visualization.editor_profile).toBe("hifi");
      expect(editedSc.studio_state.visualization.repair_profile).toBe("precise");
      expect(editedSc.studio_state.visualization.accessibility_profile).toBe("open");
      expect(editedSc.studio_state.visualization.selected_scale_id).toBe("cell");
      expect(editedSc.studio_state.visualization.selected_outcome_label).toBe("small_deletion");

      const screenshot = await callTool("studio_capture_screenshot", {
        bridge_file: bridgeFile,
        path: screenshotPath,
        tab: "visualizations",
        overwrite: true
      });
      const screenshotSc = screenshot.structuredContent as any;
      expect(screenshotSc.studio_state.current_tab).toBe("visualizations");
      expect(screenshotSc.screenshot.path).toBe(screenshotPath);
      expect(screenshotSc.screenshot.artifact_id).toMatch(/^art_/);
      expect(screenshotSc.screenshot.format).toBe("PNG");
      expect(screenshotSc.screenshot.width_px).toBe(1);
      expect(screenshotSc.screenshot.height_px).toBe(1);
      expect(screenshotSc.screenshot.size_bytes).toBe(String(pngBytes.byteLength));
      expect(screenshotSc.screenshot.checksum_sha256).toMatch(/^sha256:[a-f0-9]{64}$/);

      const cleared = await callTool("studio_clear_compare", { bridge_file: bridgeFile });
      const clearedSc = cleared.structuredContent as any;
      expect(clearedSc.studio_state.visualization.compare_loaded).toBe(false);

      const reset = await callTool("studio_reset_layout", { bridge_file: bridgeFile });
      const resetSc = reset.structuredContent as any;
      expect(resetSc.studio_state.current_tab).toBe("analysis");

      expect(commands.map((item) => item.command)).toEqual([
        "get_state",
        "open_tab",
        "load_evs",
        "set_visualization_state",
        "capture_screenshot",
        "clear_compare",
        "reset_layout"
      ]);
    } finally {
      await new Promise<void>((resolve) => bridgeServer.close(() => resolve()));
    }
  });

  it("binds ZIP inputs, rejects schema/state bypass, and registers the observation output", async () => {
    const bridgeFile = path.join(tmpDir, "review-bridge.json");
    const commands: string[] = [];
    let review: Record<string, unknown> = {
      visible: false, status: "Automated mock bridge fixture", integrity_verified: false,
      candidate_count: 0, manifest_sha256: null, observation_state: "pending",
      export_enabled: false, exported_path: null
    };
    let recordedAnswers: unknown;
    const fixtureAnswers = {
      schema: "helix.workflow_evaluation.answers.v1",
      tasks: Object.fromEntries(["compare_candidates","inspect_assumptions","explain_decision","verify_package"].map((name) => [name,{status:"skipped",assistance_needed:false,notes:"Automated mocked gateway fixture"}])),
      selected_candidate_id: null, alternatives_considered: [], decision_rationale: "",
      assumptions: [], change_explanation: null, intent_to_export_seconds: null,
      manual_handoffs_removed: null, return_visit: false, would_use_again: null
    };
    const listener = net.createServer((socket) => {
      let buffer = "";
      socket.setEncoding("utf8");
      socket.on("data", async (chunk) => {
        buffer += chunk;
        if (!buffer.includes("\n")) return;
        const {command, params} = JSON.parse(buffer.split("\n")[0]!);
        commands.push(command);
        try {
          if (command === "pilot_review_open") {
            expect(await readFile(params.package, "utf8")).toBe("ZIP mock fixture");
            expect(await readFile(params.baseline, "utf8")).toBe("ZIP mock fixture");
            review = {...review,visible:true,integrity_verified:true,candidate_count:3,manifest_sha256:"c".repeat(64)};
          } else if (command === "pilot_review_start") {
            review = {...review,observation_state:"started",export_enabled:true};
          } else if (command === "pilot_review_fill") {
            if (review.observation_state !== "started") throw new Error("start an observation before entering answers");
            recordedAnswers = params.answers;
          } else if (command === "pilot_review_export") {
            await writeFile(params.path, JSON.stringify({schema:"automated.mock.gateway.fixture",answers:recordedAnswers}));
            review = {...review,observation_state:"finished",export_enabled:false,exported_path:params.path};
          }
          socket.end(JSON.stringify({ok:true,command,bridge:bridgeInfo,pilot_review:review}) + "\n");
        } catch(error) {
          socket.end(JSON.stringify({ok:false,error:String(error)}) + "\n");
        }
      });
    });
    await new Promise<void>((resolve) => listener.listen(0,"127.0.0.1",resolve));
    const address = listener.address() as net.AddressInfo;
    const bridgeInfo = {bridge_file:bridgeFile,host:"127.0.0.1",port:address.port,pid:process.pid,session_id:"automated-mock-review",started_at:"2026-10-02T00:00:00Z"};
    await writeFile(bridgeFile,JSON.stringify(bridgeInfo));
    const projectId = "proj_01ARZ3NDEKTSV4RRFFQ69G5FAV" as ProjectId;
    const zip = await artifacts.importArtifact({projectId,source:{kind:"inline_text",text:"ZIP mock fixture"},typeHint:"ZIP",label:"fixture.zip",createdByRunId:null,maxBytes:null});
    const wrong = await artifacts.importArtifact({projectId,source:{kind:"inline_text",text:"plain text"},typeHint:"TEXT",label:"wrong.txt",createdByRunId:null,maxBytes:null});
    try {
      const before = await callTool("studio_pilot_review_get_state",{bridge_file:bridgeFile});
      expect((before.structuredContent as any).pilot_review.visible).toBe(false);
      const badType = await callTool("studio_pilot_review_open",{bridge_file:bridgeFile,package_artifact_id:wrong.artifactId});
      expect(badType.isError).toBe(true);
      expect(commands).toEqual(["pilot_review_state"]);
      const opened = await callTool("studio_pilot_review_open",{bridge_file:bridgeFile,package_artifact_id:zip.artifactId,baseline_artifact_id:zip.artifactId});
      const open = opened.structuredContent as any;
      expect(open.pilot_review.candidate_count).toBe(3);
      expect(await store.listRunInputs(open.provenance_run_id as RunId)).toEqual([
        {artifactId:zip.artifactId,role:"package"},{artifactId:zip.artifactId,role:"baseline"}
      ]);
      const tooEarly = await callTool("studio_pilot_review_fill",{bridge_file:bridgeFile,answers:fixtureAnswers});
      expect(tooEarly.isError).toBe(true);
      const beforeInvalid = commands.length;
      const badAnswers = await callTool("studio_pilot_review_fill",{bridge_file:bridgeFile,answers:{...fixtureAnswers,approved:true}});
      expect(badAnswers.isError).toBe(true);
      expect(commands.length).toBe(beforeInvalid);
      const started = await callTool("studio_pilot_review_start",{bridge_file:bridgeFile,study_id:"automated-study",participant_id:"automated-fixture"});
      expect((started.structuredContent as any).pilot_review.observation_state).toBe("started");
      await callTool("studio_pilot_review_fill",{bridge_file:bridgeFile,answers:fixtureAnswers});
      const exported = await callTool("studio_pilot_review_export",{bridge_file:bridgeFile});
      const result = exported.structuredContent as any;
      expect(result.pilot_review.observation_state).toBe("finished");
      const outputs = await store.listRunOutputs(result.provenance_run_id as RunId);
      expect(outputs).toContainEqual({artifactId:result.observation_artifact_id,role:"observation"});
      const observation = await artifacts.getArtifact(result.observation_artifact_id as ArtifactId);
      expect(observation?.checksumSha256).toBe(result.observation_checksum_sha256);
      expect(observation?.type).toBe("JSON");
      expect(JSON.parse(await readFile(result.pilot_review.exported_path,"utf8")).answers).toEqual(fixtureAnswers);
      const beforeDenied = commands.length;
      const deny = vi.spyOn(policy,"assertToolAllowed").mockImplementation(() => {throw new Error("policy denied reviewer fixture");});
      try {
        const denied = await callTool("studio_pilot_review_get_state",{bridge_file:bridgeFile});
        expect(denied.isError).toBe(true);
        expect(commands.length).toBe(beforeDenied);
      } finally {deny.mockRestore();}
    } finally {
      await new Promise<void>((resolve) => listener.close(() => resolve()));
    }
  });
});
