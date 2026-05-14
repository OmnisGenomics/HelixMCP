import * as net from "node:net";
import os from "node:os";
import path from "node:path";
import { promises as fs } from "node:fs";

import type { JsonObject } from "../core/json.js";

export interface StudioBridgeInfo {
  bridge_file: string;
  host: string;
  port: number;
  pid: number;
  session_id: string;
  started_at: string;
  project_root?: string;
}

export interface StudioBridgeCallOptions {
  bridgeFile?: string;
  timeoutMs?: number;
}

export function defaultStudioBridgeFile(): string {
  return process.env.HELIX_STUDIO_MCP_BRIDGE_FILE || path.join(os.tmpdir(), "helix-studio-mcp-bridge.json");
}

export async function readStudioBridgeInfo(bridgeFile?: string): Promise<StudioBridgeInfo> {
  const resolved = path.resolve(bridgeFile || defaultStudioBridgeFile());
  const payload = JSON.parse(await fs.readFile(resolved, "utf8")) as Record<string, unknown>;
  const host = String(payload.host || "").trim();
  const port = Number(payload.port || 0);
  const pid = Number(payload.pid || 0);
  const sessionId = String(payload.session_id || "").trim();
  const startedAt = String(payload.started_at || "").trim();
  if (!host) throw new Error(`studio bridge info missing host in ${resolved}`);
  if (!Number.isInteger(port) || port <= 0 || port > 65535) {
    throw new Error(`studio bridge info has invalid port in ${resolved}`);
  }
  if (!Number.isInteger(pid) || pid <= 0) {
    throw new Error(`studio bridge info has invalid pid in ${resolved}`);
  }
  if (!sessionId) throw new Error(`studio bridge info missing session_id in ${resolved}`);
  if (!startedAt) throw new Error(`studio bridge info missing started_at in ${resolved}`);
  const info: StudioBridgeInfo = {
    bridge_file: resolved,
    host,
    port,
    pid,
    session_id: sessionId,
    started_at: startedAt
  };
  if (typeof payload.project_root === "string" && payload.project_root.length > 0) {
    info.project_root = payload.project_root;
  }
  return info;
}

export async function callStudioBridge(
  command: string,
  params: Record<string, unknown> = {},
  options: StudioBridgeCallOptions = {}
): Promise<JsonObject> {
  const info = await readStudioBridgeInfo(options.bridgeFile);
  const timeoutMs = Math.max(1_000, options.timeoutMs ?? 10_000);

  return await new Promise<JsonObject>((resolve, reject) => {
    const socket = net.createConnection({ host: info.host, port: info.port });
    let settled = false;
    let buffer = "";

    const fail = (error: Error): void => {
      if (settled) return;
      settled = true;
      try {
        socket.destroy();
      } catch {
        // Best effort.
      }
      reject(error);
    };

    socket.setEncoding("utf8");
    socket.setTimeout(timeoutMs, () => fail(new Error(`studio bridge timed out after ${timeoutMs} ms`)));
    socket.once("error", (error) => fail(error instanceof Error ? error : new Error(String(error))));
    socket.on("data", (chunk: string) => {
      if (settled) return;
      buffer += chunk;
      const newline = buffer.indexOf("\n");
      if (newline < 0) return;
      const line = buffer.slice(0, newline).trim();
      buffer = buffer.slice(newline + 1);
      try {
        const payload = JSON.parse(line) as Record<string, unknown>;
        if (!payload.ok) {
          throw new Error(String(payload.error || "studio bridge rejected request"));
        }
        settled = true;
        socket.end();
        resolve(payload as JsonObject);
      } catch (error) {
        fail(error instanceof Error ? error : new Error(String(error)));
      }
    });
    socket.once("connect", () => {
      socket.write(`${JSON.stringify({ command, params })}\n`);
    });
  });
}
