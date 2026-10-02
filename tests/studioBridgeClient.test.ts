import { expect, it } from "vitest";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import net from "node:net";
import { callStudioBridge, readStudioBridgeInfo } from "../src/mcp/studioBridgeClient.js";

it("rejects remote bridge hosts before making a connection", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "bridge-host-"));
  const file = path.join(dir, "bridge.json");
  try {
    await fs.writeFile(file, JSON.stringify({host:"192.0.2.1",port:1234,pid:123,session_id:"fixture",started_at:"2026-10-02T00:00:00Z"}));
    await expect(readStudioBridgeInfo(file)).rejects.toThrow("loopback host");
  } finally {await fs.rm(dir,{recursive:true,force:true});}
});

it("rejects incomplete responses promptly instead of hanging", async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "bridge-eof-"));
  const file = path.join(dir, "bridge.json");
  const server = net.createServer((socket) => {socket.once("data",() => socket.end('{"ok":'));});
  try {
    await new Promise<void>((resolve) => server.listen(0,"127.0.0.1",resolve));
    const address = server.address() as net.AddressInfo;
    await fs.writeFile(file,JSON.stringify({host:"127.0.0.1",port:address.port,pid:process.pid,session_id:"fixture",started_at:"2026-10-02T00:00:00Z"}));
    await expect(callStudioBridge("get_state",{}, {bridgeFile:file,timeoutMs:1000})).rejects.toThrow("closed before a complete response");
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await fs.rm(dir,{recursive:true,force:true});
  }
});
