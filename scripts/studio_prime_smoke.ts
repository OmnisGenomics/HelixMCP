/** Synthetic Prime wizard qualification through an actual stdio MCP client. */
import { promises as fs } from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { parseArgs } from "node:util";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { CallToolResultSchema } from "@modelcontextprotocol/sdk/types.js";

const { values } = parseArgs({ options: { "bridge-file": {type:"string"}, outdir:{type:"string"}, "reuse-session":{type:"boolean",default:false} } });
if (!values["bridge-file"] || !values.outdir) throw new Error("--bridge-file and --outdir required");
const outdir = path.resolve(values.outdir);
await fs.mkdir(outdir, { recursive: false });
const env = Object.fromEntries(Object.entries(process.env).filter((p): p is [string,string] => typeof p[1] === "string"));
Object.assign(env, { DATABASE_URL:"", AUTO_SCHEMA:"true", OBJECT_STORE_DIR:path.join(outdir,"objects"), RUNS_DIR:path.join(outdir,"runs") });
const client = new Client({name:"helix-prime-synthetic-qualification",version:"1.0.0"});
const transport = new StdioClientTransport({command:process.execPath,args:[path.resolve("dist/index.js")],cwd:process.cwd(),env,stderr:"pipe"});
const calls: unknown[] = [];
const check = (condition: unknown, message: string) => { if (!condition) throw new Error(message); };
const call = async (name: string, args: Record<string, unknown>) => {
  const result = await client.request({method:"tools/call",params:{name,arguments:args}},CallToolResultSchema,{timeout:45000});
  check(!result.isError, JSON.stringify(result.content));
  calls.push({name,args,result:result.structuredContent});
  return result.structuredContent as any;
};
const prime = async (action: string, params: Record<string,unknown> = {}) =>
  (await call("studio_prime_workflow",{bridge_file:values["bridge-file"],action,...params})).prime;
const waitFor = async (predicate: (state:any)=>boolean) => {
  const end=Date.now()+60000;
  while(Date.now()<end) { const state=await prime("get_state"); if(predicate(state)) return state; await new Promise(r=>setTimeout(r,200)); }
  throw new Error("Timed out waiting for Studio state");
};
const capture = async (label:string) => {
  // Let queued Qt layout/paint events settle after an evidence-panel transition.
  await new Promise(resolve=>setTimeout(resolve,250));
  const result=await call("studio_capture_screenshot",{bridge_file:values["bridge-file"],path:path.join(outdir,label+".png"),target:"native",overwrite:false});
  check(result.screenshot.target?.capture_mode === "native_window","Native screenshot required");
};
try {
  await client.connect(transport);
  const tools=await client.listTools();
  check(tools.tools.some(t=>t.name==="studio_prime_workflow"),"Prime tool missing");
  const target="A".repeat(40)+"GGGCCAGGAGCCTGAAGTCT"+"AGG"+"ACGT".repeat(10);
  const edited=target.slice(0,63)+"T"+target.slice(64);
  const rc=(s:string)=>s.split("").reverse().map(c=>({A:"T",C:"G",G:"C",T:"A"}[c])).join("");
  const imported=await call("artifact_import",{project_id:"proj_01ARZ3NDEKTSV4RRFFQ69G5FAV",type_hint:"TEXT",source:{kind:"inline_text",text:">synthetic\n"+target+"\n"}});
  await prime("wizard_open",{mode:values["reuse-session"] ? "edit" : "new"});
  await waitFor(s=>s.wizard!==null);
  await prime("wizard_target",{target_artifact_id:imported.artifact.artifact_id,synthetic:true});
  const inputs={spacer:"GGGCCAGGAGCCTGAAGTCT",pbs:rc(target.slice(52,57)),rtt:rc(edited.slice(57,70)),offset:"63",reference:"A",alternative:"T"};
  await prime("wizard_fill",{inputs});
  await prime("wizard_navigate",{stage:2});
  const form=await prime("get_state");
  check(form.wizard.feedback.includes("supported"),"Compatible wizard feedback missing");
  await capture("supported-parameters");
  await prime("wizard_navigate",{stage:3});
  await prime("wizard_commit");
  await waitFor(s=>s.wizard===null && !!s.intent_id);
  await prime("run");
  const completed=await waitFor(s=>!s.run_in_progress && s.prediction!==null);
  check(completed.prediction.compatibility.status==="supported","Supported prediction required");
  check(completed.prediction.edited_sequence===edited,"RTT-derived sequence mismatch");
  await prime("evidence");
  const evidence=await prime("get_state");
  check(evidence.verification.includes("Bundle integrity verified"),"Bundle verification failed");
  await capture("supported-evidence");
  const original = await fs.readFile(path.join(completed.bundle,"SHA256SUMS.txt"));
  await prime("wizard_open",{mode:"edit"}); await waitFor(s=>s.wizard!==null);
  await prime("wizard_fill",{inputs:{rtt:"AAAAAAAAAAAAA"}});
  check((await prime("get_state")).wizard.feedback.includes("incompatible"),"Mismatch feedback missing");
  await prime("wizard_navigate",{stage:3}); await prime("wizard_commit");
  await waitFor(s=>s.wizard===null && s.previous_design);
  await prime("run");
  const mismatch=await waitFor(s=>!s.run_in_progress && !s.previous_design && s.run_id>completed.run_id);
  check(mismatch.prediction.compatibility.status==="incompatible","Mismatch accepted as compatible");
  check(mismatch.prediction.edited_sequence===null,"Mismatch produced fabricated sequence");
  await prime("evidence"); await capture("incompatible-evidence");
  check(original.equals(await fs.readFile(path.join(completed.bundle,"SHA256SUMS.txt"))),"Previous bundle changed");
  await fs.writeFile(path.join(outdir,"receipt.json"),JSON.stringify({schema:"helixmcp.prime_windows_qualification/v1",status:"passed",in_silico:true,
    reference_sha256:createHash("sha256").update(target).digest("hex"),edited_sha256:createHash("sha256").update(edited).digest("hex"),
    completed,mismatch,calls},null,2));
  console.log(JSON.stringify({status:"passed",calls:calls.length,bundle:completed.bundle,mismatch_bundle:mismatch.bundle}));
} finally { await client.close(); }
