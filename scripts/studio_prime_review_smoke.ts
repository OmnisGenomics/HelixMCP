/** Native Prime Results Review revise-and-compare proof through a stdio MCP client. */
import { promises as fs } from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { parseArgs } from "node:util";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { CallToolResultSchema } from "@modelcontextprotocol/sdk/types.js";

const { values } = parseArgs({ options: { "bridge-file": {type:"string"}, outdir:{type:"string"}, "reuse-session":{type:"boolean",default:false}, "history-only":{type:"boolean",default:false} } });
if (!values["bridge-file"] || !values.outdir) throw new Error("--bridge-file and --outdir required");
const outdir = path.resolve(values.outdir);
await fs.mkdir(outdir, { recursive: false });
const env = Object.fromEntries(Object.entries(process.env).filter((p): p is [string,string] => typeof p[1] === "string"));
Object.assign(env, { DATABASE_URL:"", AUTO_SCHEMA:"true", OBJECT_STORE_DIR:path.join(outdir,"objects"), RUNS_DIR:path.join(outdir,"runs") });
const client = new Client({name:"helix-prime-synthetic-qualification",version:"1.0.0"});
const transport = new StdioClientTransport({command:process.execPath,args:[path.resolve("dist/index.js")],cwd:process.cwd(),env,stderr:"pipe"});
const gatewayLog: string[] = [];
transport.stderr?.on("data", chunk=>gatewayLog.push(String(chunk)));
const calls: unknown[] = [];
const check = (condition: unknown, message: string) => { if (!condition) throw new Error(message); };
const call = async (name: string, args: Record<string, unknown>) => {
  const result = await client.request({method:"tools/call",params:{name,arguments:args}},CallToolResultSchema,{timeout:45000});
  check(!result.isError, JSON.stringify(result.content));
  calls.push({name,args,result:result.structuredContent});
  return result.structuredContent as any;
};
const review = async (action: string, params: Record<string,unknown> = {}) =>
  (await call("studio_prime_results_review",{bridge_file:values["bridge-file"],action,...params})).prime_review;
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
  if (values["history-only"]) {
    const before=await prime("get_state");
    const opened=await review("open");
    check(opened.status==="ready" && opened.run_ids.length===3,"Project did not restore its three registered Prime runs");
    const ids=opened.run_ids;
    const restored=await review("select",{run_id:ids[ids.length-2],compare_run_id:ids[ids.length-1],full_sequence:true});
    check(restored.comparison.sequence_comparable && JSON.stringify(restored.comparison.different_offsets)==="[63]","Reopened project comparison failed");
    const after=await prime("get_state");
    check(after.run_id===before.run_id && after.bundle===before.bundle,"History review replaced the current draft");
    await capture("reopened-project-comparison");
    await fs.writeFile(path.join(outdir,"receipt.json"),JSON.stringify({schema:"helixmcp.prime_review_reopen_qualification/v1",status:"passed",opened,restored,before,after,calls},null,2));
    console.log(JSON.stringify({status:"passed",calls:calls.length,restored_runs:ids.length}));
  } else {
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
  const firstReview = await review("open");
  check(firstReview.status === "ready", "Initial result view failed");
  const firstId = firstReview.selected_run_id;
  const originalFiles: Record<string,string> = {};
  for (const name of await fs.readdir(completed.bundle)) {
    const p=path.join(completed.bundle,name);
    if ((await fs.stat(p)).isFile()) originalFiles[name]=createHash("sha256").update(await fs.readFile(p)).digest("hex");
  }
  await capture("first-results-review");
  await prime("wizard_open",{mode:"edit"}); await waitFor(s=>s.wizard!==null);
  const revisedSequence=target.slice(0,63)+"G"+target.slice(64);
  await prime("wizard_fill",{inputs:{alternative:"G",rtt:rc(revisedSequence.slice(57,70))}});
  await prime("wizard_navigate",{stage:3}); await prime("wizard_commit");
  await waitFor(s=>s.wizard===null && s.previous_design);
  const pending=await review("open");
  check(pending.previous_design && pending.selected_run_id===firstId,"Pending revision lost previous evidence context");
  await capture("pending-revision");
  await prime("run");
  const revised=await waitFor(s=>!s.run_in_progress && !s.previous_design && s.run_id>completed.run_id);
  check(revised.prediction.edited_sequence===revisedSequence,"Revised sequence mismatch");
  await review("open");
  const revisedId=path.basename(revised.bundle);
  const comparison=await review("select",{run_id:revisedId,compare_run_id:firstId});
  check(comparison.status==="ready" && comparison.comparison.sequence_comparable,"Compatible revisions did not compare");
  check(JSON.stringify(comparison.comparison.different_offsets)==="[63]","Wrong sequence differences");
  check(comparison.alignment.includes("Previous") && comparison.alignment.includes("Selected"),"Missing alignment tracks");
  await capture("revised-comparison");
  await review("select",{run_id:revisedId,compare_run_id:firstId,scroll_y:100000});
  const scrolled=await review("get_state");
  check(scrolled.scroll_y===scrolled.scroll_max,"Evidence footer not reachable");
  await capture("comparison-categories-and-integrity");
  await review("select",{run_id:revisedId,compare_run_id:firstId,scroll_y:0});
  const full=await review("select",{run_id:revisedId,compare_run_id:firstId,full_sequence:true});
  check(full.alignment.includes("Target [0:60)"),"Full sequence toggle failed");
  await capture("full-sequence-comparison");
  await prime("wizard_open",{mode:"edit"}); await waitFor(s=>s.wizard!==null);
  await prime("wizard_fill",{inputs:{rtt:"AAAAAAAAAAAAA"}});
  await prime("wizard_navigate",{stage:3}); await prime("wizard_commit");
  await waitFor(s=>s.wizard===null && s.previous_design);
  await prime("run");
  const mismatch=await waitFor(s=>!s.run_in_progress && !s.previous_design && s.run_id>revised.run_id);
  await review("open");
  const unresolved=await review("select",{run_id:path.basename(mismatch.bundle),compare_run_id:revisedId,full_sequence:false});
  check(unresolved.status==="ready" && unresolved.selected_compatibility.status==="incompatible","Missing incompatible review state");
  check(!unresolved.comparison.sequence_comparable && unresolved.comparison.different_offsets===null,"Unresolved sequence fabricated a difference");
  check(unresolved.alignment.includes("unresolved"),"Unresolved sequence not identified");
  await capture("incompatible-comparison");
  await prime("evidence");
  check((await prime("get_state")).verification.includes("Bundle integrity verified"),"Mismatch bundle verification failed");
  const historical=await review("open");
  await review("select",{run_id:firstId,compare_run_id:revisedId});
  check((await review("get_state")).comparison.sequence_comparable,"Historical comparison failed");
  for(const [name,hash] of Object.entries(originalFiles)) check(createHash("sha256").update(await fs.readFile(path.join(completed.bundle,name))).digest("hex")===hash,"Historical evidence changed: "+name);
  await fs.writeFile(path.join(outdir,"receipt.json"),JSON.stringify({schema:"helixmcp.prime_results_review_qualification/v1",status:"passed",in_silico:true,
    completed,revised,mismatch,pending,comparison,unresolved,historical,original_files_unchanged:Object.keys(originalFiles).length,calls},null,2));
  console.log(JSON.stringify({status:"passed",calls:calls.length,firstId,revisedId,mismatch:unresolved.selected_run_id}));
  }
} finally {
  await client.close();
  await fs.writeFile(path.join(outdir,"gateway.log"),gatewayLog.join(""));
}
