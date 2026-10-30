/** Wizard-only insertion/revise/compare/export proof through actual stdio MCP. */
import {promises as fs} from "node:fs";
import path from "node:path";
import {createHash} from "node:crypto";
import {parseArgs,promisify} from "node:util";
import {execFile} from "node:child_process";
import {Client} from "@modelcontextprotocol/sdk/client/index.js";
import {StdioClientTransport} from "@modelcontextprotocol/sdk/client/stdio.js";
import {CallToolResultSchema} from "@modelcontextprotocol/sdk/types.js";

const {values}=parseArgs({options:{"bridge-file":{type:"string"},outdir:{type:"string"},python:{type:"string"},"history-only":{type:"boolean",default:false}}});
if(!values["bridge-file"]||!values.outdir||!values.python)throw new Error("--bridge-file, --outdir, --python required");
const outdir=path.resolve(values.outdir);
await fs.mkdir(outdir,{recursive:false});
const env=Object.fromEntries(Object.entries(process.env).filter((p):p is [string,string]=>typeof p[1]==="string"));
Object.assign(env,{DATABASE_URL:"",AUTO_SCHEMA:"true",OBJECT_STORE_DIR:path.join(outdir,"objects"),RUNS_DIR:path.join(outdir,"runs")});
const client=new Client({name:"helix-synthetic-insertion-proof",version:"1.0.0"});
const transport=new StdioClientTransport({command:process.execPath,args:[path.resolve("dist/index.js")],cwd:process.cwd(),env,stderr:"pipe"});
const logs:string[]=[],calls:unknown[]=[];
transport.stderr?.on("data",chunk=>logs.push(String(chunk)));
const check=(ok:unknown,message:string)=>{if(!ok)throw new Error(message);};
const call=async(name:string,args:Record<string,unknown>)=>{
  const result=await client.request({method:"tools/call",params:{name,arguments:args}},CallToolResultSchema,{timeout:90000});
  calls.push({name,args,result});check(!result.isError,JSON.stringify(result.content));return result.structuredContent as any;
};
const prime=async(action:string,params:Record<string,unknown>={})=>(await call("studio_prime_workflow",{bridge_file:values["bridge-file"],action,...params})).prime;
const review=async(action:string,params:Record<string,unknown>={})=>(await call("studio_prime_results_review",{bridge_file:values["bridge-file"],action,...params})).prime_review;
const waitFor=async(predicate:(state:any)=>boolean)=>{
  const end=Date.now()+90000;
  while(Date.now()<end){const state=await prime("get_state");if(predicate(state))return state;await new Promise(r=>setTimeout(r,200));}
  throw new Error("Timed out waiting for live Studio state");
};
const capture=async(label:string)=>{
  await new Promise(r=>setTimeout(r,250));
  const result=await call("studio_capture_screenshot",{bridge_file:values["bridge-file"],path:path.join(outdir,label+".png"),target:"native"});
  check(result.screenshot.target?.capture_mode==="native_window","Native capture required");return result.screenshot;
};
const rc=(s:string)=>s.split("").reverse().map(c=>({A:"T",C:"G",G:"C",T:"A"}[c])).join("");
const exec=promisify(execFile);
try{
  await client.connect(transport);
  const initial=await call("studio_get_state",{bridge_file:values["bridge-file"]});
  if(values["history-only"]){
    const before=await prime("get_state"), opened=await review("open");
    check(opened.status==="ready"&&opened.run_ids.length===3,"Reopened project lost its three insertion runs");
    const [,revisedId,firstId]=opened.run_ids;
    const comparison=await review("select",{run_id:revisedId,compare_run_id:firstId,full_sequence:true});
    check(comparison.comparison.sequence_comparable&&JSON.stringify(comparison.comparison.alignment_difference_columns)==="[65]","Reopened comparison failed");
    const screenshot=await capture("reopened-insertion-comparison");
    // Opening a project restores its registered history, not an unsaved session.
    // Review must leave the independently opened draft unchanged.
    const after=await prime("get_state");check(after.bundle===before.bundle&&after.run_id===before.run_id&&after.previous_design===before.previous_design,"History review replaced the current draft");
    const exported=await call("studio_prime_comparison_export",{bridge_file:values["bridge-file"]});
    const offline=JSON.parse((await exec(values.python!,["-m","helix.prime.comparison_export","verify",exported.comparison_export.receipt.path],{cwd:outdir,env})).stdout);
    check(offline.status==="verified","Reopened export failed offline verification");
    await fs.writeFile(path.join(outdir,"receipt.json"),JSON.stringify({schema:"helixmcp.prime_insertion_reopen_qualification/v1",status:"passed",initial,before,after,comparison,screenshot,exported,offline,calls},null,2));
    console.log(JSON.stringify({status:"passed",calls:calls.length,restored_runs:opened.run_ids.length,sha256:exported.comparison_checksum_sha256}));
  }else{
  const target="A".repeat(40)+"GGGCCAGGAGCCTGAAGTCT"+"AGG"+"ACGT".repeat(10);
  const edited=(insertion:string)=>target.slice(0,63)+insertion+target.slice(63);
  const inputs=(insertion:string)=>({edit_kind:"INSERTION",spacer:"GGGCCAGGAGCCTGAAGTCT",pbs:rc(target.slice(52,57)),
    rtt:rc(edited(insertion).slice(57,70+insertion.length)),offset:"63",inserted_sequence:insertion});
  const imported=await call("artifact_import",{project_id:"proj_01ARZ3NDEKTSV4RRFFQ69G5FAV",type_hint:"TEXT",source:{kind:"inline_text",text:">synthetic\n"+target+"\n"}});
  await prime("wizard_open",{mode:"new"});await waitFor(s=>s.wizard!==null);
  await prime("wizard_target",{target_artifact_id:imported.artifact.artifact_id,synthetic:true});
  await prime("wizard_fill",{inputs:inputs("AG")});
  await prime("wizard_navigate",{stage:2});
  check((await prime("get_state")).wizard.feedback.includes("supported"),"Wizard insertion compatibility missing");
  const parameters=await capture("insertion-parameters");
  for(const stage of [1,2,3])await prime("wizard_navigate",{stage});
  check((await prime("get_state")).wizard.inputs.inserted_sequence==="AG","Stage navigation lost insertion");
  await prime("wizard_commit");await waitFor(s=>s.wizard===null&&!!s.intent_id);
  await prime("run");const first=await waitFor(s=>!s.run_in_progress&&s.prediction!==null);
  check(first.prediction.edited_sequence===edited("AG")&&first.prediction.model_id==="prime_rtt_insertion_v1","First insertion sequence mismatch");
  await prime("evidence");check((await prime("get_state")).verification.includes("Bundle integrity verified"),"Initial evidence failed");
  await capture("insertion-evidence");await review("open");
  const firstId=path.basename(first.bundle),hashes:Record<string,string>={};
  for(const name of await fs.readdir(first.bundle)){
    const p=path.join(first.bundle,name);if((await fs.stat(p)).isFile())hashes[name]=createHash("sha256").update(await fs.readFile(p)).digest("hex");
  }
  await prime("wizard_open",{mode:"edit"});const restored=await waitFor(s=>s.wizard!==null);
  check(restored.wizard.inputs.edit_kind==="INSERTION"&&restored.wizard.inputs.inserted_sequence==="AG","Revision did not restore insertion controls");
  await prime("wizard_fill",{inputs:inputs("AGT")});await prime("wizard_navigate",{stage:3});await prime("wizard_commit");
  await waitFor(s=>s.wizard===null&&s.previous_design);
  const pending=await review("open");check(pending.previous_design&&pending.selected_run_id===firstId,"Revision detached old evidence");
  await capture("insertion-pending-revision");
  await prime("run");const revised=await waitFor(s=>!s.run_in_progress&&!s.previous_design&&s.run_id>first.run_id);
  check(revised.prediction.edited_sequence===edited("AGT"),"Revised insertion sequence mismatch");
  await review("open");const revisedId=path.basename(revised.bundle);
  const comparison=await review("select",{run_id:revisedId,compare_run_id:firstId,full_sequence:true});
  check(comparison.status==="ready"&&comparison.schema==="helix.studio.prime_results_review/v2","Insertion review failed");
  check(comparison.comparison.sequence_comparable&&!comparison.comparison.edited_sequences_equal,"Length-changing comparison failed");
  check(JSON.stringify(comparison.comparison.alignment_difference_columns)==="[65]","Unexpected declared alignment differences");
  check(comparison.alignment.includes("---")&&comparison.alignment.includes("AGT"),"Gapped comparison not displayed");
  const comparisonCapture=await capture("insertion-length-comparison");
  const exported=await call("studio_prime_comparison_export",{bridge_file:values["bridge-file"]});
  const repeat=await call("studio_prime_comparison_export",{bridge_file:values["bridge-file"]});
  check(exported.comparison_checksum_sha256===repeat.comparison_checksum_sha256,"Insertion export nondeterministic");
  const bytes=await fs.readFile(path.join(outdir,"objects",exported.comparison_artifact_id));
  const archive=path.join(outdir,"standalone-insertion-comparison.zip");await fs.writeFile(archive,bytes,{flag:"wx"});
  const offline=JSON.parse((await exec(values.python!,["-m","helix.prime.comparison_export","verify",archive,"--expected-sha256",exported.comparison_checksum_sha256.slice(7)],{cwd:outdir,env})).stdout);
  check(offline.status==="verified"&&offline.sequence_comparable,"Offline insertion verification failed");
  await review("select",{run_id:revisedId,compare_run_id:firstId,scroll_y:100000});
  const scrolled=await review("get_state");check(scrolled.scroll_y===scrolled.scroll_max,"Footer inaccessible");
  await capture("insertion-export-and-integrity");
  await prime("wizard_open",{mode:"edit"});await waitFor(s=>s.wizard!==null);
  await prime("wizard_fill",{inputs:{rtt:"AAAAAAAAAAAAAAAA"}});await prime("wizard_navigate",{stage:3});await prime("wizard_commit");
  await waitFor(s=>s.wizard===null&&s.previous_design);await prime("run");
  const unresolved=await waitFor(s=>!s.run_in_progress&&!s.previous_design&&s.run_id>revised.run_id);
  await review("open");const mismatch=await review("select",{run_id:path.basename(unresolved.bundle),compare_run_id:revisedId,scroll_y:0,full_sequence:false});
  check(mismatch.status==="ready"&&mismatch.selected_compatibility.status==="incompatible"&&!mismatch.comparison.sequence_comparable,"Incompatible insertion failed closed");
  check(mismatch.comparison.alignment===null&&mismatch.comparison.alignment_difference_columns===null,"Unresolved result invented differences");
  await capture("insertion-incompatible");await prime("evidence");
  check((await prime("get_state")).verification.includes("Bundle integrity verified"),"Unresolved bundle failed integrity");
  for(const [name,hash] of Object.entries(hashes))check(createHash("sha256").update(await fs.readFile(path.join(first.bundle,name))).digest("hex")===hash,"Historic evidence changed: "+name);
  await review("open");await review("select",{run_id:revisedId,compare_run_id:firstId,scroll_y:0,full_sequence:false});
  await fs.writeFile(path.join(outdir,"receipt.json"),JSON.stringify({schema:"helixmcp.prime_insertion_qualification/v1",status:"passed",in_silico:true,
    initial,parameters,comparisonCapture,first,revised,unresolved,pending,comparison,mismatch,exported,repeat,offline,historical_files_unchanged:Object.keys(hashes).length,calls},null,2));
  console.log(JSON.stringify({status:"passed",calls:calls.length,firstId,revisedId,sha256:exported.comparison_checksum_sha256}));
  }
}finally{
  await client.close();await fs.writeFile(path.join(outdir,"gateway.log"),logs.join(""));
  await fs.writeFile(path.join(outdir,"calls.json"),JSON.stringify(calls,null,2));
}
