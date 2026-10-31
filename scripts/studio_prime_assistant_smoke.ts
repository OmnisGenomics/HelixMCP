/** Target -> generated candidate -> simulation -> verified evidence, native Windows. */
import {promises as fs} from "node:fs";
import path from "node:path";
import {fileURLToPath} from "node:url";
import {spawn,execFile,type ChildProcess} from "node:child_process";
import {createHash} from "node:crypto";
import {parseArgs,promisify,isDeepStrictEqual} from "node:util";
import {Client} from "@modelcontextprotocol/sdk/client/index.js";
import {StdioClientTransport} from "@modelcontextprotocol/sdk/client/stdio.js";
import {CallToolResultSchema} from "@modelcontextprotocol/sdk/types.js";

const {values}=parseArgs({options:{"helix-root":{type:"string"},python:{type:"string"},outdir:{type:"string"}}});
if(!values["helix-root"]||!values.python||!values.outdir)throw new Error("--helix-root, --python, --outdir required");
if(process.platform!=="win32")throw new Error("A native Windows display is required");
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),".."),helix=path.resolve(values["helix-root"]);
const outdir=path.resolve(values.outdir),project=path.join(outdir,"project"),bridgeFile=path.join(outdir,"bridge.json");
await fs.mkdir(outdir,{recursive:false});
const env=Object.fromEntries(Object.entries(process.env).filter((p):p is [string,string]=>typeof p[1]==="string"));
Object.assign(env,{PYTHONHASHSEED:"0",PYTHONUTF8:"1",OMP_NUM_THREADS:"1",OPENBLAS_NUM_THREADS:"1",MKL_NUM_THREADS:"1",
  DATABASE_URL:"",AUTO_SCHEMA:"true",OBJECT_STORE_DIR:path.join(outdir,"objects"),RUNS_DIR:path.join(outdir,"runs"),
  GATEWAY_POLICY_PATH:path.join(root,"policies/default.policy.yaml")});
const exec=promisify(execFile),sleep=(ms:number)=>new Promise(r=>setTimeout(r,ms));
const check=(ok:unknown,message:string)=>{if(!ok)throw new Error(message);};
const digest=(bytes:Buffer)=>createHash("sha256").update(bytes).digest("hex");
const client=new Client({name:"helix-prime-assistant-proof",version:"1.0.0"});
const transport=new StdioClientTransport({command:process.execPath,args:[path.join(root,"dist/index.js")],cwd:root,env,stderr:"pipe"});
const calls:unknown[]=[],logs:string[]=[],receipt:Record<string,unknown>={schema:"helixmcp.prime_assistant_qualification/v1",status:"failed",in_silico_only:true};
transport.stderr?.on("data",chunk=>logs.push(String(chunk)));
const call=async(name:string,args:Record<string,unknown>)=>{
  const result=await client.request({method:"tools/call",params:{name,arguments:args}},CallToolResultSchema,{timeout:180000});
  calls.push({name,args,result});check(!result.isError,`${name}: ${JSON.stringify(result.content)}`);
  return result.structuredContent as any;
};
const studioCall=(name:string,args:Record<string,unknown>={})=>call(name,{bridge_file:bridgeFile,...args});
const prime=async(action:string,params:Record<string,unknown>={})=>(await studioCall("studio_prime_workflow",{action,...params})).prime;
const review=async(action:string,params:Record<string,unknown>={})=>(await studioCall("studio_prime_results_review",{action,...params})).prime_review;
const assistant=async(action:string,params:Record<string,unknown>={})=>(await studioCall("studio_prime_design_assistant",{action,...params})).design_assistant;
const generated=async()=>{
  const end=Date.now()+90000;
  while(Date.now()<end){const state=await assistant("get_state");if(state.status!=="running")return state;await sleep(100);}
  throw new Error("Assistant search timed out");
};
const waitFor=async(predicate:(state:any)=>boolean)=>{
  const end=Date.now()+90000;
  while(Date.now()<end){const state=await prime("get_state");if(predicate(state))return state;await sleep(150);}
  throw new Error("Timed out waiting for Studio");
};
const capture=async(label:string)=>{
  await sleep(250);
  const result=await studioCall("studio_capture_screenshot",{path:path.join(outdir,label+".png"),target:"native"});
  check(result.screenshot.target?.capture_mode==="native_window","Native capture required");
  return result.screenshot;
};
const benchmark=async()=>{
  const result=await studioCall("studio_prime_validation_benchmark");
  check(result.validation.receipt.status==="PASS"&&result.validation.receipt.case_count===236&&result.validation.session_unchanged===true,"Benchmark failed or changed the session");
  return result;
};
const history: {bundle:string;hashes:Record<string,string>}[]=[];
const historyUnchanged=async()=>{
  for(const item of history)for(const [name,hash] of Object.entries(item.hashes))
    check(digest(await fs.readFile(path.join(item.bundle,name)))===hash,"Historical evidence changed: "+name);
};
let studio:ChildProcess|undefined,bridge:any,startupError:Error|undefined;
const log=await fs.open(path.join(outdir,"studio.log"),"wx");
try{
  studio=spawn(path.join(path.dirname(values.python),"omnis-helix.exe"),[project,"--windowed"],{cwd:helix,windowsHide:true,stdio:["ignore",log.fd,log.fd],
    env:{...env,QT_QPA_PLATFORM:"windows",QT_SCALE_FACTOR:"1.5",HELIX_DISABLE_USER_CONFIG:"1",OMNIS_HELIX_SKIP_WELCOME:"1",
      OMNIS_HELIX_SHOW_CRASH_DIALOG:"0",OMNIS_HELIX_ENABLE_MCP_BRIDGE:"1",OMNIS_HELIX_MCP_BRIDGE_FILE:bridgeFile,
      OMNIS_HELIX_SETTINGS_DIR:path.join(outdir,"settings"),OMNIS_HELIX_RECOVERY_DIR:path.join(outdir,"recovery")}});
  studio.once("error",error=>{startupError=error;});
  const deadline=Date.now()+90000;
  while(Date.now()<deadline){
    if(startupError)throw startupError;
    if(studio.exitCode!==null)throw new Error("Studio exited before bridge startup; see studio.log");
    try{bridge=JSON.parse(await fs.readFile(bridgeFile,"utf8"));break;}catch{await sleep(250);}
  }
  check(bridge,"Studio bridge did not appear");
  await client.connect(transport);
  receipt.initial=await studioCall("studio_get_state");
  check(path.resolve((receipt.initial as any).studio_state.project_root)===project,"Wrong Studio project");
  // Read pinned expected values, never calculate them with production predictors.
  const suite=JSON.parse((await exec(values.python,["-c","from helix.prime.validation_suite import load_fixture_files; print(load_fixture_files()['cases.json'].decode())"],{env,windowsHide:true,maxBuffer:2*1024*1024})).stdout);
  const results:unknown[]=[];
  for(const kind of ["substitution","insertion","deletion","replacement"]){
    const c=suite.cases.find((c:any)=>c.id===`wizard-${kind}-1`);
    check(c?.expected.status==="supported","Frozen wizard fixture missing");
    const inputs:Record<string,string>={edit_kind:kind.toUpperCase(),spacer:"",pbs:"",rtt:"",offset:String(c.edit.offset)};
    if(kind==="substitution")Object.assign(inputs,{reference:c.edit.reference,alternative:c.edit.alternative});
    if(kind==="insertion")inputs.inserted_sequence=c.edit.sequence;
    if(kind==="deletion")inputs.deletion_length=String(c.edit.length);
    if(kind==="replacement")Object.assign(inputs,{replacement_length:String(c.edit.length),replacement_sequence:c.edit.sequence});
    const before=await prime("get_state");
    await prime("wizard_open",{mode:history.length?"edit":"new"});await waitFor(s=>s.wizard!==null);
    if(!history.length){
      const imported=await call("artifact_import",{project_id:"proj_01ARZ3NDEKTSV4RRFFQ69G5FAV",type_hint:"TEXT",source:{kind:"inline_text",text:">synthetic\n"+c.reference+"\n"}});
      await prime("wizard_target",{target_artifact_id:imported.artifact.artifact_id,synthetic:true});
    }
    await prime("wizard_fill",{inputs});await prime("wizard_navigate",{stage:2});
    // Missing context must explain an empty search without inventing extensions.
    await assistant("generate",{pbs_lengths:[100],rtt_lengths:[16]});const empty=await generated();
    check(empty.status==="empty"&&empty.report.candidate_count===0&&Object.keys(empty.report.rejection_counts).length>0,"Missing context not explained");
    check(!(await prime("get_state")).wizard.inputs.pbs,"Empty search wrote design sequences");
    if(!history.length)await capture("assistant-empty-search");
    await assistant("generate",{pbs_lengths:[5,6],rtt_lengths:[16,17]});const search=await generated();
    check(search.status==="ready"&&search.selection===null,"Generation failed or auto-selected a candidate");
    check(!(await prime("get_state")).wizard.inputs.rtt,"Generation wrote RTT before explicit selection");
    await assistant("generate",{pbs_lengths:[6,5],rtt_lengths:[17,16]});const repeat=await generated();
    check(JSON.stringify(search.report)===JSON.stringify(repeat.report),"Equivalent search is nondeterministic");
    const candidate=search.report.candidates.find((x:any)=>x.spacer===c.wizard.spacer&&x.pbs.length===5&&x.rtt.length===16);
    check(candidate&&candidate.pbs===c.peg.pbs&&candidate.rtt===c.peg.rtt,"Generated extensions differ from frozen independent fixture");
    const selected=await assistant("select",{candidate_id:candidate.candidate_id});
    check(selected.selection?.candidate.candidate_id===candidate.candidate_id,"Explicit selection did not populate wizard");
    check((await prime("get_state")).wizard.feedback.includes("supported"),"Wizard compatibility failed: "+kind);
    await capture(kind+"-assistant");
    for(const stage of [1,2])await prime("wizard_navigate",{stage});
    check((await assistant("get_state")).selection?.candidate.candidate_id===candidate.candidate_id,"Navigation lost selection provenance");
    await prime("wizard_navigate",{stage:3});await prime("wizard_commit");await waitFor(s=>s.wizard===null&&!!s.intent_id);
    if(history.length)check((await prime("get_state")).previous_design,"Revision lost previous-design state");
    await prime("run");const state=await waitFor(s=>!s.run_in_progress&&s.run_id>before.run_id&&!!s.prediction&&!s.previous_design);
    check(state.prediction.edited_sequence===c.expected.edited_sequence&&state.prediction.reference_sequence===c.reference,"Wizard output differs from frozen expectation: "+kind);
    const manifest=JSON.parse(await fs.readFile(path.join(state.bundle,"manifest.json"),"utf8"));
    check(isDeepStrictEqual(manifest.policy.constraints.prime_design_assistant,selected.selection),"Execution manifest lost selected candidate provenance");
    await prime("evidence");check((await prime("get_state")).verification.includes("Bundle integrity verified"),"Run bundle failed verification: "+kind);
    const reviewed=await review("open");check(reviewed.status==="ready","Results review unavailable: "+kind);
    const screenshot=await capture(kind+"-results");
    const hashes:Record<string,string>={};
    for(const name of await fs.readdir(state.bundle)){
      const file=path.join(state.bundle,name);if((await fs.stat(file)).isFile())hashes[name]=digest(await fs.readFile(file));
    }
    history.push({bundle:state.bundle,hashes});await historyUnchanged();results.push({case_id:c.id,state,reviewed,screenshot,search,selected});
  }
  // Revise to another generated candidate for the same edit, preserving old evidence.
  const beforeRevision=await prime("get_state");
  await prime("wizard_open",{mode:"edit"});await waitFor(s=>s.wizard!==null);
  check((await assistant("get_state")).selection!==null,"Reopened wizard lost candidate provenance");
  await prime("wizard_navigate",{stage:2});
  await assistant("generate",{pbs_lengths:[5],rtt_lengths:[16,17]});const alternatives=await generated();
  const alternate=alternatives.report.candidates.find((x:any)=>x.spacer===suite.cases.find((c:any)=>c.id==="wizard-replacement-1").wizard.spacer&&x.rtt.length===17);
  check(alternate,"Alternate generated candidate missing");
  const alternateSelected=await assistant("select",{candidate_id:alternate.candidate_id});
  await capture("assistant-revised-candidate");
  await prime("wizard_navigate",{stage:3});await prime("wizard_commit");await waitFor(s=>s.wizard===null&&s.previous_design);
  const previous=await review("open");check(previous.previous_design,"Revision did not preserve previous-design evidence");
  await prime("run");const revised=await waitFor(s=>!s.run_in_progress&&s.run_id>beforeRevision.run_id&&!s.previous_design);
  check(revised.prediction.edited_sequence===suite.cases.find((c:any)=>c.id==="wizard-replacement-1").expected.edited_sequence,"Alternate candidate changed the declared edited sequence");
  const revisedManifest=JSON.parse(await fs.readFile(path.join(revised.bundle,"manifest.json"),"utf8"));
  check(isDeepStrictEqual(revisedManifest.policy.constraints.prime_design_assistant,alternateSelected.selection),"Revised candidate provenance missing");
  await prime("evidence");check((await prime("get_state")).verification.includes("Bundle integrity verified"),"Revised candidate bundle failed verification");
  const revisedHashes:Record<string,string>={};
  for(const name of await fs.readdir(revised.bundle)){
    const file=path.join(revised.bundle,name);if((await fs.stat(file)).isFile())revisedHashes[name]=digest(await fs.readFile(file));
  }
  history.push({bundle:revised.bundle,hashes:revisedHashes});await historyUnchanged();
  await review("open");
  const compared=await review("select",{run_id:path.basename(revised.bundle),compare_run_id:path.basename(history[3]!.bundle),full_sequence:true,scroll_y:100000});
  check(compared.comparison.sequence_comparable&&compared.comparison.edited_sequences_equal&&compared.comparison.alignment_difference_columns.length===0,"Equivalent candidate sequence comparison failed");
  check(compared.scroll_y===compared.scroll_max,"Results footer inaccessible");
  await capture("assistant-comparison-footer");
  const exported=await studioCall("studio_prime_comparison_export"),repeatExport=await studioCall("studio_prime_comparison_export");
  check(exported.comparison_checksum_sha256===repeatExport.comparison_checksum_sha256,"Comparison archive nondeterministic");
  const comparisonBytes=await fs.readFile(path.join(outdir,"objects",exported.comparison_artifact_id));
  const archive=path.join(outdir,"standalone-assistant-comparison.zip");await fs.writeFile(archive,comparisonBytes,{flag:"wx"});
  const offline=JSON.parse((await exec(values.python,["-m","helix.prime.comparison_export","verify",archive,"--expected-sha256",digest(comparisonBytes)],{cwd:outdir,env,windowsHide:true})).stdout);
  check(offline.status==="verified","Standalone comparison failed verification");
  // Reopening edit restores the selected candidate; a manual override clears it.
  await prime("wizard_open",{mode:"edit"});await waitFor(s=>s.wizard!==null);
  const restored=await assistant("get_state");check(restored.selection!==null,"Reopened wizard lost its selected candidate");
  const beforeManual=await prime("get_state");
  await prime("wizard_fill",{inputs:{rtt:"AAAAAAAAAAAAAAAA"}});
  check((await assistant("get_state")).selection===null,"Manual override retained candidate provenance");
  await prime("wizard_navigate",{stage:3});await prime("wizard_commit");await waitFor(s=>s.wizard===null&&s.previous_design);
  await prime("run");const manual=await waitFor(s=>!s.run_in_progress&&s.run_id>beforeManual.run_id&&!s.previous_design);
  const manualManifest=JSON.parse(await fs.readFile(path.join(manual.bundle,"manifest.json"),"utf8"));
  check(!manualManifest.policy.constraints?.prime_design_assistant,"Manual execution claimed generated candidate provenance");
  check(manual.prediction.compatibility.status==="incompatible"&&manual.prediction.edited_sequence===null,"Manual mismatch did not remain unresolved");
  await prime("evidence");check((await prime("get_state")).verification.includes("Bundle integrity verified"),"Manual unresolved bundle failed integrity");
  await review("open");await capture("assistant-manual-override");
  const active=await benchmark();
  await historyUnchanged();
  receipt.status="passed";Object.assign(receipt,{active,offline,exported,restored,manual,revised,alternateSelected,compared,wizard_results:results,historical_files_unchanged:history.reduce((n,h)=>n+Object.keys(h.hashes).length,0),
    boundaries:{synthetic_software_only:true,external_review:"not_performed",wizard_strands:["+"],benchmark_strands:["+","-"],native_scale_factor:1.5},calls:calls.length});
  console.log(JSON.stringify({status:"passed",cases:236,wizard_runs:results.length,revised_candidate_runs:1,manual_override_runs:1,calls:calls.length,comparison_sha256:exported.comparison_checksum_sha256,outdir}));
}catch(error){receipt.error=String(error);throw error;}
finally{
  await client.close().catch(()=>{});
  await fs.writeFile(path.join(outdir,"receipt.json"),JSON.stringify(receipt,null,2));
  await fs.writeFile(path.join(outdir,"calls.json"),JSON.stringify(calls,null,2));
  await fs.writeFile(path.join(outdir,"gateway.log"),logs.join(""));
  if(bridge?.pid){
    // Stop only the process whose observed command line contains this unique project.
    await exec("powershell.exe",["-NoProfile","-NonInteractive","-Command",
      "$proofProcessId=[int]$env:HELIX_PROOF_PID; $proofProcess=Get-CimInstance Win32_Process -Filter ('ProcessId='+$proofProcessId); if($proofProcess){if(-not $proofProcess.CommandLine.Contains($env:HELIX_PROOF_PROJECT)){throw 'Studio ownership check failed'}; Stop-Process -Id $proofProcessId}"],
      {windowsHide:true,env:{...env,HELIX_PROOF_PID:String(bridge.pid),HELIX_PROOF_PROJECT:project}});
  }else if(studio?.pid&&studio.exitCode===null){studio.kill();}
  await log.close();
}
