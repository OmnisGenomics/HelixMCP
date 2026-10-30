/** Actual MCP/native Studio proof of deterministic comparison export and offline verification. */
import { promises as fs } from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { parseArgs, promisify } from "node:util";
import { execFile } from "node:child_process";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { CallToolResultSchema } from "@modelcontextprotocol/sdk/types.js";

const {values}=parseArgs({options:{"bridge-file":{type:"string"},outdir:{type:"string"},"project-root":{type:"string"},python:{type:"string"}}});
if(!values["bridge-file"] || !values.outdir || !values["project-root"] || !values.python)throw new Error("--bridge-file, --outdir, --project-root, --python required");
const outdir=path.resolve(values.outdir),project=path.resolve(values["project-root"]);
if(project!==path.join(path.dirname(outdir),"project"))throw new Error("Requires an isolated sibling project fixture");
await fs.access(path.join(project,".prime-comparison-fixture"));
await fs.mkdir(outdir,{recursive:false});
const env=Object.fromEntries(Object.entries(process.env).filter((p):p is [string,string]=>typeof p[1]==="string"));
Object.assign(env,{DATABASE_URL:"",AUTO_SCHEMA:"true",OBJECT_STORE_DIR:path.join(outdir,"objects"),RUNS_DIR:path.join(outdir,"runs")});
const client=new Client({name:"helix-prime-comparison-export-proof",version:"1.0.0"});
const transport=new StdioClientTransport({command:process.execPath,args:[path.resolve("dist/index.js")],cwd:process.cwd(),env,stderr:"pipe"});
const gatewayLog:string[]=[],calls:unknown[]=[];
transport.stderr?.on("data",chunk=>gatewayLog.push(String(chunk)));
const check=(ok:unknown,message:string)=>{if(!ok)throw new Error(message);};
const call=async(name:string,args:Record<string,unknown>,expectError=false)=>{
  const response=await client.request({method:"tools/call",params:{name,arguments:args}},CallToolResultSchema,{timeout:90000});
  calls.push({name,args,response});
  check(expectError?response.isError:!response.isError,JSON.stringify(response.content));
  return response.structuredContent as any;
};
const review=async(action:string,params:Record<string,unknown>={})=>(await call("studio_prime_results_review",{bridge_file:values["bridge-file"],action,...params})).prime_review;
const capture=async(label:string)=>{
  await new Promise(resolve=>setTimeout(resolve,250));
  const result=await call("studio_capture_screenshot",{bridge_file:values["bridge-file"],path:path.join(outdir,label+".png"),target:"native"});
  check(result.screenshot.target?.capture_mode==="native_window","Native capture required");
};
const exec=promisify(execFile);
try{
  await client.connect(transport);
  const initial=await call("studio_get_state",{bridge_file:values["bridge-file"]});
  check(path.resolve(initial.studio_state.project_root)===project,"Unexpected live project root");
  const state=await review("open");
  check(state.run_ids.length===3,"Use the three-run synthetic Prime review fixture");
  const [incompatible,revised,first]=state.run_ids;
  await review("select",{run_id:revised,compare_run_id:first});
  const source=path.join(project,"out","helixspec_runs");
  const original:Record<string,string>={};
  for(const id of state.run_ids)for(const name of await fs.readdir(path.join(source,id))){
    const p=path.join(source,id,name);
    if((await fs.stat(p)).isFile())original[path.relative(source,p)]=createHash("sha256").update(await fs.readFile(p)).digest("hex");
  }
  const firstExport=await call("studio_prime_comparison_export",{bridge_file:values["bridge-file"]});
  const repeatExport=await call("studio_prime_comparison_export",{bridge_file:values["bridge-file"]});
  check(firstExport.comparison_checksum_sha256===repeatExport.comparison_checksum_sha256,"Export bytes were nondeterministic");
  const artifact=await call("artifact_get",{artifact_id:firstExport.comparison_artifact_id});
  check(artifact.artifact.type==="ZIP" && artifact.artifact.checksum_sha256===firstExport.comparison_checksum_sha256,"ZIP artifact registration mismatch");
  const registered=await fs.readFile(path.join(outdir,"objects",firstExport.comparison_artifact_id));
  check("sha256:"+createHash("sha256").update(registered).digest("hex")===firstExport.comparison_checksum_sha256,"Stored artifact bytes changed");
  const portable=path.join(outdir,"standalone-comparison.zip");
  await fs.writeFile(portable,registered,{flag:"wx"});
  const verified=JSON.parse((await exec(values.python!,["-m","helix.prime.comparison_export","verify",portable,"--expected-sha256",firstExport.comparison_checksum_sha256.slice(7)],{cwd:outdir,env})).stdout);
  check(verified.status==="verified" && verified.sequence_comparable,"Offline comparison verification failed");
  await review("select",{run_id:revised,compare_run_id:first,scroll_y:100000});
  await capture("comparison-export-complete");
  await review("select",{run_id:incompatible,compare_run_id:revised,scroll_y:0});
  const unresolvedExport=await call("studio_prime_comparison_export",{bridge_file:values["bridge-file"]});
  const unresolvedVerified=JSON.parse((await exec(values.python!,["-m","helix.prime.comparison_export","verify",unresolvedExport.comparison_export.receipt.path],{cwd:outdir,env})).stdout);
  check(!unresolvedVerified.sequence_comparable,"Unresolved export invented sequence differences");
  await review("select",{run_id:revised,compare_run_id:first});
  const changed=path.join(source,first,"results.json"),saved=await fs.readFile(changed);
  try{
    await fs.writeFile(changed,Buffer.concat([saved,Buffer.from(" ")]));
    await call("studio_prime_comparison_export",{bridge_file:values["bridge-file"]},true);
    const invalid=await review("get_state");
    check(invalid.status==="error" && !invalid.alignment,"Changed evidence did not clear the review display");
    const retained=JSON.parse((await exec(values.python!,["-m","helix.prime.comparison_export","verify",portable],{cwd:outdir,env})).stdout);
    check(retained.status==="verified","Previously exported immutable evidence became dependent on changed original inputs");
    await capture("changed-input-rejected");
  }finally{await fs.writeFile(changed,saved);}
  for(const [name,hash] of Object.entries(original))check(createHash("sha256").update(await fs.readFile(path.join(source,name))).digest("hex")===hash,"Input evidence changed: "+name);
  await review("open");await review("select",{run_id:revised,compare_run_id:first});
  await fs.writeFile(path.join(outdir,"receipt.json"),JSON.stringify({schema:"helixmcp.prime_comparison_export_qualification/v1",status:"passed",calls,
    firstExport,repeatExport,unresolvedExport,verified,unresolvedVerified,source_files_unchanged:Object.keys(original).length},null,2));
  console.log(JSON.stringify({status:"passed",calls:calls.length,source_files_unchanged:Object.keys(original).length,sha256:firstExport.comparison_checksum_sha256}));
}finally{await client.close();await fs.writeFile(path.join(outdir,"gateway.log"),gatewayLog.join(""));}
