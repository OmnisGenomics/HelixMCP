import type { ToolDefinition } from "../types.js";
import { seqkitStatsTool } from "./seqkitStats.js";
import { samtoolsFlagstatTool } from "./samtoolsFlagstat.js";
import { samtoolsFlagstatSlurmTool } from "./samtoolsFlagstatSlurm.js";
import { fastqcTool } from "./fastqc.js";
import { multiqcTool } from "./multiqc.js";
import { qcBundleFastqTool } from "./qcBundleFastq.js";

export const builtinToolDefinitions: Array<ToolDefinition<any, any>> = [
  seqkitStatsTool,
  samtoolsFlagstatTool,
  samtoolsFlagstatSlurmTool,
  fastqcTool,
  multiqcTool,
  qcBundleFastqTool
];
