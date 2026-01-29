import type { ToolDefinition } from "../types.js";
import { seqkitStatsTool } from "./seqkitStats.js";
import { samtoolsFlagstatTool } from "./samtoolsFlagstat.js";
import { samtoolsFlagstatSlurmTool } from "./samtoolsFlagstatSlurm.js";

export const builtinToolDefinitions: Array<ToolDefinition<any, any>> = [
  seqkitStatsTool,
  samtoolsFlagstatTool,
  samtoolsFlagstatSlurmTool
];
