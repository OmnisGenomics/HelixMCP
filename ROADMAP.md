# HelixMCP Roadmap (conservative)

HelixMCP is an execution + provenance gateway. The goal is to keep execution **boringly correct**: deterministic, inspectable, and policy-gated.

## v0.3 (next): one more real tool

Add exactly one more Docker-backed tool, implemented the same way as `seqkit_stats`:

- pinned image digest (policy allowlist)
- network default `none`
- canonical params include input checksums and backend spec
- outputs captured as artifacts + parsed metrics
- replay test asserts deterministic behavior and `param_sets` dedupe

Candidate tool (pick one, do it well):

- `samtools_flagstat` (BAM → metrics + report artifact), or
- `fastqc` (FASTQ → HTML + metrics + report artifacts)

## v0.4: Slurm runner (policy-gated)

Add a Slurm backend behind the same execution boundary:

- submit jobs via a constrained adapter (no freeform commands)
- isolate per-run workspaces on shared storage
- collect logs and outputs back into artifacts
- enforce quotas and queue policies at submission time

## v0.5: Toolpacks as code modules

Keep tools as explicit TypeScript modules (not YAML magic) until patterns are proven:

- a `ToolDefinition` interface with strong typing
- a registry loader to keep `gatewayServer.ts` small
- deterministic contracts + provenance fields per tool

## Non-goals (explicit)

- No SaaS requirement
- No opaque execution or hidden side effects
- No default network access for tools
- No ad-hoc shell command execution via tool parameters
- No schema drift between “published contracts” and runtime validation

