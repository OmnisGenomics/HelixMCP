# Security Policy

HelixMCP is an in silico execution and provenance system. It is designed to make tool execution deterministic, policy-gated, and auditable. It is **not** a sandbox, and it does not claim to prevent all possible compromise if an operator configures unsafe policies.

## Reporting a vulnerability

Please report security issues via **GitHub Security Advisories / private vulnerability reporting** for this repository.

- Do not open public GitHub issues for vulnerabilities.
- We aim to acknowledge reports within **7 days**.

## Threat model (what we try to protect)

HelixMCP aims to reduce risk and improve auditability in compute and data handling:

- Policy-gated execution boundaries (allowlists + quotas enforced before work runs).
- Artifact integrity tracking via checksums and provenance linking.
- Deterministic run identity and replay for audit and reproducibility.
- Execution isolation defaults for supported backends (e.g. Docker network none, digest-pinned images; Slurm apptainer only, network none).

## Out of scope (explicit non-goals)

HelixMCP does **not** claim to provide:

- Strong isolation against malicious tools, containers, or users with access to the same host/cluster.
- Protection against a compromised object store, database, host OS, or scheduler.
- Protection against unsafe operator configuration (e.g. overly-permissive allowlists).
- Confidentiality guarantees for data stored in the configured object store or database.
- Any real-world biological or clinical guidance. This project is in silico only.

## Security-relevant invariants

The following are treated as security-relevant design constraints:

- **Artifact-first IO**: tools accept/return artifact identifiers; filesystem paths are only permitted at controlled import boundaries and must be policy-gated.
- **Fail-closed policy checks**: allowlists, quotas, and egress rules must be enforced at the execution boundary.
- **Deterministic identity and replay**: `run_id` is derived from `(toolName, contractVersion, policyHash, canonicalParamsHash)`; canonical params are forced-canonical before hashing and storage.
- **No hidden IO by toolpacks**: planned inputs must match linked inputs; output roles/types/labels must match declared outputs.

