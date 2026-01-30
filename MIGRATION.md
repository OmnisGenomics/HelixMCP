# Migration Guide

## v0.9.x → v1.0.0

No migration is required from **v0.9.x** to **v1.0.0**.

This release is a stability declaration: it formalizes and commits to the contracts already present in v0.9.x:

- Toolpack ABI v1 (see `TOOLPACK_ABI_V1.md`)
- Events v1 (see `EVENTS_V1.md`)
- Versioning and stability rules (see `VERSIONING.md`)

### What remains stable in v1.x

In the v1.x line, HelixMCP is additive-only with respect to:

- Toolpack ABI v1 surface and semantics
- Run identity derivation and canonicalization rules
- Artifact model semantics for existing types/fields
- Replay semantics for successful runs (and queued Slurm checkpoints where applicable)
- Events v1 envelope and existing kind meanings

Breaking changes require a major version bump (v2.0.0) or a new, versioned contract/ABI.

