Read PROMPTS/IMPLEMENTATION_PROMPT.md carefully.
Also read SPEC.md for additional design context.
Use PROMPTS/IMPLEMENTATION_PROMPT.md as the primary implementation source of truth for code generation scope, API shape, and wire-contract behavior.

Also read REQUIREMENTS_CHECKLIST.md and the current codebase carefully.

Build on the already-corrected Phase 1 public API under agentcore/.
Treat the current public types and method signatures as the baseline unless a minimal compatibility correction is absolutely required.

Now implement only:
1. internal/contract
2. transport-level validation for the public envelopes/models
3. centralized JSON encode/decode helpers for the standard wire envelopes

Requirements to focus on:
- NATS-LIB-08
- NATS-LIB-09
- NATS-LIB-10
- NATS-LIB-14

Goal:
Add the shared contract layer that enforces the wire format and shared envelope sanity checks, while keeping validation strictly transport-level and workload-agnostic.

Rules:
- Keep validation limited to shared transport/wire sanity checks only
- Do not add cloud/business-policy validation
- Do not add workload-specific validation
- Do not implement subjects/routing yet
- Do not implement session/NATS connection/JetStream/KV yet
- Do not implement submission flows, handlers, registry, or reconnect logic yet
- Do not add tests in this phase
- Keep payloads as json.RawMessage where the public API defines them that way
- Keep codec logic centralized, small, and reusable
- Keep code modular, testable, and race-safe
- Do not invent extra domain-specific fields not present in the implementation prompt or corrected public API

Validation expectations:
- Validate only required transport-level fields and shared envelope correctness
- Preserve rpc_id, target, uuid/config UUID, action, timestamp, and version semantics from PROMPTS/IMPLEMENTATION_PROMPT.md
- Reject malformed or missing required identifiers where the wire contract requires them
- Do not treat UUID as an ordering field, version sequence, or freshness indicator
- Do not introduce historical config retrieval or revision-ordered behavior
- Do not embed business meaning into validation beyond shared contract sanity

Suggested files:
- internal/contract/codec.go
- internal/contract/validate.go
- internal/contract/helpers.go

Add more small focused files only if clearly needed.

After coding:
- summarize exactly which files were added or changed
- explain how the implementation aligns with PROMPTS/IMPLEMENTATION_PROMPT.md
- map each added/changed file to the relevant requirement IDs from REQUIREMENTS_CHECKLIST.md, especially:
  - NATS-LIB-08
  - NATS-LIB-09
  - NATS-LIB-10
  - NATS-LIB-14
- list any minimal public API adjustments made, if any
- list what is intentionally deferred to later phases

Review checklist for this phase:
- validation is not too smart
- only transport-level checks are implemented
- rpc_id, target, uuid, and action are preserved properly
- codec logic is centralized
- no session/KV/subjects/transport behavior leaked into this phase
- no workload/business logic was added
