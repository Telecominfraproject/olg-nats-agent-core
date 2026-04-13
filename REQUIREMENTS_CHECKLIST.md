# NATS Agent Core - Requirements Checklist

Use this checklist while generating and reviewing the library phase by phase.

How to use:
- Mark `[x]` only when you can trace the requirement in:
  - public API
  - implementation
  - tests
- Add file names, commit IDs, or short notes in the **Review Notes** column
- Do not mark a requirement complete just because a generated answer mentioned it; verify it in code
- Keep the wording of each requirement stable so the mapping from requirement -> implementation -> tests stays clear during review

---

## Review status legend

- `[ ]` Not reviewed yet
- `[~]` Partially covered / needs more review
- `[x]` Reviewed and acceptable
- `[!]` Problem found / redesign needed

---

## Functional Requirements

| Status | Requirement ID | Requirement | What this means in simple words | Expected proof in code | Expected proof in tests | Review Notes |
|---|---|---|---|---|---|---|
| [ ] | NATS-LIB-01 | The library shall provide a standard mechanism to establish a client connection to the configured NATS server. | The library must know how to connect to the configured NATS servers in a reusable, library-owned way rather than making each agent build its own connection logic. | `New(...)`, `Start(...)` and/or `Connect(...)`, NATS config usage in `client.go` or session layer | Integration test connects to a real NATS server using configured settings | |
| [ ] | NATS-LIB-02 | The library shall manage the full lifecycle of the NATS client session, including initialization, active use, orderly shutdown, and cleanup of resources. | The library is responsible for startup, normal running state, and clean shutdown. It should not only connect once and leave the rest undefined. | `Start(ctx)`, `Close(ctx)`, drain/cleanup logic, connection/session state management | Tests for start, normal use, shutdown, and cleanup behavior | |
| [ ] | NATS-LIB-03 | The library shall detect loss of connection to the NATS server and attempt reconnection according to a defined retry policy. | If the connection drops, the library should try to recover using configured reconnect behavior instead of forcing the owning agent to rebuild the client. | reconnect options, callbacks, retry policy, config-driven reconnect settings | Integration or targeted session test for disconnect and reconnect behavior | |
| [ ] | NATS-LIB-04 | The library shall restore required subscriptions after reconnection to the NATS server. | After reconnect, the library should bring back the subscriptions that the agent had already registered so message handling can continue automatically. | in-memory subscription registry plus reconnect restoration logic | Test that handlers continue working after reconnect without manual re-registration | |
| [ ] | NATS-LIB-05 | The library shall create and manage the JetStream and Key-Value access context required for durable configuration storage and retrieval. | The library must own the shared JetStream and KV setup needed for durable desired-state behavior. | `jetstream.New(nc)`, KV bind/create logic, stored JetStream and KeyValue handles | Integration test with `nats-server -js` covering JetStream and KV setup | |
| [ ] | NATS-LIB-06 | The library shall expose connection and session health state to the owning agent. | The agent should be able to ask the library whether it is healthy, connected, reconnecting, or carrying important error state. | `Health()` plus a structured health snapshot or health state model | Unit test for health state changes across connect, disconnect, reconnect, and error cases | |
| [ ] | NATS-LIB-07 | The library shall provide default timeout and retry behavior for bus operations, with support for agent-level override when needed. | Common bus operations should not all invent their own timeout and retry behavior. The library should provide sensible defaults, while still allowing an agent to override them when necessary. | timeout/retry options in config or options layer, shared operation wrappers using defaults and overrides | Unit and integration tests showing default behavior and override behavior | |
| [ ] | NATS-LIB-08 | The library shall define and use a standard message envelope for all supported bus messages. | Configure, action, result, and status messages should follow one shared contract so all agents speak the same language on the bus. | public envelope and message types in `models.go` or contract layer | Unit tests for model consistency and required shared fields | |
| [ ] | NATS-LIB-09 | The library shall provide common encoding and decoding functions for command, result, and status envelopes. | The library should handle standard JSON encode/decode behavior in one reusable way instead of scattering serialization rules everywhere. | marshal/unmarshal helpers, JSON tags, common codec helpers | Unit tests for encode/decode success and failure paths | |
| [ ] | NATS-LIB-10 | The library shall preserve and expose the request correlation identifier `rpc_id` in all command and result flows where it is applicable. | `rpc_id` should not be dropped. It must remain available end to end so requests and results can be matched reliably. | `rpc_id` present in models and preserved in publish/receive paths | Tests that `rpc_id` survives round-trip and is available to receive-side handlers | |
| [ ] | NATS-LIB-11 | The library shall preserve the configuration identity field, such as configuration `uuid`, for configure flows where that field is present. | Configure flows need to keep the configuration identity intact so the owning side can know which desired configuration instance is being referred to. | config identity field in configure-related models and configure publish/receive flow | Tests that configure identity is preserved and exposed correctly | |
| [ ] | NATS-LIB-12 | The library shall provide standard helpers to generate and validate subject names for configure, action, result, and status messaging. | Subject naming should be centralized and consistent so agents do not hand-build raw subject strings differently in different places. | subject builders and validators for configure, action, result, and status subjects | Unit tests for correct subject generation and invalid-input rejection | |
| [ ] | NATS-LIB-13 | The library shall perform shared sanity validation on message envelopes before publish or processing. | The library should reject incomplete or malformed standard messages early, before trying to publish them or hand them to business logic. | validation helpers in contract layer for required fields and basic message sanity | Unit tests for invalid, incomplete, or malformed messages | |
| [ ] | NATS-LIB-14 | The library shall support a version field in the standard message contract. | Standard messages should carry a version field so schema evolution and compatibility can be handled more safely over time. | version field present in standard envelope types and preserved by codec/helpers | Unit tests verifying version field presence and encode/decode behavior | |
| [ ] | NATS-LIB-15 | The library shall provide common publish wrappers for configure notifications, action commands, results, and status messages. | The library should provide shared publish paths for the main bus message types so agents do not build raw publish logic on their own. | exported or shared publish helpers covering configure notifications, actions, results, and status | Tests showing common publish wrappers build the right message and subject patterns | |
| [ ] | NATS-LIB-16 | The library shall provide common subscribe wrappers for command, result, and status subjects. | The library should provide reusable subscribe helpers so agents do not duplicate low-level NATS subscription code for each subject family. | exported or shared subscribe helpers for configure/action/result/status-related subjects | Unit or integration tests for subscribe wrapper registration and message delivery | |
| [ ] | NATS-LIB-17 | The library shall provide a standard mechanism for agents to register subscriptions and bind them to callbacks or handlers. | Agents should have one clear way to register interest in messages and attach their handlers, instead of hand-wiring callbacks differently in each place. | handler registration methods, callback binding, registry of subscription intent | Tests for handler registration, callback execution, and registry behavior | |
| [ ] | NATS-LIB-18 | The library shall provide a standard function to store desired configuration in JetStream Key-Value storage. | Desired configuration storage should go through one library-owned API so the storage model stays consistent and reviewable. | `StoreDesiredConfig(...)` or equivalent KV write helper | KV-related unit or integration tests for desired-config storage | |
| [ ] | NATS-LIB-19 | The library shall provide a standard function to retrieve desired configuration from JetStream Key-Value storage. | Agents should be able to load desired configuration through a shared library API instead of directly reading KV in custom ways. | `LoadDesiredConfig(...)` or equivalent KV read helper | KV-related unit or integration tests for desired-config retrieval | |
| [ ] | NATS-LIB-20 | The library shall support publishing a lightweight configure notification after successful desired configuration storage. | Configure should follow a store-then-notify pattern: first store the desired config durably, then publish a lightweight trigger telling the target agent to reload it. | `SubmitConfigure(...)` flow that validates, stores desired config in KV, then publishes a lightweight configure notification | Tests confirming configure is not direct-publish-only and that notify happens after successful store | |
| [ ] | NATS-LIB-21 | The library shall provide a standard helper for result publication. | Agents should have one shared way to publish final outcome messages that preserves the common contract and correlation fields. | `PublishResult(...)` or equivalent shared result publish helper | Tests for standard result publication and subject usage | |
| [ ] | NATS-LIB-22 | The library shall provide a standard helper for status publication. | Agents should have one shared way to publish progress or intermediate status updates using the common contract. | `PublishStatus(...)` or equivalent shared status publish helper | Tests for standard status publication and subject usage | |
| [ ] | NATS-LIB-23 | The library shall support correlation of received result messages using `rpc_id`. | `rpc_id` is not only a field that gets passed through. It must also be available on the receive side so results can be matched back to the original request. | receive-side decode/handler flow preserves `rpc_id` and makes it available for correlation logic | Tests showing incoming results can be matched to original requests using `rpc_id` | |
| [ ] | NATS-LIB-24 | The library shall provide an optional mechanism to watch desired configuration updates in Key-Value storage. | Some agents may want to watch desired-config changes instead of only loading on demand. The library should support that as an optional capability. | `WatchDesiredConfig(...)` or equivalent optional KV watch helper | Unit or integration test for watch callback or change notification behavior | |
| [ ] | NATS-LIB-25 | The library shall provide helper support for startup reconciliation by retrieving the latest desired configuration state. | On startup, restart, or recovery, the library should help the agent reload the current desired state so it can reconcile local state against it. | `StartupReconcile(...)` and/or documented helper flow using desired-config load APIs | Recovery-focused tests covering startup or restart reconciliation path | |
| [ ] | NATS-LIB-26 | The library shall return clear success and failure outcomes for bus operations such as connect, publish, subscribe, store, retrieve, and watch. | Bus operations should fail clearly and predictably. The caller should receive useful outcomes instead of having failures hidden only in logs. | typed errors, named errors, result wrappers, or clear error-return paths in public APIs | Unit tests for failure exposure across main bus operations | |
| [ ] | NATS-LIB-27 | The library shall expose JetStream KV revision metadata for desired configuration entries returned by library operations. | When desired configuration is read or returned by the library, any relevant KV revision metadata should be exposed in a clear way instead of being silently discarded. | desired-config return types or metadata wrappers that expose KV revision details where available | Tests that revision metadata is returned when expected and stays associated with the right desired-config entry | |
| [ ] | NATS-LIB-28 | The library shall provide helpers to retrieve desired configuration revision information so that an owning agent can detect duplicate or previously seen configuration entries. | The owning agent may need revision information to recognize that it has already seen a stored desired-config entry. This should be available through the library without making revision the only sync mechanism. | helper methods or return types that expose revision information needed for duplicate/previously-seen detection | Tests for duplicate-detection or previously-seen revision handling using returned revision information | |
| [ ] | NATS-LIB-29 | The library shall provide logging hooks around major bus operations. | The agent should be able to plug in logging so important bus events and failures are visible without forcing one logging framework. | `Logger` interface and logging integration around connect, reconnect, publish, subscribe, handler, and KV operations | Unit tests or usage examples showing logger injection and hook usage | |
| [ ] | NATS-LIB-30 | The library shall provide metrics hooks around major bus operations. | The agent should be able to plug in metrics so key library behavior can be observed in monitoring and dashboards. | `Metrics` interface and instrumentation hook points around core operations | Unit tests or usage examples showing metrics hook integration | |
| [ ] | NATS-LIB-31 | Configure result and status outcomes shall carry the configuration identity that was attempted or applied, in addition to request correlation data. | For configure flows, `rpc_id` tells which request triggered the outcome, while configuration identity tells which desired configuration instance was actually processed. Both are needed for clear review and traceability. | configure result/status models include both `rpc_id` and config identity field such as `uuid` | Unit or integration tests showing configure outcomes carry both request correlation and configuration identity | |

---

## Non-Requirements / Out of Scope

These are also important during review.  
If you see these implemented inside the shared library, that is a design violation.

| Status | Requirement ID | Non-requirement | What this means in simple words | What should NOT exist in shared library code | Review Notes |
|---|---|---|---|---|---|
| [ ] | NATS-LIB-NR-01 | No workload-specific config translation in library | Shared library should not translate generic config into VyOS-specific, host-specific, or workload-specific commands. | no VyOS CLI translation, no host-specific config conversion, no workload-specific translators | |
| [ ] | NATS-LIB-NR-02 | No reboot/script/trace/rtty execution in library | Shared library should not perform the actual business actions that belong to the consuming agent. | no reboot calls, no script execution, no trace implementation, no rtty execution logic | |
| [ ] | NATS-LIB-NR-03 | No local apply/rollback/state transition logic in library | Shared library should not own workload execution engines, rollback behavior, or business-state transitions. | no rollback engine, no workload state machine, no agent-specific apply logic | |
| [ ] | NATS-LIB-NR-04 | No cloud-side business validation policy in library | Shared library should only perform shared transport and envelope validation, not deep policy checks that belong to higher-level application logic. | no deep cloud policy validation logic, no workload-specific rule engine | |
| [ ] | NATS-LIB-NR-05 | No revision-driven configuration decision contract in library | The library may expose revision metadata where needed, but it should not make revision ordering or revision history the primary configuration-sync contract used by all agents. | no revision-first sync design, no required revision-ordered config API as the main contract, no rollback/history contract built into the shared library | |

---

## Phase-by-Phase Review Plan

### Phase 1 - Bootstrap and Public API
Goal:
- module setup
- public types
- public config
- client skeleton
- error/logger/metrics types

Main requirements to review:
- NATS-LIB-08
- NATS-LIB-09
- NATS-LIB-10
- NATS-LIB-11
- NATS-LIB-13
- NATS-LIB-14
- NATS-LIB-26
- NATS-LIB-29
- NATS-LIB-30
- NATS-LIB-31

Checklist:
- [ ] Standard envelope and message types exist
- [ ] JSON tags are clean and consistent
- [ ] `rpc_id` is present where required
- [ ] configure-related identity field such as `uuid` is preserved
- [ ] version field is present in standard contract types
- [ ] error, logger, and metrics types are present
- [ ] no internal business logic leaked into public models
- [ ] configure result/status models include both request correlation and configuration identity

Commit note:
- `feat(api): bootstrap module and public API types`

---

### Phase 2 - Contract, Codec, and Validation
Goal:
- standard contract
- JSON codec
- shared validation
- request/result correlation support

Main requirements to review:
- NATS-LIB-08
- NATS-LIB-09
- NATS-LIB-10
- NATS-LIB-11
- NATS-LIB-13
- NATS-LIB-14
- NATS-LIB-23
- NATS-LIB-31

Checklist:
- [ ] Shared encode/decode helpers exist
- [ ] Validation checks required fields before publish or processing
- [ ] `rpc_id` is preserved through encode/decode and receive-side handling
- [ ] configure identity field is preserved in configure-related flows
- [ ] version field is carried by standard messages
- [ ] configure outcome models carry both `rpc_id` and configuration identity

Commit note:
- `feat(contract): add standard models, codec, and validation`

---

### Phase 3 - Subjects and Publish Paths
Goal:
- central subject helpers
- common publish wrappers
- standard message routing

Main requirements to review:
- NATS-LIB-12
- NATS-LIB-15
- NATS-LIB-20
- NATS-LIB-21
- NATS-LIB-22

Checklist:
- [ ] Configure, action, result, and status subject helpers exist
- [ ] Subject validation rejects malformed target/action inputs
- [ ] Common publish wrappers exist for major message types
- [ ] Configure follows a store-then-notify publish path
- [ ] Result and status publication are handled through explicit shared helpers
- [ ] Raw subject strings are not scattered through the codebase

Commit note:
- `feat(messaging): add subject helpers and shared publish paths`

---

### Phase 4 - Session, JetStream, KV, and Recovery
Goal:
- NATS session
- JetStream handle
- KV bucket
- timeout/retry defaults
- desired-state recovery support

Main requirements to review:
- NATS-LIB-01
- NATS-LIB-02
- NATS-LIB-03
- NATS-LIB-05
- NATS-LIB-06
- NATS-LIB-07
- NATS-LIB-18
- NATS-LIB-19
- NATS-LIB-24
- NATS-LIB-25
- NATS-LIB-27
- NATS-LIB-28

Checklist:
- [ ] `jetstream.New(nc)` is used
- [ ] KV bind/create logic exists
- [ ] timeout and retry defaults are defined centrally
- [ ] agent-level timeout/retry override is possible
- [ ] desired-config store and load helpers exist
- [ ] optional watch path exists if part of the implementation
- [ ] startup or restart reconciliation path is clear
- [ ] relevant revision metadata or revision info is exposed where expected
- [ ] health state is visible to the owning agent
- [ ] shutdown drains or closes the session cleanly

Commit note:
- `feat(session): add NATS session, JetStream, KV, health, and recovery support`

---

### Phase 5 - Subscribe APIs, Handlers, and Reconnect Restore
Goal:
- subscribe wrappers
- handler registration
- callback binding
- reconnect-safe subscription restore
- receive-side result correlation

Main requirements to review:
- NATS-LIB-04
- NATS-LIB-16
- NATS-LIB-17
- NATS-LIB-23
- NATS-LIB-25

Checklist:
- [ ] Common subscribe wrappers exist
- [ ] handler registration methods exist
- [ ] callback binding is clear and reusable
- [ ] subscription registry exists
- [ ] reconnect restoration uses the saved registry
- [ ] received results expose `rpc_id` clearly for correlation
- [ ] startup/recovery flows and receive-side handlers work together cleanly

Commit note:
- `feat(handlers): add subscribe wrappers, handler registration, and reconnect restoration`

---

### Phase 6 - Errors and Observability
Goal:
- clear failure exposure
- logging hooks
- metrics hooks
- operational visibility

Main requirements to review:
- NATS-LIB-26
- NATS-LIB-29
- NATS-LIB-30

Checklist:
- [ ] public APIs return clear success/failure outcomes
- [ ] failures are not hidden only in logs
- [ ] logger hook is injectable
- [ ] metrics hook is injectable
- [ ] no global observability state is required
- [ ] major bus operations are observable through hooks

Commit note:
- `feat(observe): add clear error outcomes, logger hooks, and metrics hooks`

---

### Phase 7 - Unit Tests
Goal:
- prove core behavior in isolated tests

Checklist:
- [ ] contract and validation tests
- [ ] subject generation and validation tests
- [ ] timeout/retry default and override tests
- [ ] publish/subscribe helper tests
- [ ] `rpc_id` propagation and receive-side correlation tests
- [ ] configure identity propagation tests
- [ ] desired-config store/load/watch tests
- [ ] revision metadata and revision info tests where applicable
- [ ] logger/metrics hook tests

Commit note:
- `test(unit): add unit coverage for core library behavior`

---

### Phase 8 - Integration Tests
Goal:
- prove behavior against real NATS and JetStream

Checklist:
- [ ] real `nats-server -js` test setup
- [ ] connect/start/close tested
- [ ] reconnect behavior tested if practical
- [ ] configure store-then-notify flow tested
- [ ] action publish flow tested
- [ ] result/status publication tested
- [ ] desired-config recovery path tested
- [ ] `rpc_id` result correlation tested
- [ ] configure outcome identity tested

Commit note:
- `test(integration): add real NATS and JetStream integration coverage`

---

### Phase 9 - Examples and README
Goal:
- make library usage easy to understand

Checklist:
- [ ] command-agent example
- [ ] host-agent example
- [ ] vyos-agent example
- [ ] quick-start README
- [ ] examples show shared-library usage rather than business logic internals
- [ ] examples reflect configure store-then-notify behavior clearly
- [ ] examples show how correlation and configuration identity are preserved in outcomes

Commit note:
- `docs(examples): add examples and quickstart README`

---

## Quick File Review Map

| File / Area | What to check |
|---|---|
| `agentcore/config.go` | NATS config, timeout/retry defaults, and override points are clear |
| `agentcore/models.go` | standard envelope types, version field, `rpc_id`, config identity, result/status separation |
| `agentcore/client.go` | client lifecycle, public API surface, connect/close/health behavior |
| `internal/contract` | codec, validation, and receive-side correlation support |
| `internal/subjects` | subject generation and validation |
| `internal/session` | NATS connection, reconnect handling, lifecycle, and health state |
| `internal/kv` | desired-config storage, retrieval, watch, and revision info exposure |
| `internal/transport` | publish wrappers, configure store-then-notify path, action publish path |
| `internal/registry` | handler registration, subscribe intent, and reconnect restoration |
| `internal/observe` | logging and metrics hooks |
| `internal/errors` | clear success/failure outcomes and typed error behavior |
| `tests` | requirement proof through behavior |

---

## Final Review Questions

Before calling the library acceptable, answer these:

- [ ] Can I trace each requirement to API + implementation + tests?
- [ ] Is configure clearly implemented as **store desired config in KV, then publish a lightweight notification**?
- [ ] Are configure, action, result, and status subject helpers centralized and validated?
- [ ] Are standard publish and subscribe helpers present instead of ad hoc raw NATS usage everywhere?
- [ ] Is `rpc_id` preserved and also usable for receive-side result correlation?
- [ ] Is configuration identity preserved for configure flows and configure outcomes?
- [ ] Is the version field present in the standard message contract?
- [ ] Are timeout and retry defaults defined clearly, with agent-level override support?
- [ ] Is desired configuration stored, loaded, and optionally watched through shared library APIs?
- [ ] Is startup or restart reconciliation supported through clear desired-state retrieval helpers?
- [ ] Are revision metadata and revision information exposed where the requirement expects them, without making revision the primary sync contract?
- [ ] Are logging and metrics hooks present for major bus operations?
- [ ] Is business logic kept out of the shared library?
- [ ] Are examples and README understandable for future developers?

---

## Notes

Use this file as a living review document.

Add:
- commit hashes
- file names
- gaps found
- redesign notes
- follow-up questions
