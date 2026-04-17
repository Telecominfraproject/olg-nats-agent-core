# olg-nats-agent-core Specification

## 1. Purpose

`olg-nats-agent-core` is a shared Go library for agents that communicate over a NATS bus.

The library provides common bus-facing behavior for long-running agents, including:
- NATS connection and reconnect handling
- JetStream access
- JetStream KV access for desired configuration
- standard subject naming
- standard message envelopes
- configure and action submission helpers
- result and status publication helpers
- desired configuration storage and retrieval
- handler registration and subscription restoration

The library itself is **not a daemon**.
It is intended to be embedded inside agent processes.

---

## 2. Scope

This library is intended to support agents such as:
- cloud-facing / ucentral-side agent
- host agent
- VyOS agent

The library standardizes transport, message contract, and desired-state handling.
It does **not** implement platform-specific business logic.

---

## 3. Non-goals

The shared library must **not** implement:
- workload-specific config translation
- host reboot/script execution
- trace, rtty, or other action business logic
- cloud-side business validation beyond transport/message sanity checks
- local apply/rollback engines
- historical config management or rollback orchestration

These responsibilities belong to the consuming agents.

---

## 4. Architecture model

The architecture is:

- **library** = shared NATS/JetStream/KV contract and helper layer
- **agent** = long-running process that owns local business logic

Each agent embeds the library and uses it to:
- connect to NATS
- publish and subscribe on standard subjects
- store and load desired configuration
- publish results and status
- recover subscriptions after reconnect

---

## 5. Desired configuration model

### 5.1 Latest-state model

This design uses a **UUID-based latest-state model**.

JetStream KV is used as a **single latest desired-config slot** per target, not as a historical configuration store.

The library stores only the current desired configuration for a target in KV.
Agents always load the **current** desired configuration from KV.

Rollback, revision-ordered history, and historical configuration lookup are out of scope for this design.

### 5.2 Config identity semantics

Each desired configuration carries a **config UUID** assigned by the cloud-facing side.

The config UUID is:
- an opaque identity token
- used for equality comparison only
- not an ordering field
- not a version sequence
- not a freshness indicator

Agents determine whether they are in sync by comparing:
- the UUID of the locally applied/running config
- the UUID stored in the desired config record in KV

If the UUIDs match, the running config is in sync with desired state.
If the UUIDs differ, the running config is not in sync with desired state.

### 5.3 rpc_id semantics

`rpc_id` is used only for request/response correlation.

It identifies which request produced a given result or status message.
It does **not** identify which configuration instance is running or applied.

---

## 6. Configure contract

### 6.1 Configure submit sequence

Configure is modeled as **store desired state, then notify**:

1. caller submits a validated configure request
2. library stores the desired configuration record in KV
3. library publishes a lightweight configure notification on the configure subject
4. target agent receives the notification
5. target agent loads the current desired config from KV
6. target agent performs local apply logic
7. target agent publishes configure result and/or status

### 6.2 Configure notification semantics

The configure notification is a trigger telling the target agent to reload desired state from KV.

The notification is **not** the full desired configuration payload.

Configure notification required fields are:
- `version`
- `rpc_id`
- `target`
- `command_type`
- `uuid`
- `kv_bucket`
- `kv_key`
- `timestamp`

### 6.3 Configure result contract

Configure result/status messages must include:
- `rpc_id` for correlation to the triggering request
- the config UUID that was attempted or applied

This is required so the cloud-facing side can determine:
- which request produced the outcome
- which desired config instance was actually processed

`rpc_id` alone is not sufficient for configure outcomes.

### 6.4 SubmitConfigure failure semantics

`SubmitConfigure(...)` succeeds only when both steps complete successfully:
1. desired config is written to KV
2. configure notification is published

When either step fails, the call returns a submission error and does not return a successful `SubmissionAck`.
`SubmissionAck` is a direct API return value from `SubmitConfigure(...)`, not a bus-delivered message.

The following cases define caller-visible behavior:

- Validation failure
  Missing or invalid required fields.
  Operational result: no KV write, no notification publish, returns submission error.

- KV store failure
  Desired config could not be persisted.
  Operational result: no successful `SubmissionAck`, no notification publish, returns submission error.

- Notification publish failure after KV write success
  Desired config was already written to KV, but notification publish failed.
  Operational result: returns submission error with no successful `SubmissionAck`; desired state may already have changed in KV.

- Context timeout or cancellation
  Context ended before submission completed.
  Operational result: returns submission error. Side effects depend on how far execution progressed before context end:
  if cancellation happened before KV write, no state change; if cancellation happened after KV write, desired state may already be updated even when the call returns error.

Caller guidance:
- Treat `SubmissionAck` as transport acceptance only, not apply success.
- Treat apply success as confirmed only by result/status from the owning target agent.
- For retries after error, assume partial side effects are possible and use config UUID comparison to determine current desired state.

---

## 7. Action contract

Actions are modeled as direct command messages.

Action submit sequence:
1. caller submits a validated action request
2. library publishes the action command on the target action subject
3. target agent receives the action
4. target agent executes local business logic
5. target agent publishes result and/or status

Actions are not stored in KV as desired state.

---

## 8. Result and status contract

The library must provide standard result and status message envelopes.

Result and status messages must preserve shared correlation fields consistently.

For configure flows:
- result/status must carry `rpc_id`
- result/status must carry config UUID

For action flows:
- result/status must carry `rpc_id`
- action-specific fields may be included as needed by the action contract

---

## 9. Subject model

The default subject structure is target-oriented:

- `cmd.configure.<target>`
- `cmd.action.<target>.<action>`
- `result.<target>`
- `status.<target>`
- `health.<target>`

All subject generation must be centralized in the library.
Consuming agents must not construct raw subjects ad hoc throughout the codebase.

The library must validate subject inputs such as target and action before publishing or subscribing.

---

## 10. KV model

### 10.1 Bucket usage

The default KV bucket is used to store desired configuration records.

Default conventions:
- bucket: `cfg_desired`
- key pattern: `desired.<target>`

### 10.2 Contract-level behavior

At the design level, KV is treated as storage for the **current desired config only**.

The application contract does **not** depend on:
- KV revision ordering
- historical revision retrieval
- revision-based config freshness decisions

KV implementation metadata may exist internally, but it is not part of the design contract.

### 10.3 API expectation

The library must expose APIs to:
- store desired config
- load current desired config
- optionally watch desired config changes
- help agents reload current desired state on startup/recovery

The primary load path is **load current desired config**, not load arbitrary historical revisions.

---

## 11. Reconnect and recovery model

The library must support daemon-friendly reconnect behavior.

After reconnect, the library must:
- restore subscriptions
- restore handler registrations
- rebuild required NATS/JetStream/KV handles as needed

The library must also support agent recovery flows in which an agent:
- starts or restarts
- reloads the current desired config from KV
- reconciles local state against desired state

Recovery is based on the **latest desired config** in KV.

---

## 12. Public API expectations

The public API should provide, at minimum, support for:
- connection lifecycle
- health reporting
- configure submission
- action submission
- result/status publication
- desired config store/load/watch
- handler registration for configure, action, result, and status

The desired-config read API should return the decoded current desired-config record, including the config UUID needed for sync decisions.

The design does **not** require an API for loading arbitrary KV revisions.

---

## 13. Validation and error handling

The library must perform:
- transport-level validation
- message envelope sanity checks
- required field validation for standard contracts

The library must **not** implement deep cloud business-policy validation.

Public APIs must return clear, typed errors with structured codes where appropriate.

---

## 14. Health and observability

The library must expose health information in a safe, read-only form.

The library should support:
- logger hooks
- metrics hooks

These observability facilities must be pluggable and must not rely on global mutable state.

---

## 15. Concurrency model

The library must be safe for concurrent use within an agent process.

Internal mutable shared state such as:
- connection/session state
- handler registry
- subscription restoration state

must be protected appropriately inside the implementation.

The design does not require application-level locking around desired config identity decisions.
Sync decisions are based on comparing the running config UUID with the current desired config UUID.

---

## 16. Summary of key invariants

The following are normative design invariants:

1. `olg-nats-agent-core` is a library, not a daemon.
2. Configure uses **store desired state in KV, then notify**.
3. Action uses **direct publish to target subject**.
4. KV is a **single latest desired-config slot**, not a historical config store.
5. Config UUID is an **opaque identity token** used for equality comparison only.
6. `rpc_id` is used only for request/response correlation.
7. Agents determine sync by comparing **running UUID** vs **desired UUID in KV**.
8. Configure outcomes must include the **config UUID** that was attempted or applied.
9. Revision-driven config ordering and rollback semantics are out of scope.
10. Platform-specific execution logic remains outside the shared library.
