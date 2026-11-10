# olg-nats-agent-core

Core NATS agent library for OLG

`olg-nats-agent-core` is a shared Go library for agents that communicate over a NATS bus.

It is intended to provide common bus-facing functionality such as:
- NATS connection and reconnect handling
- JetStream and Key-Value access
- standard subject naming
- standard message envelopes
- configure and action submission helpers
- result, status, and state publication helpers
- desired configuration storage and retrieval

The library is **not a daemon**.  
It is meant to be used **inside long-running agents** such as:
- ucentral-client agent
- host agent
- VyOS agent

---

## Purpose

The goal of this library is to keep all common NATS/JetStream messaging logic in one reusable place, while leaving platform-specific logic inside the agents.

In simple words:

- **library** = common messaging and state helper
- **agent** = local business logic and execution

---

## Current status

This repository currently includes:

- Phase 1 bootstrap and public API
- Phase 2 contract, codec, and validation helpers
- Phase 3 subject helpers and publish-path foundations
- Phase 4 session, JetStream, KV, health, and recovery support
- Phase 5 bidirectional public transport APIs (send + receive), handler runtime wiring, and reconnect-safe subscription restore

Phase 6 remains scoped to:
- clear error outcomes
- logging hooks
- metrics hooks
- operational visibility

Current code includes:
- public config, model, error, logger, and metrics types
- centralized subject generation and validation helpers (`internal/subjects`)
- runtime session management (`internal/session`)
- JetStream and Key-Value setup for desired configuration storage
- centralized timeout and retry defaults with override support
- public lifecycle and state APIs:
  - `Start(ctx)`
  - `Close(ctx)`
  - `Health()`
- public desired-config APIs:
  - `StoreDesiredConfig(...)`
  - `LoadDesiredConfig(...)`
  - `WatchDesiredConfig(...)`
  - `StartupReconcile(...)`
- public send/publish APIs:
  - `SubmitConfigure(...)`
  - `SubmitAction(...)`
  - `PublishResult(...)`
  - `PublishStatus(...)`
  - `PublishState(...)`
- public handler registration APIs:
  - `RegisterConfigureHandler(...)`
  - `RegisterActionHandler(...)`
  - `RegisterResultHandler(...)`
  - `RegisterStatusHandler(...)`
  - `RegisterStateHandler(...)`
- optional queue-group subscription option:
  - `WithQueueGroup(...)`
- optional client constructor options:
  - `WithReconnectHandler(...)`
- deferred registration support before `Start(ctx)`
- configure store-then-notify flow via `SubmitConfigure(...)`
- direct action publish flow via `SubmitAction(...)`
- public result/status/state publish via `PublishResult(...)`, `PublishStatus(...)`, and `PublishState(...)`
- low-level NATS publish/flush/timeout behavior centralized in `internal/transport.Publisher`
- subscribe readiness synchronized with NATS using a flush before marking subscriptions active
- reconnect-safe subscription restoration from in-memory registry intent
- session-closed subscription cleanup that clears active handles while preserving registration intent
- receive-side callback decode/validate/dispatch for configure/action/result/status/state
- receive-side result correlation via preserved `rpc_id` in `ResultEnvelope`
- lifecycle-context handler dispatch with cancellation on close/session shutdown
- public sender-to-receiver integration coverage on real `nats-server`, including action/result round-trip and reconnect restore with public `PublishResult(...)`

---

## What this library does today

The library currently helps agents:
- start and close a NATS-backed runtime session
- expose connection/session health to the owning agent
- create and use JetStream through the shared session layer
- bind to or create the desired-config KV bucket
- store desired configuration in JetStream KV
- load desired configuration from JetStream KV
- optionally watch desired configuration updates
- retrieve the latest desired configuration for startup reconciliation
- register configure/action/result/status/state handlers before or after `Start(ctx)`
- restore handler subscriptions after reconnect without manual re-registration
- receive typed configure/action/result/status/state messages through callback binding
- correlate received results using preserved `rpc_id`
- submit configure commands with store-then-notify semantics
- submit action commands through direct publish
- publish result envelopes
- publish status envelopes
- publish state envelopes
- compose public sender and receiver APIs into agent-to-agent flows

---

## Design overview

The library follows a **latest desired-state** model for configuration.

At a high level:
- desired configuration is stored in JetStream KV
- target agents reload the current desired config from KV
- configure uses a **store-then-notify** flow
- action uses a **direct publish** flow
- sync is determined using config UUID comparison

The library is designed around the idea that agents use shared transport/state helpers from one common package, while keeping execution logic in the agents themselves.

---

## Basic communication model

The flows below describe the current library communication model.

As of the current implementation:
- configure uses `SubmitConfigure(...)` (store desired config, then publish notification)
- action uses `SubmitAction(...)` (direct action publish)
- result/status use `PublishResult(...)` and `PublishStatus(...)`
- receive handlers are available for configure/action/result/status
- registered subscriptions are restored after reconnect

### Configure flow

1. Agent calls `SubmitConfigure(...)` with a validated configure command
2. Library stores desired configuration in JetStream KV
3. Library publishes a lightweight configure notification
4. Target agent receives the notification
5. Target agent loads the current desired config from KV
6. Target agent applies it locally
7. Target agent publishes a result or status message

### Action flow

1. Agent calls `SubmitAction(...)` with a validated action command
2. Library publishes the action command on the target action subject
3. Target agent receives the action
4. Target agent executes the local action
5. Target agent publishes a result or status message

### Result flow

1. Target agent publishes result/status via `PublishResult(...)` or `PublishStatus(...)`
2. Calling side receives the message through the library
3. Correlation is performed using shared message fields

### State flow (periodic operational state)

1. Publishing agent collects operational metrics and telemetry
2. Agent calls `PublishState(...)` with a validated `StateEnvelope`
3. Library publishes the payload exclusively on `state.<target>`
4. Consuming agents (e.g. ucentral-client) receive the state via `RegisterStateHandler(...)`

### Configure store-then-notify semantics

`SubmitConfigure(...)` performs two separate operations:
- store desired configuration in JetStream KV
- publish a lightweight configure notification on NATS

`SubmitConfigure(...)` is a store-then-notify operation, not an atomic transaction across KV storage and NATS publish. If KV storage succeeds but configure notification publish fails, the desired config remains stored and the caller receives the publish error.

There is currently no separate public API to retry only the configure notification. Callers that want to retry through the public API can retry `SubmitConfigure(...)` with the same command, with the understanding that this may create a new KV revision. Target agents can also use `WatchDesiredConfig(...)`, `StartupReconcile(...)`, or `LoadDesiredConfig(...)` to observe the latest durable desired state depending on their workflow.

### Context behavior for send APIs

Public send APIs (`SubmitConfigure(...)`, `SubmitAction(...)`, `PublishResult(...)`, `PublishStatus(...)`, and `PublishState(...)`) require a non-nil, active context. Nil or already canceled contexts are rejected before any KV store or publish work is attempted. The library does not silently replace nil context with `context.Background()`; callers are expected to provide cancellation and timeout behavior explicitly.

---

## Default subject model

The default subject structure is target-oriented:

- `cmd.configure.<target>`
- `cmd.action.<target>.<action>`
- `result.<target>`
- `status.<target>`
- `state.<target>`
- `health.<target>`

### Subject separation rules

- **`state.<target>` (exclusive):** Used exclusively for periodic operational device state and telemetry.
- **`status.<target>` (reserved):** Strictly reserved for lifecycle states (startup) and configure/action execution progress transitions.
- **Non-interchangeability:** Periodic device state has a completely different schema and purpose than lifecycle/progress status. Publishing full device-state payloads on the status subject could cause consumers to interpret them as progress/status messages or reject them as invalid.

---

## Default KV model

Default KV conventions:
- bucket: `cfg_desired`
- key pattern: `desired.<target>`

The library uses KV to hold the current desired configuration for a target.

---

## Currently usable public API

The main public APIs currently usable by an owning agent are:

- `New(...)`
- `Start(ctx)`
- `Close(ctx)`
- `Health()`
- `StoreDesiredConfig(...)`
- `LoadDesiredConfig(...)`
- `WatchDesiredConfig(...)`
- `StartupReconcile(...)`
- `SubmitConfigure(...)`
- `SubmitAction(...)`
- `PublishResult(...)`
- `PublishStatus(...)`
- `PublishState(...)`
- `RegisterConfigureHandler(...)`
- `RegisterActionHandler(...)`
- `RegisterResultHandler(...)`
- `RegisterStatusHandler(...)`
- `RegisterStateHandler(...)`
- `WithReconnectHandler(...)`

---

## Handler execution model

Registered configure/action/result/status/state handlers, as well as the custom reconnect handler registered via `WithReconnectHandler`, are executed inline from the NATS subscription and connection callback paths.

Handlers and callbacks must return quickly. Any long-running or blocking work (for example, applying configurations, executing actions, or performing database reconnects inside the reconnect handler) must be offloaded by the agent to a separate goroutine, worker pool, or internal job queue. This prevents stalling the connection reconnect dispatch, avoids delaying subscription restorations, and prevents backpressure in callback processing.

The library recovers from any panics thrown by user configure, action, result, status, and state handlers (which are caught, logged, and recorded as metrics failures). For the custom reconnect handler, any caught panic is logged and also reported to the registered `errorSink` (if provided) to prevent agent crashes.

The library owns subscription registration, callback binding, envelope decoding, and reconnect restore. Agents remain responsible for workload execution and handler concurrency policy.

---

### Current startup limitation

`RetryOnFailedConnect` is not supported by the current synchronous `Start(ctx)` behavior.

If enabled, `Start(ctx)` returns a validation error instead of entering a partially connected retrying startup mode.

---

## Notes

For the normative design contract and exact behavior, see `SPEC.md`.
SubmitConfigure failure semantics are defined in `SPEC.md` section `6.4`.
---

## Build / toolchain note

This repository currently targets Go 1.25.x.

## Testing

This repository includes:
- receive-side integration tests using raw NATS message injection
- public sender-to-receiver integration tests using public APIs on both clients
- public action/result round-trip integration coverage
- reconnect-restore integration coverage with public `PublishResult(...)` after server restart

For local integration runs, `nats-server` must be installed and available in
`PATH`.

Unit tests:

`go test ./...`

Integration tests:

`go test -count=1 -v -tags=integration ./tests/integration/...`
