# olg-nats-agent-core

Core NATS agent library for OLG

`olg-nats-agent-core` is a shared Go library for agents that communicate over a NATS bus.

It is intended to provide common bus-facing functionality such as:
- NATS connection and reconnect handling
- JetStream and Key-Value access
- standard subject naming
- standard message envelopes
- configure and action submission helpers
- result and status publication helpers
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

Current code includes:
- public config, model, error, logger, and metrics types
- shared contract codec and validation helpers (`internal/contract`)
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

Still deferred to later phases:
- submit/publish wrappers for configure, action, result, and status
- subscribe wrappers and handler registration
- reconnect-safe subscription restore
- receive-side result correlation helpers
- integration examples / full quick-start flows

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

## What is planned next

Later phases are intended to add:
- configure/action submit wrappers
- result/status publish wrappers
- subscribe wrappers and handler registration
- reconnect-safe subscription restoration
- receive-side result correlation helpers

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

The flows below describe the intended library communication model.

As of the current implementation:
- session startup, JetStream/KV access, desired-config store/load/watch, and startup reconciliation are implemented
- configure/action submit wrappers and receive-side handler flows are still planned for later phases

### Configure flow

1. Agent receives a validated configure request
2. Library stores desired configuration in JetStream KV
3. Library publishes a lightweight configure notification
4. Target agent receives the notification
5. Target agent loads the current desired config from KV
6. Target agent applies it locally
7. Target agent publishes a result or status message

### Action flow

1. Agent receives a validated action request
2. Library publishes the action command on the target action subject
3. Target agent receives the action
4. Target agent executes the local action
5. Target agent publishes a result or status message

### Result flow

1. Target agent publishes result/status
2. Calling side receives the message through the library
3. Correlation is performed using shared message fields

---

## Default subject model

The default subject structure is target-oriented:

- `cmd.configure.<target>`
- `cmd.action.<target>.<action>`
- `result.<target>`
- `status.<target>`
- `health.<target>`

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

This repository includes real-server integration tests for the currently
implemented Phase 4 runtime/session/KV/recovery behavior.

For local integration runs, `nats-server` must be installed and available in
`PATH`.

Unit tests:

`go test ./...`

Integration tests:

`go test -count=1 -v -tags=integration ./tests/integration/...`
