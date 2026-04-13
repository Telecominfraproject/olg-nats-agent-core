# olg-nats-agent-core

Core NATS agent library for OLG

`olg-nats-agent-core` is a shared Go library for agents that communicate over a NATS bus.

It provides common bus-facing functionality such as:
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

## What this library does

This library helps agents:

- connect to NATS
- reconnect after temporary disconnects
- create and use JetStream
- store desired configuration in JetStream KV
- publish configure notifications
- publish action commands
- publish result and status messages
- subscribe to message subjects
- restore subscriptions after reconnect

---

## What this library does not do

This library does **not** implement workload-specific or platform-specific logic.

Examples of things that should stay outside this library:

- VyOS configuration translation
- host reboot/script execution
- trace or remote terminal implementation
- cloud-side business validation
- local apply / rollback logic

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

## Notes

For the normative design contract and exact behavior, see `SPEC.md`.
>>>>>>> c5881d3 (feat(docs): add NATS agent library spec and requirements checklist)
