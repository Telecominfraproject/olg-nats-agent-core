Read PROMPTS/TESTS_STANDARD_TEMPLATE.md first and follow it exactly for:
- structure
- wording
- formatting
- naming
- comment style

Also read:
- SPEC.md
- REQUIREMENTS_CHECKLIST.md
- current codebase
- existing test files in the repository, especially:
  - agentcore/client_test.go
  - internal/contract/*_test.go
  - internal/subjects/*_test.go
  - internal/transport/*_test.go

Generate unit tests only for the implemented Phase 4 code in:

- agentcore/client.go
- internal/session/options.go
- internal/session/health.go
- internal/session/callbacks.go
- internal/session/session.go
- internal/session/types.go
- internal/kv/helpers.go
- internal/kv/store.go
- internal/kv/watch.go

Requirements to focus on:
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

Add or extend tests in these files:
- agentcore/client_test.go
- internal/session/options_test.go
- internal/session/health_test.go
- internal/session/callbacks_test.go
- internal/session/session_test.go
- internal/kv/helpers_test.go
- internal/kv/store_test.go
- internal/kv/watch_test.go

Test only implemented behavior.

Do not rewrite stable existing tests unless the actual behavior under test changed.
If you modify any existing tests, explain exactly why.

General test design rules:
- For each function under test, cover:
  1. valid success path
  2. direct validation failure
  3. dependency failure
  4. multi-step boundary failure if the function performs more than one operation
  5. side-effect assertions such as call count, call order, state transition, handler invocation, or error-sink usage where relevant
- For every multi-step flow, add one test for each failure boundary, not only the first failure path
- If a function does validate -> build key -> get runtime handle -> read/write/watch, test each meaningful boundary
- On failure, assert whether returned value should be nil where applicable
- Always assert whether downstream side effects were skipped or already performed
- Prefer wrapper-level tests, not only helper tests, for invalid input and runtime failure paths
- Keep tests readable, deterministic, and focused
- Use small test doubles and explicit assertions rather than over-abstracting helpers
- Do not refactor production code only to make testing easier unless a tiny, behavior-neutral seam is absolutely required
- Prefer package-local tests for unexported helpers where the current implementation already allows it

Important scope rules for this phase:
- Unit tests only
- Do not add real NATS server integration tests in this prompt
- Do not start `nats-server`
- Do not require Docker
- Do not test actual external reconnect against a live server
- Do not test future subscribe wrappers, handler registration, registry restore, or receive-side result correlation
- Do not invent tests for behavior the current code does not implement
- If a success path would require a live NATS/JetStream server and no unit seam exists in the current implementation, do not force a production refactor just to fake that success path; cover the unit-testable boundary behavior and note the live-runtime path as intentionally deferred to integration testing

Priority order:

1. Public client facade behavior
Cover:
- New
- Start
- Close
- Health
- StoreDesiredConfig
- LoadDesiredConfig
- WatchDesiredConfig
- StartupReconcile

Include tests for:
- existing stable tests remain valid
- nil watch handler rejected with CodeValidation
- disconnected runtime-backed APIs return CodeDisconnected before Start
- Start with canceled context returns CodeConnectionFailed
- Close before Start is safe and moves health to StateClosed
- Health returns the public mapped snapshot
- StartupReconcile delegates to latest desired-config load path behavior
- watch tracking cleanup remains safe and idempotent if additional coverage is needed
- if any existing client tests already cover these behaviors, extend only where there is a real gap

Preserve existing helper style in agentcore/client_test.go:
- requireErrorCode
- requireNotImplementedError
- testConfig
- TC comment numbering and wording pattern

2. Session config normalization and defaults
Cover:
- normalizeConfig
- validateKeyPattern
- EffectiveConfig accessors where meaningful

Include tests for:
- empty NATS server list falls back to nats.DefaultURL
- server entries are trimmed and empty entries dropped
- default connect timeout is applied
- default reconnect wait is applied
- default max reconnects is applied
- default reconnect buffer size is applied
- default JetStream timeout is applied
- default publish/subscribe/KV/shutdown/handler timeouts are applied
- default publish attempts/backoff are applied
- default KV bucket/key/history/replicas/storage are applied
- partial override preserves supplied values while defaulting unspecified ones
- agent-level override values remain intact
- negative timeout values are rejected
- negative retry values are rejected
- invalid KV history is rejected
- invalid KV storage is rejected
- invalid key pattern whitespace is rejected
- invalid key pattern placeholder count is rejected
- invalid unsupported format directives are rejected

3. Session health helpers
Cover:
- setConnectedLocked
- setReconnectingLocked
- setDegradedLocked
- setClosedLocked
- setStateLocked

Include tests for:
- connected state when JetStream and KV are both ready
- degraded state when readiness is partial
- reconnecting state clears readiness and preserves last error text
- closed state clears connected URL and readiness
- metrics hook receives state transitions when present
- nil metrics hook is safe

4. Session callbacks and manager boundary behavior
Cover:
- NewManager
- HealthSnapshot
- DesiredConfigBucket
- DesiredConfigKeyPattern
- KVTimeout
- KeyValue
- Start
- Close
- onDisconnect
- onClosed
- buildTLSConfig
- withKVTimeout
- drainConnection

Include tests for:
- NewManager returns StateNew initially
- NewManager rejects invalid normalized config
- KeyValue before Start returns CodeDisconnected
- Start with canceled context returns typed connection failure
- repeated Close before active connection is safe
- onDisconnect sets reconnecting when not closing
- onDisconnect does nothing harmful when closing
- onClosed sets closed state
- buildTLSConfig returns nil when config is nil or disabled
- buildTLSConfig rejects only-cert or only-key configuration
- buildTLSConfig loads CA file successfully
- buildTLSConfig loads client cert and key successfully using temp files
- withKVTimeout preserves an existing deadline
- withKVTimeout adds a deadline when one is missing
- drainConnection with nil connection returns nil

Do not force full unit tests for successful Start connect/JetStream/KV setup if they require a live server.
Do not add integration-style tests in this phase prompt.

5. KV helper validation
Cover:
- buildDesiredConfigKey
- validateToken
- validationError helpers only where behavior matters through public return values

Include positive and negative tests for:
- valid key pattern and valid target build the expected key
- empty pattern rejected
- whitespace in pattern rejected
- placeholder count mismatch rejected
- unsupported format directives rejected
- empty target rejected
- whitespace in target rejected
- '.' rejected
- wildcard tokens '*' and '>' rejected
- unsupported characters rejected
- valid target token accepted

6. KV store behavior
Cover:
- NewStore
- StoreDesiredConfig
- LoadDesiredConfig
- encodeDesiredConfigRecord
- decodeDesiredConfigRecord
- validateDesiredConfigRecord
- isConfigNotFound
- withTimeout helper if not already adequately covered elsewhere

Include tests for:
- nil runtime provider rejected
- valid desired config record accepted
- missing version rejected
- missing rpc_id rejected
- missing target rejected
- missing uuid rejected
- zero timestamp rejected
- empty payload rejected
- invalid JSON payload rejected
- StoreDesiredConfig validation fails before runtime KeyValue lookup
- StoreDesiredConfig propagates runtime KeyValue disconnected failure
- StoreDesiredConfig wraps Put failure as CodeKVStoreFailed
- StoreDesiredConfig success path returns stored metadata:
  - bucket
  - key
  - revision
  - created_at
- StoreDesiredConfig uses entry revision/created time from post-read when available
- StoreDesiredConfig falls back to record timestamp when post-read created time is zero
- StoreDesiredConfig does not fail the overall store when post-read metadata lookup fails
- StoreDesiredConfig reports post-read metadata lookup failure to error sink when present
- LoadDesiredConfig not-found returns CodeConfigNotFound
- LoadDesiredConfig generic get failure returns CodeKVReadFailed
- LoadDesiredConfig decode failure is surfaced
- LoadDesiredConfig success path returns decoded record plus bucket/key/revision metadata
- decodeDesiredConfigRecord rejects empty payload
- decodeDesiredConfigRecord rejects malformed JSON
- encodeDesiredConfigRecord rejects invalid records
- isConfigNotFound recognizes not-found and deleted forms the implementation supports

Use small test doubles for:
- RuntimeProvider
- jetstream.KeyValue
- jetstream.KeyValueEntry

The test doubles should let you assert:
- call count
- keys used
- payload written
- get/put errors
- returned revision
- created time
- error sink behavior

7. KV watch behavior
Cover:
- WatchDesiredConfig
- consumeWatch

Include tests for:
- nil handler rejected
- invalid target/pattern rejected before watch setup
- runtime KeyValue failure propagated
- watch setup failure wrapped as CodeKVReadFailed
- valid watch update decodes into StoredDesiredConfig and calls handler once
- nil entry is skipped
- empty entry value is skipped
- decode failure is sent to error sink and handler is not called
- handler error is sent to error sink
- CreatedAt uses entry.Created() when available
- CreatedAt falls back to record timestamp when entry.Created() is zero
- returned StopFunc is safe and idempotent
- context cancellation stops the watch path cleanly

Use small test doubles for:
- RuntimeProvider
- jetstream.KeyValue
- jetstream.KeyWatcher
- jetstream.KeyValueEntry

The test doubles should let you assert:
- watch setup call count
- watched key
- stop called
- update stream behavior
- handler call count
- handler received values
- async error sink calls

Test-double rules:
- Keep doubles local to the relevant test file unless sharing clearly reduces duplication
- Prefer explicit fake structs over reflection-heavy helpers
- Only implement the interface methods needed to satisfy the compiler and current tests
- Keep doubles readable and deterministic

Do not add tests for:
- live NATS connection success
- live JetStream creation success
- live KV bind/create success against a running server
- actual reconnect after a real server drop
- subscribe wrappers
- handler registration
- reconnect restore
- public receive-side correlation
- future phases
- README/examples/docs

TC naming guidance:
- Keep existing client TC numbering stable
- Continue client numbering only if new client tests are added
- For new internal package suites, use consistent area prefixes such as:
  - TC-SESSION-001...
  - TC-KV-001...
- Follow the exact comment-block format from PROMPTS/TESTS_STANDARD_TEMPLATE.md
- Keep wording style consistent with the current repository tests

At the end, provide:
- files added
- files modified
- positive coverage summary
- negative coverage summary
- any existing tests changed and why
- any intentionally deferred gaps
- clear note that live NATS/JetStream runtime success behavior remains for integration testing if not unit-testable with the current implementation
