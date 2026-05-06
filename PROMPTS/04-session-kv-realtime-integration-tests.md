Read these files carefully before making any changes:
- SPEC.md
- REQUIREMENTS_CHECKLIST.md
- README.md
- agentcore/client.go
- agentcore/client_test.go
- internal/session/session.go
- internal/session/options.go
- internal/session/health.go
- internal/session/callbacks.go
- internal/kv/store.go
- internal/kv/watch.go
- internal/kv/helpers.go

Also read existing tests so style stays consistent:
- internal/session/session_test.go
- internal/session/options_test.go
- internal/session/health_test.go
- internal/kv/store_test.go
- internal/kv/watch_test.go

You are adding real-server integration tests only for the currently implemented runtime/session/KV/recovery behavior.

Do not broaden scope beyond what is already implemented in the codebase.
Do not add tests for future features.
Do not rework production code unless a very small and clearly justified test seam is absolutely required.

Goal:
Add integration tests that verify the current Phase 4 implementation against a real `nats-server` with JetStream enabled.

Scope:
Test only the public and runtime behaviors that are already implemented now:
- `New(...)`
- `Start(ctx)`
- `Close(ctx)`
- `Health()`
- `StoreDesiredConfig(...)`
- `LoadDesiredConfig(...)`
- `WatchDesiredConfig(...)`
- `StartupReconcile(...)`

Do not test deferred features such as:
- `SubmitConfigure(...)`
- `SubmitAction(...)`
- `PublishResult(...)`
- `PublishStatus(...)`
- subscribe wrappers
- handler registration
- reconnect-safe subscription restore
- receive-side result correlation
- workload/business logic

Integration-test rules:
- use a real `nats-server` process with JetStream enabled
- do not use mocks/fakes for the server, JetStream, or KV in these integration tests
- keep tests self-contained and deterministic
- start and stop the test server from the tests where practical
- do not require Docker
- do not depend on external infrastructure
- prefer dynamically chosen free local ports instead of hard-coded ports
- clean up all subprocesses and temporary state
- use bounded waits with timeouts rather than unbounded blocking
- keep tests readable and production-oriented

Directory/layout expectation:
- create a separate integration-test area instead of mixing these into existing unit-test files
- preferred path:
  - `tests/integration/phase4_session_kv_integration_test.go`
- if a small shared helper file is needed, add:
  - `tests/integration/testserver_test.go`
  - or another very small helper file in the same directory

Test execution expectation:
- integration tests should be runnable independently from the fast unit suite
- choose one simple mechanism and apply it consistently:
  - build tag based integration tests, or
  - environment-gated integration tests, or
  - separate integration package path with clear skip behavior if `nats-server` is unavailable
- prefer the least surprising approach for this repository
- document in code comments how to run the integration tests

Recommended practical approach:
- use a build tag such as `integration`, OR
- skip with a clear message if `nats-server` binary is not available in PATH
- if using a build tag, keep it minimal and standard
- if using skip behavior, the skip message must clearly say that real `nats-server -js` is required

Real-server setup expectations:
- launch `nats-server` with JetStream enabled
- use a temporary storage directory for server state if needed
- wait until the server is ready before starting client operations
- use a fresh bucket name per test where needed to avoid cross-test interference
- ensure server process is terminated on test cleanup even if assertions fail

Configuration expectations:
- construct real `agentcore.Config` values suitable for local integration tests
- point the client to the dynamically chosen local server URL
- enable/configure the desired-config KV bucket and key pattern as needed
- keep config minimal and explicit
- prefer test-local bucket/key names over shared hard-coded global names when possible

Coverage required:

1. Client start/close integration
Add an integration test that verifies:
- a real client can start against a real JetStream-enabled server
- `Health()` reports a connected/ready runtime after start
- `Close(ctx)` succeeds
- after close, health reflects closed state

2. KV bucket bind/create integration
Add integration coverage that verifies:
- the configured desired-config bucket is created when auto-create is enabled and the bucket does not already exist
- a client can also bind to an already existing bucket on a later start
- startup fails clearly if the bucket is missing and auto-create is disabled

3. Store/load desired-config integration
Add integration coverage that verifies:
- `StoreDesiredConfig(...)` writes a real desired-config record into KV
- returned metadata includes bucket/key/revision where available
- `LoadDesiredConfig(...)` returns the same logical record
- revision metadata is exposed as expected
- stored payload round-trips correctly through real KV

4. Watch desired-config integration
Add integration coverage that verifies:
- `WatchDesiredConfig(...)` receives real updates from JetStream KV
- the decoded watched value matches what was stored
- the returned `StopFunc` stops the watch cleanly
- watch callback execution is deterministic and properly synchronized in test code
- avoid sleep-based flakiness; prefer channels/select with timeout

5. Startup reconciliation integration
Add integration coverage that verifies:
- after storing desired config, a fresh client/session can call `StartupReconcile(...)`
- the latest desired config is returned correctly
- this works as a recovery-oriented load path using the real KV store

6. Health visibility integration
Add integration assertions where appropriate for:
- initial post-start state
- readiness of JetStream/KV after startup
- final closed state after `Close(ctx)`

Negative-path integration coverage:
Add a small number of real integration negatives where they are practical and meaningful:
- startup with missing bucket and auto-create disabled returns a clear failure
- loading desired config for a missing target returns the expected not-found behavior
Do not invent exotic failure scenarios that belong better in unit tests.

Test-style rules:
- preserve the repository’s existing testing style where practical
- keep test names explicit and descriptive
- if adding TC comment blocks, keep the same style used elsewhere in the repo
- use helper functions sparingly and only when they clearly improve readability
- avoid giant monolithic integration tests; use a few focused tests instead
- keep asynchronous assertions deterministic
- no `time.Sleep(...)` used as the correctness mechanism

Production-code modification rule:
- prefer zero production-code changes
- if a tiny production change is truly necessary to support reliable integration testing, keep it minimal, behavior-neutral, and explain it clearly at the end
- do not redesign runtime/session/KV code just for test convenience

Success criteria:
- integration tests exercise real NATS + JetStream + KV behavior
- tests validate the currently usable runtime/session/KV/recovery API only
- tests are stable and readable
- tests do not rely on deferred future features
- repository still formats and builds cleanly

After coding:
1. run `gofmt` on all changed Go files
2. run `go test ./...`
3. run the integration tests using the chosen mechanism
4. summarize:
   - files added
   - files modified
   - how the real test server is started/stopped
   - which real behaviors were proven
   - any skipped/deferred integration scenarios
   - any production-code changes made, if any
