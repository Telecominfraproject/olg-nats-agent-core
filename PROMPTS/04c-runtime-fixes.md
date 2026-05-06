Read these files carefully before making any changes:
- PROMPTS/IMPLEMENTATION_PROMPT.md
- PROMPTS/04-session-kv-recovery.md
- PROMPTS/04-session-kv-recovery-tests.md
- REQUIREMENTS_CHECKLIST.md
- agentcore/client.go
- internal/session/session.go
- internal/session/callbacks.go
- internal/session/session_test.go
- internal/kv/watch.go
- internal/kv/watch_test.go

You are fixing only the remaining Phase 4 review issues in the current branch.
Do not broaden scope beyond the issues listed below.
Do not reimplement Phase 4 from scratch.
Preserve all exported public API types, method signatures, JSON tags, and overall Phase 4 design.

Goal:
Fix the remaining correctness and test-quality issues found during PR review for Phase 4 runtime support.

Issues to fix:

1. Start/Close lifecycle race
Problem:
- `internal/session.Manager.Start(...)` can still complete successfully after `Close(...)` has already returned.
- Today `Close(...)` may observe `m.nc == nil` while `Start(...)` is still in progress, mark the session closed, return success, and then `Start(...)` may later store a live `nc/js/kv` runtime anyway.
- This breaks shutdown correctness and can leave a connected runtime after the caller believes shutdown already completed.

Required fix:
- make Start and Close coordinate correctly
- if Close is requested while Start is in progress, Start must not publish a live runtime afterward
- Close must not return a misleading successful shutdown while startup is still unresolved
- keep repeated Start and repeated Close safe and predictable

Implementation guidance:
- keep the solution simple and idiomatic
- use existing mutex-based lifecycle state rather than adding a large abstraction
- it is acceptable to add a small internal synchronization mechanism if needed, such as:
  - a start-in-progress channel
  - a condition variable
  - another minimal lifecycle signal
- whichever mechanism you choose, ensure:
  - no deadlock
  - no goroutine leak
  - no lock held across blocking network calls
  - Start checks whether closing was requested before committing `m.nc`, `m.js`, and `m.kv`
  - Close handles in-flight startup explicitly rather than treating `m.nc == nil` as fully closed when startup is still running

Behavior requirements:
- Close before Start remains safe
- repeated Close remains safe
- Start while already started remains safe
- if Close races with Start, final state must be deterministic and must not leave a live runtime after Close has completed

2. Start(ctx) must honor context cancellation more accurately during connect/setup
Problem:
- `Start(ctx)` currently checks `ctx.Err()` only once and then calls `nats.Connect(...)`
- if the context is canceled or its deadline expires after startup begins, Start may still block until NATS connect timeout logic finishes
- this is not good public context behavior

Required fix:
- make `Start(ctx)` honor caller cancellation/deadline during connection setup as closely as practical
- use the caller’s context to influence connection timeout behavior
- do not silently ignore the context after startup begins

Implementation guidance:
- if the NATS API does not accept a context directly, use a practical and safe workaround
- acceptable approaches include:
  - clamping the effective NATS connect timeout to the remaining context deadline
  - or running connect in a goroutine and selecting on connect result vs `ctx.Done()`, then cleaning up safely if connect wins late
- keep the implementation simple and production-safe
- avoid leaking goroutines or connections
- do not introduce background work that can outlive the manager unintentionally

Behavior requirements:
- if context is already canceled, existing fast-fail behavior remains
- if context deadline is shorter than configured connect timeout, startup should respect that shorter deadline
- cancellation during startup should not leave a partially published live runtime

3. RetryOnFailedConnect behavior must be made consistent with synchronous startup
Problem:
- the current code accepts `RetryOnFailedConnect`, but `Start(...)` immediately tries JetStream and KV setup after `nats.Connect(...)`
- if the initial connection is not fully established yet, JetStream/KV setup can fail immediately and the code closes the connection
- this makes `RetryOnFailedConnect` behavior inconsistent or misleading for the synchronous Start contract

Required fix:
Choose one clear behavior and implement it consistently.

Preferred option:
- keep `Start(ctx)` as a synchronous “ready when returned” API
- only proceed to JetStream/KV setup once the NATS connection is actually usable
- if that cannot be achieved cleanly within current scope, then reject unsupported startup modes clearly rather than pretending to support them

Acceptable fallback option:
- if `RetryOnFailedConnect` cannot be supported correctly in this synchronous Phase 4 startup contract, return a clear validation/setup error for that mode and document it in code comments or tests

Important:
- do not leave `RetryOnFailedConnect` in a half-supported misleading state
- keep behavior explicit and testable

4. Make async watch tests deterministic and race-safe
Problem:
- some watch tests in `internal/kv/watch_test.go` currently use `time.Sleep(...)`
- some test state is shared unsafely across goroutines, such as sink error capture / handler counters
- this can become flaky and can fail under `go test -race`

Required fix:
- remove sleep-based synchronization from affected tests
- replace it with deterministic signaling:
  - channels
  - atomics
  - mutex-protected state
  - or select-with-timeout patterns
- make the async watch tests race-safe and CI-stable

Files expected to change:
- internal/session/session.go
- internal/session/callbacks.go only if needed for lifecycle correctness
- internal/session/session_test.go
- internal/kv/watch_test.go

Only change other files if truly needed for a clean fix.

Constraints:
- preserve current public facade behavior in `agentcore/client.go`
- do not add Phase 5 features
- do not add subscribe wrappers or handler registration
- do not add integration tests in this pass
- do not change exported API signatures unless absolutely unavoidable
- prefer minimal, well-reasoned fixes over large refactors

Test expectations:
Add or update unit tests for:
1. Start/Close race safety
   - concurrent Start and Close does not leave a live runtime after Close completes
   - final health/runtime state is deterministic
2. context-aware startup
   - Start respects a shorter context deadline than configured connect timeout
   - cancellation during startup does not publish a live runtime
3. RetryOnFailedConnect behavior
   - whichever behavior you choose is covered by tests
4. watch tests
   - async error sink tests are deterministic
   - no sleeps required for correctness
   - race-safe state handling

Important testing rules:
- keep tests unit-level only
- do not require a live NATS server
- do not add flaky timing assumptions
- preserve existing TC comment style and numbering conventions
- update existing tests only where necessary
- explain any changed expectations if a prior test must be adjusted

After coding:
1. run gofmt on all changed Go files
2. run go test ./...
3. run go test -race ./...
4. summarize:
   - files changed
   - which issue each file addresses
   - lifecycle fix approach chosen
   - context-handling fix approach chosen
   - RetryOnFailedConnect decision chosen
   - which async tests were made deterministic
   - any remaining limitations, if any

Success criteria:
- no live runtime can appear after Close has already completed during a Start/Close race
- Start behaves sensibly with caller cancellation/deadlines
- RetryOnFailedConnect is either truly supported for this startup model or clearly rejected
- watch tests are deterministic and race-safe
- repository remains compiling and tests pass
