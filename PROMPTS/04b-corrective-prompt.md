Refactor the current Phase 4 implementation to remove duplicated runtime logic while preserving the current behavior and public API.

Required cleanup:
- keep `internal/session` and `internal/kv` as the single implementation source for Phase 4 runtime behavior
- update `agentcore/client.go` to use those internal packages directly
- remove duplicated runtime/session/KV logic currently implemented in `agentcore/runtime_session.go`
- do not keep two copies of the same behavior

Required wiring:
- `agentcore.Client` must use `internal/session.Manager`
- `agentcore.Client` must use `internal/kv.Store`
- `Start`, `Close`, and `Health` must delegate to `internal/session`
- `StoreDesiredConfig`, `LoadDesiredConfig`, `WatchDesiredConfig`, and `StartupReconcile` must delegate to `internal/kv`

Architecture rule:
- `agentcore` must remain the public facade and API surface
- concrete runtime/session/KV behavior must live in `internal/session` and `internal/kv`
- do not duplicate normalization, session lifecycle, health, KV store/load/watch, reconnect, or TLS/runtime helper logic across both public and internal packages

Constraints:
- preserve all existing exported public structs, field names, JSON tags, and method signatures
- do not broaden scope beyond this cleanup
- do not reimplement Phase 4 from scratch
- keep behavior equivalent unless a small correction is required for clean wiring

Files to touch:
- update `agentcore/client.go`
- delete `agentcore/runtime_session.go`
- update `agentcore/client_test.go` only if needed due to rewiring
- keep and reuse:
  - `internal/session/session.go`
  - `internal/session/options.go`
  - `internal/session/health.go`
  - `internal/session/callbacks.go`
  - `internal/kv/store.go`
  - `internal/kv/watch.go`
  - `internal/kv/helpers.go`

After refactor:
1. run gofmt on changed Go files
2. run go test ./...
3. summarize exactly what was rewired
4. confirm that runtime logic now exists in only one place
5. list any remaining follow-up items, if any
