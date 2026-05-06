Read PROMPTS/IMPLEMENTATION_PROMPT.md carefully.
Also read SPEC.md for additional design context.
Use PROMPTS/IMPLEMENTATION_PROMPT.md as the primary source of truth for implementation scope, architecture, runtime behavior, and boundaries.

Also read REQUIREMENTS_CHECKLIST.md and the current codebase carefully before making changes.

Build on the existing Phase 1, Phase 2, and Phase 3 code.
Treat the current public API as the baseline.
Preserve all existing exported structs, field names, JSON tags, and method signatures unless a compile-blocking issue requires a minimal compatibility fix.

Now implement only:
1. internal/session
2. internal/kv
3. the client lifecycle and runtime wiring needed to back these existing public APIs:
   - Start(ctx)
   - Close(ctx)
   - Health()
   - StoreDesiredConfig(...)
   - LoadDesiredConfig(...)
   - WatchDesiredConfig(...)
   - StartupReconcile(...)
4. centralized timeout/retry default handling used by session and KV-backed operations
5. health state updates needed for connection lifecycle, JetStream/KV readiness, and recovery visibility

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

Goal:
Add the real runtime/session/KV layer so the existing public API becomes operational for:
- NATS connection lifecycle
- JetStream setup
- KV bind/create and desired-config storage/retrieval
- health exposure
- startup/restart desired-state recovery support

Core implementation rules:
- Use only the Go standard library plus:
  - github.com/nats-io/nats.go
  - github.com/nats-io/nats.go/jetstream
- Use jetstream.New(nc), not the legacy Conn.JetStream() API
- Keep the library workload-agnostic and agent-agnostic
- Keep the implementation modular, race-safe, and easy to review
- Protect mutable runtime state with mutexes where needed
- Avoid global variables
- Reuse the existing Phase 2 contract/codec/validation helpers where appropriate
- Reuse the existing Phase 3 publish/configure helpers where appropriate rather than re-implementing messaging logic
- Keep the configure contract as store desired config in KV, then publish a lightweight configure notification
- Do not replace existing public models with a different architecture
- Do not invent extra domain-specific APIs unless clearly necessary and minimal

Strict public API rule:
- Preserve all existing exported public structs, field names, JSON tags, and method signatures unless there is a compile-blocking issue that cannot be solved internally.
- If a minimal public API fix is absolutely required, keep it as small as possible and report it clearly at the end.

Implementation scope boundaries:
- Do not implement Phase 5 work in this phase
- Do not implement subscribe wrappers yet
- Do not implement handler registration yet
- Do not implement subscription registry or reconnect restoration yet
- Do not implement receive-side result correlation yet
- Do not add workload-specific logic
- Do not add tests in this phase
- Do not add examples or README work in this phase unless a tiny comment/doc correction is required
- Do not implement historical desired-config retrieval or rollback/history APIs
- Do not make KV revision/history part of the functional desired-state contract

Session / lifecycle expectations:
The client must own the runtime session state needed for:
- *nats.Conn
- jetstream.JetStream
- jetstream.KeyValue
- watch stop functions / cleanup state
- health snapshot state
- effective runtime config/defaults needed during operation

Start(ctx) should:
1. validate the effective runtime config required for startup
2. apply central timeout/retry defaults wherever the config leaves values unset
3. establish a NATS connection using configured server/auth/TLS settings
4. install connection lifecycle callbacks needed for health visibility
5. create a JetStream handle using jetstream.New(nc)
6. bind or create the configured KV bucket for desired config
7. wire the client to concrete session/KV-backed helpers
8. expose a healthy connected snapshot through Health()

Close(ctx) should:
1. transition health into draining/closing
2. stop any active KV watch helpers created by this client
3. drain the NATS connection cleanly where practical
4. fall back to close if drain cannot complete cleanly
5. release internal runtime references safely
6. expose a final closed health state

Idempotency expectations:
- Repeated Start must not corrupt runtime state
- Repeated Close must be safe and predictable
- If Start is called when already started, either:
  - no-op safely, or
  - return a clear typed state/validation error
- If Close is called after already closed, it should be safe and minimal
- Operations that require an active session must fail clearly when disconnected or not started

Reconnect expectations:
- Use daemon-friendly reconnect behavior from config/defaults
- Expose reconnecting/degraded state through Health()
- Update last-error/health information on disconnect/reconnect/close events
- Keep this phase limited to connection recovery and readiness visibility
- Do not implement subscription re-registration in this phase
- If JetStream or KV handles need to be refreshed or rebound after reconnect, handle that within the session layer
- Keep reconnect behavior library-owned rather than leaving it to consuming agents

Health expectations:
Health() must return a read-only snapshot that reflects current runtime state.
The health snapshot should reflect:
- new / connecting / connected / reconnecting / draining / closed / degraded states as appropriate
- connected server URL where available
- JetStream readiness
- KV readiness
- last error text where useful

Keep health updates centralized rather than scattered across unrelated code.
Keep the health model operational and transport-focused, not business-logic-focused.

Timeout / retry expectations:
- Define sensible central defaults for connection/session/KV-related operations where config leaves them unset
- Agent-level override must remain possible through the existing public config model
- Define all defaults as explicit named constants or a single normalization helper
- Do not scatter fallback values across multiple call sites
- Apply effective defaults consistently across startup, shutdown, KV operations, and watch setup where relevant
- Prefer one small normalization/defaulting layer over repeated inline fallback logic
- Keep retry behavior aligned with the existing public config model and typed error model

NATS connection expectations:
- Build connection options from the existing public NATS config
- Support the currently exposed auth and TLS fields if configured
- Use reconnect-related config values from the existing public config model
- Set client name if configured
- Return clear typed errors on connection/setup failure
- Do not hide major startup failures only in logs or callbacks

JetStream expectations:
- Create the JetStream context from the active NATS connection using jetstream.New(nc)
- Respect existing public JetStream config fields where applicable
- Keep JetStream setup centralized in the session/runtime layer
- Expose JetStream readiness through health state

KV expectations:
Implement a concrete desired-config KV store backed by JetStream Key-Value.

KV bucket behavior must be explicit:
- If the configured bucket already exists, bind to it
- If the bucket does not exist and AutoCreateBucket is true, create it
- If the bucket does not exist and AutoCreateBucket is false, return a typed setup failure

KV expectations in detail:
- Use the configured key pattern for desired config keys
- Keep KV concerns centralized in internal/kv
- StoreDesiredConfig(...) must:
  1. validate the input as needed through shared transport-level validation
  2. derive the configured KV key for the target
  3. encode/store the DesiredConfigRecord in KV
  4. return StoredDesiredConfig metadata including bucket, key, and revision where available
- LoadDesiredConfig(...) must:
  1. load the latest desired config for the target from KV
  2. decode it into the shared public model
  3. return StoredDesiredConfig metadata including revision where available
  4. return a clear typed not-found error when no desired config exists

Revision expectations:
- Expose revision metadata where the current public types already support it
- Satisfy revision-related requirements primarily through StoredDesiredConfig and related return metadata
- Keep UUID as the desired-state identity
- Treat KV revision as supporting metadata only
- Do not introduce a revision-first synchronization contract

Watch expectations:
- Implement WatchDesiredConfig(...) if practical in this phase using JetStream KV watch support
- Keep watch behavior optional and scoped to the current target/key
- Return a StopFunc that cleanly stops the watch path and associated goroutine/resources
- Decode watched KV values back into StoredDesiredConfig / DesiredConfigRecord before calling the public handler
- Keep watch callback execution simple and safe
- Track active watch stop functions in client-owned runtime state so Close(ctx) can stop them cleanly
- Do not mix KV watch state with future subscription registry responsibilities
- If asynchronous watch callback errors need surfacing, prefer existing logger/error-sink style hooks rather than inventing a large new error-channel API

Startup / recovery expectations:
- Implement StartupReconcile(ctx, target) as a desired-state recovery helper that loads the latest desired config for the target from KV
- Keep this helper focused on retrieval/recovery support only
- Do not trigger business logic, local apply logic, or result publication from StartupReconcile itself
- The recovery path should be clear enough that a consuming agent can call it on startup or restart to compare desired state vs locally applied state
- Not-found behavior should be explicit and predictable

Client integration expectations:
- Wire the existing public Client methods to the concrete internal/session and internal/kv implementation
- Avoid large public API churn in this phase
- Keep runtime wiring inside the library
- Consuming agents should not need to assemble low-level NATS/JetStream/KV details themselves
- Small focused changes to existing Phase 2/3 internal files are acceptable only where required for clean integration

Typed error expectations:
Use the existing public typed error model and existing error codes consistently.

Map failures as follows:
- connection setup failures -> CodeConnectionFailed
- disconnected/not-started runtime usage -> CodeDisconnected
- JetStream setup/bind/create failures -> CodeJetStreamFailed
- KV write failures -> CodeKVStoreFailed
- KV read/load/watch failures -> CodeKVReadFailed
- desired-config missing/not found -> CodeConfigNotFound
- shutdown/drain/close failures -> CodeShutdown

General error rules:
- Keep retryability metadata sensible and consistent
- Do not replace typed errors with plain formatted errors
- Wrap underlying causes where useful
- Prefer clear typed failures over vague generic errors

Suggested files:
- internal/session/session.go
- internal/session/options.go
- internal/session/health.go
- internal/session/callbacks.go
- internal/kv/store.go
- internal/kv/watch.go
- internal/kv/helpers.go

Potential integration touch points:
- agentcore/client.go
- agentcore/config.go only if a minimal compatibility correction is truly required
- internal/transport/configure.go only if a small integration adjustment is needed
- internal/contract only if a small codec/helper addition is required for desired-config storage

Add more small focused files only if clearly needed.

Build / verification requirements:
- Run gofmt on all changed Go files
- Run go test ./...
- If go test ./... fails because of pre-existing issues or incomplete later-phase work, report that clearly instead of masking it
- Do not silently leave the repository in a non-compiling state

After coding:
- summarize exactly which files were added or changed
- explain how the implementation aligns with PROMPTS/IMPLEMENTATION_PROMPT.md
- map each added/changed file to these requirement IDs:
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
- list any minimal public API adjustments made, if any
- list what is intentionally deferred to later phases

Review checklist for this phase:
- jetstream.New(nc) is used
- KV bind/create logic exists
- timeout and retry defaults are defined centrally
- agent-level timeout/retry override is possible
- Start/Close/Health are backed by real runtime behavior
- desired-config store and load helpers exist and use real KV operations
- optional watch path exists if implemented
- startup or restart reconciliation path is clear
- revision metadata is exposed where expected without making revision the primary sync contract
- health state is visible and updated through the session lifecycle
- shutdown drains or closes the session cleanly
- no subscribe/handler/reconnect-restore logic leaked into this phase
- no workload/business logic was added
