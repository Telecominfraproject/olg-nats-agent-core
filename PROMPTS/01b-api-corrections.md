Read PROMPTS/IMPLEMENTATION_PROMPT.md, SPEC.md, REQUIREMENTS_CHECKLIST.md, and the LLD carefully.
Use PROMPTS/IMPLEMENTATION_PROMPT.md as the primary implementation source of truth for code generation scope and API shape.
Use SPEC.md as supporting design context.

Revise the existing Phase 1 bootstrap only. Do not implement Phase 2 or later work.

Scope:
- Update only public package files under agentcore/ and go.mod if needed
- Do not create internal packages
- Do not add business logic
- Do not add tests
- Do not implement transport/session/KV/subjects/handlers internals

Goal:
Align the current Phase 1 public API with the LLD exactly where the current generated code drifted.

Required corrections:

1. config.go
- Use the LLD config model as the primary baseline
- Keep AgentConfig and ObserveConfig only if they fit cleanly
- Restore explicit JetStreamConfig
- Restore TLSConfig
- Restore RetryConfig
- Restore ExecutionConfig
- Use pattern-oriented SubjectConfig names
- Keep the config surface strictly public and implementation-free

2. client.go
- Make the public API signatures match the LLD
- New must be: New(cfg Config, opts ...Option) (*Client, error)
- ConfigureHandler must accept ConfigureNotification, not ConfigureCommand
- WatchDesiredConfig must return (StopFunc, error)
- Registration methods must match the LLD:
  - RegisterConfigureHandler(target string, handler ConfigureHandler, opts ...SubscriptionOption) error
  - RegisterActionHandler(target, action string, handler ActionHandler, opts ...SubscriptionOption) error
  - RegisterResultHandler(target string, handler ResultHandler, opts ...SubscriptionOption) error
  - RegisterStatusHandler(target string, handler StatusHandler, opts ...SubscriptionOption) error
- Do not add implementation logic; keep bootstrap stubs only
- Health state names must align with the LLD health model

3. contracts.go
- Rewrite public models to match the LLD exactly
- Use Version, RPCID, Target, Timestamp in the common/base model where defined by the LLD
- ConfigureCommand, DesiredConfigRecord, ConfigureNotification, ActionCommand, ResultEnvelope, StatusEnvelope, StoredDesiredConfig, SubmissionAck, and HealthSnapshot must align with the LLD field names and semantics
- Keep payloads as json.RawMessage
- Do not introduce extra domain-specific fields not present in the LLD

4. errors.go
- Keep typed public errors
- Align Code enum and Error fields with the LLD
- It is acceptable to keep CodeNotImplemented temporarily for bootstrap stubs only
- Prefer the LLD structure for Code, Op, Subject, Key, Retryable, Err

5. observe.go
- Logger must use: Debug/Info/Warn/Error(msg string, kv ...any)
- Metrics should match the LLD observability interface, not a generic counter/gauge/histogram interface

6. doc.go
- Ensure package documentation describes the library as a reusable shared component used by long-running agents, not a daemon

Rules:
- Preserve Phase 1 boundaries
- Keep exported APIs documented
- Keep files small and focused
- Use context.Context only on public methods that can block or perform network I/O, consistent with the LLD
- Return bootstrap not-implemented errors where behavior is deferred
- Do not invent extra API surface beyond the LLD unless clearly harmless and justified

After editing:
- summarize exactly which files changed
- explain how each changed file now aligns with the LLD
- list which Phase 1 requirement IDs are now partially covered at API level
- list anything still intentionally deferred to later phases
