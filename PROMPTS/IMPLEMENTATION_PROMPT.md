# VS Code Code Generation Prompt for `olg-nats-agent-core`

```text
Generate a production-grade Go library module named olg-nats-agent-core that implements the following low-level design.

GOALS
- Provide a reusable shared library for NATS bus-facing behavior used by multiple long-running agents.
- The library itself is not a daemon.
- The library must be workload-agnostic and agent-agnostic.
- Use Go with only standard library plus github.com/nats-io/nats.go and github.com/nats-io/nats.go/jetstream.
- Use the newer jetstream package, not the legacy Conn.JetStream() API.

ARCHITECTURE
- Public package name: agentcore
- Internal packages:
  - internal/contract
  - internal/subjects
  - internal/session
  - internal/kv
  - internal/transport
  - internal/registry
  - internal/observe
  - internal/errors

REQUIRED BEHAVIOR
- Connect to configured NATS servers with daemon-friendly reconnect policy.
- Build JetStream handle with jetstream.New(nc).
- Bind or create a Key-Value bucket for desired config.
- Expose Start(ctx), Close(ctx), Health().
- Expose SubmitConfigure, SubmitAction, PublishResult, PublishStatus.
- Expose StoreDesiredConfig, LoadDesiredConfig, WatchDesiredConfig, StartupReconcile.
- Expose RegisterConfigureHandler, RegisterActionHandler, RegisterResultHandler, RegisterStatusHandler.
- Maintain an in-memory subscription registry and restore subscriptions after reconnect.
- Use json.RawMessage for workload payloads.
- Implement transport-level validation only.
- Return typed errors with error codes and retryability metadata.
- Provide logger and metrics hooks.

WIRE CONTRACT
- Configure desired state is authoritative in KV.
- KV is used as a single latest desired-config slot per target.
- The design is UUID-based latest-state, not revision-ordered.
- Configure submit sequence:
  1. validate
  2. store DesiredConfigRecord in KV
  3. publish lightweight ConfigureNotification on cmd.configure.<target>
- Action submit sequence:
  1. validate
  2. publish ActionCommand on cmd.action.<target>.<action>
- Result/status are published on result.<target> and status.<target>.
- Configure result/status must include both:
  - rpc_id for request/response correlation
  - config UUID for the desired config instance that was attempted or applied

CONFIG IDENTITY MODEL
- Desired config UUID is assigned by the cloud-facing side.
- UUID is an opaque identity token used only for equality comparison.
- UUID is not an ordering field, not a version sequence, and not a freshness indicator.
- Agents determine sync by comparing locally applied UUID vs UUID stored in the current desired config record in KV.
- rpc_id is only for request/response correlation.
- Do not make KV revision/history part of the functional contract.

DEFAULT SUBJECTS
- cmd.configure.%s
- cmd.action.%s.%s
- result.%s
- status.%s
- health.%s

DEFAULT KV
- bucket: cfg_desired
- key pattern: desired.%s

PUBLIC TYPES TO IMPLEMENT
- Config and nested configs
- Client
- BaseEnvelope
- ConfigureCommand
- DesiredConfigRecord
- ConfigureNotification
- ActionCommand
- ResultEnvelope
- StatusEnvelope
- StoredDesiredConfig
- SubmissionAck
- HealthSnapshot
- Logger interface
- Metrics interface
- typed Error with Code enum

CRITICAL CONSTRAINTS
- Do not implement workload translation or local execution logic.
- Do not implement reboot/script/trace/rtty logic.
- Do not implement cloud business validation beyond shared envelope sanity validation.
- Do not implement arbitrary historical config retrieval as part of the design contract.
- Graceful shutdown must drain the NATS connection.
- All networked public methods must accept context.Context.
- Keep code modular, testable, and race-safe.

OUTPUT
1. go.mod
2. public package files under agentcore/
3. internal package implementation
4. unit tests
5. integration tests with a real nats-server -js test instance
6. example command-agent, host-agent, and vyos-agent usage programs
7. README with quick-start instructions

CODING STANDARDS
- Prefer explicit structs and small focused files.
- Keep exported APIs documented.
- Wrap errors with context.
- Use mutexes carefully around mutable shared state.
- Avoid global variables.
- Keep handlers thin; assume long-running business work is handed off by the agent.

Now generate the project file-by-file, starting with go.mod and public API types.
