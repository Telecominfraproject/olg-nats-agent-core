package agentcore

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Telecominfraproject/olg-nats-agent-core/internal/registry"
	"github.com/nats-io/nats.go"
)

type testLogger struct {
	entries []string
}

func (l *testLogger) Debug(msg string, kv ...any) { l.entries = append(l.entries, "DEBUG:"+msg) }
func (l *testLogger) Info(msg string, kv ...any)  { l.entries = append(l.entries, "INFO:"+msg) }
func (l *testLogger) Warn(msg string, kv ...any)  { l.entries = append(l.entries, "WARN:"+msg) }
func (l *testLogger) Error(msg string, kv ...any) { l.entries = append(l.entries, "ERROR:"+msg) }

type handlerLatencyCall struct {
	kind    string
	subject string
	d       time.Duration
}

type testMetrics struct {
	mu                  sync.Mutex
	connectCalls        int
	states              []string
	handlerLatencyCalls []handlerLatencyCall
}

func (m *testMetrics) IncConnect(result string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connectCalls++
}

func (m *testMetrics) SetConnectionState(state string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.states = append(m.states, state)
}

func (m *testMetrics) IncPublish(kind, subject, result string)                     {}
func (m *testMetrics) ObservePublishLatency(kind, subject string, d time.Duration) {}
func (m *testMetrics) IncSubscribe(kind, subject, result string)                   {}

func (m *testMetrics) ObserveHandlerLatency(kind, subject string, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlerLatencyCalls = append(m.handlerLatencyCalls, handlerLatencyCall{kind: kind, subject: subject, d: d})
}

func testConfig() Config {
	return Config{
		AgentName: "test-agent",
		Version:   "1.0",
		NATS: NATSConfig{
			Servers:              []string{"nats://localhost:4222"},
			ClientName:           "agentcore-test",
			ConnectTimeout:       3 * time.Second,
			RetryOnFailedConnect: true,
			MaxReconnects:        5,
			ReconnectWait:        500 * time.Millisecond,
			ReconnectBufSize:     1024,
		},
		JetStream: JetStreamConfig{
			Domain:         "test-domain",
			APIPrefix:      "$JS.API",
			DefaultTimeout: 2 * time.Second,
		},
		Subjects: SubjectConfig{
			ConfigurePattern: "cmd.configure.%s",
			ActionPattern:    "cmd.action.%s.%s",
			ResultPattern:    "result.%s",
			StatusPattern:    "status.%s",
			HealthPattern:    "health.%s",
		},
		KV: KVConfig{
			Bucket:           "cfg_desired",
			KeyPattern:       "desired.%s",
			AutoCreateBucket: true,
			History:          5,
			TTL:              30 * time.Minute,
			MaxValueSize:     4096,
			Storage:          "file",
			Replicas:         1,
		},
		Timeouts: TimeoutConfig{
			PublishTimeout:   1 * time.Second,
			SubscribeTimeout: 1 * time.Second,
			KVTimeout:        1 * time.Second,
			ShutdownTimeout:  2 * time.Second,
			HandlerWarnAfter: 500 * time.Millisecond,
		},
		Retry: RetryConfig{
			PublishAttempts: 3,
			PublishBackoff:  100 * time.Millisecond,
		},
		Execution: ExecutionConfig{
			HandlerMode: "sync",
		},
	}
}

func requireErrorCode(t *testing.T, err error, wantCode Code) *Error {
	t.Helper()

	if err == nil {
		t.Fatal("expected non-nil error")
	}

	var got *Error
	if !errors.As(err, &got) {
		t.Fatalf("expected *Error, got %T", err)
	}
	if got.Code != wantCode {
		t.Fatalf("expected error code %q, got %q", wantCode, got.Code)
	}

	return got
}

/*
TC-CLIENT-001
Type: Positive
Title: New creates client with initial bootstrap state
Summary:
Verifies that New(...) constructs a client successfully, initializes health to
StateNew, and preserves the caller-provided config values exactly as passed in.

Validates:
  - constructor succeeds without error
  - returned client is non-nil
  - initial health state is StateNew
  - Config() returns the same public config values supplied to New(...)
*/
func TestNewCreatesClientWithInitialState(t *testing.T) {
	cfg := testConfig()

	client, err := New(cfg)
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	if client == nil {
		t.Fatal("New returned nil client")
	}

	health := client.Health()
	if health.State != StateNew {
		t.Fatalf("expected initial health state %q, got %q", StateNew, health.State)
	}

	gotCfg := client.Config()

	if gotCfg.AgentName != cfg.AgentName {
		t.Fatalf("expected AgentName %q, got %q", cfg.AgentName, gotCfg.AgentName)
	}
	if gotCfg.Version != cfg.Version {
		t.Fatalf("expected Version %q, got %q", cfg.Version, gotCfg.Version)
	}

	if len(gotCfg.NATS.Servers) != len(cfg.NATS.Servers) {
		t.Fatalf("expected %d NATS servers, got %d", len(cfg.NATS.Servers), len(gotCfg.NATS.Servers))
	}
	for i := range cfg.NATS.Servers {
		if gotCfg.NATS.Servers[i] != cfg.NATS.Servers[i] {
			t.Fatalf("expected NATS server %q at index %d, got %q", cfg.NATS.Servers[i], i, gotCfg.NATS.Servers[i])
		}
	}

	if gotCfg.NATS.ClientName != cfg.NATS.ClientName {
		t.Fatalf("expected ClientName %q, got %q", cfg.NATS.ClientName, gotCfg.NATS.ClientName)
	}
	if gotCfg.NATS.ConnectTimeout != cfg.NATS.ConnectTimeout {
		t.Fatalf("expected ConnectTimeout %v, got %v", cfg.NATS.ConnectTimeout, gotCfg.NATS.ConnectTimeout)
	}
	if gotCfg.NATS.RetryOnFailedConnect != cfg.NATS.RetryOnFailedConnect {
		t.Fatalf("expected RetryOnFailedConnect %v, got %v", cfg.NATS.RetryOnFailedConnect, gotCfg.NATS.RetryOnFailedConnect)
	}
	if gotCfg.NATS.MaxReconnects != cfg.NATS.MaxReconnects {
		t.Fatalf("expected MaxReconnects %d, got %d", cfg.NATS.MaxReconnects, gotCfg.NATS.MaxReconnects)
	}
	if gotCfg.NATS.ReconnectWait != cfg.NATS.ReconnectWait {
		t.Fatalf("expected ReconnectWait %v, got %v", cfg.NATS.ReconnectWait, gotCfg.NATS.ReconnectWait)
	}
	if gotCfg.NATS.ReconnectBufSize != cfg.NATS.ReconnectBufSize {
		t.Fatalf("expected ReconnectBufSize %d, got %d", cfg.NATS.ReconnectBufSize, gotCfg.NATS.ReconnectBufSize)
	}

	if gotCfg.JetStream.Domain != cfg.JetStream.Domain {
		t.Fatalf("expected JetStream Domain %q, got %q", cfg.JetStream.Domain, gotCfg.JetStream.Domain)
	}
	if gotCfg.JetStream.APIPrefix != cfg.JetStream.APIPrefix {
		t.Fatalf("expected JetStream APIPrefix %q, got %q", cfg.JetStream.APIPrefix, gotCfg.JetStream.APIPrefix)
	}
	if gotCfg.JetStream.DefaultTimeout != cfg.JetStream.DefaultTimeout {
		t.Fatalf("expected JetStream DefaultTimeout %v, got %v", cfg.JetStream.DefaultTimeout, gotCfg.JetStream.DefaultTimeout)
	}

	if gotCfg.Subjects.ConfigurePattern != cfg.Subjects.ConfigurePattern {
		t.Fatalf("expected ConfigurePattern %q, got %q", cfg.Subjects.ConfigurePattern, gotCfg.Subjects.ConfigurePattern)
	}
	if gotCfg.Subjects.ActionPattern != cfg.Subjects.ActionPattern {
		t.Fatalf("expected ActionPattern %q, got %q", cfg.Subjects.ActionPattern, gotCfg.Subjects.ActionPattern)
	}
	if gotCfg.Subjects.ResultPattern != cfg.Subjects.ResultPattern {
		t.Fatalf("expected ResultPattern %q, got %q", cfg.Subjects.ResultPattern, gotCfg.Subjects.ResultPattern)
	}
	if gotCfg.Subjects.StatusPattern != cfg.Subjects.StatusPattern {
		t.Fatalf("expected StatusPattern %q, got %q", cfg.Subjects.StatusPattern, gotCfg.Subjects.StatusPattern)
	}
	if gotCfg.Subjects.HealthPattern != cfg.Subjects.HealthPattern {
		t.Fatalf("expected HealthPattern %q, got %q", cfg.Subjects.HealthPattern, gotCfg.Subjects.HealthPattern)
	}

	if gotCfg.KV.Bucket != cfg.KV.Bucket {
		t.Fatalf("expected KV Bucket %q, got %q", cfg.KV.Bucket, gotCfg.KV.Bucket)
	}
	if gotCfg.KV.KeyPattern != cfg.KV.KeyPattern {
		t.Fatalf("expected KV KeyPattern %q, got %q", cfg.KV.KeyPattern, gotCfg.KV.KeyPattern)
	}
	if gotCfg.KV.AutoCreateBucket != cfg.KV.AutoCreateBucket {
		t.Fatalf("expected AutoCreateBucket %v, got %v", cfg.KV.AutoCreateBucket, gotCfg.KV.AutoCreateBucket)
	}
	if gotCfg.KV.History != cfg.KV.History {
		t.Fatalf("expected History %d, got %d", cfg.KV.History, gotCfg.KV.History)
	}
	if gotCfg.KV.TTL != cfg.KV.TTL {
		t.Fatalf("expected TTL %v, got %v", cfg.KV.TTL, gotCfg.KV.TTL)
	}
	if gotCfg.KV.MaxValueSize != cfg.KV.MaxValueSize {
		t.Fatalf("expected MaxValueSize %d, got %d", cfg.KV.MaxValueSize, gotCfg.KV.MaxValueSize)
	}
	if gotCfg.KV.Storage != cfg.KV.Storage {
		t.Fatalf("expected Storage %q, got %q", cfg.KV.Storage, gotCfg.KV.Storage)
	}
	if gotCfg.KV.Replicas != cfg.KV.Replicas {
		t.Fatalf("expected Replicas %d, got %d", cfg.KV.Replicas, gotCfg.KV.Replicas)
	}

	if gotCfg.Timeouts.PublishTimeout != cfg.Timeouts.PublishTimeout {
		t.Fatalf("expected PublishTimeout %v, got %v", cfg.Timeouts.PublishTimeout, gotCfg.Timeouts.PublishTimeout)
	}
	if gotCfg.Timeouts.SubscribeTimeout != cfg.Timeouts.SubscribeTimeout {
		t.Fatalf("expected SubscribeTimeout %v, got %v", cfg.Timeouts.SubscribeTimeout, gotCfg.Timeouts.SubscribeTimeout)
	}
	if gotCfg.Timeouts.KVTimeout != cfg.Timeouts.KVTimeout {
		t.Fatalf("expected KVTimeout %v, got %v", cfg.Timeouts.KVTimeout, gotCfg.Timeouts.KVTimeout)
	}
	if gotCfg.Timeouts.ShutdownTimeout != cfg.Timeouts.ShutdownTimeout {
		t.Fatalf("expected ShutdownTimeout %v, got %v", cfg.Timeouts.ShutdownTimeout, gotCfg.Timeouts.ShutdownTimeout)
	}
	if gotCfg.Timeouts.HandlerWarnAfter != cfg.Timeouts.HandlerWarnAfter {
		t.Fatalf("expected HandlerWarnAfter %v, got %v", cfg.Timeouts.HandlerWarnAfter, gotCfg.Timeouts.HandlerWarnAfter)
	}

	if gotCfg.Retry.PublishAttempts != cfg.Retry.PublishAttempts {
		t.Fatalf("expected PublishAttempts %d, got %d", cfg.Retry.PublishAttempts, gotCfg.Retry.PublishAttempts)
	}
	if gotCfg.Retry.PublishBackoff != cfg.Retry.PublishBackoff {
		t.Fatalf("expected PublishBackoff %v, got %v", cfg.Retry.PublishBackoff, gotCfg.Retry.PublishBackoff)
	}

	if gotCfg.Execution.HandlerMode != cfg.Execution.HandlerMode {
		t.Fatalf("expected HandlerMode %q, got %q", cfg.Execution.HandlerMode, gotCfg.Execution.HandlerMode)
	}
}

/*
TC-CLIENT-002
Type: Positive
Title: Health returns the expected zeroed bootstrap snapshot
Summary:
Verifies that a newly created client exposes the expected initial health state
and zero-value health counters/flags before any runtime transport work exists.

Validates:
  - Health().State starts at StateNew
  - JetStreamReady and KVReady are false
  - subscription counters start at zero
  - LastError and ConnectedURL are empty
*/
func TestHealthReturnsBootstrapSnapshot(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	got := client.Health()

	if got.State != StateNew {
		t.Fatalf("expected state %q, got %q", StateNew, got.State)
	}
	if got.JetStreamReady {
		t.Fatal("expected JetStreamReady to be false at bootstrap")
	}
	if got.KVReady {
		t.Fatal("expected KVReady to be false at bootstrap")
	}
	if got.RegisteredSubscriptions != 0 {
		t.Fatalf("expected RegisteredSubscriptions 0, got %d", got.RegisteredSubscriptions)
	}
	if got.ActiveSubscriptions != 0 {
		t.Fatalf("expected ActiveSubscriptions 0, got %d", got.ActiveSubscriptions)
	}
	if got.LastError != "" {
		t.Fatalf("expected empty LastError, got %q", got.LastError)
	}
	if got.ConnectedURL != "" {
		t.Fatalf("expected empty ConnectedURL, got %q", got.ConnectedURL)
	}
}

/*
TC-CLIENT-003
Type: Positive
Title: Constructor applies optional logger, metrics, clock, and error sink hooks
Summary:
Verifies that supported Phase 1 constructor options are accepted and stored on
the client facade without requiring transport/runtime setup.

Validates:
  - WithLogger stores the provided logger
  - WithMetrics stores the provided metrics hook
  - WithClock stores the provided clock function
  - WithErrorSink stores the provided async error sink hook
*/
func TestNewAppliesConstructorOptions(t *testing.T) {
	logger := &testLogger{}
	metrics := &testMetrics{}
	fixedNow := func() time.Time { return time.Unix(1700000000, 0).UTC() }

	sinkCalls := 0
	var sinkErr error
	sink := func(err error) {
		sinkCalls++
		sinkErr = err
	}

	client, err := New(
		testConfig(),
		WithLogger(logger),
		WithMetrics(metrics),
		WithClock(fixedNow),
		WithErrorSink(sink),
	)
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	if client.options.logger != logger {
		t.Fatal("expected logger option to be stored on client")
	}
	if client.options.metrics != metrics {
		t.Fatal("expected metrics option to be stored on client")
	}
	if client.options.now == nil {
		t.Fatal("expected clock option to be stored on client")
	}
	if got := client.options.now(); !got.Equal(fixedNow()) {
		t.Fatalf("expected stored clock value %v, got %v", fixedNow(), got)
	}
	if client.options.errorSink == nil {
		t.Fatal("expected error sink option to be stored on client")
	}

	wantErr := errors.New("sink-test")
	client.options.errorSink(wantErr)
	if sinkCalls != 1 {
		t.Fatalf("expected error sink to be called once, got %d", sinkCalls)
	}
	if !errors.Is(sinkErr, wantErr) {
		t.Fatalf("expected sink error %v, got %v", wantErr, sinkErr)
	}
}

/*
TC-CLIENT-004
Type: Negative
Title: WithClock rejects a nil clock function
Summary:
Verifies that bootstrap option validation rejects a nil clock override and
returns a typed validation error.

Validates:
  - New(...) fails when WithClock(nil) is supplied
  - returned error is typed as *Error
  - error code is CodeValidation
  - error op is with_clock
*/
func TestWithClockRejectsNilFunction(t *testing.T) {
	_, err := New(testConfig(), WithClock(nil))
	if err == nil {
		t.Fatal("expected error from WithClock(nil), got nil")
	}

	var got *Error
	if !errors.As(err, &got) {
		t.Fatalf("expected *Error, got %T", err)
	}
	if got.Code != CodeValidation {
		t.Fatalf("expected error code %q, got %q", CodeValidation, got.Code)
	}
	if got.Op != "with_clock" {
		t.Fatalf("expected error op %q, got %q", "with_clock", got.Op)
	}
}

/*
TC-CLIENT-005
Type: Negative
Title: Start rejects canceled context with typed connection error
Summary:
Verifies that Start(...) fails fast when called with a canceled context and
returns the expected typed connection failure.

Validates:
  - Start(...) returns a non-nil *Error
  - error code is CodeConnectionFailed
  - error op is start
  - health state remains StateNew after fast-fail
*/
func TestStartReturnsConnectionFailedOnCanceledContext(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = client.Start(ctx)
	got := requireErrorCode(t, err, CodeConnectionFailed)
	if got.Op != "start" {
		t.Fatalf("expected error op %q, got %q", "start", got.Op)
	}

	if got := client.Health().State; got != StateNew {
		t.Fatalf("expected health state %q, got %q", StateNew, got)
	}
}

/*
TC-CLIENT-006
Type: Negative
Title: Close is safe before Start and transitions health to closed
Summary:
Verifies that Close(...) is idempotent and safe before Start, and updates
health to StateClosed.

Validates:
  - Close(...) returns nil error
  - health state transitions to StateClosed
*/
func TestCloseBeforeStartMovesHealthToClosed(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	err = client.Close(context.Background())
	if err != nil {
		t.Fatalf("expected nil close error, got %v", err)
	}

	if got := client.Health().State; got != StateClosed {
		t.Fatalf("expected health state %q, got %q", StateClosed, got)
	}
}

/*
TC-CLIENT-007
Type: Negative
Title: Runtime-backed APIs return disconnected before start
Summary:
Verifies that runtime-backed desired-config APIs fail clearly with
CodeDisconnected before Start. This includes send/publish APIs that require an
active runtime connection.

Validates:
  - runtime-backed desired-config APIs return CodeDisconnected when not started
  - send/publish APIs return CodeDisconnected when not started
  - handler registration APIs succeed pre-start and store deferred registration state
*/
func TestPhase4RuntimeAndDeferredMethodsReturnExpectedErrors(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	now := time.Unix(1700000001, 0).UTC()

	cfgCmd := ConfigureCommand{
		Version:   "1.0",
		RPCID:     "rpc-config-1",
		Target:    "vyos",
		UUID:      "cfg-1",
		Payload:   json.RawMessage(`{"hostname":"router-1"}`),
		Timestamp: now,
	}
	actionCmd := ActionCommand{
		Version:     "1.0",
		RPCID:       "rpc-action-1",
		Target:      "vyos",
		CommandType: "action",
		Action:      "trace",
		Payload:     json.RawMessage(`{"destination":"8.8.8.8"}`),
		Timestamp:   now,
	}
	resultMsg := ResultEnvelope{
		Version:     "1.0",
		RPCID:       "rpc-result-1",
		Target:      "vyos",
		CommandType: "configure",
		UUID:        "cfg-1",
		Result:      "success",
		Timestamp:   now,
	}
	statusMsg := StatusEnvelope{
		Version:   "1.0",
		RPCID:     "rpc-status-1",
		Target:    "vyos",
		Status:    "running",
		Timestamp: now,
	}
	record := DesiredConfigRecord{
		Version:   "1.0",
		RPCID:     "rpc-store-1",
		Target:    "vyos",
		UUID:      "cfg-2",
		Payload:   json.RawMessage(`{"interfaces":[]}`),
		Timestamp: now,
	}

	runtimeTests := []struct {
		name string
		call func(*Client) error
	}{
		{
			name: "StoreDesiredConfig",
			call: func(c *Client) error {
				stored, err := c.StoreDesiredConfig(context.Background(), record)
				if stored != nil {
					t.Fatalf("expected nil StoredDesiredConfig, got %#v", stored)
				}
				return err
			},
		},
		{
			name: "LoadDesiredConfig",
			call: func(c *Client) error {
				stored, err := c.LoadDesiredConfig(context.Background(), "vyos")
				if stored != nil {
					t.Fatalf("expected nil StoredDesiredConfig, got %#v", stored)
				}
				return err
			},
		},
		{
			name: "WatchDesiredConfig",
			call: func(c *Client) error {
				stop, err := c.WatchDesiredConfig(context.Background(), "vyos", func(context.Context, StoredDesiredConfig) error {
					return nil
				})
				if stop != nil {
					t.Fatal("expected nil StopFunc when runtime is disconnected")
				}
				return err
			},
		},
		{
			name: "StartupReconcile",
			call: func(c *Client) error {
				stored, err := c.StartupReconcile(context.Background(), "vyos")
				if stored != nil {
					t.Fatalf("expected nil StoredDesiredConfig, got %#v", stored)
				}
				return err
			},
		},
	}

	for _, tc := range runtimeTests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call(client)
			requireErrorCode(t, err, CodeDisconnected)
		})
	}

	sendTests := []struct {
		name string
		call func(*Client) error
	}{
		{
			name: "SubmitConfigure",
			call: func(c *Client) error {
				ack, err := c.SubmitConfigure(context.Background(), cfgCmd)
				if ack != nil {
					t.Fatalf("expected nil SubmissionAck, got %#v", ack)
				}
				return err
			},
		},
		{
			name: "SubmitAction",
			call: func(c *Client) error {
				ack, err := c.SubmitAction(context.Background(), actionCmd)
				if ack != nil {
					t.Fatalf("expected nil SubmissionAck, got %#v", ack)
				}
				return err
			},
		},
		{
			name: "PublishResult",
			call: func(c *Client) error {
				return c.PublishResult(context.Background(), resultMsg)
			},
		},
		{
			name: "PublishStatus",
			call: func(c *Client) error {
				return c.PublishStatus(context.Background(), statusMsg)
			},
		},
	}

	for _, tc := range sendTests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call(client)
			requireErrorCode(t, err, CodeDisconnected)
		})
	}

	registrationTests := []struct {
		name string
		call func(*Client) error
	}{
		{
			name: "RegisterConfigureHandler",
			call: func(c *Client) error {
				return c.RegisterConfigureHandler("vyos", func(context.Context, ConfigureNotification) error { return nil })
			},
		},
		{
			name: "RegisterActionHandler",
			call: func(c *Client) error {
				return c.RegisterActionHandler("vyos", "trace", func(context.Context, ActionCommand) error { return nil })
			},
		},
		{
			name: "RegisterResultHandler",
			call: func(c *Client) error {
				return c.RegisterResultHandler("vyos", func(context.Context, ResultEnvelope) error { return nil })
			},
		},
		{
			name: "RegisterStatusHandler",
			call: func(c *Client) error {
				return c.RegisterStatusHandler("vyos", func(context.Context, StatusEnvelope) error { return nil })
			},
		},
	}

	for _, tc := range registrationTests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(client); err != nil {
				t.Fatalf("expected nil pre-start registration error, got %v", err)
			}
		})
	}

	if got := client.Health().RegisteredSubscriptions; got != 4 {
		t.Fatalf("expected RegisteredSubscriptions %d, got %d", 4, got)
	}
	if got := client.Health().ActiveSubscriptions; got != 0 {
		t.Fatalf("expected ActiveSubscriptions %d before start, got %d", 0, got)
	}
}

/*
TC-CLIENT-008
Type: Positive
Title: New tolerates a nil constructor option
Summary:
Verifies that New(...) safely ignores a nil option entry in the variadic option
list. This is intentional bootstrap behavior in the current implementation.

Validates:
  - New(cfg, nil) does not panic
  - New(cfg, nil) does not return an error
  - returned client is non-nil
  - initial health state remains StateNew
*/
func TestNewToleratesNilOption(t *testing.T) {
	client, err := New(testConfig(), nil)
	if err != nil {
		t.Fatalf("New returned unexpected error for nil option: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client, got nil")
	}

	if got := client.Health().State; got != StateNew {
		t.Fatalf("expected initial health state %q, got %q", StateNew, got)
	}
}

/*
TC-CLIENT-009
Type: Negative
Title: WatchDesiredConfig rejects a nil handler
Summary:
Verifies that WatchDesiredConfig(...) fails fast when called with a nil
DesiredConfigWatchHandler. The public facade should reject the call before any
watch is created or delegated to the runtime KV layer.

Validates:
  - WatchDesiredConfig(ctx, target, nil) returns a non-nil error
  - returned StopFunc is nil
  - error code is CodeValidation
  - error op is watch_desired_config
*/
func TestWatchDesiredConfigRejectsNilHandler(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	stop, err := client.WatchDesiredConfig(context.Background(), "vyos", nil)
	if stop != nil {
		t.Fatalf("expected nil StopFunc, got non-nil")
	}

	got := requireErrorCode(t, err, CodeValidation)
	if got.Op != "watch_desired_config" {
		t.Fatalf("expected error op %q, got %q", "watch_desired_config", got.Op)
	}
}

/*
TC-CLIENT-010
Type: Negative
Title: StartupReconcile delegates to desired-config load path
Summary:
Verifies that StartupReconcile(...) uses the same runtime-backed desired-config
load behavior as LoadDesiredConfig(...) before Start.

Validates:
  - StartupReconcile returns CodeDisconnected before Start
  - returned error op matches load_desired_config delegation path
*/
func TestStartupReconcileDelegatesToLoadDesiredConfigPath(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	stored, err := client.StartupReconcile(context.Background(), "vyos")
	if stored != nil {
		t.Fatalf("expected nil StoredDesiredConfig, got %#v", stored)
	}

	got := requireErrorCode(t, err, CodeDisconnected)
	if got.Op != "key_value" {
		t.Fatalf("expected error op %q, got %q", "key_value", got.Op)
	}
}

/*
TC-CLIENT-011
Type: Negative
Title: Close returns unified CodeShutdown error containing joined failures
Summary:
Verifies that Client.Close(...) combines multiple internal cleanup failures
(deactivating subscriptions, stopping watches, closing session) using errors.Join
and wraps them in a single unified agentcore.Error with CodeShutdown.
*/
func TestClientCloseUnifiesErrors(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	mockSubErr := errors.New("deactivate-subs-fail")
	mockSessionErr := errors.New("close-session-fail")

	client.deactivateAllSubscriptionsFn = func(op string) error {
		return mockSubErr
	}
	client.closeSessionFn = func(ctx context.Context) error {
		return mockSessionErr
	}

	err = client.Close(context.Background())
	if err == nil {
		t.Fatal("expected non-nil error from Close")
	}

	got := requireErrorCode(t, err, CodeShutdown)
	if got.Op != "close" {
		t.Fatalf("expected error op %q, got %q", "close", got.Op)
	}
	if got.Message != "client close operation encountered errors" {
		t.Fatalf("unexpected error message: %q", got.Message)
	}
	if got.Retryable != false {
		t.Fatal("expected Retryable to be false")
	}

	// Verify that both mock errors are wrapped and discoverable via errors.Is/As or Unwrap
	unwrapped := got.Unwrap()
	if unwrapped == nil {
		t.Fatal("expected unwrapped error to be non-nil")
	}

	if !errors.Is(unwrapped, mockSubErr) {
		t.Fatalf("expected unwrapped error to contain %v", mockSubErr)
	}
	if !errors.Is(unwrapped, mockSessionErr) {
		t.Fatalf("expected unwrapped error to contain %v", mockSessionErr)
	}
}

/*
TC-CLIENT-012
Type: Negative
Title: Close returns unified CodeShutdown even when only one internal cleanup operation fails
Summary:
Verifies that Client.Close(...) returns a unified agentcore.Error with CodeShutdown even when only a single cleanup operation fails.
*/
func TestClientCloseUnifiesSingleError(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	mockSubErr := errors.New("deactivate-subs-fail")
	client.deactivateAllSubscriptionsFn = func(op string) error {
		return mockSubErr
	}

	err = client.Close(context.Background())
	if err == nil {
		t.Fatal("expected non-nil error from Close")
	}

	got := requireErrorCode(t, err, CodeShutdown)
	if got.Op != "close" {
		t.Fatalf("expected error op %q, got %q", "close", got.Op)
	}
	if !errors.Is(got.Unwrap(), mockSubErr) {
		t.Fatalf("expected unwrapped error to contain %v", mockSubErr)
	}
}

/*
TC-CLIENT-013
Type: Positive
Title: WithReconnectHandler registers correctly and fires on reconnect
Summary:
Verifies that WithReconnectHandler registers the handler in options and invokes
it when onSessionReconnected runs during connection recovery.

Validates:
  - constructor accepts WithReconnectHandler option
  - reconnect handler fires when onSessionReconnected is called with callbacks enabled
*/
func TestWithReconnectHandlerOption(t *testing.T) {
	fired := false
	handler := func() {
		fired = true
	}

	client, err := New(testConfig(), WithReconnectHandler(handler))
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	if client.options.reconnectHandler == nil {
		t.Fatal("expected reconnectHandler option to be stored on client")
	}

	// Enable callbacks to simulate active running state
	client.callbacksEnabled.Store(true)

	// Invoke the callback triggering onSessionReconnected
	client.onSessionReconnected()

	if !fired {
		t.Fatal("expected reconnect handler to be fired")
	}
}

/*
TC-CLIENT-014
Type: Negative
Title: Reconnect handler does not fire when callbacks are disabled
Summary:
Verifies that the reconnect handler is not invoked if callbacks are disabled.

Validates:
  - reconnect handler is not fired when callbacksEnabled is false
*/
func TestWithReconnectHandler_CallbacksDisabled(t *testing.T) {
	fired := false
	handler := func() {
		fired = true
	}

	client, err := New(testConfig(), WithReconnectHandler(handler))
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	// Keep callbacks disabled
	client.callbacksEnabled.Store(false)

	client.onSessionReconnected()

	if fired {
		t.Fatal("expected reconnect handler not to fire when callbacks are disabled")
	}
}

/*
TC-CLIENT-015
Type: Positive
Title: Reconnect handler is safe to run when nil
Summary:
Verifies that the client does not panic when onSessionReconnected is called without a registered reconnect handler.

Validates:
  - client does not panic when reconnectHandler option is nil (unregistered)
*/
func TestWithReconnectHandler_NoHandlerRegistered(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	client.callbacksEnabled.Store(true)

	// Invoke onSessionReconnected without registering a reconnect handler.
	// This should run without panicking.
	client.onSessionReconnected()
}

/*
TC-CLIENT-016
Type: Negative
Title: Reconnect handler does not fire if subscription restore fails
Summary:
Verifies that the reconnect handler is not invoked when one or more registered subscriptions fail to restore.

Validates:
  - reconnect handler is not fired if restoreAllRegisteredSubscriptions returns a non-nil error
*/
func TestWithReconnectHandler_SubscriptionRestoreFailed(t *testing.T) {
	fired := false
	handler := func() {
		fired = true
	}

	client, err := New(testConfig(), WithReconnectHandler(handler))
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	// Register a configure handler (which creates a subscription record)
	err = client.RegisterConfigureHandler("vyos", func(context.Context, ConfigureNotification) error { return nil })
	if err != nil {
		t.Fatalf("failed to register handler: %v", err)
	}

	client.callbacksEnabled.Store(true)

	// Since we are not started, client.session lacks an active connection,
	// so attempting to restore the subscription will return a connection/disconnected error.
	// This will cause restoreAllRegisteredSubscriptions to return a non-nil error.
	client.onSessionReconnected()

	if fired {
		t.Fatal("expected reconnect handler not to fire because subscription restore failed")
	}
}

/*
TC-CLIENT-017
Type: Positive
Title: Reconnect handler panic is caught and reported to error sink
Summary:
Verifies that client recovers gracefully from a panicking reconnect handler,
preventing application crash and propagating the panic as a formatted error
to the registered errorSink.

Validates:
  - client catches panic thrown by reconnect handler
  - caught panic does not abort execution
  - formatted error is forwarded to options.errorSink
*/
func TestWithReconnectHandlerPanicSafety(t *testing.T) {
	panicMsg := "simulated database reconnect failure"
	var caughtErr error
	sink := func(err error) {
		caughtErr = err
	}

	handler := func() {
		panic(panicMsg)
	}

	client, err := New(testConfig(), WithReconnectHandler(handler), WithErrorSink(sink))
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	client.callbacksEnabled.Store(true)

	// Invoke the callback triggering onSessionReconnected
	// This function must run to completion without panicking.
	client.onSessionReconnected()

	if caughtErr == nil {
		t.Fatal("expected panic to be reported to error sink, got nil error")
	}

	expectedMsg := "reconnect handler panicked: simulated database reconnect failure"
	if caughtErr.Error() != expectedMsg {
		t.Fatalf("expected error message %q, got %q", expectedMsg, caughtErr.Error())
	}
}

/*
TC-CLIENT-018
Type: Positive
Title: Handler context has correct lifecycle transitions
Summary:
Verifies that handlerContext() returns a pre-canceled context when the client is not yet started,
an active context after starting, a canceled context after closing, and a new active context after restart.
Validates:
  - handlerContext() returns canceled context pre-start
  - handlerContext() returns active context post-start
  - handlerContext() returns canceled context post-close
  - handlerContext() returns new active context on restart
*/
func TestHandlerContextLifecycle(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	// 1. Pre-start: should return pre-canceled context
	ctx := client.handlerContext()
	if ctx == nil {
		t.Fatal("expected non-nil context pre-start")
	}
	if ctx.Err() == nil {
		t.Fatal("expected pre-start context to be canceled")
	}

	// 2. Start: should return active context
	client.ensureHandlerContext()
	ctx = client.handlerContext()
	if ctx == nil {
		t.Fatal("expected non-nil context after ensure")
	}
	if ctx.Err() != nil {
		t.Fatalf("expected context to be active after ensure, got error: %v", ctx.Err())
	}

	// 3. Cancel: should return canceled context (not nil)
	client.cancelHandlerContext()
	ctx = client.handlerContext()
	if ctx == nil {
		t.Fatal("expected non-nil context after cancel")
	}
	if ctx.Err() == nil {
		t.Fatal("expected context to be canceled after cancel")
	}

	// 4. Restart (ensure again): should return new active context
	client.ensureHandlerContext()
	ctx2 := client.handlerContext()
	if ctx2 == nil {
		t.Fatal("expected non-nil context after second ensure")
	}
	if ctx2.Err() != nil {
		t.Fatalf("expected context to be active after second ensure, got error: %v", ctx2.Err())
	}
	if ctx == ctx2 {
		t.Fatal("expected a new context instance on restart")
	}
}

/*
TC-CLIENT-019
Type: Positive
Title: Inbound callbacks invoke ObserveHandlerLatency with correct parameters
Summary:
Verifies that the configure, action, result, and status callbacks invoke the ObserveHandlerLatency
metrics hook with the correct kind, subject, and latency duration.
Validates:
  - bindConfigureCallback calls ObserveHandlerLatency
  - bindActionCallback calls ObserveHandlerLatency
  - bindResultCallback calls ObserveHandlerLatency
  - bindStatusCallback calls ObserveHandlerLatency
*/
func TestInboundHandlerLatencyMetrics(t *testing.T) {
	metrics := &testMetrics{}
	client, err := New(testConfig(), WithMetrics(metrics))
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	// Enable callbacks
	client.callbacksEnabled.Store(true)

	// 1. Configure Callback
	cfgCb := client.bindConfigureCallback(func(context.Context, ConfigureNotification) error {
		return nil
	})
	cfgNotif := ConfigureNotification{
		Version:     "1.0",
		RPCID:       "rpc-1",
		Target:      "vyos",
		CommandType: "configure",
		UUID:        "uuid-1",
		KVBucket:    "bucket-1",
		KVKey:       "key-1",
		Timestamp:   time.Now(),
	}
	cfgNotifBytes, _ := json.Marshal(cfgNotif)
	cfgCb(&nats.Msg{Subject: "cmd.configure.vyos", Data: cfgNotifBytes})

	// 2. Action Callback
	actCb := client.bindActionCallback(func(context.Context, ActionCommand) error {
		return nil
	})
	actCmd := ActionCommand{
		Version:     "1.0",
		RPCID:       "rpc-2",
		Target:      "vyos",
		CommandType: "action",
		Action:      "reboot",
		Timestamp:   time.Now(),
	}
	actCmdBytes, _ := json.Marshal(actCmd)
	actCb(&nats.Msg{Subject: "cmd.action.vyos.reboot", Data: actCmdBytes})

	// 3. Result Callback
	resCb := client.bindResultCallback(func(context.Context, ResultEnvelope) error {
		return nil
	})
	resEnv := ResultEnvelope{
		Version:   "1.0",
		RPCID:     "rpc-3",
		Target:    "vyos",
		Result:    "success",
		Timestamp: time.Now(),
	}
	resEnvBytes, _ := json.Marshal(resEnv)
	resCb(&nats.Msg{Subject: "result.vyos", Data: resEnvBytes})

	// 4. Status Callback
	statCb := client.bindStatusCallback(func(context.Context, StatusEnvelope) error {
		return nil
	})
	statEnv := StatusEnvelope{
		Version:   "1.0",
		RPCID:     "rpc-4",
		Target:    "vyos",
		Status:    "running",
		Timestamp: time.Now(),
	}
	statEnvBytes, _ := json.Marshal(statEnv)
	statCb(&nats.Msg{Subject: "status.vyos", Data: statEnvBytes})

	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	if len(metrics.handlerLatencyCalls) != 4 {
		t.Fatalf("expected 4 ObserveHandlerLatency calls, got %d", len(metrics.handlerLatencyCalls))
	}

	expectedKinds := []string{
		string(registry.KindConfigure),
		string(registry.KindAction),
		string(registry.KindResult),
		string(registry.KindStatus),
	}
	expectedSubjects := []string{
		"cmd.configure.vyos",
		"cmd.action.vyos.reboot",
		"result.vyos",
		"status.vyos",
	}

	for i, call := range metrics.handlerLatencyCalls {
		if call.kind != expectedKinds[i] {
			t.Errorf("call %d: expected kind %q, got %q", i, expectedKinds[i], call.kind)
		}
		if call.subject != expectedSubjects[i] {
			t.Errorf("call %d: expected subject %q, got %q", i, expectedSubjects[i], call.subject)
		}
		if call.d <= 0 {
			t.Errorf("call %d: expected positive duration, got %v", i, call.d)
		}
	}
}
