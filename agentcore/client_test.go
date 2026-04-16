package agentcore

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type testLogger struct {
	entries []string
}

func (l *testLogger) Debug(msg string, kv ...any) { l.entries = append(l.entries, "DEBUG:"+msg) }
func (l *testLogger) Info(msg string, kv ...any)  { l.entries = append(l.entries, "INFO:"+msg) }
func (l *testLogger) Warn(msg string, kv ...any)  { l.entries = append(l.entries, "WARN:"+msg) }
func (l *testLogger) Error(msg string, kv ...any) { l.entries = append(l.entries, "ERROR:"+msg) }

type testMetrics struct {
	connectCalls int
	states       []string
}

func (m *testMetrics) IncConnect(result string)                           { m.connectCalls++ }
func (m *testMetrics) SetConnectionState(state string)                    { m.states = append(m.states, state) }
func (m *testMetrics) IncPublish(kind, subject, result string)            {}
func (m *testMetrics) ObservePublishLatency(kind, subject string, d time.Duration) {}
func (m *testMetrics) IncSubscribe(kind, subject, result string)          {}
func (m *testMetrics) IncKV(op, result string)                            {}

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

func requireNotImplementedError(t *testing.T, err error, wantOp string) {
	t.Helper()

	if err == nil {
		t.Fatal("expected non-nil error")
	}

	var got *Error
	if !errors.As(err, &got) {
		t.Fatalf("expected *Error, got %T", err)
	}
	if got.Code != CodeNotImplemented {
		t.Fatalf("expected error code %q, got %q", CodeNotImplemented, got.Code)
	}
	if got.Op != wantOp {
		t.Fatalf("expected error op %q, got %q", wantOp, got.Op)
	}
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
Title: Start returns bootstrap not-implemented error
Summary:
Verifies that Start(...) remains a Phase 1 stub and returns the expected typed
not-implemented error while updating the health state to connecting.

Validates:
  - Start(...) returns a non-nil *Error
  - error code is CodeNotImplemented
  - error op is start
  - health state transitions to StateConnecting
*/
func TestStartReturnsNotImplementedAndMovesHealthToConnecting(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	err = client.Start(context.Background())
	requireNotImplementedError(t, err, "start")

	if got := client.Health().State; got != StateConnecting {
		t.Fatalf("expected health state %q, got %q", StateConnecting, got)
	}
}

/*
TC-CLIENT-006
Type: Negative
Title: Close returns bootstrap not-implemented error
Summary:
Verifies that Close(...) remains a Phase 1 stub and returns the expected typed
not-implemented error while updating the health state to draining.

Validates:
  - Close(...) returns a non-nil *Error
  - error code is CodeNotImplemented
  - error op is close
  - health state transitions to StateDraining
*/
func TestCloseReturnsNotImplementedAndMovesHealthToDraining(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	err = client.Close(context.Background())
	requireNotImplementedError(t, err, "close")

	if got := client.Health().State; got != StateDraining {
		t.Fatalf("expected health state %q, got %q", StateDraining, got)
	}
}

/*
TC-CLIENT-007
Type: Negative
Title: Bootstrap stubs return CodeNotImplemented across deferred APIs
Summary:
Verifies that the Phase 1 APIs intentionally deferred to later transport/runtime
phases return consistent typed not-implemented errors and no success value.

Validates:
  - each deferred public API returns a non-nil *Error
  - each returned *Error has CodeNotImplemented
  - each error carries the correct operation name
  - methods that return values do not return success payloads in Phase 1
*/
func TestBootstrapStubMethodsReturnCodeNotImplemented(t *testing.T) {
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

	tests := []struct {
		name string
		op   string
		call func(*Client) error
	}{
		{
			name: "SubmitConfigure",
			op:   "submit_configure",
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
			op:   "submit_action",
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
			op:   "publish_result",
			call: func(c *Client) error {
				return c.PublishResult(context.Background(), resultMsg)
			},
		},
		{
			name: "PublishStatus",
			op:   "publish_status",
			call: func(c *Client) error {
				return c.PublishStatus(context.Background(), statusMsg)
			},
		},
		{
			name: "StoreDesiredConfig",
			op:   "store_desired_config",
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
			op:   "load_desired_config",
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
			op:   "watch_desired_config",
			call: func(c *Client) error {
				stop, err := c.WatchDesiredConfig(context.Background(), "vyos", func(context.Context, StoredDesiredConfig) error {
					return nil
				})
				if stop != nil {
					t.Fatal("expected nil StopFunc for bootstrap watch")
				}
				return err
			},
		},
		{
			name: "StartupReconcile",
			op:   "startup_reconcile",
			call: func(c *Client) error {
				stored, err := c.StartupReconcile(context.Background(), "vyos")
				if stored != nil {
					t.Fatalf("expected nil StoredDesiredConfig, got %#v", stored)
				}
				return err
			},
		},
		{
			name: "RegisterConfigureHandler",
			op:   "register_configure_handler",
			call: func(c *Client) error {
				return c.RegisterConfigureHandler("vyos", func(context.Context, ConfigureNotification) error { return nil })
			},
		},
		{
			name: "RegisterActionHandler",
			op:   "register_action_handler",
			call: func(c *Client) error {
				return c.RegisterActionHandler("vyos", "trace", func(context.Context, ActionCommand) error { return nil })
			},
		},
		{
			name: "RegisterResultHandler",
			op:   "register_result_handler",
			call: func(c *Client) error {
				return c.RegisterResultHandler("vyos", func(context.Context, ResultEnvelope) error { return nil })
			},
		},
		{
			name: "RegisterStatusHandler",
			op:   "register_status_handler",
			call: func(c *Client) error {
				return c.RegisterStatusHandler("vyos", func(context.Context, StatusEnvelope) error { return nil })
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call(client)
			requireNotImplementedError(t, err, tc.op)
		})
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
