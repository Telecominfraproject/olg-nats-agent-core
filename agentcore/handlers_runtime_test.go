package agentcore

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Telecominfraproject/olg-nats-agent-core/internal/registry"
	"github.com/nats-io/nats.go"
)

/*
TC-CLIENT-HANDLERS-001
Type: Positive
Title: Handler registration before Start succeeds and stores subscription intent
Summary:
Verifies that Phase 5 handler registration APIs succeed before Start and store
deferred subscription intent with expected subject routing values.

Validates:
  - configure action result and status registration succeed before Start
  - health counters reflect registered deferred subscriptions
  - stored registration subjects follow target-first subject conventions
*/
func TestRegisterHandlersBeforeStartStoresSubscriptionIntent(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	if err := client.RegisterConfigureHandler("vyos", func(context.Context, ConfigureNotification) error { return nil }, WithQueueGroup("cfg-workers")); err != nil {
		t.Fatalf("expected nil configure registration error, got %v", err)
	}
	if err := client.RegisterActionHandler("vyos", "trace", func(context.Context, ActionCommand) error { return nil }); err != nil {
		t.Fatalf("expected nil action registration error, got %v", err)
	}
	if err := client.RegisterResultHandler("vyos", func(context.Context, ResultEnvelope) error { return nil }); err != nil {
		t.Fatalf("expected nil result registration error, got %v", err)
	}
	if err := client.RegisterStatusHandler("vyos", func(context.Context, StatusEnvelope) error { return nil }); err != nil {
		t.Fatalf("expected nil status registration error, got %v", err)
	}

	if got := client.Health().RegisteredSubscriptions; got != 4 {
		t.Fatalf("expected RegisteredSubscriptions %d, got %d", 4, got)
	}
	if got := client.Health().ActiveSubscriptions; got != 0 {
		t.Fatalf("expected ActiveSubscriptions %d before start, got %d", 0, got)
	}

	snapshots := client.subscriptions.List()
	if len(snapshots) != 4 {
		t.Fatalf("expected %d registry entries, got %d", 4, len(snapshots))
	}

	gotBySubject := make(map[string]registry.Snapshot, len(snapshots))
	for _, snap := range snapshots {
		gotBySubject[snap.Subject] = snap
	}

	if got, ok := gotBySubject["cmd.configure.vyos"]; !ok {
		t.Fatalf("expected configure subject %q in registry", "cmd.configure.vyos")
	} else {
		if got.Kind != registry.KindConfigure {
			t.Fatalf("expected configure kind %q, got %q", registry.KindConfigure, got.Kind)
		}
		if got.QueueGroup != "cfg-workers" {
			t.Fatalf("expected configure queue group %q, got %q", "cfg-workers", got.QueueGroup)
		}
	}
	if got, ok := gotBySubject["cmd.action.vyos.trace"]; !ok {
		t.Fatalf("expected action subject %q in registry", "cmd.action.vyos.trace")
	} else if got.Kind != registry.KindAction {
		t.Fatalf("expected action kind %q, got %q", registry.KindAction, got.Kind)
	}
	if got, ok := gotBySubject["result.vyos"]; !ok {
		t.Fatalf("expected result subject %q in registry", "result.vyos")
	} else if got.Kind != registry.KindResult {
		t.Fatalf("expected result kind %q, got %q", registry.KindResult, got.Kind)
	}
	if got, ok := gotBySubject["status.vyos"]; !ok {
		t.Fatalf("expected status subject %q in registry", "status.vyos")
	} else if got.Kind != registry.KindStatus {
		t.Fatalf("expected status kind %q, got %q", registry.KindStatus, got.Kind)
	}
}

/*
TC-CLIENT-HANDLERS-002
Type: Negative
Title: Handler registration rejects invalid handler and routing input
Summary:
Verifies that handler registration fails with validation errors for nil handler,
malformed target or action, and invalid queue-group option values.

Validates:
  - nil handlers are rejected with handler registration op names
  - malformed target and action values are rejected by token validators
  - queue group values with whitespace are rejected as validation failures
*/
func TestRegisterHandlersRejectInvalidInput(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	tests := []struct {
		name    string
		call    func() error
		wantOp  string
		msgPart string
	}{
		{
			name: "nil configure handler",
			call: func() error {
				return client.RegisterConfigureHandler("vyos", nil)
			},
			wantOp:  "register_configure_handler",
			msgPart: "configure handler is required",
		},
		{
			name: "invalid configure target",
			call: func() error {
				return client.RegisterConfigureHandler("vyos core", func(context.Context, ConfigureNotification) error { return nil })
			},
			wantOp:  "validate_target",
			msgPart: "target cannot contain whitespace",
		},
		{
			name: "nil action handler",
			call: func() error {
				return client.RegisterActionHandler("vyos", "trace", nil)
			},
			wantOp:  "register_action_handler",
			msgPart: "action handler is required",
		},
		{
			name: "invalid action token",
			call: func() error {
				return client.RegisterActionHandler("vyos", "tr ace", func(context.Context, ActionCommand) error { return nil })
			},
			wantOp:  "validate_action",
			msgPart: "action cannot contain whitespace",
		},
		{
			name: "invalid queue group",
			call: func() error {
				return client.RegisterStatusHandler("vyos", func(context.Context, StatusEnvelope) error { return nil }, WithQueueGroup("bad group"))
			},
			wantOp:  "register_handler_options",
			msgPart: "queue group cannot contain whitespace",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := requireErrorCode(t, tc.call(), CodeValidation)
			if got.Op != tc.wantOp {
				t.Fatalf("expected error op %q, got %q", tc.wantOp, got.Op)
			}
			if !strings.Contains(got.Message, tc.msgPart) {
				t.Fatalf("expected error message to contain %q, got %q", tc.msgPart, got.Message)
			}
		})
	}
}

/*
TC-CLIENT-HANDLERS-003
Type: Negative
Title: Registration after Close returns disconnected lifecycle error
Summary:
Verifies that valid registration attempted after Close does not activate and
returns a disconnected lifecycle error from runtime connection access.

Validates:
  - registration after close returns CodeDisconnected
  - error op is connection
*/
func TestRegisterHandlerAfterCloseReturnsDisconnected(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("expected nil close error, got %v", err)
	}

	got := requireErrorCode(t, client.RegisterResultHandler("vyos", func(context.Context, ResultEnvelope) error { return nil }), CodeDisconnected)
	if got.Op != "connection" {
		t.Fatalf("expected error op %q, got %q", "connection", got.Op)
	}

	if got := client.Health().RegisteredSubscriptions; got != 0 {
		t.Fatalf("expected RegisteredSubscriptions %d after failed immediate registration, got %d", 0, got)
	}
	if got := len(client.subscriptions.List()); got != 0 {
		t.Fatalf("expected no stale registry intent after failed immediate registration, got %d entries", got)
	}

	retryErr := client.RegisterResultHandler("vyos", func(context.Context, ResultEnvelope) error { return nil })
	got = requireErrorCode(t, retryErr, CodeDisconnected)
	if got.Code == CodeRegistryConflict {
		t.Fatalf("expected disconnected retry error, got stale conflict error: %v", got)
	}
}

/*
TC-CLIENT-HANDLERS-004
Type: Positive
Title: Callback binding decodes valid payloads and invokes typed handlers
Summary:
Verifies that callback binders for configure action result and status decode
valid payloads and invoke user handlers with preserved transport fields.

Validates:
  - valid payloads invoke each typed handler exactly once
  - configure callback preserves rpc_id and uuid
  - action/result/status callbacks preserve rpc_id and key fields
*/
func TestCallbackBindingInvokesTypedHandlersForValidPayloads(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	client.callbacksEnabled.Store(true)

	now := time.Unix(1700000100, 0).UTC()
	configPayload := []byte(`{"version":"1.0","rpc_id":"rpc-cfg-1","target":"vyos","command_type":"configure","uuid":"cfg-1","kv_bucket":"cfg_desired","kv_key":"desired.vyos","timestamp":"` + now.Format(time.RFC3339Nano) + `"}`)
	actionPayload := []byte(`{"version":"1.0","rpc_id":"rpc-act-1","target":"vyos","command_type":"action","action":"trace","payload":{"dst":"8.8.8.8"},"timestamp":"` + now.Format(time.RFC3339Nano) + `"}`)
	resultPayload := []byte(`{"version":"1.0","rpc_id":"rpc-res-1","target":"vyos","result":"success","timestamp":"` + now.Format(time.RFC3339Nano) + `"}`)
	statusPayload := []byte(`{"version":"1.0","rpc_id":"rpc-st-1","target":"vyos","status":"running","timestamp":"` + now.Format(time.RFC3339Nano) + `"}`)

	var (
		configCalls int
		actionCalls int
		resultCalls int
		statusCalls int
	)

	configCB := client.bindConfigureCallback(func(_ context.Context, msg ConfigureNotification) error {
		configCalls++
		if msg.RPCID != "rpc-cfg-1" {
			t.Fatalf("expected configure rpc_id %q, got %q", "rpc-cfg-1", msg.RPCID)
		}
		if msg.UUID != "cfg-1" {
			t.Fatalf("expected configure uuid %q, got %q", "cfg-1", msg.UUID)
		}
		return nil
	})
	actionCB := client.bindActionCallback(func(_ context.Context, msg ActionCommand) error {
		actionCalls++
		if msg.RPCID != "rpc-act-1" {
			t.Fatalf("expected action rpc_id %q, got %q", "rpc-act-1", msg.RPCID)
		}
		if msg.Action != "trace" {
			t.Fatalf("expected action value %q, got %q", "trace", msg.Action)
		}
		return nil
	})
	resultCB := client.bindResultCallback(func(_ context.Context, msg ResultEnvelope) error {
		resultCalls++
		if msg.RPCID != "rpc-res-1" {
			t.Fatalf("expected result rpc_id %q, got %q", "rpc-res-1", msg.RPCID)
		}
		if msg.Result != "success" {
			t.Fatalf("expected result value %q, got %q", "success", msg.Result)
		}
		return nil
	})
	statusCB := client.bindStatusCallback(func(_ context.Context, msg StatusEnvelope) error {
		statusCalls++
		if msg.RPCID != "rpc-st-1" {
			t.Fatalf("expected status rpc_id %q, got %q", "rpc-st-1", msg.RPCID)
		}
		if msg.Status != "running" {
			t.Fatalf("expected status value %q, got %q", "running", msg.Status)
		}
		return nil
	})

	configCB(&nats.Msg{Subject: "cmd.configure.vyos", Data: configPayload})
	actionCB(&nats.Msg{Subject: "cmd.action.vyos.trace", Data: actionPayload})
	resultCB(&nats.Msg{Subject: "result.vyos", Data: resultPayload})
	statusCB(&nats.Msg{Subject: "status.vyos", Data: statusPayload})

	if configCalls != 1 || actionCalls != 1 || resultCalls != 1 || statusCalls != 1 {
		t.Fatalf(
			"expected one callback call each, got configure=%d action=%d result=%d status=%d",
			configCalls, actionCalls, resultCalls, statusCalls,
		)
	}
}

/*
TC-CLIENT-HANDLERS-005
Type: Negative
Title: Callback binding drops malformed and invalid payloads
Summary:
Verifies that callback binders safely drop malformed JSON or validation-failing
payloads and do not invoke registered user handlers.

Validates:
  - malformed JSON payload is dropped without handler invocation
  - schema validation failure payload is dropped without handler invocation
*/
func TestCallbackBindingDropsMalformedOrInvalidPayloads(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	client.callbacksEnabled.Store(true)

	calls := 0
	resultCB := client.bindResultCallback(func(_ context.Context, msg ResultEnvelope) error {
		calls++
		return nil
	})

	resultCB(&nats.Msg{Subject: "result.vyos", Data: []byte("{")})
	resultCB(&nats.Msg{Subject: "result.vyos", Data: []byte(`{"version":"1.0","target":"vyos","result":"ok","timestamp":"2026-01-01T00:00:00Z"}`)})

	if calls != 0 {
		t.Fatalf("expected handler not to be called for malformed/invalid payloads, got %d calls", calls)
	}
}

/*
TC-CLIENT-HANDLERS-006
Type: Positive
Title: Result callback preserves rpc_id exactly for correlation
Summary:
Verifies that receive-side result callback preserves rpc_id exactly as encoded
in the incoming payload and exposes it to the registered result handler.

Validates:
  - decoded result passed to handler keeps rpc_id unchanged
  - no rpc_id rewrite occurs during callback decode/dispatch
*/
func TestResultCallbackPreservesRPCIDUnchanged(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	client.callbacksEnabled.Store(true)

	now := time.Unix(1700000200, 0).UTC()
	payload := ResultEnvelope{
		Version:   "1.0",
		RPCID:     "rpc-correlation-99",
		Target:    "vyos",
		Result:    "success",
		Timestamp: now,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to encode test result payload: %v", err)
	}

	var seenRPCID string
	cb := client.bindResultCallback(func(_ context.Context, msg ResultEnvelope) error {
		seenRPCID = msg.RPCID
		return nil
	})
	cb(&nats.Msg{Subject: "result.vyos", Data: encoded})

	if seenRPCID != "rpc-correlation-99" {
		t.Fatalf("expected preserved rpc_id %q, got %q", "rpc-correlation-99", seenRPCID)
	}
}

/*
TC-CLIENT-HANDLERS-007
Type: Positive
Title: Callback binder logs user handler errors without panic
Summary:
Verifies that callback dispatch logs user-handler failures and safely returns
without panic when the user callback returns an error.

Validates:
  - user handler error does not panic callback path
  - logger records an error entry for handler failure
*/
func TestCallbackBindingLogsUserHandlerError(t *testing.T) {
	logger := &testLogger{}
	client, err := New(testConfig(), WithLogger(logger))
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	client.callbacksEnabled.Store(true)

	now := time.Unix(1700000300, 0).UTC()
	payload := []byte(`{"version":"1.0","rpc_id":"rpc-err-1","target":"vyos","result":"failure","timestamp":"` + now.Format(time.RFC3339Nano) + `"}`)

	cb := client.bindResultCallback(func(_ context.Context, msg ResultEnvelope) error {
		return errors.New("handler failed")
	})
	cb(&nats.Msg{Subject: "result.vyos", Data: payload})

	found := false
	for _, entry := range logger.entries {
		if strings.Contains(entry, "ERROR:result handler returned error") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected error log entry for result handler failure, got entries: %+v", logger.entries)
	}
}

/*
TC-CLIENT-HANDLERS-008
Type: Negative
Title: Start keeps callbacks disabled and cleans partial activations on failure
Summary:
Verifies that Start(...) does not enable callbacks until activation succeeds,
and cleans up partially activated subscriptions when activation fails.

Validates:
  - Start returns activation error
  - callbacksEnabled remains false on Start failure
  - ActiveSubscriptions resets to zero after cleanup
  - deferred registered intent remains stored for retry
*/
func TestStartActivationFailureDisablesCallbacksAndCleansPartials(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	if err := client.RegisterConfigureHandler("vyos", func(context.Context, ConfigureNotification) error { return nil }); err != nil {
		t.Fatalf("expected nil pre-start registration error, got %v", err)
	}

	client.startSessionFn = func(context.Context) error { return nil }
	client.activateAllSubscriptionsFn = func(_ string) error {
		records := client.subscriptions.ListActivations()
		if len(records) != 1 {
			t.Fatalf("expected one activation record, got %d", len(records))
		}
		client.subscriptions.MarkActive(records[0].ID, &nats.Subscription{})
		client.syncSubscriptionHealth()
		return &Error{
			Code:      CodeSubscribeFailed,
			Op:        "start",
			Message:   "activation failed",
			Retryable: true,
		}
	}

	got := requireErrorCode(t, client.Start(context.Background()), CodeSubscribeFailed)
	if got.Op != "start" {
		t.Fatalf("expected error op %q, got %q", "start", got.Op)
	}
	if client.callbacksEnabled.Load() {
		t.Fatal("expected callbacksEnabled to remain false on failed start")
	}
	if got := client.Health().RegisteredSubscriptions; got != 1 {
		t.Fatalf("expected RegisteredSubscriptions %d to remain deferred intent, got %d", 1, got)
	}
	if got := client.Health().ActiveSubscriptions; got != 0 {
		t.Fatalf("expected ActiveSubscriptions %d after cleanup, got %d", 0, got)
	}
	handlerCtx := client.handlerCtx.Load()
	if handlerCtx != nil {
		t.Fatal("expected handler context to remain nil on failed start")
	}
}

/*
TC-CLIENT-HANDLERS-009
Type: Positive
Title: Start enables callbacks only after successful activation
Summary:
Verifies that Start(...) enables callback dispatch only after subscription
activation succeeds and active subscription counters reflect success.

Validates:
  - Start succeeds when session start and activation succeed
  - callbacksEnabled becomes true only after activation pass
  - ActiveSubscriptions reflects activated registrations
*/
func TestStartEnablesCallbacksAfterSuccessfulActivation(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	if err := client.RegisterResultHandler("vyos", func(context.Context, ResultEnvelope) error { return nil }); err != nil {
		t.Fatalf("expected nil pre-start registration error, got %v", err)
	}

	client.startSessionFn = func(context.Context) error { return nil }
	client.activateAllSubscriptionsFn = func(_ string) error {
		records := client.subscriptions.ListActivations()
		if len(records) != 1 {
			t.Fatalf("expected one activation record, got %d", len(records))
		}
		client.subscriptions.MarkActive(records[0].ID, &nats.Subscription{})
		client.syncSubscriptionHealth()
		return nil
	}

	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("expected successful start, got %v", err)
	}
	if !client.callbacksEnabled.Load() {
		t.Fatal("expected callbacksEnabled to be true after successful start")
	}
	if got := client.Health().RegisteredSubscriptions; got != 1 {
		t.Fatalf("expected RegisteredSubscriptions %d, got %d", 1, got)
	}
	if got := client.Health().ActiveSubscriptions; got != 1 {
		t.Fatalf("expected ActiveSubscriptions %d after activation, got %d", 1, got)
	}
	handlerCtxPtr := client.handlerCtx.Load()
	if handlerCtxPtr == nil {
		t.Fatal("expected handler context to be created after successful start")
	}
	handlerCtx := *handlerCtxPtr
	select {
	case <-handlerCtx.Done():
		t.Fatal("expected handler context to remain active after successful start")
	default:
	}
}

/*
TC-CLIENT-HANDLERS-009B
Type: Positive
Title: Repeated Start preserves active handler lifecycle context
Summary:
Verifies that calling Start(...) repeatedly on an already-running client does
not cancel and replace an active handler lifecycle context.

Validates:
  - first and second Start share the same active lifecycle context
  - repeated Start does not cancel existing handler context
  - callbacksEnabled remains true
*/
func TestRepeatedStartDoesNotCancelActiveHandlerContext(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	client.startSessionFn = func(context.Context) error { return nil }
	client.activateAllSubscriptionsFn = func(_ string) error { return nil }

	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("first Start returned unexpected error: %v", err)
	}
	firstCtx := client.handlerContext()

	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("second Start returned unexpected error: %v", err)
	}
	secondCtx := client.handlerContext()

	if firstCtx != secondCtx {
		t.Fatal("expected repeated Start to preserve active handler context")
	}
	select {
	case <-firstCtx.Done():
		t.Fatal("expected repeated Start not to cancel active handler context")
	default:
	}
	if !client.callbacksEnabled.Load() {
		t.Fatal("expected callbacksEnabled to remain true after repeated Start")
	}
}

/*
TC-CLIENT-HANDLERS-009C
Type: Positive
Title: Start recreates handler lifecycle context after cancellation
Summary:
Verifies that Start(...) creates a new lifecycle context when no active
handler context exists because the previous one was canceled.

Validates:
  - canceled handler context is replaced on next successful Start
  - replacement context is active and distinct from the canceled one
*/
func TestStartRecreatesHandlerContextAfterCancellation(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	client.startSessionFn = func(context.Context) error { return nil }
	client.activateAllSubscriptionsFn = func(_ string) error { return nil }

	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("first Start returned unexpected error: %v", err)
	}
	firstCtx := client.handlerContext()

	client.cancelHandlerContext()
	select {
	case <-firstCtx.Done():
	default:
		t.Fatal("expected first handler context to be canceled")
	}

	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("second Start returned unexpected error: %v", err)
	}
	secondCtx := client.handlerContext()

	if secondCtx == firstCtx {
		t.Fatal("expected Start to replace canceled handler context")
	}
	select {
	case <-secondCtx.Done():
		t.Fatal("expected recreated handler context to be active")
	default:
	}
}

/*
TC-CLIENT-HANDLERS-010
Type: Positive
Title: Handler registration uses configured custom subject patterns
Summary:
Verifies that registration subject construction honors custom subject patterns
configured on client creation while preserving target-first token binding.

Validates:
  - configure action result and status subjects use configured patterns
*/
func TestRegisterHandlersUseConfiguredCustomSubjectPatterns(t *testing.T) {
	cfg := testConfig()
	cfg.Subjects.ConfigurePattern = "custom.configure.%s"
	cfg.Subjects.ActionPattern = "custom.action.%s.%s"
	cfg.Subjects.ResultPattern = "custom.result.%s"
	cfg.Subjects.StatusPattern = "custom.status.%s"

	client, err := New(cfg)
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	if err := client.RegisterConfigureHandler("vyos", func(context.Context, ConfigureNotification) error { return nil }); err != nil {
		t.Fatalf("expected nil configure registration error, got %v", err)
	}
	if err := client.RegisterActionHandler("vyos", "trace", func(context.Context, ActionCommand) error { return nil }); err != nil {
		t.Fatalf("expected nil action registration error, got %v", err)
	}
	if err := client.RegisterResultHandler("vyos", func(context.Context, ResultEnvelope) error { return nil }); err != nil {
		t.Fatalf("expected nil result registration error, got %v", err)
	}
	if err := client.RegisterStatusHandler("vyos", func(context.Context, StatusEnvelope) error { return nil }); err != nil {
		t.Fatalf("expected nil status registration error, got %v", err)
	}

	snapshots := client.subscriptions.List()
	if len(snapshots) != 4 {
		t.Fatalf("expected %d registry entries, got %d", 4, len(snapshots))
	}

	gotBySubject := make(map[string]registry.Snapshot, len(snapshots))
	for _, snap := range snapshots {
		gotBySubject[snap.Subject] = snap
	}

	if _, ok := gotBySubject["custom.configure.vyos"]; !ok {
		t.Fatalf("expected configure subject %q in registry", "custom.configure.vyos")
	}
	if _, ok := gotBySubject["custom.action.vyos.trace"]; !ok {
		t.Fatalf("expected action subject %q in registry", "custom.action.vyos.trace")
	}
	if _, ok := gotBySubject["custom.result.vyos"]; !ok {
		t.Fatalf("expected result subject %q in registry", "custom.result.vyos")
	}
	if _, ok := gotBySubject["custom.status.vyos"]; !ok {
		t.Fatalf("expected status subject %q in registry", "custom.status.vyos")
	}
}

/*
TC-CLIENT-HANDLERS-011
Type: Negative
Title: New rejects invalid configured subject patterns during bootstrap
Summary:
Verifies that invalid configured subject patterns fail fast during New(...) and
surface a typed validation error before runtime/session initialization.

Validates:
  - malformed action subject pattern returns CodeValidation
  - error op remains validate_subject_pattern
*/
func TestNewRejectsInvalidConfiguredSubjectPatterns(t *testing.T) {
	cfg := testConfig()
	cfg.Subjects.ActionPattern = "cmd.action.%s"

	_, err := New(cfg)
	got := requireErrorCode(t, err, CodeValidation)
	if got.Op != "validate_subject_pattern" {
		t.Fatalf("expected error op %q, got %q", "validate_subject_pattern", got.Op)
	}
	if !strings.Contains(got.Message, "action_pattern placeholder count is invalid") {
		t.Fatalf("expected error message to mention invalid action pattern placeholders, got %q", got.Message)
	}
}

/*
TC-CLIENT-HANDLERS-012
Type: Positive
Title: Handler dispatch uses lifecycle context and close cancels it
Summary:
Verifies that dispatched handlers receive the client lifecycle context and that
Close cancels that context for already-running handler work.

Validates:
  - callback dispatch receives non-canceled lifecycle context after Start
  - Close cancels the same lifecycle context
*/
func TestHandlerDispatchUsesLifecycleContextAndCloseCancelsIt(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	client.startSessionFn = func(context.Context) error { return nil }
	client.activateAllSubscriptionsFn = func(_ string) error { return nil }

	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("expected successful start, got %v", err)
	}

	handlerStarted := make(chan context.Context, 1)
	handlerDone := make(chan struct{}, 1)
	go func() {
		_ = client.callResultHandler(func(ctx context.Context, _ ResultEnvelope) error {
			handlerStarted <- ctx
			<-ctx.Done()
			handlerDone <- struct{}{}
			return nil
		}, ResultEnvelope{Version: "1.0", RPCID: "rpc-1", Target: "vyos", Result: "ok", Timestamp: time.Now().UTC()})
	}()

	var handlerCtx context.Context
	select {
	case handlerCtx = <-handlerStarted:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for handler start")
	}

	select {
	case <-handlerCtx.Done():
		t.Fatal("expected handler context to be active before close")
	default:
	}

	if err := client.Close(context.Background()); err != nil {
		t.Fatalf("expected nil close error, got %v", err)
	}

	select {
	case <-handlerDone:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for handler context cancelation on close")
	}
}

/*
TC-CLIENT-HANDLERS-013
Type: Positive
Title: Session closed cleanup clears active handles and preserves intent
Summary:
Verifies unexpected session-closed cleanup disables callbacks, cancels handler
context, clears active handle state, and preserves deferred registration intent.

Validates:
  - callbacksEnabled becomes false on session-closed cleanup
  - handler lifecycle context is canceled
  - ActiveSubscriptions is zeroed while RegisteredSubscriptions is preserved
  - records remain registered and inactive for future reactivation
*/
func TestOnSessionClosedClearsActiveStateAndPreservesIntent(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	if err := client.RegisterResultHandler("vyos", func(context.Context, ResultEnvelope) error { return nil }); err != nil {
		t.Fatalf("expected nil registration error, got %v", err)
	}

	records := client.subscriptions.ListActivations()
	if len(records) != 1 {
		t.Fatalf("expected one activation record, got %d", len(records))
	}
	client.subscriptions.MarkActive(records[0].ID, &nats.Subscription{})
	client.syncSubscriptionHealth()

	client.ensureHandlerContext()
	handlerCtx := client.handlerContext()
	client.callbacksEnabled.Store(true)

	client.onSessionClosed()

	if client.callbacksEnabled.Load() {
		t.Fatal("expected callbacksEnabled false after session closed cleanup")
	}
	select {
	case <-handlerCtx.Done():
	default:
		t.Fatal("expected handler context to be canceled on session closed cleanup")
	}
	if got := client.Health().RegisteredSubscriptions; got != 1 {
		t.Fatalf("expected RegisteredSubscriptions %d, got %d", 1, got)
	}
	if got := client.Health().ActiveSubscriptions; got != 0 {
		t.Fatalf("expected ActiveSubscriptions %d after session closed cleanup, got %d", 0, got)
	}
	activation, ok := client.subscriptions.GetActivationRecord(records[0].ID)
	if !ok {
		t.Fatal("expected registration intent to remain after session closed cleanup")
	}
	if activation.Active {
		t.Fatal("expected activation record to be marked inactive after cleanup")
	}
}

/*
TC-CLIENT-HANDLERS-015
Type: Positive
Title: Start can reactivate intents after session-closed cleanup
Summary:
Verifies that after unexpected session-closed cleanup, a later Start can
reactivate saved subscription intent without re-registration.

Validates:
  - post-close cleanup leaves record inactive
  - later Start activation pass can mark it active again
*/
func TestStartReactivatesIntentAfterSessionClosedCleanup(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	if err := client.RegisterResultHandler("vyos", func(context.Context, ResultEnvelope) error { return nil }); err != nil {
		t.Fatalf("expected nil registration error, got %v", err)
	}

	records := client.subscriptions.ListActivations()
	if len(records) != 1 {
		t.Fatalf("expected one activation record, got %d", len(records))
	}
	client.subscriptions.MarkActive(records[0].ID, &nats.Subscription{})
	client.syncSubscriptionHealth()

	client.onSessionClosed()

	client.startSessionFn = func(context.Context) error { return nil }
	client.activateAllSubscriptionsFn = func(_ string) error {
		current := client.subscriptions.ListActivations()
		if len(current) != 1 {
			t.Fatalf("expected one activation record, got %d", len(current))
		}
		if current[0].Active {
			t.Fatal("expected cleanup to clear stale active state before start reactivation")
		}
		client.subscriptions.MarkActive(current[0].ID, &nats.Subscription{})
		client.syncSubscriptionHealth()
		return nil
	}

	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("expected successful start, got %v", err)
	}
	if got := client.Health().RegisteredSubscriptions; got != 1 {
		t.Fatalf("expected RegisteredSubscriptions %d, got %d", 1, got)
	}
	if got := client.Health().ActiveSubscriptions; got != 1 {
		t.Fatalf("expected ActiveSubscriptions %d after reactivation, got %d", 1, got)
	}
}

/*
TC-CLIENT-HANDLERS-014
Type: Positive
Title: Nil callback messages are dropped safely for all handler kinds
Summary:
Verifies callback binders guard against nil *nats.Msg inputs and drop them
without panicking or invoking user handlers.

Validates:
  - nil configure/action/result/status messages do not panic
  - nil configure/action/result/status messages do not invoke handlers
*/
func TestCallbackBindingDropsNilMessagesWithoutPanic(t *testing.T) {
	client, err := New(testConfig())
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	client.callbacksEnabled.Store(true)

	calls := 0
	configureCB := client.bindConfigureCallback(func(context.Context, ConfigureNotification) error {
		calls++
		return nil
	})
	actionCB := client.bindActionCallback(func(context.Context, ActionCommand) error {
		calls++
		return nil
	})
	resultCB := client.bindResultCallback(func(context.Context, ResultEnvelope) error {
		calls++
		return nil
	})
	statusCB := client.bindStatusCallback(func(context.Context, StatusEnvelope) error {
		calls++
		return nil
	})

	configureCB(nil)
	actionCB(nil)
	resultCB(nil)
	statusCB(nil)

	if calls != 0 {
		t.Fatalf("expected no handler invocations for nil messages, got %d calls", calls)
	}
}
