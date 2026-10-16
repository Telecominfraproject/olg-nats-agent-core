package session

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Telecominfraproject/olg-nats-agent-core/internal/runtimeerr"
	"github.com/nats-io/nats.go"
)

/*
TC-SESSION-CALLBACKS-001
Type: Positive
Title: onDisconnect transitions health to reconnecting when session is active
Summary:
Verifies that disconnect callback transitions state into reconnecting and
captures the disconnect error when the manager is not closing.

Validates:
  - state transitions to reconnecting
  - readiness flags are cleared
  - last error text stores disconnect cause
*/
func TestOnDisconnectTransitionsToReconnectingWhenActive(t *testing.T) {
	m := &Manager{}
	m.health.JetStreamReady = true
	m.health.KVReady = true

	m.onDisconnect(nil, errors.New("socket closed"))

	if m.health.State != StateReconnecting {
		t.Fatalf("expected state %q, got %q", StateReconnecting, m.health.State)
	}
	if m.health.JetStreamReady || m.health.KVReady {
		t.Fatalf("expected readiness false after disconnect, got js=%v kv=%v", m.health.JetStreamReady, m.health.KVReady)
	}
	if m.health.LastError != "socket closed" {
		t.Fatalf("expected LastError %q, got %q", "socket closed", m.health.LastError)
	}
}

/*
TC-SESSION-CALLBACKS-002
Type: Positive
Title: onDisconnect does not mutate health while closing
Summary:
Verifies that disconnect callback is a no-op during close path so close state
is not overwritten by reconnecting transitions.

Validates:
  - closing flag prevents reconnect transition
  - existing health state is preserved
*/
func TestOnDisconnectNoOpWhenClosing(t *testing.T) {
	hookFired := false
	m := &Manager{
		closing: true,
		hooks: Hooks{
			OnDisconnected: func() {
				hookFired = true
			},
		},
	}
	m.health.State = StateDraining
	m.health.LastError = "existing"

	m.onDisconnect(nil, errors.New("ignored"))

	if m.health.State != StateDraining {
		t.Fatalf("expected state to remain %q, got %q", StateDraining, m.health.State)
	}
	if m.health.LastError != "existing" {
		t.Fatalf("expected LastError to remain %q, got %q", "existing", m.health.LastError)
	}
	if hookFired {
		t.Fatal("expected OnDisconnected hook not to fire when closing")
	}
}

func TestOnDisconnectForwardsErrorToSink(t *testing.T) {
	sinkErrCh := make(chan error, 1)
	hookFiredCh := make(chan struct{}, 1)
	m := &Manager{
		hooks: Hooks{
			ErrorSink: func(err error) {
				sinkErrCh <- err
			},
			OnDisconnected: func() {
				hookFiredCh <- struct{}{}
			},
		},
	}

	testErr := errors.New("connection reset by peer")
	m.onDisconnect(nil, testErr)

	select {
	case <-hookFiredCh:
		// success
	case <-time.After(1 * time.Second):
		t.Fatal("expected OnDisconnected hook to be invoked")
	}

	select {
	case sinkErr := <-sinkErrCh:
		if sinkErr == nil {
			t.Fatal("expected disconnect error to be forwarded to ErrorSink")
		}
		var rtErr *runtimeerr.Error
		if !errors.As(sinkErr, &rtErr) {
			t.Fatalf("expected runtimeerr.Error, got: %T", sinkErr)
		}
		if rtErr.Code != runtimeerr.CodeDisconnected {
			t.Errorf("expected CodeDisconnected, got: %v", rtErr.Code)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for ErrorSink invocation")
	}
}

func TestOnDisconnectIndependentOfSlowErrorSink(t *testing.T) {
	blockSink := make(chan struct{})
	defer close(blockSink)

	hookFiredCh := make(chan struct{}, 1)
	m := &Manager{
		hooks: Hooks{
			ErrorSink: func(err error) {
				<-blockSink
			},
			OnDisconnected: func() {
				hookFiredCh <- struct{}{}
			},
		},
	}

	go m.onDisconnect(nil, errors.New("connection lost"))

	select {
	case <-hookFiredCh:
		// Succeeded: OnDisconnected executed immediately without waiting on blocked ErrorSink
	case <-time.After(500 * time.Millisecond):
		t.Fatal("OnDisconnected hook was blocked or delayed by slow ErrorSink")
	}
}

func TestLifecycleHookOrdering_DisconnectBeforeReconnect(t *testing.T) {
	order := make([]string, 0, 2)
	var orderMu sync.Mutex

	m := &Manager{
		hooks: Hooks{
			OnDisconnected: func() {
				orderMu.Lock()
				order = append(order, "disconnected")
				orderMu.Unlock()
			},
			OnReconnected: func() {
				orderMu.Lock()
				order = append(order, "reconnected")
				orderMu.Unlock()
			},
		},
	}

	// 1. Disconnect triggers OnDisconnected synchronously
	m.onDisconnect(nil, errors.New("connection drop"))

	// 2. Reconnect triggers OnReconnected
	if m.hooks.OnReconnected != nil {
		m.hooks.OnReconnected()
	}

	orderMu.Lock()
	defer orderMu.Unlock()
	if len(order) != 2 {
		t.Fatalf("expected 2 lifecycle events, got %d: %v", len(order), order)
	}
	if order[0] != "disconnected" || order[1] != "reconnected" {
		t.Fatalf("lifecycle order violated: expected [disconnected, reconnected], got %v", order)
	}
}

func TestOnDisconnectInvokesRegisteredHookOutsideLock(t *testing.T) {
	m := &Manager{}
	done := make(chan struct{}, 1)
	m.hooks.OnDisconnected = func() {
		// Calling a lock-taking method will deadlock if m.mu is held during onDisconnect
		m.SetReconnectHandler(nil)
		done <- struct{}{}
	}

	go m.onDisconnect(nil, errors.New("network drop"))

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for OnDisconnected hook invocation (possible deadlock)")
	}
}

/*
TC-SESSION-CALLBACKS-003
Type: Positive
Title: onClosed transitions health to closed
Summary:
Verifies that closed callback updates manager health snapshot to a closed state
and clears connectivity readiness flags.

Validates:
  - state transitions to closed
  - connected URL and readiness flags are cleared
*/
func TestOnClosedTransitionsHealthToClosed(t *testing.T) {
	m := &Manager{}
	m.health.ConnectedURL = "nats://server:4222"
	m.health.JetStreamReady = true
	m.health.KVReady = true

	m.onClosed(nil)

	if m.health.State != StateClosed {
		t.Fatalf("expected state %q, got %q", StateClosed, m.health.State)
	}
	if m.health.ConnectedURL != "" {
		t.Fatalf("expected ConnectedURL to be cleared, got %q", m.health.ConnectedURL)
	}
	if m.health.JetStreamReady || m.health.KVReady {
		t.Fatalf("expected readiness false after close, got js=%v kv=%v", m.health.JetStreamReady, m.health.KVReady)
	}
}

/*
TC-SESSION-CALLBACKS-004
Type: Positive
Title: onClosed invokes registered closed hook outside lock
Summary:
Verifies that closed callback invokes the registered OnClosed hook and does so
without holding the manager lock.

Validates:
  - OnClosed hook is called once
  - hook can safely call lock-taking manager methods
*/
func TestOnClosedInvokesRegisteredHookOutsideLock(t *testing.T) {
	m := &Manager{}
	done := make(chan struct{}, 1)
	m.hooks.OnClosed = func() {
		m.SetReconnectHandler(nil)
		done <- struct{}{}
	}

	go m.onClosed(nil)

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for OnClosed hook invocation")
	}
}

/*
TC-SESSION-CALLBACKS-005
Type: Positive
Title: isBucketNotFound recognizes typed and message-based not-found errors
Summary:
Verifies that bucket-not-found helper recognizes both typed NATS not-found
errors and textual stream or bucket not-found variants.

Validates:
  - nats.ErrBucketNotFound is recognized
  - message-based not-found forms are recognized
  - unrelated errors are not treated as not-found
*/
func TestIsBucketNotFoundRecognizesSupportedForms(t *testing.T) {
	if !isBucketNotFound(nats.ErrBucketNotFound) {
		t.Fatal("expected nats.ErrBucketNotFound to be recognized")
	}
	if !isBucketNotFound(errors.New("stream not found")) {
		t.Fatal("expected stream not found message to be recognized")
	}
	if !isBucketNotFound(errors.New("bucket not found")) {
		t.Fatal("expected bucket not found message to be recognized")
	}
	if isBucketNotFound(errors.New("permission denied")) {
		t.Fatal("expected unrelated error not to be recognized")
	}
}
