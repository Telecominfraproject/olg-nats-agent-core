package session

import (
	"errors"
	"testing"

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
	m := &Manager{closing: true}
	m.health.State = StateDraining
	m.health.LastError = "existing"

	m.onDisconnect(nil, errors.New("ignored"))

	if m.health.State != StateDraining {
		t.Fatalf("expected state to remain %q, got %q", StateDraining, m.health.State)
	}
	if m.health.LastError != "existing" {
		t.Fatalf("expected LastError to remain %q, got %q", "existing", m.health.LastError)
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
