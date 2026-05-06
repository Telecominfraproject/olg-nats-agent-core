package session

import (
	"errors"
	"testing"
)

type testSessionMetrics struct {
	states []string
}

func (m *testSessionMetrics) IncConnect(string) {}
func (m *testSessionMetrics) SetConnectionState(state string) {
	m.states = append(m.states, state)
}

/*
TC-SESSION-HEALTH-001
Type: Positive
Title: setConnectedLocked marks connected when both readiness flags are true
Summary:
Verifies that connected health updates set connection metadata, clear last
error text, and transition state to connected when JS and KV are both ready.

Validates:
  - state transitions to connected
  - connected URL and readiness flags are updated
  - last error text is cleared
*/
func TestSetConnectedLockedMarksConnectedWhenBothReady(t *testing.T) {
	metrics := &testSessionMetrics{}
	m := &Manager{hooks: Hooks{Metrics: metrics}}
	m.health.LastError = "previous error"

	m.setConnectedLocked("nats://server:4222", true, true)

	if m.health.State != StateConnected {
		t.Fatalf("expected state %q, got %q", StateConnected, m.health.State)
	}
	if m.health.ConnectedURL != "nats://server:4222" {
		t.Fatalf("expected ConnectedURL %q, got %q", "nats://server:4222", m.health.ConnectedURL)
	}
	if !m.health.JetStreamReady || !m.health.KVReady {
		t.Fatalf("expected JS and KV readiness true, got js=%v kv=%v", m.health.JetStreamReady, m.health.KVReady)
	}
	if m.health.LastError != "" {
		t.Fatalf("expected LastError to be cleared, got %q", m.health.LastError)
	}
	if len(metrics.states) == 0 || metrics.states[len(metrics.states)-1] != string(StateConnected) {
		t.Fatalf("expected metrics to record %q transition, got %+v", StateConnected, metrics.states)
	}
}

/*
TC-SESSION-HEALTH-002
Type: Positive
Title: setConnectedLocked marks degraded when readiness is partial
Summary:
Verifies that connected helper transitions to degraded state when only one of
JetStream or KV readiness flags is true.

Validates:
  - partial readiness transitions state to degraded
  - health readiness flags still reflect the provided values
*/
func TestSetConnectedLockedMarksDegradedWhenReadinessIsPartial(t *testing.T) {
	m := &Manager{}

	m.setConnectedLocked("nats://server:4222", true, false)

	if m.health.State != StateDegraded {
		t.Fatalf("expected state %q, got %q", StateDegraded, m.health.State)
	}
	if !m.health.JetStreamReady || m.health.KVReady {
		t.Fatalf("expected js=true kv=false, got js=%v kv=%v", m.health.JetStreamReady, m.health.KVReady)
	}
}

/*
TC-SESSION-HEALTH-003
Type: Positive
Title: setReconnectingLocked clears readiness and stores error message
Summary:
Verifies that reconnecting helper clears readiness flags and stores the last
disconnect error message while transitioning state to reconnecting.

Validates:
  - state transitions to reconnecting
  - readiness flags are set false
  - last error text is preserved
*/
func TestSetReconnectingLockedClearsReadinessAndStoresError(t *testing.T) {
	m := &Manager{}
	m.health.JetStreamReady = true
	m.health.KVReady = true

	m.setReconnectingLocked(errors.New("network partition"))

	if m.health.State != StateReconnecting {
		t.Fatalf("expected state %q, got %q", StateReconnecting, m.health.State)
	}
	if m.health.JetStreamReady || m.health.KVReady {
		t.Fatalf("expected readiness false after reconnecting, got js=%v kv=%v", m.health.JetStreamReady, m.health.KVReady)
	}
	if m.health.LastError != "network partition" {
		t.Fatalf("expected LastError %q, got %q", "network partition", m.health.LastError)
	}
}

/*
TC-SESSION-HEALTH-004
Type: Positive
Title: setClosedLocked clears connection URL and readiness flags
Summary:
Verifies that close helper clears transport connectivity metadata and moves
health state to closed.

Validates:
  - state transitions to closed
  - connected URL is cleared
  - readiness flags are cleared
*/
func TestSetClosedLockedClearsConnectionMetadata(t *testing.T) {
	m := &Manager{}
	m.health.ConnectedURL = "nats://server:4222"
	m.health.JetStreamReady = true
	m.health.KVReady = true

	m.setClosedLocked(nil)

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
TC-SESSION-HEALTH-005
Type: Positive
Title: setStateLocked is safe without metrics hook
Summary:
Verifies that state transitions are still applied when metrics hook is nil,
without panics or side effects beyond state mutation.

Validates:
  - nil metrics does not panic
  - state is updated directly
*/
func TestSetStateLockedIsSafeWithoutMetrics(t *testing.T) {
	m := &Manager{}

	m.setStateLocked(StateDraining)

	if m.health.State != StateDraining {
		t.Fatalf("expected state %q, got %q", StateDraining, m.health.State)
	}
}
