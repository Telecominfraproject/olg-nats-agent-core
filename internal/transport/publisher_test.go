package transport

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Telecominfraproject/olg-nats-agent-core/internal/runtimeerr"
	"github.com/nats-io/nats.go"
)

type stubConnectionProvider struct {
	nc  *nats.Conn
	err error
}

func (s *stubConnectionProvider) Connection() (*nats.Conn, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.nc, nil
}

type publishMetricsCall struct {
	kind    string
	subject string
	result  string
}

type latencyMetricsCall struct {
	kind    string
	subject string
	d       time.Duration
}

type stubPublishMetrics struct {
	publishCalls []publishMetricsCall
	latencyCalls []latencyMetricsCall
}

func (m *stubPublishMetrics) IncPublish(kind, subject, result string) {
	m.publishCalls = append(m.publishCalls, publishMetricsCall{
		kind:    kind,
		subject: subject,
		result:  result,
	})
}

func (m *stubPublishMetrics) ObservePublishLatency(kind, subject string, d time.Duration) {
	m.latencyCalls = append(m.latencyCalls, latencyMetricsCall{
		kind:    kind,
		subject: subject,
		d:       d,
	})
}

func requireRuntimeError(t *testing.T, err error, code runtimeerr.Code, op, msgPart string) *runtimeerr.Error {
	t.Helper()

	if err == nil {
		t.Fatal("expected non-nil error")
	}
	var got *runtimeerr.Error
	if !errors.As(err, &got) {
		t.Fatalf("expected *runtimeerr.Error, got %T", err)
	}
	if got.Code != code {
		t.Fatalf("expected error code %q, got %q", code, got.Code)
	}
	if got.Op != op {
		t.Fatalf("expected error op %q, got %q", op, got.Op)
	}
	if msgPart != "" && !strings.Contains(got.Message, msgPart) {
		t.Fatalf("expected error message to contain %q, got %q", msgPart, got.Message)
	}
	return got
}

/*
TC-TRANSPORT-PUBLISHER-001
Type: Negative
Title: NewPublisher rejects a nil connection provider
Summary:
Verifies that publisher construction fails fast when the required connection
provider dependency is not supplied.

Validates:
  - constructor returns nil publisher
  - constructor returns runtime validation error
  - error op is new_publisher
*/
func TestNewPublisherRejectsNilConnectionProvider(t *testing.T) {
	pub, err := NewPublisher(nil, nil, nil, nil, nil)
	if pub != nil {
		t.Fatalf("expected nil publisher, got %#v", pub)
	}
	requireRuntimeError(t, err, runtimeerr.CodeValidation, "new_publisher", "connection provider is required")
}

/*
TC-TRANSPORT-PUBLISHER-002
Type: Negative
Title: Publish rejects invalid context, subject, and payload input
Summary:
Verifies that publish input validation rejects unusable caller context and
missing required subject or payload fields before transport work starts.

Validates:
  - nil context is rejected
  - canceled context is rejected
  - blank subject is rejected
  - nil payload is rejected
  - all failures use runtime validation errors
*/
func TestPublishRejectsInvalidInput(t *testing.T) {
	pub, err := NewPublisher(&stubConnectionProvider{nc: &nats.Conn{}}, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}
	pub.publishFn = func(*nats.Conn, string, []byte) error { return nil }
	pub.flushFn = func(*nats.Conn, context.Context) error { return nil }

	err = pub.Publish(nil, "publish_result", "result", "result.vyos", []byte(`{}`))
	requireRuntimeError(t, err, runtimeerr.CodeValidation, "publish_result", "context is required")

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	err = pub.Publish(canceled, "publish_result", "result", "result.vyos", []byte(`{}`))
	requireRuntimeError(t, err, runtimeerr.CodeValidation, "publish_result", "context is not usable")

	err = pub.Publish(context.Background(), "publish_result", "result", " ", []byte(`{}`))
	requireRuntimeError(t, err, runtimeerr.CodeValidation, "publish_result", "publish subject is required")

	err = pub.Publish(context.Background(), "publish_result", "result", "result.vyos", nil)
	requireRuntimeError(t, err, runtimeerr.CodeValidation, "publish_result", "payload is required")
}

/*
TC-TRANSPORT-PUBLISHER-003
Type: Negative
Title: Publish propagates connection provider errors
Summary:
Verifies that publish returns the underlying connection-provider error directly
when active connection lookup fails.

Validates:
  - connection provider failure is returned
  - disconnected runtime error remains reachable with errors.Is/errors.As
  - publish is not attempted when connection lookup fails
*/
func TestPublishPropagatesConnectionError(t *testing.T) {
	cause := &runtimeerr.Error{
		Code:      runtimeerr.CodeDisconnected,
		Op:        "connection",
		Message:   "runtime disconnected",
		Retryable: true,
	}
	pub, err := NewPublisher(&stubConnectionProvider{err: cause}, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	err = pub.Publish(context.Background(), "publish_result", "result", "result.vyos", []byte(`{}`))
	if !errors.Is(err, cause) {
		t.Fatalf("expected connection error cause, got %v", err)
	}
}

/*
TC-TRANSPORT-PUBLISHER-004
Type: Negative
Title: Publish wraps publish and flush failures
Summary:
Verifies that low-level publish and flush failures are wrapped as
CodePublishFailed while preserving the original cause and failure metrics.

Validates:
  - publish failure returns CodePublishFailed
  - flush failure returns CodePublishFailed
  - original failure cause remains wrapped
  - failure metrics are recorded
*/
func TestPublishWrapsPublishAndFlushFailures(t *testing.T) {
	metrics := &stubPublishMetrics{}
	pub, err := NewPublisher(&stubConnectionProvider{nc: &nats.Conn{}}, nil, nil, nil, metrics)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	publishCause := errors.New("publish failed")
	pub.publishFn = func(*nats.Conn, string, []byte) error { return publishCause }
	pub.flushFn = func(*nats.Conn, context.Context) error { return nil }

	err = pub.Publish(context.Background(), "publish_result", "result", "result.vyos", []byte(`{}`))
	got := requireRuntimeError(t, err, runtimeerr.CodePublishFailed, "publish_result", "publish failed")
	if !errors.Is(got, publishCause) {
		t.Fatal("expected wrapped publish cause to be reachable")
	}
	if len(metrics.publishCalls) != 1 || metrics.publishCalls[0].result != "failure" {
		t.Fatalf("expected one failure metric call, got %+v", metrics.publishCalls)
	}

	metrics.publishCalls = nil

	flushCause := errors.New("flush failed")
	pub.publishFn = func(*nats.Conn, string, []byte) error { return nil }
	pub.flushFn = func(*nats.Conn, context.Context) error { return flushCause }

	err = pub.Publish(context.Background(), "publish_result", "result", "result.vyos", []byte(`{}`))
	got = requireRuntimeError(t, err, runtimeerr.CodePublishFailed, "publish_result", "flush failed")
	if !errors.Is(got, flushCause) {
		t.Fatal("expected wrapped flush cause to be reachable")
	}
	if len(metrics.publishCalls) != 1 || metrics.publishCalls[0].result != "failure" {
		t.Fatalf("expected one failure metric call, got %+v", metrics.publishCalls)
	}
}

/*
TC-TRANSPORT-PUBLISHER-005
Type: Positive
Title: Publish success reports publish metrics and latency
Summary:
Verifies that a successful publish-and-flush path returns nil and records the
expected success and latency metrics through the injected hook.

Validates:
  - successful publish returns nil
  - success metric is recorded
  - publish latency metric is recorded
*/
func TestPublishSuccessReportsMetricsAndLatency(t *testing.T) {
	metrics := &stubPublishMetrics{}
	pub, err := NewPublisher(&stubConnectionProvider{nc: &nats.Conn{}}, nil, nil, nil, metrics)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	pub.publishFn = func(*nats.Conn, string, []byte) error { return nil }
	pub.flushFn = func(*nats.Conn, context.Context) error { return nil }

	err = pub.Publish(context.Background(), "publish_status", "status", "status.vyos", []byte(`{"ok":true}`))
	if err != nil {
		t.Fatalf("expected nil publish error, got %v", err)
	}

	if len(metrics.publishCalls) != 1 {
		t.Fatalf("expected one publish metric call, got %d", len(metrics.publishCalls))
	}
	if metrics.publishCalls[0].result != "success" {
		t.Fatalf("expected success metric result, got %q", metrics.publishCalls[0].result)
	}
	if len(metrics.latencyCalls) != 1 {
		t.Fatalf("expected one latency metric call, got %d", len(metrics.latencyCalls))
	}
}

/*
TC-TRANSPORT-PUBLISHER-006
Type: Positive
Title: Publish applies configured timeout when caller context has no deadline
Summary:
Verifies that the publish path derives a flush deadline from configured publish
timeout when the caller context does not provide one.

Validates:
  - publish succeeds
  - flush context receives a deadline from configured publish timeout
*/
func TestPublishUsesTimeoutWhenContextHasNoDeadline(t *testing.T) {
	pub, err := NewPublisher(&stubConnectionProvider{nc: &nats.Conn{}}, func() time.Duration { return 200 * time.Millisecond }, nil, nil, nil)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}
	pub.publishFn = func(*nats.Conn, string, []byte) error { return nil }

	flushObservedDeadline := false
	pub.flushFn = func(_ *nats.Conn, flushCtx context.Context) error {
		_, flushObservedDeadline = flushCtx.Deadline()
		return nil
	}

	err = pub.Publish(context.Background(), "publish_result", "result", "result.vyos", []byte(`{}`))
	if err != nil {
		t.Fatalf("expected nil publish error, got %v", err)
	}
	if !flushObservedDeadline {
		t.Fatal("expected flush context to include deadline from publish timeout")
	}
}

/*
TC-TRANSPORT-PUBLISHER-007
Type: Positive
Title: publishContext preserves an existing caller deadline
Summary:
Verifies that publishContext keeps the caller-provided deadline intact instead
of replacing it with configured publish timeout.

Validates:
  - existing caller deadline is preserved
  - publish timeout does not override an existing deadline
*/
func TestPublishContextRespectsExistingDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	flushCtx, cleanup := publishContext(ctx, 30*time.Second)
	defer cleanup()

	deadlineA, okA := ctx.Deadline()
	deadlineB, okB := flushCtx.Deadline()
	if okA != okB {
		t.Fatalf("expected deadline presence to match, ctx=%v flushCtx=%v", okA, okB)
	}
	if okA && !deadlineA.Equal(deadlineB) {
		t.Fatalf("expected flush context to preserve caller deadline, got %v vs %v", deadlineA, deadlineB)
	}
}

/*
TC-TRANSPORT-PUBLISHER-008
Type: Positive
Title: Publish succeeds on first try with no retries
Summary:
Verifies that Publish successfully delivers the payload on the first attempt
and does not execute any subsequent retry attempts.

Validates:
  - publish succeeds and returns nil error
  - publish function is executed exactly once
*/
func TestPublishSuccessOnFirstTry(t *testing.T) {
	metrics := &stubPublishMetrics{}
	attempts := 3
	backoff := 10 * time.Millisecond
	pub, err := NewPublisher(
		&stubConnectionProvider{nc: &nats.Conn{}},
		nil,
		func() int { return attempts },
		func() time.Duration { return backoff },
		metrics,
	)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	publishCalls := 0
	pub.publishFn = func(*nats.Conn, string, []byte) error {
		publishCalls++
		return nil
	}
	pub.flushFn = func(*nats.Conn, context.Context) error { return nil }

	err = pub.Publish(context.Background(), "publish_status", "status", "status.vyos", []byte(`{"ok":true}`))
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if publishCalls != 1 {
		t.Fatalf("expected exactly 1 publish call, got %d", publishCalls)
	}
}

/*
TC-TRANSPORT-PUBLISHER-009
Type: Positive
Title: Publish retries on transient error and succeeds
Summary:
Verifies that Publish retries the transmission after a transient failure
and successfully delivers the message on a subsequent attempt.

Validates:
  - publish succeeds after a transient failure
  - publish function is executed more than once (exactly twice)
*/
func TestPublishRetryAndSucceed(t *testing.T) {
	metrics := &stubPublishMetrics{}
	attempts := 3
	backoff := 10 * time.Millisecond
	pub, err := NewPublisher(
		&stubConnectionProvider{nc: &nats.Conn{}},
		nil,
		func() int { return attempts },
		func() time.Duration { return backoff },
		metrics,
	)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	publishCalls := 0
	pub.publishFn = func(*nats.Conn, string, []byte) error {
		publishCalls++
		if publishCalls == 1 {
			return errors.New("temporary error")
		}
		return nil
	}
	pub.flushFn = func(*nats.Conn, context.Context) error { return nil }

	err = pub.Publish(context.Background(), "publish_status", "status", "status.vyos", []byte(`{"ok":true}`))
	if err != nil {
		t.Fatalf("expected nil error on retry success, got %v", err)
	}
	if publishCalls != 2 {
		t.Fatalf("expected exactly 2 publish calls, got %d", publishCalls)
	}
}

/*
TC-TRANSPORT-PUBLISHER-010
Type: Negative
Title: Publish retries until attempts are exhausted
Summary:
Verifies that Publish retries a failing transmission until the maximum
configured attempts are exhausted, returning the final failure error.

Validates:
  - publish fails and returns CodePublishFailed
  - publish function is executed exactly the configured number of attempts
  - the final transient error is wrapped in the returned error
*/
func TestPublishRetryExhausted(t *testing.T) {
	metrics := &stubPublishMetrics{}
	attempts := 3
	backoff := 10 * time.Millisecond
	pub, err := NewPublisher(
		&stubConnectionProvider{nc: &nats.Conn{}},
		nil,
		func() int { return attempts },
		func() time.Duration { return backoff },
		metrics,
	)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	publishCalls := 0
	publishCause := errors.New("persistent error")
	pub.publishFn = func(*nats.Conn, string, []byte) error {
		publishCalls++
		return publishCause
	}
	pub.flushFn = func(*nats.Conn, context.Context) error { return nil }

	err = pub.Publish(context.Background(), "publish_status", "status", "status.vyos", []byte(`{"ok":true}`))
	got := requireRuntimeError(t, err, runtimeerr.CodePublishFailed, "publish_status", "publish failed")
	if !errors.Is(got, publishCause) {
		t.Fatalf("expected wrapped cause %v, got %v", publishCause, got)
	}
	if publishCalls != 3 {
		t.Fatalf("expected exactly 3 publish calls, got %d", publishCalls)
	}
}

/*
TC-TRANSPORT-PUBLISHER-011
Type: Negative
Title: Publish retries respect context cancellation
Summary:
Verifies that Publish aborts its retry loop immediately when the caller's
context is canceled or timed out.

Validates:
  - publish fails and returns CodePublishFailed with context.Canceled error
  - publish attempts stop immediately (only 1 attempt completed)
*/
func TestPublishContextCancellation(t *testing.T) {
	metrics := &stubPublishMetrics{}
	attempts := 3
	backoff := 100 * time.Millisecond
	pub, err := NewPublisher(
		&stubConnectionProvider{nc: &nats.Conn{}},
		nil,
		func() int { return attempts },
		func() time.Duration { return backoff },
		metrics,
	)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	publishCalls := 0
	pub.publishFn = func(*nats.Conn, string, []byte) error {
		publishCalls++
		return errors.New("temp error")
	}
	pub.flushFn = func(*nats.Conn, context.Context) error { return nil }

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel context after a small delay (during first backoff)
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	err = pub.Publish(ctx, "publish_status", "status", "status.vyos", []byte(`{"ok":true}`))
	if err == nil {
		t.Fatal("expected non-nil error after context cancellation")
	}

	got := requireRuntimeError(t, err, runtimeerr.CodePublishFailed, "publish_status", "publish aborted")
	if !errors.Is(got, context.Canceled) {
		t.Fatalf("expected wrapped context.Canceled error, got %v", got)
	}

	// Verify that it only attempted once before aborting
	if publishCalls != 1 {
		t.Fatalf("expected only 1 publish call before cancellation, got %d", publishCalls)
	}
}
