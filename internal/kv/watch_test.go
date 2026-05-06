package kv

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/routerarchitects/nats-agent-core/internal/runtimeerr"
)

/*
TC-KV-WATCH-001
Type: Negative
Title: WatchDesiredConfig rejects nil handler
Summary:
Verifies that watch registration fails fast when handler is nil and does not
perform runtime KV lookup.

Validates:
  - nil handler returns CodeValidation
  - runtime KeyValue lookup is skipped
*/
func TestWatchDesiredConfigRejectsNilHandler(t *testing.T) {
	runtime := &stubRuntimeProvider{}
	store, err := NewStore(runtime, nil)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	stop, err := store.WatchDesiredConfig(context.Background(), "vyos", nil)
	if stop != nil {
		t.Fatalf("expected nil stop func, got %#v", stop)
	}
	requireKVRuntimeError(t, err, runtimeerr.CodeValidation, "watch_desired_config", "watch handler is required")
	if runtime.keyValueCalls != 0 {
		t.Fatalf("expected runtime KeyValue lookup to be skipped, got %d calls", runtime.keyValueCalls)
	}
}

/*
TC-KV-WATCH-002
Type: Negative
Title: WatchDesiredConfig propagates runtime KeyValue failure
Summary:
Verifies that watch setup returns runtime KeyValue lookup failure directly when
runtime is disconnected.

Validates:
  - KeyValue failure returns CodeDisconnected
  - no watch is created
*/
func TestWatchDesiredConfigPropagatesRuntimeKeyValueFailure(t *testing.T) {
	runtime := &stubRuntimeProvider{
		kvErr: &runtimeerr.Error{Code: runtimeerr.CodeDisconnected, Op: "key_value", Message: "runtime not connected"},
	}
	store, err := NewStore(runtime, nil)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	stop, err := store.WatchDesiredConfig(context.Background(), "vyos", func(context.Context, StoredDesiredConfig) error { return nil })
	if stop != nil {
		t.Fatalf("expected nil stop func, got %#v", stop)
	}
	requireKVRuntimeError(t, err, runtimeerr.CodeDisconnected, "key_value", "runtime not connected")
}

/*
TC-KV-WATCH-003
Type: Negative
Title: WatchDesiredConfig wraps KV watch start failure
Summary:
Verifies that watch setup wraps KV Watch failures as typed KV read errors.

Validates:
  - watch setup failure returns CodeKVReadFailed
  - watch_desired_config op is preserved
*/
func TestWatchDesiredConfigWrapsWatchStartFailure(t *testing.T) {
	kvHandle := &stubKeyValue{
		watchFn: func(context.Context, string, ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
			return nil, errors.New("watch setup failed")
		},
	}
	store, err := NewStore(&stubRuntimeProvider{kv: kvHandle}, nil)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	stop, err := store.WatchDesiredConfig(context.Background(), "vyos", func(context.Context, StoredDesiredConfig) error { return nil })
	if stop != nil {
		t.Fatalf("expected nil stop func, got %#v", stop)
	}
	requireKVRuntimeError(t, err, runtimeerr.CodeKVReadFailed, "watch_desired_config", "failed to start desired-config watch")
}

/*
TC-KV-WATCH-004
Type: Positive
Title: WatchDesiredConfig delivers decoded entries to handler
Summary:
Verifies that watch consumer decodes desired-config updates and invokes handler
with decoded record and entry metadata.

Validates:
  - handler is invoked for valid entry payload
  - stored metadata bucket key revision and created time are mapped
*/
func TestWatchDesiredConfigDeliversDecodedEntriesToHandler(t *testing.T) {
	rec := validDesiredConfigRecordForTests()
	payload, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("failed to marshal record: %v", err)
	}
	createdAt := rec.Timestamp.Add(2 * time.Second)

	watcher := &stubKeyWatcher{updates: make(chan jetstream.KeyValueEntry, 2)}
	watcher.updates <- nil
	kvHandle := &stubKeyValue{
		watchFn: func(context.Context, string, ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
			return watcher, nil
		},
	}
	store, err := NewStore(&stubRuntimeProvider{kv: kvHandle}, nil)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	handlerCalled := make(chan StoredDesiredConfig, 1)
	stop, err := store.WatchDesiredConfig(context.Background(), "vyos", func(_ context.Context, stored StoredDesiredConfig) error {
		handlerCalled <- stored
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if stop == nil {
		t.Fatal("expected non-nil stop func")
	}

	watcher.updates <- stubKeyValueEntry{
		bucket:   "cfg_desired",
		key:      "desired.vyos",
		revision: 27,
		created:  createdAt,
		value:    payload,
	}

	select {
	case got := <-handlerCalled:
		if got.Record.UUID != rec.UUID || got.Record.RPCID != rec.RPCID || got.Record.Target != rec.Target {
			t.Fatalf("unexpected handler record %+v", got.Record)
		}
		if got.Bucket != "cfg_desired" || got.Key != "desired.vyos" || got.Revision != 27 {
			t.Fatalf("unexpected handler metadata bucket=%q key=%q revision=%d", got.Bucket, got.Key, got.Revision)
		}
		if !got.CreatedAt.Equal(createdAt) {
			t.Fatalf("expected CreatedAt %v, got %v", createdAt, got.CreatedAt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected handler to be called")
	}

	close(watcher.updates)
	if err := stop(); err != nil {
		t.Fatalf("expected stop to succeed, got %v", err)
	}
}

/*
TC-KV-WATCH-005
Type: Negative
Title: WatchDesiredConfig reports decode failures to error sink
Summary:
Verifies that watch consumer reports decode failures to error sink and skips
handler invocation for malformed entry payloads.

Validates:
  - malformed watch entry triggers CodeKVReadFailed error report
  - handler is not invoked for malformed payload
*/
func TestWatchDesiredConfigReportsDecodeFailuresToErrorSink(t *testing.T) {
	watcher := &stubKeyWatcher{updates: make(chan jetstream.KeyValueEntry, 2)}
	watcher.updates <- nil
	kvHandle := &stubKeyValue{
		watchFn: func(context.Context, string, ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
			return watcher, nil
		},
	}

	sinkErrCh := make(chan error, 1)
	store, err := NewStore(&stubRuntimeProvider{kv: kvHandle}, func(err error) {
		select {
		case sinkErrCh <- err:
		default:
		}
	})
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	var handlerCalls int32
	stop, err := store.WatchDesiredConfig(context.Background(), "vyos", func(context.Context, StoredDesiredConfig) error {
		atomic.AddInt32(&handlerCalls, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	watcher.updates <- stubKeyValueEntry{bucket: "cfg_desired", key: "desired.vyos", revision: 1, value: []byte(`{"version":`)}
	close(watcher.updates)

	var sinkErr error
	select {
	case sinkErr = <-sinkErrCh:
	case <-time.After(2 * time.Second):
		t.Fatal("expected decode failure to be reported to error sink")
	}
	if got := atomic.LoadInt32(&handlerCalls); got != 0 {
		t.Fatalf("expected handler not to be called, got %d calls", got)
	}
	requireKVRuntimeError(t, sinkErr, runtimeerr.CodeKVReadFailed, "watch_desired_config_decode", "failed to decode desired-config watch entry")

	if err := stop(); err != nil {
		t.Fatalf("expected stop to succeed, got %v", err)
	}
}

/*
TC-KV-WATCH-006
Type: Negative
Title: WatchDesiredConfig reports handler errors to error sink
Summary:
Verifies that watch consumer forwards handler-returned errors to error sink as
typed KV read failures.

Validates:
  - handler error is reported via error sink
  - watch_desired_config_handler op is preserved
*/
func TestWatchDesiredConfigReportsHandlerErrorsToErrorSink(t *testing.T) {
	rec := validDesiredConfigRecordForTests()
	payload, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("failed to marshal record: %v", err)
	}

	watcher := &stubKeyWatcher{updates: make(chan jetstream.KeyValueEntry, 2)}
	watcher.updates <- nil
	kvHandle := &stubKeyValue{
		watchFn: func(context.Context, string, ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
			return watcher, nil
		},
	}

	sinkErrCh := make(chan error, 1)
	store, err := NewStore(&stubRuntimeProvider{kv: kvHandle}, func(err error) {
		select {
		case sinkErrCh <- err:
		default:
		}
	})
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	stop, err := store.WatchDesiredConfig(context.Background(), "vyos", func(context.Context, StoredDesiredConfig) error {
		return errors.New("handler failed")
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	watcher.updates <- stubKeyValueEntry{bucket: "cfg_desired", key: "desired.vyos", revision: 1, value: payload}
	close(watcher.updates)

	var sinkErr error
	select {
	case sinkErr = <-sinkErrCh:
	case <-time.After(2 * time.Second):
		t.Fatal("expected handler error to be reported to error sink")
	}
	requireKVRuntimeError(t, sinkErr, runtimeerr.CodeKVReadFailed, "watch_desired_config_handler", "desired-config watch handler returned error")

	if err := stop(); err != nil {
		t.Fatalf("expected stop to succeed, got %v", err)
	}
}

/*
TC-KV-WATCH-007
Type: Positive
Title: WatchDesiredConfig stop function is idempotent
Summary:
Verifies that returned stop function can be called multiple times safely and
underlying watcher stop is performed only once.

Validates:
  - repeated stop calls return nil
  - underlying watcher Stop executes once
*/
func TestWatchDesiredConfigStopIsIdempotent(t *testing.T) {
	watcher := &stubKeyWatcher{updates: make(chan jetstream.KeyValueEntry, 1)}
	watcher.updates <- nil
	kvHandle := &stubKeyValue{
		watchFn: func(context.Context, string, ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
			return watcher, nil
		},
	}
	store, err := NewStore(&stubRuntimeProvider{kv: kvHandle}, nil)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	stop, err := store.WatchDesiredConfig(context.Background(), "vyos", func(context.Context, StoredDesiredConfig) error { return nil })
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if err := stop(); err != nil {
		t.Fatalf("expected first stop to succeed, got %v", err)
	}
	if err := stop(); err != nil {
		t.Fatalf("expected second stop to succeed, got %v", err)
	}
	watcher.mu.Lock()
	stopCalls := watcher.stopCall
	watcher.mu.Unlock()
	if stopCalls != 1 {
		t.Fatalf("expected watcher Stop to be called once, got %d", stopCalls)
	}
}

/*
TC-KV-WATCH-008
Type: Positive
Title: WatchDesiredConfig remains stoppable after parent context cancellation
Summary:
Verifies that parent context cancellation still stops the watch path and the
returned stop function completes cleanly without blocking.

Validates:
  - parent context cancellation propagates to watch cancellation
  - subsequent stop call returns without blocking
*/
func TestWatchDesiredConfigParentCancellationStopsWatchPath(t *testing.T) {
	watcher := &stubKeyWatcher{updates: make(chan jetstream.KeyValueEntry, 1)}
	watcher.updates <- nil
	kvHandle := &stubKeyValue{
		watchFn: func(context.Context, string, ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
			return watcher, nil
		},
	}
	store, err := NewStore(&stubRuntimeProvider{kv: kvHandle}, nil)
	if err != nil {
		t.Fatalf("expected nil constructor error, got %v", err)
	}

	parentCtx, cancel := context.WithCancel(context.Background())
	stop, err := store.WatchDesiredConfig(parentCtx, "vyos", func(context.Context, StoredDesiredConfig) error { return nil })
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	cancel()

	stopDone := make(chan struct{})
	go func() {
		_ = stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("expected stop to complete after parent cancellation")
	}
}
