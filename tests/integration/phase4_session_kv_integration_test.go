//go:build integration
// +build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Telecominfraproject/olg-nats-agent-core/agentcore"
)

/*
Run these integration tests with:

	go test -tags=integration ./tests/integration/...

These tests require a real `nats-server` binary in PATH and start it with `-js`.
*/

/*
TC-INT-PHASE4-001
Type: Positive
Title: Client start and close report real connected and closed health states
Summary:
Verifies a real client can connect to a real JetStream-enabled nats-server,
report connected runtime health, then close cleanly and report closed state.

Validates:
  - Start succeeds against real server
  - Health after start is connected with JetStream and KV ready
  - Close succeeds
  - Health after close is closed with readiness false
*/
func TestIntegrationClientStartCloseAndHealth(t *testing.T) {
	srv := startTestNATSServer(t)
	bucket := uniqueName("cfg_desired")

	client, err := agentcore.New(newIntegrationConfig(srv.URL, bucket, true))
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}

	startCtx, cancelStart := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelStart()
	if err := client.Start(startCtx); err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}

	health := client.Health()
	if health.State != agentcore.StateConnected {
		t.Fatalf("expected health state %q, got %q", agentcore.StateConnected, health.State)
	}
	if !health.JetStreamReady || !health.KVReady {
		t.Fatalf("expected JetStreamReady and KVReady true, got js=%v kv=%v", health.JetStreamReady, health.KVReady)
	}
	if health.ConnectedURL == "" {
		t.Fatal("expected non-empty ConnectedURL after successful Start")
	}

	closeCtx, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelClose()
	if err := client.Close(closeCtx); err != nil {
		t.Fatalf("Close returned unexpected error: %v", err)
	}

	health = client.Health()
	if health.State != agentcore.StateClosed {
		t.Fatalf("expected health state %q after close, got %q", agentcore.StateClosed, health.State)
	}
	if health.JetStreamReady || health.KVReady {
		t.Fatalf("expected readiness false after close, got js=%v kv=%v", health.JetStreamReady, health.KVReady)
	}
}

/*
TC-INT-PHASE4-002
Type: Mixed
Title: KV bucket startup behavior covers create bind and missing-bucket failure
Summary:
Verifies real startup behavior for desired-config bucket: auto-create when
missing, bind when already present, and explicit failure when missing and
auto-create is disabled.

Validates:
  - auto-create enabled creates missing bucket
  - later start with auto-create disabled binds existing bucket
  - missing bucket with auto-create disabled fails with JetStream setup error
*/
func TestIntegrationKVBucketCreateBindAndMissingFailure(t *testing.T) {
	srv := startTestNATSServer(t)
	bucket := uniqueName("cfg_desired")

	clientCreate, err := agentcore.New(newIntegrationConfig(srv.URL, bucket, true))
	if err != nil {
		t.Fatalf("New(create) returned unexpected error: %v", err)
	}
	if err := clientCreate.Start(context.Background()); err != nil {
		t.Fatalf("Start(create) returned unexpected error: %v", err)
	}
	if err := clientCreate.Close(context.Background()); err != nil {
		t.Fatalf("Close(create) returned unexpected error: %v", err)
	}

	clientBind, err := agentcore.New(newIntegrationConfig(srv.URL, bucket, false))
	if err != nil {
		t.Fatalf("New(bind) returned unexpected error: %v", err)
	}
	if err := clientBind.Start(context.Background()); err != nil {
		t.Fatalf("Start(bind) returned unexpected error: %v", err)
	}
	if err := clientBind.Close(context.Background()); err != nil {
		t.Fatalf("Close(bind) returned unexpected error: %v", err)
	}

	missingBucket := uniqueName("cfg_missing")
	clientMissing, err := agentcore.New(newIntegrationConfig(srv.URL, missingBucket, false))
	if err != nil {
		t.Fatalf("New(missing) returned unexpected error: %v", err)
	}

	err = clientMissing.Start(context.Background())
	requireClientErrorCode(t, err, agentcore.CodeJetStreamFailed)
}

/*
TC-INT-PHASE4-003
Type: Positive
Title: Store and load desired config round-trip through real KV
Summary:
Verifies desired config can be stored and loaded through real JetStream KV,
including record round-trip and returned metadata.

Validates:
  - StoreDesiredConfig persists record and returns bucket/key/revision metadata
  - LoadDesiredConfig returns equivalent logical record and revision metadata
  - payload round-trips through real KV
*/
func TestIntegrationStoreLoadDesiredConfigRoundTrip(t *testing.T) {
	srv := startTestNATSServer(t)
	bucket := uniqueName("cfg_desired")
	target := "vyos"

	client, err := agentcore.New(newIntegrationConfig(srv.URL, bucket, true))
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}
	defer func() {
		_ = client.Close(context.Background())
	}()

	rec := agentcore.DesiredConfigRecord{
		Version:   "1.0",
		RPCID:     "rpc-1",
		Target:    target,
		UUID:      "cfg-1",
		Payload:   json.RawMessage(`{"hostname":"edge-1"}`),
		Timestamp: time.Unix(1700000000, 123456000).UTC(),
	}

	stored, err := client.StoreDesiredConfig(context.Background(), rec)
	if err != nil {
		t.Fatalf("StoreDesiredConfig returned unexpected error: %v", err)
	}
	if stored.Bucket != bucket {
		t.Fatalf("expected bucket %q, got %q", bucket, stored.Bucket)
	}
	if stored.Key != fmt.Sprintf("desired.%s", target) {
		t.Fatalf("expected key %q, got %q", fmt.Sprintf("desired.%s", target), stored.Key)
	}
	if stored.Revision == 0 {
		t.Fatal("expected non-zero revision from StoreDesiredConfig")
	}
	if stored.CreatedAt.IsZero() {
		t.Fatal("expected non-zero CreatedAt from StoreDesiredConfig")
	}

	loaded, err := client.LoadDesiredConfig(context.Background(), target)
	if err != nil {
		t.Fatalf("LoadDesiredConfig returned unexpected error: %v", err)
	}
	if loaded.Record.Version != rec.Version || loaded.Record.RPCID != rec.RPCID || loaded.Record.Target != rec.Target || loaded.Record.UUID != rec.UUID {
		t.Fatalf("loaded record identity mismatch: got %+v", loaded.Record)
	}
	if string(loaded.Record.Payload) != string(rec.Payload) {
		t.Fatalf("expected payload %s, got %s", string(rec.Payload), string(loaded.Record.Payload))
	}
	if loaded.Revision == 0 {
		t.Fatal("expected non-zero revision from LoadDesiredConfig")
	}
}

/*
TC-INT-PHASE4-004
Type: Mixed
Title: Missing desired config load returns not-found error from real KV
Summary:
Verifies loading an unknown target against a real KV bucket surfaces the public
not-found error code.

Validates:
  - LoadDesiredConfig for missing target returns CodeConfigNotFound
*/
func TestIntegrationLoadMissingDesiredConfigReturnsNotFound(t *testing.T) {
	srv := startTestNATSServer(t)
	bucket := uniqueName("cfg_desired")

	client, err := agentcore.New(newIntegrationConfig(srv.URL, bucket, true))
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}
	defer func() {
		_ = client.Close(context.Background())
	}()

	_, err = client.LoadDesiredConfig(context.Background(), "missing-target")
	requireClientErrorCode(t, err, agentcore.CodeConfigNotFound)
}

/*
TC-INT-PHASE4-005
Type: Positive
Title: WatchDesiredConfig receives real KV updates and stop halts delivery
Summary:
Verifies desired-config watch receives real JetStream KV updates and that the
returned stop function stops further delivery cleanly.

Validates:
  - watch callback receives decoded stored record from real KV
  - stop function succeeds
  - no further callback delivery occurs after stop
*/
func TestIntegrationWatchDesiredConfigReceivesUpdatesAndStops(t *testing.T) {
	srv := startTestNATSServer(t)
	bucket := uniqueName("cfg_desired")
	target := "vyos"

	client, err := agentcore.New(newIntegrationConfig(srv.URL, bucket, true))
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}
	defer func() {
		_ = client.Close(context.Background())
	}()

	updates := make(chan agentcore.StoredDesiredConfig, 2)
	stop, err := client.WatchDesiredConfig(context.Background(), target, func(_ context.Context, stored agentcore.StoredDesiredConfig) error {
		updates <- stored
		return nil
	})
	if err != nil {
		t.Fatalf("WatchDesiredConfig returned unexpected error: %v", err)
	}

	rec1 := agentcore.DesiredConfigRecord{
		Version:   "1.0",
		RPCID:     "rpc-watch-1",
		Target:    target,
		UUID:      "cfg-watch-1",
		Payload:   json.RawMessage(`{"hostname":"edge-watch-1"}`),
		Timestamp: time.Now().UTC(),
	}
	_, err = client.StoreDesiredConfig(context.Background(), rec1)
	if err != nil {
		t.Fatalf("StoreDesiredConfig(rec1) returned unexpected error: %v", err)
	}

	select {
	case got := <-updates:
		if got.Record.UUID != rec1.UUID || got.Record.RPCID != rec1.RPCID || got.Record.Target != rec1.Target {
			t.Fatalf("unexpected watch record: %+v", got.Record)
		}
		if got.Key != fmt.Sprintf("desired.%s", target) {
			t.Fatalf("expected watch key %q, got %q", fmt.Sprintf("desired.%s", target), got.Key)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for watch update")
	}

	if err := stop(); err != nil {
		t.Fatalf("watch stop returned unexpected error: %v", err)
	}

	rec2 := agentcore.DesiredConfigRecord{
		Version:   "1.0",
		RPCID:     "rpc-watch-2",
		Target:    target,
		UUID:      "cfg-watch-2",
		Payload:   json.RawMessage(`{"hostname":"edge-watch-2"}`),
		Timestamp: time.Now().UTC(),
	}
	_, err = client.StoreDesiredConfig(context.Background(), rec2)
	if err != nil {
		t.Fatalf("StoreDesiredConfig(rec2) returned unexpected error: %v", err)
	}

	select {
	case got := <-updates:
		t.Fatalf("unexpected post-stop watch update: %+v", got)
	case <-time.After(400 * time.Millisecond):
	}
}

/*
TC-INT-PHASE4-006
Type: Positive
Title: StartupReconcile loads latest desired config from fresh runtime session
Summary:
Verifies startup reconciliation path by storing desired config with one client,
then loading latest desired config via StartupReconcile from a fresh client.

Validates:
  - fresh client Start succeeds against existing KV bucket
  - StartupReconcile returns latest desired config from real KV
*/
func TestIntegrationStartupReconcileFromFreshClient(t *testing.T) {
	srv := startTestNATSServer(t)
	bucket := uniqueName("cfg_desired")
	target := "vyos"

	writer, err := agentcore.New(newIntegrationConfig(srv.URL, bucket, true))
	if err != nil {
		t.Fatalf("New(writer) returned unexpected error: %v", err)
	}
	if err := writer.Start(context.Background()); err != nil {
		t.Fatalf("Start(writer) returned unexpected error: %v", err)
	}

	rec := agentcore.DesiredConfigRecord{
		Version:   "1.0",
		RPCID:     "rpc-reconcile-1",
		Target:    target,
		UUID:      "cfg-reconcile-1",
		Payload:   json.RawMessage(`{"hostname":"edge-reconcile"}`),
		Timestamp: time.Now().UTC(),
	}
	_, err = writer.StoreDesiredConfig(context.Background(), rec)
	if err != nil {
		t.Fatalf("StoreDesiredConfig(writer) returned unexpected error: %v", err)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatalf("Close(writer) returned unexpected error: %v", err)
	}

	reader, err := agentcore.New(newIntegrationConfig(srv.URL, bucket, false))
	if err != nil {
		t.Fatalf("New(reader) returned unexpected error: %v", err)
	}
	if err := reader.Start(context.Background()); err != nil {
		t.Fatalf("Start(reader) returned unexpected error: %v", err)
	}
	defer func() {
		_ = reader.Close(context.Background())
	}()

	reconciled, err := reader.StartupReconcile(context.Background(), target)
	if err != nil {
		t.Fatalf("StartupReconcile returned unexpected error: %v", err)
	}
	if reconciled.Record.UUID != rec.UUID || reconciled.Record.RPCID != rec.RPCID || reconciled.Record.Target != rec.Target {
		t.Fatalf("unexpected reconciled record: %+v", reconciled.Record)
	}
	if string(reconciled.Record.Payload) != string(rec.Payload) {
		t.Fatalf("expected reconcile payload %s, got %s", string(rec.Payload), string(reconciled.Record.Payload))
	}
}

func requireClientErrorCode(t *testing.T, err error, want agentcore.Code) *agentcore.Error {
	t.Helper()

	if err == nil {
		t.Fatal("expected non-nil error")
	}

	var got *agentcore.Error
	if !errors.As(err, &got) {
		t.Fatalf("expected *agentcore.Error, got %T", err)
	}
	if got.Code != want {
		t.Fatalf("expected error code %q, got %q (op=%q message=%q)", want, got.Code, got.Op, got.Message)
	}
	return got
}

/*
TC-INT-PHASE4-007
Type: Positive
Title: KV watch continues receiving updates after session reconnect
Summary:
Verifies that active desired-config watches are automatically restored on session reconnect
and continue receiving updates.
*/
func TestIntegrationWatchDesiredConfigReconnectRestore(t *testing.T) {
	srv := startTestNATSServer(t)
	bucket := uniqueName("cfg_desired")
	target := "vyos"

	cfg := newIntegrationConfig(srv.URL, bucket, true)
	cfg.NATS.MaxReconnects = 100
	cfg.NATS.ReconnectWait = 10 * time.Millisecond

	client, err := agentcore.New(cfg)
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}
	defer func() {
		_ = client.Close(context.Background())
	}()

	updates := make(chan agentcore.StoredDesiredConfig, 10)
	stop, err := client.WatchDesiredConfig(context.Background(), target, func(_ context.Context, stored agentcore.StoredDesiredConfig) error {
		updates <- stored
		return nil
	})
	if err != nil {
		t.Fatalf("WatchDesiredConfig returned unexpected error: %v", err)
	}
	defer stop()

	rec1 := agentcore.DesiredConfigRecord{
		Version:   "1.0",
		RPCID:     "rpc-restore-1",
		Target:    target,
		UUID:      "cfg-restore-1",
		Payload:   json.RawMessage(`{"val":1}`),
		Timestamp: time.Now().UTC(),
	}
	_, err = client.StoreDesiredConfig(context.Background(), rec1)
	if err != nil {
		t.Fatalf("StoreDesiredConfig(rec1) failed: %v", err)
	}

	select {
	case got := <-updates:
		if got.Record.UUID != rec1.UUID {
			t.Fatalf("expected uuid %q, got %q", rec1.UUID, got.Record.UUID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for update 1")
	}

	// Restart NATS server to trigger disconnect and reconnect
	srv.restart(t)

	if err := waitForClientKVConnected(client, 10*time.Second); err != nil {
		t.Fatalf("client did not reconnect: %v", err)
	}

	// Since the watch was restored, NATS JetStream KV Watch will re-deliver the latest value
	// currently stored in the KV bucket (which is rec1).
	select {
	case got := <-updates:
		if got.Record.UUID != rec1.UUID {
			t.Fatalf("expected re-delivered uuid %q on watch restore, got %q", rec1.UUID, got.Record.UUID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for re-delivered update 1 after reconnect")
	}

	// Store second config to verify watch continues working
	rec2 := agentcore.DesiredConfigRecord{
		Version:   "1.0",
		RPCID:     "rpc-restore-2",
		Target:    target,
		UUID:      "cfg-restore-2",
		Payload:   json.RawMessage(`{"val":2}`),
		Timestamp: time.Now().UTC(),
	}
	_, err = client.StoreDesiredConfig(context.Background(), rec2)
	if err != nil {
		t.Fatalf("StoreDesiredConfig(rec2) failed: %v", err)
	}

	select {
	case got := <-updates:
		if got.Record.UUID != rec2.UUID {
			t.Fatalf("expected uuid %q, got %q", rec2.UUID, got.Record.UUID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for update 2 after reconnect")
	}
}

/*
TC-INT-PHASE4-008
Type: Positive
Title: KV Watch Stop exits cleanly with timeout and does not block indefinitely on slow handlers
*/
func TestIntegrationWatchDesiredConfigStopTimeout(t *testing.T) {
	srv := startTestNATSServer(t)
	bucket := uniqueName("cfg_desired")
	target := "vyos"

	cfg := newIntegrationConfig(srv.URL, bucket, true)
	cfg.Timeouts.KVTimeout = 500 * time.Millisecond
	cfg.Timeouts.ShutdownTimeout = 500 * time.Millisecond

	errSinkChan := make(chan error, 10)
	client, err := agentcore.New(cfg, agentcore.WithErrorSink(func(e error) {
		errSinkChan <- e
	}))
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}
	defer func() {
		_ = client.Close(context.Background())
	}()

	started := make(chan struct{})
	handlerDone := make(chan struct{})
	stop, err := client.WatchDesiredConfig(context.Background(), target, func(ctx context.Context, _ agentcore.StoredDesiredConfig) error {
		close(started)
		time.Sleep(3 * time.Second)
		close(handlerDone)
		return nil
	})
	if err != nil {
		t.Fatalf("WatchDesiredConfig returned unexpected error: %v", err)
	}

	rec := agentcore.DesiredConfigRecord{
		Version:   "1.0",
		RPCID:     "rpc-slow-1",
		Target:    target,
		UUID:      "cfg-slow-1",
		Payload:   json.RawMessage(`{"val":1}`),
		Timestamp: time.Now().UTC(),
	}
	_, err = client.StoreDesiredConfig(context.Background(), rec)
	if err != nil {
		t.Fatalf("StoreDesiredConfig failed: %v", err)
	}

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for watch handler to start")
	}

	stopStarted := time.Now()
	stopErr := stop()
	stopDuration := time.Since(stopStarted)

	if stopDuration >= 2*time.Second {
		t.Fatalf("stop took %v, expected it to timeout and exit in ~500ms", stopDuration)
	}

	if stopErr == nil {
		t.Fatal("expected stop to return timeout error, got nil")
	}

	select {
	case reportedErr := <-errSinkChan:
		var clientErr *agentcore.Error
		if !errors.As(reportedErr, &clientErr) || clientErr.Code != agentcore.CodeKVReadFailed {
			t.Fatalf("unexpected error in sink: %v", reportedErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected timeout error to be reported to sink")
	}

	select {
	case <-handlerDone:
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for slow handler to finish")
	}
}

func waitForClientKVConnected(client *agentcore.Client, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		health := client.Health()
		if health.State == agentcore.StateConnected && health.KVReady {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return context.DeadlineExceeded
}
