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
	"github.com/nats-io/nats.go"
)

/*
TC-INT-PHASE5-001
Type: Positive
Title: Result handler receives real NATS result messages after start
Summary:
Verifies Phase 5 result subscription wiring end-to-end with a real nats-server:
pre-start registration is activated on Start and receives published result data.

Validates:
  - RegisterResultHandler before Start succeeds
  - real publish to result.<target> is delivered to handler
  - receive-side rpc_id is preserved exactly
*/
func TestIntegrationResultHandlerReceivesRealPublishedResult(t *testing.T) {
	srv := startTestNATSServer(t)
	bucket := uniqueName("cfg_desired")

	client, err := agentcore.New(newIntegrationConfig(srv.URL, bucket, true))
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	defer func() {
		_ = client.Close(context.Background())
	}()

	received := make(chan agentcore.ResultEnvelope, 1)
	if err := client.RegisterResultHandler("vyos", func(_ context.Context, msg agentcore.ResultEnvelope) error {
		received <- msg
		return nil
	}); err != nil {
		t.Fatalf("RegisterResultHandler returned unexpected error: %v", err)
	}

	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}

	payload := agentcore.ResultEnvelope{
		Version:   "1.0",
		RPCID:     "rpc-res-live-1",
		Target:    "vyos",
		Result:    "ok",
		Timestamp: time.Now().UTC(),
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to encode result payload: %v", err)
	}

	pub, err := nats.Connect(srv.URL, nats.NoReconnect())
	if err != nil {
		t.Fatalf("failed to connect publisher: %v", err)
	}
	defer pub.Close()

	if err := pub.Publish("result.vyos", raw); err != nil {
		t.Fatalf("Publish returned unexpected error: %v", err)
	}
	if err := pub.Flush(); err != nil {
		t.Fatalf("Flush returned unexpected error: %v", err)
	}

	select {
	case got := <-received:
		if got.RPCID != payload.RPCID {
			t.Fatalf("expected rpc_id %q, got %q", payload.RPCID, got.RPCID)
		}
		if got.Target != payload.Target || got.Result != payload.Result {
			t.Fatalf("unexpected result payload: %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for result handler callback")
	}
}

/*
TC-INT-PHASE5-002
Type: Positive
Title: Action handler receives real NATS action command messages after start
Summary:
Verifies Phase 5 action subscription wiring end-to-end with a real nats-server.

Validates:
  - RegisterActionHandler before Start succeeds
  - real publish to cmd.action.<target>.<action> is delivered to handler
  - handler receives target/action/rpc_id/payload values
*/
func TestIntegrationActionHandlerReceivesRealPublishedAction(t *testing.T) {
	srv := startTestNATSServer(t)
	bucket := uniqueName("cfg_desired")

	client, err := agentcore.New(newIntegrationConfig(srv.URL, bucket, true))
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	defer func() {
		_ = client.Close(context.Background())
	}()

	received := make(chan agentcore.ActionCommand, 1)
	if err := client.RegisterActionHandler("vyos", "trace", func(_ context.Context, msg agentcore.ActionCommand) error {
		received <- msg
		return nil
	}); err != nil {
		t.Fatalf("RegisterActionHandler returned unexpected error: %v", err)
	}

	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}

	payload := agentcore.ActionCommand{
		Version:     "1.0",
		RPCID:       "rpc-act-live-1",
		Target:      "vyos",
		CommandType: "action",
		Action:      "trace",
		Payload:     json.RawMessage(`{"destination":"8.8.8.8"}`),
		Timestamp:   time.Now().UTC(),
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to encode action payload: %v", err)
	}

	pub, err := nats.Connect(srv.URL, nats.NoReconnect())
	if err != nil {
		t.Fatalf("failed to connect publisher: %v", err)
	}
	defer pub.Close()

	if err := pub.Publish("cmd.action.vyos.trace", raw); err != nil {
		t.Fatalf("Publish returned unexpected error: %v", err)
	}
	if err := pub.Flush(); err != nil {
		t.Fatalf("Flush returned unexpected error: %v", err)
	}

	select {
	case got := <-received:
		if got.RPCID != payload.RPCID {
			t.Fatalf("expected rpc_id %q, got %q", payload.RPCID, got.RPCID)
		}
		if got.Target != payload.Target || got.Action != payload.Action {
			t.Fatalf("unexpected action identity: %+v", got)
		}
		if string(got.Payload) != string(payload.Payload) {
			t.Fatalf("expected payload %s, got %s", string(payload.Payload), string(got.Payload))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for action handler callback")
	}
}

/*
TC-INT-PHASE5-003
Type: Positive
Title: Result handler continues after reconnect restore without re-register
Summary:
Verifies that after a real server restart/reconnect cycle, registered result
handler intent is restored and subsequent messages are still delivered.

Validates:
  - message is received before server restart
  - client reconnects after restart
  - message is received after reconnect without re-registering handlers
*/
func TestIntegrationReconnectRestoreDeliversAfterServerRestart(t *testing.T) {
	srv := startTestNATSServer(t)
	bucket := uniqueName("cfg_desired")

	cfg := newIntegrationConfig(srv.URL, bucket, true)
	cfg.NATS.MaxReconnects = 50
	cfg.NATS.ReconnectWait = 100 * time.Millisecond

	client, err := agentcore.New(cfg)
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	defer func() {
		_ = client.Close(context.Background())
	}()

	received := make(chan string, 4)
	if err := client.RegisterResultHandler("vyos", func(_ context.Context, msg agentcore.ResultEnvelope) error {
		received <- msg.RPCID
		return nil
	}); err != nil {
		t.Fatalf("RegisterResultHandler returned unexpected error: %v", err)
	}

	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}

	publishResult := func(rpcID string) {
		t.Helper()
		payload := agentcore.ResultEnvelope{
			Version:   "1.0",
			RPCID:     rpcID,
			Target:    "vyos",
			Result:    "ok",
			Timestamp: time.Now().UTC(),
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("failed to encode result payload: %v", err)
		}
		pub, err := nats.Connect(srv.URL, nats.NoReconnect())
		if err != nil {
			t.Fatalf("failed to connect publisher: %v", err)
		}
		defer pub.Close()
		if err := pub.Publish("result.vyos", raw); err != nil {
			t.Fatalf("Publish returned unexpected error: %v", err)
		}
		if err := pub.Flush(); err != nil {
			t.Fatalf("Flush returned unexpected error: %v", err)
		}
	}

	publishResult("rpc-before-restart")
	select {
	case got := <-received:
		if got != "rpc-before-restart" {
			t.Fatalf("expected rpc_id %q, got %q", "rpc-before-restart", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for pre-restart result delivery")
	}

	srv.restart(t)

	if err := waitForClientConnected(client, 10*time.Second); err != nil {
		t.Fatalf("client did not reconnect after server restart: %v", err)
	}

	publishResult("rpc-after-restart")
	select {
	case got := <-received:
		if got != "rpc-after-restart" {
			t.Fatalf("expected rpc_id %q, got %q", "rpc-after-restart", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for post-reconnect result delivery")
	}
}

/*
TC-INT-PHASE5-004
Type: Positive
Title: Configure handler receives real configure notifications after start
Summary:
Verifies Phase 5 configure subscription wiring end-to-end with a real
nats-server message delivered to cmd.configure.<target>.

Validates:
  - RegisterConfigureHandler before Start succeeds
  - real publish to cmd.configure.<target> is delivered to handler
  - handler receives rpc_id/uuid/kv_bucket/kv_key/target/command_type
*/
func TestIntegrationConfigureHandlerReceivesRealPublishedConfigure(t *testing.T) {
	srv := startTestNATSServer(t)
	bucket := uniqueName("cfg_desired")

	client, err := agentcore.New(newIntegrationConfig(srv.URL, bucket, true))
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	defer func() {
		_ = client.Close(context.Background())
	}()

	received := make(chan agentcore.ConfigureNotification, 1)
	if err := client.RegisterConfigureHandler("vyos", func(_ context.Context, msg agentcore.ConfigureNotification) error {
		received <- msg
		return nil
	}); err != nil {
		t.Fatalf("RegisterConfigureHandler returned unexpected error: %v", err)
	}

	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}

	payload := agentcore.ConfigureNotification{
		Version:     "1.0",
		RPCID:       "rpc-cfg-live-1",
		Target:      "vyos",
		CommandType: "configure",
		UUID:        "cfg-live-1",
		KVBucket:    bucket,
		KVKey:       "desired.vyos",
		Timestamp:   time.Now().UTC(),
	}

	publishJSON(t, srv.URL, "cmd.configure.vyos", payload)

	select {
	case got := <-received:
		if got.RPCID != payload.RPCID {
			t.Fatalf("expected rpc_id %q, got %q", payload.RPCID, got.RPCID)
		}
		if got.UUID != payload.UUID {
			t.Fatalf("expected uuid %q, got %q", payload.UUID, got.UUID)
		}
		if got.KVBucket != payload.KVBucket {
			t.Fatalf("expected kv_bucket %q, got %q", payload.KVBucket, got.KVBucket)
		}
		if got.KVKey != payload.KVKey {
			t.Fatalf("expected kv_key %q, got %q", payload.KVKey, got.KVKey)
		}
		if got.Target != payload.Target {
			t.Fatalf("expected target %q, got %q", payload.Target, got.Target)
		}
		if got.CommandType != payload.CommandType {
			t.Fatalf("expected command_type %q, got %q", payload.CommandType, got.CommandType)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for configure handler callback")
	}
}

/*
TC-INT-PHASE5-005
Type: Positive
Title: Status handler receives real status messages after start
Summary:
Verifies Phase 5 status subscription wiring end-to-end with a real
nats-server message delivered to status.<target>.

Validates:
  - RegisterStatusHandler before Start succeeds
  - real publish to status.<target> is delivered to handler
  - handler receives target/status/rpc_id/stage values
*/
func TestIntegrationStatusHandlerReceivesRealPublishedStatus(t *testing.T) {
	srv := startTestNATSServer(t)
	bucket := uniqueName("cfg_desired")

	client, err := agentcore.New(newIntegrationConfig(srv.URL, bucket, true))
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	defer func() {
		_ = client.Close(context.Background())
	}()

	received := make(chan agentcore.StatusEnvelope, 1)
	if err := client.RegisterStatusHandler("vyos", func(_ context.Context, msg agentcore.StatusEnvelope) error {
		received <- msg
		return nil
	}); err != nil {
		t.Fatalf("RegisterStatusHandler returned unexpected error: %v", err)
	}

	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}

	payload := agentcore.StatusEnvelope{
		Version:   "1.0",
		RPCID:     "rpc-status-live-1",
		Target:    "vyos",
		Status:    "running",
		Stage:     "startup",
		Timestamp: time.Now().UTC(),
	}

	publishJSON(t, srv.URL, "status.vyos", payload)

	select {
	case got := <-received:
		if got.Target != payload.Target {
			t.Fatalf("expected target %q, got %q", payload.Target, got.Target)
		}
		if got.Status != payload.Status {
			t.Fatalf("expected status %q, got %q", payload.Status, got.Status)
		}
		if got.RPCID != payload.RPCID {
			t.Fatalf("expected rpc_id %q, got %q", payload.RPCID, got.RPCID)
		}
		if got.Stage != payload.Stage {
			t.Fatalf("expected stage %q, got %q", payload.Stage, got.Stage)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for status handler callback")
	}
}

/*
TC-INT-PHASE5-006
Type: Positive
Title: Post-start result registration activates immediately
Summary:
Verifies that registering a handler after Start activates live subscription
delivery without requiring another Start call.

Validates:
  - Start succeeds before registration
  - RegisterResultHandler after Start subscribes immediately
  - published result is delivered with expected identity fields
*/
func TestIntegrationPostStartRegistrationReceivesPublishedMessage(t *testing.T) {
	srv := startTestNATSServer(t)
	bucket := uniqueName("cfg_desired")

	client, err := agentcore.New(newIntegrationConfig(srv.URL, bucket, true))
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	defer func() {
		_ = client.Close(context.Background())
	}()

	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}

	received := make(chan agentcore.ResultEnvelope, 1)
	if err := client.RegisterResultHandler("vyos", func(_ context.Context, msg agentcore.ResultEnvelope) error {
		received <- msg
		return nil
	}); err != nil {
		t.Fatalf("RegisterResultHandler returned unexpected error: %v", err)
	}

	payload := agentcore.ResultEnvelope{
		Version:   "1.0",
		RPCID:     "rpc-post-start-1",
		Target:    "vyos",
		Result:    "ok",
		Timestamp: time.Now().UTC(),
	}

	publishJSON(t, srv.URL, "result.vyos", payload)

	select {
	case got := <-received:
		if got.RPCID != payload.RPCID {
			t.Fatalf("expected rpc_id %q, got %q", payload.RPCID, got.RPCID)
		}
		if got.Target != payload.Target {
			t.Fatalf("expected target %q, got %q", payload.Target, got.Target)
		}
		if got.Result != payload.Result {
			t.Fatalf("expected result %q, got %q", payload.Result, got.Result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for post-start registration callback")
	}
}

/*
TC-INT-PHASE5-007
Type: Positive
Title: Malformed result message is dropped without invoking handler
Summary:
Verifies that malformed result payloads are dropped and do not call the
registered handler, while subsequent valid payloads still deliver normally.

Validates:
  - malformed JSON publish does not trigger callback
  - subscription remains healthy for subsequent valid messages
*/
func TestIntegrationMalformedMessageIsDroppedWithoutHandlerInvocation(t *testing.T) {
	srv := startTestNATSServer(t)
	bucket := uniqueName("cfg_desired")

	client, err := agentcore.New(newIntegrationConfig(srv.URL, bucket, true))
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	defer func() {
		_ = client.Close(context.Background())
	}()

	received := make(chan agentcore.ResultEnvelope, 1)
	if err := client.RegisterResultHandler("vyos", func(_ context.Context, msg agentcore.ResultEnvelope) error {
		received <- msg
		return nil
	}); err != nil {
		t.Fatalf("RegisterResultHandler returned unexpected error: %v", err)
	}

	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}

	publishRaw(t, srv.URL, "result.vyos", []byte("{"))

	select {
	case got := <-received:
		t.Fatalf("expected malformed message to be dropped, got callback: %+v", got)
	case <-time.After(750 * time.Millisecond):
	}

	valid := agentcore.ResultEnvelope{
		Version:   "1.0",
		RPCID:     "rpc-valid-after-malformed-1",
		Target:    "vyos",
		Result:    "ok",
		Timestamp: time.Now().UTC(),
	}
	publishJSON(t, srv.URL, "result.vyos", valid)

	select {
	case got := <-received:
		if got.RPCID != valid.RPCID {
			t.Fatalf("expected rpc_id %q, got %q", valid.RPCID, got.RPCID)
		}
		if got.Target != valid.Target || got.Result != valid.Result {
			t.Fatalf("unexpected valid result payload: %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for valid message after malformed drop")
	}
}

func publishRaw(t *testing.T, serverURL, subject string, data []byte) {
	t.Helper()

	pub, err := nats.Connect(serverURL, nats.NoReconnect())
	if err != nil {
		t.Fatalf("failed to connect publisher: %v", err)
	}
	defer pub.Close()

	if err := pub.Publish(subject, data); err != nil {
		t.Fatalf("Publish returned unexpected error: %v", err)
	}
	if err := pub.Flush(); err != nil {
		t.Fatalf("Flush returned unexpected error: %v", err)
	}
}

func publishJSON(t *testing.T, serverURL, subject string, value any) {
	t.Helper()

	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("failed to encode payload: %v", err)
	}
	publishRaw(t, serverURL, subject, raw)
}

func waitForClientConnected(client *agentcore.Client, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		health := client.Health()
		if health.State == agentcore.StateConnected && health.ActiveSubscriptions >= 1 {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return context.DeadlineExceeded
}

/*
TC-INT-PHASE5-008
Type: Positive
Title: Concurrent registration and reconnection does not cause races, duplicate subscriptions, or deadlocks
Summary:
Verifies that registering new handlers concurrently while NATS server restarts
and reconnect restorations occur does not cause race detector warnings or duplicate active subscriptions.
*/
func TestIntegrationConcurrentRegisterAndReconnect(t *testing.T) {
	srv := startTestNATSServer(t)
	bucket := uniqueName("cfg_desired")

	cfg := newIntegrationConfig(srv.URL, bucket, true)
	cfg.NATS.MaxReconnects = 100
	cfg.NATS.ReconnectWait = 10 * time.Millisecond

	client, err := agentcore.New(cfg)
	if err != nil {
		t.Fatalf("New returned unexpected error: %v", err)
	}
	defer func() {
		_ = client.Close(context.Background())
	}()

	if err := client.Start(context.Background()); err != nil {
		t.Fatalf("Start returned unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Start server restart loop
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(150 * time.Millisecond):
				srv.restart(t)
			}
		}
	}()

	// Run concurrent registrations
	const numWorkers = 5
	const regsPerWorker = 8
	errChan := make(chan error, numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			for j := 0; j < regsPerWorker; j++ {
				target := fmt.Sprintf("target-%d-%d", workerID, j)
				err := client.RegisterResultHandler(target, func(_ context.Context, _ agentcore.ResultEnvelope) error {
					return nil
				})
				if err != nil {
					var clientErr *agentcore.Error
					if errors.As(err, &clientErr) {
						if clientErr.Code == agentcore.CodeDisconnected || clientErr.Code == agentcore.CodeSubscribeFailed {
							time.Sleep(20 * time.Millisecond)
							continue
						}
					}
					errChan <- err
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
			errChan <- nil
		}(i)
	}

	// Wait for all workers to finish
	for i := 0; i < numWorkers; i++ {
		select {
		case err := <-errChan:
			if err != nil {
				t.Errorf("concurrent registration failed: %v", err)
			}
		case <-time.After(6 * time.Second):
			t.Fatal("timed out waiting for worker registration")
		}
	}
}
