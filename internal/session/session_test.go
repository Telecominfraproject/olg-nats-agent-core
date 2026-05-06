package session

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/routerarchitects/nats-agent-core/internal/runtimeerr"
)

func testSessionConfig() Config {
	return Config{
		AgentName: "agent-test",
		NATS: NATSConfig{
			Servers: []string{"nats://localhost:4222"},
		},
		KV: KVConfig{
			Bucket:     "cfg_desired",
			KeyPattern: "desired.%s",
		},
		Timeouts: TimeoutConfig{
			KVTimeout:       500 * time.Millisecond,
			ShutdownTimeout: 1 * time.Second,
		},
	}
}

func generateTestCertificatePEM(t *testing.T) (certPEM []byte, keyPEM []byte) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate private key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if certPEM == nil {
		t.Fatal("failed to encode certificate PEM")
	}

	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if keyPEM == nil {
		t.Fatal("failed to encode private key PEM")
	}

	return certPEM, keyPEM
}

/*
TC-SESSION-MANAGER-001
Type: Positive
Title: NewManager initializes health and exposes normalized accessors
Summary:
Verifies that manager construction starts in StateNew and exposes normalized
runtime accessors for health snapshot and desired-config settings.

Validates:
  - initial health state is new
  - DesiredConfigBucket and DesiredConfigKeyPattern accessors are populated
  - KVTimeout accessor returns normalized timeout value
*/
func TestNewManagerInitializesHealthAndAccessors(t *testing.T) {
	m, err := NewManager(testSessionConfig(), Hooks{})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	health := m.HealthSnapshot()
	if health.State != StateNew {
		t.Fatalf("expected initial state %q, got %q", StateNew, health.State)
	}
	if m.DesiredConfigBucket() != "cfg_desired" {
		t.Fatalf("expected bucket %q, got %q", "cfg_desired", m.DesiredConfigBucket())
	}
	if m.DesiredConfigKeyPattern() != "desired.%s" {
		t.Fatalf("expected key pattern %q, got %q", "desired.%s", m.DesiredConfigKeyPattern())
	}
	if m.KVTimeout() != 500*time.Millisecond {
		t.Fatalf("expected KVTimeout %v, got %v", 500*time.Millisecond, m.KVTimeout())
	}
}

/*
TC-SESSION-MANAGER-002
Type: Negative
Title: NewManager rejects invalid runtime configuration
Summary:
Verifies that manager construction fails when normalization rejects invalid
runtime config values.

Validates:
  - invalid config returns CodeValidation
  - normalize_runtime_config op is preserved
*/
func TestNewManagerRejectsInvalidRuntimeConfig(t *testing.T) {
	cfg := testSessionConfig()
	cfg.KV.History = 65

	_, err := NewManager(cfg, Hooks{})
	requireSessionRuntimeError(t, err, runtimeerr.CodeValidation, "normalize_runtime_config", "kv.history must be between 1 and 64")
}

/*
TC-SESSION-MANAGER-003
Type: Negative
Title: KeyValue before Start returns disconnected runtime error
Summary:
Verifies that KeyValue guard rejects access before an active runtime session is
established.

Validates:
  - KeyValue before Start returns CodeDisconnected
  - error op is key_value
*/
func TestKeyValueBeforeStartReturnsDisconnected(t *testing.T) {
	m, err := NewManager(testSessionConfig(), Hooks{})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	kv, err := m.KeyValue()
	if kv != nil {
		t.Fatalf("expected nil KeyValue handle, got %#v", kv)
	}
	requireSessionRuntimeError(t, err, runtimeerr.CodeDisconnected, "key_value", "client runtime is not connected")
}

/*
TC-SESSION-MANAGER-004
Type: Negative
Title: Start rejects canceled context before connection attempts
Summary:
Verifies that Start fails fast with a typed connection error when provided
context is already canceled.

Validates:
  - Start returns CodeConnectionFailed
  - error op is start
  - health remains in new state after fast-fail
*/
func TestStartRejectsCanceledContext(t *testing.T) {
	m, err := NewManager(testSessionConfig(), Hooks{})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = m.Start(ctx)
	requireSessionRuntimeError(t, err, runtimeerr.CodeConnectionFailed, "start", "start context is not usable")

	if got := m.HealthSnapshot().State; got != StateNew {
		t.Fatalf("expected state %q, got %q", StateNew, got)
	}
}

/*
TC-SESSION-MANAGER-005
Type: Positive
Title: Close is safe and idempotent before active connection
Summary:
Verifies that Close can be called repeatedly before Start without error and
sets health to closed.

Validates:
  - repeated Close calls return nil
  - health state is closed after Close
*/
func TestCloseBeforeStartIsSafeAndIdempotent(t *testing.T) {
	m, err := NewManager(testSessionConfig(), Hooks{})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if err := m.Close(context.Background()); err != nil {
		t.Fatalf("expected first Close to succeed, got %v", err)
	}
	if err := m.Close(context.Background()); err != nil {
		t.Fatalf("expected second Close to succeed, got %v", err)
	}

	if got := m.HealthSnapshot().State; got != StateClosed {
		t.Fatalf("expected state %q, got %q", StateClosed, got)
	}
}

/*
TC-SESSION-MANAGER-006
Type: Positive
Title: buildTLSConfig returns nil for nil or disabled TLS config
Summary:
Verifies that TLS helper skips TLS object creation when TLS config is nil or
explicitly disabled.

Validates:
  - nil config returns nil TLS config and nil error
  - disabled config returns nil TLS config and nil error
*/
func TestBuildTLSConfigReturnsNilForNilOrDisabled(t *testing.T) {
	tlsCfg, err := buildTLSConfig(nil)
	if err != nil {
		t.Fatalf("expected nil error for nil config, got %v", err)
	}
	if tlsCfg != nil {
		t.Fatalf("expected nil TLS config for nil input, got %#v", tlsCfg)
	}

	tlsCfg, err = buildTLSConfig(&TLSConfig{Enabled: false})
	if err != nil {
		t.Fatalf("expected nil error for disabled config, got %v", err)
	}
	if tlsCfg != nil {
		t.Fatalf("expected nil TLS config for disabled input, got %#v", tlsCfg)
	}
}

/*
TC-SESSION-MANAGER-007
Type: Negative
Title: buildTLSConfig rejects incomplete client certificate settings
Summary:
Verifies that TLS helper rejects one-sided cert/key configuration where only
one file is provided.

Validates:
  - cert without key returns error
  - key without cert returns error
*/
func TestBuildTLSConfigRejectsIncompleteClientCertificateSettings(t *testing.T) {
	_, err := buildTLSConfig(&TLSConfig{Enabled: true, CertFile: "cert.pem"})
	if err == nil {
		t.Fatal("expected error when key file is missing")
	}

	_, err = buildTLSConfig(&TLSConfig{Enabled: true, KeyFile: "key.pem"})
	if err == nil {
		t.Fatal("expected error when cert file is missing")
	}
}

/*
TC-SESSION-MANAGER-008
Type: Positive
Title: buildTLSConfig loads CA file into root certificate pool
Summary:
Verifies that TLS helper reads a CA PEM file and populates RootCAs when CAFile
is provided.

Validates:
  - CA PEM file is loaded successfully
  - returned TLS config contains non-nil RootCAs
*/
func TestBuildTLSConfigLoadsCAFile(t *testing.T) {
	certPEM, _ := generateTestCertificatePEM(t)

	tmpDir := t.TempDir()
	caPath := filepath.Join(tmpDir, "ca.pem")
	if err := os.WriteFile(caPath, certPEM, 0o600); err != nil {
		t.Fatalf("failed to write CA file: %v", err)
	}

	tlsCfg, err := buildTLSConfig(&TLSConfig{Enabled: true, CAFile: caPath})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if tlsCfg == nil {
		t.Fatal("expected non-nil TLS config")
	}
	if tlsCfg.RootCAs == nil {
		t.Fatal("expected RootCAs to be populated")
	}
}

/*
TC-SESSION-MANAGER-009
Type: Positive
Title: buildTLSConfig loads client certificate and key pair
Summary:
Verifies that TLS helper reads client certificate and key files and configures
client certificate chain on returned TLS config.

Validates:
  - valid cert and key pair load successfully
  - returned TLS config contains one client certificate
*/
func TestBuildTLSConfigLoadsClientCertificateAndKey(t *testing.T) {
	certPEM, keyPEM := generateTestCertificatePEM(t)

	tmpDir := t.TempDir()
	certPath := filepath.Join(tmpDir, "client-cert.pem")
	keyPath := filepath.Join(tmpDir, "client-key.pem")
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("failed to write cert file: %v", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("failed to write key file: %v", err)
	}

	tlsCfg, err := buildTLSConfig(&TLSConfig{Enabled: true, CertFile: certPath, KeyFile: keyPath})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if tlsCfg == nil {
		t.Fatal("expected non-nil TLS config")
	}
	if len(tlsCfg.Certificates) != 1 {
		t.Fatalf("expected one loaded certificate, got %d", len(tlsCfg.Certificates))
	}
}

/*
TC-SESSION-MANAGER-010
Type: Positive
Title: withKVTimeout preserves an existing context deadline
Summary:
Verifies that withKVTimeout returns the original context when a deadline is
already present.

Validates:
  - existing deadline is preserved
  - no replacement deadline is created
*/
func TestWithKVTimeoutPreservesExistingDeadline(t *testing.T) {
	m := &Manager{effective: EffectiveConfig{Config: Config{Timeouts: TimeoutConfig{KVTimeout: 25 * time.Millisecond}}}}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	wantDeadline, _ := ctx.Deadline()

	gotCtx, gotCancel := m.withKVTimeout(ctx)
	defer gotCancel()

	gotDeadline, ok := gotCtx.Deadline()
	if !ok {
		t.Fatal("expected deadline to exist")
	}
	if !gotDeadline.Equal(wantDeadline) {
		t.Fatalf("expected preserved deadline %v, got %v", wantDeadline, gotDeadline)
	}
}

/*
TC-SESSION-MANAGER-011
Type: Positive
Title: withKVTimeout adds deadline when context lacks one
Summary:
Verifies that withKVTimeout wraps contexts without deadline using manager KV
timeout configuration.

Validates:
  - returned context has deadline
  - deadline is within expected KV timeout window
*/
func TestWithKVTimeoutAddsDeadlineWhenMissing(t *testing.T) {
	timeout := 40 * time.Millisecond
	m := &Manager{effective: EffectiveConfig{Config: Config{Timeouts: TimeoutConfig{KVTimeout: timeout}}}}

	start := time.Now()
	ctx, cancel := m.withKVTimeout(context.Background())
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected deadline to be added")
	}
	if deadline.Before(start.Add(timeout-10*time.Millisecond)) || deadline.After(start.Add(timeout+50*time.Millisecond)) {
		t.Fatalf("expected deadline near %v from start, got %v", timeout, deadline.Sub(start))
	}
}

/*
TC-SESSION-MANAGER-012
Type: Positive
Title: drainConnection returns nil for nil connection
Summary:
Verifies that drain helper exits cleanly when no NATS connection exists.

Validates:
  - nil connection returns nil error
*/
func TestDrainConnectionReturnsNilForNilConnection(t *testing.T) {
	if err := drainConnection(context.Background(), nil, time.Second); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

/*
TC-SESSION-MANAGER-013
Type: Negative
Title: Start rejects retry_on_failed_connect for synchronous startup
Summary:
Verifies that synchronous Start explicitly rejects retry-on-failed-connect mode
to avoid partially connected startup behavior.

Validates:
  - Start returns CodeValidation
  - error op is start
*/
func TestStartRejectsRetryOnFailedConnectMode(t *testing.T) {
	cfg := testSessionConfig()
	cfg.NATS.RetryOnFailedConnect = true

	m, err := NewManager(cfg, Hooks{})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	err = m.Start(context.Background())
	requireSessionRuntimeError(t, err, runtimeerr.CodeValidation, "start", "retry_on_failed_connect is not supported")
}

/*
TC-SESSION-MANAGER-014
Type: Positive
Title: clampConnectTimeout respects shorter context deadline
Summary:
Verifies that connect timeout is clamped to the remaining context deadline when
the caller deadline is shorter than configured timeout.

Validates:
  - returned timeout is less than or equal to configured timeout
  - returned timeout is bounded by context deadline
*/
func TestClampConnectTimeoutRespectsShorterContextDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	got, err := clampConnectTimeout(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got <= 0 {
		t.Fatalf("expected positive timeout, got %v", got)
	}
	if got > 5*time.Second {
		t.Fatalf("expected timeout <= configured timeout, got %v", got)
	}
	if got > 100*time.Millisecond {
		t.Fatalf("expected timeout to be clamped near context deadline, got %v", got)
	}
}

/*
TC-SESSION-MANAGER-015
Type: Negative
Title: clampConnectTimeout fails for expired context deadline
Summary:
Verifies that connect timeout clamp returns a deadline error when context
deadline is already expired.

Validates:
  - expired deadline returns context.DeadlineExceeded
*/
func TestClampConnectTimeoutRejectsExpiredDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-1*time.Second))
	defer cancel()

	_, err := clampConnectTimeout(ctx, 2*time.Second)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
}

/*
TC-SESSION-MANAGER-016
Type: Positive
Title: Close waits for in-flight Start and leaves runtime closed
Summary:
Verifies that Close does not return before an in-flight Start has resolved and
that runtime references remain nil after close completes.

Validates:
  - Close blocks while Start is still connecting
  - after Start resolves, Close succeeds and runtime remains nil
  - final health state is closed
*/
func TestCloseWaitsForInFlightStartAndLeavesRuntimeClosed(t *testing.T) {
	originalConnect := natsConnect
	t.Cleanup(func() {
		natsConnect = originalConnect
	})

	connectEntered := make(chan struct{})
	releaseConnect := make(chan struct{})
	natsConnect = func(_ string, _ ...nats.Option) (*nats.Conn, error) {
		close(connectEntered)
		<-releaseConnect
		return nil, errors.New("connect failed after close request")
	}

	m, err := NewManager(testSessionConfig(), Hooks{})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	startDone := make(chan error, 1)
	go func() {
		startDone <- m.Start(context.Background())
	}()

	select {
	case <-connectEntered:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected Start to enter connect path")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- m.Close(context.Background())
	}()

	select {
	case err := <-closeDone:
		t.Fatalf("expected Close to wait for Start completion, got early return: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseConnect)

	startErr := <-startDone
	requireSessionRuntimeError(t, startErr, runtimeerr.CodeConnectionFailed, "start_connect", "failed to connect to NATS")

	if err := <-closeDone; err != nil {
		t.Fatalf("expected Close to succeed, got %v", err)
	}

	if m.nc != nil || m.js != nil || m.kv != nil {
		t.Fatalf("expected runtime handles to be nil after Close, got nc=%#v js=%#v kv=%#v", m.nc, m.js, m.kv)
	}
	if got := m.HealthSnapshot().State; got != StateClosed {
		t.Fatalf("expected state %q, got %q", StateClosed, got)
	}
}

/*
TC-SESSION-MANAGER-017
Type: Negative
Title: Start honors context cancellation while connect is blocked
Summary:
Verifies that Start returns promptly with a connection failure when context is
canceled while connect is still in progress.

Validates:
  - cancellation during connect returns CodeConnectionFailed
  - error op is start_connect
  - runtime handles are not published
*/
func TestStartHonorsCancellationDuringConnect(t *testing.T) {
	originalConnect := natsConnect
	t.Cleanup(func() {
		natsConnect = originalConnect
	})

	connectEntered := make(chan struct{})
	releaseConnect := make(chan struct{})
	natsConnect = func(_ string, _ ...nats.Option) (*nats.Conn, error) {
		close(connectEntered)
		<-releaseConnect
		return nil, errors.New("late connect failure")
	}

	m, err := NewManager(testSessionConfig(), Hooks{})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- m.Start(ctx)
	}()

	select {
	case <-connectEntered:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected Start to enter connect path")
	}

	cancel()

	startErr := <-errCh
	requireSessionRuntimeError(t, startErr, runtimeerr.CodeConnectionFailed, "start_connect", "failed to connect to NATS")
	close(releaseConnect)

	if m.nc != nil || m.js != nil || m.kv != nil {
		t.Fatalf("expected runtime handles to stay nil, got nc=%#v js=%#v kv=%#v", m.nc, m.js, m.kv)
	}
}
