package session

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/routerarchitects/nats-agent-core/internal/runtimeerr"
)

func requireSessionRuntimeError(t *testing.T, err error, wantCode runtimeerr.Code, wantOp string, wantMsgPart string) *runtimeerr.Error {
	t.Helper()

	if err == nil {
		t.Fatal("expected non-nil error")
	}

	var got *runtimeerr.Error
	if !errors.As(err, &got) {
		t.Fatalf("expected *runtimeerr.Error, got %T", err)
	}
	if got.Code != wantCode {
		t.Fatalf("expected error code %q, got %q", wantCode, got.Code)
	}
	if got.Op != wantOp {
		t.Fatalf("expected error op %q, got %q", wantOp, got.Op)
	}
	if wantMsgPart != "" && !strings.Contains(got.Message, wantMsgPart) {
		t.Fatalf("expected error message to contain %q, got %q", wantMsgPart, got.Message)
	}

	return got
}

func baseSessionConfig() Config {
	return Config{
		AgentName: "agent-1",
		NATS: NATSConfig{
			Servers: []string{"nats://n1:4222"},
		},
		JetStream: JetStreamConfig{},
		KV:        KVConfig{},
		Timeouts:  TimeoutConfig{},
		Retry:     RetryConfig{},
	}
}

/*
TC-SESSION-OPTIONS-001
Type: Positive
Title: normalizeConfig applies defaults when fields are unset
Summary:
Verifies that runtime normalization applies the expected default values for
session connection, timeout, retry, and KV settings when config is mostly empty.

Validates:
  - empty server list falls back to nats.DefaultURL
  - default timeout and retry values are applied
  - default KV bucket key history replicas and storage are applied
*/
func TestNormalizeConfigAppliesDefaultsWhenUnset(t *testing.T) {
	cfg := Config{}

	effective, err := normalizeConfig(cfg)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	got := effective.Config
	if len(got.NATS.Servers) != 1 || got.NATS.Servers[0] != nats.DefaultURL {
		t.Fatalf("expected default server %q, got %+v", nats.DefaultURL, got.NATS.Servers)
	}
	if got.NATS.ConnectTimeout != defaultConnectTimeout {
		t.Fatalf("expected ConnectTimeout %v, got %v", defaultConnectTimeout, got.NATS.ConnectTimeout)
	}
	if got.NATS.ReconnectWait != defaultReconnectWait {
		t.Fatalf("expected ReconnectWait %v, got %v", defaultReconnectWait, got.NATS.ReconnectWait)
	}
	if got.NATS.MaxReconnects != defaultMaxReconnects {
		t.Fatalf("expected MaxReconnects %d, got %d", defaultMaxReconnects, got.NATS.MaxReconnects)
	}
	if got.NATS.ReconnectBufSize != nats.DefaultReconnectBufSize {
		t.Fatalf("expected ReconnectBufSize %d, got %d", nats.DefaultReconnectBufSize, got.NATS.ReconnectBufSize)
	}
	if got.JetStream.DefaultTimeout != defaultJetStreamTimeout {
		t.Fatalf("expected JetStream DefaultTimeout %v, got %v", defaultJetStreamTimeout, got.JetStream.DefaultTimeout)
	}
	if got.Timeouts.PublishTimeout != defaultPublishTimeout {
		t.Fatalf("expected PublishTimeout %v, got %v", defaultPublishTimeout, got.Timeouts.PublishTimeout)
	}
	if got.Timeouts.SubscribeTimeout != defaultSubscribeTimeout {
		t.Fatalf("expected SubscribeTimeout %v, got %v", defaultSubscribeTimeout, got.Timeouts.SubscribeTimeout)
	}
	if got.Timeouts.KVTimeout != defaultKVTimeout {
		t.Fatalf("expected KVTimeout %v, got %v", defaultKVTimeout, got.Timeouts.KVTimeout)
	}
	if got.Timeouts.ShutdownTimeout != defaultShutdownTimeout {
		t.Fatalf("expected ShutdownTimeout %v, got %v", defaultShutdownTimeout, got.Timeouts.ShutdownTimeout)
	}
	if got.Timeouts.HandlerWarnAfter != defaultHandlerWarnAfter {
		t.Fatalf("expected HandlerWarnAfter %v, got %v", defaultHandlerWarnAfter, got.Timeouts.HandlerWarnAfter)
	}
	if got.Retry.PublishAttempts != defaultPublishAttempts {
		t.Fatalf("expected PublishAttempts %d, got %d", defaultPublishAttempts, got.Retry.PublishAttempts)
	}
	if got.Retry.PublishBackoff != defaultPublishBackoff {
		t.Fatalf("expected PublishBackoff %v, got %v", defaultPublishBackoff, got.Retry.PublishBackoff)
	}
	if got.KV.Bucket != defaultKVBucket {
		t.Fatalf("expected KV Bucket %q, got %q", defaultKVBucket, got.KV.Bucket)
	}
	if got.KV.KeyPattern != defaultKVKey {
		t.Fatalf("expected KV KeyPattern %q, got %q", defaultKVKey, got.KV.KeyPattern)
	}
	if got.KV.History != defaultKVHistory {
		t.Fatalf("expected KV History %d, got %d", defaultKVHistory, got.KV.History)
	}
	if got.KV.Replicas != defaultKVReplicas {
		t.Fatalf("expected KV Replicas %d, got %d", defaultKVReplicas, got.KV.Replicas)
	}
	if got.KV.Storage != defaultKVStorage {
		t.Fatalf("expected KV Storage %q, got %q", defaultKVStorage, got.KV.Storage)
	}
}

/*
TC-SESSION-OPTIONS-002
Type: Positive
Title: normalizeConfig trims servers and preserves explicit overrides
Summary:
Verifies that runtime normalization drops empty server entries, trims values,
and preserves explicitly configured override values.

Validates:
  - server list is trimmed and empty entries are removed
  - explicit timeout retry and KV overrides remain intact
  - unspecified defaults still populate where needed
*/
func TestNormalizeConfigTrimsServersAndPreservesOverrides(t *testing.T) {
	cfg := baseSessionConfig()
	cfg.NATS.Servers = []string{"  nats://one:4222  ", "", "   ", "nats://two:4222"}
	cfg.NATS.ConnectTimeout = 11 * time.Second
	cfg.NATS.ReconnectWait = 7 * time.Second
	cfg.NATS.MaxReconnects = 9
	cfg.JetStream.DefaultTimeout = 4 * time.Second
	cfg.Timeouts.KVTimeout = 13 * time.Second
	cfg.Retry.PublishAttempts = 7
	cfg.Retry.PublishBackoff = 350 * time.Millisecond
	cfg.KV.Bucket = "custom_bucket"
	cfg.KV.KeyPattern = "custom.%s"
	cfg.KV.History = 5
	cfg.KV.Replicas = 3
	cfg.KV.Storage = "memory"

	effective, err := normalizeConfig(cfg)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	got := effective.Config

	if len(got.NATS.Servers) != 2 {
		t.Fatalf("expected 2 trimmed servers, got %d", len(got.NATS.Servers))
	}
	if got.NATS.Servers[0] != "nats://one:4222" || got.NATS.Servers[1] != "nats://two:4222" {
		t.Fatalf("unexpected normalized servers: %+v", got.NATS.Servers)
	}
	if got.NATS.ConnectTimeout != 11*time.Second {
		t.Fatalf("expected ConnectTimeout override to be preserved, got %v", got.NATS.ConnectTimeout)
	}
	if got.NATS.ReconnectWait != 7*time.Second {
		t.Fatalf("expected ReconnectWait override to be preserved, got %v", got.NATS.ReconnectWait)
	}
	if got.NATS.MaxReconnects != 9 {
		t.Fatalf("expected MaxReconnects override to be preserved, got %d", got.NATS.MaxReconnects)
	}
	if got.JetStream.DefaultTimeout != 4*time.Second {
		t.Fatalf("expected JetStream timeout override to be preserved, got %v", got.JetStream.DefaultTimeout)
	}
	if got.Timeouts.KVTimeout != 13*time.Second {
		t.Fatalf("expected KVTimeout override to be preserved, got %v", got.Timeouts.KVTimeout)
	}
	if got.Retry.PublishAttempts != 7 {
		t.Fatalf("expected PublishAttempts override to be preserved, got %d", got.Retry.PublishAttempts)
	}
	if got.Retry.PublishBackoff != 350*time.Millisecond {
		t.Fatalf("expected PublishBackoff override to be preserved, got %v", got.Retry.PublishBackoff)
	}
	if got.KV.Bucket != "custom_bucket" {
		t.Fatalf("expected KV bucket override to be preserved, got %q", got.KV.Bucket)
	}
	if got.KV.KeyPattern != "custom.%s" {
		t.Fatalf("expected KV key pattern override to be preserved, got %q", got.KV.KeyPattern)
	}
	if got.KV.History != 5 {
		t.Fatalf("expected KV history override to be preserved, got %d", got.KV.History)
	}
	if got.KV.Replicas != 3 {
		t.Fatalf("expected KV replicas override to be preserved, got %d", got.KV.Replicas)
	}
	if got.KV.Storage != "memory" {
		t.Fatalf("expected KV storage override to be preserved, got %q", got.KV.Storage)
	}
	if got.Timeouts.PublishTimeout != defaultPublishTimeout {
		t.Fatalf("expected unspecified PublishTimeout default %v, got %v", defaultPublishTimeout, got.Timeouts.PublishTimeout)
	}
}

/*
TC-SESSION-OPTIONS-003
Type: Negative
Title: normalizeConfig rejects negative timeout and retry values
Summary:
Verifies that runtime normalization rejects invalid negative timeout and retry
inputs across connection, operation timeout, and retry fields.

Validates:
  - negative timeout and retry values return CodeValidation
  - normalize_runtime_config op is preserved on validation errors
*/
func TestNormalizeConfigRejectsNegativeTimeoutAndRetryValues(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		msgPart string
	}{
		{name: "negative connect timeout", mutate: func(c *Config) { c.NATS.ConnectTimeout = -1 }, msgPart: "nats.connect_timeout cannot be negative"},
		{name: "negative reconnect wait", mutate: func(c *Config) { c.NATS.ReconnectWait = -1 }, msgPart: "nats.reconnect_wait cannot be negative"},
		{name: "negative reconnect buffer", mutate: func(c *Config) { c.NATS.ReconnectBufSize = -1 }, msgPart: "nats.reconnect_buf_size cannot be negative"},
		{name: "negative jetstream timeout", mutate: func(c *Config) { c.JetStream.DefaultTimeout = -1 }, msgPart: "jetstream.default_timeout cannot be negative"},
		{name: "negative publish timeout", mutate: func(c *Config) { c.Timeouts.PublishTimeout = -1 }, msgPart: "timeouts.publish_timeout cannot be negative"},
		{name: "negative subscribe timeout", mutate: func(c *Config) { c.Timeouts.SubscribeTimeout = -1 }, msgPart: "timeouts.subscribe_timeout cannot be negative"},
		{name: "negative kv timeout", mutate: func(c *Config) { c.Timeouts.KVTimeout = -1 }, msgPart: "timeouts.kv_timeout cannot be negative"},
		{name: "negative shutdown timeout", mutate: func(c *Config) { c.Timeouts.ShutdownTimeout = -1 }, msgPart: "timeouts.shutdown_timeout cannot be negative"},
		{name: "negative handler warn timeout", mutate: func(c *Config) { c.Timeouts.HandlerWarnAfter = -1 }, msgPart: "timeouts.handler_warn_after cannot be negative"},
		{name: "negative publish attempts", mutate: func(c *Config) { c.Retry.PublishAttempts = -1 }, msgPart: "retry.publish_attempts cannot be negative"},
		{name: "negative publish backoff", mutate: func(c *Config) { c.Retry.PublishBackoff = -1 }, msgPart: "retry.publish_backoff cannot be negative"},
		{name: "negative kv ttl", mutate: func(c *Config) { c.KV.TTL = -1 }, msgPart: "kv.ttl cannot be negative"},
		{name: "negative kv max value size", mutate: func(c *Config) { c.KV.MaxValueSize = -1 }, msgPart: "kv.max_value_size cannot be negative"},
		{name: "negative kv replicas", mutate: func(c *Config) { c.KV.Replicas = -1 }, msgPart: "kv.replicas cannot be negative"},
		{name: "invalid max reconnects less than -1", mutate: func(c *Config) { c.NATS.MaxReconnects = -2 }, msgPart: "nats.max_reconnects must be -1 or greater"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseSessionConfig()
			tc.mutate(&cfg)

			_, err := normalizeConfig(cfg)
			requireSessionRuntimeError(t, err, runtimeerr.CodeValidation, "normalize_runtime_config", tc.msgPart)
		})
	}
}

/*
TC-SESSION-OPTIONS-004
Type: Negative
Title: normalizeConfig rejects invalid KV history and storage
Summary:
Verifies that runtime normalization enforces KV-specific constraints for
history range and supported storage modes.

Validates:
  - kv.history values above supported max are rejected
  - kv.storage values outside file or memory are rejected
*/
func TestNormalizeConfigRejectsInvalidKVHistoryAndStorage(t *testing.T) {
	cfg := baseSessionConfig()
	cfg.KV.History = 65

	_, err := normalizeConfig(cfg)
	requireSessionRuntimeError(t, err, runtimeerr.CodeValidation, "normalize_runtime_config", "kv.history must be between 1 and 64")

	cfg = baseSessionConfig()
	cfg.KV.Storage = "disk"

	_, err = normalizeConfig(cfg)
	requireSessionRuntimeError(t, err, runtimeerr.CodeValidation, "normalize_runtime_config", "kv.storage must be file or memory")
}

/*
TC-SESSION-OPTIONS-005
Type: Negative
Title: normalizeConfig rejects invalid KV key patterns
Summary:
Verifies that runtime normalization rejects key patterns that violate required
placeholder, whitespace, or format-directive constraints.

Validates:
  - key patterns with whitespace are rejected
  - key patterns with wrong placeholder count are rejected
  - key patterns with unsupported format directives are rejected
*/
func TestNormalizeConfigRejectsInvalidKeyPatterns(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		msgPart string
	}{
		{name: "pattern with whitespace", pattern: "desired. %s", msgPart: "kv.key_pattern cannot contain whitespace"},
		{name: "pattern without placeholder", pattern: "desired.target", msgPart: "kv.key_pattern must contain exactly one %s placeholder"},
		{name: "pattern with unsupported directive", pattern: "desired.%s.%d", msgPart: "kv.key_pattern contains unsupported format directives"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseSessionConfig()
			cfg.KV.KeyPattern = tc.pattern

			_, err := normalizeConfig(cfg)
			requireSessionRuntimeError(t, err, runtimeerr.CodeValidation, "normalize_runtime_config", tc.msgPart)
		})
	}
}

/*
TC-SESSION-OPTIONS-006
Type: Positive
Title: validateKeyPattern accepts a valid pattern
Summary:
Verifies that key pattern helper validation accepts a correctly formatted
single-placeholder pattern used for desired config keys.

Validates:
  - one %s placeholder pattern passes validation
*/
func TestValidateKeyPatternAcceptsValidPattern(t *testing.T) {
	if err := validateKeyPattern("desired.%s"); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}
