package session

import (
	"errors"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/routerarchitects/nats-agent-core/internal/runtimeerr"
)

const (
	defaultConnectTimeout   = 5 * time.Second
	defaultReconnectWait    = 2 * time.Second
	defaultMaxReconnects    = -1
	defaultJetStreamTimeout = 5 * time.Second

	defaultPublishTimeout   = 5 * time.Second
	defaultSubscribeTimeout = 5 * time.Second
	defaultKVTimeout        = 5 * time.Second
	defaultShutdownTimeout  = 10 * time.Second
	defaultHandlerWarnAfter = 5 * time.Second

	defaultPublishAttempts = 3
	defaultPublishBackoff  = 200 * time.Millisecond

	defaultKVBucket     = "cfg_desired"
	defaultKVKey        = "desired.%s"
	defaultKVHistory    = uint8(1)
	defaultKVReplicas   = 1
	defaultKVStorage    = "file"
	maxSupportedHistory = uint8(64)
)

// EffectiveConfig is the normalized runtime config used by session and KV paths.
type EffectiveConfig struct {
	Config Config
}

func normalizeConfig(cfg Config) (EffectiveConfig, error) {
	const op = "normalize_runtime_config"

	out := cfg

	if out.NATS.ConnectTimeout < 0 {
		return EffectiveConfig{}, validationError(op, "nats.connect_timeout cannot be negative")
	}
	if out.NATS.ReconnectWait < 0 {
		return EffectiveConfig{}, validationError(op, "nats.reconnect_wait cannot be negative")
	}
	if out.NATS.MaxReconnects < -1 {
		return EffectiveConfig{}, validationError(op, "nats.max_reconnects must be -1 or greater")
	}
	if out.NATS.ReconnectBufSize < 0 {
		return EffectiveConfig{}, validationError(op, "nats.reconnect_buf_size cannot be negative")
	}
	if out.JetStream.DefaultTimeout < 0 {
		return EffectiveConfig{}, validationError(op, "jetstream.default_timeout cannot be negative")
	}
	if out.Timeouts.PublishTimeout < 0 {
		return EffectiveConfig{}, validationError(op, "timeouts.publish_timeout cannot be negative")
	}
	if out.Timeouts.SubscribeTimeout < 0 {
		return EffectiveConfig{}, validationError(op, "timeouts.subscribe_timeout cannot be negative")
	}
	if out.Timeouts.KVTimeout < 0 {
		return EffectiveConfig{}, validationError(op, "timeouts.kv_timeout cannot be negative")
	}
	if out.Timeouts.ShutdownTimeout < 0 {
		return EffectiveConfig{}, validationError(op, "timeouts.shutdown_timeout cannot be negative")
	}
	if out.Timeouts.HandlerWarnAfter < 0 {
		return EffectiveConfig{}, validationError(op, "timeouts.handler_warn_after cannot be negative")
	}
	if out.Retry.PublishAttempts < 0 {
		return EffectiveConfig{}, validationError(op, "retry.publish_attempts cannot be negative")
	}
	if out.Retry.PublishBackoff < 0 {
		return EffectiveConfig{}, validationError(op, "retry.publish_backoff cannot be negative")
	}
	if out.KV.TTL < 0 {
		return EffectiveConfig{}, validationError(op, "kv.ttl cannot be negative")
	}
	if out.KV.MaxValueSize < 0 {
		return EffectiveConfig{}, validationError(op, "kv.max_value_size cannot be negative")
	}
	if out.KV.Replicas < 0 {
		return EffectiveConfig{}, validationError(op, "kv.replicas cannot be negative")
	}

	servers := make([]string, 0, len(out.NATS.Servers))
	for _, server := range out.NATS.Servers {
		trimmed := strings.TrimSpace(server)
		if trimmed == "" {
			continue
		}
		servers = append(servers, trimmed)
	}
	if len(servers) == 0 {
		servers = []string{nats.DefaultURL}
	}
	out.NATS.Servers = servers

	if out.NATS.ConnectTimeout == 0 {
		out.NATS.ConnectTimeout = defaultConnectTimeout
	}
	if out.NATS.MaxReconnects == 0 {
		out.NATS.MaxReconnects = defaultMaxReconnects
	}
	if out.NATS.ReconnectWait == 0 {
		out.NATS.ReconnectWait = defaultReconnectWait
	}
	if out.NATS.ReconnectBufSize == 0 {
		out.NATS.ReconnectBufSize = nats.DefaultReconnectBufSize
	}

	if out.JetStream.DefaultTimeout == 0 {
		out.JetStream.DefaultTimeout = defaultJetStreamTimeout
	}

	if out.Timeouts.PublishTimeout == 0 {
		out.Timeouts.PublishTimeout = defaultPublishTimeout
	}
	if out.Timeouts.SubscribeTimeout == 0 {
		out.Timeouts.SubscribeTimeout = defaultSubscribeTimeout
	}
	if out.Timeouts.KVTimeout == 0 {
		out.Timeouts.KVTimeout = defaultKVTimeout
	}
	if out.Timeouts.ShutdownTimeout == 0 {
		out.Timeouts.ShutdownTimeout = defaultShutdownTimeout
	}
	if out.Timeouts.HandlerWarnAfter == 0 {
		out.Timeouts.HandlerWarnAfter = defaultHandlerWarnAfter
	}

	if out.Retry.PublishAttempts == 0 {
		out.Retry.PublishAttempts = defaultPublishAttempts
	}
	if out.Retry.PublishBackoff == 0 {
		out.Retry.PublishBackoff = defaultPublishBackoff
	}

	out.KV.Bucket = strings.TrimSpace(out.KV.Bucket)
	if out.KV.Bucket == "" {
		out.KV.Bucket = defaultKVBucket
	}

	out.KV.KeyPattern = strings.TrimSpace(out.KV.KeyPattern)
	if out.KV.KeyPattern == "" {
		out.KV.KeyPattern = defaultKVKey
	}
	if err := validateKeyPattern(out.KV.KeyPattern); err != nil {
		return EffectiveConfig{}, validationError(op, err.Error())
	}

	if out.KV.History == 0 {
		out.KV.History = defaultKVHistory
	}
	if out.KV.History > maxSupportedHistory {
		return EffectiveConfig{}, validationError(op, "kv.history must be between 1 and 64")
	}
	if out.KV.Replicas == 0 {
		out.KV.Replicas = defaultKVReplicas
	}

	storage := strings.ToLower(strings.TrimSpace(out.KV.Storage))
	if storage == "" {
		storage = defaultKVStorage
	}
	if storage != "file" && storage != "memory" {
		return EffectiveConfig{}, validationError(op, "kv.storage must be file or memory")
	}
	out.KV.Storage = storage

	return EffectiveConfig{Config: out}, nil
}

func validationError(op, message string) error {
	return &runtimeerr.Error{
		Code:      runtimeerr.CodeValidation,
		Op:        op,
		Message:   message,
		Retryable: false,
	}
}

func validateKeyPattern(pattern string) error {
	if strings.ContainsAny(pattern, " \t\r\n") {
		return errors.New("kv.key_pattern cannot contain whitespace")
	}
	if strings.Count(pattern, "%s") != 1 {
		return errors.New("kv.key_pattern must contain exactly one %s placeholder")
	}
	residual := strings.ReplaceAll(pattern, "%s", "")
	if strings.Contains(residual, "%") {
		return errors.New("kv.key_pattern contains unsupported format directives")
	}
	return nil
}
