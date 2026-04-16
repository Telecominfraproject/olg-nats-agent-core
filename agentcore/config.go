package agentcore

import "time"

// Config configures the reusable agentcore library used inside long-running
// agent processes.
type Config struct {
	AgentName string          `json:"agent_name"`
	Version   string          `json:"version"`
	NATS      NATSConfig      `json:"nats"`
	JetStream JetStreamConfig `json:"jetstream"`
	Subjects  SubjectConfig   `json:"subjects"`
	KV        KVConfig        `json:"kv"`
	Timeouts  TimeoutConfig   `json:"timeouts"`
	Retry     RetryConfig     `json:"retry"`
	Execution ExecutionConfig `json:"execution"`
	Observe   ObserveConfig   `json:"observe,omitempty"`
}

// NATSConfig contains core NATS connectivity settings.
type NATSConfig struct {
	Servers              []string      `json:"servers"`
	ClientName           string        `json:"client_name,omitempty"`
	CredentialsFile      string        `json:"credentials_file,omitempty"`
	NKeySeedFile         string        `json:"nkey_seed_file,omitempty"`
	UserJWTFile          string        `json:"user_jwt_file,omitempty"`
	Username             string        `json:"username,omitempty"`
	Password             string        `json:"password,omitempty"`
	Token                string        `json:"token,omitempty"`
	ConnectTimeout       time.Duration `json:"connect_timeout,omitempty"`
	RetryOnFailedConnect bool          `json:"retry_on_failed_connect,omitempty"`
	MaxReconnects        int           `json:"max_reconnects,omitempty"`
	ReconnectWait        time.Duration `json:"reconnect_wait,omitempty"`
	ReconnectBufSize     int           `json:"reconnect_buf_size,omitempty"`
	TLS                  *TLSConfig    `json:"tls,omitempty"`
}

// TLSConfig contains public TLS configuration for the NATS connection.
type TLSConfig struct {
	Enabled            bool   `json:"enabled,omitempty"`
	InsecureSkipVerify bool   `json:"insecure_skip_verify,omitempty"`
	CAFile             string `json:"ca_file,omitempty"`
	CertFile           string `json:"cert_file,omitempty"`
	KeyFile            string `json:"key_file,omitempty"`
	ServerName         string `json:"server_name,omitempty"`
}

// JetStreamConfig contains JetStream-level configuration defaults.
type JetStreamConfig struct {
	Domain         string        `json:"domain,omitempty"`
	APIPrefix      string        `json:"api_prefix,omitempty"`
	DefaultTimeout time.Duration `json:"default_timeout,omitempty"`
}

// SubjectConfig defines the subject patterns used by the shared contract.
type SubjectConfig struct {
	ConfigurePattern string `json:"configure_pattern"`
	ActionPattern    string `json:"action_pattern"`
	ResultPattern    string `json:"result_pattern"`
	StatusPattern    string `json:"status_pattern"`
	HealthPattern    string `json:"health_pattern"`
}

// KVConfig defines desired-configuration storage settings.
type KVConfig struct {
	Bucket           string        `json:"bucket"`
	KeyPattern       string        `json:"key_pattern"`
	AutoCreateBucket bool          `json:"auto_create_bucket,omitempty"`
	History          uint8         `json:"history,omitempty"`
	TTL              time.Duration `json:"ttl,omitempty"`
	MaxValueSize     int32         `json:"max_value_size,omitempty"`
	Storage          string        `json:"storage,omitempty"`
	Replicas         int           `json:"replicas,omitempty"`
}

// TimeoutConfig groups public timeout values used by networked operations.
type TimeoutConfig struct {
	PublishTimeout   time.Duration `json:"publish_timeout,omitempty"`
	SubscribeTimeout time.Duration `json:"subscribe_timeout,omitempty"`
	KVTimeout        time.Duration `json:"kv_timeout,omitempty"`
	ShutdownTimeout  time.Duration `json:"shutdown_timeout,omitempty"`
	HandlerWarnAfter time.Duration `json:"handler_warn_after,omitempty"`
}

// RetryConfig defines retry policy knobs for public operations.
type RetryConfig struct {
	PublishAttempts int           `json:"publish_attempts,omitempty"`
	PublishBackoff  time.Duration `json:"publish_backoff,omitempty"`
}

// ExecutionConfig describes handler execution mode without exposing internals.
type ExecutionConfig struct {
	HandlerMode string `json:"handler_mode,omitempty"`
}

// ObserveConfig allows optional observability hooks to be supplied at config time.
type ObserveConfig struct {
	Logger  Logger  `json:"-"`
	Metrics Metrics `json:"-"`
}
