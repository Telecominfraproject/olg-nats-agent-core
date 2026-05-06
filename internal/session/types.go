package session

import "time"

// Logger is the session-layer logging hook.
type Logger interface {
	Debug(msg string, kv ...any)
	Info(msg string, kv ...any)
	Warn(msg string, kv ...any)
	Error(msg string, kv ...any)
}

// Metrics is the session-layer metrics hook.
type Metrics interface {
	IncConnect(result string)
	SetConnectionState(state string)
}

// Hooks bundles optional observability hooks consumed by the session layer.
type Hooks struct {
	Logger    Logger
	Metrics   Metrics
	ErrorSink func(error)
}

// Config is the runtime session configuration.
type Config struct {
	AgentName string
	NATS      NATSConfig
	JetStream JetStreamConfig
	KV        KVConfig
	Timeouts  TimeoutConfig
	Retry     RetryConfig
}

// NATSConfig contains NATS connectivity settings.
type NATSConfig struct {
	Servers              []string
	ClientName           string
	CredentialsFile      string
	NKeySeedFile         string
	UserJWTFile          string
	Username             string
	Password             string
	Token                string
	ConnectTimeout       time.Duration
	RetryOnFailedConnect bool
	MaxReconnects        int
	ReconnectWait        time.Duration
	ReconnectBufSize     int
	TLS                  *TLSConfig
}

// TLSConfig contains TLS settings for NATS connectivity.
type TLSConfig struct {
	Enabled            bool
	InsecureSkipVerify bool
	CAFile             string
	CertFile           string
	KeyFile            string
	ServerName         string
}

// JetStreamConfig contains JetStream defaults.
type JetStreamConfig struct {
	Domain         string
	APIPrefix      string
	DefaultTimeout time.Duration
}

// KVConfig defines desired-configuration storage settings.
type KVConfig struct {
	Bucket           string
	KeyPattern       string
	AutoCreateBucket bool
	History          uint8
	TTL              time.Duration
	MaxValueSize     int32
	Storage          string
	Replicas         int
}

// TimeoutConfig groups timeout values used by runtime operations.
type TimeoutConfig struct {
	PublishTimeout   time.Duration
	SubscribeTimeout time.Duration
	KVTimeout        time.Duration
	ShutdownTimeout  time.Duration
	HandlerWarnAfter time.Duration
}

// RetryConfig defines retry policy knobs for runtime operations.
type RetryConfig struct {
	PublishAttempts int
	PublishBackoff  time.Duration
}

// ConnectionState is the session-level state model.
type ConnectionState string

const (
	StateNew          ConnectionState = "new"
	StateConnecting   ConnectionState = "connecting"
	StateConnected    ConnectionState = "connected"
	StateReconnecting ConnectionState = "reconnecting"
	StateDraining     ConnectionState = "draining"
	StateClosed       ConnectionState = "closed"
	StateDegraded     ConnectionState = "degraded"
)

// HealthSnapshot exposes a read-only view of connection health.
type HealthSnapshot struct {
	State                   ConnectionState
	ConnectedURL            string
	JetStreamReady          bool
	KVReady                 bool
	RegisteredSubscriptions int
	ActiveSubscriptions     int
	LastError               string
}
