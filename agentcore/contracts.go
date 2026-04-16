package agentcore

import (
	"encoding/json"
	"time"
)

// BaseEnvelope contains common fields used across the shared wire contract.
// RPCID is used for request/response correlation across transport flows.
type BaseEnvelope struct {
	Version   string    `json:"version"`
	RPCID     string    `json:"rpc_id,omitempty"`
	Target    string    `json:"target"`
	Timestamp time.Time `json:"timestamp"`
}

// ConfigureCommand is the caller-facing configure submission payload.
// UUID is the opaque desired-config identity used for sync/apply decisions.
type ConfigureCommand struct {
	Version   string          `json:"version"`
	RPCID     string          `json:"rpc_id"`
	Target    string          `json:"target"`
	UUID      string          `json:"uuid"`
	Payload   json.RawMessage `json:"payload"`
	Timestamp time.Time       `json:"timestamp"`
}

// DesiredConfigRecord is the authoritative desired state stored in KV.
// UUID is the opaque desired-config identity used for sync/apply decisions.
type DesiredConfigRecord struct {
	Version   string          `json:"version"`
	RPCID     string          `json:"rpc_id"`
	Target    string          `json:"target"`
	UUID      string          `json:"uuid"`
	Payload   json.RawMessage `json:"payload"`
	Timestamp time.Time       `json:"timestamp"`
}

// ConfigureNotification is the lightweight post-store configure trigger.
// RPCID is used for correlation, while UUID is the desired-config identity
// agents compare against locally applied state.
type ConfigureNotification struct {
	Version     string    `json:"version"`
	RPCID       string    `json:"rpc_id"`
	Target      string    `json:"target"`
	CommandType string    `json:"command_type"`
	UUID        string    `json:"uuid"`
	KVBucket    string    `json:"kv_bucket"`
	KVKey       string    `json:"kv_key"`
	Timestamp   time.Time `json:"timestamp"`
}

// ActionCommand is the transient action request published on Core NATS.
type ActionCommand struct {
	Version     string          `json:"version"`
	RPCID       string          `json:"rpc_id"`
	Target      string          `json:"target"`
	CommandType string          `json:"command_type"`
	Action      string          `json:"action"`
	Payload     json.RawMessage `json:"payload"`
	Timestamp   time.Time       `json:"timestamp"`
}

// ResultEnvelope reports the outcome of configure or action processing.
type ResultEnvelope struct {
	Version     string          `json:"version"`
	RPCID       string          `json:"rpc_id"`
	Target      string          `json:"target"`
	CommandType string          `json:"command_type,omitempty"`
	UUID        string          `json:"uuid,omitempty"`
	Action      string          `json:"action,omitempty"`
	Result      string          `json:"result"`
	Message     string          `json:"message,omitempty"`
	ErrorCode   string          `json:"error_code,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	Timestamp   time.Time       `json:"timestamp"`
}

// StatusEnvelope reports target-owned status updates.
// UUID is optional for generic status, but should be provided for configure
// progress/status so consumers can correlate to desired-config identity.
type StatusEnvelope struct {
	Version   string          `json:"version"`
	RPCID     string          `json:"rpc_id,omitempty"`
	Target    string          `json:"target"`
	UUID      string          `json:"uuid,omitempty"`
	Status    string          `json:"status"`
	Stage     string          `json:"stage,omitempty"`
	Message   string          `json:"message,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Timestamp time.Time       `json:"timestamp"`
}

// StoredDesiredConfig represents a desired-config record loaded from KV.
// Revision is KV storage metadata only and is intended for diagnostics and
// storage introspection, not desired-state version semantics.
type StoredDesiredConfig struct {
	Record    DesiredConfigRecord `json:"record"`
	Bucket    string              `json:"bucket"`
	Key       string              `json:"key"`
	Revision  uint64              `json:"revision"`
	CreatedAt time.Time           `json:"created_at"`
}

// SubmissionAck reports acceptance of a configure or action submission.
// KVRevision is write metadata only and does not determine agent sync or apply
// correctness.
type SubmissionAck struct {
	Accepted   bool      `json:"accepted"`
	RPCID      string    `json:"rpc_id,omitempty"`
	Target     string    `json:"target,omitempty"`
	Subject    string    `json:"subject,omitempty"`
	AcceptedAt time.Time `json:"accepted_at,omitempty"`
	KVBucket   string    `json:"kv_bucket,omitempty"`
	KVKey      string    `json:"kv_key,omitempty"`
	KVRevision uint64    `json:"kv_revision,omitempty"`
}

// ConnectionState is the public state model exposed by HealthSnapshot.
type ConnectionState string

const (
	// StateNew indicates a client has been created but not started.
	StateNew ConnectionState = "new"
	// StateConnecting indicates the client is attempting initial connect.
	StateConnecting ConnectionState = "connecting"
	// StateConnected indicates the client is connected and usable.
	StateConnected ConnectionState = "connected"
	// StateReconnecting indicates the transport is recovering from a disconnect.
	StateReconnecting ConnectionState = "reconnecting"
	// StateDraining indicates graceful shutdown is in progress.
	StateDraining ConnectionState = "draining"
	// StateClosed indicates the client is fully closed.
	StateClosed ConnectionState = "closed"
	// StateDegraded indicates the client is reachable but not fully healthy.
	StateDegraded ConnectionState = "degraded"
)

// HealthSnapshot exposes a read-only view of client connection health.
type HealthSnapshot struct {
	State                   ConnectionState `json:"state"`
	ConnectedURL            string          `json:"connected_url,omitempty"`
	JetStreamReady          bool            `json:"jetstream_ready"`
	KVReady                 bool            `json:"kv_ready"`
	RegisteredSubscriptions int             `json:"registered_subscriptions"`
	ActiveSubscriptions     int             `json:"active_subscriptions"`
	LastError               string          `json:"last_error,omitempty"`
}
