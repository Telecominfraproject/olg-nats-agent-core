package agentcore

import "time"

// Logger is the caller-provided structured logging hook.
type Logger interface {
	Debug(msg string, kv ...any)
	Info(msg string, kv ...any)
	Warn(msg string, kv ...any)
	Error(msg string, kv ...any)
}

// Metrics is the caller-provided observability hook defined by the LLD.
type Metrics interface {
	IncConnect(result string)
	SetConnectionState(state string)
	IncPublish(kind, subject, result string)
	ObservePublishLatency(kind, subject string, d time.Duration)
	IncSubscribe(kind, subject, result string)
	IncKV(op, result string)
}
