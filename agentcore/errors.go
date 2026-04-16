package agentcore

import "fmt"

// Code identifies a machine-readable public failure category.
type Code string

const (
	// CodeValidation indicates a transport-level validation failure.
	CodeValidation Code = "validation"
	// CodeConnectionFailed indicates connection setup failed.
	CodeConnectionFailed Code = "connection_failed"
	// CodeDisconnected indicates an operation was attempted while disconnected.
	CodeDisconnected Code = "disconnected"
	// CodeJetStreamFailed indicates JetStream setup or usage failed.
	CodeJetStreamFailed Code = "jetstream_failed"
	// CodeKVStoreFailed indicates writing desired config to KV failed.
	CodeKVStoreFailed Code = "kv_store_failed"
	// CodeKVReadFailed indicates reading desired config from KV failed.
	CodeKVReadFailed Code = "kv_read_failed"
	// CodeConfigNotFound indicates desired config was not found.
	CodeConfigNotFound Code = "config_not_found"
	// CodePublishFailed indicates a publish operation failed.
	CodePublishFailed Code = "publish_failed"
	// CodeSubscribeFailed indicates a subscribe operation failed.
	CodeSubscribeFailed Code = "subscribe_failed"
	// CodeDecodeFailed indicates message decoding failed.
	CodeDecodeFailed Code = "decode_failed"
	// CodeEncodeFailed indicates message encoding failed.
	CodeEncodeFailed Code = "encode_failed"
	// CodeRegistryConflict indicates a subscription registration conflict.
	CodeRegistryConflict Code = "registry_conflict"
	// CodeShutdown indicates a shutdown-related failure.
	CodeShutdown Code = "shutdown"
	// CodeNotImplemented is used temporarily for bootstrap stubs.
	CodeNotImplemented Code = "not_implemented"
)

// Error is the typed public error model used by the library facade.
type Error struct {
	Code      Code   `json:"code"`
	Op        string `json:"op,omitempty"`
	Subject   string `json:"subject,omitempty"`
	Key       string `json:"key,omitempty"`
	Message   string `json:"message,omitempty"`
	Retryable bool   `json:"retryable"`
	Err       error  `json:"-"`
}

// Error satisfies the error interface.
func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Op == "" && e.Message == "" {
		return string(e.Code)
	}
	if e.Op == "" {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	if e.Message == "" {
		return fmt.Sprintf("%s: %s", e.Op, e.Code)
	}
	return fmt.Sprintf("%s: %s (%s)", e.Op, e.Message, e.Code)
}

// Unwrap exposes the underlying cause when present.
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Wrap creates a typed Error around an underlying cause.
func Wrap(code Code, op string, err error) *Error {
	return &Error{
		Code: code,
		Op:   op,
		Err:  err,
	}
}
