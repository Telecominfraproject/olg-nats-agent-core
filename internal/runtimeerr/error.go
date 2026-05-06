package runtimeerr

import "fmt"

// Code identifies an internal runtime failure category.
type Code string

const (
	CodeValidation       Code = "validation"
	CodeConnectionFailed Code = "connection_failed"
	CodeDisconnected     Code = "disconnected"
	CodeJetStreamFailed  Code = "jetstream_failed"
	CodeKVStoreFailed    Code = "kv_store_failed"
	CodeKVReadFailed     Code = "kv_read_failed"
	CodeConfigNotFound   Code = "config_not_found"
	CodeDecodeFailed     Code = "decode_failed"
	CodeEncodeFailed     Code = "encode_failed"
	CodeShutdown         Code = "shutdown"
)

// Error captures runtime-layer failures without importing the public facade.
type Error struct {
	Code      Code
	Op        string
	Subject   string
	Key       string
	Message   string
	Retryable bool
	Err       error
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
