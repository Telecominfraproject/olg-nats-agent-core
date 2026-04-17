package contract

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

    "github.com/Telecominfraproject/olg-nats-agent-core/agentcore"
)

func contractTestTime() time.Time {
	return time.Unix(1700000000, 0).UTC()
}

func rawJSON(s string) json.RawMessage {
	return json.RawMessage(s)
}

func validBaseEnvelope() agentcore.BaseEnvelope {
	return agentcore.BaseEnvelope{
		Version:   "1.0",
		RPCID:     "rpc-base-1",
		Target:    "vyos",
		Timestamp: contractTestTime(),
	}
}

func validConfigureCommand() agentcore.ConfigureCommand {
	return agentcore.ConfigureCommand{
		Version:   "1.0",
		RPCID:     "rpc-config-1",
		Target:    "vyos",
		UUID:      "cfg-001",
		Payload:   rawJSON(`{"hostname":"router-1"}`),
		Timestamp: contractTestTime(),
	}
}

func validDesiredConfigRecord() agentcore.DesiredConfigRecord {
	return agentcore.DesiredConfigRecord{
		Version:   "1.0",
		RPCID:     "rpc-record-1",
		Target:    "vyos",
		UUID:      "cfg-001",
		Payload:   rawJSON(`{"interfaces":[]}`),
		Timestamp: contractTestTime(),
	}
}

func validConfigureNotification() agentcore.ConfigureNotification {
	return agentcore.ConfigureNotification{
		Version:     "1.0",
		RPCID:       "rpc-notify-1",
		Target:      "vyos",
		CommandType: "configure",
		UUID:        "cfg-001",
		KVBucket:    "cfg_desired",
		KVKey:       "desired.vyos",
		Timestamp:   contractTestTime(),
	}
}

func validActionCommand() agentcore.ActionCommand {
	return agentcore.ActionCommand{
		Version:     "1.0",
		RPCID:       "rpc-action-1",
		Target:      "vyos",
		CommandType: "action",
		Action:      "trace",
		Payload:     rawJSON(`{"destination":"8.8.8.8"}`),
		Timestamp:   contractTestTime(),
	}
}

func validResultEnvelope() agentcore.ResultEnvelope {
	return agentcore.ResultEnvelope{
		Version:     "1.0",
		RPCID:       "rpc-result-1",
		Target:      "vyos",
		CommandType: "configure",
		UUID:        "cfg-001",
		Action:      "trace",
		Result:      "success",
		Message:     "applied",
		ErrorCode:   "",
		Payload:     rawJSON(`{"applied":true}`),
		Timestamp:   contractTestTime(),
	}
}

func validStatusEnvelope() agentcore.StatusEnvelope {
	return agentcore.StatusEnvelope{
		Version:   "1.0",
		RPCID:     "rpc-status-1",
		Target:    "vyos",
		UUID:      "cfg-001",
		Status:    "running",
		Stage:     "startup",
		Message:   "agent ready",
		Payload:   rawJSON(`{"ready":true}`),
		Timestamp: contractTestTime(),
	}
}

func validStoredDesiredConfig() agentcore.StoredDesiredConfig {
	return agentcore.StoredDesiredConfig{
		Record:    validDesiredConfigRecord(),
		Bucket:    "cfg_desired",
		Key:       "desired.vyos",
		Revision:  7,
		CreatedAt: contractTestTime(),
	}
}

func requireAgentcoreError(t *testing.T, err error, wantCode agentcore.Code, wantOp string, wantMsgPart string) *agentcore.Error {
	t.Helper()

	if err == nil {
		t.Fatal("expected non-nil error")
	}

	var got *agentcore.Error
	if !errors.As(err, &got) {
		t.Fatalf("expected *agentcore.Error, got %T", err)
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

/*
TC-CONTRACT-HELPERS-001
Type: Positive
Title: validationError returns a typed validation error
Summary:
Verifies that the shared helper for validation failures produces the expected
typed public error with validation semantics.

Validates:
  - returned error is *agentcore.Error
  - error code is CodeValidation
  - error op and message are preserved
*/
func TestValidationErrorReturnsTypedValidationError(t *testing.T) {
	err := validationError("validate_configure_command", "rpc_id is required")

	got := requireAgentcoreError(t, err, agentcore.CodeValidation, "validate_configure_command", "rpc_id is required")
	if got.Retryable {
		t.Fatal("expected validation error to be non-retryable")
	}
}

/*
TC-CONTRACT-HELPERS-002
Type: Positive
Title: decodeError wraps the underlying decode cause
Summary:
Verifies that the shared decode helper returns a typed decode failure while
preserving the original lower-level cause.

Validates:
  - returned error is *agentcore.Error
  - error code is CodeDecodeFailed
  - underlying cause is preserved
*/
func TestDecodeErrorWrapsUnderlyingCause(t *testing.T) {
	cause := errors.New("unexpected end of JSON input")

	err := decodeError("decode_configure_command", cause)

	got := requireAgentcoreError(t, err, agentcore.CodeDecodeFailed, "decode_configure_command", "failed to decode JSON payload")
	if !errors.Is(got, cause) {
		t.Fatal("expected wrapped decode cause to be reachable via errors.Is")
	}
}

/*
TC-CONTRACT-HELPERS-003
Type: Positive
Title: encodeError wraps the underlying encode cause
Summary:
Verifies that the shared encode helper returns a typed encode failure while
preserving the original lower-level cause.

Validates:
  - returned error is *agentcore.Error
  - error code is CodeEncodeFailed
  - underlying cause is preserved
*/
func TestEncodeErrorWrapsUnderlyingCause(t *testing.T) {
	cause := errors.New("json: unsupported type")

	err := encodeError("encode_configure_command", cause)

	got := requireAgentcoreError(t, err, agentcore.CodeEncodeFailed, "encode_configure_command", "failed to encode JSON payload")
	if !errors.Is(got, cause) {
		t.Fatal("expected wrapped encode cause to be reachable via errors.Is")
	}
}

/*
TC-CONTRACT-HELPERS-004
Type: Positive
Title: requiredString accepts a normal non-empty value
Summary:
Verifies that required string validation succeeds for a non-empty value.

Validates:
  - no error is returned for a valid required string
*/
func TestRequiredStringAcceptsNonEmptyValue(t *testing.T) {
	if err := requiredString("validate_configure_command", "target", "vyos"); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

/*
TC-CONTRACT-HELPERS-005
Type: Negative
Title: requiredString rejects whitespace-only values
Summary:
Verifies that required string validation trims whitespace and rejects empty or
whitespace-only values.

Validates:
  - whitespace-only value returns CodeValidation
  - error op and message are correct
*/
func TestRequiredStringRejectsWhitespaceOnlyValue(t *testing.T) {
	err := requiredString("validate_configure_command", "target", "   ")

	requireAgentcoreError(t, err, agentcore.CodeValidation, "validate_configure_command", "target is required")
}

/*
TC-CONTRACT-HELPERS-006
Type: Positive
Title: optionalString allows an empty optional field
Summary:
Verifies that optional string validation permits a truly empty optional field.

Validates:
  - empty string returns no error
*/
func TestOptionalStringAllowsEmptyValue(t *testing.T) {
	if err := optionalString("validate_status_envelope", "stage", ""); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

/*
TC-CONTRACT-HELPERS-007
Type: Negative
Title: optionalString rejects whitespace-only optional values
Summary:
Verifies that optional string validation distinguishes between empty and
whitespace-only values.

Validates:
  - whitespace-only value returns CodeValidation
  - error message reports cannot be whitespace
*/
func TestOptionalStringRejectsWhitespaceOnlyValue(t *testing.T) {
	err := optionalString("validate_status_envelope", "stage", "   ")

	requireAgentcoreError(t, err, agentcore.CodeValidation, "validate_status_envelope", "stage cannot be whitespace")
}

/*
TC-CONTRACT-HELPERS-008
Type: Positive
Title: requiredTimestamp accepts a non-zero time
Summary:
Verifies that required timestamp validation succeeds for a populated timestamp.

Validates:
  - non-zero timestamp returns no error
*/
func TestRequiredTimestampAcceptsNonZeroValue(t *testing.T) {
	if err := requiredTimestamp("validate_result_envelope", "timestamp", contractTestTime()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

/*
TC-CONTRACT-HELPERS-009
Type: Negative
Title: requiredTimestamp rejects zero time
Summary:
Verifies that required timestamp validation rejects a zero-value timestamp.

Validates:
  - zero time returns CodeValidation
  - error message reports field is required
*/
func TestRequiredTimestampRejectsZeroValue(t *testing.T) {
	err := requiredTimestamp("validate_result_envelope", "timestamp", time.Time{})

	requireAgentcoreError(t, err, agentcore.CodeValidation, "validate_result_envelope", "timestamp is required")
}

/*
TC-CONTRACT-HELPERS-010
Type: Positive
Title: requiredJSON accepts valid JSON payload
Summary:
Verifies that required JSON validation accepts a non-empty valid JSON payload.

Validates:
  - valid JSON payload returns no error
*/
func TestRequiredJSONAcceptsValidPayload(t *testing.T) {
	if err := requiredJSON("validate_configure_command", "payload", rawJSON(`{"hostname":"router-1"}`)); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

/*
TC-CONTRACT-HELPERS-011
Type: Negative
Title: requiredJSON rejects empty payload
Summary:
Verifies that required JSON validation rejects a missing payload.

Validates:
  - empty payload returns CodeValidation
  - error message reports payload is required
*/
func TestRequiredJSONRejectsEmptyPayload(t *testing.T) {
	err := requiredJSON("validate_configure_command", "payload", nil)

	requireAgentcoreError(t, err, agentcore.CodeValidation, "validate_configure_command", "payload is required")
}

/*
TC-CONTRACT-HELPERS-012
Type: Negative
Title: requiredJSON rejects invalid JSON payload
Summary:
Verifies that required JSON validation rejects syntactically invalid JSON.

Validates:
  - invalid payload returns CodeValidation
  - error message reports payload must contain valid JSON
*/
func TestRequiredJSONRejectsInvalidPayload(t *testing.T) {
	err := requiredJSON("validate_configure_command", "payload", rawJSON(`{"hostname":`))

	requireAgentcoreError(t, err, agentcore.CodeValidation, "validate_configure_command", "payload must contain valid JSON")
}

/*
TC-CONTRACT-HELPERS-013
Type: Positive
Title: optionalJSON allows empty payload
Summary:
Verifies that optional JSON validation permits an omitted optional payload.

Validates:
  - empty optional payload returns no error
*/
func TestOptionalJSONAllowsEmptyPayload(t *testing.T) {
	if err := optionalJSON("validate_status_envelope", "payload", nil); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

/*
TC-CONTRACT-HELPERS-014
Type: Negative
Title: optionalJSON rejects invalid JSON payload
Summary:
Verifies that optional JSON validation still rejects malformed JSON when a
payload is supplied.

Validates:
  - invalid optional payload returns CodeValidation
*/
func TestOptionalJSONRejectsInvalidPayload(t *testing.T) {
	err := optionalJSON("validate_status_envelope", "payload", rawJSON(`{"ready":`))

	requireAgentcoreError(t, err, agentcore.CodeValidation, "validate_status_envelope", "payload must contain valid JSON")
}
