package contract

import (
	"errors"
	"testing"

    "github.com/Telecominfraproject/olg-nats-agent-core/agentcore"
)

/*
TC-CONTRACT-CODEC-001
Type: Positive
Title: Encode and decode ConfigureCommand round-trips successfully
Summary:
Verifies that the configure command codec validates, encodes, decodes, and
preserves the important wire fields and payload.

Validates:
  - encode succeeds for a valid configure command
  - decode succeeds for the encoded payload
  - payload and identifiers are preserved
*/
func TestEncodeDecodeConfigureCommandRoundTrip(t *testing.T) {
	want := validConfigureCommand()

	encoded, err := EncodeConfigureCommand(want)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	got, err := DecodeConfigureCommand(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if got.Version != want.Version {
		t.Fatalf("expected Version %q, got %q", want.Version, got.Version)
	}
	if got.RPCID != want.RPCID {
		t.Fatalf("expected RPCID %q, got %q", want.RPCID, got.RPCID)
	}
	if got.Target != want.Target {
		t.Fatalf("expected Target %q, got %q", want.Target, got.Target)
	}
	if got.UUID != want.UUID {
		t.Fatalf("expected UUID %q, got %q", want.UUID, got.UUID)
	}
	if string(got.Payload) != string(want.Payload) {
		t.Fatalf("expected Payload %s, got %s", string(want.Payload), string(got.Payload))
	}
	if !got.Timestamp.Equal(want.Timestamp) {
		t.Fatalf("expected Timestamp %v, got %v", want.Timestamp, got.Timestamp)
	}
}

/*
TC-CONTRACT-CODEC-002
Type: Negative
Title: EncodeConfigureCommand rejects semantically invalid command
Summary:
Verifies that configure command encoding performs validation first and rejects
invalid commands instead of producing JSON.

Validates:
  - invalid configure command returns CodeValidation
  - validator op is preserved
*/
func TestEncodeConfigureCommandRejectsInvalidCommand(t *testing.T) {
	msg := validConfigureCommand()
	msg.UUID = ""

	_, err := EncodeConfigureCommand(msg)
	requireAgentcoreError(t, err, agentcore.CodeValidation, "validate_configure_command", "uuid is required")
}

/*
TC-CONTRACT-CODEC-003
Type: Negative
Title: DecodeConfigureCommand rejects empty payload
Summary:
Verifies that configure command decoding rejects missing raw input before JSON
unmarshal is attempted.

Validates:
  - empty payload returns CodeValidation
  - decode op is preserved
*/
func TestDecodeConfigureCommandRejectsEmptyPayload(t *testing.T) {
	_, err := DecodeConfigureCommand(nil)

	requireAgentcoreError(t, err, agentcore.CodeValidation, "decode_configure_command", "payload is required")
}

/*
TC-CONTRACT-CODEC-004
Type: Negative
Title: DecodeConfigureCommand rejects malformed JSON
Summary:
Verifies that configure command decoding wraps malformed JSON errors as typed
decode failures.

Validates:
  - malformed JSON returns CodeDecodeFailed
  - wrapped low-level JSON error is preserved
*/
func TestDecodeConfigureCommandRejectsMalformedJSON(t *testing.T) {
	_, err := DecodeConfigureCommand([]byte(`{"version":"1.0","rpc_id":"rpc-1",`))

	got := requireAgentcoreError(t, err, agentcore.CodeDecodeFailed, "decode_configure_command", "failed to decode JSON payload")
	if got.Unwrap() == nil {
		t.Fatal("expected wrapped decode cause to be preserved")
	}
}

/*
TC-CONTRACT-CODEC-005
Type: Negative
Title: DecodeConfigureCommand rejects decoded command that fails validation
Summary:
Verifies that configure command decoding performs validation after successful
JSON unmarshal and rejects invalid decoded content.

Validates:
  - decoded message missing uuid returns CodeValidation
  - validator op is configure-command specific
*/
func TestDecodeConfigureCommandRejectsSemanticallyInvalidDecodedCommand(t *testing.T) {
	data := []byte(`{
		"version":"1.0",
		"rpc_id":"rpc-config-1",
		"target":"vyos",
		"uuid":"",
		"payload":{"hostname":"router-1"},
		"timestamp":"2023-11-14T22:13:20Z"
	}`)

	_, err := DecodeConfigureCommand(data)
	requireAgentcoreError(t, err, agentcore.CodeValidation, "validate_configure_command", "uuid is required")
}

/*
TC-CONTRACT-CODEC-006
Type: Positive
Title: Encode and decode DesiredConfigRecord round-trips successfully
Summary:
Verifies that desired-config record codec preserves record identifiers and raw
payload across encode/decode operations.

Validates:
  - encode succeeds
  - decode succeeds
  - payload and identifiers are preserved
*/
func TestEncodeDecodeDesiredConfigRecordRoundTrip(t *testing.T) {
	want := validDesiredConfigRecord()

	encoded, err := EncodeDesiredConfigRecord(want)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	got, err := DecodeDesiredConfigRecord(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if got.RPCID != want.RPCID || got.UUID != want.UUID || got.Target != want.Target {
		t.Fatalf("expected identifiers %+v, got %+v", want, got)
	}
	if string(got.Payload) != string(want.Payload) {
		t.Fatalf("expected Payload %s, got %s", string(want.Payload), string(got.Payload))
	}
}

/*
TC-CONTRACT-CODEC-007
Type: Positive
Title: Encode and decode ConfigureNotification round-trips successfully
Summary:
Verifies that configure notification codec preserves the notification contract
fields used for configure signaling.

Validates:
  - encode succeeds
  - decode succeeds
  - kv_bucket kv_key rpc_id and uuid are preserved
*/
func TestEncodeDecodeConfigureNotificationRoundTrip(t *testing.T) {
	want := validConfigureNotification()

	encoded, err := EncodeConfigureNotification(want)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	got, err := DecodeConfigureNotification(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if got.RPCID != want.RPCID {
		t.Fatalf("expected RPCID %q, got %q", want.RPCID, got.RPCID)
	}
	if got.UUID != want.UUID {
		t.Fatalf("expected UUID %q, got %q", want.UUID, got.UUID)
	}
	if got.KVBucket != want.KVBucket {
		t.Fatalf("expected KVBucket %q, got %q", want.KVBucket, got.KVBucket)
	}
	if got.KVKey != want.KVKey {
		t.Fatalf("expected KVKey %q, got %q", want.KVKey, got.KVKey)
	}
}

/*
TC-CONTRACT-CODEC-008
Type: Positive
Title: Encode and decode ActionCommand round-trips successfully
Summary:
Verifies that action command codec preserves action-specific identifiers and
payload across encode/decode.

Validates:
  - encode succeeds
  - decode succeeds
  - action and payload are preserved
*/
func TestEncodeDecodeActionCommandRoundTrip(t *testing.T) {
	want := validActionCommand()

	encoded, err := EncodeActionCommand(want)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	got, err := DecodeActionCommand(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if got.Action != want.Action {
		t.Fatalf("expected Action %q, got %q", want.Action, got.Action)
	}
	if got.CommandType != want.CommandType {
		t.Fatalf("expected CommandType %q, got %q", want.CommandType, got.CommandType)
	}
	if string(got.Payload) != string(want.Payload) {
		t.Fatalf("expected Payload %s, got %s", string(want.Payload), string(got.Payload))
	}
}

/*
TC-CONTRACT-CODEC-009
Type: Negative
Title: DecodeActionCommand rejects malformed JSON
Summary:
Verifies that malformed action JSON returns a typed decode failure.

Validates:
  - malformed JSON returns CodeDecodeFailed
  - wrapped cause is preserved
*/
func TestDecodeActionCommandRejectsMalformedJSON(t *testing.T) {
	_, err := DecodeActionCommand([]byte(`{"version":"1.0","rpc_id":"rpc-1",`))

	got := requireAgentcoreError(t, err, agentcore.CodeDecodeFailed, "decode_action_command", "failed to decode JSON payload")
	if got.Unwrap() == nil {
		t.Fatal("expected wrapped decode cause to be preserved")
	}
}

/*
TC-CONTRACT-CODEC-010
Type: Positive
Title: Encode and decode ResultEnvelope round-trips successfully
Summary:
Verifies that generic result codec preserves result identity fields and payload.

Validates:
  - encode succeeds
  - decode succeeds
  - result payload is preserved
*/
func TestEncodeDecodeResultEnvelopeRoundTrip(t *testing.T) {
	want := validResultEnvelope()

	encoded, err := EncodeResultEnvelope(want)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	got, err := DecodeResultEnvelope(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if got.Result != want.Result {
		t.Fatalf("expected Result %q, got %q", want.Result, got.Result)
	}
	if got.UUID != want.UUID {
		t.Fatalf("expected UUID %q, got %q", want.UUID, got.UUID)
	}
	if string(got.Payload) != string(want.Payload) {
		t.Fatalf("expected Payload %s, got %s", string(want.Payload), string(got.Payload))
	}
}

/*
TC-CONTRACT-CODEC-011
Type: Positive
Title: Encode and decode configure result round-trips successfully
Summary:
Verifies that configure-specific result codec preserves the stricter configure
result semantics, including required uuid.

Validates:
  - encode succeeds
  - decode succeeds
  - required configure uuid is preserved
*/
func TestEncodeDecodeConfigureResultEnvelopeRoundTrip(t *testing.T) {
	want := validResultEnvelope()

	encoded, err := EncodeConfigureResultEnvelope(want)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	got, err := DecodeConfigureResultEnvelope(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if got.UUID != want.UUID {
		t.Fatalf("expected UUID %q, got %q", want.UUID, got.UUID)
	}
}

/*
TC-CONTRACT-CODEC-012
Type: Negative
Title: EncodeConfigureResultEnvelope rejects missing uuid
Summary:
Verifies that configure-specific result encoding enforces required configure
uuid semantics before JSON is produced.

Validates:
  - missing uuid returns CodeValidation
  - configure-result validator op is preserved
*/
func TestEncodeConfigureResultEnvelopeRejectsMissingUUID(t *testing.T) {
	msg := validResultEnvelope()
	msg.UUID = ""

	_, err := EncodeConfigureResultEnvelope(msg)
	requireAgentcoreError(t, err, agentcore.CodeValidation, "validate_configure_result_envelope", "uuid is required")
}

/*
TC-CONTRACT-CODEC-013
Type: Positive
Title: Encode and decode StatusEnvelope round-trips successfully
Summary:
Verifies that generic status codec preserves status fields and the Phase 2
optional uuid addition.

Validates:
  - encode succeeds
  - decode succeeds
  - uuid and payload are preserved
*/
func TestEncodeDecodeStatusEnvelopeRoundTrip(t *testing.T) {
	want := validStatusEnvelope()

	encoded, err := EncodeStatusEnvelope(want)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	got, err := DecodeStatusEnvelope(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if got.UUID != want.UUID {
		t.Fatalf("expected UUID %q, got %q", want.UUID, got.UUID)
	}
	if got.Status != want.Status {
		t.Fatalf("expected Status %q, got %q", want.Status, got.Status)
	}
	if string(got.Payload) != string(want.Payload) {
		t.Fatalf("expected Payload %s, got %s", string(want.Payload), string(got.Payload))
	}
}

/*
TC-CONTRACT-CODEC-014
Type: Negative
Title: DecodeStatusEnvelope rejects empty payload
Summary:
Verifies that generic status decoding rejects missing raw input before decode.

Validates:
  - empty raw input returns CodeValidation
  - decode op is preserved
*/
func TestDecodeStatusEnvelopeRejectsEmptyPayload(t *testing.T) {
	_, err := DecodeStatusEnvelope(nil)

	requireAgentcoreError(t, err, agentcore.CodeValidation, "decode_status_envelope", "payload is required")
}

/*
TC-CONTRACT-CODEC-015
Type: Positive
Title: Encode and decode configure status round-trips successfully
Summary:
Verifies that configure-specific status codec preserves configure rpc_id and
uuid semantics across encode/decode.

Validates:
  - encode succeeds
  - decode succeeds
  - rpc_id and uuid are preserved
*/
func TestEncodeDecodeConfigureStatusEnvelopeRoundTrip(t *testing.T) {
	want := validStatusEnvelope()

	encoded, err := EncodeConfigureStatusEnvelope(want)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	got, err := DecodeConfigureStatusEnvelope(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if got.RPCID != want.RPCID {
		t.Fatalf("expected RPCID %q, got %q", want.RPCID, got.RPCID)
	}
	if got.UUID != want.UUID {
		t.Fatalf("expected UUID %q, got %q", want.UUID, got.UUID)
	}
}

/*
TC-CONTRACT-CODEC-016
Type: Negative
Title: EncodeConfigureStatusEnvelope rejects missing rpc_id or uuid
Summary:
Verifies that configure-specific status encoding enforces required configure
status identifiers before JSON is produced.

Validates:
  - missing rpc_id returns CodeValidation
  - missing uuid returns CodeValidation
*/
func TestEncodeConfigureStatusEnvelopeRejectsMissingRPCIDOrUUID(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*agentcore.StatusEnvelope)
		msgPart string
	}{
		{
			name: "missing rpc_id",
			mutate: func(msg *agentcore.StatusEnvelope) {
				msg.RPCID = ""
			},
			msgPart: "rpc_id is required",
		},
		{
			name: "missing uuid",
			mutate: func(msg *agentcore.StatusEnvelope) {
				msg.UUID = ""
			},
			msgPart: "uuid is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := validStatusEnvelope()
			tc.mutate(&msg)

			_, err := EncodeConfigureStatusEnvelope(msg)
			requireAgentcoreError(t, err, agentcore.CodeValidation, "validate_configure_status_envelope", tc.msgPart)
		})
	}
}

/*
TC-CONTRACT-CODEC-017
Type: Negative
Title: Decode helper preserves wrapped cause for malformed JSON
Summary:
Verifies that malformed decode paths expose an underlying cause through the
typed error wrapper.

Validates:
  - errors.As reaches *agentcore.Error
  - errors.Is can reach the wrapped cause when present
*/
func TestDecodeMalformedJSONPreservesWrappedCause(t *testing.T) {
	_, err := DecodeResultEnvelope([]byte(`{"version":"1.0",`))
	got := requireAgentcoreError(t, err, agentcore.CodeDecodeFailed, "decode_result_envelope", "failed to decode JSON payload")
	if got.Unwrap() == nil {
		t.Fatal("expected wrapped decode cause to be preserved")
	}
	if !errors.Is(got, got.Unwrap()) {
		t.Fatal("expected errors.Is to match wrapped decode cause")
	}
}
