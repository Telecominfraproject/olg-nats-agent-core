package contract

import (
	"testing"
	"time"

    "github.com/Telecominfraproject/olg-nats-agent-core/agentcore"
)

/*
TC-CONTRACT-VALIDATE-001
Type: Positive
Title: ValidateBaseEnvelope accepts a valid base envelope
Summary:
Verifies that shared base envelope validation succeeds when all required
transport-level fields are present and valid.

Validates:
  - version is accepted
  - target is accepted
  - timestamp is accepted
  - rpc_id is allowed when non-empty and valid
*/
func TestValidateBaseEnvelopeAcceptsValidEnvelope(t *testing.T) {
	if err := ValidateBaseEnvelope(validBaseEnvelope()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

/*
TC-CONTRACT-VALIDATE-002
Type: Negative
Title: ValidateBaseEnvelope rejects missing required fields
Summary:
Verifies that base envelope validation rejects missing required version, target,
and timestamp values.

Validates:
  - missing version fails
  - missing target fails
  - zero timestamp fails
*/
func TestValidateBaseEnvelopeRejectsMissingRequiredFields(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*agentcore.BaseEnvelope)
		msgPart string
	}{
		{
			name: "missing version",
			mutate: func(msg *agentcore.BaseEnvelope) {
				msg.Version = ""
			},
			msgPart: "version is required",
		},
		{
			name: "missing target",
			mutate: func(msg *agentcore.BaseEnvelope) {
				msg.Target = ""
			},
			msgPart: "target is required",
		},
		{
			name: "zero timestamp",
			mutate: func(msg *agentcore.BaseEnvelope) {
				msg.Timestamp = time.Time{}
			},
			msgPart: "timestamp is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := validBaseEnvelope()
			tc.mutate(&msg)

			err := ValidateBaseEnvelope(msg)
			requireAgentcoreError(t, err, agentcore.CodeValidation, "validate_base_envelope", tc.msgPart)
		})
	}
}

/*
TC-CONTRACT-VALIDATE-003
Type: Negative
Title: ValidateBaseEnvelope rejects whitespace-only optional rpc_id
Summary:
Verifies that rpc_id remains optional on the base envelope, but whitespace-only
values are rejected as malformed optional input.

Validates:
  - empty rpc_id is allowed
  - whitespace-only rpc_id is rejected
*/
func TestValidateBaseEnvelopeAllowsEmptyRPCIDButRejectsWhitespaceOnlyRPCID(t *testing.T) {
	msg := validBaseEnvelope()
	msg.RPCID = ""
	if err := ValidateBaseEnvelope(msg); err != nil {
		t.Fatalf("expected nil error for empty rpc_id, got %v", err)
	}

	msg.RPCID = "   "
	err := ValidateBaseEnvelope(msg)
	requireAgentcoreError(t, err, agentcore.CodeValidation, "validate_base_envelope", "rpc_id cannot be whitespace")
}

/*
TC-CONTRACT-VALIDATE-004
Type: Positive
Title: ValidateConfigureCommand accepts a valid configure command
Summary:
Verifies that configure command validation succeeds when all required
transport-level fields and payload are present.

Validates:
  - required identifiers are accepted
  - required payload is accepted
  - timestamp is accepted
*/
func TestValidateConfigureCommandAcceptsValidCommand(t *testing.T) {
	if err := ValidateConfigureCommand(validConfigureCommand()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

/*
TC-CONTRACT-VALIDATE-005
Type: Negative
Title: ValidateConfigureCommand rejects missing required fields
Summary:
Verifies that configure command validation rejects missing required version,
rpc_id, target, uuid, timestamp, and payload fields.

Validates:
  - missing required strings fail
  - zero timestamp fails
  - missing payload fails
*/
func TestValidateConfigureCommandRejectsMissingRequiredFields(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*agentcore.ConfigureCommand)
		msgPart string
	}{
		{
			name: "missing version",
			mutate: func(msg *agentcore.ConfigureCommand) {
				msg.Version = ""
			},
			msgPart: "version is required",
		},
		{
			name: "missing rpc_id",
			mutate: func(msg *agentcore.ConfigureCommand) {
				msg.RPCID = ""
			},
			msgPart: "rpc_id is required",
		},
		{
			name: "missing target",
			mutate: func(msg *agentcore.ConfigureCommand) {
				msg.Target = ""
			},
			msgPart: "target is required",
		},
		{
			name: "missing uuid",
			mutate: func(msg *agentcore.ConfigureCommand) {
				msg.UUID = ""
			},
			msgPart: "uuid is required",
		},
		{
			name: "zero timestamp",
			mutate: func(msg *agentcore.ConfigureCommand) {
				msg.Timestamp = time.Time{}
			},
			msgPart: "timestamp is required",
		},
		{
			name: "missing payload",
			mutate: func(msg *agentcore.ConfigureCommand) {
				msg.Payload = nil
			},
			msgPart: "payload is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := validConfigureCommand()
			tc.mutate(&msg)

			err := ValidateConfigureCommand(msg)
			requireAgentcoreError(t, err, agentcore.CodeValidation, "validate_configure_command", tc.msgPart)
		})
	}
}

/*
TC-CONTRACT-VALIDATE-006
Type: Negative
Title: ValidateConfigureCommand rejects invalid payload JSON
Summary:
Verifies that configure command validation rejects malformed JSON payloads even
when all other fields are present.

Validates:
  - malformed payload returns CodeValidation
*/
func TestValidateConfigureCommandRejectsInvalidPayloadJSON(t *testing.T) {
	msg := validConfigureCommand()
	msg.Payload = rawJSON(`{"hostname":`)

	err := ValidateConfigureCommand(msg)
	requireAgentcoreError(t, err, agentcore.CodeValidation, "validate_configure_command", "payload must contain valid JSON")
}

/*
TC-CONTRACT-VALIDATE-007
Type: Positive
Title: ValidateDesiredConfigRecord accepts a valid desired-config record
Summary:
Verifies that desired-config record validation succeeds for a valid stored
desired-state payload.

Validates:
  - required record identifiers are accepted
  - payload is accepted
*/
func TestValidateDesiredConfigRecordAcceptsValidRecord(t *testing.T) {
	if err := ValidateDesiredConfigRecord(validDesiredConfigRecord()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

/*
TC-CONTRACT-VALIDATE-008
Type: Positive
Title: ValidateConfigureNotification accepts a valid notification
Summary:
Verifies that configure notification validation succeeds for a valid
post-store notification envelope.

Validates:
  - command_type is accepted
  - kv_bucket and kv_key are accepted
  - rpc_id and uuid are accepted
*/
func TestValidateConfigureNotificationAcceptsValidNotification(t *testing.T) {
	if err := ValidateConfigureNotification(validConfigureNotification()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

/*
TC-CONTRACT-VALIDATE-009
Type: Negative
Title: ValidateConfigureNotification rejects missing notification fields
Summary:
Verifies that configure notification validation rejects missing command_type,
kv_bucket, and kv_key values.

Validates:
  - missing command_type fails
  - missing kv_bucket fails
  - missing kv_key fails
*/
func TestValidateConfigureNotificationRejectsMissingFields(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*agentcore.ConfigureNotification)
		msgPart string
	}{
		{
			name: "missing command_type",
			mutate: func(msg *agentcore.ConfigureNotification) {
				msg.CommandType = ""
			},
			msgPart: "command_type is required",
		},
		{
			name: "missing kv_bucket",
			mutate: func(msg *agentcore.ConfigureNotification) {
				msg.KVBucket = ""
			},
			msgPart: "kv_bucket is required",
		},
		{
			name: "missing kv_key",
			mutate: func(msg *agentcore.ConfigureNotification) {
				msg.KVKey = ""
			},
			msgPart: "kv_key is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := validConfigureNotification()
			tc.mutate(&msg)

			err := ValidateConfigureNotification(msg)
			requireAgentcoreError(t, err, agentcore.CodeValidation, "validate_configure_notification", tc.msgPart)
		})
	}
}

/*
TC-CONTRACT-VALIDATE-010
Type: Positive
Title: ValidateActionCommand accepts a valid action command
Summary:
Verifies that action command validation succeeds for a valid action wire model.

Validates:
  - action command required fields are accepted
  - payload is accepted
*/
func TestValidateActionCommandAcceptsValidAction(t *testing.T) {
	if err := ValidateActionCommand(validActionCommand()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

/*
TC-CONTRACT-VALIDATE-011
Type: Negative
Title: ValidateActionCommand rejects missing required fields
Summary:
Verifies that action command validation rejects missing action, command_type,
and payload fields.

Validates:
  - missing action fails
  - missing command_type fails
  - missing payload fails
*/
func TestValidateActionCommandRejectsMissingFields(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*agentcore.ActionCommand)
		msgPart string
	}{
		{
			name: "missing action",
			mutate: func(msg *agentcore.ActionCommand) {
				msg.Action = ""
			},
			msgPart: "action is required",
		},
		{
			name: "missing command_type",
			mutate: func(msg *agentcore.ActionCommand) {
				msg.CommandType = ""
			},
			msgPart: "command_type is required",
		},
		{
			name: "missing payload",
			mutate: func(msg *agentcore.ActionCommand) {
				msg.Payload = nil
			},
			msgPart: "payload is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := validActionCommand()
			tc.mutate(&msg)

			err := ValidateActionCommand(msg)
			requireAgentcoreError(t, err, agentcore.CodeValidation, "validate_action_command", tc.msgPart)
		})
	}
}

/*
TC-CONTRACT-VALIDATE-012
Type: Positive
Title: ValidateResultEnvelope accepts a valid result envelope
Summary:
Verifies that generic result validation succeeds when required shared result
fields are present.

Validates:
  - version, rpc_id, target, result, and timestamp are accepted
  - optional payload is accepted when valid
*/
func TestValidateResultEnvelopeAcceptsValidResult(t *testing.T) {
	if err := ValidateResultEnvelope(validResultEnvelope()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

/*
TC-CONTRACT-VALIDATE-013
Type: Negative
Title: ValidateResultEnvelope rejects missing result field
Summary:
Verifies that generic result validation rejects a missing required result value.

Validates:
  - missing result returns CodeValidation
*/
func TestValidateResultEnvelopeRejectsMissingResult(t *testing.T) {
	msg := validResultEnvelope()
	msg.Result = ""

	err := ValidateResultEnvelope(msg)
	requireAgentcoreError(t, err, agentcore.CodeValidation, "validate_result_envelope", "result is required")
}

/*
TC-CONTRACT-VALIDATE-014
Type: Negative
Title: ValidateResultEnvelope rejects whitespace-only optional fields
Summary:
Verifies that optional generic result fields may be empty, but whitespace-only
values are rejected.

Validates:
  - empty optional fields are allowed
  - whitespace-only optional fields are rejected
*/
func TestValidateResultEnvelopeOptionalFieldBehavior(t *testing.T) {
	msg := validResultEnvelope()
	msg.CommandType = ""
	msg.UUID = ""
	msg.Action = ""
	msg.ErrorCode = ""
	msg.Payload = nil

	if err := ValidateResultEnvelope(msg); err != nil {
		t.Fatalf("expected nil error for empty optional fields, got %v", err)
	}

	msg = validResultEnvelope()
	msg.CommandType = "   "
	err := ValidateResultEnvelope(msg)
	requireAgentcoreError(t, err, agentcore.CodeValidation, "validate_result_envelope", "command_type cannot be whitespace")
}

/*
TC-CONTRACT-VALIDATE-015
Type: Positive
Title: ValidateConfigureResultEnvelope accepts configure result with uuid
Summary:
Verifies that the configure-specific result validator requires the generic
result fields and also accepts a configure UUID.

Validates:
  - generic result validation passes
  - uuid is required and accepted for configure-flow results
*/
func TestValidateConfigureResultEnvelopeAcceptsValidConfigureResult(t *testing.T) {
	if err := ValidateConfigureResultEnvelope(validResultEnvelope()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

/*
TC-CONTRACT-VALIDATE-016
Type: Negative
Title: ValidateConfigureResultEnvelope requires uuid
Summary:
Verifies that configure-flow result validation is stricter than generic result
validation and requires a uuid.

Validates:
  - missing uuid returns CodeValidation
  - validator op is configure-result specific
*/
func TestValidateConfigureResultEnvelopeRequiresUUID(t *testing.T) {
	msg := validResultEnvelope()
	msg.UUID = ""

	err := ValidateConfigureResultEnvelope(msg)
	requireAgentcoreError(t, err, agentcore.CodeValidation, "validate_configure_result_envelope", "uuid is required")
}

/*
TC-CONTRACT-VALIDATE-017
Type: Positive
Title: ValidateStatusEnvelope accepts a valid status envelope
Summary:
Verifies that generic status validation succeeds for a valid status wire model,
including the new optional uuid field.

Validates:
  - status required fields are accepted
  - optional uuid is accepted when valid
*/
func TestValidateStatusEnvelopeAcceptsValidStatus(t *testing.T) {
	if err := ValidateStatusEnvelope(validStatusEnvelope()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

/*
TC-CONTRACT-VALIDATE-018
Type: Positive
Title: ValidateStatusEnvelope allows empty optional rpc_id uuid and stage
Summary:
Verifies that generic status validation permits omitted optional fields while
still validating the required core status fields.

Validates:
  - empty rpc_id is allowed
  - empty uuid is allowed
  - empty stage is allowed
*/
func TestValidateStatusEnvelopeAllowsEmptyOptionalFields(t *testing.T) {
	msg := validStatusEnvelope()
	msg.RPCID = ""
	msg.UUID = ""
	msg.Stage = ""
	msg.Payload = nil

	if err := ValidateStatusEnvelope(msg); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

/*
TC-CONTRACT-VALIDATE-019
Type: Negative
Title: ValidateStatusEnvelope rejects whitespace-only optional values
Summary:
Verifies that generic status validation rejects malformed whitespace-only
values for optional string fields.

Validates:
  - whitespace-only uuid is rejected
  - validator returns CodeValidation
*/
func TestValidateStatusEnvelopeRejectsWhitespaceOnlyOptionalValues(t *testing.T) {
	msg := validStatusEnvelope()
	msg.UUID = "   "

	err := ValidateStatusEnvelope(msg)
	requireAgentcoreError(t, err, agentcore.CodeValidation, "validate_status_envelope", "uuid cannot be whitespace")
}

/*
TC-CONTRACT-VALIDATE-020
Type: Positive
Title: ValidateConfigureStatusEnvelope accepts configure status with rpc_id and uuid
Summary:
Verifies that configure-flow status validation succeeds when both rpc_id and
uuid are present in addition to the generic status fields.

Validates:
  - generic status validation passes
  - rpc_id is required for configure status
  - uuid is required for configure status
*/
func TestValidateConfigureStatusEnvelopeAcceptsValidConfigureStatus(t *testing.T) {
	if err := ValidateConfigureStatusEnvelope(validStatusEnvelope()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

/*
TC-CONTRACT-VALIDATE-021
Type: Negative
Title: ValidateConfigureStatusEnvelope requires rpc_id and uuid
Summary:
Verifies that configure-flow status validation is stricter than generic status
validation and requires both rpc_id and uuid.

Validates:
  - missing rpc_id fails
  - missing uuid fails
*/
func TestValidateConfigureStatusEnvelopeRequiresRPCIDAndUUID(t *testing.T) {
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

			err := ValidateConfigureStatusEnvelope(msg)
			requireAgentcoreError(t, err, agentcore.CodeValidation, "validate_configure_status_envelope", tc.msgPart)
		})
	}
}

/*
TC-CONTRACT-VALIDATE-022
Type: Positive
Title: ValidateStoredDesiredConfig accepts valid stored desired config
Summary:
Verifies that storage-facing desired config validation succeeds for a valid
record plus bucket, key, and created_at metadata.

Validates:
  - nested desired-config record passes
  - bucket key and created_at are accepted
*/
func TestValidateStoredDesiredConfigAcceptsValidStoredConfig(t *testing.T) {
	if err := ValidateStoredDesiredConfig(validStoredDesiredConfig()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

/*
TC-CONTRACT-VALIDATE-023
Type: Negative
Title: ValidateStoredDesiredConfig rejects missing storage metadata
Summary:
Verifies that storage-facing desired config validation rejects missing bucket,
key, and created_at metadata.

Validates:
  - missing bucket fails
  - missing key fails
  - zero created_at fails
*/
func TestValidateStoredDesiredConfigRejectsMissingStorageMetadata(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*agentcore.StoredDesiredConfig)
		wantOp  string
		msgPart string
	}{
		{
			name: "missing bucket",
			mutate: func(msg *agentcore.StoredDesiredConfig) {
				msg.Bucket = ""
			},
			wantOp:  "validate_stored_desired_config",
			msgPart: "bucket is required",
		},
		{
			name: "missing key",
			mutate: func(msg *agentcore.StoredDesiredConfig) {
				msg.Key = ""
			},
			wantOp:  "validate_stored_desired_config",
			msgPart: "key is required",
		},
		{
			name: "zero created_at",
			mutate: func(msg *agentcore.StoredDesiredConfig) {
				msg.CreatedAt = time.Time{}
			},
			wantOp:  "validate_stored_desired_config",
			msgPart: "created_at is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := validStoredDesiredConfig()
			tc.mutate(&msg)

			err := ValidateStoredDesiredConfig(msg)
			requireAgentcoreError(t, err, agentcore.CodeValidation, tc.wantOp, tc.msgPart)
		})
	}
}
