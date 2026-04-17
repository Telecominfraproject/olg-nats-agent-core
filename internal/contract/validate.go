package contract

import "github.com/Telecominfraproject/olg-nats-agent-core/agentcore"

// ValidateBaseEnvelope validates shared transport-level envelope fields.
func ValidateBaseEnvelope(msg agentcore.BaseEnvelope) error {
	const op = "validate_base_envelope"

	if err := requiredString(op, "version", msg.Version); err != nil {
		return err
	}
	if err := requiredString(op, "target", msg.Target); err != nil {
		return err
	}
	if err := requiredTimestamp(op, "timestamp", msg.Timestamp); err != nil {
		return err
	}
	if err := optionalString(op, "rpc_id", msg.RPCID); err != nil {
		return err
	}
	return nil
}

// ValidateConfigureCommand validates transport-level configure command fields.
func ValidateConfigureCommand(msg agentcore.ConfigureCommand) error {
	const op = "validate_configure_command"

	if err := requiredString(op, "version", msg.Version); err != nil {
		return err
	}
	if err := requiredString(op, "rpc_id", msg.RPCID); err != nil {
		return err
	}
	if err := requiredString(op, "target", msg.Target); err != nil {
		return err
	}
	if err := requiredString(op, "uuid", msg.UUID); err != nil {
		return err
	}
	if err := requiredTimestamp(op, "timestamp", msg.Timestamp); err != nil {
		return err
	}
	return requiredJSON(op, "payload", msg.Payload)
}

// ValidateDesiredConfigRecord validates transport-level desired-config fields.
func ValidateDesiredConfigRecord(msg agentcore.DesiredConfigRecord) error {
	const op = "validate_desired_config_record"

	if err := requiredString(op, "version", msg.Version); err != nil {
		return err
	}
	if err := requiredString(op, "rpc_id", msg.RPCID); err != nil {
		return err
	}
	if err := requiredString(op, "target", msg.Target); err != nil {
		return err
	}
	if err := requiredString(op, "uuid", msg.UUID); err != nil {
		return err
	}
	if err := requiredTimestamp(op, "timestamp", msg.Timestamp); err != nil {
		return err
	}
	return requiredJSON(op, "payload", msg.Payload)
}

// ValidateConfigureNotification validates transport-level configure notification fields.
func ValidateConfigureNotification(msg agentcore.ConfigureNotification) error {
	const op = "validate_configure_notification"

	if err := requiredString(op, "version", msg.Version); err != nil {
		return err
	}
	if err := requiredString(op, "rpc_id", msg.RPCID); err != nil {
		return err
	}
	if err := requiredString(op, "target", msg.Target); err != nil {
		return err
	}
	if err := requiredString(op, "command_type", msg.CommandType); err != nil {
		return err
	}
	if err := requiredString(op, "uuid", msg.UUID); err != nil {
		return err
	}
	if err := requiredString(op, "kv_bucket", msg.KVBucket); err != nil {
		return err
	}
	if err := requiredString(op, "kv_key", msg.KVKey); err != nil {
		return err
	}
	return requiredTimestamp(op, "timestamp", msg.Timestamp)
}

// ValidateActionCommand validates transport-level action command fields.
func ValidateActionCommand(msg agentcore.ActionCommand) error {
	const op = "validate_action_command"

	if err := requiredString(op, "version", msg.Version); err != nil {
		return err
	}
	if err := requiredString(op, "rpc_id", msg.RPCID); err != nil {
		return err
	}
	if err := requiredString(op, "target", msg.Target); err != nil {
		return err
	}
	if err := requiredString(op, "command_type", msg.CommandType); err != nil {
		return err
	}
	if err := requiredString(op, "action", msg.Action); err != nil {
		return err
	}
	if err := requiredTimestamp(op, "timestamp", msg.Timestamp); err != nil {
		return err
	}
	return requiredJSON(op, "payload", msg.Payload)
}

// ValidateResultEnvelope validates transport-level result fields.
func ValidateResultEnvelope(msg agentcore.ResultEnvelope) error {
	const op = "validate_result_envelope"

	if err := requiredString(op, "version", msg.Version); err != nil {
		return err
	}
	if err := requiredString(op, "rpc_id", msg.RPCID); err != nil {
		return err
	}
	if err := requiredString(op, "target", msg.Target); err != nil {
		return err
	}
	if err := requiredString(op, "result", msg.Result); err != nil {
		return err
	}
	if err := requiredTimestamp(op, "timestamp", msg.Timestamp); err != nil {
		return err
	}
	if err := optionalString(op, "command_type", msg.CommandType); err != nil {
		return err
	}
	if err := optionalString(op, "uuid", msg.UUID); err != nil {
		return err
	}
	if err := optionalString(op, "action", msg.Action); err != nil {
		return err
	}
	if err := optionalString(op, "error_code", msg.ErrorCode); err != nil {
		return err
	}
	return optionalJSON(op, "payload", msg.Payload)
}

// ValidateConfigureResultEnvelope validates a configure-flow result.
func ValidateConfigureResultEnvelope(msg agentcore.ResultEnvelope) error {
	const op = "validate_configure_result_envelope"

	if err := ValidateResultEnvelope(msg); err != nil {
		return err
	}
	if err := requiredString(op, "uuid", msg.UUID); err != nil {
		return err
	}
	return nil
}

// ValidateStatusEnvelope validates transport-level status fields.
func ValidateStatusEnvelope(msg agentcore.StatusEnvelope) error {
	const op = "validate_status_envelope"

	if err := requiredString(op, "version", msg.Version); err != nil {
		return err
	}
	if err := requiredString(op, "target", msg.Target); err != nil {
		return err
	}
	if err := requiredString(op, "status", msg.Status); err != nil {
		return err
	}
	if err := requiredTimestamp(op, "timestamp", msg.Timestamp); err != nil {
		return err
	}
	if err := optionalString(op, "rpc_id", msg.RPCID); err != nil {
		return err
	}
	if err := optionalString(op, "uuid", msg.UUID); err != nil {
		return err
	}
	if err := optionalString(op, "stage", msg.Stage); err != nil {
		return err
	}
	return optionalJSON(op, "payload", msg.Payload)
}

// ValidateConfigureStatusEnvelope validates a configure-flow status update.
func ValidateConfigureStatusEnvelope(msg agentcore.StatusEnvelope) error {
	const op = "validate_configure_status_envelope"

	if err := ValidateStatusEnvelope(msg); err != nil {
		return err
	}
	if err := requiredString(op, "rpc_id", msg.RPCID); err != nil {
		return err
	}
	if err := requiredString(op, "uuid", msg.UUID); err != nil {
		return err
	}
	return nil
}

// ValidateStoredDesiredConfig validates storage-facing desired-config metadata.
func ValidateStoredDesiredConfig(msg agentcore.StoredDesiredConfig) error {
	const op = "validate_stored_desired_config"

	if err := ValidateDesiredConfigRecord(msg.Record); err != nil {
		return err
	}
	if err := requiredString(op, "bucket", msg.Bucket); err != nil {
		return err
	}
	if err := requiredString(op, "key", msg.Key); err != nil {
		return err
	}
	return requiredTimestamp(op, "created_at", msg.CreatedAt)
}
