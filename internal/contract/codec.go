package contract

import (
	"encoding/json"

	"github.com/Telecominfraproject/olg-nats-agent-core/agentcore"
)

func encodeJSON(op string, value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, encodeError(op, err)
	}
	return encoded, nil
}

func decodeJSON(op string, data []byte, out any) error {
	if len(data) == 0 {
		return validationError(op, "payload is required")
	}
	if err := json.Unmarshal(data, out); err != nil {
		return decodeError(op, err)
	}
	return nil
}

// EncodeConfigureCommand validates and encodes a ConfigureCommand.
func EncodeConfigureCommand(msg agentcore.ConfigureCommand) ([]byte, error) {
	if err := ValidateConfigureCommand(msg); err != nil {
		return nil, err
	}
	return encodeJSON("encode_configure_command", msg)
}

// DecodeConfigureCommand decodes and validates a ConfigureCommand.
func DecodeConfigureCommand(data []byte) (agentcore.ConfigureCommand, error) {
	var msg agentcore.ConfigureCommand
	if err := decodeJSON("decode_configure_command", data, &msg); err != nil {
		return agentcore.ConfigureCommand{}, err
	}
	if err := ValidateConfigureCommand(msg); err != nil {
		return agentcore.ConfigureCommand{}, err
	}
	return msg, nil
}

// EncodeDesiredConfigRecord validates and encodes a DesiredConfigRecord.
func EncodeDesiredConfigRecord(msg agentcore.DesiredConfigRecord) ([]byte, error) {
	if err := ValidateDesiredConfigRecord(msg); err != nil {
		return nil, err
	}
	return encodeJSON("encode_desired_config_record", msg)
}

// DecodeDesiredConfigRecord decodes and validates a DesiredConfigRecord.
func DecodeDesiredConfigRecord(data []byte) (agentcore.DesiredConfigRecord, error) {
	var msg agentcore.DesiredConfigRecord
	if err := decodeJSON("decode_desired_config_record", data, &msg); err != nil {
		return agentcore.DesiredConfigRecord{}, err
	}
	if err := ValidateDesiredConfigRecord(msg); err != nil {
		return agentcore.DesiredConfigRecord{}, err
	}
	return msg, nil
}

// EncodeConfigureNotification validates and encodes a ConfigureNotification.
func EncodeConfigureNotification(msg agentcore.ConfigureNotification) ([]byte, error) {
	if err := ValidateConfigureNotification(msg); err != nil {
		return nil, err
	}
	return encodeJSON("encode_configure_notification", msg)
}

// DecodeConfigureNotification decodes and validates a ConfigureNotification.
func DecodeConfigureNotification(data []byte) (agentcore.ConfigureNotification, error) {
	var msg agentcore.ConfigureNotification
	if err := decodeJSON("decode_configure_notification", data, &msg); err != nil {
		return agentcore.ConfigureNotification{}, err
	}
	if err := ValidateConfigureNotification(msg); err != nil {
		return agentcore.ConfigureNotification{}, err
	}
	return msg, nil
}

// EncodeActionCommand validates and encodes an ActionCommand.
func EncodeActionCommand(msg agentcore.ActionCommand) ([]byte, error) {
	if err := ValidateActionCommand(msg); err != nil {
		return nil, err
	}
	return encodeJSON("encode_action_command", msg)
}

// DecodeActionCommand decodes and validates an ActionCommand.
func DecodeActionCommand(data []byte) (agentcore.ActionCommand, error) {
	var msg agentcore.ActionCommand
	if err := decodeJSON("decode_action_command", data, &msg); err != nil {
		return agentcore.ActionCommand{}, err
	}
	if err := ValidateActionCommand(msg); err != nil {
		return agentcore.ActionCommand{}, err
	}
	return msg, nil
}

// EncodeResultEnvelope validates and encodes a ResultEnvelope.
func EncodeResultEnvelope(msg agentcore.ResultEnvelope) ([]byte, error) {
	if err := ValidateResultEnvelope(msg); err != nil {
		return nil, err
	}
	return encodeJSON("encode_result_envelope", msg)
}

// DecodeResultEnvelope decodes and validates a ResultEnvelope.
func DecodeResultEnvelope(data []byte) (agentcore.ResultEnvelope, error) {
	var msg agentcore.ResultEnvelope
	if err := decodeJSON("decode_result_envelope", data, &msg); err != nil {
		return agentcore.ResultEnvelope{}, err
	}
	if err := ValidateResultEnvelope(msg); err != nil {
		return agentcore.ResultEnvelope{}, err
	}
	return msg, nil
}

// EncodeConfigureResultEnvelope validates and encodes a configure result.
func EncodeConfigureResultEnvelope(msg agentcore.ResultEnvelope) ([]byte, error) {
	if err := ValidateConfigureResultEnvelope(msg); err != nil {
		return nil, err
	}
	return encodeJSON("encode_configure_result_envelope", msg)
}

// DecodeConfigureResultEnvelope decodes and validates a configure result.
func DecodeConfigureResultEnvelope(data []byte) (agentcore.ResultEnvelope, error) {
	var msg agentcore.ResultEnvelope
	if err := decodeJSON("decode_configure_result_envelope", data, &msg); err != nil {
		return agentcore.ResultEnvelope{}, err
	}
	if err := ValidateConfigureResultEnvelope(msg); err != nil {
		return agentcore.ResultEnvelope{}, err
	}
	return msg, nil
}

// EncodeStatusEnvelope validates and encodes a StatusEnvelope.
func EncodeStatusEnvelope(msg agentcore.StatusEnvelope) ([]byte, error) {
	if err := ValidateStatusEnvelope(msg); err != nil {
		return nil, err
	}
	return encodeJSON("encode_status_envelope", msg)
}

// DecodeStatusEnvelope decodes and validates a StatusEnvelope.
func DecodeStatusEnvelope(data []byte) (agentcore.StatusEnvelope, error) {
	var msg agentcore.StatusEnvelope
	if err := decodeJSON("decode_status_envelope", data, &msg); err != nil {
		return agentcore.StatusEnvelope{}, err
	}
	if err := ValidateStatusEnvelope(msg); err != nil {
		return agentcore.StatusEnvelope{}, err
	}
	return msg, nil
}

// EncodeConfigureStatusEnvelope validates and encodes a configure status update.
func EncodeConfigureStatusEnvelope(msg agentcore.StatusEnvelope) ([]byte, error) {
	if err := ValidateConfigureStatusEnvelope(msg); err != nil {
		return nil, err
	}
	return encodeJSON("encode_configure_status_envelope", msg)
}

// DecodeConfigureStatusEnvelope decodes and validates a configure status update.
func DecodeConfigureStatusEnvelope(data []byte) (agentcore.StatusEnvelope, error) {
	var msg agentcore.StatusEnvelope
	if err := decodeJSON("decode_configure_status_envelope", data, &msg); err != nil {
		return agentcore.StatusEnvelope{}, err
	}
	if err := ValidateConfigureStatusEnvelope(msg); err != nil {
		return agentcore.StatusEnvelope{}, err
	}
	return msg, nil
}
