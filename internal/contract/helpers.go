package contract

import (
	"encoding/json"
	"strings"
	"time"

    "github.com/Telecominfraproject/olg-nats-agent-core/agentcore"
)

func validationError(op, msg string) error {
	return &agentcore.Error{
		Code:      agentcore.CodeValidation,
		Op:        op,
		Message:   msg,
		Retryable: false,
	}
}

func decodeError(op string, err error) error {
	return &agentcore.Error{
		Code:      agentcore.CodeDecodeFailed,
		Op:        op,
		Message:   "failed to decode JSON payload",
		Retryable: false,
		Err:       err,
	}
}

func encodeError(op string, err error) error {
	return &agentcore.Error{
		Code:      agentcore.CodeEncodeFailed,
		Op:        op,
		Message:   "failed to encode JSON payload",
		Retryable: false,
		Err:       err,
	}
}

func requiredString(op, field, value string) error {
	if strings.TrimSpace(value) == "" {
		return validationError(op, field+" is required")
	}
	return nil
}

func optionalString(op, field, value string) error {
	if value == "" {
		return nil
	}
	if strings.TrimSpace(value) == "" {
		return validationError(op, field+" cannot be whitespace")
	}
	return nil
}

func requiredTimestamp(op, field string, value time.Time) error {
	if value.IsZero() {
		return validationError(op, field+" is required")
	}
	return nil
}

func requiredJSON(op, field string, payload json.RawMessage) error {
	if len(payload) == 0 {
		return validationError(op, field+" is required")
	}
	if !json.Valid(payload) {
		return validationError(op, field+" must contain valid JSON")
	}
	return nil
}

func optionalJSON(op, field string, payload json.RawMessage) error {
	if len(payload) == 0 {
		return nil
	}
	if !json.Valid(payload) {
		return validationError(op, field+" must contain valid JSON")
	}
	return nil
}
