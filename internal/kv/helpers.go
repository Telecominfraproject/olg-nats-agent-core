package kv

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/routerarchitects/nats-agent-core/internal/runtimeerr"
)

func buildDesiredConfigKey(pattern, target string) (string, error) {
	trimmedPattern := strings.TrimSpace(pattern)
	if trimmedPattern == "" {
		return "", validationError("build_desired_config_key", "kv key pattern is required")
	}
	if strings.ContainsAny(trimmedPattern, " \t\r\n") {
		return "", validationError("build_desired_config_key", "kv key pattern cannot contain whitespace")
	}
	if strings.Count(trimmedPattern, "%s") != 1 {
		return "", validationError("build_desired_config_key", "kv key pattern must contain exactly one %s placeholder")
	}
	residual := strings.ReplaceAll(trimmedPattern, "%s", "")
	if strings.Contains(residual, "%") {
		return "", validationError("build_desired_config_key", "kv key pattern contains unsupported format directives")
	}
	if err := validateToken("validate_target", "target", target); err != nil {
		return "", err
	}
	return fmt.Sprintf(trimmedPattern, target), nil
}

func kvStoreError(op, message string, cause error) error {
	return &runtimeerr.Error{
		Code:      runtimeerr.CodeKVStoreFailed,
		Op:        op,
		Message:   message,
		Retryable: true,
		Err:       cause,
	}
}

func kvReadError(op, message string, cause error) error {
	return &runtimeerr.Error{
		Code:      runtimeerr.CodeKVReadFailed,
		Op:        op,
		Message:   message,
		Retryable: true,
		Err:       cause,
	}
}

func validationError(op, message string) error {
	return &runtimeerr.Error{
		Code:      runtimeerr.CodeValidation,
		Op:        op,
		Message:   message,
		Retryable: false,
	}
}

func validateToken(op, field, value string) error {
	if strings.TrimSpace(value) == "" {
		return validationError(op, field+" is required")
	}
	if strings.ContainsAny(value, " \t\r\n") {
		return validationError(op, field+" cannot contain whitespace")
	}
	if strings.Contains(value, ".") {
		return validationError(op, field+" cannot contain '.'")
	}
	if strings.Contains(value, "*") || strings.Contains(value, ">") {
		return validationError(op, field+" cannot contain wildcard tokens")
	}
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-' {
			continue
		}
		return validationError(op, field+" contains unsupported characters")
	}
	return nil
}
