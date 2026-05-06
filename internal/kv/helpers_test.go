package kv

import (
	"errors"
	"strings"
	"testing"

	"github.com/routerarchitects/nats-agent-core/internal/runtimeerr"
)

func requireKVRuntimeError(t *testing.T, err error, wantCode runtimeerr.Code, wantOp string, wantMsgPart string) *runtimeerr.Error {
	t.Helper()

	if err == nil {
		t.Fatal("expected non-nil error")
	}

	var got *runtimeerr.Error
	if !errors.As(err, &got) {
		t.Fatalf("expected *runtimeerr.Error, got %T", err)
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
TC-KV-HELPERS-001
Type: Positive
Title: buildDesiredConfigKey builds key for valid pattern and target
Summary:
Verifies that desired-config key helper formats key values correctly when both
pattern and target token are valid.

Validates:
  - valid pattern and target produce expected key
*/
func TestBuildDesiredConfigKeyBuildsKeyForValidInput(t *testing.T) {
	key, err := buildDesiredConfigKey("desired.%s", "vyos_1")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if key != "desired.vyos_1" {
		t.Fatalf("expected key %q, got %q", "desired.vyos_1", key)
	}
}

/*
TC-KV-HELPERS-002
Type: Negative
Title: buildDesiredConfigKey rejects malformed key pattern values
Summary:
Verifies that desired-config key helper rejects malformed key patterns before
attempting to build the final key.

Validates:
  - empty pattern is rejected
  - whitespace in pattern is rejected
  - placeholder count mismatch is rejected
  - unsupported format directives are rejected
*/
func TestBuildDesiredConfigKeyRejectsMalformedPatternValues(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		msgPart string
	}{
		{name: "empty pattern", pattern: "  ", msgPart: "kv key pattern is required"},
		{name: "pattern contains whitespace", pattern: "desired. %s", msgPart: "kv key pattern cannot contain whitespace"},
		{name: "missing placeholder", pattern: "desired.target", msgPart: "kv key pattern must contain exactly one %s placeholder"},
		{name: "unsupported format directive", pattern: "desired.%s.%d", msgPart: "kv key pattern contains unsupported format directives"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildDesiredConfigKey(tc.pattern, "vyos")
			requireKVRuntimeError(t, err, runtimeerr.CodeValidation, "build_desired_config_key", tc.msgPart)
		})
	}
}

/*
TC-KV-HELPERS-003
Type: Negative
Title: buildDesiredConfigKey rejects malformed target tokens
Summary:
Verifies that desired-config key helper validates target token rules and
rejects unsupported target values.

Validates:
  - empty target is rejected
  - whitespace dot wildcard and unsupported characters are rejected
*/
func TestBuildDesiredConfigKeyRejectsMalformedTargetTokens(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		msgPart string
	}{
		{name: "empty target", target: "", msgPart: "target is required"},
		{name: "whitespace target", target: "vyos core", msgPart: "target cannot contain whitespace"},
		{name: "dot target", target: "vyos.core", msgPart: "target cannot contain '.'"},
		{name: "star wildcard target", target: "vyos*", msgPart: "target cannot contain wildcard tokens"},
		{name: "full wildcard target", target: "vyos>", msgPart: "target cannot contain wildcard tokens"},
		{name: "unsupported char target", target: "vyos/core", msgPart: "target contains unsupported characters"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildDesiredConfigKey("desired.%s", tc.target)
			requireKVRuntimeError(t, err, runtimeerr.CodeValidation, "validate_target", tc.msgPart)
		})
	}
}

/*
TC-KV-HELPERS-004
Type: Positive
Title: validateToken accepts valid subject token
Summary:
Verifies that token helper accepts standard alphanumeric underscore and hyphen
token values.

Validates:
  - valid token passes validation
*/
func TestValidateTokenAcceptsValidToken(t *testing.T) {
	if err := validateToken("validate_target", "target", "vyos_01-edge"); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}
