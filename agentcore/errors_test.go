package agentcore

import (
	"errors"
	"testing"
)

/*
TC-ERROR-001
Type: Positive
Title: Error string returns code when only code is set
Summary:
Verifies the minimal string representation for a typed error that only carries
a machine-readable code.

Validates:
  - Error() returns the code string when Op and Message are empty
*/
func TestErrorStringReturnsCodeWhenOnlyCodeIsSet(t *testing.T) {
	err := &Error{Code: CodeNotImplemented}

	got := err.Error()
	want := "not_implemented"

	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

/*
TC-ERROR-002
Type: Positive
Title: Error string includes code and message when op is absent
Summary:
Verifies the public error formatting branch where only Code and Message are
present.

Validates:
  - Error() returns "<code>: <message>" when Op is empty
*/
func TestErrorStringReturnsCodeAndMessageWhenOpIsAbsent(t *testing.T) {
	err := &Error{
		Code:    CodeValidation,
		Message: "clock function is nil",
	}

	got := err.Error()
	want := "validation: clock function is nil"

	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

/*
TC-ERROR-003
Type: Positive
Title: Error string includes op, message, and code when all are present
Summary:
Verifies the richest error formatting branch used by typed public API failures.

Validates:
  - Error() returns "<op>: <message> (<code>)" when all fields are set
*/
func TestErrorStringReturnsOpMessageAndCodeWhenAllFieldsAreSet(t *testing.T) {
	err := &Error{
		Code:    CodePublishFailed,
		Op:      "publish_status",
		Message: "publish failed",
	}

	got := err.Error()
	want := "publish_status: publish failed (publish_failed)"

	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

/*
TC-ERROR-004
Type: Negative
Title: Nil typed error returns stable placeholder string
Summary:
Verifies nil receiver safety for the Error() method.

Validates:
  - calling Error() on a nil *Error does not panic
  - returned value is the defined placeholder string
*/
func TestErrorStringOnNilReceiverReturnsPlaceholder(t *testing.T) {
	var err *Error

	got := err.Error()
	want := "<nil>"

	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

/*
TC-ERROR-005
Type: Positive
Title: Unwrap exposes the underlying cause
Summary:
Verifies that the typed public error integrates with Go's standard error
wrapping behavior.

Validates:
  - Unwrap() returns the underlying cause
  - errors.Is(...) works through the typed wrapper
*/
func TestUnwrapExposesUnderlyingCause(t *testing.T) {
	cause := errors.New("root cause")
	err := &Error{
		Code: CodeDecodeFailed,
		Op:   "decode_result",
		Err:  cause,
	}

	if got := err.Unwrap(); got != cause {
		t.Fatalf("expected cause %v, got %v", cause, got)
	}
	if !errors.Is(err, cause) {
		t.Fatal("expected errors.Is to match wrapped cause")
	}
}

/*
TC-ERROR-006
Type: Positive
Title: Wrap creates a typed error around a cause
Summary:
Verifies the helper constructor used to build typed errors around underlying
Go errors.

Validates:
  - Wrap(...) stores code, op, and cause
  - returned value is non-nil
*/
func TestWrapCreatesTypedError(t *testing.T) {
	cause := errors.New("decode failed")

	err := Wrap(CodeDecodeFailed, "decode_result", cause)
	if err == nil {
		t.Fatal("expected non-nil wrapped error")
	}
	if err.Code != CodeDecodeFailed {
		t.Fatalf("expected code %q, got %q", CodeDecodeFailed, err.Code)
	}
	if err.Op != "decode_result" {
		t.Fatalf("expected op %q, got %q", "decode_result", err.Op)
	}
	if err.Err != cause {
		t.Fatalf("expected cause %v, got %v", cause, err.Err)
	}
}

/*
TC-ERROR-007
Type: Negative
Title: Nil typed error unwrap is safe
Summary:
Verifies nil receiver safety for Unwrap().

Validates:
  - calling Unwrap() on a nil *Error does not panic
  - returned cause is nil
*/
func TestUnwrapOnNilReceiverReturnsNil(t *testing.T) {
	var err *Error

	if got := err.Unwrap(); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

/*
TC-ERROR-008
Type: Positive
Title: Error string includes op and code when message is empty
Summary:
Verifies the Error.Error() formatting branch where Op is set but Message is
empty. In this case, the string should still be stable and meaningful.

Validates:
  - Error() returns "<op>: <code>" when Message is empty
  - formatting does not require Message to be populated
*/
func TestErrorStringReturnsOpAndCodeWhenMessageIsEmpty(t *testing.T) {
	err := &Error{
		Code: CodeDecodeFailed,
		Op:   "decode_result",
	}

	got := err.Error()
	want := "decode_result: decode_failed"

	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}
