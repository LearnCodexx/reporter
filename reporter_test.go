package reporter

import (
	"errors"
	"runtime"
	"strings"
	"testing"
)

func TestWrapCapturesCallerMetadata(t *testing.T) {
	appEnv = "production"
	appName = "reporter-test"
	isPublishing = false

	_, _, callLine, _ := runtime.Caller(0)
	err := Wrap(errors.New("boom"), "custom description")

	customErr, ok := err.(*CustomError)
	if !ok {
		t.Fatalf("expected *CustomError, got %T", err)
	}

	if customErr.Line != callLine+1 {
		t.Fatalf("expected caller line %d, got %d", callLine+1, customErr.Line)
	}
	if !strings.HasSuffix(customErr.File, "reporter/reporter_test.go") {
		t.Fatalf("expected file to end with reporter/reporter_test.go, got %q", customErr.File)
	}
	if !strings.Contains(customErr.FunctionName, "TestWrapCapturesCallerMetadata") {
		t.Fatalf("expected function name to contain test name, got %q", customErr.FunctionName)
	}
	if customErr.Description != "custom description" {
		t.Fatalf("expected custom description, got %q", customErr.Description)
	}
}

func TestAutoWrapClassifiesErrorCaseInsensitive(t *testing.T) {
	appEnv = "production"
	isPublishing = false

	err := AutoWrap(errors.New("DUPLICATE KEY value violates unique constraint"))
	customErr, ok := err.(*CustomError)
	if !ok {
		t.Fatalf("expected *CustomError, got %T", err)
	}

	if customErr.ErrorType != "DATABASE_CONSTRAINT" {
		t.Fatalf("expected DATABASE_CONSTRAINT, got %q", customErr.ErrorType)
	}
}
