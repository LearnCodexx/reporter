package reporter

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"
)

type channelPublisher struct {
	reports chan *CustomError
}

func (p *channelPublisher) Publish(_ context.Context, report *CustomError) error {
	p.reports <- report
	return nil
}

func TestWrapCapturesCallerMetadata(t *testing.T) {
	appEnv = "production"
	appName = "reporter-test"
	isPublishing = false
	publisher = nil

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
	publisher = nil

	err := AutoWrap(errors.New("DUPLICATE KEY value violates unique constraint"))
	customErr, ok := err.(*CustomError)
	if !ok {
		t.Fatalf("expected *CustomError, got %T", err)
	}

	if customErr.ErrorType != "DATABASE_CONSTRAINT" {
		t.Fatalf("expected DATABASE_CONSTRAINT, got %q", customErr.ErrorType)
	}
}

func TestDevelopmentPublishingUsesStructuredPayload(t *testing.T) {
	testPublisher := &channelPublisher{reports: make(chan *CustomError, 1)}

	Init(Config{
		AppName:          "reporter-test",
		AppEnv:           "development",
		EnablePublishing: true,
		Publisher:        testPublisher,
	})
	defer Close()

	err := Wrap(errors.New("boom"), "custom description")
	customErr, ok := err.(*CustomError)
	if !ok {
		t.Fatalf("expected *CustomError, got %T", err)
	}

	select {
	case published := <-testPublisher.reports:
		if published != customErr {
			t.Fatal("expected publisher to receive the structured CustomError")
		}
		if published.Environment != "development" {
			t.Fatalf("expected development environment, got %q", published.Environment)
		}
		if published.Service != "reporter-test" {
			t.Fatalf("expected reporter-test service, got %q", published.Service)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for published report")
	}
}
