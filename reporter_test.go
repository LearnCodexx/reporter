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
		if published.Severity != SeverityDanger {
			t.Fatalf("expected danger severity, got %q", published.Severity)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for published report")
	}
}

func TestAutoWrapDoesNotPublishHandledDatabaseConstraintByDefault(t *testing.T) {
	testPublisher := &channelPublisher{reports: make(chan *CustomError, 1)}

	Init(Config{
		AppName:          "reporter-test",
		AppEnv:           "production",
		EnablePublishing: true,
		Publisher:        testPublisher,
	})
	defer Close()

	err := AutoWrap(errors.New("duplicate key value violates unique constraint"))
	customErr, ok := err.(*CustomError)
	if !ok {
		t.Fatalf("expected *CustomError, got %T", err)
	}
	if customErr.Severity != SeverityInfo {
		t.Fatalf("expected info severity, got %q", customErr.Severity)
	}

	select {
	case published := <-testPublisher.reports:
		t.Fatalf("expected duplicate key not to publish by default, got %#v", published)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestAutoWrapPublishesCriticalInfrastructureError(t *testing.T) {
	testPublisher := &channelPublisher{reports: make(chan *CustomError, 1)}

	Init(Config{
		AppName:          "reporter-test",
		AppEnv:           "production",
		EnablePublishing: true,
		Publisher:        testPublisher,
	})
	defer Close()

	err := AutoWrap(errors.New("dial tcp database:5432 connection refused"))
	customErr, ok := err.(*CustomError)
	if !ok {
		t.Fatalf("expected *CustomError, got %T", err)
	}
	if customErr.Severity != SeverityCritical {
		t.Fatalf("expected critical severity, got %q", customErr.Severity)
	}

	select {
	case published := <-testPublisher.reports:
		if published != customErr {
			t.Fatal("expected publisher to receive the structured CustomError")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for published report")
	}
}

func TestWrapWithSeverityRespectsPublishThreshold(t *testing.T) {
	testPublisher := &channelPublisher{reports: make(chan *CustomError, 1)}

	Init(Config{
		AppName:            "reporter-test",
		AppEnv:             "production",
		EnablePublishing:   true,
		Publisher:          testPublisher,
		PublishMinSeverity: SeverityCritical,
	})
	defer Close()

	err := WrapWithSeverity(errors.New("unexpected retry drift"), SeverityDanger, "Retry state is inconsistent")
	customErr, ok := err.(*CustomError)
	if !ok {
		t.Fatalf("expected *CustomError, got %T", err)
	}
	if customErr.Severity != SeverityDanger {
		t.Fatalf("expected danger severity, got %q", customErr.Severity)
	}

	select {
	case published := <-testPublisher.reports:
		t.Fatalf("expected danger report not to publish with critical threshold, got %#v", published)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestAutoWrapUsesConfiguredFallbackSeverityForUnknownErrors(t *testing.T) {
	testPublisher := &channelPublisher{reports: make(chan *CustomError, 1)}

	Init(Config{
		AppName:                  "reporter-test",
		AppEnv:                   "production",
		EnablePublishing:         true,
		Publisher:                testPublisher,
		AutoWrapFallbackSeverity: SeverityWarning,
	})
	defer Close()

	err := AutoWrap(errors.New("temporary business rule rejected the request"))
	customErr, ok := err.(*CustomError)
	if !ok {
		t.Fatalf("expected *CustomError, got %T", err)
	}
	if customErr.ErrorType != "GENERAL_ERROR" {
		t.Fatalf("expected GENERAL_ERROR, got %q", customErr.ErrorType)
	}
	if customErr.Severity != SeverityWarning {
		t.Fatalf("expected warning severity, got %q", customErr.Severity)
	}

	select {
	case published := <-testPublisher.reports:
		t.Fatalf("expected fallback warning not to publish by default, got %#v", published)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestAutoWrapClassifiesCommonOperationalErrors(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		errType  string
		severity string
	}{
		{
			name:     "validation",
			raw:      "validation failed: missing required email",
			errType:  "VALIDATION_ERROR",
			severity: SeverityInfo,
		},
		{
			name:     "authentication",
			raw:      "jwt expired",
			errType:  "AUTHENTICATION_ERROR",
			severity: SeverityWarning,
		},
		{
			name:     "authorization",
			raw:      "permission denied",
			errType:  "AUTHORIZATION_ERROR",
			severity: SeverityWarning,
		},
		{
			name:     "rate limit",
			raw:      "too many requests",
			errType:  "RATE_LIMIT_ERROR",
			severity: SeverityWarning,
		},
		{
			name:     "database query",
			raw:      "relation does not exist",
			errType:  "DATABASE_QUERY_ERROR",
			severity: SeverityDanger,
		},
		{
			name:     "serialization",
			raw:      "json: cannot unmarshal string into Go struct field",
			errType:  "SERIALIZATION_ERROR",
			severity: SeverityDanger,
		},
		{
			name:     "external service",
			raw:      "upstream service unavailable",
			errType:  "EXTERNAL_SERVICE_ERROR",
			severity: SeverityDanger,
		},
		{
			name:     "resource exhaustion",
			raw:      "no space left on device",
			errType:  "RESOURCE_EXHAUSTION",
			severity: SeverityCritical,
		},
		{
			name:     "configuration",
			raw:      "missing environment variable DATABASE_URL",
			errType:  "CONFIGURATION_ERROR",
			severity: SeverityCritical,
		},
		{
			name:     "internal server error",
			raw:      "status 500 internal server error",
			errType:  "INTERNAL_SERVER_ERROR",
			severity: SeverityDanger,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			appEnv = "production"
			isPublishing = false
			publisher = nil
			autoWrapFallbackSeverity = SeverityDanger

			err := AutoWrap(errors.New(tt.raw))
			customErr, ok := err.(*CustomError)
			if !ok {
				t.Fatalf("expected *CustomError, got %T", err)
			}
			if customErr.ErrorType != tt.errType {
				t.Fatalf("expected %s, got %q", tt.errType, customErr.ErrorType)
			}
			if customErr.Severity != tt.severity {
				t.Fatalf("expected %s, got %q", tt.severity, customErr.Severity)
			}
		})
	}
}

func TestWrapHTTPStatusUsesStatusCodeAsClassificationSignal(t *testing.T) {
	appEnv = "production"
	appName = "reporter-test"
	isPublishing = false
	publisher = nil

	err := WrapHTTPStatus(errors.New("handler failed"), 500, "Checkout handler failed")
	customErr, ok := err.(*CustomError)
	if !ok {
		t.Fatalf("expected *CustomError, got %T", err)
	}
	if customErr.ErrorType != "INTERNAL_SERVER_ERROR" {
		t.Fatalf("expected INTERNAL_SERVER_ERROR, got %q", customErr.ErrorType)
	}
	if customErr.Severity != SeverityDanger {
		t.Fatalf("expected danger severity, got %q", customErr.Severity)
	}
	if customErr.StatusCode != 500 {
		t.Fatalf("expected status code 500, got %d", customErr.StatusCode)
	}
	if customErr.Description != "Checkout handler failed" {
		t.Fatalf("expected custom description, got %q", customErr.Description)
	}
}

func TestWrapReportCombinesHTTPStatusAndExplicitSeverity(t *testing.T) {
	appEnv = "production"
	appName = "reporter-test"
	isPublishing = false
	publisher = nil

	err := WrapReport(errors.New("payment provider returned 502"), ReportOptions{
		Description: "Payment provider failed during checkout",
		StatusCode:  502,
		Severity:    SeverityCritical,
	})
	customErr, ok := err.(*CustomError)
	if !ok {
		t.Fatalf("expected *CustomError, got %T", err)
	}
	if customErr.ErrorType != "INTERNAL_SERVER_ERROR" {
		t.Fatalf("expected INTERNAL_SERVER_ERROR, got %q", customErr.ErrorType)
	}
	if customErr.Severity != SeverityCritical {
		t.Fatalf("expected critical severity, got %q", customErr.Severity)
	}
	if customErr.StatusCode != 502 {
		t.Fatalf("expected status code 502, got %d", customErr.StatusCode)
	}
	if customErr.Description != "Payment provider failed during checkout" {
		t.Fatalf("expected custom description, got %q", customErr.Description)
	}
}
