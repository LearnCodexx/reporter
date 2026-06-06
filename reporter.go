package reporter

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// ANSI color codes for local terminal output.
const (
	cReset  = "\033[0m"
	cRed    = "\033[31m"
	cCyan   = "\033[36m"
	cYellow = "\033[33m"
	cGray   = "\033[90m"
	cBold   = "\033[1m"
)

// Severity values describe how urgent a report is.
const (
	SeverityInfo     = "info"
	SeverityWarning  = "warning"
	SeverityDanger   = "danger"
	SeverityCritical = "critical"
)

// Global configuration state.
var (
	appName                  string
	appEnv                   string
	kafkaWriter              *kafka.Writer
	publisher                Publisher
	isPublishing             bool
	publishMinSeverity       string
	autoWrapFallbackSeverity string
)

// Publisher sends a structured error report to an external destination.
//
// Implement this interface when you want to publish reports to something other
// than the built-in Kafka publisher, such as Redis, a file spool, or a webhook.
type Publisher interface {
	Publish(ctx context.Context, report *CustomError) error
}

// ClosePublisher can be implemented by publishers that hold resources.
type ClosePublisher interface {
	Close() error
}

// Config holds all the configuration parameters required to initialize the reporter.
// Passing this struct explicitly via function parameters provides better flexibility
// and decouples the package from direct environment variable access.
type Config struct {
	AppName                  string
	AppEnv                   string
	KafkaBrokers             []string
	KafkaTopic               string
	EnablePublishing         bool
	Publisher                Publisher
	PublishMinSeverity       string
	AutoWrapFallbackSeverity string
}

// KafkaPublisher publishes error reports to Kafka as JSON.
type KafkaPublisher struct {
	writer *kafka.Writer
}

// NewKafkaPublisher creates a Kafka-backed reporter publisher.
func NewKafkaPublisher(brokers []string, topic string) *KafkaPublisher {
	return &KafkaPublisher{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

// Publish serializes the report to JSON and writes it to Kafka.
func (p *KafkaPublisher) Publish(ctx context.Context, report *CustomError) error {
	payload, err := json.Marshal(report)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(report.Service),
		Value: payload,
	})
}

// Close releases the Kafka writer.
func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}

// CustomError is the structured error payload produced by this package.
//
// It contains the information needed for logs and alerts: timestamp,
// environment, service name, error type, human-readable description, raw error
// text, caller file path, caller line number, and caller function name.
// This structure is passed to publishers and can be serialized to JSON for
// downstream alert delivery, such as Telegram notifications.
type CustomError struct {
	Timestamp    string `json:"timestamp"`
	Environment  string `json:"environment"`
	Service      string `json:"service"`
	Severity     string `json:"severity"`
	ErrorType    string `json:"error_type"`
	Description  string `json:"description"`
	RawError     string `json:"raw_error"`
	StatusCode   int    `json:"status_code,omitempty"`
	File         string `json:"file"`
	Line         int    `json:"line"`
	FunctionName string `json:"function"`
}

// ReportOptions describes explicit context for a reported error.
type ReportOptions struct {
	Description string
	Severity    string
	ErrorType   string
	StatusCode  int
}

// Error returns a formatted representation of the structured error.
//
// In non-production environments it returns a colored terminal-friendly string
// containing the timestamp, file, line, error type, description, and raw error.
// In production it returns the JSON representation of CustomError, which is
// suitable for logs, Kafka messages, and alert consumers.
func (e *CustomError) Error() string {
	if appEnv != "production" {
		return fmt.Sprintf("%s[%s]%s %s➔%s %s%s:%d%s |\033[33m [%s:%s]\033[0m %s%s%s (%s%s%s)",
			cGray, e.Timestamp, cReset,
			cRed, cReset,
			cCyan, e.File, e.Line, cReset,
			e.Severity, e.ErrorType,
			cRed+cBold, e.Description, cReset,
			cGray, e.RawError, cReset,
		)
	}
	// Use pure JSON for server logging.
	b, _ := json.Marshal(e)
	return string(b)
}

// Init applies reporter configuration for the current service.
//
// Call Init once during application startup before using AutoWrap or Wrap. It
// stores the service name, environment, and optional publishing settings. When
// EnablePublishing is true and a publisher or complete Kafka settings are
// provided, every reported error can be published asynchronously.
//
// Example:
//
//	reporter.Init(reporter.Config{
//		AppName: "payment-service",
//		AppEnv:  "development",
//	})
//	defer reporter.Close()
func Init(cfg Config) {
	appName = cfg.AppName
	appEnv = cfg.AppEnv

	if appName == "" {
		appName = "unknown-service"
	}
	if appEnv == "" {
		appEnv = "development"
	}
	publishMinSeverity = normalizeSeverity(cfg.PublishMinSeverity)
	if publishMinSeverity == "" {
		publishMinSeverity = SeverityDanger
	}
	autoWrapFallbackSeverity = normalizeSeverity(cfg.AutoWrapFallbackSeverity)
	if autoWrapFallbackSeverity == "" {
		autoWrapFallbackSeverity = SeverityDanger
	}

	kafkaWriter = nil
	publisher = nil

	if !cfg.EnablePublishing {
		isPublishing = false
		return
	}

	if cfg.Publisher != nil {
		publisher = cfg.Publisher
		isPublishing = true
		return
	}

	// Build the Kafka connection only when publishing is enabled and settings are complete.
	if len(cfg.KafkaBrokers) > 0 && cfg.KafkaTopic != "" {
		kafkaPublisher := NewKafkaPublisher(cfg.KafkaBrokers, cfg.KafkaTopic)
		kafkaWriter = kafkaPublisher.writer
		publisher = kafkaPublisher
		isPublishing = true
		return
	}

	// Keep publishing disabled when any requirement is missing or explicitly disabled.
	isPublishing = false
}

// Close releases the Kafka writer used by reporter.
//
// Call Close during graceful shutdown after Init has been called. It is safe to
// call even when Kafka publishing is not enabled.
//
// Example:
//
//	defer reporter.Close()
func Close() {
	if closeable, ok := publisher.(ClosePublisher); ok {
		_ = closeable.Close()
	}
	kafkaWriter = nil
	publisher = nil
	isPublishing = false
}

// AutoWrap converts an ordinary error into a structured report with automatic classification.
//
// AutoWrap returns nil when err is nil. Otherwise it inspects err.Error() and
// assigns an error type and description for known patterns such as connection
// failures, duplicate keys, deadline timeouts, and missing data. The returned
// error captures the caller file path, line number, and function name. In
// production, the report is also published when Init configured a publisher.
//
// Example:
//
//	if err := repository.FindUser(id); err != nil {
//		return reporter.AutoWrap(err)
//	}
func AutoWrap(err error) error {
	if err == nil {
		return nil
	}

	errStr := err.Error()
	errText := strings.ToLower(errStr)
	errType := "GENERAL_ERROR"
	autoDesc := "An internal system error occurred"
	severity := autoWrapFallbackSeverity

	// Automatically classify common error text patterns.
	switch {
	case containsAny(errText,
		"connection refused",
		"connection reset",
		"connection timed out",
		"no route to host",
		"network is unreachable",
		"dial tcp",
		"temporary failure in name resolution",
		"server selection timeout",
		"database is down",
		"db down",
		"broker not available",
	):
		errType = "INFRASTRUCTURE_ERROR"
		autoDesc = "Failed to connect to a database or third-party API (timeout/refused)"
		severity = SeverityCritical

	case containsAny(errText,
		"duplicate key",
		"violates unique constraint",
		"unique constraint failed",
		"duplicate entry",
		"already exists",
	):
		errType = "DATABASE_CONSTRAINT"
		autoDesc = "Failed to save data because of a duplicate data conflict"
		severity = SeverityInfo

	case containsAny(errText,
		"context deadline exceeded",
		"request timeout",
		"gateway timeout",
		"i/o timeout",
	):
		errType = "TIMEOUT_ERROR"
		autoDesc = "The process stopped because the execution deadline was exceeded"
		severity = SeverityDanger

	case containsAny(errText,
		"no rows in result set",
		"record not found",
		"not found",
		"404",
	):
		errType = "DATA_NOT_FOUND"
		autoDesc = "The requested data was not found"
		severity = SeverityInfo

	case containsAny(errText,
		"bad request",
		"validation failed",
		"invalid input",
		"invalid request",
		"missing required",
		"unprocessable entity",
		"malformed",
		"400",
		"422",
	):
		errType = "VALIDATION_ERROR"
		autoDesc = "The request failed because the input is invalid"
		severity = SeverityInfo

	case containsAny(errText,
		"unauthorized",
		"invalid token",
		"token expired",
		"jwt expired",
		"401",
	):
		errType = "AUTHENTICATION_ERROR"
		autoDesc = "The request failed because authentication is invalid or expired"
		severity = SeverityWarning

	case containsAny(errText,
		"forbidden",
		"permission denied",
		"access denied",
		"not allowed",
		"403",
	):
		errType = "AUTHORIZATION_ERROR"
		autoDesc = "The request failed because the caller does not have permission"
		severity = SeverityWarning

	case containsAny(errText,
		"rate limit",
		"too many requests",
		"quota exceeded",
		"429",
	):
		errType = "RATE_LIMIT_ERROR"
		autoDesc = "The request failed because a rate limit or quota was exceeded"
		severity = SeverityWarning

	case containsAny(errText,
		"foreign key constraint",
		"violates foreign key",
		"check constraint",
		"not null constraint",
		"cannot be null",
	):
		errType = "DATABASE_CONSTRAINT"
		autoDesc = "Failed to save data because a database constraint was violated"
		severity = SeverityWarning

	case containsAny(errText,
		"deadlock detected",
		"lock wait timeout",
		"could not serialize access",
		"serialization failure",
	):
		errType = "DATABASE_QUERY_ERROR"
		autoDesc = "Database operation failed because of locking or transaction contention"
		severity = SeverityDanger

	case containsAny(errText,
		"syntax error at or near",
		"unknown column",
		"column does not exist",
		"table does not exist",
		"relation does not exist",
	):
		errType = "DATABASE_QUERY_ERROR"
		autoDesc = "Database query failed because of an invalid schema or query"
		severity = SeverityDanger

	case containsAny(errText,
		"json:",
		"cannot unmarshal",
		"unsupported value",
		"invalid character",
		"yaml:",
	):
		errType = "SERIALIZATION_ERROR"
		autoDesc = "Failed to encode or decode structured data"
		severity = SeverityDanger

	case containsAny(errText,
		"service unavailable",
		"bad gateway",
		"upstream",
		"third-party",
		"external service",
		"502",
		"503",
	):
		errType = "EXTERNAL_SERVICE_ERROR"
		autoDesc = "A downstream or third-party service failed"
		severity = SeverityDanger

	case containsAny(errText,
		"out of memory",
		"cannot allocate memory",
		"no space left on device",
		"disk full",
		"too many open files",
		"resource exhausted",
	):
		errType = "RESOURCE_EXHAUSTION"
		autoDesc = "The service is running out of critical system resources"
		severity = SeverityCritical

	case containsAny(errText,
		"missing environment variable",
		"invalid configuration",
		"config not found",
		"missing config",
	):
		errType = "CONFIGURATION_ERROR"
		autoDesc = "The service failed because configuration is missing or invalid"
		severity = SeverityCritical

	case containsAny(errText,
		"panic",
		"nil pointer",
		"index out of range",
		"slice bounds out of range",
		"invalid state",
		"invariant",
		"unreachable code",
	):
		errType = "LOGIC_ANOMALY"
		autoDesc = "Unexpected application logic anomaly detected"
		severity = SeverityDanger

	case containsAny(errText,
		"internal server error",
		"status 500",
		"http 500",
		"500 internal",
	):
		errType = "INTERNAL_SERVER_ERROR"
		autoDesc = "The service returned an internal server error"
		severity = SeverityDanger
	}

	return newError(errType, severity, autoDesc, errStr, 2)
}

// Wrap converts an ordinary error into a structured report with a custom description.
//
// Wrap returns nil when err is nil. Use Wrap when the application can provide
// better business context than automatic classification. The original error is
// preserved in RawError, while customDesc is stored in Description. The returned
// error captures the caller file path, line number, and function name. In
// production, the report is also published when Init configured a publisher.
//
// Example:
//
//	if err := repository.SaveOrder(order); err != nil {
//		return reporter.Wrap(err, "Failed to save checkout order after payment was confirmed")
//	}
func Wrap(err error, customDesc string) error {
	if err == nil {
		return nil
	}
	return wrapReport(err, ReportOptions{Description: customDesc}, 3)
}

// WrapWithSeverity converts an ordinary error into a structured report with a
// custom description and explicit severity.
func WrapWithSeverity(err error, severity, customDesc string) error {
	if err == nil {
		return nil
	}
	return wrapReport(err, ReportOptions{
		Description: customDesc,
		Severity:    severity,
	}, 3)
}

// WrapHTTPStatus converts an ordinary error into a structured report using the
// HTTP status code as the primary classification signal.
func WrapHTTPStatus(err error, statusCode int, customDesc string) error {
	if err == nil {
		return nil
	}

	return wrapReport(err, ReportOptions{
		Description: customDesc,
		StatusCode:  statusCode,
	}, 3)
}

// WrapReport converts an ordinary error into a structured report using explicit
// options supplied by the application.
func WrapReport(err error, opts ReportOptions) error {
	if err == nil {
		return nil
	}
	return wrapReport(err, opts, 3)
}

func wrapReport(err error, opts ReportOptions, skip int) error {
	errType := "CUSTOM_ERROR"
	severity := SeverityDanger
	desc := opts.Description

	if opts.StatusCode > 0 {
		errType, severity, desc = classifyHTTPStatus(opts.StatusCode)
		if opts.Description != "" {
			desc = opts.Description
		}
	}
	if opts.ErrorType != "" {
		errType = opts.ErrorType
	}
	if normalized := normalizeSeverity(opts.Severity); normalized != "" {
		severity = normalized
	}

	return newErrorWithStatus(errType, severity, desc, err.Error(), opts.StatusCode, skip)
}

// Helper that builds the error object and triggers asynchronous publishing.
func newError(errType, severity, desc, rawErr string, skip int) error {
	return newErrorWithStatus(errType, severity, desc, rawErr, 0, skip+1)
}

func newErrorWithStatus(errType, severity, desc, rawErr string, statusCode int, skip int) error {
	pc, file, line, ok := runtime.Caller(skip)
	fnName := "unknown"
	if ok {
		fn := runtime.FuncForPC(pc)
		if fn != nil {
			fnName = filepath.Base(fn.Name())
		}
		dir := filepath.Base(filepath.Dir(file))
		file = filepath.Join(dir, filepath.Base(file))
	} else {
		file = "unknown"
		line = 0
	}

	customErr := &CustomError{
		Timestamp:    time.Now().Format("2006-01-02 15:04:05"),
		Environment:  appEnv,
		Service:      appName,
		Severity:     normalizeSeverityOrDefault(severity, SeverityDanger),
		ErrorType:    errType,
		Description:  desc,
		RawError:     rawErr,
		StatusCode:   statusCode,
		File:         file,
		Line:         line,
		FunctionName: fnName,
	}

	fmt.Println(customErr.Error())

	// Publish in a non-blocking background goroutine.
	if isPublishing && publisher != nil && shouldPublish(customErr.Severity) {
		activePublisher := publisher
		go publish(activePublisher, customErr)
	}

	return customErr
}

// Internal function to publish the structured payload to the configured publisher.
func publish(p Publisher, e *CustomError) {
	if err := p.Publish(context.Background(), e); err != nil {
		// If publishing fails, pipe the error trace directly to Stderr to prevent silent data loss.
		fmt.Fprintf(os.Stderr, "\033[31m[Reporter Publish Error]: %v\033[0m\n", err)
	}
}

func shouldPublish(severity string) bool {
	return severityRank(severity) >= severityRank(publishMinSeverity)
}

func normalizeSeverityOrDefault(severity, fallback string) string {
	normalized := normalizeSeverity(severity)
	if normalized == "" {
		return fallback
	}
	return normalized
}

func normalizeSeverity(severity string) string {
	switch strings.ToLower(strings.TrimSpace(severity)) {
	case SeverityInfo:
		return SeverityInfo
	case SeverityWarning:
		return SeverityWarning
	case SeverityDanger:
		return SeverityDanger
	case SeverityCritical:
		return SeverityCritical
	default:
		return ""
	}
}

func severityRank(severity string) int {
	switch normalizeSeverity(severity) {
	case SeverityInfo:
		return 1
	case SeverityWarning:
		return 2
	case SeverityDanger:
		return 3
	case SeverityCritical:
		return 4
	default:
		return 3
	}
}

func classifyHTTPStatus(statusCode int) (string, string, string) {
	switch {
	case statusCode >= 500:
		return "INTERNAL_SERVER_ERROR", SeverityCritical, "The service returned an internal server error"
	case statusCode == 429:
		return "RATE_LIMIT_ERROR", SeverityWarning, "The request failed because a rate limit or quota was exceeded"
	case statusCode == 401:
		return "AUTHENTICATION_ERROR", SeverityWarning, "The request failed because authentication is invalid or expired"
	case statusCode == 403:
		return "AUTHORIZATION_ERROR", SeverityWarning, "The request failed because the caller does not have permission"
	case statusCode == 404:
		return "DATA_NOT_FOUND", SeverityInfo, "The requested data was not found"
	case statusCode >= 400:
		return "VALIDATION_ERROR", SeverityInfo, "The request failed because the input is invalid"
	default:
		return "GENERAL_ERROR", autoWrapFallbackSeverity, "An internal system error occurred"
	}
}

// Internal helper to check if a string contains any of the given substrings.
func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if byteContains(s, sub) {
			return true
		}
	}
	return false
}

// Optimized byte-level string match verification.
func byteContains(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// Low-level allocation-optimized case insensitivity converter.
func jsonErrTextLower(s string) string {
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}
