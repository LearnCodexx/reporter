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

// Global configuration state.
var (
	appName      string
	appEnv       string
	kafkaWriter  *kafka.Writer
	isPublishing bool
)

// Config holds all the configuration parameters required to initialize the reporter.
// Passing this struct explicitly via function parameters provides better flexibility
// and decouples the package from direct environment variable access.
type Config struct {
	AppName          string
	AppEnv           string
	KafkaBrokers     []string
	KafkaTopic       string
	EnablePublishing bool
}

// CustomError is the structured error payload produced by this package.
//
// It contains the information needed for logs and alerts: timestamp,
// environment, service name, error type, human-readable description, raw error
// text, caller file path, caller line number, and caller function name.
// In production, this structure is serialized to JSON and can be published to
// Kafka for downstream alert delivery, such as Telegram notifications.
type CustomError struct {
	Timestamp    string `json:"timestamp"`
	Environment  string `json:"environment"`
	Service      string `json:"service"`
	ErrorType    string `json:"error_type"`
	Description  string `json:"description"`
	RawError     string `json:"raw_error"`
	File         string `json:"file"`
	Line         int    `json:"line"`
	FunctionName string `json:"function"`
}

// Error returns a formatted representation of the structured error.
//
// In non-production environments it returns a colored terminal-friendly string
// containing the timestamp, file, line, error type, description, and raw error.
// In production it returns the JSON representation of CustomError, which is
// suitable for logs, Kafka messages, and alert consumers.
func (e *CustomError) Error() string {
	if appEnv != "production" {
		return fmt.Sprintf("%s[%s]%s %s➔%s %s%s:%d%s |\033[33m [%s]\033[0m %s%s%s (%s%s%s)",
			cGray, e.Timestamp, cReset,
			cRed, cReset,
			cCyan, e.File, e.Line, cReset,
			e.ErrorType,
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
// stores the service name, environment, and optional Kafka settings. When
// EnablePublishing is true and Kafka settings are complete, Init prepares a
// Kafka writer so every reported error can be published asynchronously.
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

	// Build the Kafka connection only when publishing is enabled and settings are complete.
	if cfg.EnablePublishing && len(cfg.KafkaBrokers) > 0 && cfg.KafkaTopic != "" {
		kafkaWriter = &kafka.Writer{
			Addr:     kafka.TCP(cfg.KafkaBrokers...),
			Topic:    cfg.KafkaTopic,
			Balancer: &kafka.LeastBytes{},
		}
		isPublishing = true
	} else {
		// Keep publishing disabled when any requirement is missing or explicitly disabled.
		isPublishing = false
	}
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
	if kafkaWriter != nil {
		kafkaWriter.Close()
	}
}

// AutoWrap converts an ordinary error into a structured report with automatic classification.
//
// AutoWrap returns nil when err is nil. Otherwise it inspects err.Error() and
// assigns an error type and description for known patterns such as connection
// failures, duplicate keys, deadline timeouts, and missing data. The returned
// error captures the caller file path, line number, and function name. In
// production, the report is also published to Kafka when Init configured a
// writer.
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

	// Automatically classify common error text patterns.
	switch {
	case strings.Contains(errText, "connection refused"), strings.Contains(errText, "timeout"), strings.Contains(errText, "dial tcp"):
		errType = "INFRASTRUCTURE_ERROR"
		autoDesc = "Failed to connect to a database or third-party API (timeout/refused)"

	case strings.Contains(errText, "duplicate key"), strings.Contains(errText, "violates unique constraint"):
		errType = "DATABASE_CONSTRAINT"
		autoDesc = "Failed to save data because of a duplicate data conflict"

	case strings.Contains(errText, "context deadline exceeded"):
		errType = "TIMEOUT_ERROR"
		autoDesc = "The process stopped because the execution deadline was exceeded"

	case strings.Contains(errText, "no rows in result set"), strings.Contains(errText, "not found"):
		errType = "DATA_NOT_FOUND"
		autoDesc = "The requested data was not found"
	}

	return newError(errType, autoDesc, errStr, 2)
}

// Wrap converts an ordinary error into a structured report with a custom description.
//
// Wrap returns nil when err is nil. Use Wrap when the application can provide
// better business context than automatic classification. The original error is
// preserved in RawError, while customDesc is stored in Description. The returned
// error captures the caller file path, line number, and function name. In
// production, the report is also published to Kafka when Init configured a
// writer.
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
	return newError("CUSTOM_ERROR", customDesc, err.Error(), 2)
}

// Helper that builds the error object and triggers asynchronous publishing.
func newError(errType, desc, rawErr string, skip int) error {
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
		ErrorType:    errType,
		Description:  desc,
		RawError:     rawErr,
		File:         file,
		Line:         line,
		FunctionName: fnName,
	}

	fmt.Println(customErr.Error())

	// Publish to Kafka in a non-blocking background goroutine.
	if isPublishing {
		go sendToKafka(customErr)
	}

	return customErr
}

// Internal function to publish serialized JSON log payload to the Kafka broker.
func sendToKafka(e *CustomError) {
	payload, _ := json.Marshal(e)

	err := kafkaWriter.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(e.Service),
		Value: payload,
	})

	if err != nil {
		// If Kafka is unreachable, pipe the error trace directly to Stderr to prevent silent data loss
		fmt.Fprintf(os.Stderr, "\033[31m[Reporter Kafka Error]: %v\033[0m\n", err)
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
