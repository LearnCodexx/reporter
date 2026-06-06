# Reporter

`reporter` is a small Go package for turning ordinary errors into structured error reports. It captures the caller file path, line number, function name, error description, raw error text, service name, environment, and timestamp. The same structured payload can be published to Kafka or a custom publisher so another service can forward the alert to Telegram or any other notification channel.

## What It Produces

Each wrapped error is represented as `CustomError`:

```json
{
  "timestamp": "2026-06-04 14:35:12",
  "environment": "production",
  "service": "payment-service",
  "severity": "info",
  "error_type": "DATABASE_CONSTRAINT",
  "description": "Failed to save data because of a duplicate data conflict",
  "raw_error": "duplicate key value violates unique constraint",
  "file": "service/order.go",
  "line": 42,
  "function": "service.CreateOrder"
}
```

In non-production environments, `err.Error()` returns a colored terminal-friendly message. In production, `err.Error()` returns JSON.

Publishing does not use `err.Error()`. Publishers receive the structured `CustomError` value, and the built-in Kafka publisher always sends JSON. That means `AppEnv: "development"` still prints a colored local message, but Kafka receives JSON like this:

```json
{
  "timestamp": "2026-06-04 14:35:12",
  "environment": "development",
  "service": "payment-service",
  "severity": "info",
  "error_type": "DATABASE_CONSTRAINT",
  "description": "Failed to save data because of a duplicate data conflict",
  "raw_error": "duplicate key value violates unique constraint",
  "file": "service/order.go",
  "line": 42,
  "function": "service.CreateOrder"
}
```

`AutoWrap` and `Wrap` print the formatted report automatically when they create a `CustomError`. Do not print the returned error again unless you intentionally want duplicate output.

## Configuration

Call `reporter.Init(config)` once during service startup. Configuration is passed explicitly through `reporter.Config`, so this package does not read environment variables directly.

```go
reporter.Init(reporter.Config{
    AppName:          "payment-service",
    AppEnv:           "development",
    EnablePublishing: false,
})
defer reporter.Close()
```

`Config` fields:

| Field                      | Required | Description                                                                                                                         |
| -------------------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `AppName`                  | No       | Service name included in every report. Defaults to `unknown-service`.                                                               |
| `AppEnv`                   | No       | Runtime environment. Defaults to `development`. `production` makes `Error()` return JSON.                                           |
| `KafkaBrokers`             | Kafka    | Kafka broker list, for example `[]string{"kafka-1:9092", "kafka-2:9092"}`.                                                          |
| `KafkaTopic`               | Kafka    | Kafka topic used for alert messages.                                                                                                |
| `EnablePublishing`         | No       | Enables Kafka publishing when `KafkaBrokers` and `KafkaTopic` are also provided. Defaults to `false`.                               |
| `Publisher`                | No       | Optional custom publisher. When provided with `EnablePublishing=true`, it is used instead of Kafka config.                          |
| `PublishMinSeverity`       | No       | Minimum severity that may be published. Defaults to `danger`, so handled errors such as duplicate data are not sent to Kafka/Redis. |
| `AutoWrapFallbackSeverity` | No       | Severity for `AutoWrap` errors that do not match any known pattern. Defaults to `danger`.                                           |

Publishing is non-blocking. When `EnablePublishing=true` and either `Publisher` or Kafka configuration is complete, the package sends the structured payload in a background goroutine. If publishing fails, the failure is written to `stderr` so the original error is not lost.

Severity values:

| Severity   | Typical Use                                                         | Published by Default |
| ---------- | ------------------------------------------------------------------- | -------------------- |
| `info`     | Handled business/data cases such as duplicate data or not found.    | No                   |
| `warning`  | Recoverable issues that should be watched but do not need alerts.   | No                   |
| `danger`   | Serious application issues, HTTP 500-style failures, logic anomaly. | Yes                  |
| `critical` | Infrastructure outage such as database/API connection failure.      | Yes                  |

Set `PublishMinSeverity` when you want a different alert threshold:

```go
reporter.Init(reporter.Config{
    AppName:            "payment-service",
    AppEnv:             "production",
    EnablePublishing:   true,
    KafkaBrokers:       []string{"kafka-1:9092"},
    KafkaTopic:         "service-alerts",
    PublishMinSeverity: reporter.SeverityCritical,
})
```

Set `AutoWrapFallbackSeverity` when unknown `AutoWrap` errors should not automatically trigger Telegram alerts:

```go
reporter.Init(reporter.Config{
    AppName:                  "payment-service",
    AppEnv:                   "production",
    EnablePublishing:         true,
    KafkaBrokers:             []string{"kafka-1:9092"},
    KafkaTopic:               "service-alerts",
    PublishMinSeverity:       reporter.SeverityDanger,
    AutoWrapFallbackSeverity: reporter.SeverityWarning,
})
```

If your application stores config in environment variables, read and map them in your own service before calling `Init`:

```go
reporter.Init(reporter.Config{
    AppName:          os.Getenv("APP_NAME"),
    AppEnv:           os.Getenv("APP_ENV"),
    KafkaBrokers:     strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
    KafkaTopic:       os.Getenv("KAFKA_TOPIC"),
    EnablePublishing: os.Getenv("APP_ENV") == "production",
})
```

## Installation

```bash
go get github.com/learncodexx/reporter
```

If this package is used from a private/local module, replace the module path with your repository import path.

## Basic Usage

```go
package main

import (
    "errors"

    "github.com/learncodexx/reporter"
)

func main() {
    reporter.Init(reporter.Config{
        AppName: "example-service",
        AppEnv:  "development",
    })
    defer reporter.Close()

    err := doWork()
    if err != nil {
        reporter.AutoWrap(err)
        return
    }
}

func doWork() error {
    return errors.New("connection refused")
}
```

`AutoWrap` inspects the raw error text and assigns a useful `error_type` and `description` when it recognizes common patterns.

Use `AutoWrap` as a convenient fallback when the application only has an ordinary `error`. When your application already knows stronger context, prefer the explicit APIs:

- `Wrap` when you know the business description.
- `WrapWithSeverity` when you know the alert priority.
- `WrapHTTPStatus` when the HTTP handler already knows the response status code.
- `WrapReport` when you want to provide several signals in one call.

## Custom Description

Use `Wrap` when the application already knows the business context and you want to provide a specific description.

```go
err := repository.SaveOrder(order)
if err != nil {
    return reporter.Wrap(err, "Failed to save checkout order after payment was confirmed")
}
```

This keeps the original error in `raw_error` while adding a human-readable explanation in `description`.

## HTTP Status Classification

Use `WrapHTTPStatus` when the handler already knows the final response status. This is more reliable than asking `AutoWrap` to infer HTTP 500 from an error string.

```go
if err := checkout(order); err != nil {
    return reporter.WrapHTTPStatus(err, 500, "Checkout handler failed")
}
```

When you know more than one signal, use `WrapReport`:

```go
if err := checkout(order); err != nil {
    return reporter.WrapReport(err, reporter.ReportOptions{
        Description: "Checkout handler failed",
        StatusCode:  500,
        Severity:    reporter.SeverityDanger,
    })
}
```

HTTP status mapping:

| Status Code | Error Type              | Severity   |
| ----------- | ----------------------- | ---------- |
| `>=500`     | `INTERNAL_SERVER_ERROR` | `critical` |
| `429`       | `RATE_LIMIT_ERROR`      | `warning`  |
| `401`       | `AUTHENTICATION_ERROR`  | `warning`  |
| `403`       | `AUTHORIZATION_ERROR`   | `warning`  |
| `404`       | `DATA_NOT_FOUND`        | `info`     |
| `400-499`   | `VALIDATION_ERROR`      | `info`     |

## Automatic Error Classification

`AutoWrap` currently recognizes these common error families:

| Error Type               | Matched Text Examples                                                                          | Description Purpose                                              |
| ------------------------ | ---------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| `INFRASTRUCTURE_ERROR`   | `connection refused`, `dial tcp`, `no route to host`, `broker not available`                   | Network, database, broker, or third-party connectivity failures. |
| `DATABASE_CONSTRAINT`    | `duplicate key`, `violates unique constraint`, `foreign key constraint`, `not null constraint` | Database constraint conflicts while saving data.                 |
| `DATABASE_QUERY_ERROR`   | `deadlock detected`, `relation does not exist`, `unknown column`                               | Query, schema, lock, or transaction failures.                    |
| `TIMEOUT_ERROR`          | `context deadline exceeded`, `gateway timeout`, `i/o timeout`                                  | Work stopped because the execution deadline was reached.         |
| `DATA_NOT_FOUND`         | `no rows in result set`, `record not found`, `404`                                             | Requested data does not exist.                                   |
| `VALIDATION_ERROR`       | `validation failed`, `invalid input`, `missing required`, `400`, `422`                         | Request/input is invalid and usually handled by the application. |
| `AUTHENTICATION_ERROR`   | `unauthorized`, `invalid token`, `jwt expired`, `401`                                          | Authentication is missing, invalid, or expired.                  |
| `AUTHORIZATION_ERROR`    | `forbidden`, `permission denied`, `access denied`, `403`                                       | Caller does not have permission.                                 |
| `RATE_LIMIT_ERROR`       | `rate limit`, `too many requests`, `quota exceeded`, `429`                                     | Rate limit or quota was exceeded.                                |
| `SERIALIZATION_ERROR`    | `json:`, `cannot unmarshal`, `invalid character`, `yaml:`                                      | Encoding or decoding structured data failed.                     |
| `EXTERNAL_SERVICE_ERROR` | `service unavailable`, `bad gateway`, `upstream`, `502`, `503`                                 | Downstream or third-party service failed.                        |
| `RESOURCE_EXHAUSTION`    | `out of memory`, `no space left on device`, `too many open files`                              | Service is running out of critical system resources.             |
| `CONFIGURATION_ERROR`    | `missing environment variable`, `invalid configuration`, `missing config`                      | Runtime configuration is missing or invalid.                     |
| `LOGIC_ANOMALY`          | `panic`, `nil pointer`, `index out of range`, `invalid state`, `invariant`                     | Unexpected application logic failure.                            |
| `INTERNAL_SERVER_ERROR`  | `status 500`, `http 500`, `internal server error`                                              | Service returned an HTTP 500-style failure.                      |
| `GENERAL_ERROR`          | Anything else                                                                                  | Fallback for errors that do not match known patterns.            |

Automatic severity mapping:

| Error Type               | Severity                                         |
| ------------------------ | ------------------------------------------------ |
| `DATABASE_CONSTRAINT`    | `info` or `warning`, depending on the constraint |
| `DATA_NOT_FOUND`         | `info`                                           |
| `VALIDATION_ERROR`       | `info`                                           |
| `AUTHENTICATION_ERROR`   | `warning`                                        |
| `AUTHORIZATION_ERROR`    | `warning`                                        |
| `RATE_LIMIT_ERROR`       | `warning`                                        |
| `TIMEOUT_ERROR`          | `danger`                                         |
| `DATABASE_QUERY_ERROR`   | `danger`                                         |
| `SERIALIZATION_ERROR`    | `danger`                                         |
| `EXTERNAL_SERVICE_ERROR` | `danger`                                         |
| `LOGIC_ANOMALY`          | `danger`                                         |
| `INTERNAL_SERVER_ERROR`  | `danger`                                         |
| `RESOURCE_EXHAUSTION`    | `critical`                                       |
| `CONFIGURATION_ERROR`    | `critical`                                       |
| `GENERAL_ERROR`          | `AutoWrapFallbackSeverity`, default `danger`     |

If `AutoWrap` does not match any known pattern, it returns `GENERAL_ERROR`. The severity comes from `AutoWrapFallbackSeverity`; when that config is empty, reporter uses `danger`. This keeps unknown 500-style failures visible by default, while still allowing a service to lower the fallback to `warning` if it has many known-but-unclassified handled errors.

## Production Kafka Example

```go
package main

import (
    "github.com/learncodexx/reporter"
)

func main() {
    reporter.Init(reporter.Config{
        AppName:          "payment-service",
        AppEnv:           "production",
        KafkaBrokers:     []string{"kafka-1:9092", "kafka-2:9092"},
        KafkaTopic:       "service-alerts",
        EnablePublishing: true,
    })
    defer reporter.Close()

    if err := run(); err != nil {
        reporter.AutoWrap(err)
        return
    }
}
```

The Kafka message value is the JSON `CustomError` payload. The message key is the service name, which helps consumers group alerts by service. A Telegram alert worker can consume `KAFKA_TOPIC`, decode the JSON, and format a Telegram message using `service`, `environment`, `file`, `line`, `error_type`, `description`, and `raw_error`.

## Custom Publisher

Use a custom publisher when you want Redis, a file spool, a webhook, or another transport without adding that dependency to this package.

```go
type Publisher interface {
    Publish(ctx context.Context, report *reporter.CustomError) error
}
```

Example Redis-style publisher:

```go
type RedisPublisher struct {
    client *redis.Client
    stream string
}

func (p *RedisPublisher) Publish(ctx context.Context, report *reporter.CustomError) error {
    payload, err := json.Marshal(report)
    if err != nil {
        return err
    }

    return p.client.XAdd(ctx, &redis.XAddArgs{
        Stream: p.stream,
        Values: map[string]any{
            "service": report.Service,
            "payload": string(payload),
        },
    }).Err()
}
```

Then pass it to `Init`:

```go
reporter.Init(reporter.Config{
    AppName:          "payment-service",
    AppEnv:           "production",
    EnablePublishing: true,
    Publisher: &RedisPublisher{
        client: redisClient,
        stream: "service-alerts",
    },
})
```

## Telegram Alert Message Example

A Kafka consumer can convert the JSON payload into a message like this:

```text
[production] payment-service
[critical] INFRASTRUCTURE_ERROR
Failed to connect to a database or third-party API (timeout/refused)

Location: service/order.go:42
Function: service.CreateOrder
Raw error: dial tcp database:5432 connection refused
Time: 2026-06-04 14:35:12
```

For custom logic anomalies, use explicit severity:

```go
if err := validateState(order); err != nil {
    return reporter.WrapWithSeverity(
        err,
        reporter.SeverityDanger,
        "Checkout state is inconsistent after payment confirmation",
    )
}
```

## API Summary

```go
type Config struct {
    AppName          string
    AppEnv           string
    KafkaBrokers     []string
    KafkaTopic       string
    EnablePublishing bool
    Publisher        Publisher
    PublishMinSeverity string
    AutoWrapFallbackSeverity string
}

type Publisher interface {
    Publish(ctx context.Context, report *CustomError) error
}

type ClosePublisher interface {
    Close() error
}

type ReportOptions struct {
    Description string
    Severity    string
    ErrorType   string
    StatusCode  int
}

func NewKafkaPublisher(brokers []string, topic string) *KafkaPublisher
func Init(cfg Config)
func Close()
func AutoWrap(err error) error
func Wrap(err error, customDesc string) error
func WrapWithSeverity(err error, severity, customDesc string) error
func WrapHTTPStatus(err error, statusCode int, customDesc string) error
func WrapReport(err error, opts ReportOptions) error
```

- `Init(cfg)` stores reporter configuration and prepares publishing when `EnablePublishing` is true and either `Publisher` or Kafka settings are complete.
- `Close()` closes the active publisher during graceful shutdown when it implements `Close() error`.
- `AutoWrap(err)` returns `nil` for `nil` input, otherwise prints and returns a structured `CustomError` with automatic classification.
- `Wrap(err, customDesc)` returns `nil` for `nil` input, otherwise prints and returns a structured `CustomError` using your custom description.
- `WrapWithSeverity(err, severity, customDesc)` works like `Wrap` but lets application code decide alert priority.
- `WrapHTTPStatus(err, statusCode, customDesc)` uses the HTTP status code as the primary classification signal.
- `WrapReport(err, opts)` accepts description, severity, error type, and status code in one call.

The returned error can be type-asserted to `*reporter.CustomError` when you need direct access to fields such as `ErrorType`, `File`, `Line`, or `FunctionName`:

```go
err := reporter.AutoWrap(rawErr)
if customErr, ok := err.(*reporter.CustomError); ok {
    _ = customErr.ErrorType
    _ = customErr.File
    _ = customErr.Line
}
```

## Internal Helpers

The package also has several unexported helper functions. They are implementation details and are not part of the public API:

| Function           | Purpose                                                                                          |
| ------------------ | ------------------------------------------------------------------------------------------------ |
| `newError`         | Builds `CustomError`, captures caller metadata, prints it, and triggers publishing when enabled. |
| `publish`          | Sends `CustomError` to the configured publisher.                                                 |
| `containsAny`      | Checks whether a string contains at least one expected substring.                                |
| `byteContains`     | Performs byte-level substring matching.                                                          |
| `jsonErrTextLower` | Converts ASCII uppercase letters to lowercase.                                                   |

These helpers are not exported, so application code should use only `Init`, `Close`, `AutoWrap`, `Wrap`, `Config`, and `CustomError`.

## Notes

- Always call `Init(reporter.Config{...})` before wrapping errors if you want `service`, `environment`, and publishing to be configured correctly.
- Always call `Close()` during shutdown in services that publish externally.
- Do not use this package as a replacement for normal application error handling. It is intended for reporting and alerting.
