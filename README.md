# Reporter

`reporter` is a small Go package for turning ordinary errors into structured error reports. It captures the caller file path, line number, function name, error description, raw error text, service name, environment, and timestamp. In production, the same structured payload can be published to Kafka so another service can forward the alert to Telegram or any other notification channel.

## What It Produces

Each wrapped error is represented as `CustomError`:

```json
{
  "timestamp": "2026-06-04 14:35:12",
  "environment": "production",
  "service": "payment-service",
  "error_type": "DATABASE_CONSTRAINT",
  "description": "Failed to save data because of a duplicate data conflict",
  "raw_error": "duplicate key value violates unique constraint",
  "file": "service/order.go",
  "line": 42,
  "function": "service.CreateOrder"
}
```

In non-production environments, `err.Error()` returns a colored terminal-friendly message. In production, `err.Error()` returns JSON.

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

| Field              | Required | Description                                                                                          |
| ------------------ | -------- | ---------------------------------------------------------------------------------------------------- |
| `AppName`          | No       | Service name included in every report. Defaults to `unknown-service`.                                |
| `AppEnv`           | No       | Runtime environment. Defaults to `development`. `production` makes `Error()` return JSON.            |
| `KafkaBrokers`     | Kafka    | Kafka broker list, for example `[]string{"kafka-1:9092", "kafka-2:9092"}`.                           |
| `KafkaTopic`       | Kafka    | Kafka topic used for alert messages.                                                                 |
| `EnablePublishing` | No       | Enables Kafka publishing when `KafkaBrokers` and `KafkaTopic` are also provided. Defaults to `false`. |

Kafka publishing is non-blocking. When `EnablePublishing=true` and Kafka configuration is complete, the package sends the JSON payload in a background goroutine. If Kafka publishing fails, the failure is written to `stderr` so the original error is not lost.

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
    "fmt"

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
        wrappedErr := reporter.AutoWrap(err)
        fmt.Println(wrappedErr.Error())
        return
    }
}

func doWork() error {
    return errors.New("connection refused")
}
```

`AutoWrap` inspects the raw error text and assigns a useful `error_type` and `description` when it recognizes common patterns.

## Custom Description

Use `Wrap` when the application already knows the business context and you want to provide a specific description.

```go
err := repository.SaveOrder(order)
if err != nil {
    return reporter.Wrap(err, "Failed to save checkout order after payment was confirmed")
}
```

This keeps the original error in `raw_error` while adding a human-readable explanation in `description`.

## Automatic Error Classification

`AutoWrap` currently recognizes these common error families:

| Error Type             | Matched Text                                  | Description Purpose                                      |
| ---------------------- | --------------------------------------------- | -------------------------------------------------------- |
| `INFRASTRUCTURE_ERROR` | `connection refused`, `timeout`, `dial tcp`   | Network, database, or third-party connectivity failures. |
| `DATABASE_CONSTRAINT`  | `duplicate key`, `violates unique constraint` | Duplicate or uniqueness conflicts while saving data.     |
| `TIMEOUT_ERROR`        | `context deadline exceeded`                   | Work stopped because the execution deadline was reached. |
| `DATA_NOT_FOUND`       | `no rows in result set`, `not found`          | Requested data does not exist.                           |
| `GENERAL_ERROR`        | Anything else                                 | Fallback for errors that do not match known patterns.    |

## Production Kafka Example

```go
package main

import (
    "log"

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
        log.Println(reporter.AutoWrap(err))
    }
}
```

The Kafka message value is the JSON `CustomError` payload. The message key is the service name, which helps consumers group alerts by service. A Telegram alert worker can consume `KAFKA_TOPIC`, decode the JSON, and format a Telegram message using `service`, `environment`, `file`, `line`, `error_type`, `description`, and `raw_error`.

## Telegram Alert Message Example

A Kafka consumer can convert the JSON payload into a message like this:

```text
[production] payment-service
DATABASE_CONSTRAINT
Failed to save data because of a duplicate data conflict

Location: service/order.go:42
Function: service.CreateOrder
Raw error: duplicate key value violates unique constraint
Time: 2026-06-04 14:35:12
```

## API Summary

```go
type Config struct {
    AppName          string
    AppEnv           string
    KafkaBrokers     []string
    KafkaTopic       string
    EnablePublishing bool
}

func Init(cfg Config)
func Close()
func AutoWrap(err error) error
func Wrap(err error, customDesc string) error
```

- `Init(cfg)` stores reporter configuration and prepares Kafka publishing when `EnablePublishing`, `KafkaBrokers`, and `KafkaTopic` are complete.
- `Close()` closes the Kafka writer during graceful shutdown.
- `AutoWrap(err)` returns `nil` for `nil` input, otherwise returns a structured `CustomError` with automatic classification.
- `Wrap(err, customDesc)` returns `nil` for `nil` input, otherwise returns a structured `CustomError` using your custom description.

The returned error can be type-asserted to `*reporter.CustomError` when you need direct access to fields such as `ErrorType`, `File`, `Line`, or `FunctionName`:

```go
err := reporter.AutoWrap(rawErr)
if customErr, ok := err.(*reporter.CustomError); ok {
    log.Println(customErr.ErrorType, customErr.File, customErr.Line)
}
```

## Internal Helpers

The package also has several unexported helper functions used internally:

| Function           | Purpose                                                                 |
| ------------------ | ----------------------------------------------------------------------- |
| `newError`         | Builds `CustomError`, captures caller metadata, prints it, and triggers Kafka publishing when enabled. |
| `sendToKafka`      | Serializes `CustomError` to JSON and writes it to Kafka.                |
| `containsAny`      | Checks whether a string contains at least one expected substring.       |
| `byteContains`     | Performs byte-level substring matching for internal checks.             |
| `jsonErrTextLower` | Converts ASCII uppercase letters to lowercase for internal normalization. |

These helpers are not exported, so application code should use only `Init`, `Close`, `AutoWrap`, `Wrap`, `Config`, and `CustomError`.

## Notes

- Always call `Init(reporter.Config{...})` before wrapping errors if you want `service`, `environment`, and Kafka publishing to be configured correctly.
- Always call `Close()` during shutdown in services that publish to Kafka.
- Do not use this package as a replacement for normal application error handling. It is intended for reporting and alerting.
