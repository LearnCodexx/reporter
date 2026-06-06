// Package reporter turns ordinary Go errors into structured reports that are
// suitable for local logs, production JSON logs, and external alerting.
//
// A report includes the caller file path, line number, function name, service,
// environment, timestamp, raw error, error type, and human-readable description.
// Reports can be published to Kafka or a custom publisher so a separate worker
// can forward the alert to Telegram or another notification channel.
//
// Initialize the package once during application startup:
//
//	reporter.Init(reporter.Config{
//		AppName: "payment-service",
//		AppEnv:  "development",
//	})
//	defer reporter.Close()
//
// Use AutoWrap when reporter should classify the error from its text:
//
//	if err := run(); err != nil {
//		return reporter.AutoWrap(err)
//	}
//
// Use Wrap when the application can provide better business context:
//
//	if err := repository.SaveOrder(order); err != nil {
//		return reporter.Wrap(err, "Failed to save checkout order after payment was confirmed")
//	}
package reporter
