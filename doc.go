// Package reporter turns ordinary Go errors into structured reports that are
// suitable for local logs, production JSON logs, and Kafka-based alerting.
//
// A report includes the caller file path, line number, function name, service,
// environment, timestamp, raw error, error type, and human-readable description.
// In production, reports can be published to Kafka so a separate worker can
// forward the alert to Telegram or another notification channel.
//
// Initialize the package once during application startup:
//
//	reporter.Init()
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
