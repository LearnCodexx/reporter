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

// ANSI Color Codes untuk log di terminal lokal
const (
	cReset  = "\033[0m"
	cRed    = "\033[31m"
	cCyan   = "\033[36m"
	cYellow = "\033[33m"
	cGray   = "\033[90m"
	cBold   = "\033[1m"
)

// Global Variables untuk konfigurasi
var (
	appName      string
	appEnv       string
	kafkaWriter  *kafka.Writer
	isPublishing bool
)

// CustomError adalah struktur data log yang rapi dan terstruktur
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

// Error mengubah output sesuai Environment (Lokal = Berwarna, Prod = JSON)
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
	// Format JSON murni untuk server logging
	b, _ := json.Marshal(e)
	return string(b)
}

// Init wajib dipanggil sekali di main.go service Anda sebelum service berjalan
func Init() {
	appName = os.Getenv("APP_NAME")
	appEnv = os.Getenv("APP_ENV")
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	kafkaTopic := os.Getenv("KAFKA_TOPIC")

	if appName == "" {
		appName = "unknown-service"
	}
	if appEnv == "" {
		appEnv = "development"
	}

	brokers := parseBrokers(kafkaBrokers)

	// Hanya aktifkan pengiriman Kafka jika berada di env production dan setup env lengkap
	if appEnv == "production" && len(brokers) > 0 && kafkaTopic != "" {
		kafkaWriter = &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    kafkaTopic,
			Balancer: &kafka.LeastBytes{},
		}
		isPublishing = true
	}
}

// Close digunakan untuk menutup koneksi Kafka secara aman (graceful shutdown)
func Close() {
	if kafkaWriter != nil {
		kafkaWriter.Close()
	}
}

// AutoWrap mendeteksi error secara otomatis dan membuat deskripsi tanpa ketik manual
func AutoWrap(err error) error {
	if err == nil {
		return nil
	}

	errStr := err.Error()
	errText := strings.ToLower(errStr)
	errType := "GENERAL_ERROR"
	autoDesc := "Terjadi kesalahan pada internal sistem"

	// Aturan pencarian otomatis kata kunci error
	switch {
	case strings.Contains(errText, "connection refused"), strings.Contains(errText, "timeout"), strings.Contains(errText, "dial tcp"):
		errType = "INFRASTRUCTURE_ERROR"
		autoDesc = "Gagal terhubung ke database atau third-party API (Timeout/Refused)"

	case strings.Contains(errText, "duplicate key"), strings.Contains(errText, "violates unique constraint"):
		errType = "DATABASE_CONSTRAINT"
		autoDesc = "Gagal menyimpan data karena konflik duplikasi data"

	case strings.Contains(errText, "context deadline exceeded"):
		errType = "TIMEOUT_ERROR"
		autoDesc = "Proses dihentikan karena waktu eksekusi habis (Deadline Exceeded)"

	case strings.Contains(errText, "no rows in result set"), strings.Contains(errText, "not found"):
		errType = "DATA_NOT_FOUND"
		autoDesc = "Data yang diminta tidak ditemukan di dalam sistem"
	}

	return newError(errType, autoDesc, errStr, 2)
}

// Wrap tetap disediakan jika Anda ingin memaksakan deskripsi buatan sendiri
func Wrap(err error, customDesc string) error {
	if err == nil {
		return nil
	}
	return newError("CUSTOM_ERROR", customDesc, err.Error(), 2)
}

// Helper untuk menyusun objek error dan trigger goroutine publish
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

	// Kirim ke kafka secara non-blocking di background menggunakan goroutine
	if isPublishing {
		go sendToKafka(customErr)
	}

	return customErr
}

func sendToKafka(e *CustomError) {
	payload, _ := json.Marshal(e)

	err := kafkaWriter.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(e.Service),
		Value: payload,
	})

	if err != nil {
		// Jika Kafka mati/gagal koneksi, log dicetak ke Stderr agar tidak hilang begitu saja
		fmt.Fprintf(os.Stderr, "\033[31m[Reporter Kafka Error]: %v\033[0m\n", err)
	}
}

func parseBrokers(value string) []string {
	if value == "" {
		return nil
	}

	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		broker := strings.TrimSpace(part)
		if broker != "" {
			brokers = append(brokers, broker)
		}
	}

	return brokers
}
