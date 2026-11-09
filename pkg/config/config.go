// Package config provides environment-based configuration loading with
// sensible defaults, so every service boots with zero local configuration.
package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds the common configuration shared by all services.
type Config struct {
	ServiceName string
	HTTPAddr    string
	MongoURI    string
	MongoDB     string
	RedisAddr   string
	KafkaBrokers []string
	KafkaTopic  string
	OTLPEndpoint string
	Environment string // dev | staging | production
	ShutdownTimeout time.Duration
}

// Load reads configuration from environment variables, applying production
// defaults. Service-specific options can be attached by the caller.
func Load(serviceName string) Config {
	return Config{
		ServiceName: serviceName,
		HTTPAddr:    getEnv("HTTP_ADDR", ":8080"),
		MongoURI:    getEnv("MONGO_URI", "mongodb://localhost:27017"),
		MongoDB:     getEnv("MONGO_DB", "recsplatform"),
		RedisAddr:   getEnv("REDIS_ADDR", "localhost:6379"),
		KafkaBrokers: getEnvSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
		KafkaTopic:  getEnv("KAFKA_TOPIC", "user-events"),
		OTLPEndpoint: getEnv("OTLP_ENDPOINT", "localhost:4317"),
		Environment: getEnv("ENVIRONMENT", "dev"),
		ShutdownTimeout: getEnvDuration("SHUTDOWN_TIMEOUT", 15*time.Second),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvSlice(key string, fallback []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	var out []string
	for _, part := range splitComma(v) {
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func splitComma(s string) []string {
	var parts []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	return parts
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

// Atoi helper used by services for numeric knobs.
func GetEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}
