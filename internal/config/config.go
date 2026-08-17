package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	ServiceName       string
	KafkaBootstrap    string
	KafkaTopic        string
	KafkaGroup        string
	ClickHouseDSN     string
	FlushInterval     time.Duration
	FlushMaxMessages  int
	HTTPListenAddress string
}

func Load() (Config, error) {
	var cfg Config
	var err error

	cfg.ServiceName = getenv("SERVICE_NAME", "analytics")
	cfg.KafkaBootstrap = os.Getenv("KAFKA_BOOTSTRAP")
	if cfg.KafkaBootstrap == "" {
		err = joinErr(err, "KAFKA_BOOTSTRAP is required")
	}
	cfg.KafkaTopic = getenv("KAFKA_TOPIC", "answers.received")
	cfg.KafkaGroup = getenv("KAFKA_GROUP", "analytics")
	cfg.ClickHouseDSN = os.Getenv("CLICKHOUSE_DSN")
	if cfg.ClickHouseDSN == "" {
		err = joinErr(err, "CLICKHOUSE_DSN is required")
	}
	cfg.FlushInterval = durationEnv("FLUSH_INTERVAL", time.Second)
	cfg.FlushMaxMessages = intEnv("FLUSH_MAX_MESSAGES", 500)
	cfg.HTTPListenAddress = getenv("HTTP_LISTEN_ADDR", ":8080")

	return cfg, err
}

func getenv(name, fallback string) string {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	return v
}

func intEnv(name string, fallback int) int {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(v)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func durationEnv(name string, fallback time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(v)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func joinErr(existing error, msg string) error {
	if existing == nil {
		return fmt.Errorf("%s", msg)
	}
	return fmt.Errorf("%w; %s", existing, msg)
}
