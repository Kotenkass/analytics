package main

import (
	"context"
	"errors"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"analytics/internal/api"
	"analytics/internal/clickhouse"
	"analytics/internal/config"
	"analytics/internal/kafkaconsumer"
	"analytics/internal/logging"
	"analytics/internal/metrics"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := logging.Setup("analytics")

	cfg, err := config.Load()
	if err != nil {
		logger.WithError(err).Fatal("configuration error")
	}

	chClient, err := clickhouse.Open(cfg.ClickHouseDSN)
	if err != nil {
		logger.WithError(err).Fatal("failed to connect to clickhouse")
	}
	defer chClient.Close()

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	httpServer := api.New(chClient, logger, reg)
	go func() {
		if err := httpServer.Start(cfg.HTTPListenAddress); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.WithError(err).Fatal("http server failed")
		}
	}()

	consumer, err := kafkaconsumer.New(kafkaconsumer.Options{
		Bootstrap:        cfg.KafkaBootstrap,
		Group:            cfg.KafkaGroup,
		Topic:            cfg.KafkaTopic,
		FlushInterval:    cfg.FlushInterval,
		FlushMaxMessages: cfg.FlushMaxMessages,
		ClickHouse:       chClient,
		Metrics:          m,
		Logger:           logger,
	})
	if err != nil {
		logger.WithError(err).Fatal("failed to create kafka consumer")
	}

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		consumer.Run(ctx)
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.WithError(err).Warn("http server shutdown failed")
	}

	if err := consumer.Stop(shutdownCtx); err != nil {
		logger.WithError(err).Warn("kafka consumer shutdown failed")
	}

	select {
	case <-consumerDone:
	case <-shutdownCtx.Done():
		logger.WithError(shutdownCtx.Err()).Warn("timed out waiting for kafka consumer shutdown")
	}

	logger.Info("shutdown complete")
}
