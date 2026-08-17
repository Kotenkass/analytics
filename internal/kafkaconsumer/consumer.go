package kafkaconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	ch "analytics/internal/clickhouse"
	"analytics/internal/metrics"
)

const shutdownFlushTimeout = 30 * time.Second

type Options struct {
	Bootstrap        string
	Group            string
	Topic            string
	FlushInterval    time.Duration
	FlushMaxMessages int
	ClickHouse       *ch.Client
	Metrics          *metrics.Metrics
	Logger           *logrus.Logger
}

type Consumer struct {
	client     *kgo.Client
	admin      *kadm.Client
	ch         *ch.Client
	metrics    *metrics.Metrics
	logger     *logrus.Logger
	flushEvery time.Duration
	flushMax   int
	topic      string
	group      string
	running    atomic.Bool
}

type answer = ch.Answer

func New(opts Options) (*Consumer, error) {
	if opts.Bootstrap == "" {
		return nil, errors.New("bootstrap is required")
	}
	if opts.Group == "" {
		return nil, errors.New("group is required")
	}
	if opts.Topic == "" {
		return nil, errors.New("topic is required")
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = time.Second
	}
	if opts.FlushMaxMessages <= 0 {
		opts.FlushMaxMessages = 500
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(opts.Bootstrap, ",")...),
		kgo.ConsumerGroup(opts.Group),
		kgo.ConsumeTopics(opts.Topic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		client:     client,
		admin:      kadm.NewClient(client),
		ch:         opts.ClickHouse,
		metrics:    opts.Metrics,
		logger:     opts.Logger,
		flushEvery: opts.FlushInterval,
		flushMax:   opts.FlushMaxMessages,
		topic:      opts.Topic,
		group:      opts.Group,
	}, nil
}

func (c *Consumer) Run(ctx context.Context) {
	defer c.client.Close()

	ticker := time.NewTicker(c.flushEvery)
	defer ticker.Stop()

	var batch []answer
	var records []*kgo.Record

	flush := func(flushCtx context.Context) error {
		if len(batch) == 0 {
			return nil
		}
		if err := c.ch.InsertAnswers(flushCtx, batch); err != nil {
			c.metrics.CHInsertErrors.Inc()
			return err
		}
		c.metrics.CHBatchSize.Observe(float64(len(batch)))
		if err := c.client.CommitRecords(flushCtx, records...); err != nil {
			return err
		}
		batch = batch[:0]
		records = records[:0]
		return nil
	}

	c.running.Store(true)
	defer c.running.Store(false)

	for {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownFlushTimeout)
			defer cancel()
			if err := flush(shutdownCtx); err != nil {
				c.logger.WithError(err).Error("failed to flush in-flight batch during shutdown")
			}
			return
		case <-ticker.C:
			if err := flush(ctx); err != nil {
				c.logger.WithError(err).Error("failed to flush batch")
			}
		default:
		}

		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return
		}
		if err := fetches.Err(); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownFlushTimeout)
				defer cancel()
				if flushErr := flush(shutdownCtx); flushErr != nil {
					c.logger.WithError(flushErr).Error("failed to flush in-flight batch during shutdown")
				}
				return
			}
			c.logger.WithError(err).Error("kafka fetch error")
			continue
		}

		fetches.EachRecord(func(r *kgo.Record) {
			var a answer
			if err := json.Unmarshal(r.Value, &a); err != nil {
				c.logger.WithError(err).WithField("topic", r.Topic).WithField("partition", r.Partition).WithField("offset", r.Offset).Error("failed to decode message")
				return
			}
			a.Raw = string(r.Value)
			batch = append(batch, a)
			records = append(records, r)
			if len(batch) >= c.flushMax {
				if err := flush(ctx); err != nil {
					c.logger.WithError(err).Error("failed to flush batch")
				}
			}
		})

		if err := c.updateLag(ctx); err != nil {
			c.logger.WithError(err).Debug("failed to update kafka lag")
		}
	}
}

func (c *Consumer) updateLag(ctx context.Context) error {
	groups, err := c.admin.Lag(ctx, c.group)
	if err != nil {
		return err
	}
	var total int64
	for _, group := range groups {
		if group.Group != c.group {
			continue
		}
		for _, memberLag := range group.Lag.Sorted() {
			if memberLag.Topic != c.topic {
				continue
			}
			if memberLag.Err != nil {
				continue
			}
			total += memberLag.Lag
		}
	}
	c.metrics.KafkaLag.Set(float64(total))
	return nil
}

func (c *Consumer) Stop(ctx context.Context) error {
	if c.client == nil || !c.running.Load() {
		return nil
	}
	// Stop is intentionally a no-op because Run drains on context cancellation.
	// This method exists so callers have a named shutdown hook.
	return nil
}

func (c *Consumer) String() string {
	return fmt.Sprintf("kafka consumer group=%s topic=%s", c.group, c.topic)
}
