package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	KafkaLag       prometheus.Gauge
	CHBatchSize    prometheus.Histogram
	CHInsertErrors prometheus.Counter
}

func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		KafkaLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "kafka_lag",
			Help: "Current Kafka consumer lag.",
		}),
		CHBatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "ch_batch_size",
			Help:    "ClickHouse insert batch size.",
			Buckets: prometheus.DefBuckets,
		}),
		CHInsertErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ch_insert_errors_total",
			Help: "Total ClickHouse insert errors.",
		}),
	}

	reg.MustRegister(m.KafkaLag, m.CHBatchSize, m.CHInsertErrors)
	return m
}

func Handler(reg prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}
