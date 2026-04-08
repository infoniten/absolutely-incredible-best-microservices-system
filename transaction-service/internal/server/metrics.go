package server

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type transactionMetrics struct {
	beginCounter         prometheus.Counter
	saveCounter          prometheus.Counter
	saveErrorCounter     prometheus.Counter
	commitCounter        prometheus.Counter
	commitEmptyCounter   prometheus.Counter
	commitErrorCounter   prometheus.Counter
	rollbackCounter      prometheus.Counter
	rollbackErrorCounter prometheus.Counter
	commitDuration       prometheus.Histogram
	commitDurationByType *prometheus.HistogramVec
}

var (
	metricsOnce sync.Once
	metricsInst *transactionMetrics
)

func newTransactionMetrics() *transactionMetrics {
	metricsOnce.Do(func() {
		metricsInst = &transactionMetrics{
			beginCounter: registerCounter(
				"transactions_begin_total",
				"Total number of begun transactions.",
			),
			saveCounter: registerCounter(
				"transactions_save_total",
				"Total number of successful save operations.",
			),
			saveErrorCounter: registerCounter(
				"transactions_save_errors_total",
				"Total number of failed save operations.",
			),
			commitCounter: registerCounter(
				"transactions_commit_total",
				"Total number of successful commit operations.",
			),
			commitEmptyCounter: registerCounter(
				"transactions_commit_empty_total",
				"Total number of commit calls that had no objects to persist.",
			),
			commitErrorCounter: registerCounter(
				"transactions_commit_errors_total",
				"Total number of failed commit operations.",
			),
			rollbackCounter: registerCounter(
				"transactions_rollback_total",
				"Total number of successful rollback operations.",
			),
			rollbackErrorCounter: registerCounter(
				"transactions_rollback_errors_total",
				"Total number of failed rollback operations.",
			),
			commitDuration: registerHistogram(
				"transactions_commit_duration_seconds",
				"Commit duration in seconds.",
			),
			commitDurationByType: registerHistogramVec(
				"transactions_commit_duration_bytype_seconds",
				"Commit duration by stage/type in seconds.",
				[]string{"type"},
			),
		}
	})

	return metricsInst
}

func registerCounter(name, help string) prometheus.Counter {
	collector := prometheus.NewCounter(prometheus.CounterOpts{
		Name: name,
		Help: help,
	})
	if err := prometheus.DefaultRegisterer.Register(collector); err != nil {
		if alreadyRegistered, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return alreadyRegistered.ExistingCollector.(prometheus.Counter)
		}
	}
	return collector
}

func registerHistogram(name, help string) prometheus.Histogram {
	collector := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    name,
		Help:    help,
		Buckets: prometheus.DefBuckets,
	})
	if err := prometheus.DefaultRegisterer.Register(collector); err != nil {
		if alreadyRegistered, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return alreadyRegistered.ExistingCollector.(prometheus.Histogram)
		}
	}
	return collector
}

func registerHistogramVec(name, help string, labels []string) *prometheus.HistogramVec {
	collector := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    name,
		Help:    help,
		Buckets: prometheus.DefBuckets,
	}, labels)
	if err := prometheus.DefaultRegisterer.Register(collector); err != nil {
		if alreadyRegistered, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return alreadyRegistered.ExistingCollector.(*prometheus.HistogramVec)
		}
	}
	return collector
}

func (m *transactionMetrics) incBegin(_ context.Context) {
	m.beginCounter.Inc()
}

func (m *transactionMetrics) incSave(_ context.Context) {
	m.saveCounter.Inc()
}

func (m *transactionMetrics) incSaveError(_ context.Context) {
	m.saveErrorCounter.Inc()
}

func (m *transactionMetrics) incCommit(_ context.Context) {
	m.commitCounter.Inc()
}

func (m *transactionMetrics) incCommitEmpty(_ context.Context) {
	m.commitEmptyCounter.Inc()
}

func (m *transactionMetrics) incCommitError(_ context.Context) {
	m.commitErrorCounter.Inc()
}

func (m *transactionMetrics) observeCommitDuration(_ context.Context, d time.Duration) {
	m.commitDuration.Observe(d.Seconds())
}

func (m *transactionMetrics) observeCommitDurationByType(_ context.Context, typ string, d time.Duration) {
	m.commitDurationByType.WithLabelValues(typ).Observe(d.Seconds())
}

func (m *transactionMetrics) incRollback(_ context.Context) {
	m.rollbackCounter.Inc()
}

func (m *transactionMetrics) incRollbackError(_ context.Context) {
	m.rollbackErrorCounter.Inc()
}
