package main

import (
	"errors"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	FactsTotal = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "coven_facts_total",
			Help: "Total number of facts in long-term semantic memory.",
		},
	)

	OperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "coven_operations_total",
			Help: "Total number of memory operations executed by operation name and status.",
		},
		[]string{"op", "status"},
	)

	OperationDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "coven_operation_duration_seconds",
			Help:    "Execution duration of memory operations in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"op"},
	)

	FactsStoredTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "coven_facts_stored_total",
			Help: "Total number of fact storage actions performed by action type.",
		},
		[]string{"action"},
	)
)

var (
	knownOps         = []string{"search", "store", "forget", "context", "extract", "consolidate"}
	knownStatuses    = []string{"success", "error"}
	knownFactActions = []string{"inserted", "updated", "reinforced", "deleted"}
)

func registerCovenCollector(c prometheus.Collector) {
	if err := prometheus.Register(c); err != nil {
		if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); !ok {
			panic(err)
		}
	}
}

func init() {
	registerCovenCollector(FactsTotal)
	registerCovenCollector(OperationsTotal)
	registerCovenCollector(OperationDurationSeconds)
	registerCovenCollector(FactsStoredTotal)

	FactsTotal.Set(0)
	for _, op := range knownOps {
		OperationDurationSeconds.WithLabelValues(op)
		for _, status := range knownStatuses {
			OperationsTotal.WithLabelValues(op, status).Add(0)
		}
	}
	for _, action := range knownFactActions {
		FactsStoredTotal.WithLabelValues(action).Add(0)
	}
}

// RecordOperation increments coven_operations_total and observes coven_operation_duration_seconds.
func RecordOperation(op, status string, durationSeconds float64) {
	OperationsTotal.WithLabelValues(op, status).Inc()
	OperationDurationSeconds.WithLabelValues(op).Observe(durationSeconds)
}

// RecordFactStored increments coven_facts_stored_total for an action (inserted, updated, reinforced, deleted).
func RecordFactStored(action string) {
	FactsStoredTotal.WithLabelValues(action).Inc()
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if s.handler != nil && s.handler.store != nil {
		if res, err := s.handler.GetFacts(r.Context(), FactsFilter{Limit: 1}); err == nil && res != nil {
			FactsTotal.Set(float64(res.Total))
		}
	}
	promhttp.Handler().ServeHTTP(w, r)
}
