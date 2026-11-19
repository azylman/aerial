package main

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/azylman/aerial/services/vanadis/internal/metrics"
)

var (
	// BuildsActive tracks active Vanadis builds by queue state.
	BuildsActive = metrics.BuildsActive
	// ChunksActive tracks active Vanadis chunks across non-completed builds by lifecycle status.
	ChunksActive = metrics.ChunksActive
	// BuildsTotal tracks total Vanadis builds by lifecycle or terminal status.
	BuildsTotal = metrics.BuildsTotal
	// ChunksTotal tracks total Vanadis chunks by lifecycle event status.
	ChunksTotal = metrics.ChunksTotal
	// QueueDepth tracks chunk execution jobs in the Vanadis JetStream queue categorized by state.
	QueueDepth = metrics.QueueDepth
	// QueueOldestAgeSeconds tracks the age in seconds of the oldest pending job in the Vanadis queue.
	QueueOldestAgeSeconds = metrics.QueueOldestAgeSeconds
	// RecordBuildTotal increments the vanadis_builds_total counter for a given status.
	RecordBuildTotal = metrics.RecordBuildTotal
	// NewHandler returns the Prometheus metrics HTTP handler.
	NewHandler = metrics.NewHandler
)

// consumerStatsQuerier reports pending and in-flight consumer counts.
type consumerStatsQuerier = metrics.ConsumerStatsQuerier

func (s *VanadisServer) collectBuildMetrics(ctx context.Context) {
	if s == nil || s.store == nil {
		return
	}
	builds, err := s.store.ListBuilds(ctx, 100)
	if err != nil {
		return
	}
	metrics.CollectBuildMetrics(builds)
}

func (s *VanadisServer) collectQueueMetrics(ctx context.Context) {
	if s == nil {
		metrics.ResetQueueMetrics()
		return
	}
	metrics.CollectQueueMetrics(ctx, s.consumerStatsQuerier())
}

func (s *VanadisServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if s != nil {
		s.collectBuildMetrics(r.Context())
		s.collectQueueMetrics(r.Context())
	}
	promhttp.Handler().ServeHTTP(w, r)
}
