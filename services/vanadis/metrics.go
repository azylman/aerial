package main

import (
	"context"
	"errors"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// BuildsActive tracks active Vanadis builds by queue state.
	BuildsActive = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "vanadis_builds_active",
			Help: "Number of active Vanadis builds by queue state.",
		},
		[]string{"queue_state"},
	)

	// ChunksActive tracks active Vanadis chunks across non-completed builds by lifecycle status.
	ChunksActive = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "vanadis_chunks_active",
			Help: "Number of active Vanadis chunks across non-completed builds by status.",
		},
		[]string{"status"},
	)

	// BuildsTotal tracks total Vanadis builds by lifecycle or terminal status.
	BuildsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "vanadis_builds_total",
			Help: "Total number of Vanadis builds by lifecycle status.",
		},
		[]string{"status"},
	)

	// ChunksTotal tracks total Vanadis chunks by lifecycle event status.
	ChunksTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "vanadis_chunks_total",
			Help: "Total number of Vanadis chunks by lifecycle event status.",
		},
		[]string{"status"},
	)

	// QueueDepth tracks depth of the Vanadis River job queue by job state.
	QueueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "vanadis_queue_depth",
			Help: "Depth of the Vanadis River job queue by job state.",
		},
		[]string{"state"},
	)

	// QueueOldestAgeSeconds tracks the age in seconds of the oldest pending job in the Vanadis River queue.
	QueueOldestAgeSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "vanadis_queue_oldest_age_seconds",
			Help: "Age in seconds of the oldest pending job in the Vanadis River queue.",
		},
	)
)

var (
	knownQueueStates         = []string{"running", "queued"}
	knownChunkActiveStatuses = []string{"pending", "running", "pr_opened"}
	knownBuildStatuses       = []string{"created", "completed", "failed", "cancelled"}
	knownChunkStatuses       = []string{"dispatched", "merged", "failed", "cancelled"}
	knownRiverJobStates      = []string{"available", "running", "retryable", "scheduled"}
)

func registerVanadisCollector(c prometheus.Collector) {
	if err := prometheus.Register(c); err != nil {
		if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); !ok {
			panic(err)
		}
	}
}

func init() {
	registerVanadisCollector(BuildsActive)
	registerVanadisCollector(ChunksActive)
	registerVanadisCollector(BuildsTotal)
	registerVanadisCollector(ChunksTotal)
	registerVanadisCollector(QueueDepth)
	registerVanadisCollector(QueueOldestAgeSeconds)

	for _, qs := range knownQueueStates {
		BuildsActive.WithLabelValues(qs).Set(0)
	}
	for _, s := range knownChunkActiveStatuses {
		ChunksActive.WithLabelValues(s).Set(0)
	}
	for _, s := range knownBuildStatuses {
		BuildsTotal.WithLabelValues(s).Add(0)
	}
	for _, s := range knownChunkStatuses {
		ChunksTotal.WithLabelValues(s).Add(0)
	}
	for _, js := range knownRiverJobStates {
		QueueDepth.WithLabelValues(js).Set(0)
	}
	QueueOldestAgeSeconds.Set(0)
}

// RecordBuildTotal increments the vanadis_builds_total counter for a given status.
func RecordBuildTotal(status string) {
	BuildsTotal.WithLabelValues(status).Inc()
}

// RecordChunkTotal increments the vanadis_chunks_total counter for a given status.
func RecordChunkTotal(status string) {
	ChunksTotal.WithLabelValues(status).Inc()
}

func (s *VanadisServer) collectBuildMetrics(ctx context.Context) {
	if s == nil || s.store == nil {
		return
	}
	builds, err := s.store.ListBuilds(ctx, 100)
	if err != nil {
		return
	}
	var runningBuilds, queuedBuilds float64
	var pendingChunks, runningChunks, prOpenedChunks float64
	for _, b := range builds {
		switch b.QueueState {
		case "running":
			runningBuilds++
		case "queued":
			queuedBuilds++
		}
		if b.QueueState == "completed" {
			continue
		}
		for _, c := range b.Chunks {
			switch c.Status {
			case "pending":
				pendingChunks++
			case "running", "coding":
				runningChunks++
			case "pr_opened":
				prOpenedChunks++
			}
		}
	}
	BuildsActive.WithLabelValues("running").Set(runningBuilds)
	BuildsActive.WithLabelValues("queued").Set(queuedBuilds)
	ChunksActive.WithLabelValues("pending").Set(pendingChunks)
	ChunksActive.WithLabelValues("running").Set(runningChunks)
	ChunksActive.WithLabelValues("pr_opened").Set(prOpenedChunks)
}

func (s *VanadisServer) collectQueueMetrics(ctx context.Context) {
	stateCounts := make(map[string]float64, len(knownRiverJobStates))
	for _, st := range knownRiverJobStates {
		stateCounts[st] = 0
	}
	var maxAge float64
	defer func() {
		for _, st := range knownRiverJobStates {
			QueueDepth.WithLabelValues(st).Set(stateCounts[st])
		}
		QueueOldestAgeSeconds.Set(maxAge)
	}()

	if s == nil {
		return
	}
	pgStore, ok := s.store.(*PostgresVanadisStore)
	if !ok || pgStore == nil || pgStore.db == nil {
		return
	}
	const q = `SELECT state::text, COUNT(*), COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(created_at)))::float8, 0) FROM river_job WHERE queue = 'vanadis' AND state IN ('available', 'running', 'retryable', 'scheduled') GROUP BY state`
	rows, err := pgStore.db.Query(ctx, q)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var st string
		var count int64
		var age float64
		if scanErr := rows.Scan(&st, &count, &age); scanErr == nil {
			stateCounts[st] = float64(count)
			if age > maxAge {
				maxAge = age
			}
		}
	}
}

func (s *VanadisServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if s != nil {
		s.collectBuildMetrics(r.Context())
		s.collectQueueMetrics(r.Context())
	}
	promhttp.Handler().ServeHTTP(w, r)
}
