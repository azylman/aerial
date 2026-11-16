package main

import (
	"context"
	"errors"
	"log"
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
		[]string{fieldStatus},
	)

	// BuildsTotal tracks total Vanadis builds by lifecycle or terminal status.
	BuildsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "vanadis_builds_total",
			Help: "Total number of Vanadis builds by lifecycle status.",
		},
		[]string{fieldStatus},
	)

	// ChunksTotal tracks total Vanadis chunks by lifecycle event status.
	ChunksTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "vanadis_chunks_total",
			Help: "Total number of Vanadis chunks by lifecycle event status.",
		},
		[]string{fieldStatus},
	)

	// QueueDepth tracks chunk execution jobs in the Vanadis JetStream queue categorized by state.
	QueueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "vanadis_queue_depth",
			Help: "Current count of chunk execution jobs by state.",
		},
		[]string{"state"},
	)

	// QueueOldestAgeSeconds tracks the age in seconds of the oldest pending job in the Vanadis queue.
	QueueOldestAgeSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "vanadis_queue_oldest_age_seconds",
			Help: "Age in seconds of the oldest pending job in the Vanadis queue.",
		},
	)
)

var (
	knownQueueStates         = []string{statusRunning, statusQueued}
	knownChunkActiveStatuses = []string{statusPending, statusRunning, statusPROpened}
	knownBuildStatuses       = []string{"created", statusCompleted, statusFailed, statusCancelled}
	knownChunkStatuses       = []string{"dispatched", statusMerged, statusCompleted, statusFailed, statusCancelled}
	knownQueueDepthStates    = []string{statusPending, "in_flight"}
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
	for _, js := range knownQueueDepthStates {
		QueueDepth.WithLabelValues(js).Set(0)
	}
	QueueOldestAgeSeconds.Set(0)
}

// RecordBuildTotal increments the vanadis_builds_total counter for a given status.
func RecordBuildTotal(status string) {
	BuildsTotal.WithLabelValues(status).Inc()
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
		case statusRunning:
			runningBuilds++
		case statusQueued:
			queuedBuilds++
		}
		if b.QueueState == statusCompleted {
			continue
		}
		for _, c := range b.Chunks {
			switch c.Status {
			case statusPending:
				pendingChunks++
			case statusRunning, statusCoding:
				runningChunks++
			case statusPROpened:
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

type consumerStatsQuerier interface {
	ConsumerStats(ctx context.Context, stream, consumer string) (pending int64, inFlight int64, err error)
}

func (s *VanadisServer) collectQueueMetrics(ctx context.Context) {
	for _, st := range knownQueueDepthStates {
		QueueDepth.WithLabelValues(st).Set(0)
	}
	QueueOldestAgeSeconds.Set(0)
	if s == nil {
		return
	}
	q := s.consumerStatsQuerier()
	if q == nil {
		return
	}
	pending, inFlight, err := q.ConsumerStats(ctx, "AERIAL_JOBS", "vanadis-chunk")
	if err != nil {
		log.Printf("[vanadis] [metrics] error querying consumer stats: %v", err)
		return
	}
	QueueDepth.WithLabelValues("pending").Set(float64(pending))
	QueueDepth.WithLabelValues("in_flight").Set(float64(inFlight))
}

func (s *VanadisServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if s != nil {
		s.collectBuildMetrics(r.Context())
		s.collectQueueMetrics(r.Context())
	}
	promhttp.Handler().ServeHTTP(w, r)
}
