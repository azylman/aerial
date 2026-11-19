// Package metrics holds the Vanadis Prometheus collectors and the pure
// build/queue aggregation helpers shared by the service packages.
package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/azylman/aerial/services/vanadis/internal/model"
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
		[]string{labelStatus},
	)

	// BuildsTotal tracks total Vanadis builds by lifecycle or terminal status.
	BuildsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "vanadis_builds_total",
			Help: "Total number of Vanadis builds by lifecycle status.",
		},
		[]string{labelStatus},
	)

	// ChunksTotal tracks total Vanadis chunks by lifecycle event status.
	ChunksTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "vanadis_chunks_total",
			Help: "Total number of Vanadis chunks by lifecycle event status.",
		},
		[]string{labelStatus},
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

const (
	labelStatus    = "status"
	stateRunning   = "running"
	statePending   = "pending"
	stateCompleted = "completed"
)

var (
	knownQueueStates         = []string{stateRunning, "queued"}
	knownChunkActiveStatuses = []string{statePending, stateRunning, "pr_opened"}
	knownBuildStatuses       = []string{"created", stateCompleted, "failed", "cancelled"}
	knownChunkStatuses       = []string{"dispatched", "merged", stateCompleted, "failed", "cancelled"}
	knownQueueDepthStates    = []string{statePending, "in_flight"}
)

// RegisterCollector registers a Prometheus collector, tolerating duplicate registration.
func RegisterCollector(c prometheus.Collector) {
	if err := prometheus.Register(c); err != nil {
		if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); !ok {
			panic(err)
		}
	}
}

func init() {
	RegisterCollector(BuildsActive)
	RegisterCollector(ChunksActive)
	RegisterCollector(BuildsTotal)
	RegisterCollector(ChunksTotal)
	RegisterCollector(QueueDepth)
	RegisterCollector(QueueOldestAgeSeconds)

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

// CollectBuildMetrics aggregates active build and chunk gauges from build entries.
func CollectBuildMetrics(builds []model.VanadisBuildEntry) {
	var runningBuilds, queuedBuilds float64
	var pendingChunks, runningChunks, prOpenedChunks float64
	for _, b := range builds {
		switch b.QueueState {
		case stateRunning:
			runningBuilds++
		case "queued":
			queuedBuilds++
		}
		if b.QueueState == stateCompleted {
			continue
		}
		for _, c := range b.Chunks {
			switch c.Status {
			case statePending:
				pendingChunks++
			case stateRunning, "coding":
				runningChunks++
			case "pr_opened":
				prOpenedChunks++
			}
		}
	}
	BuildsActive.WithLabelValues(stateRunning).Set(runningBuilds)
	BuildsActive.WithLabelValues("queued").Set(queuedBuilds)
	ChunksActive.WithLabelValues(statePending).Set(pendingChunks)
	ChunksActive.WithLabelValues(stateRunning).Set(runningChunks)
	ChunksActive.WithLabelValues("pr_opened").Set(prOpenedChunks)
}

// ConsumerStatsQuerier reports pending and in-flight consumer counts.
type ConsumerStatsQuerier interface {
	ConsumerStats(ctx context.Context, stream, consumer string) (pending int64, inFlight int64, err error)
}

// ResetQueueMetrics zeroes the queue depth gauges.
func ResetQueueMetrics() {
	for _, st := range knownQueueDepthStates {
		QueueDepth.WithLabelValues(st).Set(0)
	}
	QueueOldestAgeSeconds.Set(0)
}

// CollectQueueMetrics refreshes the queue depth gauges from a consumer stats querier.
func CollectQueueMetrics(ctx context.Context, q ConsumerStatsQuerier) {
	ResetQueueMetrics()
	if q == nil {
		return
	}
	pending, inFlight, err := q.ConsumerStats(ctx, "AERIAL_JOBS", "vanadis-chunk")
	if err != nil {
		slog.ErrorContext(ctx, "[vanadis] [metrics] error querying consumer stats", "error", err)
		return
	}
	QueueDepth.WithLabelValues("pending").Set(float64(pending))
	QueueDepth.WithLabelValues("in_flight").Set(float64(inFlight))
}

// NewHandler returns the Prometheus metrics HTTP handler.
func NewHandler() http.Handler { return promhttp.Handler() }
