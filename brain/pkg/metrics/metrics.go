package metrics

import (
	"net/http"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Registry is the dedicated Prometheus registry for Aerial Brain.
	Registry = prometheus.NewRegistry()

	// Turn & Worker Pool Metrics
	TurnsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerial_brain_turns_total",
			Help: "Total number of execution turns processed by Aerial Brain.",
		},
		[]string{"status", "trigger_type", "model"},
	)

	TurnDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "aerial_brain_turn_duration_seconds",
			Help:    "Execution duration of turns processed by Aerial Brain in seconds.",
			Buckets: []float64{0.5, 1.0, 5.0, 10.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0, 900.0},
		},
		[]string{"status", "trigger_type", "model"},
	)

	ActiveWorkers = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "aerial_brain_active_workers",
			Help: "Current number of concurrently executing worker goroutines.",
		},
	)

	QueueDepth = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "aerial_brain_queue_depth",
			Help: "Current number of pending CAS tasks/messages queued in memory.",
		},
	)

	InterruptedTurnsRecovered = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "aerial_brain_interrupted_turns_recovered_total",
			Help: "Total count of orphaned processing turns recovered on startup.",
		},
	)

	// Classifier Telemetry (Ambient Relevance & Latency)
	ClassifierDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "aerial_brain_classifier_duration_seconds",
			Help:    "Latency of ambient message relevance classification in seconds.",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 3.0, 5.0, 10.0, 15.0},
		},
		[]string{"status", "model"},
	)

	ClassifierDecisionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerial_brain_classifier_decisions_total",
			Help: "Total number of ambient classifier triage decisions.",
		},
		[]string{"decision", "model"},
	)

	ClassifierConfidenceScore = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aerial_brain_classifier_confidence_score",
			Help:    "Distribution of ambient relevance confidence scores (0.0 - 1.0).",
			Buckets: prometheus.LinearBuckets(0.1, 0.1, 10),
		},
	)

	// Discord Funnel Telemetry
	DiscordEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerial_brain_discord_events_total",
			Help: "Total incoming Discord gateway events received.",
		},
		[]string{"event_type"},
	)

	DiscordMessagesProcessedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerial_brain_discord_messages_processed_total",
			Help: "Total Discord messages evaluated by the ingestion funnel.",
		},
		[]string{"is_ambient", "decision"},
	)

	// Scheduler Telemetry
	SchedulerExecutionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerial_brain_scheduler_executions_total",
			Help: "Total executions of scheduled cron and one-shot routines.",
		},
		[]string{"schedule_type", "status"},
	)

	SchedulerExecutionDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "aerial_brain_scheduler_execution_duration_seconds",
			Help:    "Execution duration of scheduled routines in seconds.",
			Buckets: []float64{0.1, 0.5, 1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0},
		},
		[]string{"schedule_type"},
	)

	// Semantic Memory Telemetry
	MemoryOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerial_brain_memory_operations_total",
			Help: "Total count of semantic memory operations (extract vs search).",
		},
		[]string{"op", "status"},
	)

	MemorySearchDurationSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aerial_brain_memory_search_duration_seconds",
			Help:    "pgvector cosine similarity search duration in seconds.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
		},
	)

	// Hot-Reload & Build Metadata
	ConfigReloadsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerial_brain_config_reloads_total",
			Help: "Total hot-reload events triggered by file watcher or sidecar.",
		},
		[]string{"source", "status"},
	)

	BuildInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "aerial_brain_build_info",
			Help: "Static build metadata for Aerial Brain.",
		},
		[]string{"version", "goversion"},
	)
)

func init() {
	// Register Go runtime and process collectors
	Registry.MustRegister(collectors.NewGoCollector())
	Registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	// Register Aerial Brain application metrics
	Registry.MustRegister(
		TurnsTotal,
		TurnDurationSeconds,
		ActiveWorkers,
		QueueDepth,
		InterruptedTurnsRecovered,
		ClassifierDurationSeconds,
		ClassifierDecisionsTotal,
		ClassifierConfidenceScore,
		DiscordEventsTotal,
		DiscordMessagesProcessedTotal,
		SchedulerExecutionsTotal,
		SchedulerExecutionDurationSeconds,
		MemoryOperationsTotal,
		MemorySearchDurationSeconds,
		ConfigReloadsTotal,
		BuildInfo,
	)

	BuildInfo.WithLabelValues("1.0.0", runtime.Version()).Set(1)
}

// Handler returns the HTTP handler for scraping registered Prometheus metrics.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// RecordTurnCompleted records a finished turn with its duration, status, trigger type, and model.
func RecordTurnCompleted(status, triggerType, model string, duration time.Duration) {
	if status == "" {
		status = "unknown"
	}
	if triggerType == "" {
		triggerType = "direct"
	}
	if model == "" {
		model = "default"
	}
	TurnsTotal.WithLabelValues(status, triggerType, model).Inc()
	TurnDurationSeconds.WithLabelValues(status, triggerType, model).Observe(duration.Seconds())
}

// RecordClassifierRun records ambient classifier execution duration, status, and confidence score.
func RecordClassifierRun(status, model string, duration time.Duration, score float64, decision string) {
	if status == "" {
		status = "unknown"
	}
	if model == "" {
		model = "default"
	}
	if decision == "" {
		decision = "unknown"
	}
	ClassifierDurationSeconds.WithLabelValues(status, model).Observe(duration.Seconds())
	ClassifierDecisionsTotal.WithLabelValues(decision, model).Inc()
	if score >= 0 {
		ClassifierConfidenceScore.Observe(score)
	}
}
