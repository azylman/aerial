package metrics

import (
	"database/sql"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Registry is the dedicated Prometheus registry for Aerial Brain.
	Registry = prometheus.NewRegistry()

	dbStatsOnce sync.Once

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

	// Subprocess Runner Metrics
	RunnerExecutionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerial_brain_runner_executions_total",
			Help: "Total number of Antigravity CLI (agy) subprocess executions.",
		},
		[]string{"status", "model"},
	)

	RunnerDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "aerial_brain_runner_duration_seconds",
			Help:    "Execution duration of Antigravity CLI subprocesses in seconds.",
			Buckets: []float64{0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 900.0},
		},
		[]string{"status", "model"},
	)

	RunnerErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerial_brain_runner_errors_total",
			Help: "Total categorized errors encountered during runner subprocess execution.",
		},
		[]string{"error_type", "model"},
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

	// Discord Funnel & Gateway Telemetry
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

	DiscordGatewayLatencySeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "aerial_brain_discord_gateway_latency_seconds",
			Help: "Current Discord Gateway heartbeat ping latency in seconds.",
		},
	)

	DiscordGatewayReconnectsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "aerial_brain_discord_gateway_reconnects_total",
			Help: "Total Discord Gateway disconnection and reconnection events.",
		},
	)

	DiscordTypingSessionsActive = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "aerial_brain_discord_typing_sessions_active",
			Help: "Current number of active Discord typing indicator refresh loops.",
		},
	)

	DiscordThreadsCreatedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerial_brain_discord_threads_created_total",
			Help: "Total Discord thread creation attempts in forum/text channels.",
		},
		[]string{"status"},
	)

	// Discord Outbound Delivery Telemetry
	DiscordDeliveriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerial_brain_discord_deliveries_total",
			Help: "Total outbound Discord response deliveries attempted.",
		},
		[]string{"status"},
	)

	DiscordDeliveryDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "aerial_brain_discord_delivery_duration_seconds",
			Help:    "Latency of outbound Discord message delivery in seconds.",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0},
		},
		[]string{"status"},
	)

	DiscordMessagesChunkedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "aerial_brain_discord_messages_chunked_total",
			Help: "Total messages split into multiple chunks to respect Discord length limits.",
		},
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

	// Semantic Memory & Embeddings Telemetry
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

	EmbeddingsGeneratedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerial_brain_embeddings_generated_total",
			Help: "Total vector embeddings generated via Ollama or local model.",
		},
		[]string{"model", "type", "status"},
	)

	EmbeddingDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "aerial_brain_embedding_duration_seconds",
			Help:    "Latency of vector embedding generation in seconds.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 3.0},
		},
		[]string{"model", "type", "status"},
	)

	FactsExtractedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerial_brain_facts_extracted_total",
			Help: "Total fact extraction operations evaluated from conversations.",
		},
		[]string{"status"},
	)

	FactExtractionDurationSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aerial_brain_fact_extraction_duration_seconds",
			Help:    "Latency of fact extraction execution in seconds.",
			Buckets: []float64{0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0},
		},
	)

	// Database Query Telemetry
	DBQueryDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "aerial_brain_db_query_duration_seconds",
			Help:    "Database query execution latency by symbolic operation name.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5},
		},
		[]string{"op", "status"},
	)

	// HTTP Ingress API Telemetry
	HTTPRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerial_brain_http_requests_total",
			Help: "Total HTTP requests served by Aerial Brain.",
		},
		[]string{"endpoint", "method", "status"},
	)

	HTTPRequestDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "aerial_brain_http_request_duration_seconds",
			Help:    "HTTP request latency in seconds by endpoint and method.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
		},
		[]string{"endpoint", "method", "status"},
	)

	HTTPInFlightRequests = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "aerial_brain_http_in_flight_requests",
			Help: "Current number of in-flight HTTP requests being processed.",
		},
	)

	// Channel History Telemetry
	ChannelHistoryFetchesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerial_brain_channel_history_fetches_total",
			Help: "Total channel history context fetches during turn bootstrapping.",
		},
		[]string{"source", "status"},
	)

	ChannelHistoryFetchDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "aerial_brain_channel_history_fetch_duration_seconds",
			Help:    "Latency of channel history context fetching in seconds.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0},
		},
		[]string{"source"},
	)

	ChannelHistoryMessagesCount = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aerial_brain_channel_history_messages_count",
			Help:    "Number of historical messages fetched and formatted for turn context.",
			Buckets: []float64{1, 2, 5, 10, 20, 50, 100},
		},
	)

	// Fallback Notifier Telemetry
	FallbackNotificationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerial_brain_fallback_notifications_total",
			Help: "Total dynamic fallback notifications generated for session recovery/errors.",
		},
		[]string{"trigger", "outcome"},
	)

	FallbackNotificationDurationSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aerial_brain_fallback_notification_duration_seconds",
			Help:    "Latency of dynamic fallback notification synthesis in seconds.",
			Buckets: []float64{0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0},
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
		RunnerExecutionsTotal,
		RunnerDurationSeconds,
		RunnerErrorsTotal,
		ClassifierDurationSeconds,
		ClassifierDecisionsTotal,
		ClassifierConfidenceScore,
		DiscordEventsTotal,
		DiscordMessagesProcessedTotal,
		DiscordGatewayLatencySeconds,
		DiscordGatewayReconnectsTotal,
		DiscordTypingSessionsActive,
		DiscordThreadsCreatedTotal,
		DiscordDeliveriesTotal,
		DiscordDeliveryDurationSeconds,
		DiscordMessagesChunkedTotal,
		SchedulerExecutionsTotal,
		SchedulerExecutionDurationSeconds,
		MemoryOperationsTotal,
		MemorySearchDurationSeconds,
		EmbeddingsGeneratedTotal,
		EmbeddingDurationSeconds,
		FactsExtractedTotal,
		FactExtractionDurationSeconds,
		DBQueryDurationSeconds,
		HTTPRequestsTotal,
		HTTPRequestDurationSeconds,
		HTTPInFlightRequests,
		ChannelHistoryFetchesTotal,
		ChannelHistoryFetchDurationSeconds,
		ChannelHistoryMessagesCount,
		FallbackNotificationsTotal,
		FallbackNotificationDurationSeconds,
		ConfigReloadsTotal,
		BuildInfo,
	)

	BuildInfo.WithLabelValues("1.0.0", runtime.Version()).Set(1)
}

// RegisterDBStats registers sql.DB connection pool stats to the dedicated registry safely once.
func RegisterDBStats(db *sql.DB) {
	if db != nil {
		dbStatsOnce.Do(func() {
			Registry.MustRegister(collectors.NewDBStatsCollector(db, "aerial_brain_db"))
		})
	}
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

// RecordRunnerExecution records runner subprocess duration and status.
func RecordRunnerExecution(status, model string, duration time.Duration) {
	if status == "" {
		status = "unknown"
	}
	if model == "" {
		model = "default"
	}
	RunnerExecutionsTotal.WithLabelValues(status, model).Inc()
	RunnerDurationSeconds.WithLabelValues(status, model).Observe(duration.Seconds())
}

// RecordRunnerError records categorized error occurrences.
func RecordRunnerError(errorType, model string) {
	if errorType == "" {
		errorType = "unknown"
	}
	if model == "" {
		model = "default"
	}
	RunnerErrorsTotal.WithLabelValues(errorType, model).Inc()
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

// RecordDelivery records Discord outbound message delivery outcome and latency.
func RecordDelivery(status string, duration time.Duration) {
	if status == "" {
		status = "unknown"
	}
	DiscordDeliveriesTotal.WithLabelValues(status).Inc()
	DiscordDeliveryDurationSeconds.WithLabelValues(status).Observe(duration.Seconds())
}

// RecordMessageChunked increments the counter when a response exceeds single-message length.
func RecordMessageChunked() {
	DiscordMessagesChunkedTotal.Inc()
}

// RecordGatewayLatency sets the current Discord Gateway heartbeat latency gauge.
func RecordGatewayLatency(latency time.Duration) {
	if latency > 0 {
		DiscordGatewayLatencySeconds.Set(latency.Seconds())
	}
}

// RecordGatewayReconnect increments the gateway reconnect counter.
func RecordGatewayReconnect() {
	DiscordGatewayReconnectsTotal.Inc()
}

// RecordThreadCreated records thread creation outcomes.
func RecordThreadCreated(status string) {
	if status == "" {
		status = "unknown"
	}
	DiscordThreadsCreatedTotal.WithLabelValues(status).Inc()
}

// RecordEmbedding records embedding generation count and latency.
func RecordEmbedding(model, embedType, status string, duration time.Duration) {
	if model == "" {
		model = "default"
	}
	if embedType == "" {
		embedType = "document"
	}
	if status == "" {
		status = "unknown"
	}
	EmbeddingsGeneratedTotal.WithLabelValues(model, embedType, status).Inc()
	EmbeddingDurationSeconds.WithLabelValues(model, embedType, status).Observe(duration.Seconds())
}

// RecordFactExtraction records fact extraction latency and status.
func RecordFactExtraction(status string, duration time.Duration) {
	if status == "" {
		status = "unknown"
	}
	FactsExtractedTotal.WithLabelValues(status).Inc()
	FactExtractionDurationSeconds.Observe(duration.Seconds())
}

// RecordDBQuery records database query duration with bounded symbolic operation name.
func RecordDBQuery(op, status string, duration time.Duration) {
	if op == "" {
		op = "unknown"
	}
	if status == "" {
		status = "unknown"
	}
	DBQueryDurationSeconds.WithLabelValues(op, status).Observe(duration.Seconds())
}

// RecordHTTPRequest records HTTP request count and latency.
func RecordHTTPRequest(endpoint, method, status string, duration time.Duration) {
	if endpoint == "" {
		endpoint = "unknown"
	}
	if method == "" {
		method = "GET"
	}
	if status == "" {
		status = "200"
	}
	HTTPRequestsTotal.WithLabelValues(endpoint, method, status).Inc()
	HTTPRequestDurationSeconds.WithLabelValues(endpoint, method, status).Observe(duration.Seconds())
}

// RecordChannelHistoryFetch records context bootstrapping duration and message count.
func RecordChannelHistoryFetch(source, status string, duration time.Duration, count int) {
	if source == "" {
		source = "unknown"
	}
	if status == "" {
		status = "unknown"
	}
	ChannelHistoryFetchesTotal.WithLabelValues(source, status).Inc()
	ChannelHistoryFetchDurationSeconds.WithLabelValues(source).Observe(duration.Seconds())
	if count > 0 {
		ChannelHistoryMessagesCount.Observe(float64(count))
	}
}

// RecordFallbackNotification records dynamic fallback notification execution.
func RecordFallbackNotification(trigger, outcome string, duration time.Duration) {
	if trigger == "" {
		trigger = "unknown"
	}
	if outcome == "" {
		outcome = "unknown"
	}
	FallbackNotificationsTotal.WithLabelValues(trigger, outcome).Inc()
	FallbackNotificationDurationSeconds.Observe(duration.Seconds())
}
