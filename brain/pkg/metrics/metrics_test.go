package metrics

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestMetricsRegistryAndHandler(t *testing.T) {
	RecordTurnCompleted("success", "direct", "gemini-2.5-pro", 1500*time.Millisecond)
	RecordClassifierRun("success", "gemini-2.5-flash", 250*time.Millisecond, 0.95, "triage_pass")
	RecordRunnerExecution("success", "gemini-2.5-pro", 2000*time.Millisecond)
	RecordRunnerError("transient", "gemini-2.5-pro")
	RecordDelivery("success", 120*time.Millisecond)
	RecordMessageChunked()
	RecordGatewayLatency(45 * time.Millisecond)
	RecordGatewayReconnect()
	RecordThreadCreated("created")
	RecordEmbedding("nomic-embed-text", "query", "success", 30*time.Millisecond)
	RecordFactExtraction("extracted", 800*time.Millisecond)
	RecordDBQuery("insert_message", "success", 5*time.Millisecond)
	RecordHTTPRequest("/prompt", "POST", "200", 15*time.Millisecond)
	RecordChannelHistoryFetch("discord_api", "success", 150*time.Millisecond, 15)
	RecordFallbackNotification("session_reset", "dynamic", 600*time.Millisecond)

	ActiveWorkers.Set(2)
	QueueDepth.Set(5)
	InterruptedTurnsRecovered.Inc()
	DiscordEventsTotal.WithLabelValues("MESSAGE_CREATE").Inc()
	DiscordMessagesProcessedTotal.WithLabelValues("false", "enqueued").Inc()
	SchedulerExecutionsTotal.WithLabelValues("cron", "success").Inc()
	SchedulerExecutionDurationSeconds.WithLabelValues("cron").Observe(1.2)
	MemoryOperationsTotal.WithLabelValues("search", "success").Inc()
	MemorySearchDurationSeconds.Observe(0.015)
	ConfigReloadsTotal.WithLabelValues("watcher", "success").Inc()

	// Register DB stats with in-memory sqlite
	db, err := sql.Open("sqlite", ":memory:")
	if err == nil {
		defer db.Close()
		RegisterDBStats(db)
	}

	handler := Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	expectedMetrics := []string{
		"aerial_brain_turns_total",
		"aerial_brain_turn_duration_seconds",
		"aerial_brain_active_workers",
		"aerial_brain_queue_depth",
		"aerial_brain_interrupted_turns_recovered_total",
		"aerial_brain_runner_executions_total",
		"aerial_brain_runner_duration_seconds",
		"aerial_brain_runner_errors_total",
		"aerial_brain_classifier_duration_seconds",
		"aerial_brain_classifier_decisions_total",
		"aerial_brain_classifier_confidence_score",
		"aerial_brain_discord_events_total",
		"aerial_brain_discord_messages_processed_total",
		"aerial_brain_discord_gateway_latency_seconds",
		"aerial_brain_discord_gateway_reconnects_total",
		"aerial_brain_discord_typing_sessions_active",
		"aerial_brain_discord_threads_created_total",
		"aerial_brain_discord_deliveries_total",
		"aerial_brain_discord_delivery_duration_seconds",
		"aerial_brain_discord_messages_chunked_total",
		"aerial_brain_scheduler_executions_total",
		"aerial_brain_scheduler_execution_duration_seconds",
		"aerial_brain_memory_operations_total",
		"aerial_brain_memory_search_duration_seconds",
		"aerial_brain_embeddings_generated_total",
		"aerial_brain_embedding_duration_seconds",
		"aerial_brain_facts_extracted_total",
		"aerial_brain_fact_extraction_duration_seconds",
		"aerial_brain_db_query_duration_seconds",
		"aerial_brain_http_requests_total",
		"aerial_brain_http_request_duration_seconds",
		"aerial_brain_http_in_flight_requests",
		"aerial_brain_channel_history_fetches_total",
		"aerial_brain_channel_history_fetch_duration_seconds",
		"aerial_brain_channel_history_messages_count",
		"aerial_brain_fallback_notifications_total",
		"aerial_brain_fallback_notification_duration_seconds",
		"aerial_brain_config_reloads_total",
		"aerial_brain_build_info",
	}

	for _, m := range expectedMetrics {
		if !strings.Contains(body, m) {
			t.Errorf("expected metrics output to contain %q, but it was missing", m)
		}
	}
}
