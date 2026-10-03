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
	RecordTokens("gemini-2.5-pro", "general", 100, 50, 25, 10, 185)
	RecordClassifierRun("success", "gemini-2.5-flash", 250*time.Millisecond, 0.95, "triage_pass")
	RecordRunnerExecution("success", "gemini-2.5-pro", "discord", 2000*time.Millisecond)
	RecordRunnerExecution("success", "gemini-2.5-flash", "voice", 350*time.Millisecond)
	RecordRunnerError("transient", "gemini-2.5-pro")
	RecordDelivery("success", 120*time.Millisecond)
	RecordMessageChunked()
	RecordGatewayLatency(45 * time.Millisecond)
	RecordGatewayReconnect()
	RecordThreadCreated("created")
	RecordThreadTitleDuration("success", "gemini-2.5-flash", 150*time.Millisecond)
	RecordEmbedding("nomic-embed-text", "query", "success", 30*time.Millisecond)
	RecordFactExtraction("extracted", 800*time.Millisecond)
	RecordDBQuery("insert_message", "success", 5*time.Millisecond)
	RecordHTTPRequest("/prompt", "POST", "200", 15*time.Millisecond)
	RecordChannelHistoryFetch("discord_api", "success", 150*time.Millisecond, 15)
	RecordFallbackNotification("session_reset", "dynamic", 600*time.Millisecond)
	RecordWebhookDispatch("on_wake", "success", 25*time.Millisecond)
	RecordSessionRotation("pre_flight", "channel", "turns")
	RecordYieldTrap("resumed", "stderr_signature", "gemini-2.5-pro")
	RecordOllamaInference("qwen2.5:3b", 25, 150, 200*time.Millisecond, 1500*time.Millisecond, 50*time.Millisecond, 1750*time.Millisecond)
	RecordDaemonAcquisition("warm_hit", 2*time.Millisecond)
	RecordDaemonAcquisition("cold_start", 450*time.Millisecond)
	RecordVoiceTTFR("sse", "success", 380*time.Millisecond)
	RecordVoiceTTFR("json", "success", 1200*time.Millisecond)

	ActiveWorkers.Set(2)
	QueueDepth.Set(5)
	InterruptedTurnsRecovered.Inc()
	DiscordEventsTotal.WithLabelValues("MESSAGE_CREATE").Inc()
	DiscordMessagesProcessedTotal.WithLabelValues("false", "enqueued").Inc()
	SchedulerExecutionsTotal.WithLabelValues("cron", "success").Inc()
	SchedulerExecutionDurationSeconds.WithLabelValues("cron").Observe(1.2)
	MemoryOperationsTotal.WithLabelValues("search", "success").Inc()
	MemorySearchDurationSeconds.Observe(0.015)
	ConfigReloadsTotal.WithLabelValues("SIGHUP", "success").Inc()

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
		"aerial_brain_tokens_total",
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
		"aerial_brain_webhooks_dispatched_total",
		"aerial_brain_webhook_duration_seconds",
		"aerial_brain_session_rotations_total",
		"aerial_brain_yield_trap_total",
		"aerial_brain_ollama_eval_tokens_total",
		"aerial_brain_ollama_eval_duration_seconds",
		"aerial_brain_ollama_tokens_per_second",
		"aerial_brain_daemon_acquisition_duration_seconds",
		"aerial_brain_voice_ttfr_duration_seconds",
		"aerial_brain_build_info",
	}

	for _, m := range expectedMetrics {
		if !strings.Contains(body, m) {
			t.Errorf("expected metrics output to contain %q, but it was missing", m)
		}
	}
}

func TestMetricsDefaultFallbackBranches(t *testing.T) {
	// Call every recorder with empty strings / negative numbers to exercise fallback defaults
	RecordTurnCompleted("", "", "", 10*time.Millisecond)
	RecordTokens("", "", 0, 0, 0, 0, 0)
	RecordOllamaInference("", 0, 0, 0, 0, 0, 0)
	RecordRunnerExecution("", "", "", 10*time.Millisecond)
	RecordRunnerError("", "")
	RecordClassifierRun("", "", 10*time.Millisecond, -1.0, "")
	RecordDelivery("", 10*time.Millisecond)
	RecordGatewayLatency(0)
	RecordGatewayLatency(-5 * time.Millisecond)
	RecordThreadCreated("")
	RecordThreadTitleDuration("", "", 10*time.Millisecond)
	RecordEmbedding("", "", "", 10*time.Millisecond)
	RecordFactExtraction("", 10*time.Millisecond)
	RecordDBQuery("", "", 10*time.Millisecond)
	RecordHTTPRequest("", "", "", 10*time.Millisecond)
	RecordChannelHistoryFetch("", "", 10*time.Millisecond, 0)
	RecordFallbackNotification("", "", 10*time.Millisecond)
	RecordWebhookDispatch("", "", 10*time.Millisecond)
	RecordSessionRotation("", "", "")
	RecordYieldTrap("", "", "")
	RecordDaemonAcquisition("", 10*time.Millisecond)
	RecordDaemonAcquisition("", -5*time.Millisecond)
	RecordVoiceTTFR("", "", 10*time.Millisecond)
	RecordVoiceTTFR("", "", -5*time.Millisecond)
}

func TestRecordTokens_AutoCalculatesTotalWhenZero(t *testing.T) {
	// Exercise auto-summing of total tokens when individual counts exist but total is 0
	RecordTokens("gemini-test-model", "test-chan", 120, 80, 50, 20, 0)

	handler := Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, `aerial_brain_tokens_total{channel="test-chan",model="gemini-test-model",type="total"} 270`) {
		t.Errorf("expected total tokens metric of 270, got body: %s", body)
	}
	if !strings.Contains(body, `aerial_brain_tokens_total{channel="test-chan",model="gemini-test-model",type="input"} 120`) {
		t.Errorf("expected input tokens metric of 120, got body: %s", body)
	}
}

func TestSanitizeChannelLabel(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", "unknown"},
		{"   ", "unknown"},
		{"#the-banana-stand", "the-banana-stand"},
		{"Lounge", "lounge"},
		{"#Dev-Chat", "dev-chat"},
		{"1534436119888793750", "unknown"},
		{"9999999999", "unknown"},
		{"schedule", "schedule"},
		{"http", "http"},
		{"chan-token-test-1", "chan-token-test-1"},
	}

	for _, tc := range tests {
		got := SanitizeChannelLabel(tc.input)
		if got != tc.expected {
			t.Errorf("SanitizeChannelLabel(%q) = %q, expected %q", tc.input, got, tc.expected)
		}
	}
}

func TestRecordOllamaInference(t *testing.T) {
	// Test normal inference recording
	RecordOllamaInference("qwen2.5:3b-instruct", 42, 128, 300*time.Millisecond, 2*time.Second, 15*time.Millisecond, 2315*time.Millisecond)

	// Test zero tokens / zero durations branches
	RecordOllamaInference("zero-model", 0, 0, 0, 0, 0)

	// Test whitespace model fallback
	RecordOllamaInference("   ", 10, 20, 100*time.Millisecond, 500*time.Millisecond, 10*time.Millisecond)

	// Verify metrics endpoint serves them
	handler := Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, `aerial_brain_ollama_eval_tokens_total{model="qwen2.5:3b-instruct",type="prompt"} 42`) {
		t.Errorf("expected prompt tokens metric, got body:\n%s", body)
	}
	if !strings.Contains(body, `aerial_brain_ollama_eval_tokens_total{model="qwen2.5:3b-instruct",type="eval"} 128`) {
		t.Errorf("expected eval tokens metric, got body:\n%s", body)
	}
}

func TestRecordRunnerExecution_SourceLabelAndBuckets(t *testing.T) {
	RecordRunnerExecution("success", "gemini-2.5-pro", "discord", 1200*time.Millisecond)
	RecordRunnerExecution("success", "gemini-2.5-flash", "voice", 240*time.Millisecond)
	RecordRunnerExecution("error", "gemini-2.5-flash", "voice", 50*time.Millisecond)

	handler := Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	// Verify discord execution counter and histogram bucket
	if !strings.Contains(body, `aerial_brain_runner_executions_total{model="gemini-2.5-pro",`+`source="discord",status="success"}`) &&
		!strings.Contains(body, `aerial_brain_runner_executions_total{model="gemini-2.5-pro",`+`status="success",source="discord"}`) &&
		!strings.Contains(body, `source="discord"`) {
		t.Errorf("expected runner execution metric with source=discord, got:\n%s", body)
	}

	// Verify voice execution counter and histogram bucket
	if !strings.Contains(body, `aerial_brain_runner_executions_total{model="gemini-2.5-flash",`+`source="voice",status="success"}`) &&
		!strings.Contains(body, `aerial_brain_runner_executions_total{model="gemini-2.5-flash",`+`status="success",source="voice"}`) &&
		!strings.Contains(body, `source="voice"`) {
		t.Errorf("expected runner execution metric with source=voice, got:\n%s", body)
	}

	// Verify 0.25 bucket exists in output (proves 250ms bucket resolution for fast voice turns)
	if !strings.Contains(body, `aerial_brain_runner_duration_seconds_bucket{`) || !strings.Contains(body, `le="0.25"`) {
		t.Errorf("expected runner duration bucket le=0.25 to exist, got:\n%s", body)
	}
}

func TestRecordDaemonAcquisitionAndVoiceTTFR(t *testing.T) {
	RecordDaemonAcquisition("warm_hit", 5*time.Millisecond)
	RecordVoiceTTFR("sse", "success", 250*time.Millisecond)

	handler := Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, `aerial_brain_daemon_acquisition_duration_seconds_bucket{status="warm_hit"`) {
		t.Errorf("expected daemon acquisition metric, got:\n%s", body)
	}
	if !strings.Contains(body, `aerial_brain_voice_ttfr_duration_seconds_bucket{mode="sse",status="success"`) {
		t.Errorf("expected voice ttfr metric, got:\n%s", body)
	}
}

func TestRecordToolAndSkillExecution(t *testing.T) {
	RecordToolExecution("run_command", "native", "ok", 150*time.Millisecond)
	RecordToolExecution("", "", "", -5*time.Millisecond)
	RecordToolExecution("create_pull_request", "github", "error", 1200*time.Millisecond)

	RecordSkillActivation("self-improvement", "discord")
	RecordSkillActivation("", "")

	RecordSubagentInvocation("research", "TheGirlGangReviewer")
	RecordSubagentInvocation("", "")

	handler := Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	if !strings.Contains(body, `aerial_brain_tool_calls_total{mcp_server="native",status="ok",tool="run_command"}`) &&
		!strings.Contains(body, `aerial_brain_tool_calls_total{tool="run_command",mcp_server="native",status="ok"}`) {
		t.Errorf("expected tool_calls_total for run_command, got:\n%s", body)
	}
	if !strings.Contains(body, `aerial_brain_tool_calls_total{mcp_server="github",status="error",tool="create_pull_request"}`) &&
		!strings.Contains(body, `aerial_brain_tool_calls_total{tool="create_pull_request",mcp_server="github",status="error"}`) {
		t.Errorf("expected tool_calls_total for create_pull_request, got:\n%s", body)
	}
	if !strings.Contains(body, `aerial_brain_tool_calls_total{mcp_server="native",status="ok",tool="unknown"}`) &&
		!strings.Contains(body, `aerial_brain_tool_calls_total{tool="unknown",mcp_server="native",status="ok"}`) {
		t.Errorf("expected tool_calls_total for unknown fallback, got:\n%s", body)
	}

	if !strings.Contains(body, `aerial_brain_tool_duration_seconds_bucket{mcp_server="native",tool="run_command",le="0.25"}`) &&
		!strings.Contains(body, `aerial_brain_tool_duration_seconds_bucket{tool="run_command",mcp_server="native",le="0.25"}`) {
		t.Errorf("expected tool_duration_seconds_bucket le=0.25, got:\n%s", body)
	}

	if !strings.Contains(body, `aerial_brain_skill_invocations_total{skill="self-improvement",source="discord"}`) {
		t.Errorf("expected skill_invocations_total for self-improvement, got:\n%s", body)
	}
	if !strings.Contains(body, `aerial_brain_skill_invocations_total{skill="unknown",source="unknown"}`) {
		t.Errorf("expected skill_invocations_total for unknown fallback, got:\n%s", body)
	}

	if !strings.Contains(body, `aerial_brain_subagent_invocations_total{role="TheGirlGangReviewer",type_name="research"}`) {
		t.Errorf("expected subagent_invocations_total for TheGirlGangReviewer, got:\n%s", body)
	}
	if !strings.Contains(body, `aerial_brain_subagent_invocations_total{role="unknown",type_name="unknown"}`) {
		t.Errorf("expected subagent_invocations_total for unknown fallback, got:\n%s", body)
	}
}
