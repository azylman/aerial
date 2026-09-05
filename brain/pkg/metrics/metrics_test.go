package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMetricsHandler(t *testing.T) {
	RecordTurnCompleted("success", "direct", "gemini-3.8-flash", 2500*time.Millisecond)
	RecordClassifierRun("success", "gemini-3.8-flash", 450*time.Millisecond, 0.85, "wake")
	ActiveWorkers.Inc()
	ActiveWorkers.Dec()
	QueueDepth.Set(3)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	expectedMetrics := []string{
		"aerial_brain_turns_total",
		"aerial_brain_turn_duration_seconds",
		"aerial_brain_active_workers",
		"aerial_brain_queue_depth",
		"aerial_brain_classifier_duration_seconds",
		"aerial_brain_classifier_decisions_total",
		"aerial_brain_classifier_confidence_score",
		"aerial_brain_build_info",
		"go_goroutines",
	}

	for _, metricName := range expectedMetrics {
		if !strings.Contains(body, metricName) {
			t.Errorf("expected output to contain metric %q", metricName)
		}
	}
}

func TestConcurrentMetrics(t *testing.T) {
	var wg sync.WaitGroup
	workers := 20
	iterations := 50

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				ActiveWorkers.Inc()
				RecordTurnCompleted("success", "thread", "gemini-2.5-flash", 1200*time.Millisecond)
				RecordClassifierRun("success", "gemini-3.8-flash", 300*time.Millisecond, 0.90, "wake")
				DiscordEventsTotal.WithLabelValues("message_create").Inc()
				ConfigReloadsTotal.WithLabelValues("Hot-Reload", "success").Inc()
				ActiveWorkers.Dec()
			}
		}(i)
	}

	wg.Wait()
}
