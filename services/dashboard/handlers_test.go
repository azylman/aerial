package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestFactsHandler_TableDriven(t *testing.T) {
	var capturedQuery url.Values
	mockBrain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedQuery = r.URL.Query()
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/facts" {
			http.NotFound(w, r)
			return
		}
		if capturedQuery.Get("q") == "error500" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"db failure"}`))
			return
		}
		if capturedQuery.Get("q") == "badjson" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`not-valid-json`))
			return
		}
		if capturedQuery.Get("limit") == "10" && capturedQuery.Get("offset") == "5" {
			_ = json.NewEncoder(w).Encode(FactsAPIResponse{
				Facts:  nil,
				Total:  0,
				Limit:  10,
				Offset: 5,
				Status: "ok",
			})
			return
		}
		if capturedQuery.Get("limit") == "25" && capturedQuery.Get("q") == "dark mode" {
			_ = json.NewEncoder(w).Encode(FactsAPIResponse{
				Facts: []FactItem{
					{
						ID:               1,
						FactText:         "User prefers dark mode",
						ReinforceCount:   2,
						LastReinforcedAt: time.Now().UTC(),
						CreatedAt:        time.Now().UTC(),
					},
				},
				Total:  1,
				Limit:  25,
				Offset: 0,
			})
			return
		}
		_ = json.NewEncoder(w).Encode(FactsAPIResponse{
			Facts: []FactItem{
				{
					ID:             1,
					FactText:       "Fact 1",
					ReinforceCount: 1,
					CreatedAt:      time.Now().UTC(),
				},
			},
			Total:  1,
			Limit:  0,
			Offset: 0,
		})
	}))
	defer mockBrain.Close()

	offlineURL := "http://127.0.0.1:54321"

	tests := []struct {
		name               string
		brainURL           string
		method             string
		target             string
		wantStatus         int
		wantBodySubstr     []string
		wantBodyNotSubstr  []string
		wantForwardedLimit string
		wantMaxQRunes      int
	}{
		{
			name:           "Success_QueryFilter",
			brainURL:       mockBrain.URL,
			method:         http.MethodGet,
			target:         "/api/facts?limit=25&q=dark%20mode",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{"User prefers dark mode", `"last_reinforced_at":`},
		},
		{
			name:               "Success_UnlimitedDefault",
			brainURL:           mockBrain.URL,
			method:             http.MethodGet,
			target:             "/api/facts",
			wantStatus:         http.StatusOK,
			wantBodyNotSubstr:  []string{`"last_reinforced_at"`},
			wantForwardedLimit: "none",
		},
		{
			name:               "Success_HighLimit",
			brainURL:           mockBrain.URL,
			method:             http.MethodGet,
			target:             "/api/facts?limit=500",
			wantStatus:         http.StatusOK,
			wantForwardedLimit: "500",
		},
		{
			name:           "Success_AllParamsAndNilFacts",
			brainURL:       mockBrain.URL,
			method:         http.MethodGet,
			target:         "/api/facts?limit=10&offset=5&category=ignored&q=" + strings.Repeat("searchterm", 10),
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{`"facts":[]`},
			wantMaxQRunes:  64,
		},
		{
			name:           "DegradedFallback",
			brainURL:       offlineURL,
			method:         http.MethodGet,
			target:         "/api/facts",
			wantStatus:     http.StatusServiceUnavailable,
			wantBodySubstr: []string{`"status":"degraded"`, `"facts":[]`},
		},
		{
			name:       "Upstream500",
			brainURL:   mockBrain.URL,
			method:     http.MethodGet,
			target:     "/api/facts?q=error500",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "BadGateway_InvalidJSON",
			brainURL:   mockBrain.URL,
			method:     http.MethodGet,
			target:     "/api/facts?q=badjson",
			wantStatus: http.StatusBadGateway,
		},
		{
			name:       "MethodNotAllowed",
			brainURL:   mockBrain.URL,
			method:     http.MethodPost,
			target:     "/api/facts",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "InvalidUpstreamURL",
			brainURL:   "http://brain:\x7f8080",
			method:     http.MethodGet,
			target:     "/api/facts",
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := factsHandler(tt.brainURL)
			req := httptest.NewRequest(tt.method, tt.target, nil)
			rr := httptest.NewRecorder()
			h(rr, req)
			if rr.Code != tt.wantStatus {
				t.Fatalf("expected status %d, got %d", tt.wantStatus, rr.Code)
			}
			bodyStr := rr.Body.String()
			for _, sub := range tt.wantBodySubstr {
				if !strings.Contains(bodyStr, sub) {
					t.Errorf("expected body to contain %q, got: %s", sub, bodyStr)
				}
			}
			for _, sub := range tt.wantBodyNotSubstr {
				if strings.Contains(bodyStr, sub) {
					t.Errorf("expected body NOT to contain %q, got: %s", sub, bodyStr)
				}
			}
			if tt.wantForwardedLimit != "" {
				if tt.wantForwardedLimit == "none" {
					if capturedQuery.Get("limit") != "" {
						t.Errorf("expected empty limit forwarded, got %q", capturedQuery.Get("limit"))
					}
				} else if capturedQuery.Get("limit") != tt.wantForwardedLimit {
					t.Errorf("expected limit=%s forwarded, got %q", tt.wantForwardedLimit, capturedQuery.Get("limit"))
				}
			}
			if tt.wantMaxQRunes > 0 {
				gotQ := capturedQuery.Get("q")
				if len([]rune(gotQ)) > tt.wantMaxQRunes {
					t.Errorf("expected q truncated to %d runes, got len %d: %q", tt.wantMaxQRunes, len([]rune(gotQ)), gotQ)
				}
			}
		})
	}
}

func TestSchedulesAndRunsHandlers_TableDriven(t *testing.T) {
	mockBrain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/error500"):
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"db failure"}`))
		case strings.Contains(r.URL.Path, "/badjson"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`not-valid-json`))
		case strings.Contains(r.URL.Path, "/nil/schedules"):
			_ = json.NewEncoder(w).Encode(SchedulesAPIResponse{
				Status:   "ok",
				Crons:    nil,
				OneShots: nil,
			})
		case strings.Contains(r.URL.Path, "/nil/runs"):
			_ = json.NewEncoder(w).Encode(ScheduleRunsAPIResponse{
				Status: "ok",
				Total:  0,
				Limit:  10,
				Offset: 5,
				Runs:   nil,
			})
		case strings.HasSuffix(r.URL.Path, "/schedules/runs"):
			q := r.URL.Query()
			if q.Get("limit") != "10" || q.Get("offset") != "5" || q.Get("schedule_id") != "cron-1" || q.Get("status") != "success" {
				t.Errorf("unexpected upstream query params: %s", r.URL.RawQuery)
			}
			_ = json.NewEncoder(w).Encode(ScheduleRunsAPIResponse{
				Status: "ok", Total: 42, Limit: 10, Offset: 5,
				Runs: []ScheduleRun{{ID: "cron-brief-101", ScheduleID: "cron-1", ScheduleType: "cron", Prompt: "Daily summary", Title: "Morning Brief #101", Status: "success", StartedAt: time.Now().UTC().Add(-1 * time.Hour), DurationMs: 1250}},
			})
		case strings.HasSuffix(r.URL.Path, "/schedules"):
			_ = json.NewEncoder(w).Encode(SchedulesAPIResponse{
				Status:   "ok",
				Summary:  ScheduleSummaryMetrics{TotalActive: 3, CronCount: 2, OneShotCount: 1, TotalRuns24h: 15, SuccessRate24h: 93.3},
				Crons:    []CronSchedule{{ID: "cron-1", ChannelID: "chan-1", TitlePrefix: "Morning Brief", CronExpr: "0 9 * * *", CronDescription: "Every day at 9:00 AM", Prompt: "Generate morning brief", Timezone: "America/Los_Angeles", Enabled: true}},
				OneShots: []OneShotSchedule{{ID: "oneshot-1", ThreadID: "thread-1", Prompt: "Remind about tea", RunAt: time.Now().UTC().Add(10 * time.Minute)}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer mockBrain.Close()

	mockHaro := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/schedules/runs"):
			_ = json.NewEncoder(w).Encode(ScheduleRunsAPIResponse{
				Status: "ok", Total: 42, Limit: 10, Offset: 5,
				Runs: []ScheduleRun{{ID: "haro-run-101", ScheduleID: "cron-1", ScheduleType: "cron", Prompt: "Haro summary", Title: "Haro Brief #101", Status: "success", StartedAt: time.Now().UTC().Add(-1 * time.Hour), DurationMs: 1250}},
			})
		case strings.HasSuffix(r.URL.Path, "/schedules"):
			_ = json.NewEncoder(w).Encode(SchedulesAPIResponse{
				Status:   "ok",
				Summary:  ScheduleSummaryMetrics{TotalActive: 1, CronCount: 1, OneShotCount: 0, TotalRuns24h: 1, SuccessRate24h: 100.0},
				Crons:    []CronSchedule{{ID: "haro-cron-1", ChannelID: "chan-1", TitlePrefix: "Haro Schedule", CronExpr: "0 * * * *", CronDescription: "Every hour", Prompt: "Haro run", Timezone: "America/Los_Angeles", Enabled: true}},
				OneShots: []OneShotSchedule{},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer mockHaro.Close()

	offlineURL := "http://127.0.0.1:54321"
	tests := []struct {
		name           string
		isRuns         bool
		useMux         bool
		haroURL        string
		brainURL       string
		method         string
		target         string
		wantStatus     int
		wantBodySubstr []string
	}{
		{
			name:           "Schedules_Success",
			brainURL:       mockBrain.URL,
			method:         "GET",
			target:         "/api/schedules",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{"Morning Brief", "Remind about tea", `"total_active":3`},
		},
		{
			name:           "Schedules_SuccessParams_NilSlices",
			brainURL:       mockBrain.URL + "/nil",
			method:         "GET",
			target:         "/api/schedules?limit=5&offset=2&active=true&q=weather",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{`"crons":[]`, `"one_shots":[]`},
		},
		{
			name:       "Schedules_ActiveFalse",
			brainURL:   mockBrain.URL,
			method:     "GET",
			target:     "/api/schedules?active=false",
			wantStatus: http.StatusOK,
		},
		{
			name:           "Schedules_DegradedFallback",
			brainURL:       offlineURL,
			method:         "GET",
			target:         "/api/schedules",
			wantStatus:     http.StatusServiceUnavailable,
			wantBodySubstr: []string{`"status":"degraded"`, "Brain service unreachable. Retrying...", `"crons":[]`, `"one_shots":[]`},
		},
		{
			name:       "Schedules_Upstream500",
			brainURL:   mockBrain.URL + "/error500",
			method:     "GET",
			target:     "/api/schedules",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "Schedules_BadGateway",
			brainURL:   mockBrain.URL + "/badjson",
			method:     "GET",
			target:     "/api/schedules",
			wantStatus: http.StatusBadGateway,
		},
		{
			name:       "Schedules_MethodNotAllowed",
			brainURL:   offlineURL,
			method:     "POST",
			target:     "/api/schedules",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "Schedules_InvalidUpstreamURL",
			brainURL:   "http://brain:\x7f8080",
			method:     "GET",
			target:     "/api/schedules",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:           "ScheduleRuns_Success",
			isRuns:         true,
			brainURL:       mockBrain.URL,
			method:         "GET",
			target:         "/api/schedules/runs?limit=10&offset=5&schedule_id=cron-1&status=success",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{"cron-brief-101", `"total":42`, `"duration_ms":1250`},
		},
		{
			name:           "ScheduleRuns_SuccessParams_NilSlices",
			isRuns:         true,
			brainURL:       mockBrain.URL + "/nil/runs",
			method:         "GET",
			target:         "/api/schedules/runs?limit=10&offset=5&status=SUCCESS&schedule_id=42",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{`"runs":[]`},
		},
		{
			name:           "ScheduleRuns_DegradedFallback",
			isRuns:         true,
			brainURL:       offlineURL,
			method:         "GET",
			target:         "/api/schedules/runs?limit=20&offset=10",
			wantStatus:     http.StatusServiceUnavailable,
			wantBodySubstr: []string{`"status":"degraded"`, "Brain service unreachable. Retrying...", `"runs":[]`},
		},
		{
			name:       "ScheduleRuns_Upstream500",
			isRuns:     true,
			brainURL:   mockBrain.URL + "/error500",
			method:     "GET",
			target:     "/api/schedules/runs",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "ScheduleRuns_BadGateway",
			isRuns:     true,
			brainURL:   mockBrain.URL + "/badjson",
			method:     "GET",
			target:     "/api/schedules/runs",
			wantStatus: http.StatusBadGateway,
		},
		{
			name:       "ScheduleRuns_MethodNotAllowed",
			isRuns:     true,
			brainURL:   offlineURL,
			method:     "DELETE",
			target:     "/api/schedules/runs",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "ScheduleRuns_InvalidUpstreamURL",
			isRuns:     true,
			brainURL:   "http://brain:\x7f8080",
			method:     "GET",
			target:     "/api/schedules/runs",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:           "SetupDashboardMux_Schedules_RoutesToHaro",
			useMux:         true,
			brainURL:       offlineURL,
			haroURL:        mockHaro.URL,
			method:         "GET",
			target:         "/api/schedules",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{"Haro Schedule", `"total_active":1`},
		},
		{
			name:           "SetupDashboardMux_DashboardSchedules_RoutesToHaro",
			useMux:         true,
			brainURL:       offlineURL,
			haroURL:        mockHaro.URL,
			method:         "GET",
			target:         "/dashboard/api/schedules",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{"Haro Schedule", `"total_active":1`},
		},
		{
			name:           "SetupDashboardMux_ScheduleRuns_RoutesToHaro",
			useMux:         true,
			isRuns:         true,
			brainURL:       offlineURL,
			haroURL:        mockHaro.URL,
			method:         "GET",
			target:         "/api/schedules/runs?limit=10&offset=5&schedule_id=cron-1&status=success",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{"Haro Brief #101", `"total":42`},
		},
		{
			name:           "SetupDashboardMux_DashboardScheduleRuns_RoutesToHaro",
			useMux:         true,
			isRuns:         true,
			brainURL:       offlineURL,
			haroURL:        mockHaro.URL,
			method:         "GET",
			target:         "/dashboard/api/schedules/runs?limit=10&offset=5&schedule_id=cron-1&status=success",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{"Haro Brief #101", `"total":42`},
		},
		{
			name:           "SetupDashboardMux_Schedules_FallbackToBrainWhenHaroEmpty",
			useMux:         true,
			brainURL:       mockBrain.URL,
			haroURL:        "",
			method:         "GET",
			target:         "/api/schedules",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{"Morning Brief", `"total_active":3`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var h http.Handler
			if tt.useMux {
				h = SetupDashboardMux(DashboardConfig{
					BrainURL: tt.brainURL,
					HaroURL:  tt.haroURL,
				}, nil)
			} else {
				if tt.isRuns {
					h = scheduleRunsHandler(tt.brainURL)
				} else {
					h = schedulesHandler(tt.brainURL)
				}
			}
			req := httptest.NewRequest(tt.method, tt.target, nil)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != tt.wantStatus {
				t.Fatalf("expected status %d, got %d", tt.wantStatus, rr.Code)
			}
			bodyStr := rr.Body.String()
			for _, sub := range tt.wantBodySubstr {
				if !strings.Contains(bodyStr, sub) {
					t.Errorf("expected body to contain %q, got: %s", sub, bodyStr)
				}
			}
		})
	}
}

func TestVanadisBuildsHandlers(t *testing.T) {
	mockVanadis := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/builds" && r.Method == http.MethodGet:
			q := r.URL.Query()
			if q.Get("status") == "error500" {
				http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
				return
			}
			if q.Get("status") == "badjson" {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{not valid json`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(BuildsAPIResponse{
				Status:         "ok",
				Total:          1,
				RunningCount:   1,
				QueuedCount:    0,
				CompletedCount: 0,
				Builds: []VanadisBuildDTO{
					{
						ID:            "build-101",
						Tier:          2,
						Title:         "Vanadis Feature",
						SharedContext: "drawer-only-shared-context",
						Status:        "running",
						QueueState:    "running",
						Chunks: []VanadisChunkDTO{
							{
								ID:                 "chunk-1",
								BuildID:            "build-101",
								ChunkIndex:         1,
								Title:              "Chunk 1",
								Instructions:       "drawer-only-instructions",
								TargetFiles:        []string{"main.go"},
								TargetSymbols:      []string{"Run"},
								AcceptanceCriteria: []string{"pass"},
								Status:             "completed",
							},
						},
					},
				},
			})
		case strings.HasPrefix(r.URL.Path, "/builds/"):
			buildID := strings.TrimPrefix(r.URL.Path, "/builds/")
			switch buildID {
			case "build-101":
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"build": VanadisBuildDTO{
						ID:            "build-101",
						Title:         "Vanadis Feature",
						SharedContext: "drawer-only-shared-context",
						Status:        "running",
					},
					"chunks": []VanadisChunkDTO{
						{
							ID:           "chunk-1",
							BuildID:      "build-101",
							ChunkIndex:   1,
							Title:        "Chunk 1",
							Instructions: "drawer-only-instructions",
							Status:       "completed",
						},
					},
					"entry": VanadisBuildDTO{
						ID:         "build-101",
						QueueState: "running",
					},
				})
			case "test-build-1", "test-build-2":
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(BuildDetailAPIResponse{Status: "ok", Chunks: []VanadisChunkDTO{}})
			case "build-badjson":
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{invalid json detail`))
			case "build-500":
				http.Error(w, `{"error":"internal upstream error"}`, http.StatusInternalServerError)
			default:
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "build not found"})
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer mockVanadis.Close()

	offlineURL := "http://127.0.0.1:54321"

	tests := []struct {
		name              string
		isDetail          bool
		useMux            bool
		vanadisURL        string
		method            string
		target            string
		wantStatus        int
		wantBodySubstr    []string
		wantBodyNotSubstr []string
	}{
		{
			name:              "Builds_Success_WithQuery",
			vanadisURL:        mockVanadis.URL,
			method:            http.MethodGet,
			target:            "/api/builds?limit=25&status=running&queue_state=running",
			wantStatus:        http.StatusOK,
			wantBodySubstr:    []string{"build-101", "Vanadis Feature", "chunk-1"},
			wantBodyNotSubstr: []string{"drawer-only-shared-context", "drawer-only-instructions", "target_files", "target_symbols", "acceptance_criteria"},
		},
		{
			name:           "Builds_DegradedFallback",
			vanadisURL:     offlineURL,
			method:         http.MethodGet,
			target:         "/api/builds",
			wantStatus:     http.StatusServiceUnavailable,
			wantBodySubstr: []string{`"status":"degraded"`, "Vanadis service unreachable. Retrying...", `"builds":[]`},
		},
		{
			name:       "Builds_MethodNotAllowed",
			vanadisURL: mockVanadis.URL,
			method:     http.MethodPost,
			target:     "/api/builds",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "Builds_Upstream500",
			vanadisURL: mockVanadis.URL,
			method:     http.MethodGet,
			target:     "/api/builds?status=error500",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "Builds_BadGateway_InvalidJSON",
			vanadisURL: mockVanadis.URL,
			method:     http.MethodGet,
			target:     "/api/builds?status=badjson",
			wantStatus: http.StatusBadGateway,
		},
		{
			name:       "Builds_InvalidUpstreamURL",
			vanadisURL: "http://vanadis:\x7f8080",
			method:     http.MethodGet,
			target:     "/api/builds",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:           "BuildDetail_Success_APIPrefix",
			isDetail:       true,
			vanadisURL:     mockVanadis.URL,
			method:         http.MethodGet,
			target:         "/api/builds/build-101",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{"build-101", "chunk-1", "drawer-only-shared-context", "drawer-only-instructions"},
		},
		{
			name:           "BuildDetail_Success_DashboardPrefix",
			isDetail:       true,
			vanadisURL:     mockVanadis.URL,
			method:         http.MethodGet,
			target:         "/dashboard/api/builds/build-101",
			wantStatus:     http.StatusOK,
			wantBodySubstr: []string{"build-101"},
		},
		{
			name:           "BuildDetail_NotFound_404",
			isDetail:       true,
			vanadisURL:     mockVanadis.URL,
			method:         http.MethodGet,
			target:         "/api/builds/nonexistent-build",
			wantStatus:     http.StatusNotFound,
			wantBodySubstr: []string{`"status":"error"`, "build not found", `"chunks":[]`},
		},
		{
			name:       "BuildDetail_MethodNotAllowed",
			isDetail:   true,
			vanadisURL: mockVanadis.URL,
			method:     http.MethodDelete,
			target:     "/api/builds/build-101",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:           "BuildDetail_DegradedFallback",
			isDetail:       true,
			vanadisURL:     offlineURL,
			method:         http.MethodGet,
			target:         "/api/builds/build-101",
			wantStatus:     http.StatusServiceUnavailable,
			wantBodySubstr: []string{`"status":"degraded"`, "Vanadis service unreachable. Retrying...", `"chunks":[]`},
		},
		{
			name:       "BuildDetail_BadGateway_InvalidJSON",
			isDetail:   true,
			vanadisURL: mockVanadis.URL,
			method:     http.MethodGet,
			target:     "/api/builds/build-badjson",
			wantStatus: http.StatusBadGateway,
		},
		{
			name:       "BuildDetail_Upstream500",
			isDetail:   true,
			vanadisURL: mockVanadis.URL,
			method:     http.MethodGet,
			target:     "/api/builds/build-500",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "BuildDetail_EmptyID_BadRequest",
			isDetail:   true,
			vanadisURL: mockVanadis.URL,
			method:     http.MethodGet,
			target:     "/api/builds/",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "BuildDetail_PathTraversal_BadRequest",
			isDetail:   true,
			vanadisURL: mockVanadis.URL,
			method:     http.MethodGet,
			target:     "/api/builds/../traversal",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "BuildDetail_InvalidUpstreamURL",
			isDetail:   true,
			vanadisURL: "http://vanadis:\x7f8080",
			method:     http.MethodGet,
			target:     "/api/builds/build-101",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "MuxWiring_BuildsRoot",
			useMux:     true,
			vanadisURL: mockVanadis.URL,
			method:     http.MethodGet,
			target:     "/api/builds",
			wantStatus: http.StatusOK,
		},
		{
			name:       "MuxWiring_DashboardBuildsRoot",
			useMux:     true,
			vanadisURL: mockVanadis.URL,
			method:     http.MethodGet,
			target:     "/dashboard/api/builds",
			wantStatus: http.StatusOK,
		},
		{
			name:       "MuxWiring_BuildDetail1",
			useMux:     true,
			vanadisURL: mockVanadis.URL,
			method:     http.MethodGet,
			target:     "/api/builds/test-build-1",
			wantStatus: http.StatusOK,
		},
		{
			name:       "MuxWiring_DashboardBuildDetail2",
			useMux:     true,
			vanadisURL: mockVanadis.URL,
			method:     http.MethodGet,
			target:     "/dashboard/api/builds/test-build-2",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var h http.Handler
			switch {
			case tt.useMux:
				h = SetupDashboardMux(DashboardConfig{VanadisURL: tt.vanadisURL}, nil)
			case tt.isDetail:
				h = buildDetailHandler(tt.vanadisURL)
			default:
				h = buildsHandler(tt.vanadisURL)
			}
			req := httptest.NewRequest(tt.method, tt.target, nil)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != tt.wantStatus {
				t.Fatalf("expected status %d, got %d (body: %s)", tt.wantStatus, rr.Code, rr.Body.String())
			}
			bodyStr := rr.Body.String()
			for _, sub := range tt.wantBodySubstr {
				if !strings.Contains(bodyStr, sub) {
					t.Errorf("expected body to contain %q, got: %s", sub, bodyStr)
				}
			}
			for _, sub := range tt.wantBodyNotSubstr {
				if strings.Contains(bodyStr, sub) {
					t.Errorf("expected body NOT to contain %q, got: %s", sub, bodyStr)
				}
			}
		})
	}
}
