package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/k8sauth"
)

// consoleRouteCase is one request through the production mux: cfg wires the upstreams, the
// row asserts status plus exact body (wantBody) or required/forbidden substrings.
type consoleRouteCase struct {
	name         string
	cfg          ConsoleConfig
	method       string
	target       string
	wantStatus   int
	wantBody     string
	wantContains []string
	wantMissing  []string
}

func runConsoleRouteCase(t *testing.T, tc consoleRouteCase) {
	t.Helper()
	rr := httptest.NewRecorder()
	SetupConsoleMux(tc.cfg, nil).ServeHTTP(rr, httptest.NewRequest(tc.method, tc.target, http.NoBody))
	body := strings.TrimSpace(rr.Body.String())
	if rr.Code != tc.wantStatus {
		t.Fatalf("status = %d, want %d (body: %s)", rr.Code, tc.wantStatus, body)
	}
	if tc.wantBody != "" && body != tc.wantBody {
		t.Errorf("body mismatch\n got: %s\nwant: %s", body, tc.wantBody)
	}
	for _, sub := range tc.wantContains {
		if !strings.Contains(body, sub) {
			t.Errorf("body %s missing %q", body, sub)
		}
	}
	for _, sub := range tc.wantMissing {
		if strings.Contains(body, sub) {
			t.Errorf("body %s must not contain %q", body, sub)
		}
	}
}

// newFactsUpstream fakes the memory /facts endpoint. q picks the error and nil-facts branches;
// every other request echoes the forwarded query as the fact text so rows assert what reached
// upstream without shared capture state.
func newFactsUpstream(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		q := r.URL.Query()
		switch {
		case r.URL.Path != "/facts":
			http.NotFound(w, r)
		case q.Get("q") == "error500":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"db failure"}`))
		case q.Get("q") == "badjson":
			_, _ = w.Write([]byte(`not-valid-json`))
		case q.Get("q") == "nilfacts":
			_, _ = w.Write([]byte(`{"status":"ok","facts":null,"total":0,"limit":10,"offset":5}`))
		case q.Get("q") == "dark mode":
			_, _ = w.Write([]byte(`{"facts":[{"id":1,"fact_text":"User prefers dark mode","reinforce_count":2,"last_reinforced_at":"2026-01-02T03:04:05Z","created_at":"2026-01-01T03:04:05Z"}],"total":1,"limit":25}`))
		default:
			resp := fmt.Sprintf(`{"facts":[{"id":1,"fact_text":%q,"reinforce_count":1,"created_at":"2026-01-01T03:04:05Z","agent":"aerial"}],"total":120,"limit":50}`, "query:"+r.URL.RawQuery)
			_, _ = io.WriteString(w, resp)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestFactsHandler(t *testing.T) {
	t.Parallel()
	mock := ConsoleConfig{BrainURL: newFactsUpstream(t).URL}
	offline := ConsoleConfig{BrainURL: downServerURL(t)}
	invalid := ConsoleConfig{BrainURL: "http://brain:\x7f8080"}
	longQ := strings.Repeat("searchterm", 10)

	tests := []consoleRouteCase{
		{name: "QueryFilter", cfg: mock, method: http.MethodGet, target: "/api/facts?limit=25&q=dark%20mode", wantStatus: http.StatusOK, wantContains: []string{"User prefers dark mode", `"last_reinforced_at":`}},
		{name: "AgentFilter_Other", cfg: mock, method: http.MethodGet, target: "/api/facts?agent=agent-alpha", wantStatus: http.StatusOK, wantContains: []string{`"facts":[]`}},
		{name: "PreservesUpstreamTotalLargerThanPage", cfg: mock, method: http.MethodGet, target: "/api/facts?limit=50&agent=aerial", wantStatus: http.StatusOK, wantContains: []string{`"fact_text":"query:agent=aerial\u0026limit=50"`, `"total":120`}},
		{name: "UnlimitedDefault_ForwardsNoLimit", cfg: mock, method: http.MethodGet, target: "/api/facts", wantStatus: http.StatusOK, wantBody: `{"facts":[{"id":1,"fact_text":"query:","reinforce_count":1,"created_at":"2026-01-01T03:04:05Z","agent":"aerial"}],"total":120,"limit":50,"offset":0}`},
		{name: "ConsolePrefix_HighLimitForwarded", cfg: mock, method: http.MethodGet, target: "/console/api/facts?limit=500", wantStatus: http.StatusOK, wantContains: []string{`"fact_text":"query:limit=500"`}},
		{name: "QueryTruncatedTo64Runes", cfg: mock, method: http.MethodGet, target: "/api/facts?q=" + longQ, wantStatus: http.StatusOK, wantContains: []string{`"fact_text":"query:q=` + longQ[:64] + `"`}},
		{name: "AllParamsAndNilFacts", cfg: mock, method: http.MethodGet, target: "/api/facts?limit=10&offset=5&category=ignored&q=nilfacts", wantStatus: http.StatusOK, wantBody: `{"facts":[],"total":0,"limit":10,"offset":5,"status":"ok"}`},
		{name: "DegradedFallback", cfg: offline, method: http.MethodGet, target: "/api/facts", wantStatus: http.StatusServiceUnavailable, wantContains: []string{`"status":"degraded"`, `"facts":[]`}},
		{name: "Upstream500", cfg: mock, method: http.MethodGet, target: "/api/facts?q=error500", wantStatus: http.StatusInternalServerError},
		{name: "BadGateway_InvalidJSON", cfg: mock, method: http.MethodGet, target: "/api/facts?q=badjson", wantStatus: http.StatusBadGateway},
		{name: "MethodNotAllowed", cfg: mock, method: http.MethodPost, target: "/api/facts", wantStatus: http.StatusMethodNotAllowed},
		{name: "InvalidUpstreamURL", cfg: invalid, method: http.MethodGet, target: "/api/facts", wantStatus: http.StatusInternalServerError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runConsoleRouteCase(t, tc)
		})
	}
}

const (
	schedulesUpstreamBody = `{"status":"ok","summary":{"total_active":3,"cron_count":2,"one_shot_count":1,"total_runs_24h":15,"success_rate_24h":93.3},` +
		`"crons":[{"id":"cron-1","channel_id":"chan-1","title_prefix":"Morning Brief","cron_expr":"0 9 * * *","prompt":"Generate morning brief","enabled":true,"agent":"aerial"},` +
		`{"id":"cron-2","channel_id":"chan-2","title_prefix":"Alpha Brief","cron_expr":"0 10 * * *","prompt":"Generate alpha brief","enabled":true,"agent":"agent-alpha"}],` +
		`"one_shots":[{"id":"oneshot-1","thread_id":"thread-1","prompt":"Remind about tea","run_at":"2026-01-02T03:04:05Z","agent":"aerial"}]}`
	scheduleRunsUpstreamFormat = `{"status":"ok","total":42,"limit":10,"offset":5,"runs":[` +
		`{"id":"cron-brief-101","schedule_id":"cron-1","schedule_type":"cron","prompt":%[1]q,"status":"success","started_at":"2026-01-02T03:04:05Z","duration_ms":1250,"agent":"aerial"},` +
		`{"id":"cron-brief-102","schedule_id":"cron-2","schedule_type":"cron","prompt":%[1]q,"status":"success","started_at":"2026-01-02T03:04:05Z","duration_ms":1100,"agent":"agent-alpha"}]}`
)

// newSchedulesUpstream fakes haro's /schedules and /schedules/runs. A base-path segment picks
// the error and nil-slice branches; runs echo the forwarded query as their prompt.
func newSchedulesUpstream(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasPrefix(r.URL.Path, "/error500/"):
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"db failure"}`))
		case strings.HasPrefix(r.URL.Path, "/badjson/"):
			_, _ = w.Write([]byte(`not-valid-json`))
		case r.URL.Path == "/nil/schedules":
			_, _ = w.Write([]byte(`{"status":"ok","crons":null,"one_shots":null}`))
		case r.URL.Path == "/nil/schedules/runs":
			_, _ = w.Write([]byte(`{"status":"ok","total":0,"limit":10,"offset":5,"runs":null}`))
		case r.URL.Path == "/schedules/runs":
			resp := fmt.Sprintf(scheduleRunsUpstreamFormat, "query:"+r.URL.RawQuery)
			_, _ = io.WriteString(w, resp)
		case r.URL.Path == "/schedules":
			_, _ = w.Write([]byte(schedulesUpstreamBody))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestSchedulesAndRunsHandlers(t *testing.T) {
	t.Parallel()
	mockURL := newSchedulesUpstream(t).URL
	offlineURL := downServerURL(t)
	haro := ConsoleConfig{HaroURL: mockURL}
	haroNil := ConsoleConfig{HaroURL: mockURL + "/nil"}
	haro500 := ConsoleConfig{HaroURL: mockURL + "/error500"}
	haroBadJSON := ConsoleConfig{HaroURL: mockURL + "/badjson"}
	offline := ConsoleConfig{HaroURL: offlineURL}
	invalid := ConsoleConfig{HaroURL: "http://brain:\x7f8080"}
	haroOverBrain := ConsoleConfig{BrainURL: offlineURL, HaroURL: mockURL}
	brainFallback := ConsoleConfig{BrainURL: mockURL}
	const runsQuery = "limit=10&offset=5&schedule_id=cron-1&status=success"
	const runsEcho = `"prompt":"query:limit=10\u0026offset=5\u0026schedule_id=cron-1\u0026status=success"`

	tests := []consoleRouteCase{
		{name: "Schedules_Success", cfg: haro, method: http.MethodGet, target: "/api/schedules", wantStatus: http.StatusOK, wantContains: []string{"Morning Brief", "Remind about tea", `"total_active":3`}},
		{name: "Schedules_AgentFilter_Aerial", cfg: haro, method: http.MethodGet, target: "/api/schedules?agent=aerial", wantStatus: http.StatusOK, wantContains: []string{"Morning Brief", "Remind about tea", `"total_active":2`}},
		{name: "Schedules_AgentFilter_Other", cfg: haro, method: http.MethodGet, target: "/api/schedules?agent=agent-alpha", wantStatus: http.StatusOK, wantContains: []string{"Alpha Brief", `"total_active":1`}},
		{name: "Schedules_SuccessParams_NilSlices", cfg: haroNil, method: http.MethodGet, target: "/api/schedules?limit=5&offset=2&active=true&q=weather", wantStatus: http.StatusOK, wantContains: []string{`"crons":[]`, `"one_shots":[]`}},
		{name: "Schedules_ActiveFalse", cfg: haro, method: http.MethodGet, target: "/api/schedules?active=false", wantStatus: http.StatusOK},
		{name: "Schedules_DegradedFallback", cfg: offline, method: http.MethodGet, target: "/api/schedules", wantStatus: http.StatusServiceUnavailable, wantContains: []string{`"status":"degraded"`, "Brain service unreachable. Retrying...", `"crons":[]`, `"one_shots":[]`}},
		{name: "Schedules_Upstream500", cfg: haro500, method: http.MethodGet, target: "/api/schedules", wantStatus: http.StatusInternalServerError},
		{name: "Schedules_BadGateway", cfg: haroBadJSON, method: http.MethodGet, target: "/api/schedules", wantStatus: http.StatusBadGateway},
		{name: "Schedules_MethodNotAllowed", cfg: offline, method: http.MethodPost, target: "/api/schedules", wantStatus: http.StatusMethodNotAllowed},
		{name: "Schedules_InvalidUpstreamURL", cfg: invalid, method: http.MethodGet, target: "/api/schedules", wantStatus: http.StatusInternalServerError},
		{name: "Schedules_ConsolePrefix_HaroWinsOverBrain", cfg: haroOverBrain, method: http.MethodGet, target: "/console/api/schedules", wantStatus: http.StatusOK, wantContains: []string{"Morning Brief", `"total_active":3`}},
		{name: "Schedules_FallbackToBrainWhenHaroEmpty", cfg: brainFallback, method: http.MethodGet, target: "/api/schedules", wantStatus: http.StatusOK, wantContains: []string{"Morning Brief", `"total_active":3`}},
		{name: "ScheduleRuns_Success", cfg: haro, method: http.MethodGet, target: "/api/schedules/runs?" + runsQuery, wantStatus: http.StatusOK, wantContains: []string{"cron-brief-101", `"total":42`, `"duration_ms":1250`, runsEcho}},
		{name: "ScheduleRuns_AgentFilter_Aerial", cfg: haro, method: http.MethodGet, target: "/api/schedules/runs?" + runsQuery + "&agent=aerial", wantStatus: http.StatusOK, wantContains: []string{"cron-brief-101", `"total":42`}, wantMissing: []string{"cron-brief-102"}},
		{name: "ScheduleRuns_AgentFilter_Other", cfg: haro, method: http.MethodGet, target: "/api/schedules/runs?" + runsQuery + "&agent=agent-alpha", wantStatus: http.StatusOK, wantContains: []string{"cron-brief-102", `"total":42`}, wantMissing: []string{"cron-brief-101"}},
		{name: "ScheduleRuns_SuccessParams_NilSlices", cfg: haroNil, method: http.MethodGet, target: "/api/schedules/runs?limit=10&offset=5&status=SUCCESS&schedule_id=42", wantStatus: http.StatusOK, wantBody: `{"status":"ok","total":0,"limit":10,"offset":5,"runs":[]}`},
		{name: "ScheduleRuns_DegradedFallback", cfg: offline, method: http.MethodGet, target: "/api/schedules/runs?limit=20&offset=10", wantStatus: http.StatusServiceUnavailable, wantContains: []string{`"status":"degraded"`, "Brain service unreachable. Retrying...", `"runs":[]`}},
		{name: "ScheduleRuns_Upstream500", cfg: haro500, method: http.MethodGet, target: "/api/schedules/runs", wantStatus: http.StatusInternalServerError},
		{name: "ScheduleRuns_BadGateway", cfg: haroBadJSON, method: http.MethodGet, target: "/api/schedules/runs", wantStatus: http.StatusBadGateway},
		{name: "ScheduleRuns_MethodNotAllowed", cfg: offline, method: http.MethodDelete, target: "/api/schedules/runs", wantStatus: http.StatusMethodNotAllowed},
		{name: "ScheduleRuns_InvalidUpstreamURL", cfg: invalid, method: http.MethodGet, target: "/api/schedules/runs", wantStatus: http.StatusInternalServerError},
		{name: "ScheduleRuns_ConsolePrefix_HaroWinsOverBrain", cfg: haroOverBrain, method: http.MethodGet, target: "/console/api/schedules/runs?" + runsQuery, wantStatus: http.StatusOK, wantContains: []string{"cron-brief-101", `"total":42`, runsEcho}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runConsoleRouteCase(t, tc)
		})
	}
}

// handlersTestSAToken is the service-account token goleak_test.go's init writes to the temp
// token file the console's vanadis client reads.
const handlersTestSAToken = "test-sa-token"

// requireVanadisBearer mirrors vanadis authorizeBuildRead: every read needs the console
// ServiceAccount bearer token, so each success row also proves console sent it. The
// status=unauthorized query and build-401 detail force a 401 for the error-path rows.
func requireVanadisBearer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+handlersTestSAToken || r.URL.Query().Get("status") == "unauthorized" || r.URL.Path == "/builds/build-401" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"missing or invalid bearer token"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}

const (
	vanadisBuildsUpstreamBody = `{"status":"ok","total":100,"running_count":10,"queued_count":20,"completed_count":70,"builds":[` +
		`{"id":"build-101","tier":2,"title":"Vanadis Feature","shared_context":"drawer-only-shared-context","status":"running","queue_state":"running","agent":"aerial","chunks":[` +
		`{"id":"chunk-0","build_id":"build-101","chunk_index":0,"title":"Chunk 0","depends_on":null,"status":"completed"},` +
		`{"id":"chunk-1","build_id":"build-101","chunk_index":1,"title":"Chunk 1","instructions":"drawer-only-instructions","target_files":["main.go"],` +
		`"target_symbols":["Run"],"acceptance_criteria":["pass"],"depends_on":["chunk-0"],"status":"completed"}]},` +
		`{"id":"build-102","tier":2,"title":"Alpha Feature","shared_context":"drawer-only-shared-context","status":"running","queue_state":"running","agent":"agent-alpha","chunks":[` +
		`{"id":"chunk-alpha-0","build_id":"build-102","chunk_index":0,"title":"Chunk Alpha 0","depends_on":null,"status":"completed"}]}]}`
	vanadisBuildDetailUpstreamBody = `{"build":{"id":"build-101","title":"Vanadis Feature","shared_context":"drawer-only-shared-context","status":"running",` +
		`"chunks":[{"id":"chunk-build-0","title":"Chunk Build 0","depends_on":null,"status":"completed"}]},` +
		`"chunks":[{"id":"chunk-0","build_id":"build-101","chunk_index":0,"title":"Chunk 0","depends_on":null,"status":"completed"},` +
		`{"id":"chunk-1","build_id":"build-101","chunk_index":1,"title":"Chunk 1","instructions":"drawer-only-instructions","depends_on":["chunk-0"],"status":"completed"}],` +
		`"entry":{"id":"build-101","queue_state":"running","chunks":[{"id":"chunk-entry-0","title":"Chunk Entry 0","depends_on":null,"status":"completed"}]}}`
)

// newVanadisBuildsUpstream fakes vanadis /builds and /builds/{id} behind the bearer check;
// the status query and the build ID pick each upstream branch.
func newVanadisBuildsUpstream(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(requireVanadisBearer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path + "?" + r.URL.Query().Get("status") {
		case "/builds?error500", "/builds/build-500?":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"internal upstream error"}`))
		case "/builds?badjson", "/builds/build-badjson?":
			_, _ = w.Write([]byte(`{not valid json`))
		case "/builds/build-101?":
			_, _ = w.Write([]byte(vanadisBuildDetailUpstreamBody))
		case "/builds/test-build-1?":
			_, _ = w.Write([]byte(`{"status":"ok","chunks":[]}`))
		default:
			if r.URL.Path == "/builds" {
				_, _ = w.Write([]byte(vanadisBuildsUpstreamBody))
				return
			}
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"build not found"}`))
		}
	})))
	t.Cleanup(srv.Close)
	return srv
}

func TestVanadisBuildsHandlers(t *testing.T) {
	t.Parallel()
	mock := ConsoleConfig{VanadisURL: newVanadisBuildsUpstream(t).URL}
	offline := ConsoleConfig{VanadisURL: downServerURL(t)}
	invalid := ConsoleConfig{VanadisURL: "http://vanadis:\x7f8080"}

	tests := []consoleRouteCase{
		{name: "Builds_Success_WithQuery", cfg: mock, method: http.MethodGet, target: "/api/builds?limit=25&status=running&queue_state=running", wantStatus: http.StatusOK, wantContains: []string{"build-101", "Vanadis Feature", "chunk-1", "chunk-0", `"depends_on":[]`, `"depends_on":["chunk-0"]`}, wantMissing: []string{"drawer-only-shared-context", "drawer-only-instructions", "target_files", "target_symbols", "acceptance_criteria", `"depends_on":null`}},
		{name: "Builds_AgentFilter_Aerial", cfg: mock, method: http.MethodGet, target: "/api/builds?agent=aerial", wantStatus: http.StatusOK, wantContains: []string{"build-101", `"total":100`, `"running_count":10`}, wantMissing: []string{"build-102"}},
		{name: "Builds_AgentFilter_Other", cfg: mock, method: http.MethodGet, target: "/api/builds?agent=agent-alpha", wantStatus: http.StatusOK, wantContains: []string{"build-102", `"total":100`, `"running_count":10`}, wantMissing: []string{"build-101"}},
		{name: "Builds_ConsolePrefix", cfg: mock, method: http.MethodGet, target: "/console/api/builds", wantStatus: http.StatusOK, wantContains: []string{"build-101", "build-102"}},
		{name: "Builds_DegradedFallback", cfg: offline, method: http.MethodGet, target: "/api/builds", wantStatus: http.StatusServiceUnavailable, wantContains: []string{`"status":"degraded"`, "Vanadis service unreachable. Retrying...", `"builds":[]`}},
		{name: "Builds_MethodNotAllowed", cfg: mock, method: http.MethodPost, target: "/api/builds", wantStatus: http.StatusMethodNotAllowed},
		{name: "Builds_Upstream500", cfg: mock, method: http.MethodGet, target: "/api/builds?status=error500", wantStatus: http.StatusInternalServerError},
		{name: "Builds_Upstream401_ErrorPayload", cfg: mock, method: http.MethodGet, target: "/api/builds?status=unauthorized", wantStatus: http.StatusUnauthorized, wantBody: `{"status":"error","total":0,"running_count":0,"queued_count":0,"completed_count":0,"builds":[],"error":"Upstream vanadis error occurred"}`},
		{name: "Builds_BadGateway_InvalidJSON", cfg: mock, method: http.MethodGet, target: "/api/builds?status=badjson", wantStatus: http.StatusBadGateway},
		{name: "Builds_InvalidUpstreamURL", cfg: invalid, method: http.MethodGet, target: "/api/builds", wantStatus: http.StatusInternalServerError},
		{name: "BuildDetail_Success_APIPrefix", cfg: mock, method: http.MethodGet, target: "/api/builds/build-101", wantStatus: http.StatusOK, wantContains: []string{`"status":"ok"`, "build-101", "chunk-1", "drawer-only-shared-context", "drawer-only-instructions", "chunk-0", "chunk-build-0", "chunk-entry-0", `"depends_on":[]`, `"depends_on":["chunk-0"]`}, wantMissing: []string{`"depends_on":null`}},
		{name: "BuildDetail_Success_ConsolePrefix", cfg: mock, method: http.MethodGet, target: "/console/api/builds/build-101", wantStatus: http.StatusOK, wantContains: []string{"build-101"}},
		{name: "BuildDetail_UpstreamStatusKept", cfg: mock, method: http.MethodGet, target: "/api/builds/test-build-1", wantStatus: http.StatusOK, wantBody: `{"status":"ok","chunks":[]}`},
		{name: "BuildDetail_NotFound_404", cfg: mock, method: http.MethodGet, target: "/api/builds/nonexistent-build", wantStatus: http.StatusNotFound, wantBody: `{"status":"error","chunks":[],"error":"build not found"}`},
		{name: "BuildDetail_MethodNotAllowed", cfg: mock, method: http.MethodDelete, target: "/api/builds/build-101", wantStatus: http.StatusMethodNotAllowed},
		{name: "BuildDetail_DegradedFallback", cfg: offline, method: http.MethodGet, target: "/api/builds/build-101", wantStatus: http.StatusServiceUnavailable, wantContains: []string{`"status":"degraded"`, "Vanadis service unreachable. Retrying...", `"chunks":[]`}},
		{name: "BuildDetail_BadGateway_InvalidJSON", cfg: mock, method: http.MethodGet, target: "/api/builds/build-badjson", wantStatus: http.StatusBadGateway},
		{name: "BuildDetail_Upstream500", cfg: mock, method: http.MethodGet, target: "/api/builds/build-500", wantStatus: http.StatusInternalServerError},
		{name: "BuildDetail_Upstream401_ErrorPayload", cfg: mock, method: http.MethodGet, target: "/api/builds/build-401", wantStatus: http.StatusUnauthorized, wantBody: `{"status":"error","chunks":[],"error":"missing or invalid bearer token"}`},
		{name: "BuildDetail_EmptyID_BadRequest", cfg: mock, method: http.MethodGet, target: "/api/builds/", wantStatus: http.StatusBadRequest, wantBody: `{"status":"error","chunks":[],"error":"missing or empty build ID"}`},
		{name: "BuildDetail_PathTraversal_BadRequest", cfg: mock, method: http.MethodGet, target: "/console/api/builds/a..b", wantStatus: http.StatusBadRequest, wantBody: `{"status":"error","chunks":[],"error":"invalid build ID \"a..b\": path traversal not allowed"}`},
		{name: "BuildDetail_InvalidUpstreamURL", cfg: invalid, method: http.MethodGet, target: "/api/builds/build-101", wantStatus: http.StatusInternalServerError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runConsoleRouteCase(t, tc)
		})
	}
}

type docketHandlerTestCase struct {
	name         string
	docketURL    string
	method       string
	target       string
	wantStatus   int
	wantBody     string
	wantContains []string
}

func TestDocketHandlers(t *testing.T) {
	t.Parallel()
	mockDocket := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		q := r.URL.Query()
		switch {
		case r.URL.Path == "/api/tasks" && q.Get("status") == "bogus":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"invalid status"}`))
		case r.URL.Path == "/api/tasks" && q.Get("status") == "badjson":
			_, _ = w.Write([]byte(`{"not":"an array"}`))
		case r.URL.Path == "/api/tasks" && q.Get("status") == "wont_do":
			_, _ = w.Write([]byte(`null`))
		case r.URL.Path == "/api/tasks":
			// Echo the forwarded query as the title so rows can assert what reached upstream.
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 1, "title": "query:" + r.URL.RawQuery, "status": "open", "parent_id": 9, "blocked_by": []int{2}}})
		case r.URL.Path == "/api/search" && q.Get("q") == "boom":
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"error":"search backend down"}`))
		case r.URL.Path == "/api/search" && q.Get("q") == "nothing":
			_, _ = w.Write([]byte(`null`))
		case r.URL.Path == "/api/search":
			_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 3, "title": "query:" + r.URL.RawQuery, "status": "open", "score": 0.5}})
		case r.URL.Path == "/api/tasks/7":
			_, _ = w.Write([]byte(`{"id":7,"title":"Root task","status":"open","description":"Body text","created_at":"2026-01-02T03:04:05Z","updated_at":"2026-01-03T03:04:05Z","parent":{"id":1,"title":"Epic","status":"open"},"subtasks":[{"id":8,"title":"Child","status":"completed"}],"blocked_by":[{"id":5,"title":"Blocker","status":"open"}],"blocks":[{"id":6,"title":"Blocked","status":"open"}],"related":[{"id":4,"title":"Sibling","status":"wont_do"}],"comments":[{"id":11,"author":"bot","comment":"First note","created_at":"2026-01-02T05:00:00Z"}]}`))
		case r.URL.Path == "/api/tasks/404":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"task 404 not found"}`))
		case r.URL.Path == "/api/tasks/405":
			w.WriteHeader(http.StatusNotFound)
		case r.URL.Path == "/api/tasks/500":
			w.WriteHeader(http.StatusBadGateway)
		case r.URL.Path == "/api/tasks/13":
			_, _ = w.Write([]byte(`{broken`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(mockDocket.Close)

	deadDocket := httptest.NewServer(http.NotFoundHandler())
	deadURL := deadDocket.URL
	deadDocket.Close()

	tests := []docketHandlerTestCase{
		{name: "Tasks_DefaultQuery", docketURL: mockDocket.URL, method: http.MethodGet, target: "/api/docket/tasks", wantStatus: http.StatusOK, wantBody: `{"status":"ok","tasks":[{"id":1,"title":"query:","status":"open","parent_id":9,"blocked_by":[2]}]}`},
		{name: "Tasks_ConsolePrefix_ForwardsOnlyStatus", docketURL: mockDocket.URL, method: http.MethodGet, target: "/console/api/docket/tasks?status=all&q=x&limit=3", wantStatus: http.StatusOK, wantBody: `{"status":"ok","tasks":[{"id":1,"title":"query:status=all","status":"open","parent_id":9,"blocked_by":[2]}]}`},
		{name: "Tasks_NullNormalizedToEmpty", docketURL: mockDocket.URL, method: http.MethodGet, target: "/api/docket/tasks?status=wont_do", wantStatus: http.StatusOK, wantBody: `{"status":"ok","tasks":[]}`},
		{name: "Tasks_UpstreamBadRequestPassedThrough", docketURL: mockDocket.URL, method: http.MethodGet, target: "/api/docket/tasks?status=bogus", wantStatus: http.StatusBadRequest, wantBody: `{"status":"error","tasks":[],"error":"Upstream docket error occurred"}`},
		{name: "Tasks_UndecodableUpstream", docketURL: mockDocket.URL, method: http.MethodGet, target: "/api/docket/tasks?status=badjson", wantStatus: http.StatusBadGateway, wantBody: `{"status":"error","tasks":[],"error":"Failed to decode upstream docket response"}`},
		{name: "Tasks_Unreachable", docketURL: deadURL, method: http.MethodGet, target: "/api/docket/tasks", wantStatus: http.StatusServiceUnavailable, wantBody: `{"status":"degraded","tasks":[],"error":"Docket service unreachable. Retrying..."}`},
		{name: "Tasks_NotConfigured", docketURL: "", method: http.MethodGet, target: "/api/docket/tasks", wantStatus: http.StatusServiceUnavailable, wantBody: `{"status":"degraded","tasks":[],"error":"docket not configured"}`},
		{name: "Tasks_InvalidBaseURL", docketURL: "http://docket:\x7f80", method: http.MethodGet, target: "/api/docket/tasks", wantStatus: http.StatusInternalServerError, wantContains: []string{"Invalid upstream URL configuration"}},
		{name: "Tasks_MethodNotAllowed", docketURL: mockDocket.URL, method: http.MethodPost, target: "/api/docket/tasks", wantStatus: http.StatusMethodNotAllowed, wantContains: []string{"Method Not Allowed"}},
		{name: "Search_ForwardsQueryStatusLimit", docketURL: mockDocket.URL, method: http.MethodGet, target: "/api/docket/search?q=auth+flow&status=all&limit=5&offset=2", wantStatus: http.StatusOK, wantBody: `{"status":"ok","results":[{"id":3,"title":"query:limit=5\u0026q=auth+flow\u0026status=all","status":"open","score":0.5}]}`},
		{name: "Search_ConsolePrefix_NullNormalizedToEmpty", docketURL: mockDocket.URL, method: http.MethodGet, target: "/console/api/docket/search?q=nothing", wantStatus: http.StatusOK, wantBody: `{"status":"ok","results":[]}`},
		{name: "Search_EmptyQuery", docketURL: mockDocket.URL, method: http.MethodGet, target: "/api/docket/search?q=+", wantStatus: http.StatusBadRequest, wantBody: `{"status":"error","results":[],"error":"invalid docket request: search query q is required"}`},
		{name: "Search_QueryTooLong", docketURL: mockDocket.URL, method: http.MethodGet, target: "/api/docket/search?q=" + strings.Repeat("z", 201), wantStatus: http.StatusBadRequest, wantBody: `{"status":"error","results":[],"error":"invalid docket request: search query q exceeds 200 characters"}`},
		{name: "Search_UpstreamErrorPassedThrough", docketURL: mockDocket.URL, method: http.MethodGet, target: "/api/docket/search?q=boom", wantStatus: http.StatusBadGateway, wantBody: `{"status":"error","results":[],"error":"Upstream docket error occurred"}`},
		{name: "Search_Unreachable", docketURL: deadURL, method: http.MethodGet, target: "/api/docket/search?q=x", wantStatus: http.StatusServiceUnavailable, wantBody: `{"status":"degraded","results":[],"error":"Docket service unreachable. Retrying..."}`},
		{name: "Search_NotConfigured", docketURL: " ", method: http.MethodGet, target: "/api/docket/search?q=x", wantStatus: http.StatusServiceUnavailable, wantBody: `{"status":"degraded","results":[],"error":"docket not configured"}`},
		{name: "Search_InvalidBaseURL", docketURL: "http://docket:\x7f80", method: http.MethodGet, target: "/api/docket/search?q=x", wantStatus: http.StatusInternalServerError, wantContains: []string{"Invalid upstream URL configuration"}},
		{name: "Search_MethodNotAllowed", docketURL: mockDocket.URL, method: http.MethodPut, target: "/api/docket/search?q=x", wantStatus: http.StatusMethodNotAllowed, wantContains: []string{"Method Not Allowed"}},
		{name: "Detail_HydratedTask", docketURL: mockDocket.URL, method: http.MethodGet, target: "/api/docket/tasks/7", wantStatus: http.StatusOK, wantBody: `{"status":"ok","task":{"id":7,"title":"Root task","status":"open","description":"Body text","created_at":"2026-01-02T03:04:05Z","updated_at":"2026-01-03T03:04:05Z","parent":{"id":1,"title":"Epic","status":"open"},"subtasks":[{"id":8,"title":"Child","status":"completed"}],"blocks":[{"id":6,"title":"Blocked","status":"open"}],"blocked_by":[{"id":5,"title":"Blocker","status":"open"}],"related":[{"id":4,"title":"Sibling","status":"wont_do"}],"comments":[{"id":11,"author":"bot","comment":"First note","created_at":"2026-01-02T05:00:00Z"}]}}`},
		{name: "Detail_ConsolePrefix", docketURL: mockDocket.URL, method: http.MethodGet, target: "/console/api/docket/tasks/7", wantStatus: http.StatusOK, wantContains: []string{`"status":"ok"`, `"title":"Root task"`}},
		{name: "Detail_UpstreamNotFoundTextPassedThrough", docketURL: mockDocket.URL, method: http.MethodGet, target: "/api/docket/tasks/404", wantStatus: http.StatusNotFound, wantBody: `{"status":"error","error":"task 404 not found"}`},
		{name: "Detail_UpstreamNotFoundWithoutBody", docketURL: mockDocket.URL, method: http.MethodGet, target: "/api/docket/tasks/405", wantStatus: http.StatusNotFound, wantBody: `{"status":"error","error":"Task not found"}`},
		{name: "Detail_UpstreamErrorWithoutBody", docketURL: mockDocket.URL, method: http.MethodGet, target: "/api/docket/tasks/500", wantStatus: http.StatusBadGateway, wantBody: `{"status":"error","error":"Upstream docket error occurred"}`},
		{name: "Detail_UndecodableUpstream", docketURL: mockDocket.URL, method: http.MethodGet, target: "/api/docket/tasks/13", wantStatus: http.StatusBadGateway, wantBody: `{"status":"error","error":"Failed to decode upstream docket response"}`},
		{name: "Detail_NonNumericID", docketURL: mockDocket.URL, method: http.MethodGet, target: "/api/docket/tasks/abc", wantStatus: http.StatusBadRequest, wantBody: `{"status":"error","error":"invalid docket request: task ID \"abc\" must be a positive integer"}`},
		{name: "Detail_EmptyID", docketURL: mockDocket.URL, method: http.MethodGet, target: "/console/api/docket/tasks/", wantStatus: http.StatusBadRequest, wantBody: `{"status":"error","error":"invalid docket request: task ID \"\" must be a positive integer"}`},
		{name: "Detail_Unreachable", docketURL: deadURL, method: http.MethodGet, target: "/api/docket/tasks/7", wantStatus: http.StatusServiceUnavailable, wantBody: `{"status":"degraded","error":"Docket service unreachable. Retrying..."}`},
		{name: "Detail_NotConfigured", docketURL: "", method: http.MethodGet, target: "/api/docket/tasks/abc", wantStatus: http.StatusServiceUnavailable, wantBody: `{"status":"degraded","error":"docket not configured"}`},
		{name: "Detail_InvalidBaseURL", docketURL: "http://docket:\x7f80", method: http.MethodGet, target: "/api/docket/tasks/7", wantStatus: http.StatusInternalServerError, wantContains: []string{"Invalid upstream URL configuration"}},
		{name: "Detail_MethodNotAllowed", docketURL: mockDocket.URL, method: http.MethodDelete, target: "/api/docket/tasks/7", wantStatus: http.StatusMethodNotAllowed, wantContains: []string{"Method Not Allowed"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDocketHandlerCase(t, tc)
		})
	}
}

func runDocketHandlerCase(t *testing.T, tc docketHandlerTestCase) {
	t.Helper()
	h := SetupConsoleMux(ConsoleConfig{DocketURL: tc.docketURL}, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(tc.method, tc.target, http.NoBody))
	if rr.Code != tc.wantStatus {
		t.Fatalf("status = %d, want %d (body: %s)", rr.Code, tc.wantStatus, rr.Body.String())
	}
	body := strings.TrimSpace(rr.Body.String())
	if tc.wantBody != "" && body != tc.wantBody {
		t.Errorf("body mismatch\n got: %s\nwant: %s", body, tc.wantBody)
	}
	for _, sub := range tc.wantContains {
		if !strings.Contains(body, sub) {
			t.Errorf("body %s missing %q", body, sub)
		}
	}
}

func TestVanadisHTTPClientAuthorization(t *testing.T) {
	t.Parallel()
	echo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(r.Header.Get("Authorization")))
	}))
	t.Cleanup(echo.Close)

	tests := []struct {
		name      string
		writeFile bool
		token     string
		wantAuth  string
		wantErr   bool
	}{
		{name: "ProjectedTokenSentAsBearer", writeFile: true, token: "sa-token-abc\n", wantAuth: "Bearer sa-token-abc"},
		{name: "MissingTokenFileSendsUnauthenticated", writeFile: false, wantAuth: ""},
		{name: "EmptyTokenFileFailsRequest", writeFile: true, token: "  \n", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tokenPath := filepath.Join(t.TempDir(), "token")
			if tc.writeFile {
				if err := os.WriteFile(tokenPath, []byte(tc.token), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			client := newVanadisHTTPClient(k8sauth.NewTokenSource(tokenPath, time.Now))
			defer client.CloseIdleConnections()

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, echo.URL, http.NoBody)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := client.Do(req)
			if tc.wantErr {
				if err == nil {
					resp.Body.Close()
					t.Fatalf("expected request error, got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("failed to read response body: %v", err)
			}
			if string(body) != tc.wantAuth {
				t.Fatalf("Authorization = %q, want %q", string(body), tc.wantAuth)
			}
		})
	}
}
