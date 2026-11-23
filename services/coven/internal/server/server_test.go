package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/k8sauthtest"
	"github.com/azylman/aerial/services/coven/internal/config"
	"github.com/azylman/aerial/services/coven/internal/store"
)

// serverRequestCase is the shared row shape for the Routes and handleMCP tests:
// one request against one handler, asserting status and an optional body substring.
type serverRequestCase struct {
	name       string
	method     string
	path       string
	headers    map[string]string
	body       io.Reader
	counterErr error
	sabotage   string
	wantStatus int
	wantSubstr string
}

// newServerFixture builds a Server over a real store seeded with one fact
// (id 1 "Server fact test", dim 0), applies sabotage, and restores the httpkit
// coverage-flush hooks when the test ends.
func newServerFixture(t *testing.T, sabotage string) *Server {
	t.Helper()
	oldCounters, oldMeta := httpkit.WriteCountersDirFn, httpkit.WriteMetaDirFn
	t.Cleanup(func() { httpkit.WriteCountersDirFn, httpkit.WriteMetaDirFn = oldCounters, oldMeta })

	db := newTestDB(t)
	s := store.NewSQLStore(db)
	if _, err := s.InsertFact(context.Background(), "Server fact test", testEmbedding(1.0)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	execSQL(t, db, sabotage)
	handler := NewToolHandler(&config.Config{}, s, &MockOllamaClient{embedding: testEmbedding(1.0)})
	server := NewServer(handler, func(_ context.Context, threadID string) error {
		if threadID == "th-err" {
			return errors.New("extract error")
		}
		return nil
	})
	return server
}

func assertServerRequest(t *testing.T, handler http.Handler, tc serverRequestCase) {
	t.Helper()
	httpkit.WriteCountersDirFn = func(string) error { return tc.counterErr }
	httpkit.WriteMetaDirFn = func(string) error { return tc.counterErr }

	req := httptest.NewRequest(tc.method, tc.path, tc.body)
	for k, v := range tc.headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != tc.wantStatus {
		t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
	}
	if tc.wantSubstr != "" && !strings.Contains(rec.Body.String(), tc.wantSubstr) {
		t.Errorf("response %q missing %q", rec.Body.String(), tc.wantSubstr)
	}
}

func TestServer_Routes(t *testing.T) {
	tests := []serverRequestCase{
		{name: "Health", method: http.MethodGet, path: "/health", wantStatus: http.StatusOK, wantSubstr: `{"status":"ok"}`},
		{name: "CoverageFlush", method: http.MethodGet, path: "/debug/coverage/flush", wantStatus: http.StatusOK},
		{name: "CoverageFlushError", method: http.MethodGet, path: "/debug/coverage/flush", counterErr: errors.New("flush err"), wantStatus: http.StatusInternalServerError},
		{name: "Metrics", method: http.MethodGet, path: "/metrics", wantStatus: http.StatusOK, wantSubstr: "coven_facts_total"},
		{name: "AdminExtractMethodNotAllowed", method: http.MethodGet, path: "/admin/extract", wantStatus: http.StatusMethodNotAllowed},
		{name: "AdminExtractBlankThreadID", method: http.MethodPost, path: "/admin/extract", wantStatus: http.StatusBadRequest},
		{name: "AdminExtractBlankJSONThreadID", method: http.MethodPost, path: "/admin/extract", body: strings.NewReader(`{"thread_id":"   "}`), wantStatus: http.StatusBadRequest},
		{name: "AdminExtractSuccessQuery", method: http.MethodPost, path: "/admin/extract?thread_id=th-1", wantStatus: http.StatusOK, wantSubstr: `{"status":"ok"}`},
		{name: "AdminExtractSuccessBody", method: http.MethodPost, path: "/admin/extract", body: strings.NewReader(`{"thread_id":"th-1"}`), wantStatus: http.StatusOK, wantSubstr: `{"status":"ok"}`},
		{name: "AdminExtractError", method: http.MethodPost, path: "/admin/extract?thread_id=th-err", wantStatus: http.StatusInternalServerError},
		{name: "FactsJSON", method: http.MethodGet, path: "/facts?limit=10&offset=0", wantStatus: http.StatusOK, wantSubstr: `"facts"`},
		{name: "FactsStoreError", method: http.MethodGet, path: "/facts", sabotage: sabotageFacts, wantStatus: http.StatusInternalServerError},
		{name: "FactsMethodNotAllowed", method: http.MethodPost, path: "/facts", wantStatus: http.StatusMethodNotAllowed},
		{name: "ContextJSON", method: http.MethodGet, path: "/context?q=test&limit=5", wantStatus: http.StatusOK, wantSubstr: `"context"`},
		{name: "ContextPlaintext", method: http.MethodGet, path: "/context?q=test&limit=5", headers: map[string]string{"Accept": "text/plain"}, wantStatus: http.StatusOK, wantSubstr: "<retrieved_memory>"},
		{name: "ContextMethodNotAllowed", method: http.MethodPost, path: "/context", wantStatus: http.StatusMethodNotAllowed},
		{name: "ConsolidateOK", method: http.MethodPost, path: "/consolidate", wantStatus: http.StatusOK, wantSubstr: `"consolidated":0`},
		{name: "ConsolidateStoreError", method: http.MethodPost, path: "/consolidate", sabotage: sabotageFacts, wantStatus: http.StatusInternalServerError},
		{name: "ConsolidateMethodNotAllowed", method: http.MethodGet, path: "/consolidate", wantStatus: http.StatusMethodNotAllowed},
		{name: "MCPRoute", method: http.MethodGet, path: "/mcp", wantStatus: http.StatusOK, wantSubstr: "Aerial Memory MCP Server"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertServerRequest(t, newServerFixture(t, tc.sabotage).Routes(), tc)
		})
	}
}

// TestServer_AdminExtractDefaultHandler runs the default extractFn (the real
// extraction pipeline) for a thread with no turns: nothing to extract, 200.
func TestServer_AdminExtractDefaultHandler(t *testing.T) {
	t.Parallel()
	handler := NewToolHandler(&config.Config{}, newTestStore(t), &MockOllamaClient{embedding: testEmbedding(1.0)})
	srv := NewServer(handler)
	req := httptest.NewRequest(http.MethodPost, "/admin/extract?thread_id=th-empty", http.NoBody)
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestServer_Routes_ExtractorRole(t *testing.T) {
	handler := NewToolHandler(&config.Config{Role: "extractor"}, newTestStore(t), &MockOllamaClient{embedding: testEmbedding(1.0)})
	server := NewServer(handler, func(_ context.Context, _ string) error {
		return nil
	})
	routes := server.Routes()

	tests := []serverRequestCase{
		{name: "ExtractorHealth", method: http.MethodGet, path: "/health", wantStatus: http.StatusOK, wantSubstr: `{"status":"ok"}`},
		{name: "ExtractorCoverageFlush", method: http.MethodGet, path: "/debug/coverage/flush", wantStatus: http.StatusOK},
		{name: "ExtractorMetrics", method: http.MethodGet, path: "/metrics", wantStatus: http.StatusOK, wantSubstr: "coven_facts_total"},
		{name: "ExtractorAdminExtract", method: http.MethodPost, path: "/admin/extract?thread_id=th-1", wantStatus: http.StatusOK, wantSubstr: `{"status":"ok"}`},
		{name: "ExtractorFactsBlocked", method: http.MethodGet, path: "/facts", wantStatus: http.StatusNotFound},
		{name: "ExtractorContextBlocked", method: http.MethodGet, path: "/context", wantStatus: http.StatusNotFound},
		{name: "ExtractorConsolidateBlocked", method: http.MethodPost, path: "/consolidate", wantStatus: http.StatusNotFound},
		{name: "ExtractorMCPBlocked", method: http.MethodGet, path: "/mcp", wantStatus: http.StatusNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertServerRequest(t, routes, tc)
		})
	}
}

// mcpToolCallBody renders a JSON-RPC tools/call request body.
func mcpToolCallBody(id int, name, args string) io.Reader {
	return strings.NewReader(fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"tools/call","params":{"name":%q,"arguments":%s}}`, id, name, args))
}

func TestServer_HandleMCP(t *testing.T) {
	tests := []serverRequestCase{
		{name: "MCPGetProbe", method: http.MethodGet, path: "/mcp", wantStatus: http.StatusOK, wantSubstr: "Aerial Memory MCP Server"},
		{name: "MCPMethodNotAllowed", method: http.MethodPut, path: "/mcp", wantStatus: http.StatusMethodNotAllowed},
		{name: "MCPEmptyBody", method: http.MethodPost, path: "/mcp", wantStatus: http.StatusBadRequest},
		{name: "MCPReadError", method: http.MethodPost, path: "/mcp", body: &mockErrReadCloser{}, wantStatus: http.StatusBadRequest},
		{name: "MCPDoublePrefixNormalized", method: http.MethodPost, path: "/mcp", body: mcpToolCallBody(1, "memory_memory_search", `{"query":"test"}`), wantStatus: http.StatusOK},
		{name: "MCPStoreToolCall", method: http.MethodPost, path: "/mcp", body: mcpToolCallBody(2, "memory_store", `{"fact_text":"tool call store"}`), wantStatus: http.StatusOK},
		{name: "MCPToolsListDocumentsAdmissionGate", method: http.MethodPost, path: "/mcp", body: strings.NewReader(`{"jsonrpc":"2.0","id":4,"method":"tools/list","params":{}}`), wantStatus: http.StatusOK, wantSubstr: "admission gate constraints"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertServerRequest(t, http.HandlerFunc(newServerFixture(t, tc.sabotage).handleMCP), tc)
		})
	}
}

func TestServer_PanicOnNilHandler(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected NewServer(nil) to panic")
		}
	}()
	NewServer(nil)
}

func TestFormatSkillCommitAndPRHelpers(t *testing.T) {
	c1 := SkillFileChange{
		RelPath:       "custom-skills/discord/ha/SKILL.md",
		CommitSummary: "update ha",
		Action:        "EDIT",
	}
	c2 := SkillFileChange{
		RelPath:       "custom-skills/common/deploy/SKILL.md",
		CommitSummary: "add deploy",
		Action:        "CREATE",
	}

	tests := []struct {
		name          string
		changes       []SkillFileChange
		targetID      string
		wantCommitSub string
		wantTitle     string
		wantBodySub   string
	}{
		{
			name:          "SingleChange",
			changes:       []SkillFileChange{c1},
			targetID:      "1562837549255495693",
			wantCommitSub: "feat(skills): update ha",
			wantTitle:     "feat(skills): update ha",
			wantBodySub:   `<!-- aerial-pr: {"target_id":"1562837549255495693","vanadis_chunk_id":""} -->`,
		},
		{
			name:          "MultipleChanges",
			changes:       []SkillFileChange{c1, c2},
			targetID:      "",
			wantCommitSub: "feat(skills): update 2 procedural skills",
			wantTitle:     "feat(skills): evolve 2 procedural skills",
			wantBodySub:   `<!-- aerial-pr: {"target_id":"","vanadis_chunk_id":""} -->`,
		},
		{
			name: "RequireReviewChangeClearsTargetIDAndRendersManualReviewBanner",
			changes: []SkillFileChange{{
				RelPath:       "custom-skills/discord/ha/SKILL.md",
				CommitSummary: "update ha",
				Action:        "EDIT",
				RequireReview: true,
				CandidateIDs:  []int64{101},
				Candidates: []store.ProceduralCandidate{{
					ID:          101,
					LoadedSkill: "self-improvement",
					Trigger:     "when running recursive grep",
					Steps:       []string{"pass --exclude-dir=.git"},
					Avoid:       []string{"do not run bare grep -r"},
				}},
			}},
			targetID:      "1562837549255495693",
			wantCommitSub: "feat(skills): update ha",
			wantTitle:     "feat(skills): update ha",
			wantBodySub:   `<!-- aerial-pr: {"target_id":"","vanadis_chunk_id":""} -->`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			commitMsg := formatSkillCommitMessage(tc.changes)
			if !strings.Contains(commitMsg, tc.wantCommitSub) {
				t.Errorf("commitMsg = %q, want containing %q", commitMsg, tc.wantCommitSub)
			}
			title := formatSkillPRTitle(tc.changes)
			if title != tc.wantTitle {
				t.Errorf("title = %q, want %q", title, tc.wantTitle)
			}
			body := formatSkillPRBody(tc.changes, tc.targetID)
			if !strings.Contains(body, tc.wantBodySub) {
				t.Errorf("body = %q, want containing %q", body, tc.wantBodySub)
			}
		})
	}

	longTrigger := strings.Repeat("x", 1800)
	alertChanges := []SkillFileChange{{
		RelPath:       "custom-skills/discord/ha/SKILL.md",
		CommitSummary: "update ha",
		Action:        "EDIT",
		RequireReview: true,
		CandidateIDs:  []int64{101},
		Candidates: []store.ProceduralCandidate{{
			ID:          101,
			LoadedSkill: "self-improvement",
			Trigger:     longTrigger,
			Steps:       []string{"step 1", "step 2"},
			Avoid:       []string{"avoid 1"},
		}},
	}}
	alertMsg := formatManualReviewPRAlert(42, "example-owner", "example-config", alertChanges)
	if len(alertMsg) > 1800 {
		t.Errorf("formatManualReviewPRAlert len = %d, want <= 1800", len(alertMsg))
	}
	if !strings.Contains(alertMsg, "PR [#42](https://github.com/example-owner/example-config/pull/42)") {
		t.Errorf("formatManualReviewPRAlert missing PR link: %q", alertMsg)
	}

	// Test parseDesignerResponse
	parseTests := []struct {
		name       string
		raw        string
		wantErr    bool
		wantAction string
	}{
		{name: "EmptyRaw", raw: "", wantErr: true},
		{name: "NoJSON", raw: "hello world", wantErr: true},
		{name: "MalformedJSON", raw: "{broken json", wantErr: true},
		{name: "ValidJSON", raw: `{"action":"EDIT","name":"foo"}`, wantErr: false, wantAction: "EDIT"},
		{name: "ValidJSONWithMarkdownPrefix", raw: "```json\n{\"action\":\"CREATE\",\"name\":\"bar\"}\n```", wantErr: false, wantAction: "CREATE"},
	}

	for _, tc := range parseTests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := parseDesignerResponse(tc.raw)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseDesignerResponse(%q) err = %v, wantErr = %v", tc.raw, err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if resp.Action != tc.wantAction {
				t.Errorf("Action = %q, want %q", resp.Action, tc.wantAction)
			}
		})
	}
}

func TestServer_Metrics_Table(t *testing.T) {
	s := newTestStore(t)
	for _, text := range []string{"Metric fact 1", "Metric fact 2"} {
		if _, err := s.InsertFact(context.Background(), text, testEmbedding(1.0)); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	handler := NewToolHandler(&config.Config{}, s, &MockOllamaClient{embedding: testEmbedding(1.0)})
	server := NewServer(handler)
	nilStoreServer := &Server{handler: &ToolHandler{}}

	registerCovenCollector(FactsTotal)

	RecordOperation("search", "success", 0.01)
	RecordOperation("search", "error", 0.02)
	RecordOperation("store", "success", 0.03)
	RecordOperation("store", "error", 0.04)
	RecordOperation("forget", "success", 0.05)
	RecordOperation("context", "success", 0.06)
	RecordOperation("extract", "success", 0.07)
	RecordOperation("consolidate", "success", 0.08)

	RecordFactStored("inserted")
	RecordFactStored("updated")
	RecordFactStored("reinforced")
	RecordFactStored("deleted")

	tests := []struct {
		name           string
		server         *Server
		method         string
		url            string
		wantStatus     int
		wantSubstrings []string
	}{
		{
			name:       "valid metrics with facts total and operations",
			server:     server,
			method:     http.MethodGet,
			url:        "/metrics",
			wantStatus: http.StatusOK,
			wantSubstrings: []string{
				"coven_facts_total 2",
				"coven_operations_total",
				`coven_operations_total{op="search",status="success"}`,
				`coven_operations_total{op="search",status="error"}`,
				`coven_operations_total{op="store",status="success"}`,
				`coven_operations_total{op="store",status="error"}`,
				`coven_operations_total{op="forget",status="success"}`,
				`coven_operations_total{op="context",status="success"}`,
				`coven_operations_total{op="extract",status="success"}`,
				`coven_operations_total{op="consolidate",status="success"}`,
				"coven_operation_duration_seconds",
				"coven_facts_stored_total",
				`coven_facts_stored_total{action="inserted"}`,
				`coven_facts_stored_total{action="updated"}`,
				`coven_facts_stored_total{action="reinforced"}`,
				`coven_facts_stored_total{action="deleted"}`,
			},
		},
		{
			name:       "nil store server serves metrics ok",
			server:     nilStoreServer,
			method:     http.MethodGet,
			url:        "/metrics",
			wantStatus: http.StatusOK,
			wantSubstrings: []string{
				"coven_facts_total",
				"coven_operations_total",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.url, http.NoBody)
			tc.server.handleMetrics(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("handleMetrics() status = %d, want %d", rec.Code, tc.wantStatus)
			}
			body := rec.Body.String()
			for _, sub := range tc.wantSubstrings {
				if !strings.Contains(body, sub) {
					t.Errorf("expected body to contain %q, body:\n%s", sub, body)
				}
			}
		})
	}
}

const serverAuthUser = "system:serviceaccount:default:eri-agent-alpha"

// serverAuthAllowed returns a fake API server that authenticates and authorizes every request.
func serverAuthAllowed() *k8sauthtest.RoundTripper {
	return &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReview(serverAuthUser), SARBody: k8sauthtest.SAR(true, "")}
}

// serverAuthDenied returns a fake API server that authenticates every request and denies it with reason.
func serverAuthDenied(reason string) *k8sauthtest.RoundTripper {
	return &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReview(serverAuthUser), SARBody: k8sauthtest.SAR(false, reason)}
}

func newServerFixtureWithAuth(t *testing.T, authCfg k8sauth.Config) *Server {
	t.Helper()
	oldCounters, oldMeta := httpkit.WriteCountersDirFn, httpkit.WriteMetaDirFn
	t.Cleanup(func() { httpkit.WriteCountersDirFn, httpkit.WriteMetaDirFn = oldCounters, oldMeta })

	s := newTestStore(t)
	if _, err := s.InsertFact(context.Background(), "Server fact test", testEmbedding(1.0)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	handler := NewToolHandler(&config.Config{Stack: config.DefaultStack, Auth: authCfg}, s, &MockOllamaClient{embedding: testEmbedding(1.0)})
	server := NewServer(handler, func(_ context.Context, threadID string) error {
		if threadID == "th-err" {
			return errors.New("extract failed")
		}
		return nil
	})
	return server
}

type authEndpointTestCase struct {
	name       string
	method     string
	path       string
	body       io.Reader
	header     map[string]string
	rt         *k8sauthtest.RoundTripper
	wantStatus int
	wantSubstr string
}

func runAuthEndpointTests(t *testing.T, trDefault *k8sauthtest.RoundTripper, tests []authEndpointTestCase) {
	t.Helper()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rt := trDefault
			if tc.rt != nil {
				rt = tc.rt
			}
			server := newServerFixtureWithAuth(t, k8sauthtest.Config(t, rt))
			req := httptest.NewRequest(tc.method, tc.path, tc.body)
			for k, v := range tc.header {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()
			server.Routes().ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantSubstr != "" && !strings.Contains(rec.Body.String(), tc.wantSubstr) {
				t.Errorf("response %q missing %q", rec.Body.String(), tc.wantSubstr)
			}
		})
	}
}

func TestServer_Auth_Endpoints_ExemptAndUnauthorized(t *testing.T) {
	trSuccess := serverAuthAllowed()
	trUnauth := &k8sauthtest.RoundTripper{AuthBody: `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":false,"error":"token expired"}}`}

	tests := []authEndpointTestCase{
		{
			name:       "health is exempt from auth",
			method:     http.MethodGet,
			path:       "/health",
			wantStatus: http.StatusOK,
		},
		{
			name:       "metrics is exempt from auth",
			method:     http.MethodGet,
			path:       "/metrics",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing token returns 401",
			method:     http.MethodGet,
			path:       "/facts",
			wantStatus: http.StatusUnauthorized,
			wantSubstr: "missing Authorization header",
		},
		{
			name:       "invalid token returns 401",
			method:     http.MethodGet,
			path:       "/facts",
			header:     map[string]string{"Authorization": "Bearer bad-token"},
			rt:         trUnauth,
			wantStatus: http.StatusUnauthorized,
			wantSubstr: "token expired",
		},
	}
	runAuthEndpointTests(t, trSuccess, tests)
}

func TestServer_Auth_Endpoints_AuthorizedAndForbidden(t *testing.T) {
	trSuccess := serverAuthAllowed()
	trDenied := serverAuthDenied("access denied by test policy")

	tests := []authEndpointTestCase{
		{
			name:       "facts allowed returns 200",
			method:     http.MethodGet,
			path:       "/facts",
			header:     map[string]string{"Authorization": "Bearer valid-token"},
			rt:         trSuccess,
			wantStatus: http.StatusOK,
			wantSubstr: `"facts"`,
		},
		{
			name:       "facts denied returns 403",
			method:     http.MethodGet,
			path:       "/facts",
			header:     map[string]string{"Authorization": "Bearer valid-token"},
			rt:         trDenied,
			wantStatus: http.StatusForbidden,
			wantSubstr: "access denied by test policy",
		},
		{
			name:       "context allowed returns 200",
			method:     http.MethodGet,
			path:       "/context?q=test",
			header:     map[string]string{"Authorization": "Bearer valid-token"},
			rt:         trSuccess,
			wantStatus: http.StatusOK,
		},
		{
			name:       "context denied returns 403",
			method:     http.MethodGet,
			path:       "/context?q=test",
			header:     map[string]string{"Authorization": "Bearer valid-token"},
			rt:         trDenied,
			wantStatus: http.StatusForbidden,
			wantSubstr: "access denied by test policy",
		},
		{
			name:       "admin extract allowed returns 200",
			method:     http.MethodPost,
			path:       "/admin/extract?thread_id=th-1",
			header:     map[string]string{"Authorization": "Bearer valid-token"},
			rt:         trSuccess,
			wantStatus: http.StatusOK,
			wantSubstr: `{"status":"ok"}`,
		},
		{
			name:       "admin extract denied returns 403",
			method:     http.MethodPost,
			path:       "/admin/extract?thread_id=th-1",
			header:     map[string]string{"Authorization": "Bearer valid-token"},
			rt:         trDenied,
			wantStatus: http.StatusForbidden,
			wantSubstr: "access denied by test policy",
		},
		{
			name:       "consolidate allowed returns 200",
			method:     http.MethodPost,
			path:       "/consolidate",
			header:     map[string]string{"Authorization": "Bearer valid-token"},
			rt:         trSuccess,
			wantStatus: http.StatusOK,
			wantSubstr: `"status":"ok"`,
		},
		{
			name:       "consolidate denied returns 403",
			method:     http.MethodPost,
			path:       "/consolidate",
			header:     map[string]string{"Authorization": "Bearer valid-token"},
			rt:         trDenied,
			wantStatus: http.StatusForbidden,
			wantSubstr: "access denied by test policy",
		},
	}
	runAuthEndpointTests(t, trSuccess, tests)
}

func TestServer_Auth_MCPTools(t *testing.T) {
	trSuccess := serverAuthAllowed()
	trDenied := serverAuthDenied("operation forbidden")

	tests := []struct {
		name       string
		toolName   string
		toolArgs   string
		rt         *k8sauthtest.RoundTripper
		wantSubstr string
	}{
		{
			name:       "memory search allowed returns results",
			toolName:   "memory_search",
			toolArgs:   `{"query":"test"}`,
			rt:         trSuccess,
			wantSubstr: `"facts"`,
		},
		{
			name:       "memory search denied returns error",
			toolName:   "memory_search",
			toolArgs:   `{"query":"test"}`,
			rt:         trDenied,
			wantSubstr: "operation forbidden",
		},
		{
			name:       "memory store allowed with target agent stamps source",
			toolName:   "memory_store",
			toolArgs:   `{"fact_text":"Durable test fact for agent-alpha","target_agent":"agent-alpha"}`,
			rt:         trSuccess,
			wantSubstr: `"status":"success"`,
		},
		{
			name:       "memory store denied returns error",
			toolName:   "memory_store",
			toolArgs:   `{"fact_text":"Durable test fact for agent-alpha","target_agent":"agent-alpha"}`,
			rt:         trDenied,
			wantSubstr: "operation forbidden",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := newServerFixtureWithAuth(t, k8sauthtest.Config(t, tc.rt))
			body := mcpToolCallBody(1, tc.toolName, tc.toolArgs)
			req := httptest.NewRequest(http.MethodPost, "/mcp", body)
			req.Header.Set("Authorization", "Bearer valid-token")
			rec := httptest.NewRecorder()
			server.Routes().ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
			}
			if tc.wantSubstr != "" && !strings.Contains(rec.Body.String(), tc.wantSubstr) {
				t.Errorf("response %q missing %q", rec.Body.String(), tc.wantSubstr)
			}
		})
	}
}
