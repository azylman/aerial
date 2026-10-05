package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/transcript"
)

func TestParseCLIArgs(t *testing.T) {
	t.Run("EmptyArgs", func(t *testing.T) {
		_, err := parseCLIArgs([]string{})
		if err == nil {
			t.Fatal("expected error on empty args")
		}
	})

	t.Run("BareQuery", func(t *testing.T) {
		cfg, err := parseCLIArgs([]string{"docker compose"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Command != "search" || cfg.Query != "docker compose" || cfg.Mode != "auto" {
			t.Errorf("unexpected cfg: %+v", cfg)
		}
	})

	t.Run("SubcommandStats", func(t *testing.T) {
		cfg, err := parseCLIArgs([]string{"stats", "--json", "--url", "http://test:8080"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Command != "stats" || !cfg.JSON || cfg.BaseURL != "http://test:8080" {
			t.Errorf("unexpected cfg: %+v", cfg)
		}
	})

	t.Run("SubcommandSearchWithFlags", func(t *testing.T) {
		cfg, err := parseCLIArgs([]string{
			"search",
			"--query", "memory leak",
			"--mode", "steps",
			"--tool", "run_command",
			"--session", "sess-123",
			"--limit", "25",
			"--json",
			"--direct-db",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Command != "search" || cfg.Query != "memory leak" || cfg.Mode != "steps" ||
			cfg.Tool != "run_command" || cfg.Session != "sess-123" || cfg.Limit != 25 ||
			!cfg.JSON || !cfg.DirectDB {
			t.Errorf("unexpected cfg: %+v", cfg)
		}
	})

	t.Run("ShorthandFlagsAndPositional", func(t *testing.T) {
		cfg, err := parseCLIArgs([]string{
			"search",
			"-m", "sessions",
			"-t", "view_file",
			"-s", "sess-abc",
			"-n", "5",
			"OOM error",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Command != "search" || cfg.Query != "OOM error" || cfg.Mode != "sessions" ||
			cfg.Tool != "view_file" || cfg.Session != "sess-abc" || cfg.Limit != 5 {
			t.Errorf("unexpected cfg: %+v", cfg)
		}
	})

	t.Run("FlagPrefixWithoutSubcommand", func(t *testing.T) {
		cfg, err := parseCLIArgs([]string{"-q", "quick test"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Command != "search" || cfg.Query != "quick test" {
			t.Errorf("unexpected cfg: %+v", cfg)
		}
	})

	t.Run("EnvURLOverride", func(t *testing.T) {
		t.Setenv("AERIAL_BRAIN_URL", "http://env-host:9090/")
		cfg, err := parseCLIArgs([]string{"stats"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.BaseURL != "http://env-host:9090" {
			t.Errorf("expected BaseURL trimmed, got %s", cfg.BaseURL)
		}
	})

	t.Run("InvalidFlag", func(t *testing.T) {
		_, err := parseCLIArgs([]string{"search", "--invalid-nonexistent-flag"})
		if err == nil {
			t.Fatal("expected error on unknown flag")
		}
	})
}

func TestFormatSearchResults(t *testing.T) {
	t.Run("EmptyMatches", func(t *testing.T) {
		res := transcript.SearchResult{
			Query: "nothing",
			Mode:  "auto",
		}
		formatted := formatSearchResults(res)
		if !strings.Contains(formatted, "*(No matching records found)*") {
			t.Errorf("expected empty notice, got: %s", formatted)
		}
	})

	t.Run("SessionsAndSteps", func(t *testing.T) {
		now := time.Now()
		res := transcript.SearchResult{
			Query: "quota limit",
			Mode:  "auto",
			Sessions: []db.SessionSummary{
				{
					SessionID: "11112222-3333-4444-5555-666677778888",
					ThreadID:  "th-100",
					Summary:   "Triaged Gemini 429 quota exhaustion and tuned backoff.",
				},
				{
					SessionID: "short",
					Summary:   "Short ID test session",
				},
			},
			Steps: []db.TranscriptStep{
				{
					SessionID:  "11112222-3333-4444-5555-666677778888",
					StepIndex:  5,
					ToolName:   "run_command",
					Content:    "docker logs aerial-brain --tail 50\nline 2 of logs",
					CreatedAt:  now,
				},
				{
					SessionID:  "short",
					StepIndex:  12,
					StepType:   "PLANNER_RESPONSE",
					Content:    strings.Repeat("a", 400),
					CreatedAt:  now,
				},
			},
		}

		out := formatSearchResults(res)
		if !strings.Contains(out, "### 📄 Session Summaries (2 matches)") {
			t.Errorf("missing session summaries header: %s", out)
		}
		if !strings.Contains(out, "[11112222...](conversation://11112222-3333-4444-5555-666677778888)") {
			t.Errorf("missing conversation link: %s", out)
		}
		if !strings.Contains(out, "### 💬 Transcript Execution Steps (2 matches)") {
			t.Errorf("missing steps header: %s", out)
		}
		if !strings.Contains(out, "**[run_command]** Step #5") {
			t.Errorf("missing tool badge: %s", out)
		}
		if !strings.Contains(out, "... [truncated]") {
			t.Errorf("expected truncation for long content: %s", out)
		}
	})
}

func TestRunCLI_HTTPStats(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/transcripts/stats" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"total_sessions": 5032,
			"status":         "ok",
		})
	}))
	defer server.Close()

	t.Run("TextFormat", func(t *testing.T) {
		var buf bytes.Buffer
		err := runCLI(context.Background(), []string{"stats", "--url", server.URL}, &buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		out := buf.String()
		if !strings.Contains(out, "Total Indexed Sessions: 5032") {
			t.Errorf("unexpected output: %s", out)
		}
	})

	t.Run("JSONFormat", func(t *testing.T) {
		var buf bytes.Buffer
		err := runCLI(context.Background(), []string{"stats", "--json", "--url", server.URL}, &buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		var parsed map[string]any
		if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
			t.Fatalf("failed unmarshaling json output: %v", err)
		}
		if parsed["total_sessions"] != float64(5032) {
			t.Errorf("unexpected sessions count: %v", parsed["total_sessions"])
		}
	})
}

func TestRunCLI_HTTPSearch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/transcripts/search" {
			http.NotFound(w, r)
			return
		}
		q := r.URL.Query().Get("q")
		if q == "error" {
			http.Error(w, "internal database failure", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(transcript.SearchResult{
			Query: q,
			Mode:  r.URL.Query().Get("mode"),
			Sessions: []db.SessionSummary{
				{
					SessionID: "sess-abc",
					Summary:   "Found matching session",
				},
			},
			Total: 1,
		})
	}))
	defer server.Close()

	t.Run("EmptyQueryError", func(t *testing.T) {
		var buf bytes.Buffer
		err := runCLI(context.Background(), []string{"search", "--url", server.URL}, &buf)
		if err == nil || !strings.Contains(err.Error(), "search query cannot be empty") {
			t.Fatalf("expected empty query error, got %v", err)
		}
	})

	t.Run("SearchSuccessText", func(t *testing.T) {
		var buf bytes.Buffer
		err := runCLI(context.Background(), []string{"search", "docker", "--url", server.URL}, &buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		out := buf.String()
		if !strings.Contains(out, "Found matching session") {
			t.Errorf("unexpected output: %s", out)
		}
	})

	t.Run("SearchSuccessJSON", func(t *testing.T) {
		var buf bytes.Buffer
		err := runCLI(context.Background(), []string{"search", "docker", "--json", "--url", server.URL}, &buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		var parsed transcript.SearchResult
		if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
			t.Fatalf("failed unmarshaling search json: %v", err)
		}
		if len(parsed.Sessions) != 1 || parsed.Sessions[0].SessionID != "sess-abc" {
			t.Errorf("unexpected parsed result: %+v", parsed)
		}
	})

	t.Run("DirectDBFallbackErrorWhenNoPostgres", func(t *testing.T) {
		t.Setenv("POSTGRES_URL", "unsupported://localhost/invalid")
		var buf bytes.Buffer
		err := runCLI(context.Background(), []string{"search", "error", "--url", server.URL}, &buf)
		if err == nil {
			t.Fatal("expected error on HTTP error + invalid direct DB fallback")
		}
	})

	t.Run("DirectDBSearchSuccessAndFallback", func(t *testing.T) {
		oldFn := newDirectStoreFn
		defer func() { newDirectStoreFn = oldFn }()

		fake := db.NewFakeStore()
		_ = fake.UpsertSessionSummary(context.Background(), db.SessionSummary{
			SessionID: "sess-direct-1",
			Summary:   "Direct DB matched session",
		})
		newDirectStoreFn = func(pgURL string) (db.Store, func(), error) {
			return fake, func() {}, nil
		}

		// Direct DB flag explicit
		var buf bytes.Buffer
		err := runCLI(context.Background(), []string{"search", "Direct", "--direct-db"}, &buf)
		if err != nil {
			t.Fatalf("unexpected error on direct DB search: %v", err)
		}
		if !strings.Contains(buf.String(), "Direct DB matched session") {
			t.Errorf("unexpected output: %s", buf.String())
		}

		// Direct DB fallback when HTTP fails
		buf.Reset()
		err = runCLI(context.Background(), []string{"search", "Direct", "--url", "http://127.0.0.1:59999"}, &buf)
		if err != nil {
			t.Fatalf("unexpected error on direct DB fallback: %v", err)
		}
		if !strings.Contains(buf.String(), "Direct DB matched session") {
			t.Errorf("unexpected output: %s", buf.String())
		}
	})

	t.Run("HTTPToolAndSessionFilterParams", func(t *testing.T) {
		var buf bytes.Buffer
		err := runCLI(context.Background(), []string{"search", "docker", "-t", "run_command", "-s", "sess-filter", "--url", server.URL}, &buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestHTTPErrors(t *testing.T) {
	badJSONServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{invalid-json"))
	}))
	defer badJSONServer.Close()

	errServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer errServer.Close()

	t.Run("ExecuteSearchHTTP_InvalidURL", func(t *testing.T) {
		cfg := CLIConfig{BaseURL: "://bad-url", Query: "test"}
		_, err := executeSearchHTTP(context.Background(), cfg)
		if err == nil {
			t.Fatal("expected error on invalid URL")
		}
	})

	t.Run("ExecuteSearchHTTP_BadJSON", func(t *testing.T) {
		cfg := CLIConfig{BaseURL: badJSONServer.URL, Query: "test"}
		_, err := executeSearchHTTP(context.Background(), cfg)
		if err == nil {
			t.Fatal("expected error on bad JSON")
		}
	})

	t.Run("ExecuteSearchHTTP_Status500", func(t *testing.T) {
		cfg := CLIConfig{BaseURL: errServer.URL, Query: "test"}
		_, err := executeSearchHTTP(context.Background(), cfg)
		if err == nil {
			t.Fatal("expected error on 500 status")
		}
	})

	t.Run("ExecuteStatsHTTP_InvalidURL", func(t *testing.T) {
		cfg := CLIConfig{BaseURL: "://bad-url"}
		_, err := executeStatsHTTP(context.Background(), cfg)
		if err == nil {
			t.Fatal("expected error on invalid URL")
		}
	})

	t.Run("ExecuteStatsHTTP_BadJSON", func(t *testing.T) {
		cfg := CLIConfig{BaseURL: badJSONServer.URL}
		_, err := executeStatsHTTP(context.Background(), cfg)
		if err == nil {
			t.Fatal("expected error on bad JSON")
		}
	})

	t.Run("ExecuteStatsHTTP_Status500", func(t *testing.T) {
		cfg := CLIConfig{BaseURL: errServer.URL}
		_, err := executeStatsHTTP(context.Background(), cfg)
		if err == nil {
			t.Fatal("expected error on 500 status")
		}
	})

	t.Run("ExecuteSearchHTTP_NetworkError", func(t *testing.T) {
		cfg := CLIConfig{BaseURL: "http://127.0.0.1:59996", Query: "test"}
		_, err := executeSearchHTTP(context.Background(), cfg)
		if err == nil {
			t.Fatal("expected network error")
		}
	})

	t.Run("ExecuteStatsHTTP_NetworkError", func(t *testing.T) {
		cfg := CLIConfig{BaseURL: "http://127.0.0.1:59996"}
		_, err := executeStatsHTTP(context.Background(), cfg)
		if err == nil {
			t.Fatal("expected network error")
		}
	})

	t.Run("RunCLI_StatsError", func(t *testing.T) {
		var buf bytes.Buffer
		err := runCLI(context.Background(), []string{"stats", "--url", errServer.URL}, &buf)
		if err == nil {
			t.Fatal("expected error for stats on 500 status")
		}
	})
}

func TestRunMain(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		var out, errOut bytes.Buffer
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"total_sessions": 10, "status": "ok"})
		}))
		defer server.Close()

		code := runMain([]string{"stats", "--url", server.URL}, &out, &errOut)
		if code != 0 {
			t.Fatalf("expected exit code 0, got %d, err: %s", code, errOut.String())
		}
	})

	t.Run("Failure", func(t *testing.T) {
		var out, errOut bytes.Buffer
		code := runMain([]string{"search", "--url", "http://127.0.0.1:59998"}, &out, &errOut)
		if code != 1 {
			t.Fatalf("expected exit code 1, got %d", code)
		}
		if !strings.Contains(errOut.String(), "Error:") {
			t.Errorf("expected Error: in stderr, got: %s", errOut.String())
		}
	})
}

func TestDirectStoreAndRunCLIEdges(t *testing.T) {
	t.Run("DefaultNewDirectStoreFnError", func(t *testing.T) {
		_, _, err := newDirectStoreFn("invalid-scheme://localhost")
		if err == nil {
			t.Fatal("expected error from default newDirectStoreFn with invalid scheme")
		}
	})

	t.Run("RunCLI_DirectDBError", func(t *testing.T) {
		oldFn := newDirectStoreFn
		defer func() { newDirectStoreFn = oldFn }()

		newDirectStoreFn = func(pgURL string) (db.Store, func(), error) {
			return nil, nil, fmt.Errorf("simulated direct DB error")
		}

		var buf bytes.Buffer
		err := runCLI(context.Background(), []string{"search", "test", "--direct-db"}, &buf)
		if err == nil || !strings.Contains(err.Error(), "direct DB search failed") {
			t.Fatalf("expected direct DB error, got: %v", err)
		}
	})

	t.Run("RunCLI_ParseError", func(t *testing.T) {
		var buf bytes.Buffer
		err := runCLI(context.Background(), []string{"--invalid-flag"}, &buf)
		if err == nil {
			t.Fatal("expected error on invalid flag to runCLI")
		}
	})

	t.Run("RunCLI_WriterErrors", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if strings.Contains(r.URL.Path, "stats") {
				_ = json.NewEncoder(w).Encode(map[string]any{"total_sessions": 5, "status": "ok"})
			} else {
				_ = json.NewEncoder(w).Encode(transcript.SearchResult{Query: "test", Mode: "auto"})
			}
		}))
		defer server.Close()

		badWriter := &failingWriter{}

		// stats text writer error
		err := runCLI(context.Background(), []string{"stats", "--url", server.URL}, badWriter)
		if err == nil {
			t.Errorf("expected error on failing writer for stats text")
		}

		// stats json writer error
		err = runCLI(context.Background(), []string{"stats", "--json", "--url", server.URL}, badWriter)
		if err == nil {
			t.Errorf("expected error on failing writer for stats json")
		}

		// search text writer error
		err = runCLI(context.Background(), []string{"search", "test", "--url", server.URL}, badWriter)
		if err == nil {
			t.Errorf("expected error on failing writer for search text")
		}

		// search json writer error
		err = runCLI(context.Background(), []string{"search", "test", "--json", "--url", server.URL}, badWriter)
		if err == nil {
			t.Errorf("expected error on failing writer for search json")
		}
	})

	t.Run("ExecuteSearchDirectDB_DefaultEnv", func(t *testing.T) {
		oldFn := newDirectStoreFn
		defer func() { newDirectStoreFn = oldFn }()

		calledWithURL := ""
		closedCalled := false
		newDirectStoreFn = func(pgURL string) (db.Store, func(), error) {
			calledWithURL = pgURL
			fake := db.NewFakeStore()
			return fake, func() { closedCalled = true }, nil
		}

		t.Setenv("POSTGRES_URL", "")
		cfg := CLIConfig{
			Command:  "search",
			Query:    "hello",
			Mode:     "auto",
			DirectDB: true,
		}
		res, err := executeSearchDirectDB(context.Background(), cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !closedCalled {
			t.Errorf("expected closeFn to be called")
		}
		if !strings.Contains(calledWithURL, "aerial-postgres:5432") {
			t.Errorf("expected default postgres url, got %s", calledWithURL)
		}
		if res.Query != "hello" {
			t.Errorf("unexpected query in res: %+v", res)
		}
	})
}

type failingWriter struct{}

func (f *failingWriter) Write(p []byte) (n int, err error) {
	return 0, errors.New("simulated disk full write error")
}

func TestHTTPConnectionTruncatedBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "500")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("truncated"))
		if hijacker, ok := w.(http.Hijacker); ok {
			conn, _, _ := hijacker.Hijack()
			_ = conn.Close()
		}
	}))
	defer server.Close()

	t.Run("ExecuteSearchHTTP_TruncatedBody", func(t *testing.T) {
		cfg := CLIConfig{BaseURL: server.URL, Query: "test"}
		_, err := executeSearchHTTP(context.Background(), cfg)
		if err == nil {
			t.Fatal("expected error on truncated body in executeSearchHTTP")
		}
	})

	t.Run("ExecuteStatsHTTP_TruncatedBody", func(t *testing.T) {
		cfg := CLIConfig{BaseURL: server.URL}
		_, err := executeStatsHTTP(context.Background(), cfg)
		if err == nil {
			t.Fatal("expected error on truncated body in executeStatsHTTP")
		}
	})
}

func TestNewRequestWithContextErrors(t *testing.T) {
	oldFn := newRequestWithContext
	defer func() { newRequestWithContext = oldFn }()

	newRequestWithContext = func(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
		return nil, errors.New("simulated request creation failure")
	}

	t.Run("ExecuteSearchHTTP_NewRequestError", func(t *testing.T) {
		cfg := CLIConfig{BaseURL: "http://localhost:8080", Query: "test"}
		_, err := executeSearchHTTP(context.Background(), cfg)
		if err == nil || !strings.Contains(err.Error(), "simulated request creation failure") {
			t.Fatalf("expected simulated error, got %v", err)
		}
	})

	t.Run("ExecuteStatsHTTP_NewRequestError", func(t *testing.T) {
		cfg := CLIConfig{BaseURL: "http://localhost:8080"}
		_, err := executeStatsHTTP(context.Background(), cfg)
		if err == nil || !strings.Contains(err.Error(), "simulated request creation failure") {
			t.Fatalf("expected simulated error, got %v", err)
		}
	})
}

func TestJSONMarshalIndentErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "stats") {
			_ = json.NewEncoder(w).Encode(map[string]any{"total_sessions": 10, "status": "ok"})
		} else {
			_ = json.NewEncoder(w).Encode(transcript.SearchResult{Query: "test", Mode: "auto"})
		}
	}))
	defer server.Close()

	oldFn := jsonMarshalIndent
	defer func() { jsonMarshalIndent = oldFn }()

	jsonMarshalIndent = func(v any, prefix, indent string) ([]byte, error) {
		return nil, errors.New("simulated json marshal indent error")
	}

	t.Run("RunCLI_StatsJSONMarshalError", func(t *testing.T) {
		var buf bytes.Buffer
		err := runCLI(context.Background(), []string{"stats", "--json", "--url", server.URL}, &buf)
		if err == nil || !strings.Contains(err.Error(), "simulated json marshal indent error") {
			t.Fatalf("expected marshal error in stats json, got %v", err)
		}
	})

	t.Run("RunCLI_SearchJSONMarshalError", func(t *testing.T) {
		var buf bytes.Buffer
		err := runCLI(context.Background(), []string{"search", "test", "--json", "--url", server.URL}, &buf)
		if err == nil || !strings.Contains(err.Error(), "simulated json marshal indent error") {
			t.Fatalf("expected marshal error in search json, got %v", err)
		}
	})
}

func TestNewDirectStoreFnSuccess(t *testing.T) {
	oldInit := initDBFn
	defer func() { initDBFn = oldInit }()

	sqlDB, err := sql.Open("pgx", "postgres://user:pass@127.0.0.1:5432/aerial?sslmode=disable")
	if err != nil {
		t.Fatalf("failed creating test sql.DB: %v", err)
	}

	initDBFn = func(dsnOrCfg any) (*sql.DB, error) {
		return sqlDB, nil
	}

	store, closeFn, err := newDirectStoreFn("postgres://mock")
	if err != nil {
		t.Fatalf("unexpected error from newDirectStoreFn: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store")
	}
	if closeFn == nil {
		t.Fatal("expected non-nil closeFn")
	}
	closeFn()
}
