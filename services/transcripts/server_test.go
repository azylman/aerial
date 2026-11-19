package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newSeededServer(t *testing.T) *Server {
	t.Helper()
	store := newTestStore(t, "aerial")
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	if _, err := store.ApplyBatch(context.Background(), testBatch("sess-a", epoch, 0, 2, false, false)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return NewServer(store)
}

func TestServerRoutes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		path       string
		wantStatus int
		wantFrag   string
	}{
		{"health ok", "/health", http.StatusOK, `"ok"`},
		{"search finds session steps", "/search?q=content", http.StatusOK, "sess-a"},
		{"steps range", "/steps?session=sess-a&start=0&end=2", http.StatusOK, "sess-a"},
		{"steps empty stack reads all", "/steps?session=sess-a", http.StatusOK, "sess-a"},
		{"stats all stacks", "/stats", http.StatusOK, "aerial"},
		{"stats scoped stack", "/stats?stack=aerial", http.StatusOK, "aerial"},
		{"watermarks all stacks", "/watermarks", http.StatusOK, "sess-a"},
		{"watermarks scoped stack", "/watermarks?stack=aerial", http.StatusOK, "sess-a"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newSeededServer(t)
			req := httptest.NewRequest(http.MethodGet, tc.path, http.NoBody)
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("GET %s status = %d, want %d", tc.path, rec.Code, tc.wantStatus)
			}
			if !strings.Contains(rec.Body.String(), tc.wantFrag) {
				t.Errorf("GET %s body = %s, want fragment %q", tc.path, rec.Body.String(), tc.wantFrag)
			}
		})
	}
}

func TestServerNilStore(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		path string
	}{
		{"health", "/health"},
		{"search", "/search?q=x"},
		{"steps", "/steps?session=s"},
		{"stats", "/stats"},
		{"watermarks", "/watermarks"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := NewServer(nil)
			req := httptest.NewRequest(http.MethodGet, tc.path, http.NoBody)
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)
			if rec.Code != http.StatusServiceUnavailable {
				t.Errorf("GET %s status = %d, want 503", tc.path, rec.Code)
			}
		})
	}
}

func TestServerSearchShape(t *testing.T) {
	t.Parallel()
	srv := newSeededServer(t)
	req := httptest.NewRequest(http.MethodGet, "/search?q=content", http.NoBody)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	var body struct {
		Sessions []SessionResult `json:"sessions"`
		Steps    []StepResult    `json:"steps"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode search body: %v", err)
	}
	if len(body.Steps) != 3 {
		t.Errorf("steps = %d, want 3", len(body.Steps))
	}
}

func TestServerDBError(t *testing.T) {
	t.Parallel()
	newClosedStore := func(t *testing.T) *Store {
		t.Helper()
		store := newTestStore(t, "aerial")
		if err := store.db.Close(); err != nil {
			t.Fatalf("close fixture db: %v", err)
		}
		return store
	}
	tests := []struct {
		name string
		path string
	}{
		{"search db closed", "/search?q=x"},
		{"steps db closed", "/steps?session=s"},
		{"stats db closed", "/stats"},
		{"watermarks db closed", "/watermarks"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := NewServer(newClosedStore(t))
			req := httptest.NewRequest(http.MethodGet, tc.path, http.NoBody)
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)
			if rec.Code != http.StatusServiceUnavailable && rec.Code != http.StatusInternalServerError {
				t.Errorf("GET %s status = %d, want 5xx", tc.path, rec.Code)
			}
		})
	}
}

func TestQueryInt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		url  string
		key  string
		def  int
		want int
	}{
		{"missing returns default", "/x", "limit", 20, 20},
		{"blank returns default", "/x?limit=", "limit", 20, 20},
		{"valid parses", "/x?limit=5", "limit", 20, 5},
		{"invalid returns default", "/x?limit=abc", "limit", 20, 20},
		{"negative returns default", "/x?limit=-3", "limit", 20, 20},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodGet, tc.url, http.NoBody)
			if got := queryInt(req, tc.key, tc.def); got != tc.want {
				t.Errorf("queryInt = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestServerNilDBStore(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{"health nil db", "/health", http.StatusServiceUnavailable},
		{"search nil db", "/search?q=x", http.StatusInternalServerError},
		{"steps nil db", "/steps?session=s", http.StatusInternalServerError},
		{"stats nil db", "/stats", http.StatusInternalServerError},
		{"watermarks nil db", "/watermarks", http.StatusInternalServerError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := NewServer(NewStore(nil, "aerial"))
			req := httptest.NewRequest(http.MethodGet, tc.path, http.NoBody)
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Errorf("GET %s status = %d, want %d", tc.path, rec.Code, tc.wantStatus)
			}
		})
	}
}

func TestServerSearchStepsFailure(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	ctx := context.Background()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	if _, err := store.ApplyBatch(ctx, testBatch("sess-a", epoch, 0, 1, false, false)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `DROP TABLE transcript_steps`); err != nil {
		t.Fatalf("drop steps: %v", err)
	}
	srv := NewServer(store)
	req := httptest.NewRequest(http.MethodGet, "/search?q=content", http.NoBody)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}

func TestWriteJSONEncodeError(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	writeJSON(rec, http.StatusOK, func() {})
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}
