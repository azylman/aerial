package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
)

func init() {
	httpkit.WriteCountersDirFn = func(string) error { return nil }
	httpkit.WriteMetaDirFn = func(string) error { return nil }
}

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
		{"healthz ok", "/healthz", http.StatusOK, `"ok"`},
		{"debug coverage flush", "/debug/coverage/flush", http.StatusOK, ""},
		{"search finds session steps", "/search?q=content", http.StatusOK, "sess-a"},
		{"search non-matching agent empty result", "/search?agent=missing-agent&q=content", http.StatusOK, `"steps":[]`},
		{"search query alias", "/search?query=content", http.StatusOK, `"query":"content"`},
		{"transcripts search", "/transcripts/search?q=content", http.StatusOK, `"mode":"auto"`},
		{"api transcripts search", "/api/transcripts/search?q=content&mode=steps", http.StatusOK, `"mode":"steps"`},
		{"search mode sessions", "/search?q=content&mode=sessions", http.StatusOK, `"steps":[]`},
		{"steps range", "/steps?session=sess-a&start=0&end=2", http.StatusOK, "sess-a"},
		{"steps empty stack reads all", "/steps?session=sess-a", http.StatusOK, "sess-a"},
		{"steps window mode", "/steps?session=sess-a&step=1&context=1", http.StatusOK, `"context":1`},
		{"transcripts steps", "/transcripts/steps?session=sess-a&step=1", http.StatusOK, `"session_id":"sess-a"`},
		{"api transcripts steps", "/api/transcripts/steps?session=sess-a&step=1", http.StatusOK, `"step":1`},
		{"steps missing session", "/steps?step=1", http.StatusBadRequest, "session is required"},
		{"steps invalid step", "/steps?session=sess-a&step=bad", http.StatusBadRequest, "invalid step parameter"},
		{"steps negative step", "/steps?session=sess-a&step=-1", http.StatusBadRequest, "invalid step parameter"},
		{"steps invalid context", "/steps?session=sess-a&step=1&context=bad", http.StatusBadRequest, "invalid context parameter"},
		{"stats all stacks", "/stats", http.StatusOK, `"total_sessions":1`},
		{"stats scoped stack", "/stats?stack=aerial", http.StatusOK, `"status":"ok"`},
		{"transcripts stats", "/transcripts/stats", http.StatusOK, `"total_steps":3`},
		{"api transcripts stats", "/api/transcripts/stats", http.StatusOK, `"status":"ok"`},
		{"watermarks all stacks", "/watermarks", http.StatusOK, "sess-a"},
		{"watermarks scoped stack", "/watermarks?stack=aerial", http.StatusOK, "sess-a"},
		{"v1 watermarks", "/v1/watermarks", http.StatusOK, "sess-a"},
		{"single watermark present", "/watermark?session_id=sess-a", http.StatusOK, `"last_indexed_step":2`},
		{"single watermark missing", "/watermark?session_id=missing", http.StatusOK, `"last_indexed_step":-1`},
		{"single watermark session param", "/watermarks?session=sess-a", http.StatusOK, `"last_indexed_step":2`},
		{"thread trajectory missing suffix", "/transcripts/threads/th-1", http.StatusNotFound, "404 page not found"},
		{"thread trajectory missing thread id", "/transcripts/threads/trajectory", http.StatusBadRequest, "thread_id is required"},
		{"thread trajectory whitespace thread id", "/transcripts/threads/%20/trajectory", http.StatusBadRequest, "thread_id is required"},
		{"thread trajectory ok", "/transcripts/threads/th-test/trajectory", http.StatusOK, `"retrieved_facts":[]`},
		{"search invalid since ignored", "/search?q=content&since=bad", http.StatusOK, `"total":`},
		{"steps context negative clamped", "/steps?session=sess-a&step=1&context=-5", http.StatusOK, `"context":0`},
		{"steps context over 20 clamped", "/steps?session=sess-a&step=1&context=50", http.StatusOK, `"context":20`},
		{"steps empty step param", "/steps?session=sess-a&step=", http.StatusBadRequest, "step is required"},
		{"search since rfc3339nano", "/search?q=content&since=2026-11-18T12:00:00.123456789Z", http.StatusOK, `"total":`},
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
		{"trajectory", "/transcripts/threads/th-1/trajectory"},
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
		{"steps window db closed", "/steps?session=s&step=1"},
		{"stats db closed", "/stats"},
		{"watermarks db closed", "/watermarks"},
		{"single watermark db closed", "/watermark?session_id=s"},
		{"thread trajectory db closed", "/transcripts/threads/th-1/trajectory"},
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

func newK8sAuthMock(t *testing.T) *httptest.Server {
	t.Helper()
	k8sMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "tokenreviews") {
			var tr struct {
				Spec struct {
					Token string `json:"token"`
				} `json:"spec"`
			}
			_ = json.NewDecoder(r.Body).Decode(&tr)
			switch tr.Spec.Token {
			case "valid-token":
				_, _ = w.Write([]byte(`{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`))
			case "scoped-token":
				_, _ = w.Write([]byte(`{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-agent-alpha"}}}`))
			case "denied-token":
				_, _ = w.Write([]byte(`{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-denied"}}}`))
			case "error-token":
				_, _ = w.Write([]byte(`{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-error"}}}`))
			default:
				_, _ = w.Write([]byte(`{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":false}}`))
			}
			return
		}
		if strings.Contains(r.URL.Path, "subjectaccessreviews") {
			var sar struct {
				Spec struct {
					User               string `json:"user"`
					ResourceAttributes struct {
						Name string `json:"name"`
					} `json:"resourceAttributes"`
				} `json:"spec"`
			}
			_ = json.NewDecoder(r.Body).Decode(&sar)
			switch sar.Spec.User {
			case "system:serviceaccount:default:eri-error":
				http.Error(w, "internal sar error", http.StatusInternalServerError)
			case "system:serviceaccount:default:eri-aerial":
				_, _ = w.Write([]byte(`{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":true}}`))
			case "system:serviceaccount:default:eri-agent-alpha":
				if sar.Spec.ResourceAttributes.Name == "agent-alpha" {
					_, _ = w.Write([]byte(`{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":true}}`))
				} else {
					_, _ = w.Write([]byte(`{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":false,"reason":"scoped to agent-alpha"}}`))
				}
			default:
				_, _ = w.Write([]byte(`{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":false,"reason":"access denied by test"}}`))
			}
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(k8sMock.Close)
	return k8sMock
}

func TestServerAuth_RBAC(t *testing.T) {
	t.Parallel()

	tokenFile := t.TempDir() + "/token"
	_ = os.WriteFile(tokenFile, []byte("host-token"), 0o600)

	k8sMock := newK8sAuthMock(t)

	auth := k8sauth.NewAuthenticator(k8sauth.Config{
		Enabled:      true,
		TokenPath:    tokenFile,
		APIServerURL: k8sMock.URL,
		HTTPClient:   k8sMock.Client(),
	})

	store := newTestStore(t, "aerial")
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	if _, err := store.ApplyBatch(context.Background(), testBatch("sess-a", epoch, 0, 2, false, false)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	srv := NewServer(store, auth)

	tests := []struct {
		name       string
		path       string
		authHeader string
		directMux  bool
		wantStatus int
	}{
		{"health exempt without auth", "/health", "", false, http.StatusOK},
		{"healthz exempt without auth", "/healthz", "", false, http.StatusOK},
		{"debug coverage flush exempt without auth", "/debug/coverage/flush", "", false, http.StatusOK},
		{"search missing auth 401", "/search?q=content", "", false, http.StatusUnauthorized},
		{"steps missing auth 401", "/steps?session=sess-a", "", false, http.StatusUnauthorized},
		{"stats missing auth 401", "/stats", "", false, http.StatusUnauthorized},
		{"watermarks missing auth 401", "/watermarks", "", false, http.StatusUnauthorized},
		{"trajectory missing auth 401", "/transcripts/threads/th-1/trajectory", "", false, http.StatusUnauthorized},
		{"search invalid bearer 401", "/search?q=content", "Bearer bad-token", false, http.StatusUnauthorized},
		{"search denied SAR 403", "/search?q=content", "Bearer denied-token", false, http.StatusForbidden},
		{"steps denied SAR 403", "/steps?session=sess-a", "Bearer denied-token", false, http.StatusForbidden},
		{"stats denied SAR 403", "/stats", "Bearer denied-token", false, http.StatusForbidden},
		{"watermark denied SAR 403", "/watermarks", "Bearer denied-token", false, http.StatusForbidden},
		{"trajectory denied SAR 403", "/transcripts/threads/th-1/trajectory", "Bearer denied-token", false, http.StatusForbidden},
		{"search sar error 500", "/search?q=content", "Bearer error-token", false, http.StatusInternalServerError},
		{"search explicit target sar error 500", "/search?agent=aerial&q=content", "Bearer error-token", false, http.StatusInternalServerError},
		{"search invalid agent slug 400", "/search?agent=invalid_slug!&q=content", "Bearer valid-token", false, http.StatusBadRequest},
		{"search scoped agent fallback 200", "/search?q=content", "Bearer scoped-token", false, http.StatusOK},
		{"search scoped agent cross-target denied 403", "/search?agent=aerial&q=content", "Bearer scoped-token", false, http.StatusForbidden},
		{"direct mux missing caller 401", "/search?q=content", "", true, http.StatusUnauthorized},
		{"search allowed 200", "/search?q=content", "Bearer valid-token", false, http.StatusOK},
		{"steps allowed 200", "/steps?session=sess-a", "Bearer valid-token", false, http.StatusOK},
		{"stats allowed 200", "/stats", "Bearer valid-token", false, http.StatusOK},
		{"watermark allowed 200", "/watermark?session_id=sess-a", "Bearer valid-token", false, http.StatusOK},
		{"watermarks allowed 200", "/watermarks", "Bearer valid-token", false, http.StatusOK},
		{"trajectory allowed 200", "/transcripts/threads/th-1/trajectory", "Bearer valid-token", false, http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodGet, tc.path, http.NoBody)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			rec := httptest.NewRecorder()
			if tc.directMux {
				srv.mux.ServeHTTP(rec, req)
			} else {
				srv.Handler().ServeHTTP(rec, req)
			}
			if rec.Code != tc.wantStatus {
				t.Errorf("GET %s status = %d, want %d (body: %s)", tc.path, rec.Code, tc.wantStatus, rec.Body.String())
			}
		})
	}
}
