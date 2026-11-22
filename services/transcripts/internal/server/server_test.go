package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/pgutil"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/transcriptwire"
	"github.com/azylman/aerial/services/transcripts/internal/config"
	"github.com/azylman/aerial/services/transcripts/internal/store"
)

func init() {
	httpkit.WriteCountersDirFn = func(string) error { return nil }
	httpkit.WriteMetaDirFn = func(string) error { return nil }
}

const transcriptsTestSchema = `CREATE TABLE session_summaries (
    agent TEXT NOT NULL,
    session_id TEXT NOT NULL,
    engine TEXT NOT NULL DEFAULT '',
    thread_id TEXT NOT NULL DEFAULT '',
    summary TEXT NOT NULL DEFAULT '',
    last_indexed_step INT NOT NULL DEFAULT -1,
    last_mtime DATETIME,
    summary_step_watermark INT NOT NULL DEFAULT -1,
    is_settled BOOLEAN NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (agent, session_id)
);
CREATE TABLE transcript_steps (
    agent TEXT NOT NULL,
    session_id TEXT NOT NULL,
    step_index INT NOT NULL,
    step_type TEXT NOT NULL DEFAULT '',
    tool_name TEXT NOT NULL DEFAULT '',
    tool_kind TEXT NOT NULL DEFAULT '',
    tool_call_id TEXT NOT NULL DEFAULT '',
    content TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (agent, session_id, step_index)
);`

var testStoreSeq atomic.Int64

func newTestStoreWithDB(t *testing.T, stack string) (*store.Store, *sql.DB) {
	t.Helper()
	dsn := fmt.Sprintf("file:transcripts-server-%d?mode=memory&cache=shared", testStoreSeq.Add(1))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open sqlite fixture: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(transcriptsTestSchema); err != nil {
		t.Fatalf("create fixture schema: %v", err)
	}
	return store.NewStore(db, stack), db
}

func newTestStore(t *testing.T, stack string) *store.Store {
	t.Helper()
	st, _ := newTestStoreWithDB(t, stack)
	return st
}

func testBatch(session string, mtime time.Time, first, last int, final, reset bool) transcriptwire.TranscriptStepBatch {
	steps := make([]transcriptwire.TranscriptStep, 0, last-first+1)
	for i := first; i <= last; i++ {
		steps = append(steps, transcriptwire.TranscriptStep{StepIndex: i, Type: "model_response", Content: "content"})
	}
	return transcriptwire.TranscriptStepBatch{
		Stack: "aerial", SessionID: session, ThreadID: "thread-1", Engine: "agy",
		FileMtime: mtime, BaseStep: first, FirstStep: first, LastStep: last,
		Final: final, Reset: reset, Steps: steps,
	}
}

func finalSeedBatch(session string, mtime time.Time) transcriptwire.TranscriptStepBatch {
	b := testBatch(session, mtime, 0, 4, false, false)
	b.Final = true
	return b
}

func newSeededServer(t *testing.T) *Server {
	t.Helper()
	st := newTestStore(t, "aerial")
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	if _, err := st.ApplyBatch(context.Background(), testBatch("sess-a", epoch, 0, 2, false, false)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return NewServer(st)
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

// degradedFixture selects how the store behind the server is broken.
type degradedFixture int

const (
	degradedNilStore degradedFixture = iota
	degradedNilDB
	degradedClosedDB
	degradedDroppedSteps
)

func newDegradedStore(t *testing.T, fixture degradedFixture) *store.Store {
	t.Helper()
	switch fixture {
	case degradedNilDB:
		return store.NewStore(nil, "aerial")
	case degradedClosedDB:
		st, db := newTestStoreWithDB(t, "aerial")
		if err := db.Close(); err != nil {
			t.Fatalf("close fixture db: %v", err)
		}
		return st
	case degradedDroppedSteps:
		st, db := newTestStoreWithDB(t, "aerial")
		ctx := context.Background()
		epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
		if _, err := st.ApplyBatch(ctx, testBatch("sess-a", epoch, 0, 1, false, false)); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if _, err := db.ExecContext(ctx, `DROP TABLE transcript_steps`); err != nil {
			t.Fatalf("drop steps: %v", err)
		}
		return st
	default:
		return nil
	}
}

func TestServerDegradedStore(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		fixture    degradedFixture
		path       string
		wantStatus int
	}{
		{"nil store health", degradedNilStore, "/health", http.StatusServiceUnavailable},
		{"nil store search", degradedNilStore, "/search?q=x", http.StatusServiceUnavailable},
		{"nil store steps", degradedNilStore, "/steps?session=s", http.StatusServiceUnavailable},
		{"nil store stats", degradedNilStore, "/stats", http.StatusServiceUnavailable},
		{"nil store watermarks", degradedNilStore, "/watermarks", http.StatusServiceUnavailable},
		{"nil store trajectory", degradedNilStore, "/transcripts/threads/th-1/trajectory", http.StatusServiceUnavailable},
		{"nil db health", degradedNilDB, "/health", http.StatusServiceUnavailable},
		{"nil db search", degradedNilDB, "/search?q=x", http.StatusInternalServerError},
		{"nil db steps", degradedNilDB, "/steps?session=s", http.StatusInternalServerError},
		{"nil db stats", degradedNilDB, "/stats", http.StatusInternalServerError},
		{"nil db watermarks", degradedNilDB, "/watermarks", http.StatusInternalServerError},
		{"closed db search", degradedClosedDB, "/search?q=x", http.StatusInternalServerError},
		{"closed db steps", degradedClosedDB, "/steps?session=s", http.StatusInternalServerError},
		{"closed db steps window", degradedClosedDB, "/steps?session=s&step=1", http.StatusInternalServerError},
		{"closed db stats", degradedClosedDB, "/stats", http.StatusInternalServerError},
		{"closed db watermarks", degradedClosedDB, "/watermarks", http.StatusInternalServerError},
		{"closed db single watermark", degradedClosedDB, "/watermark?session_id=s", http.StatusInternalServerError},
		{"closed db thread trajectory", degradedClosedDB, "/transcripts/threads/th-1/trajectory", http.StatusInternalServerError},
		{"dropped steps table search", degradedDroppedSteps, "/search?q=content", http.StatusInternalServerError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := NewServer(newDegradedStore(t, tc.fixture))
			req := httptest.NewRequest(http.MethodGet, tc.path, http.NoBody)
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Errorf("GET %s status = %d, want %d", tc.path, rec.Code, tc.wantStatus)
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

	st := newTestStore(t, "aerial")
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	if _, err := st.ApplyBatch(context.Background(), testBatch("sess-a", epoch, 0, 2, false, false)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	srv := NewServer(st, auth)

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
		{"readyz exempt reaches mux 404", "/readyz", "", false, http.StatusNotFound},
		{"metrics exempt reaches mux 404", "/metrics", "", false, http.StatusNotFound},
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

func TestNewBusIngester(t *testing.T) {
	t.Parallel()
	st := newTestStore(t, "aerial")
	bus := pubsub.NewMemoryBus()
	defer bus.Close()

	withBus := NewBusIngester(st, bus)
	if withBus.store != st {
		t.Errorf("NewBusIngester did not keep store")
	}
	if err := withBus.publish(context.Background(), "subject", "id", map[string]string{"k": "v"}); err != nil {
		t.Errorf("memory bus publish err = %v", err)
	}

	nilBus := NewBusIngester(st, nil)
	if err := nilBus.publish(context.Background(), "subject", "id", nil); err == nil {
		t.Errorf("expected error publishing through nil bus")
	}
}

// busKind selects the fake bus newBusFn returns in a table row.
type busKind int

const (
	busMemory busKind = iota
	busDialErr
	busEnsureFail
	busConsumeFail
	busCloseFail
)

var (
	errDialTest     = errors.New("dial failed")
	errEnsureTest   = errors.New("ensure down")
	errConsumeTest  = errors.New("subscribe down")
	errCloseTest    = errors.New("close down")
	errShutdownTest = errors.New("shutdown down")
)

type ensureFailBus struct{ pubsub.Bus }

func (ensureFailBus) EnsureCoreStreams(context.Context) error { return errEnsureTest }

type consumeFailBus struct{ pubsub.Bus }

func (consumeFailBus) ConsumeQueue(context.Context, string, string, pubsub.Handler) (pubsub.Subscription, error) {
	return nil, errConsumeTest
}

type closeFailBus struct{ pubsub.Bus }

func (closeFailBus) Close() error { return errCloseTest }

// stubNewBus swaps newBusFn for a factory returning the bus kind's fake.
func stubNewBus(t *testing.T, kind busKind) {
	t.Helper()
	old := newBusFn
	newBusFn = func(context.Context, string) (pubsub.Bus, error) {
		switch kind {
		case busDialErr:
			return nil, errDialTest
		case busEnsureFail:
			return ensureFailBus{Bus: pubsub.NewMemoryBus()}, nil
		case busConsumeFail:
			return consumeFailBus{Bus: pubsub.NewMemoryBus()}, nil
		case busCloseFail:
			return closeFailBus{Bus: pubsub.NewMemoryBus()}, nil
		default:
			return pubsub.NewMemoryBus(), nil
		}
	}
	t.Cleanup(func() { newBusFn = old })
}

func TestInitTranscriptsJetStream(t *testing.T) {
	tests := []struct {
		name        string
		natsURL     string
		nilIngester bool
		bus         busKind
		wantNil     bool
	}{
		{name: "empty url yields nil", natsURL: "", bus: busMemory, wantNil: true},
		{name: "nil ingester yields nil", natsURL: "nats://nats:4222", nilIngester: true, bus: busMemory, wantNil: true},
		{name: "dial error yields nil", natsURL: "nats://nats:4222", bus: busDialErr, wantNil: true},
		{name: "memory bus subscribes", natsURL: "nats://nats:4222", bus: busMemory},
		{name: "ensure streams failure still returns bus", natsURL: "nats://nats:4222", bus: busEnsureFail},
		{name: "subscribe failure still returns bus", natsURL: "nats://nats:4222", bus: busConsumeFail},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stubNewBus(t, tc.bus)
			ingester := NewIngester(newTestStore(t, "aerial"), nil)
			if tc.nilIngester {
				ingester = nil
			}
			bus := initTranscriptsJetStream(context.Background(), ingester, tc.natsURL)
			if (bus == nil) != tc.wantNil {
				t.Errorf("bus nil = %v, want nil = %v", bus == nil, tc.wantNil)
			}
			if bus != nil {
				_ = bus.Close()
			}
		})
	}
}

func stubOpenDB(t *testing.T, db *sql.DB, err error) func() {
	t.Helper()
	old := openDBFn
	openDBFn = func(context.Context, string, pgutil.SQLOptions) (*sql.DB, error) { return db, err }
	return func() { openDBFn = old }
}

// runAppConfig fills the fields every listening RunApp row shares.
func runAppConfig(c config.Config) *config.Config {
	c.Port, c.DatabaseURL, c.Stack = "0", "postgres://x", "aerial"
	return &c
}

func TestRunApp(t *testing.T) {
	tests := []struct {
		name        string
		cfg         *config.Config
		stubDB      bool
		openErr     error
		bus         busKind
		shutdownErr error
		canceled    bool
		wantErr     string
	}{
		{name: "nil config", wantErr: "config cannot be nil"},
		{name: "empty port", cfg: &config.Config{DatabaseURL: "postgres://x"}, wantErr: "port cannot be empty"},
		{name: "open db error", cfg: &config.Config{Port: "4017", DatabaseURL: "postgres://x"}, stubDB: true, openErr: errors.New("db down"), wantErr: "db down"},
		{name: "listen failure surfaces", cfg: &config.Config{Port: "99999", DatabaseURL: "postgres://x"}, stubDB: true, wantErr: "invalid port"},
		{name: "canceled context shuts down cleanly", cfg: runAppConfig(config.Config{}), stubDB: true, canceled: true},
		{name: "auth enabled shuts down cleanly", cfg: runAppConfig(config.Config{Auth: k8sauth.Config{Enabled: true}}), stubDB: true, canceled: true},
		{name: "bus close error is logged not returned", cfg: runAppConfig(config.Config{NATSURL: "nats://nats:4222"}), stubDB: true, bus: busCloseFail, canceled: true},
		{name: "shutdown error surfaces", cfg: runAppConfig(config.Config{}), stubDB: true, shutdownErr: errShutdownTest, canceled: true, wantErr: "shutdown down"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.stubDB {
				_, db := newTestStoreWithDB(t, "aerial")
				defer stubOpenDB(t, db, tc.openErr)()
			}
			stubNewBus(t, tc.bus)
			if tc.shutdownErr != nil {
				oldShutdown := httpShutdown
				httpShutdown = func(*http.Server, context.Context) error { return tc.shutdownErr }
				t.Cleanup(func() { httpShutdown = oldShutdown })
			}
			ctx := context.Background()
			if tc.canceled {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			err := RunApp(ctx, tc.cfg)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("RunApp() err = %v, want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Errorf("RunApp() err = %v", err)
			}
		})
	}
}

func TestNewBusFnDials(t *testing.T) {
	bus, err := newBusFn(context.Background(), "nats://127.0.0.1:1")
	if err != nil {
		t.Fatalf("newBusFn err = %v", err)
	}
	_ = bus.Close()
}

func TestInitJetStreamDeliversBatch(t *testing.T) {
	st := newTestStore(t, "aerial")
	ingester := NewIngester(st, nil)
	wrapped := &signalBus{Bus: pubsub.NewMemoryBus(), handled: make(chan struct{})}
	bus := initTranscriptsJetStreamWithFactory(context.Background(), ingester, "nats://nats:4222", func(context.Context, string) (pubsub.Bus, error) {
		return wrapped, nil
	})
	if bus == nil {
		t.Fatalf("expected non-nil bus")
	}
	defer bus.Close()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	raw := marshalBatch(t, testBatch("sess-live", epoch, 0, 0, false, false))
	if err := wrapped.Publish(context.Background(), pubsub.SubjectTranscriptsStepsPrefix+"sess-live", "live-1", raw); err != nil {
		t.Fatalf("publish: %v", err)
	}
	<-wrapped.handled
	marks, err := st.GetWatermarks(context.Background(), "aerial")
	if err != nil || len(marks) != 1 {
		t.Fatalf("watermarks = %+v, err = %v", marks, err)
	}
}

type signalBus struct {
	pubsub.Bus

	handled chan struct{}
	fired   atomic.Bool
}

func (b *signalBus) ConsumeQueue(ctx context.Context, subject, queue string, handler pubsub.Handler) (pubsub.Subscription, error) {
	return b.Bus.ConsumeQueue(ctx, subject, queue, func(msgCtx context.Context, msg pubsub.Message) error {
		defer func() {
			if b.fired.CompareAndSwap(false, true) {
				close(b.handled)
			}
		}()
		return handler(msgCtx, msg)
	})
}
