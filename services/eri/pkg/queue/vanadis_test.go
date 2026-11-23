package queue

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
)

type trackingBus struct {
	pubsub.Bus

	mu        sync.Mutex
	publishes []string
}

func (t *trackingBus) Publish(ctx context.Context, subject, msgID string, payload any) error {
	t.mu.Lock()
	t.publishes = append(t.publishes, subject)
	t.mu.Unlock()
	return t.Bus.Publish(ctx, subject, msgID, payload)
}

// deliveredOutcome is the observable projection of a dispatch outcome event
// received by a bus subscriber.
type deliveredOutcome struct {
	subject, dispatchID, chunkID, source, status string
}

// outcomeRecorder subscribes to dispatch outcome subjects and records every
// delivered event so tests can wait on deliveries without sleeping.
type outcomeRecorder struct {
	mu        sync.Mutex
	cond      *sync.Cond
	delivered []deliveredOutcome
}

func newOutcomeRecorder(t *testing.T, bus pubsub.Bus) *outcomeRecorder {
	t.Helper()
	r := &outcomeRecorder{}
	r.cond = sync.NewCond(&r.mu)
	_, err := bus.Subscribe(context.Background(), "aerial.dispatch.*", "test-sub", func(_ context.Context, msg pubsub.Message) error {
		var ev pubsub.DispatchOutcomeEvent
		if uErr := json.Unmarshal(msg.Data(), &ev); uErr == nil {
			r.mu.Lock()
			r.delivered = append(r.delivered, deliveredOutcome{
				subject: msg.Subject(), dispatchID: ev.DispatchID, chunkID: ev.VanadisChunkID, source: ev.Source, status: ev.Status,
			})
			r.cond.Broadcast()
			r.mu.Unlock()
		}
		return msg.Ack()
	})
	if err != nil {
		t.Fatalf("failed to subscribe to memory bus: %v", err)
	}
	return r
}

// waitFor blocks until at least n outcomes were delivered, then returns a snapshot.
func (r *outcomeRecorder) waitFor(n int) []deliveredOutcome {
	r.mu.Lock()
	defer r.mu.Unlock()
	for len(r.delivered) < n {
		r.cond.Wait()
	}
	return append([]deliveredOutcome(nil), r.delivered...)
}

// dispatchPublishes returns the dispatch-outcome subjects the bus was asked to publish.
func (t *trackingBus) dispatchPublishes() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var got []string
	for _, subject := range t.publishes {
		if strings.HasPrefix(subject, pubsub.SubjectAerialDispatchPrefix) {
			got = append(got, subject)
		}
	}
	return got
}

// outcomeDispatch builds a pending dispatch whose prompt matches its source.
func outcomeDispatch(id, source, chunkID string) db.Dispatch {
	prompt := "heartbeat prompt"
	if source == "vanadis" {
		prompt = "=== Active Chunk (Chunk 1 of 1) ===\nBuild ID: build-test\nChunk ID: " + chunkID
	}
	now := time.Now().UTC()
	return db.Dispatch{
		ID: id, Source: source, SessionID: source + "-" + id, TargetID: chunkID, VanadisChunkID: chunkID,
		Prompt: prompt, Status: db.StatusPending, CreatedAt: now, UpdatedAt: now,
	}
}

func assertAttemptedPublishes(t *testing.T, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("attempted dispatch publishes = %v, want %v", got, want)
	}
}

func assertDeliveredOutcomes(t *testing.T, got, want []deliveredOutcome) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("delivered outcomes = %+v, want %+v", got, want)
	}
}

func TestWorkerPool_PublishDispatchOutcome_JetStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		turnStatus    string
		turnError     string
		chunkID       string
		dispatchID    string
		source        string
		wantErr       bool
		wantPublishes []string
		wantDelivered []deliveredOutcome
	}{
		{
			name:          "jetstream publish success on completed status",
			turnStatus:    "success",
			chunkID:       "chunk-js-success",
			dispatchID:    "disp-js-success",
			source:        "vanadis",
			wantPublishes: []string{"aerial.dispatch.completed"},
			wantDelivered: []deliveredOutcome{{"aerial.dispatch.completed", "disp-js-success", "chunk-js-success", "vanadis", "completed"}},
		},
		{
			name:          "jetstream publish failed status routes to failed topic",
			turnStatus:    "failed",
			turnError:     "build error",
			chunkID:       "chunk-js-fail",
			dispatchID:    "disp-js-fail",
			source:        "vanadis",
			wantPublishes: []string{"aerial.dispatch.failed"},
			wantDelivered: []deliveredOutcome{{"aerial.dispatch.failed", "disp-js-fail", "chunk-js-fail", "vanadis", "failed"}},
		},
		{
			name:          "heartbeat dispatch outcome publishes to jetstream",
			turnStatus:    "success",
			dispatchID:    "disp-hb-success",
			source:        "heartbeat",
			wantPublishes: []string{"aerial.dispatch.completed"},
			wantDelivered: []deliveredOutcome{{"aerial.dispatch.completed", "disp-hb-success", "", "heartbeat", "completed"}},
		},
		{
			name:       "empty turn status does not publish",
			chunkID:    "chunk-empty-status",
			dispatchID: "disp-empty-status",
			source:     "vanadis",
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			memBus := pubsub.NewMemoryBus()
			defer memBus.Close()
			trackBus := &trackingBus{Bus: memBus}
			recorder := newOutcomeRecorder(t, memBus)

			pool := &WorkerPool{eventBus: trackBus, cfg: WorkerPoolConfig{EventBus: trackBus}}
			disp := outcomeDispatch(tc.dispatchID, tc.source, tc.chunkID)
			completedAt := time.Now().UTC()
			err := pool.publishDispatchOutcome(context.Background(), disp, tc.turnStatus, tc.turnError, completedAt.Add(-100*time.Millisecond), completedAt)
			if (err != nil) != tc.wantErr {
				t.Fatalf("publishDispatchOutcome error = %v, wantErr %v", err, tc.wantErr)
			}

			delivered := recorder.waitFor(len(tc.wantDelivered))
			assertAttemptedPublishes(t, trackBus.dispatchPublishes(), tc.wantPublishes)
			assertDeliveredOutcomes(t, delivered, tc.wantDelivered)
		})
	}
}

type flushActiveStepTestCase struct {
	name            string
	agentName       string
	buildID         string
	chunkID         string
	step            string
	serverStatus    int
	serverBody      string
	restateURL      string
	vanadisURL      string
	appVanadisURL   string
	expectErr       bool
	expectErrSubstr string
	wantURI         string
	wantActiveStep  string
	wantChunkID     string
	skipServer      bool
}

func runFlushActiveStepCase(t *testing.T, tc flushActiveStepTestCase) {
	t.Helper()

	var capturedReq updateActiveStepPayload
	var capturedURI string
	var capturedMu sync.Mutex

	var serverURL string
	var vanClient *http.Client
	if !tc.skipServer {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			capturedMu.Lock()
			capturedURI = r.URL.RequestURI()
			_ = json.NewDecoder(r.Body).Decode(&capturedReq)
			capturedMu.Unlock()

			w.WriteHeader(tc.serverStatus)
			_, _ = w.Write([]byte(tc.serverBody))
		}))
		defer srv.Close()
		serverURL = srv.URL
		vanClient = srv.Client()
	}

	restateIngress := tc.restateURL
	vanadisIngress := tc.vanadisURL
	switch {
	case !tc.skipServer && restateIngress == "" && vanadisIngress == "" && tc.appVanadisURL == "":
		restateIngress = serverURL
	case restateIngress == "USE_SERVER":
		restateIngress = serverURL
	case vanadisIngress == "USE_SERVER":
		vanadisIngress = serverURL
	}
	appVanadis := tc.appVanadisURL
	if appVanadis == "USE_SERVER" {
		appVanadis = serverURL + "/"
	}

	pool := &WorkerPool{
		cfg: WorkerPoolConfig{
			AgentName:         tc.agentName,
			RestateIngressURL: restateIngress,
			VanadisURL:        vanadisIngress,
			VanadisHTTPClient: vanClient,
		},
	}
	if appVanadis != "" {
		pool.appCfg = config.NewFromData(&config.ConfigData{VanadisURL: appVanadis})
	}

	err := pool.FlushChunkActiveStepToRestate(context.Background(), tc.buildID, tc.chunkID, tc.step)
	if tc.expectErr {
		if err == nil {
			t.Fatalf("expected error containing %q, got nil", tc.expectErrSubstr)
		}
		if !strings.Contains(err.Error(), tc.expectErrSubstr) {
			t.Fatalf("error %q does not contain %q", err.Error(), tc.expectErrSubstr)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tc.skipServer {
		return
	}

	capturedMu.Lock()
	gotURI, gotReq := capturedURI, capturedReq
	capturedMu.Unlock()

	if gotURI != tc.wantURI {
		t.Errorf("request URI = %q, want %q", gotURI, tc.wantURI)
	}
	if gotReq.ChunkID != tc.wantChunkID {
		t.Errorf("chunk_id = %q, want %q", gotReq.ChunkID, tc.wantChunkID)
	}
	if gotReq.ActiveStep != tc.wantActiveStep {
		t.Errorf("active_step = %q, want %q", gotReq.ActiveStep, tc.wantActiveStep)
	}
}

var flushActiveStepTestCases = []flushActiveStepTestCase{
	{
		name:           "successful step update default agent via restate fallback",
		agentName:      "",
		buildID:        "build-101",
		chunkID:        "chunk-live-1",
		step:           "Executing `git status`",
		serverStatus:   http.StatusOK,
		serverBody:     `{"status":"ok"}`,
		wantURI:        "/Build/build-101/UpdateActiveStep",
		wantChunkID:    "chunk-live-1",
		wantActiveStep: "Executing `git status`",
	},
	{
		name:           "successful step update via vanadis API",
		vanadisURL:     "USE_SERVER",
		buildID:        "build-vanadis-1",
		chunkID:        "chunk-live-v1",
		step:           "Running vanadis build step...",
		serverStatus:   http.StatusOK,
		serverBody:     `{"status":"ok"}`,
		wantURI:        "/builds/build-vanadis-1/active-step",
		wantChunkID:    "chunk-live-v1",
		wantActiveStep: "Running vanadis build step...",
	},
	{
		name:           "successful step update via vanadis API with special characters in buildID",
		vanadisURL:     "USE_SERVER",
		buildID:        "build/special:v2",
		chunkID:        "chunk-live-v2",
		step:           "Testing special build ID...",
		serverStatus:   http.StatusOK,
		serverBody:     `{"status":"ok"}`,
		wantURI:        "/builds/" + url.PathEscape("build/special:v2") + "/active-step",
		wantChunkID:    "chunk-live-v2",
		wantActiveStep: "Testing special build ID...",
	},
	{
		name:            "vanadis API 500 error fails loud",
		vanadisURL:      "USE_SERVER",
		buildID:         "build-err-vanadis",
		chunkID:         "chunk-err-v1",
		step:            "failing vanadis step",
		serverStatus:    http.StatusInternalServerError,
		serverBody:      `{"error":"internal vanadis failure"}`,
		expectErr:       true,
		expectErrSubstr: "vanadis returned status 500",
		wantURI:         "/builds/build-err-vanadis/active-step",
		wantChunkID:     "chunk-err-v1",
		wantActiveStep:  "failing vanadis step",
	},
	{
		name:            "vanadis API 404 error fails loud",
		vanadisURL:      "USE_SERVER",
		buildID:         "build-404-vanadis",
		chunkID:         "chunk-404-v1",
		step:            "not found step",
		serverStatus:    http.StatusNotFound,
		serverBody:      `{"error":"build not found"}`,
		expectErr:       true,
		expectErrSubstr: "vanadis returned status 404",
		wantURI:         "/builds/build-404-vanadis/active-step",
		wantChunkID:     "chunk-404-v1",
		wantActiveStep:  "not found step",
	},
	{
		name:           "successful step update custom agent via restate fallback",
		agentName:      "agent-alpha",
		buildID:        "build-202",
		chunkID:        "chunk-live-2",
		step:           "Compiling binaries...",
		serverStatus:   http.StatusOK,
		serverBody:     `{"status":"ok"}`,
		wantURI:        "/Build/build-202/UpdateActiveStep",
		wantChunkID:    "chunk-live-2",
		wantActiveStep: "Compiling binaries...",
	},
	{
		name:           "step text trimming and special characters in buildID via restate fallback",
		agentName:      "agent-beta",
		buildID:        "build/special:1",
		chunkID:        "chunk-trim",
		step:           "   Running tests...   \n",
		serverStatus:   http.StatusOK,
		serverBody:     `{"status":"ok"}`,
		wantURI:        "/Build/" + url.PathEscape("build/special:1") + "/UpdateActiveStep",
		wantChunkID:    "chunk-trim",
		wantActiveStep: "Running tests...",
	},
	{
		name:           "empty step clears active step",
		agentName:      "aerial",
		buildID:        "build-clear",
		chunkID:        "chunk-clear-1",
		step:           "",
		serverStatus:   http.StatusOK,
		serverBody:     `{"status":"ok"}`,
		wantURI:        "/Build/build-clear/UpdateActiveStep",
		wantChunkID:    "chunk-clear-1",
		wantActiveStep: "",
	},
	{
		name:       "empty chunkID is a no-op",
		restateURL: "http://restate.test:8080",
		buildID:    "build-noop",
		chunkID:    "",
		step:       "anything",
		skipServer: true,
	},
	{
		name:           "successful step update via vanadis API with empty buildID routes to chunks endpoint",
		vanadisURL:     "USE_SERVER",
		buildID:        "",
		chunkID:        "chunk-nobuild-1",
		step:           "Running custom chunk...",
		serverStatus:   http.StatusOK,
		serverBody:     `{"status":"ok"}`,
		wantURI:        "/chunks/active-step",
		wantChunkID:    "chunk-nobuild-1",
		wantActiveStep: "Running custom chunk...",
	},
	{
		name:           "app config vanadis_url routes to vanadis API",
		appVanadisURL:  "USE_SERVER",
		buildID:        "build-appcfg-1",
		chunkID:        "chunk-appcfg-1",
		step:           "Reading app config...",
		serverStatus:   http.StatusOK,
		serverBody:     `{"status":"ok"}`,
		wantURI:        "/builds/build-appcfg-1/active-step",
		wantChunkID:    "chunk-appcfg-1",
		wantActiveStep: "Reading app config...",
	},
	{
		name:       "unconfigured vanadis URL skips flush without dialing a default",
		buildID:    "build-unconfigured",
		chunkID:    "chunk-unconfigured",
		step:       "anything",
		skipServer: true,
	},
	{
		name:            "missing buildID with non-empty chunkID fails loud on direct restate fallback",
		restateURL:      "http://restate.test:8080",
		buildID:         "",
		chunkID:         "chunk-missing-build",
		step:            "anything",
		expectErr:       true,
		expectErrSubstr: "missing build_id",
		skipServer:      true,
	},
	{
		name:            "restate server 500 error fails loud",
		agentName:       "aerial",
		buildID:         "build-err-500",
		chunkID:         "chunk-err-500",
		step:            "failing step",
		serverStatus:    http.StatusInternalServerError,
		serverBody:      `{"error":"restate internal failure"}`,
		expectErr:       true,
		expectErrSubstr: "restate returned status 500",
		wantURI:         "/Build/build-err-500/UpdateActiveStep",
		wantChunkID:     "chunk-err-500",
		wantActiveStep:  "failing step",
	},
	{
		name:            "restate server 404 error fails loud",
		agentName:       "aerial",
		buildID:         "build-err-404",
		chunkID:         "chunk-err-404",
		step:            "not found step",
		serverStatus:    http.StatusNotFound,
		serverBody:      `{"error":"build not found"}`,
		expectErr:       true,
		expectErrSubstr: "restate returned status 404",
		wantURI:         "/Build/build-err-404/UpdateActiveStep",
		wantChunkID:     "chunk-err-404",
		wantActiveStep:  "not found step",
	},
}

func TestWorkerPool_FlushChunkActiveStepToRestate(t *testing.T) {
	t.Parallel()

	for _, tc := range flushActiveStepTestCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runFlushActiveStepCase(t, tc)
		})
	}
}

type vanadisHTTPClientCase struct {
	name       string
	injected   bool
	token      string
	wantAuth   string
	wantErrSub string
}

func runVanadisHTTPClientCase(t *testing.T, tc vanadisHTTPClientCase) {
	t.Helper()
	var gotAuth string
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotAuth = r.Header.Get("Authorization")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tokenPath := filepath.Join(t.TempDir(), "token")
	if tc.token != "" {
		if err := os.WriteFile(tokenPath, []byte(tc.token), 0o600); err != nil {
			t.Fatalf("write token: %v", err)
		}
	}
	pool := &WorkerPool{cfg: WorkerPoolConfig{SATokenPath: tokenPath}}
	if tc.injected {
		pool.cfg.VanadisHTTPClient = srv.Client()
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, http.NoBody)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := pool.vanadisHTTPClient().Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	gotErr := ""
	if err != nil {
		gotErr = err.Error()
	}
	if (tc.wantErrSub == "") != (err == nil) || !strings.Contains(gotErr, tc.wantErrSub) {
		t.Fatalf("error = %v, want substring %q", err, tc.wantErrSub)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotAuth != tc.wantAuth {
		t.Fatalf("Authorization = %q, want %q", gotAuth, tc.wantAuth)
	}
}

func TestWorkerPool_VanadisHTTPClient(t *testing.T) {
	t.Parallel()

	tests := []vanadisHTTPClientCase{
		{name: "injected client is used as-is", injected: true, wantAuth: ""},
		{name: "configured token path sends bearer token", token: "pool-test-token", wantAuth: "Bearer pool-test-token"},
		{name: "missing token file fails before dialing", wantErrSub: "obtain serviceaccount token"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runVanadisHTTPClientCase(t, tc)
		})
	}
}
