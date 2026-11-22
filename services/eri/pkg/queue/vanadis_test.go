package queue

import (
	"context"
	"encoding/json"
	"errors"
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
	"github.com/azylman/aerial/services/eri/pkg/runner"
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
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			memBus := pubsub.NewMemoryBus()
			defer memBus.Close()
			trackBus := &trackingBus{Bus: memBus}
			recorder := newOutcomeRecorder(t, memBus)

			poolCtx, cancelPool := context.WithCancel(context.Background())
			defer cancelPool()
			restateSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			defer restateSrv.Close()

			pool := &WorkerPool{
				ctx:      poolCtx,
				cancel:   cancelPool,
				eventBus: trackBus,
				cfg: WorkerPoolConfig{
					EventBus:          trackBus,
					RestateIngressURL: restateSrv.URL,
				},
			}

			disp := outcomeDispatch(tc.dispatchID, tc.source, tc.chunkID)
			te := &turnExecution{
				pool:        pool,
				dispatch:    &disp,
				threadID:    "dispatch-" + disp.ID,
				triggerType: "dispatch:" + tc.source,
				execStart:   time.Now().UTC().Add(-100 * time.Millisecond),
				turnStatus:  tc.turnStatus,
				turnError:   tc.turnError,
				turnCtx:     poolCtx,
			}

			cleanup := pool.setupDispatchTurn(disp, te)
			cleanup()

			delivered := recorder.waitFor(len(tc.wantDelivered))
			assertAttemptedPublishes(t, trackBus.dispatchPublishes(), tc.wantPublishes)
			assertDeliveredOutcomes(t, delivered, tc.wantDelivered)
		})
	}
}

type retryFailingBus struct {
	pubsub.Bus

	mu           sync.Mutex
	attempts     int
	failAttempts int
	afterAttempt func(att int)
}

func (r *retryFailingBus) Publish(ctx context.Context, subject, msgID string, payload any) error {
	if !strings.HasPrefix(subject, pubsub.SubjectAerialDispatchPrefix) {
		return r.Bus.Publish(ctx, subject, msgID, payload)
	}
	r.mu.Lock()
	r.attempts++
	att := r.attempts
	cb := r.afterAttempt
	r.mu.Unlock()
	if cb != nil {
		cb(att)
	}
	if att <= r.failAttempts {
		return errors.New("simulated jetstream publish error")
	}
	return r.Bus.Publish(ctx, subject, msgID, payload)
}

func TestWorkerPool_SetupDispatchTurn_OutcomePublishRetries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		failAttempts  int
		cancelPool    bool
		cancelBefore  bool
		wantAttempts  int
		wantPublishes []string
	}{
		{
			name:          "outcome publish succeeds on first attempt",
			failAttempts:  0,
			wantAttempts:  1,
			wantPublishes: []string{"aerial.dispatch.completed"},
		},
		{
			name:          "outcome publish retries and succeeds after transient error",
			failAttempts:  1,
			wantAttempts:  2,
			wantPublishes: []string{"aerial.dispatch.completed"},
		},
		{
			name:          "outcome publish exhausts 3 attempts on continuous error",
			failAttempts:  5,
			wantAttempts:  3,
			wantPublishes: nil,
		},
		{
			name:          "cancelled pool context aborts publish retries",
			failAttempts:  5,
			cancelPool:    true,
			wantAttempts:  1,
			wantPublishes: nil,
		},
		{
			name:          "cancelled pool context before cleanup still publishes first attempt",
			failAttempts:  0,
			cancelBefore:  true,
			wantAttempts:  1,
			wantPublishes: []string{"aerial.dispatch.completed"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			memBus := pubsub.NewMemoryBus()
			defer memBus.Close()
			failingBus := &retryFailingBus{Bus: memBus, failAttempts: tc.failAttempts}
			recorder := newOutcomeRecorder(t, memBus)

			poolCtx, cancelPool := context.WithCancel(context.Background())
			defer cancelPool()

			restateSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			defer restateSrv.Close()

			pool := &WorkerPool{
				ctx:      poolCtx,
				cancel:   cancelPool,
				eventBus: failingBus,
				cfg: WorkerPoolConfig{
					EventBus:           failingBus,
					RetryDelayOverride: 1 * time.Millisecond,
					RestateIngressURL:  restateSrv.URL,
				},
			}

			if tc.cancelBefore {
				cancelPool()
			}
			if tc.cancelPool {
				failingBus.afterAttempt = func(att int) {
					if att == 1 {
						cancelPool()
					}
				}
			}

			disp := outcomeDispatch("disp-retry-"+strings.ReplaceAll(tc.name, " ", "-"), "vanadis", "chunk-retry")
			te := &turnExecution{
				pool:        pool,
				dispatch:    &disp,
				threadID:    "dispatch-" + disp.ID,
				triggerType: "dispatch:vanadis",
				execStart:   time.Now().UTC().Add(-100 * time.Millisecond),
				turnStatus:  "success",
				turnCtx:     poolCtx,
			}

			cleanup := pool.setupDispatchTurn(disp, te)
			cleanup()

			failingBus.mu.Lock()
			gotAttempts := failingBus.attempts
			failingBus.mu.Unlock()

			if gotAttempts != tc.wantAttempts {
				t.Fatalf("publish attempts = %d, want %d", gotAttempts, tc.wantAttempts)
			}

			if len(tc.wantPublishes) > 0 {
				delivered := recorder.waitFor(len(tc.wantPublishes))
				if len(delivered) != len(tc.wantPublishes) {
					t.Fatalf("delivered count = %d, want %d", len(delivered), len(tc.wantPublishes))
				}
			}
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
		wantURI:        "/Build/build-101/UpdateActiveStep/send",
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
		wantURI:        "/Build/build-202/UpdateActiveStep/send",
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
		wantURI:        "/Build/" + url.PathEscape("build/special:1") + "/UpdateActiveStep/send",
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
		wantURI:        "/Build/build-clear/UpdateActiveStep/send",
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
		wantURI:         "/Build/build-err-500/UpdateActiveStep/send",
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
		wantURI:         "/Build/build-err-404/UpdateActiveStep/send",
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

type setupDispatchActiveStepCase struct {
	name             string
	source           string
	chunkID          string
	targetID         string
	prompt           string
	dispatchStatus   string
	serverStatus     int
	emitToolCommand  string
	expectUpdater    bool
	expectDirtyAfter bool
	wantFlushes      []string
}

func verifyUpdaterStep(t *testing.T, u *StatusUpdater, cmd string, expectDirty bool) {
	t.Helper()
	if cmd == "" {
		return
	}
	u.HandleStep(&runner.StepUpdateEvent{
		StepType: "tool",
		ToolName: "run_command",
		State:    "ACTIVE",
		ToolInfo: &runner.StepToolInfo{
			Parameters: runner.StepToolParameters{CommandLine: cmd},
		},
	})
	u.flush()
	if !expectDirty {
		return
	}
	u.mu.Lock()
	dirty := u.dirty
	u.mu.Unlock()
	if !dirty {
		t.Error("expected statusUpdater to remain dirty after server error")
	}
}

func runSetupDispatchActiveStepCase(t *testing.T, tc setupDispatchActiveStepCase) {
	t.Helper()

	var flushedSteps []string
	var flushedMu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p updateActiveStepPayload
		_ = json.NewDecoder(r.Body).Decode(&p)
		flushedMu.Lock()
		flushedSteps = append(flushedSteps, p.ActiveStep)
		flushedMu.Unlock()

		status := tc.serverStatus
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := &WorkerPool{
		ctx:    ctx,
		cancel: cancel,
		cfg: WorkerPoolConfig{
			RestateIngressURL: srv.URL,
		},
	}

	disp := db.Dispatch{
		ID:             "disp-" + strings.ReplaceAll(tc.name, " ", "-"),
		Source:         tc.source,
		SessionID:      tc.source + "-sess",
		TargetID:       tc.targetID,
		VanadisChunkID: tc.chunkID,
		Prompt:         tc.prompt,
		Status:         tc.dispatchStatus,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}

	te := &turnExecution{
		pool:        pool,
		dispatch:    &disp,
		threadID:    "dispatch-" + disp.ID,
		triggerType: "dispatch:" + tc.source,
		turnCtx:     ctx,
	}

	cleanup := pool.setupDispatchTurn(disp, te)
	if !tc.expectUpdater {
		if te.statusUpdater != nil {
			t.Fatal("expected statusUpdater to be nil for non-vanadis dispatch")
		}
	} else {
		if te.statusUpdater == nil {
			t.Fatal("expected statusUpdater to be non-nil")
		}
		verifyUpdaterStep(t, te.statusUpdater, tc.emitToolCommand, tc.expectDirtyAfter)
	}

	cleanup()

	flushedMu.Lock()
	got := append([]string(nil), flushedSteps...)
	flushedMu.Unlock()

	if len(got) != len(tc.wantFlushes) {
		t.Fatalf("flushed steps = %v (len %d), want %v (len %d)", got, len(got), tc.wantFlushes, len(tc.wantFlushes))
	}
	for i, want := range tc.wantFlushes {
		if !strings.Contains(got[i], want) {
			t.Errorf("flush[%d] = %q, want substring %q", i, got[i], want)
		}
	}
}

func TestWorkerPool_SetupDispatchTurn_ActiveStepLifecycle(t *testing.T) {
	t.Parallel()

	tests := []setupDispatchActiveStepCase{
		{
			name:            "live step forwarding and turn cleanup clearing",
			source:          "vanadis",
			chunkID:         "build-lifecycle-chunk-1",
			prompt:          "Build ID: build-lifecycle\nChunk ID: build-lifecycle-chunk-1",
			dispatchStatus:  db.StatusPending,
			emitToolCommand: "git commit -m tweak",
			expectUpdater:   true,
			wantFlushes:     []string{"Executing `git`", ""},
		},
		{
			name:            "resumed dispatch turn extracts chunk info and forwards steps",
			source:          "vanadis",
			chunkID:         "",
			targetID:        "build-resumed-99-chunk-1",
			prompt:          "[VANADIS BUILD CHUNK EXECUTION]\nBuild ID: build-resumed-99\nChunk ID: build-resumed-99-chunk-1\nResume turn",
			dispatchStatus:  db.StatusProcessing,
			emitToolCommand: "go test -v ./...",
			expectUpdater:   true,
			wantFlushes:     []string{"Executing `go`", ""},
		},
		{
			name:             "http error on live step logs and marks dirty for retry",
			source:           "vanadis",
			chunkID:          "build-retry-chunk-err",
			prompt:           "Build ID: build-retry\nChunk ID: build-retry-chunk-err",
			dispatchStatus:   db.StatusPending,
			serverStatus:     http.StatusInternalServerError,
			emitToolCommand:  "verify.sh",
			expectUpdater:    true,
			expectDirtyAfter: true,
			wantFlushes:      []string{"Executing `verify.sh`", ""},
		},
		{
			name:           "heartbeat dispatch without chunk info skips status updater",
			source:         "heartbeat",
			chunkID:        "",
			prompt:         "regular heartbeat check",
			dispatchStatus: db.StatusPending,
			expectUpdater:  false,
			wantFlushes:    nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runSetupDispatchActiveStepCase(t, tc)
		})
	}
}
