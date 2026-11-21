package router

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
)

func TestServerEndpoints_HealthMetricsAndCoverage(t *testing.T) {
	oldCountersFn := httpkit.WriteCountersDirFn
	oldMetaFn := httpkit.WriteMetaDirFn
	t.Cleanup(func() {
		httpkit.WriteCountersDirFn = oldCountersFn
		httpkit.WriteMetaDirFn = oldMetaFn
	})

	RecordJobEnqueued("", "", "")
	RecordJobRetry("", "", "")
	RecordJobDropped("", "", "")
	SetDiscordGatewayLatency(0.042)

	tests := []struct {
		name          string
		method        string
		path          string
		mockMetaErr   error
		mockCountErr  error
		queueStatsErr bool
		wantCode      int
		wantBodySubs  []string
	}{
		{
			name:         "/healthz returns 200 and json",
			method:       http.MethodGet,
			path:         "/healthz",
			wantCode:     http.StatusOK,
			wantBodySubs: []string{`"status":"ok"`, `"service":"synapse"`},
		},
		{
			name:     "/metrics returns 200 and prometheus metrics with queue stats and gateway latency",
			method:   http.MethodGet,
			path:     "/metrics",
			wantCode: http.StatusOK,
			wantBodySubs: []string{
				"synapse_queue_depth",
				"synapse_discord_gateway_latency_seconds 0.042",
			},
		},
		{
			name:          "/metrics with queue stats error logs and still returns 200",
			method:        http.MethodGet,
			path:          "/metrics",
			queueStatsErr: true,
			wantCode:      http.StatusOK,
			wantBodySubs:  []string{"synapse_discord_gateway_latency_seconds 0.042"},
		},
		{
			name:     "/debug/coverage/flush calls flush and returns 200",
			method:   http.MethodGet,
			path:     "/debug/coverage/flush",
			wantCode: http.StatusOK,
		},
		{
			name:        "/debug/coverage/flush meta error still returns 200",
			method:      http.MethodGet,
			path:        "/debug/coverage/flush",
			mockMetaErr: errors.New("simulated meta flush error"),
			wantCode:    http.StatusOK,
		},
		{
			name:         "/debug/coverage/flush counters error returns 500",
			method:       http.MethodGet,
			path:         "/debug/coverage/flush",
			mockCountErr: errors.New("simulated counters flush error"),
			wantCode:     http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			httpkit.WriteMetaDirFn = func(dir string) error {
				return tc.mockMetaErr
			}
			httpkit.WriteCountersDirFn = func(dir string) error {
				return tc.mockCountErr
			}

			srv := NewRouterServer(fixedConfig(Config{Port: "4020"}), nil)
			mockQ := &mockConsumerStatsQuerier{
				stats: map[string][2]int64{
					"AERIAL_JOBS/synapse-github": {4, 1},
				},
			}
			if tc.queueStatsErr {
				mockQ.err = errors.New("simulated queue query error")
			}
			srv.SetQueueStats(mockQ)
			srv.SetGatewayLatencyFn(func() time.Duration {
				return 42 * time.Millisecond
			})

			rec := doHTTPRequest(srv.Routes(), tc.method, tc.path, nil, nil)
			if rec.Code != tc.wantCode {
				t.Fatalf("code = %d, want %d (body: %s)", rec.Code, tc.wantCode, rec.Body.String())
			}
			for _, sub := range tc.wantBodySubs {
				if !strings.Contains(rec.Body.String(), sub) {
					t.Errorf("body missing %q: %s", sub, rec.Body.String())
				}
			}
		})
	}

	rec := httptest.NewRecorder()
	httpkit.WriteJSON(rec, http.StatusOK, make(chan int))
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("writeJSON code = %d, want 500", rec.Code)
	}
}

func TestConsumerStatsQuerier(t *testing.T) {
	t.Parallel()
	var nilSrv *RouterServer
	if nilSrv.consumerStatsQuerier() != nil {
		t.Errorf("expected nil for nil server")
	}
	srv := NewRouterServer(fixedConfig(Config{}), nil)
	if srv.consumerStatsQuerier() != nil {
		t.Errorf("expected nil when no stats or bus configured")
	}
	memBus := pubsub.NewMemoryBus()
	srv.SetEventBus(memBus)
	if srv.consumerStatsQuerier() == nil {
		t.Errorf("expected bus stats querier when bus implements consumerStatsQuerier")
	}
	mockQ := &mockConsumerStatsQuerier{}
	srv.SetQueueStats(mockQ)
	if srv.consumerStatsQuerier() != mockQ {
		t.Errorf("expected queueStats override")
	}
}

func newTestReloadable(initial, next Config) *httpkit.Reloadable[Config] {
	return httpkit.NewReloadable(&initial, func() (*Config, error) {
		reloaded := next
		return &reloaded, nil
	}, synapseRestartRequired)
}

// testRestateURL is a never-dialed Restate ingress for lifecycle tests: the
// server only needs restate_url to pass startup validation.
const testRestateURL = "http://restate.test:8080"

func TestRunServerLifecycle(t *testing.T) {
	tests := []runServerLifecycleCase{
		{
			name:             "runServer start, loopback reload, and graceful cancel shutdown",
			config:           Config{Port: "0", RestateURL: testRestateURL},
			wantGraceful:     true,
			wantReloadStatus: httpkit.ReloadUnchanged,
		},
		{
			name:             "runServer live reload applies non-restart field",
			config:           Config{Port: "0", RestateURL: testRestateURL},
			reloadConfig:     &Config{Port: "0", RestateURL: testRestateURL, SystemChannelID: "123456789012345678"},
			wantGraceful:     true,
			wantReloadStatus: httpkit.ReloadApplied,
		},
		{
			name:    "runServer invalid port -1 returns error",
			config:  Config{Port: "-1", RestateURL: testRestateURL},
			wantErr: true,
		},
		{
			name:      "runApp pre-canceled context returns clean or server closed",
			config:    Config{Port: "0", RestateURL: testRestateURL},
			preCancel: true,
			isRunApp:  true,
		},
		{
			name:         "runApp exits cleanly when reload requires restart",
			config:       Config{Port: "0", RestateURL: testRestateURL},
			reloadConfig: &Config{Port: "4021", RestateURL: testRestateURL},
			isRunApp:     true,
		},
		{
			name:    "runServer without restate url returns error",
			config:  Config{Port: "0"},
			wantErr: true,
		},
		{
			name:     "runApp invalid port returns error",
			config:   Config{Port: "-1", RestateURL: testRestateURL},
			isRunApp: true,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			next := tc.config
			if tc.reloadConfig != nil {
				next = *tc.reloadConfig
			}
			cfgSrc := newTestReloadable(tc.config, next)
			if tc.wantGraceful {
				runServerGracefulCase(t, tc, cfgSrc, next)
				return
			}
			if tc.isRunApp {
				runServerAppCase(t, tc, cfgSrc)
				return
			}
			err := runServer(context.Background(), cfgSrc, nil)
			if (err != nil) != tc.wantErr {
				t.Errorf("runServer() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

type runServerLifecycleCase struct {
	name             string
	config           Config
	reloadConfig     *Config
	preCancel        bool
	isRunApp         bool
	wantErr          bool
	wantGraceful     bool
	wantReloadStatus httpkit.ReloadStatus
}

func runServerGracefulCase(t *testing.T, tc runServerLifecycleCase, cfgSrc *httpkit.Reloadable[Config], next Config) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	done := make(chan error, 1)

	go func() {
		done <- runServer(ctx, cfgSrc, func(addr string) {
			ready <- addr
		})
	}()

	var addr string
	select {
	case addr = <-ready:
		if addr == "" {
			t.Errorf("expected non-empty listening address")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for server to start")
	}

	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("SplitHostPort(%q): %v", addr, err)
	}
	resp, err := http.Post("http://"+net.JoinHostPort("127.0.0.1", port)+"/-/reload", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /-/reload: %v", err)
	}
	var got struct {
		Status httpkit.ReloadStatus `json:"status"`
	}
	decodeErr := json.NewDecoder(resp.Body).Decode(&got)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || decodeErr != nil || got.Status != tc.wantReloadStatus {
		t.Errorf("POST /-/reload = %d %q (decode err %v), want 200 %q", resp.StatusCode, got.Status, decodeErr, tc.wantReloadStatus)
	}
	if gotSys := cfgSrc.Current().SystemChannelID; gotSys != next.SystemChannelID {
		t.Errorf("SystemChannelID after reload = %q, want %q", gotSys, next.SystemChannelID)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("expected clean shutdown, got error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for server shutdown")
	}
}

func runServerAppCase(t *testing.T, tc runServerLifecycleCase, cfgSrc *httpkit.Reloadable[Config]) {
	t.Helper()
	ctxC, cancelC := context.WithCancel(context.Background())
	if tc.preCancel {
		cancelC()
	} else {
		defer cancelC()
	}
	if tc.reloadConfig != nil {
		if status, err := cfgSrc.Reload(); status != httpkit.ReloadRestarting || err != nil {
			t.Fatalf("Reload() = %q, %v; want %q", status, err, httpkit.ReloadRestarting)
		}
	}
	err := runApp(ctxC, cfgSrc)
	if (err != nil) != tc.wantErr && !errors.Is(err, http.ErrServerClosed) {
		t.Errorf("unexpected error from runApp: %v", err)
	}
}
