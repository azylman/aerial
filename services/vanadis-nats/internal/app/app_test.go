package app

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
	"github.com/azylman/aerial/services/vanadis-nats/internal/config"
)

func TestSetupHealthMux(t *testing.T) {
	origMeta, origCounters := httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn
	t.Cleanup(func() {
		httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn = origMeta, origCounters
	})
	httpkit.WriteMetaDirFn = func(string) error { return nil }
	httpkit.WriteCountersDirFn = func(string) error { return nil }

	mux := SetupHealthMux()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
		wantBody   string
	}{
		{name: "GET /health", method: http.MethodGet, path: "/health", wantStatus: http.StatusOK, wantBody: `{"status":"ok"}`},
		{name: "GET /healthz", method: http.MethodGet, path: "/healthz", wantStatus: http.StatusOK, wantBody: `{"status":"ok"}`},
		{name: "GET /metrics", method: http.MethodGet, path: "/metrics", wantStatus: http.StatusOK},
		{name: "POST /debug/coverage/flush", method: http.MethodPost, path: "/debug/coverage/flush", wantStatus: http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, http.NoBody)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			assert.Equal(t, tc.wantStatus, rec.Code)
			if tc.wantBody != "" {
				assert.JSONEq(t, tc.wantBody, rec.Body.String())
			}
		})
	}
}

func getFreePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	parts := strings.Split(ln.Addr().String(), ":")
	return parts[len(parts)-1]
}

func TestRunApp(t *testing.T) {
	local := config.Config{Port: "0", NATSURL: "nats://localhost:4222"}
	tests := []struct {
		name          string
		cfg           config.Config
		bus           *pubsubtest.FaultBus
		connectErr    error
		useDefaultBus bool
		cancelEarly   bool
		failServe     bool
		wantErrMsg    string
	}{
		{name: "default bus rejects empty url", cfg: config.Config{Port: "0"}, useDefaultBus: true, wantErrMsg: "nats_url is required"},
		{name: "default bus rejects bad url", cfg: config.Config{Port: "0", NATSURL: "://"}, useDefaultBus: true, wantErrMsg: "connect to nats"},
		{name: "invalid health port causes listen error", cfg: config.Config{Port: "invalid-port"}, wantErrMsg: "listening on health port"},
		{name: "connect to nats failure", cfg: local, connectErr: errors.New("cannot connect"), wantErrMsg: "connect to nats"},
		{name: "start consumers failure", cfg: local, bus: &pubsubtest.FaultBus{EnsureErr: errors.New("streams")}, wantErrMsg: "start consumers"},
		{name: "clean shutdown on cancel", cfg: local, cancelEarly: true},
		{name: "health serve failure returns serve error", cfg: local, failServe: true, wantErrMsg: "closed network connection"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oldBusFn := NewBusFn
			defer func() { NewBusFn = oldBusFn }()
			oldListenFn := listenFn
			defer func() { listenFn = oldListenFn }()

			if tc.failServe {
				listenFn = func(context.Context, string) (net.Listener, error) {
					ln, err := net.Listen("tcp", "127.0.0.1:0")
					require.NoError(t, err)
					require.NoError(t, ln.Close())
					return ln, nil
				}
			}

			if !tc.useDefaultBus {
				NewBusFn = func(context.Context, string) (pubsub.Bus, error) {
					if tc.connectErr != nil {
						return nil, tc.connectErr
					}
					bus := tc.bus
					if bus == nil {
						bus = &pubsubtest.FaultBus{}
					}
					bus.Bus = pubsub.NewMemoryBus()
					return bus, nil
				}
			}

			cfg := tc.cfg
			if cfg.Port == "0" {
				cfg.Port = getFreePort(t)
			}

			ctx, cancel := context.WithCancel(context.Background())
			if tc.cancelEarly {
				cancel()
			} else {
				defer cancel()
			}

			err := RunApp(ctx, cfg)
			if tc.wantErrMsg == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
			}
		})
	}
}
