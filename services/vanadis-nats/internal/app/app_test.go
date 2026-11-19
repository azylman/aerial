package app

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-nats/internal/config"
)

type testBus struct {
	ensureErr   error
	consumeErrs map[string]error
	subscribed  map[string]bool
	mu          sync.Mutex
	closed      bool
}

func (*testBus) Publish(context.Context, string, string, any) error { return nil }

func (*testBus) Subscribe(context.Context, string, string, pubsub.Handler) (pubsub.Subscription, error) {
	return nil, nil
}

func (b *testBus) ConsumeQueue(_ context.Context, subject, queue string, _ pubsub.Handler) (pubsub.Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.consumeErrs != nil {
		if err, ok := b.consumeErrs[subject]; ok && err != nil {
			return nil, err
		}
	}
	if b.subscribed == nil {
		b.subscribed = make(map[string]bool)
	}
	b.subscribed[subject+":"+queue] = true
	return &testSub{}, nil
}

func (b *testBus) EnsureCoreStreams(context.Context) error { return b.ensureErr }

func (b *testBus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

type testSub struct{}

func (*testSub) Unsubscribe() error { return nil }

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
		{
			name:       "GET /health",
			method:     http.MethodGet,
			path:       "/health",
			wantStatus: http.StatusOK,
			wantBody:   `{"status":"ok"}`,
		},
		{
			name:       "GET /healthz",
			method:     http.MethodGet,
			path:       "/healthz",
			wantStatus: http.StatusOK,
			wantBody:   `{"status":"ok"}`,
		},
		{
			name:       "GET /metrics",
			method:     http.MethodGet,
			path:       "/metrics",
			wantStatus: http.StatusOK,
		},
		{
			name:       "POST /debug/coverage/flush",
			method:     http.MethodPost,
			path:       "/debug/coverage/flush",
			wantStatus: http.StatusOK,
		},
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
	tests := []struct {
		name          string
		cfg           config.Config
		fakeBus       *testBus
		connectErr    error
		useDefaultBus bool
		cancelEarly   bool
		failServe     bool
		wantErrMsg    string
	}{
		{
			name: "empty nats url returns error from default bus factory",
			cfg: config.Config{
				Port:    "0",
				NATSURL: "",
			},
			useDefaultBus: true,
			wantErrMsg:    "nats_url is required",
		},
		{
			name: "default bus factory connects and fails on invalid url",
			cfg: config.Config{
				Port:    "0",
				NATSURL: "://",
			},
			useDefaultBus: true,
			wantErrMsg:    "connect to nats",
		},
		{
			name: "invalid health port causes listen error",
			cfg: config.Config{
				Port:    "invalid-port",
				NATSURL: "nats://localhost:4222",
			},
			fakeBus:    &testBus{},
			wantErrMsg: "listening on health port",
		},
		{
			name: "connect to nats failure",
			cfg: config.Config{
				Port:    "0",
				NATSURL: "nats://invalid:4222",
			},
			connectErr: errors.New("cannot connect"),
			wantErrMsg: "connect to nats",
		},
		{
			name: "start consumers failure",
			cfg: config.Config{
				Port:    "0",
				NATSURL: "nats://localhost:4222",
			},
			fakeBus:    &testBus{ensureErr: errors.New("streams fail")},
			wantErrMsg: "start consumers",
		},
		{
			name: "clean shutdown on cancel",
			cfg: config.Config{
				Port:    "0",
				NATSURL: "nats://localhost:4222",
			},
			fakeBus:     &testBus{},
			cancelEarly: true,
			wantErrMsg:  "",
		},
		{
			name: "health serve failure returns serve error",
			cfg: config.Config{
				Port:    "0",
				NATSURL: "nats://localhost:4222",
			},
			fakeBus:    &testBus{},
			failServe:  true,
			wantErrMsg: "closed network connection",
		},
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
					if tc.fakeBus != nil {
						return tc.fakeBus, nil
					}
					return &testBus{}, nil
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
