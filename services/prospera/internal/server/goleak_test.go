package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/azylman/aerial/services/prospera/internal/config"
	"github.com/azylman/aerial/services/prospera/internal/mediastream"
)

type hermeticTestTransport struct {
	base http.RoundTripper
}

func (h *hermeticTestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil || req.URL == nil {
		return nil, errors.New("hermetic test guard: nil request or URL")
	}
	host := req.URL.Hostname()
	if host != "127.0.0.1" && host != "::1" && host != "localhost" {
		return nil, fmt.Errorf("hermetic test guard: blocked non-loopback HTTP call to %s", req.URL.Host)
	}
	switch req.URL.Port() {
	case "8080", "8088", "4020", "8081", "8082", "8083", "8090", "8070", "9070":
		return nil, fmt.Errorf("hermetic test guard: blocked dial to live daemon port %s", req.URL.Port())
	}
	base := h.base
	if base == nil {
		base = http.DefaultTransport
		if base == h {
			base = &http.Transport{}
		}
	}
	return base.RoundTrip(req)
}

func (h *hermeticTestTransport) CloseIdleConnections() {
	if cl, ok := h.base.(interface{ CloseIdleConnections() }); ok {
		cl.CloseIdleConnections()
	}
}

func wrapHermeticTransport() {
	if _, ok := http.DefaultTransport.(*hermeticTestTransport); !ok {
		http.DefaultTransport = &hermeticTestTransport{base: http.DefaultTransport}
	}
}

func init() {
	wrapHermeticTransport()
}

func TestMain(m *testing.M) {
	wrapHermeticTransport()
	goleak.VerifyTestMain(m, goleak.Cleanup(func(exitCode int) {
		http.DefaultClient.CloseIdleConnections()
		if cl, ok := http.DefaultTransport.(interface{ CloseIdleConnections() }); ok {
			cl.CloseIdleConnections()
		}
	}))
}

func TestDrain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		nilMgr      bool
		preregister bool
		timeout     time.Duration
	}{
		{name: "nil manager returns", nilMgr: true, timeout: time.Second},
		{name: "no sessions returns immediately", preregister: false, timeout: 5 * time.Second},
		{name: "canceled context returns", preregister: true, timeout: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := &config.Config{Port: "4019"}
			mgr := mediastream.NewCallSessionManager(staticConfig{cfg: cfg}, newMockStoreForDelivery(), nil, nil, nil)
			var srv *Server
			if tc.nilMgr {
				srv = &Server{}
			} else {
				srv = &Server{sessionMgr: mgr}
				if tc.preregister {
					mgr.RegisterSession("call-drain-1", &stubSession{})
				}
			}
			ctx := context.Background()
			if tc.timeout == 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			} else {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tc.timeout)
				defer cancel()
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				srv.Drain(ctx)
			}()
			select {
			case <-done:
			case <-time.After(6 * time.Second):
				t.Fatal("Drain did not return")
			}
		})
	}
}

func TestDrain_WaitsForActiveSessions(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{Port: "4019"}
	mgr := mediastream.NewCallSessionManager(staticConfig{cfg: cfg}, newMockStoreForDelivery(), nil, nil, nil)
	mgr.RegisterSession("call-drain-live", &stubSession{})
	srv := &Server{sessionMgr: mgr}

	tests := []struct {
		name        string
		timeout     time.Duration
		wantAtLeast time.Duration
	}{
		{name: "deadline bounds the pacer wait", timeout: 200 * time.Millisecond, wantAtLeast: 150 * time.Millisecond},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()
			done := make(chan struct{})
			start := time.Now()
			go func() {
				defer close(done)
				srv.Drain(ctx)
			}()
			select {
			case <-done:
			case <-time.After(6 * time.Second):
				t.Fatal("Drain did not return after context deadline")
			}
			if elapsed := time.Since(start); elapsed < tc.wantAtLeast {
				t.Fatalf("Drain returned in %v, want at least %v of pacer waiting", elapsed, tc.wantAtLeast)
			}
			if got := mgr.ActiveCount(); got != 1 {
				t.Fatalf("ActiveCount = %d, want 1 (session still live at deadline)", got)
			}
		})
	}
}

type stubSession struct {
	mu      sync.Mutex
	hangups int
}

func (*stubSession) ProviderCallSID() string { return "" }
func (*stubSession) RecordingWAV() []byte    { return nil }
func (*stubSession) Steer(_, _ string) error { return nil }
func (*stubSession) HandleAMD(_ string)      {}
func (s *stubSession) InitiateHangup(_ string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hangups++
}

func (s *stubSession) hangupCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hangups
}
func (*stubSession) Run(_ context.Context) {}
