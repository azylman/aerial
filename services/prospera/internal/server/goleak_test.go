package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/hermetictest"
	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/services/prospera/internal/config"
	"github.com/azylman/aerial/services/prospera/internal/fakes"
	"github.com/azylman/aerial/services/prospera/internal/mediastream"
)

// pgRunner runs the package under pkg/pgtest so RunApp tests open a real
// Postgres clone, inside hermetictest's goleak check.
type pgRunner struct{ m *testing.M }

func (r pgRunner) Run() int { return pgtest.Main(r.m) }

func TestMain(m *testing.M) {
	hermetictest.Main(pgRunner{m: m})
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
			mgr := mediastream.NewCallSessionManager(fakes.StaticConfig{Cfg: cfg}, fakes.NewFakeStore(), nil, nil, nil)
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
	mgr := mediastream.NewCallSessionManager(fakes.StaticConfig{Cfg: cfg}, fakes.NewFakeStore(), nil, nil, nil)
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
