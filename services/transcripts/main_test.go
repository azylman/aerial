package main

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/k8sauth"
	"github.com/azylman/aerial/pkg/pubsub"
)

func TestOpenDB(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		dsn     string
		wantSub string
	}{
		{"empty dsn", "", "cannot be empty"},
		{"blank dsn", "   ", "cannot be empty"},
		{"unsupported scheme", "sqlite:///tmp/x.db", "unsupported database scheme"},
		{"unreachable host fails ping", "postgres://127.0.0.1:1/db?sslmode=disable", "ping failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := openDB(context.Background(), tc.dsn); err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("openDB() err = %v, want substring %q", err, tc.wantSub)
			}
		})
	}
}

func TestLoadConfig(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://env:pass@host:5432/db")
	t.Setenv("POSTGRES_URL", "")
	t.Setenv("POSTGRES_HOST", "")
	t.Setenv("POSTGRES_PORT", "")
	t.Setenv("POSTGRES_USER", "")
	t.Setenv("POSTGRES_PASSWORD", "")
	t.Setenv("POSTGRES_DB", "")
	t.Setenv("NATS_URL", "")

	p := filepath.Join(t.TempDir(), "load.yaml")
	if err := os.WriteFile(p, []byte("port: '4017'\nstack: 'aerial'\n"), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	cfg, err := LoadConfig(p)
	if err != nil {
		t.Fatalf("LoadConfig() err = %v", err)
	}
	if cfg.Port != "4017" || cfg.Stack != "aerial" || cfg.DatabaseURL != "postgres://env:pass@host:5432/db" {
		t.Fatalf("unexpected config: %+v", cfg)
	}

	t.Setenv("DATABASE_URL", "")
	t.Setenv("CONFIG_PATH", filepath.Join(t.TempDir(), "missing.yaml"))
	if _, err := LoadConfig(ResolveConfigPath("")); err == nil {
		t.Errorf("expected error for missing config file")
	}
}

func TestNewBusIngester(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	bus := pubsub.NewMemoryBus()
	defer bus.Close()

	withBus := NewBusIngester(store, bus)
	if withBus.store != store {
		t.Errorf("NewBusIngester did not keep store")
	}
	if err := withBus.publish(context.Background(), "subject", "id", map[string]string{"k": "v"}); err != nil {
		t.Errorf("memory bus publish err = %v", err)
	}

	nilBus := NewBusIngester(store, nil)
	if err := nilBus.publish(context.Background(), "subject", "id", nil); err == nil {
		t.Errorf("expected error publishing through nil bus")
	}
}

func TestInitTranscriptsJetStream(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	ingester := NewIngester(store, nil)
	tests := []struct {
		name     string
		natsURL  string
		ingester *Ingester
		factory  func(ctx context.Context, url string) (pubsub.Bus, error)
		wantNil  bool
	}{
		{"empty url yields nil", "", ingester, nil, true},
		{"nil ingester yields nil", "nats://nats:4222", nil, nil, true},
		{"factory error yields nil", "nats://nats:4222", ingester, func(_ context.Context, _ string) (pubsub.Bus, error) {
			return nil, errors.New("dial failed")
		}, true},
		{"memory bus subscribes", "nats://nats:4222", ingester, func(_ context.Context, _ string) (pubsub.Bus, error) {
			return pubsub.NewMemoryBus(), nil
		}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			factory := tc.factory
			if factory == nil {
				factory = newBusFn
			}
			bus := initTranscriptsJetStreamWithFactory(context.Background(), tc.ingester, tc.natsURL, factory)
			if (bus == nil) != tc.wantNil {
				t.Errorf("bus nil = %v, want nil = %v", bus == nil, tc.wantNil)
			}
			if bus != nil {
				_ = bus.Close()
			}
		})
	}

	if bus := initTranscriptsJetStream(context.Background(), ingester, ""); bus != nil {
		t.Errorf("expected nil bus for empty url")
	}
}

func stubOpenDB(t *testing.T, db *sql.DB, err error) func() {
	t.Helper()
	old := openDBFn
	openDBFn = func(context.Context, string) (*sql.DB, error) { return db, err }
	return func() { openDBFn = old }
}

func TestRunApp(t *testing.T) {
	tests := []struct {
		name     string
		cfg      *Config
		stubErr  error
		useStub  bool
		wantSub  string
		canceled bool
	}{
		{"nil config", nil, nil, false, "config cannot be nil", false},
		{"empty port", &Config{DatabaseURL: "postgres://x"}, nil, false, "port cannot be empty", false},
		{"open db error", &Config{Port: "4017", DatabaseURL: "postgres://x"}, errors.New("db down"), true, "db down", false},
		{"listen failure surfaces", &Config{Port: "99999", DatabaseURL: "postgres://x", Stack: "aerial"}, nil, true, "", false},
		{"canceled context shuts down cleanly", &Config{Port: "0", DatabaseURL: "postgres://x", Stack: "aerial"}, nil, true, "", true},
		{"auth enabled shuts down cleanly", &Config{Port: "0", DatabaseURL: "postgres://x", Stack: "aerial", Auth: k8sauth.Config{Enabled: true}}, nil, true, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.useStub {
				store := newTestStore(t, "aerial")
				db := store.db
				defer stubOpenDB(t, db, tc.stubErr)()
			}
			ctx := context.Background()
			if tc.canceled {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			err := RunApp(ctx, tc.cfg)
			if tc.wantSub != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantSub) {
					t.Errorf("RunApp() err = %v, want substring %q", err, tc.wantSub)
				}
				return
			}
			if tc.name == "listen failure surfaces" {
				if err == nil {
					t.Errorf("expected listen error")
				}
				return
			}
			if err != nil {
				t.Errorf("RunApp() err = %v", err)
			}
		})
	}
}

func TestMainFunc(t *testing.T) {
	tmpDir := t.TempDir()
	validPath := filepath.Join(tmpDir, "transcripts.yaml")
	if err := os.WriteFile(validPath, []byte("port: '0'\nstack: 'aerial'\ndatabase_url: 'postgres://x'\n"), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	oldExit, oldRun := exitFn, runAppFn
	defer func() { exitFn, runAppFn = oldExit, oldRun }()

	tests := []struct {
		name       string
		configPath string
		runErr     error
		wantExit   bool
	}{
		{"missing config exits", filepath.Join(tmpDir, "missing.yaml"), nil, true},
		{"run error exits", validPath, errors.New("boom"), true},
		{"run success does not exit", validPath, nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CONFIG_PATH", tc.configPath)
			exited := false
			exitFn = func(string, ...any) { exited = true }
			runAppFn = func(context.Context, *Config) error { return tc.runErr }
			main()
			if exited != tc.wantExit {
				t.Errorf("exited = %v, want %v", exited, tc.wantExit)
			}
		})
	}
}

type ensureFailBus struct {
	pubsub.Bus
}

func (ensureFailBus) EnsureCoreStreams(context.Context) error { return errEnsureTest }

type consumeFailBus struct {
	pubsub.Bus
}

func (consumeFailBus) ConsumeQueue(context.Context, string, string, pubsub.Handler) (pubsub.Subscription, error) {
	return nil, errConsumeTest
}

type closeFailBus struct {
	pubsub.Bus
}

func (closeFailBus) Close() error { return errCloseTest }

var (
	errEnsureTest  = errors.New("ensure down")
	errConsumeTest = errors.New("subscribe down")
	errCloseTest   = errors.New("close down")
)

func TestInitTranscriptsJetStreamFailures(t *testing.T) {
	store := newTestStore(t, "aerial")
	ingester := NewIngester(store, nil)
	tests := []struct {
		name    string
		factory func(ctx context.Context, url string) (pubsub.Bus, error)
	}{
		{"ensure streams failure still returns bus", func(_ context.Context, _ string) (pubsub.Bus, error) {
			return ensureFailBus{Bus: pubsub.NewMemoryBus()}, nil
		}},
		{"subscribe failure still returns bus", func(_ context.Context, _ string) (pubsub.Bus, error) {
			return consumeFailBus{Bus: pubsub.NewMemoryBus()}, nil
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bus := initTranscriptsJetStreamWithFactory(context.Background(), ingester, "nats://nats:4222", tc.factory)
			if bus == nil {
				t.Fatalf("expected non-nil bus")
			}
			_ = bus.Close()
		})
	}
}

func TestRunAppWithBus(t *testing.T) {
	store := newTestStore(t, "aerial")
	restoreOpenDB := stubOpenDB(t, store.db, nil)
	defer restoreOpenDB()

	oldBus := newBusFn
	newBusFn = func(context.Context, string) (pubsub.Bus, error) { return pubsub.NewMemoryBus(), nil }
	defer func() { newBusFn = oldBus }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := &Config{Port: "0", DatabaseURL: "postgres://x", Stack: "aerial", NATSURL: "nats://nats:4222"}
	if err := RunApp(ctx, cfg); err != nil {
		t.Errorf("RunApp() err = %v", err)
	}
}

func TestRunAppBusCloseError(t *testing.T) {
	store := newTestStore(t, "aerial")
	restoreOpenDB := stubOpenDB(t, store.db, nil)
	defer restoreOpenDB()

	oldBus := newBusFn
	newBusFn = func(context.Context, string) (pubsub.Bus, error) {
		return closeFailBus{Bus: pubsub.NewMemoryBus()}, nil
	}
	defer func() { newBusFn = oldBus }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := &Config{Port: "0", DatabaseURL: "postgres://x", Stack: "aerial", NATSURL: "nats://nats:4222"}
	if err := RunApp(ctx, cfg); err != nil {
		t.Errorf("RunApp() err = %v", err)
	}
}

func TestNewBusFnDials(t *testing.T) {
	bus, err := newBusFn(context.Background(), "nats://127.0.0.1:1")
	if err != nil {
		t.Fatalf("newBusFn err = %v", err)
	}
	_ = bus.Close()
}

func TestBusIngesterPublishes(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	bus := pubsub.NewMemoryBus()
	defer bus.Close()
	ingester := NewBusIngester(store, bus)
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	msg := &fakeBatchMessage{data: marshalBatch(t, testBatch("sess-bus", epoch, 0, 1, false, false))}
	if err := ingester.HandleBatchMessage(context.Background(), msg); err != nil {
		t.Errorf("HandleBatchMessage err = %v", err)
	}
}

func TestInitJetStreamDeliversBatch(t *testing.T) {
	store := newTestStore(t, "aerial")
	ingester := NewIngester(store, nil)
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
	marks, err := store.GetWatermarks(context.Background(), "aerial")
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

func TestRunAppShutdownError(t *testing.T) {
	store := newTestStore(t, "aerial")
	restoreOpenDB := stubOpenDB(t, store.db, nil)
	defer restoreOpenDB()
	oldShutdown := httpShutdown
	httpShutdown = func(*http.Server, context.Context) error { return errShutdownTest }
	defer func() { httpShutdown = oldShutdown }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := &Config{Port: "0", DatabaseURL: "postgres://x", Stack: "aerial"}
	if err := RunApp(ctx, cfg); err == nil || !strings.Contains(err.Error(), "shutdown down") {
		t.Errorf("RunApp() err = %v, want shutdown failure", err)
	}
}

var errShutdownTest = errors.New("shutdown down")
