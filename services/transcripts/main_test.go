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
	"github.com/azylman/aerial/pkg/pgutil"
	"github.com/azylman/aerial/pkg/pubsub"
)

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
func runAppConfig(c Config) *Config {
	c.Port, c.DatabaseURL, c.Stack = "0", "postgres://x", "aerial"
	return &c
}

func TestRunApp(t *testing.T) {
	tests := []struct {
		name        string
		cfg         *Config
		stubDB      bool
		openErr     error
		bus         busKind
		shutdownErr error
		canceled    bool
		wantErr     string
	}{
		{name: "nil config", wantErr: "config cannot be nil"},
		{name: "empty port", cfg: &Config{DatabaseURL: "postgres://x"}, wantErr: "port cannot be empty"},
		{name: "open db error", cfg: &Config{Port: "4017", DatabaseURL: "postgres://x"}, stubDB: true, openErr: errors.New("db down"), wantErr: "db down"},
		{name: "listen failure surfaces", cfg: &Config{Port: "99999", DatabaseURL: "postgres://x"}, stubDB: true, wantErr: "invalid port"},
		{name: "canceled context shuts down cleanly", cfg: runAppConfig(Config{}), stubDB: true, canceled: true},
		{name: "auth enabled shuts down cleanly", cfg: runAppConfig(Config{Auth: k8sauth.Config{Enabled: true}}), stubDB: true, canceled: true},
		{name: "bus close error is logged not returned", cfg: runAppConfig(Config{NATSURL: "nats://nats:4222"}), stubDB: true, bus: busCloseFail, canceled: true},
		{name: "shutdown error surfaces", cfg: runAppConfig(Config{}), stubDB: true, shutdownErr: errShutdownTest, canceled: true, wantErr: "shutdown down"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.stubDB {
				defer stubOpenDB(t, newTestStore(t, "aerial").db, tc.openErr)()
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
			clearConfigEnv(t)
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

func TestNewBusFnDials(t *testing.T) {
	clearConfigEnv(t)
	bus, err := newBusFn(context.Background(), "nats://127.0.0.1:1")
	if err != nil {
		t.Fatalf("newBusFn err = %v", err)
	}
	_ = bus.Close()
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
