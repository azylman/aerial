package queue

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/runner"
)

func TestWorkerPool_LowEffortModelResolution(t *testing.T) {
	t.Parallel()

	// 1. Nil pool fallback
	var nilPool *WorkerPool
	if got := nilPool.LowEffortModel(); got != config.DefaultConfigData().LowEffortModel {
		t.Errorf("expected default LowEffortModel, got %q", got)
	}

	// 2. Pool with custom LowEffortModel in cfg
	p1 := New(nil, WorkerPoolConfig{
		LowEffortModel: "gemini-flash-low-test",
	})
	if got := p1.LowEffortModel(); got != "gemini-flash-low-test" {
		t.Errorf("expected gemini-flash-low-test, got %q", got)
	}

	// 3. Pool with appCfg override
	appCfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.LowEffortModel = "app-cfg-low-model"
	})
	p2 := New(appCfg, WorkerPoolConfig{})
	if got := p2.LowEffortModel(); got != "app-cfg-low-model" {
		t.Errorf("expected app-cfg-low-model, got %q", got)
	}
}

func TestWorkerPool_ExecuteVoiceTurn_CustomRunner(t *testing.T) {
	t.Parallel()

	var recordedModel string
	var recordedPrompt string
	var recordedSession string

	pool := New(nil, WorkerPoolConfig{
		Store: db.NewFakeStore(),
		LowEffortModel: "low-effort-fast-model",
		RunnerWithOptionsFunc: func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts runner.WatchdogOptions) (string, string, int, error) {
			recordedModel = model
			recordedPrompt = prompt
			recordedSession = sessionID
			return `{"event":"result","result":{"status":"SUCCESS","response":"turned off fan"}}`, "", 0, nil
		},
	})
	pool.Start()
	defer pool.Stop()

	var statusReceived string
	reply, convID, err := pool.ExecuteVoiceTurn(context.Background(), "turn off the fan", "sess-custom-1", func(status string) {
		statusReceived = status
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reply != "turned off fan" {
		t.Errorf("expected reply 'turned off fan', got %q", reply)
	}
	if convID != "sess-custom-1" {
		t.Errorf("expected convID 'sess-custom-1', got %q", convID)
	}
	if recordedModel != "low-effort-fast-model" {
		t.Errorf("expected low-effort-fast-model, got %q", recordedModel)
	}
	if recordedPrompt != "turn off the fan" {
		t.Errorf("expected prompt 'turn off the fan', got %q", recordedPrompt)
	}
	if recordedSession != "sess-custom-1" {
		t.Errorf("expected session 'sess-custom-1', got %q", recordedSession)
	}
	if !strings.Contains(statusReceived, "⚡") {
		t.Errorf("expected status notification, got %q", statusReceived)
	}
}

func TestWorkerPool_ExecuteVoiceTurn_VoiceRunnerHook(t *testing.T) {
	t.Parallel()

	pool := New(nil, WorkerPoolConfig{
		VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
			if onStatus != nil {
				onStatus("⚡ Custom hook running...")
			}
			return "voice hook reply", sessionID, nil
		},
	})
	pool.Start()
	defer pool.Stop()

	var statusCalled string
	reply, convID, err := pool.ExecuteVoiceTurn(context.Background(), "hello", "sess-hook-1", func(s string) {
		statusCalled = s
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reply != "voice hook reply" || convID != "sess-hook-1" {
		t.Errorf("unexpected result: reply=%q convID=%q", reply, convID)
	}
	if statusCalled != "⚡ Custom hook running..." {
		t.Errorf("unexpected status: %q", statusCalled)
	}
}

func TestWorkerPool_ExecuteVoiceTurn_Errors(t *testing.T) {
	t.Parallel()

	// 1. Nil pool
	var nilPool *WorkerPool
	_, _, err := nilPool.ExecuteVoiceTurn(context.Background(), "test", "", nil)
	if err == nil || !strings.Contains(err.Error(), "uninitialized") {
		t.Errorf("expected uninitialized error, got: %v", err)
	}

	// 2. Stopped pool
	poolStopped := New(nil, WorkerPoolConfig{
		VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
			return "reply", sessionID, nil
		},
	})
	poolStopped.Start()
	poolStopped.Stop()

	_, _, err = poolStopped.ExecuteVoiceTurn(context.Background(), "test", "", nil)
	if err == nil || !strings.Contains(err.Error(), "shutting down") {
		t.Errorf("expected shutting down error, got: %v", err)
	}

	// 3. Cancelled context
	poolActive := New(nil, WorkerPoolConfig{
		VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
			return "reply", sessionID, nil
		},
	})
	poolActive.Start()
	defer poolActive.Stop()

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err = poolActive.ExecuteVoiceTurn(cancelCtx, "test", "", nil)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got: %v", err)
	}
}
