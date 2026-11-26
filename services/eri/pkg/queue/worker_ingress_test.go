package queue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/services/eri/pkg/classifier"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

func TestWorkerPool_AcceptedMessageEvaluatedOnceAndNotDroppedAsAmbientInPrep(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	th := 0.75
	classifierCallCount := 0
	cls := classifier.New(nil)
	cls.LLMFunc = func(ctx context.Context, model, prompt string) (string, error) {
		classifierCallCount++
		if classifierCallCount == 1 {
			// First call (during ingress wake evaluation): wake message
			return `{"confidence": 0.95, "reason": "user ask"}`, nil
		}
		// Subsequent calls (if prep were to re-classify): would return ambient
		return `{"confidence": 0.10, "reason": "ambient"}`, nil
	}

	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, newMockDaemonSpawner())
	defer procPool.Close()

	pool := NewWorkerPool(WorkerPoolConfig{
		Store:       store,
		ProcessPool: procPool,
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode:                 "channel",
				WakeMode:             "classifier",
				AmbientWakeThreshold: &th,
			}
		},
		Classifier: cls,
	})

	msg := db.Message{
		ID:        "msg-once-1",
		ThreadID:  "th-channel-once",
		Content:   "please help me with this task",
		Status:    db.StatusProcessing,
		CreatedAt: time.Now().UTC(),
	}
	seedMessageRows(t, store, []db.Message{msg})

	// 1. Evaluate at ingress: returns true, "" and marks message as ingress gated
	should, reason := pool.EvaluateIngressWake(context.Background(), "th-channel-once", msg)
	require.True(t, should)
	require.Empty(t, reason)
	require.Equal(t, 1, classifierCallCount)

	// 2. PrepareBurstTurn for this accepted message: prep honors the ingress decision
	// and does NOT call classifier again nor drop it as ambient.
	prep, err := pool.PrepareBurstTurn(context.Background(), "th-channel-once", "turn-once-1", []string{msg.ID})
	require.NoError(t, err)
	assert.Equal(t, TurnDecisionRun, prep.Decision)
	assert.Equal(t, 1, classifierCallCount, "classifier must be called exactly once; prep must not re-evaluate")

	// 3. Contrast with a message that skipped ingress gating: prep evaluates it and drops it as ambient
	msgUngated := db.Message{
		ID:        "msg-ungated-2",
		ThreadID:  "th-channel-once",
		Content:   "some chat message",
		Status:    db.StatusProcessing,
		CreatedAt: time.Now().UTC(),
	}
	seedMessageRows(t, store, []db.Message{msgUngated})

	prepUngated, err := pool.PrepareBurstTurn(context.Background(), "th-channel-once", "turn-once-2", []string{msgUngated.ID})
	require.NoError(t, err)
	assert.Equal(t, TurnDecisionFinished, prepUngated.Decision)
	assert.Equal(t, "ambient", prepUngated.Reason)
	assert.Equal(t, 2, classifierCallCount, "ungated message must be evaluated in prep")
}
