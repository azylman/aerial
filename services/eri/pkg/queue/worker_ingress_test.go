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
	"github.com/azylman/aerial/services/eri/pkg/session"
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

	makePool := func() *WorkerPool {
		return NewWorkerPool(WorkerPoolConfig{
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
	}

	pool := makePool()

	msg := db.Message{
		ID:        "msg-once-1",
		ThreadID:  "th-channel-once",
		Content:   "please help me with this task",
		Status:    db.StatusProcessing,
		CreatedAt: time.Now().UTC(),
	}

	// 1. Evaluate at ingress: returns true, ""
	should, reason := pool.EvaluateIngressWake(context.Background(), "th-channel-once", msg)
	require.True(t, should)
	require.Empty(t, reason)
	require.Equal(t, 1, classifierCallCount)

	// Ingress gating passes gated=true to PrepareBurstTurn via BurstTurnRequest
	seedMessageRows(t, store, []db.Message{msg})

	// 2. PrepareBurstTurn using a fresh WorkerPool (between gate and prep): prep honors
	// the journaled gated=true decision and does NOT call classifier again.
	poolFresh := makePool()
	prep, err := poolFresh.PrepareBurstTurn(context.Background(), "th-channel-once", "turn-once-1", []string{msg.ID}, true)
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

	prepUngated, err := poolFresh.PrepareBurstTurn(context.Background(), "th-channel-once", "turn-once-2", []string{msgUngated.ID})
	require.NoError(t, err)
	assert.Equal(t, TurnDecisionFinished, prepUngated.Decision)
	assert.Equal(t, "ambient", prepUngated.Reason)
	assert.Equal(t, 2, classifierCallCount, "ungated message must be evaluated in prep")
}

func TestWorkerPool_GatedMessage_ResetsMissingSession(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	th := 0.75
	procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, newMockDaemonSpawner())
	defer procPool.Close()

	tmpHome := t.TempDir()
	sessMgr := session.New(tmpHome, tmpHome)

	pool := NewWorkerPool(WorkerPoolConfig{
		Store:          store,
		ProcessPool:    procPool,
		SessionManager: sessMgr,
		ResolveChannelPolicy: func(channelID, channelName string) config.ChannelPolicy {
			return config.ChannelPolicy{
				Mode:                 "channel",
				WakeMode:             "classifier",
				AmbientWakeThreshold: &th,
			}
		},
	})

	threadID := "th-ghost-channel"
	require.NoError(t, store.SaveSessionID(context.Background(), threadID, "stale-ghost-uuid"))

	msg := db.Message{
		ID:        "msg-ghost-1",
		ThreadID:  threadID,
		Content:   "hey wake up",
		Status:    db.StatusProcessing,
		CreatedAt: time.Now().UTC(),
	}
	seedMessageRows(t, store, []db.Message{msg})

	prep, err := pool.PrepareBurstTurn(context.Background(), threadID, "turn-ghost-1", []string{msg.ID}, true)
	require.NoError(t, err)
	assert.Equal(t, TurnDecisionRun, prep.Decision)

	// Verify stale session ID missing from disk was reset (not carried into prep)
	sessID, err := store.GetSessionID(context.Background(), threadID)
	require.NoError(t, err)
	assert.NotEqual(t, "stale-ghost-uuid", sessID, "stale session ID missing from disk must be reset for fresh turn 1")
	assert.NotEqual(t, "stale-ghost-uuid", prep.SessionID, "prep must not carry stale ghost session ID")
}

func TestWorkerPool_GatedMessage_PreTurnDeferral_RetainsGatedDecisionOnRescheduledTurn(t *testing.T) {
	t.Parallel()

	store := testStore(t)
	th := 0.75
	classifierCallCount := 0
	cls := classifier.New(nil)
	cls.LLMFunc = func(ctx context.Context, model, prompt string) (string, error) {
		classifierCallCount++
		// If classifier were called during prep, it would return ambient and reject
		return `{"confidence": 0.10, "reason": "ambient reject"}`, nil
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

	threadID := "th-channel-defer-test"
	msg := db.Message{
		ID:           "msg-defer-1",
		ThreadID:     threadID,
		Content:      "please do this later",
		Status:       db.StatusPending,
		ErrorMessage: preTurnDeferredReason, // pre_turn deferral requeued the row
		CreatedAt:    time.Now().UTC(),
	}
	seedMessageRows(t, store, []db.Message{msg})

	// When Kick fires and starts the rescheduled turn, it claims rows to PROCESSING
	require.NoError(t, store.UpdateMessageStatus(context.Background(), msg.ID, db.StatusProcessing, preTurnDeferredReason))

	// Rescheduled turn executes PrepareBurstTurn with gated=true (journaled on BurstTurnRequest)
	prep, err := pool.PrepareBurstTurn(context.Background(), threadID, "turn-rescheduled-1", []string{msg.ID}, true)
	require.NoError(t, err)
	assert.Equal(t, TurnDecisionRun, prep.Decision, "rescheduled gated turn must proceed with run even if classifier would reject")
	assert.Equal(t, 0, classifierCallCount, "gated turn must not call classifier during prep")
}
