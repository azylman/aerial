package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

	prepUngated, err := poolFresh.PrepareBurstTurn(context.Background(), "th-channel-once", "turn-once-2", []string{msgUngated.ID}, false)
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

func TestWorkerPool_BurstDrainedRows_RecoveredOnAbortReplayAndTerminalFail(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := testStore(t)

	// 1. Abort during attempt after mid-turn drain, followed by replay on a fresh WorkerPool.
	threadReplay := "th-drain-abort-replay"
	require.NoError(t, store.SaveSessionID(ctx, threadReplay, "11111111-1111-1111-1111-111111111111"))
	m1 := db.Message{ID: "m-rep-1", ThreadID: threadReplay, Content: "initial prompt", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(-2 * time.Second)}
	m2 := db.Message{ID: "m-rep-2", ThreadID: threadReplay, Content: "steered follow-up", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(-1 * time.Second)}
	seedMessageRows(t, store, []db.Message{m1, m2})
	require.NoError(t, recordTurnOwnedIDs(ctx, store, "turn-rep-1", []string{m2.ID}))

	var capturedPrompt string
	freshPool := NewWorkerPool(WorkerPoolConfig{
		Store:        store,
		DeliveryFunc: (&noticeRecorder{}).deliver,
		RunnerFunc: func(_ context.Context, _, prompt, _, _, _ string, _ int) (string, string, int, error) {
			capturedPrompt = prompt
			return "handled both", "", 0, nil
		},
	})

	// Simulate replay after a crash/abort: AttemptInput only carries the initial IDs ("m-rep-1")
	// and initial prompt, while "m-rep-2" was already claimed and tied to "turn-rep-1" before the crash.
	res, err := freshPool.RunBurstAttempt(ctx, AttemptInput{
		ThreadID: threadReplay,
		TurnID:   "turn-rep-1",
		IDs:      []string{m1.ID},
		Ordinal:  1,
		RunIndex: 1,
		Prep: TurnPrep{
			Decision:  TurnDecisionRun,
			SessionID: "11111111-1111-1111-1111-111111111111",
			Model:     "gemini-2.5-flash",
			Prompt:    "initial prompt",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, AttemptKindSuccess, res.Kind)
	assert.Equal(t, []string{m2.ID}, res.DrainedIDs, "replay must recover PROCESSING drained row tied to turnID")
	assert.Contains(t, capturedPrompt, "initial prompt")
	assert.Contains(t, capturedPrompt, "steered follow-up", "replay must reconstruct prompt with recovered drained message")

	require.NoError(t, freshPool.RecordBurstOutcome(ctx, []string{m1.ID, m2.ID}, AttemptSummary{Outcome: OutcomeCompleted}))
	assertMessageRows(t, store, map[string]rowWant{
		m1.ID: {status: db.StatusCompleted},
		m2.ID: {status: db.StatusCompleted},
	})

	// 2. Terminal failure and stop after mid-turn drain when live execution registration is gone.
	threadFail := "th-drain-terminal-fail"
	mf1 := db.Message{ID: "m-fail-1", ThreadID: threadFail, Content: "first", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(-2 * time.Second)}
	mf2 := db.Message{ID: "m-fail-2", ThreadID: threadFail, Content: "drained", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(-1 * time.Second)}
	seedMessageRows(t, store, []db.Message{mf1, mf2})
	require.NoError(t, recordTurnOwnedIDs(ctx, store, "turn-fail-1", []string{mf2.ID}))

	require.NoError(t, freshPool.FailBurstTurn(ctx, "turn-fail-1", []string{mf1.ID}, "turn failed"))
	assertMessageRows(t, store, map[string]rowWant{
		mf1.ID: {status: db.StatusFailed, errText: "turn failed"},
		mf2.ID: {status: db.StatusFailed, errText: "turn failed"},
	})

	threadStop := "th-drain-stop"
	ms1 := db.Message{ID: "m-stop-1", ThreadID: threadStop, Content: "first", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(-2 * time.Second)}
	ms2 := db.Message{ID: "m-stop-2", ThreadID: threadStop, Content: "drained", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(-1 * time.Second)}
	seedMessageRows(t, store, []db.Message{ms1, ms2})
	require.NoError(t, recordTurnOwnedIDs(ctx, store, "turn-stop-1", []string{ms2.ID}))

	require.NoError(t, freshPool.StopBurstTurn(ctx, "turn-stop-1", []string{ms1.ID}))
	assertMessageRows(t, store, map[string]rowWant{
		ms1.ID: {status: db.StatusFailed, errText: CancelledByOperatorReason},
		ms2.ID: {status: db.StatusFailed, errText: CancelledByOperatorReason},
	})

	// 3. Old turn and new turn on the same thread in both orderings: each changes only its own rows.
	threadIso := "th-turn-isolation"
	require.NoError(t, store.SaveSessionID(ctx, threadIso, "55555555-5555-5555-5555-555555555555"))
	mOld1 := db.Message{ID: "m-old-1", ThreadID: threadIso, Content: "old initial", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(-3 * time.Second)}
	mOld2 := db.Message{ID: "m-old-2", ThreadID: threadIso, Content: "old drained", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(-2 * time.Second)}
	mNew1 := db.Message{ID: "m-new-1", ThreadID: threadIso, Content: "new turn input", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(-1 * time.Second)}
	seedMessageRows(t, store, []db.Message{mOld1, mOld2, mNew1})
	require.NoError(t, recordTurnOwnedIDs(ctx, store, "turn-old", []string{mOld2.ID}))

	// Ordering A: new turn's attempt loads while old turn's rows are still PROCESSING -> new turn does NOT adopt old rows.
	resNew, err := freshPool.RunBurstAttempt(ctx, AttemptInput{
		ThreadID: threadIso,
		TurnID:   "turn-new",
		IDs:      []string{mNew1.ID},
		Ordinal:  1,
		RunIndex: 1,
		Prep:     TurnPrep{Decision: TurnDecisionRun, SessionID: "55555555-5555-5555-5555-555555555555", Model: "gemini-2.5-flash", Prompt: "new turn input"},
	})
	require.NoError(t, err)
	assert.Equal(t, AttemptKindSuccess, resNew.Kind)
	assert.Empty(t, resNew.DrainedIDs, "new turn must not adopt old turn's PROCESSING rows")
	assert.NotContains(t, capturedPrompt, "old drained")
	assertMessageRows(t, store, map[string]rowWant{
		mOld1.ID: {status: db.StatusProcessing},
		mOld2.ID: {status: db.StatusProcessing},
		mNew1.ID: {status: db.StatusCompleted},
	})

	// Ordering B: old turn's stop-turn runs while another turn's row (mNew2) is PROCESSING -> old turn does NOT fail mNew2.
	mNew2 := db.Message{ID: "m-new-2", ThreadID: threadIso, Content: "second new turn", Status: db.StatusProcessing, CreatedAt: time.Now().UTC()}
	seedMessageRows(t, store, []db.Message{mNew2})
	require.NoError(t, freshPool.StopBurstTurn(ctx, "turn-old", []string{mOld1.ID}))
	assertMessageRows(t, store, map[string]rowWant{
		mOld1.ID: {status: db.StatusFailed, errText: CancelledByOperatorReason},
		mOld2.ID: {status: db.StatusFailed, errText: CancelledByOperatorReason},
		mNew2.ID: {status: db.StatusProcessing},
	})
}

type faultGetDispatchStore struct {
	*db.SQLStore

	failID string
}

func (f *faultGetDispatchStore) GetDispatch(ctx context.Context, id string) (*db.Dispatch, error) {
	if f.failID != "" && id == f.failID {
		return nil, errors.New("simulated transient read failure on extra dispatch")
	}
	return f.SQLStore.GetDispatch(ctx, id)
}

func TestWorkerPool_DispatchDrainedRows_RecoveredOnAbortReplayTerminalFailAndTransientReadError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := testStore(t)

	sessReplay := "sess-drain-abort-replay"
	require.NoError(t, store.SaveSessionID(ctx, sessReplay, "22222222-2222-2222-2222-222222222222"))
	// Seed >50 older active rows belonging to other keys to verify recovery has no 50-row cutoff.
	bulkDispatches := make([]db.Dispatch, 0, 55)
	for i := range 55 {
		bulkDispatches = append(bulkDispatches, db.Dispatch{
			ID:        fmt.Sprintf("d-bulk-older-%02d", i),
			Source:    "vanadis",
			SessionID: fmt.Sprintf("sess-other-%02d", i),
			Prompt:    "older active dispatch",
			Status:    db.StatusProcessing,
			CreatedAt: time.Now().UTC().Add(-10 * time.Minute).Add(time.Duration(i) * time.Second),
		})
	}
	seedDispatchRows(t, store, bulkDispatches)

	d1 := db.Dispatch{ID: "d-rep-1", Source: "vanadis", SessionID: sessReplay, Prompt: "initial dispatch", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(-2 * time.Second)}
	d2 := db.Dispatch{ID: "d-rep-2", Source: "vanadis", SessionID: sessReplay, Prompt: "steered dispatch", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(-1 * time.Second)}
	seedDispatchRows(t, store, []db.Dispatch{d1, d2})
	require.NoError(t, recordTurnOwnedIDs(ctx, store, "turn-disp-rep-1", []string{d2.ID}))

	faultStore := &faultGetDispatchStore{SQLStore: store}
	var capturedPrompt string
	freshPool := NewWorkerPool(WorkerPoolConfig{
		Store:        faultStore,
		DeliveryFunc: (&noticeRecorder{}).deliver,
		RunnerFunc: func(_ context.Context, _, prompt, _, _, _ string, _ int) (string, string, int, error) {
			capturedPrompt = prompt
			return "dispatch complete", "", 0, nil
		},
	})

	// Replay with only d1 in IDs recovers d2 (even with >50 older active rows) and includes its prompt once.
	res, err := freshPool.RunDispatchAttempt(ctx, AttemptInput{
		SessionKey: sessReplay,
		TurnID:     "turn-disp-rep-1",
		IDs:        []string{d1.ID},
		Ordinal:    1,
		RunIndex:   1,
		Prep: TurnPrep{
			Decision:  TurnDecisionRun,
			SessionID: "22222222-2222-2222-2222-222222222222",
			Model:     "gemini-2.5-flash",
			Prompt:    "initial dispatch",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, AttemptKindSuccess, res.Kind)
	assert.Equal(t, []string{d2.ID}, res.DrainedIDs)
	assert.Contains(t, capturedPrompt, "initial dispatch")
	assert.Equal(t, 1, strings.Count(capturedPrompt, "steered dispatch"))

	require.NoError(t, freshPool.RecordDispatchOutcome(ctx, []string{d1.ID, d2.ID}, AttemptSummary{Outcome: OutcomeCompleted}))
	assertDispatchRows(t, store, map[string]rowWant{
		d1.ID: {status: db.StatusCompleted},
		d2.ID: {status: db.StatusCompleted},
	})

	// Terminal failure and stop recover drained PROCESSING dispatch rows tied to turnID, while isolating other turns on the same session.
	sessFail := "sess-drain-fail"
	df1 := db.Dispatch{ID: "d-fail-1", Source: "vanadis", SessionID: sessFail, Prompt: "p1", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(-3 * time.Second)}
	df2 := db.Dispatch{ID: "d-fail-2", Source: "vanadis", SessionID: sessFail, Prompt: "p2", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(-2 * time.Second)}
	dfOtherTurn := db.Dispatch{ID: "d-fail-other-turn", Source: "vanadis", SessionID: sessFail, Prompt: "other turn", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(-1 * time.Second)}
	seedDispatchRows(t, store, []db.Dispatch{df1, df2, dfOtherTurn})
	require.NoError(t, recordTurnOwnedIDs(ctx, store, "turn-disp-fail", []string{df2.ID}))

	require.NoError(t, freshPool.FailDispatchTurn(ctx, "turn-disp-fail", []string{df1.ID}, "dispatch failed"))
	assertDispatchRows(t, store, map[string]rowWant{
		df1.ID:         {status: db.StatusFailed, errText: "dispatch failed"},
		df2.ID:         {status: db.StatusFailed, errText: "dispatch failed"},
		dfOtherTurn.ID: {status: db.StatusProcessing},
	})

	sessStop := "sess-drain-stop"
	ds1 := db.Dispatch{ID: "d-stop-1", Source: "vanadis", SessionID: sessStop, Prompt: "p1", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(-2 * time.Second)}
	ds2 := db.Dispatch{ID: "d-stop-2", Source: "vanadis", SessionID: sessStop, Prompt: "p2", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(-1 * time.Second)}
	seedDispatchRows(t, store, []db.Dispatch{ds1, ds2})
	require.NoError(t, recordTurnOwnedIDs(ctx, store, "turn-disp-stop", []string{ds2.ID}))

	require.NoError(t, freshPool.StopDispatchTurn(ctx, "turn-disp-stop", []string{ds1.ID}))
	assertDispatchRows(t, store, map[string]rowWant{
		ds1.ID: {status: db.StatusFailed, errText: CancelledByOperatorReason},
		ds2.ID: {status: db.StatusFailed, errText: CancelledByOperatorReason},
	})

	// Transient GetDispatch failure on an extra/drained ID during retry must surface an error rather than skipping the row,
	// and two retries after a drain must include the drained prompt exactly once on each attempt.
	sessTransient := "sess-drain-transient"
	require.NoError(t, store.SaveSessionID(ctx, sessTransient, "33333333-3333-3333-3333-333333333333"))
	dt1 := db.Dispatch{ID: "d-trans-1", Source: "vanadis", SessionID: sessTransient, Prompt: "first prompt", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(-2 * time.Second)}
	dt2 := db.Dispatch{ID: "d-trans-2", Source: "vanadis", SessionID: sessTransient, Prompt: "second prompt", Status: db.StatusProcessing, CreatedAt: time.Now().UTC().Add(-1 * time.Second)}
	seedDispatchRows(t, store, []db.Dispatch{dt1, dt2})
	require.NoError(t, recordTurnOwnedIDs(ctx, store, "turn-disp-trans", []string{dt2.ID}))

	faultStore.failID = dt2.ID
	_, err = freshPool.RunDispatchAttempt(ctx, AttemptInput{
		SessionKey: sessTransient,
		TurnID:     "turn-disp-trans",
		IDs:        []string{dt1.ID, dt2.ID},
		Ordinal:    2,
		RunIndex:   2,
		Prep: TurnPrep{
			Decision:  TurnDecisionRun,
			SessionID: "33333333-3333-3333-3333-333333333333",
			Model:     "gemini-2.5-flash",
			Prompt:    "first prompt\n\nsecond prompt",
		},
	})
	require.Error(t, err, "transient GetDispatch failure on extra dispatch ID must fail the step for retry")
	assert.Contains(t, err.Error(), "simulated transient read failure on extra dispatch")

	// Clear the transient fault and run retry 1 and retry 2: verify second prompt appears exactly once each time.
	faultStore.failID = ""
	for _, ord := range []int{2, 3} {
		resRetry, rErr := freshPool.RunDispatchAttempt(ctx, AttemptInput{
			SessionKey: sessTransient,
			TurnID:     "turn-disp-trans",
			IDs:        []string{dt1.ID, dt2.ID},
			Ordinal:    ord,
			RunIndex:   ord,
			Prep: TurnPrep{
				Decision:  TurnDecisionRun,
				SessionID: "33333333-3333-3333-3333-333333333333",
				Model:     "gemini-2.5-flash",
				Prompt:    "first prompt\n\nsecond prompt",
			},
		})
		require.NoError(t, rErr)
		assert.Equal(t, AttemptKindSuccess, resRetry.Kind)
		assert.Empty(t, resRetry.DrainedPrompts, "already-known drained dispatch must not be re-reported in DrainedPrompts on retry")
		assert.Equal(t, 1, strings.Count(capturedPrompt, "second prompt"), "drained dispatch prompt must appear exactly once on retry %d", ord)
		// Reset statuses to PROCESSING before ordinal 3 so we can test the second retry.
		if ord == 2 {
			require.NoError(t, store.UpdateDispatchStatus(ctx, dt1.ID, db.StatusProcessing, ""))
			require.NoError(t, store.UpdateDispatchStatus(ctx, dt2.ID, db.StatusProcessing, ""))
		}
	}
	require.NoError(t, freshPool.RecordDispatchOutcome(ctx, []string{dt1.ID, dt2.ID}, AttemptSummary{Outcome: OutcomeCompleted}))
	assertDispatchRows(t, store, map[string]rowWant{
		dt1.ID: {status: db.StatusCompleted},
		dt2.ID: {status: db.StatusCompleted},
	})
}
