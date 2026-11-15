package queue

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/eri/pkg/db"
)

type trackingBus struct {
	pubsub.Bus

	mu        sync.Mutex
	publishes []string
}

func (t *trackingBus) Publish(ctx context.Context, subject, msgID string, payload any) error {
	t.mu.Lock()
	t.publishes = append(t.publishes, subject)
	t.mu.Unlock()
	return t.Bus.Publish(ctx, subject, msgID, payload)
}

// deliveredOutcome is the observable projection of a dispatch outcome event
// received by a bus subscriber.
type deliveredOutcome struct {
	subject, dispatchID, chunkID, source, status string
}

// outcomeRecorder subscribes to dispatch outcome subjects and records every
// delivered event so tests can wait on deliveries without sleeping.
type outcomeRecorder struct {
	mu        sync.Mutex
	cond      *sync.Cond
	delivered []deliveredOutcome
}

func newOutcomeRecorder(t *testing.T, bus pubsub.Bus) *outcomeRecorder {
	t.Helper()
	r := &outcomeRecorder{}
	r.cond = sync.NewCond(&r.mu)
	_, err := bus.Subscribe(context.Background(), "aerial.dispatch.*", "test-sub", func(_ context.Context, msg pubsub.Message) error {
		var ev pubsub.DispatchOutcomeEvent
		if uErr := json.Unmarshal(msg.Data(), &ev); uErr == nil {
			r.mu.Lock()
			r.delivered = append(r.delivered, deliveredOutcome{
				subject: msg.Subject(), dispatchID: ev.DispatchID, chunkID: ev.VanadisChunkID, source: ev.Source, status: ev.Status,
			})
			r.cond.Broadcast()
			r.mu.Unlock()
		}
		return msg.Ack()
	})
	if err != nil {
		t.Fatalf("failed to subscribe to memory bus: %v", err)
	}
	return r
}

// waitFor blocks until at least n outcomes were delivered, then returns a snapshot.
func (r *outcomeRecorder) waitFor(n int) []deliveredOutcome {
	r.mu.Lock()
	defer r.mu.Unlock()
	for len(r.delivered) < n {
		r.cond.Wait()
	}
	return append([]deliveredOutcome(nil), r.delivered...)
}

// dispatchPublishes returns the dispatch-outcome subjects the bus was asked to publish.
func (t *trackingBus) dispatchPublishes() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var got []string
	for _, subject := range t.publishes {
		if strings.HasPrefix(subject, pubsub.SubjectAerialDispatchPrefix) {
			got = append(got, subject)
		}
	}
	return got
}

// outcomeDispatch builds a pending dispatch whose prompt matches its source.
func outcomeDispatch(id, source, chunkID string) db.Dispatch {
	prompt := "heartbeat prompt"
	if source == "vanadis" {
		prompt = "=== Active Chunk (Chunk 1 of 1) ===\nBuild ID: build-test\nChunk ID: " + chunkID
	}
	now := time.Now().UTC()
	return db.Dispatch{
		ID: id, Source: source, SessionID: source + "-" + id, TargetID: chunkID, VanadisChunkID: chunkID,
		Prompt: prompt, Status: db.StatusPending, CreatedAt: now, UpdatedAt: now,
	}
}

func assertAttemptedPublishes(t *testing.T, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("attempted dispatch publishes = %v, want %v", got, want)
	}
}

func assertDeliveredOutcomes(t *testing.T, got, want []deliveredOutcome) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("delivered outcomes = %+v, want %+v", got, want)
	}
}

func TestWorkerPool_PublishDispatchOutcome_JetStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		turnStatus    string
		turnError     string
		chunkID       string
		dispatchID    string
		source        string
		wantPublishes []string
		wantDelivered []deliveredOutcome
	}{
		{
			name:          "jetstream publish success on completed status",
			turnStatus:    "success",
			chunkID:       "chunk-js-success",
			dispatchID:    "disp-js-success",
			source:        "vanadis",
			wantPublishes: []string{"aerial.dispatch.completed"},
			wantDelivered: []deliveredOutcome{{"aerial.dispatch.completed", "disp-js-success", "chunk-js-success", "vanadis", "completed"}},
		},
		{
			name:          "jetstream publish failed status routes to failed topic",
			turnStatus:    "failed",
			turnError:     "build error",
			chunkID:       "chunk-js-fail",
			dispatchID:    "disp-js-fail",
			source:        "vanadis",
			wantPublishes: []string{"aerial.dispatch.failed"},
			wantDelivered: []deliveredOutcome{{"aerial.dispatch.failed", "disp-js-fail", "chunk-js-fail", "vanadis", "failed"}},
		},
		{
			name:          "heartbeat dispatch outcome publishes to jetstream",
			turnStatus:    "success",
			dispatchID:    "disp-hb-success",
			source:        "heartbeat",
			wantPublishes: []string{"aerial.dispatch.completed"},
			wantDelivered: []deliveredOutcome{{"aerial.dispatch.completed", "disp-hb-success", "", "heartbeat", "completed"}},
		},
		{
			name:       "empty turn status does not publish",
			chunkID:    "chunk-empty-status",
			dispatchID: "disp-empty-status",
			source:     "vanadis",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			memBus := pubsub.NewMemoryBus()
			defer memBus.Close()
			trackBus := &trackingBus{Bus: memBus}
			recorder := newOutcomeRecorder(t, memBus)

			poolCtx, cancelPool := context.WithCancel(context.Background())
			defer cancelPool()
			pool := &WorkerPool{ctx: poolCtx, cancel: cancelPool, eventBus: trackBus, cfg: WorkerPoolConfig{EventBus: trackBus}}

			disp := outcomeDispatch(tc.dispatchID, tc.source, tc.chunkID)
			te := &turnExecution{
				pool:        pool,
				dispatch:    &disp,
				threadID:    "dispatch-" + disp.ID,
				triggerType: "dispatch:" + tc.source,
				execStart:   time.Now().UTC().Add(-100 * time.Millisecond),
				turnStatus:  tc.turnStatus,
				turnError:   tc.turnError,
				turnCtx:     poolCtx,
			}

			cleanup := pool.setupDispatchTurn(disp, te)
			cleanup()

			delivered := recorder.waitFor(len(tc.wantDelivered))
			assertAttemptedPublishes(t, trackBus.dispatchPublishes(), tc.wantPublishes)
			assertDeliveredOutcomes(t, delivered, tc.wantDelivered)
		})
	}
}
