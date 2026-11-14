package queue

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/eri/pkg/db"
)

type trackingBus struct {
	pubsub.Bus

	mu         sync.Mutex
	publishes  []string
	publishErr error
}

func (t *trackingBus) Publish(ctx context.Context, subject, msgID string, payload any) error {
	t.mu.Lock()
	t.publishes = append(t.publishes, subject)
	err := t.publishErr
	t.mu.Unlock()
	if err != nil {
		return err
	}
	return t.Bus.Publish(ctx, subject, msgID, payload)
}

func TestWorkerPool_PublishDispatchOutcome_JetStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		turnStatus      string
		turnError       string
		chunkID         string
		dispatchID      string
		source          string
		wantPubTopic    string
		wantAttemptPubs int
		wantDeliverPubs int
	}{
		{
			name:            "jetstream publish success on completed status",
			turnStatus:      "success",
			turnError:       "",
			chunkID:         "chunk-js-success",
			dispatchID:      "disp-js-success",
			source:          "vanadis",
			wantPubTopic:    "aerial.dispatch.completed",
			wantAttemptPubs: 1,
			wantDeliverPubs: 1,
		},
		{
			name:            "jetstream publish failed status routes to failed topic",
			turnStatus:      "failed",
			turnError:       "build error",
			chunkID:         "chunk-js-fail",
			dispatchID:      "disp-js-fail",
			source:          "vanadis",
			wantPubTopic:    "aerial.dispatch.failed",
			wantAttemptPubs: 1,
			wantDeliverPubs: 1,
		},
		{
			name:            "heartbeat dispatch outcome publishes to jetstream",
			turnStatus:      "success",
			turnError:       "",
			chunkID:         "",
			dispatchID:      "disp-hb-success",
			source:          "heartbeat",
			wantPubTopic:    "aerial.dispatch.completed",
			wantAttemptPubs: 1,
			wantDeliverPubs: 1,
		},
		{
			name:            "empty turn status does not publish",
			turnStatus:      "",
			turnError:       "",
			chunkID:         "chunk-empty-status",
			dispatchID:      "disp-empty-status",
			source:          "vanadis",
			wantAttemptPubs: 0,
			wantDeliverPubs: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			memBus := pubsub.NewMemoryBus()
			defer memBus.Close()

			trackBus := &trackingBus{
				Bus: memBus,
			}

			type deliveredMessage struct {
				subject string
				event   pubsub.DispatchOutcomeEvent
			}
			var deliveredMessages []deliveredMessage
			var deliveredMu sync.Mutex
			deliveredCond := sync.NewCond(&deliveredMu)

			_, err := memBus.Subscribe(context.Background(), "aerial.dispatch.*", "test-sub", func(msgCtx context.Context, msg pubsub.Message) error {
				var ev pubsub.DispatchOutcomeEvent
				if uErr := json.Unmarshal(msg.Data(), &ev); uErr == nil {
					deliveredMu.Lock()
					deliveredMessages = append(deliveredMessages, deliveredMessage{
						subject: msg.Subject(),
						event:   ev,
					})
					deliveredCond.Broadcast()
					deliveredMu.Unlock()
				}
				return msg.Ack()
			})
			if err != nil {
				t.Fatalf("failed to subscribe to memory bus: %v", err)
			}

			poolCtx, cancelPool := context.WithCancel(context.Background())
			defer cancelPool()

			pool := &WorkerPool{
				ctx:      poolCtx,
				cancel:   cancelPool,
				eventBus: trackBus,
				cfg: WorkerPoolConfig{
					EventBus: trackBus,
				},
			}

			src := tc.source
			if src == "" {
				src = "vanadis"
			}
			prompt := "=== Active Chunk (Chunk 1 of 1) ===\nBuild ID: build-test\nChunk ID: " + tc.chunkID
			if src != "vanadis" {
				prompt = "heartbeat prompt"
			}

			disp := db.Dispatch{
				ID:             tc.dispatchID,
				Source:         src,
				SessionID:      src + "-" + tc.dispatchID,
				TargetID:       tc.chunkID,
				VanadisChunkID: tc.chunkID,
				Prompt:         prompt,
				Status:         db.StatusPending,
				CreatedAt:      time.Now().UTC(),
				UpdatedAt:      time.Now().UTC(),
			}

			te := &turnExecution{
				pool:        pool,
				dispatch:    &disp,
				threadID:    "dispatch-" + disp.ID,
				triggerType: "dispatch:" + src,
				execStart:   time.Now().UTC().Add(-100 * time.Millisecond),
				turnStatus:  tc.turnStatus,
				turnError:   tc.turnError,
				turnCtx:     poolCtx,
			}

			cleanup := pool.setupDispatchTurn(disp, te)
			cleanup()

			if tc.wantDeliverPubs > 0 {
				deliveredMu.Lock()
				for len(deliveredMessages) < tc.wantDeliverPubs {
					deliveredCond.Wait()
				}
				deliveredMu.Unlock()
			}

			// Check attempted publishes
			trackBus.mu.Lock()
			gotPublishes := append([]string(nil), trackBus.publishes...)
			trackBus.mu.Unlock()

			if len(gotPublishes) != tc.wantAttemptPubs {
				t.Fatalf("attempted publish count = %d, want %d (subjects: %v)", len(gotPublishes), tc.wantAttemptPubs, gotPublishes)
			}
			if len(gotPublishes) > 0 && tc.wantPubTopic != "" && gotPublishes[0] != tc.wantPubTopic {
				t.Errorf("got published subject %q, want %q", gotPublishes[0], tc.wantPubTopic)
			}

			// Check delivered messages
			deliveredMu.Lock()
			gotDelivered := append([]deliveredMessage(nil), deliveredMessages...)
			deliveredMu.Unlock()

			if len(gotDelivered) != tc.wantDeliverPubs {
				t.Fatalf("delivered message count = %d, want %d", len(gotDelivered), tc.wantDeliverPubs)
			}
			if tc.wantDeliverPubs > 0 {
				dm := gotDelivered[0]
				if tc.wantPubTopic != "" && dm.subject != tc.wantPubTopic {
					t.Errorf("got delivered subject %q, want %q", dm.subject, tc.wantPubTopic)
				}
				ev := dm.event
				if ev.DispatchID != tc.dispatchID {
					t.Errorf("ev.DispatchID = %q, want %q", ev.DispatchID, tc.dispatchID)
				}
				if ev.VanadisChunkID != tc.chunkID {
					t.Errorf("ev.VanadisChunkID = %q, want %q", ev.VanadisChunkID, tc.chunkID)
				}
				if ev.Source != src {
					t.Errorf("ev.Source = %q, want %q", ev.Source, src)
				}
				wantStatusNorm := tc.turnStatus
				if wantStatusNorm == "success" {
					wantStatusNorm = "completed"
				}
				if ev.Status != wantStatusNorm {
					t.Errorf("ev.Status = %q, want %q", ev.Status, wantStatusNorm)
				}
			}
		})
	}
}
