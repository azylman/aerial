package queue

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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

func TestWorkerPool_PublishDispatchOutcome_JetStreamAndFallback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		busMode         string // "success", "error"
		turnStatus      string
		turnError       string
		chunkID         string
		dispatchID      string
		source          string
		wantPubTopic    string
		wantAttemptPubs int
		wantDeliverPubs int
		wantHTTPCall    bool
		wantHTTPEnd     string
	}{
		{
			name:            "jetstream publish success does not invoke http fallback",
			busMode:         "success",
			turnStatus:      "success",
			turnError:       "",
			chunkID:         "chunk-js-success",
			dispatchID:      "disp-js-success",
			source:          "vanadis",
			wantPubTopic:    "aerial.dispatch.completed",
			wantAttemptPubs: 1,
			wantDeliverPubs: 1,
			wantHTTPCall:    false,
		},
		{
			name:            "jetstream publish failed status routes to failed topic",
			busMode:         "success",
			turnStatus:      "failed",
			turnError:       "build error",
			chunkID:         "chunk-js-fail",
			dispatchID:      "disp-js-fail",
			source:          "vanadis",
			wantPubTopic:    "aerial.dispatch.failed",
			wantAttemptPubs: 1,
			wantDeliverPubs: 1,
			wantHTTPCall:    false,
		},
		{
			name:            "heartbeat dispatch outcome publishes to jetstream without http fallback",
			busMode:         "success",
			turnStatus:      "success",
			turnError:       "",
			chunkID:         "",
			dispatchID:      "disp-hb-success",
			source:          "heartbeat",
			wantPubTopic:    "aerial.dispatch.completed",
			wantAttemptPubs: 1,
			wantDeliverPubs: 1,
			wantHTTPCall:    false,
		},
		{
			name:            "empty turn status does not publish completed or invoke http fallback",
			busMode:         "success",
			turnStatus:      "",
			turnError:       "",
			chunkID:         "chunk-empty-status",
			dispatchID:      "disp-empty-status",
			source:          "vanadis",
			wantAttemptPubs: 0,
			wantDeliverPubs: 0,
			wantHTTPCall:    false,
		},
		{
			name:            "bus publish error invokes legacy http fallback",
			busMode:         "error",
			turnStatus:      "failed",
			turnError:       "fatal runner crash",
			chunkID:         "chunk-bus-err",
			dispatchID:      "disp-bus-err",
			source:          "vanadis",
			wantPubTopic:    "aerial.dispatch.failed",
			wantAttemptPubs: 1,
			wantDeliverPubs: 0,
			wantHTTPCall:    true,
			wantHTTPEnd:     "/chunks/fail",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var httpCallsMu sync.Mutex
			var httpEndpoints []string

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				httpCallsMu.Lock()
				httpEndpoints = append(httpEndpoints, r.URL.Path)
				httpCallsMu.Unlock()
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			memBus := pubsub.NewMemoryBus()
			defer memBus.Close()

			var publishErr error
			if tc.busMode == "error" {
				publishErr = errors.New("simulated nats publish failure")
			}
			trackBus := &trackingBus{
				Bus:        memBus,
				publishErr: publishErr,
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
					VanadisURL: srv.URL,
					EventBus:   trackBus,
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

			// Check HTTP calls
			httpCallsMu.Lock()
			callsCount := len(httpEndpoints)
			var calledPath string
			if callsCount > 0 {
				calledPath = httpEndpoints[0]
			}
			httpCallsMu.Unlock()

			if tc.wantHTTPCall {
				if callsCount != 1 {
					t.Fatalf("expected 1 HTTP fallback call, got %d", callsCount)
				}
				if calledPath != tc.wantHTTPEnd {
					t.Fatalf("expected HTTP fallback endpoint %q, got %q", tc.wantHTTPEnd, calledPath)
				}
			} else if callsCount != 0 {
				t.Fatalf("expected 0 HTTP fallback calls (no dual-fire), got %d (%v)", callsCount, httpEndpoints)
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
