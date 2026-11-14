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

type errPublishBus struct {
	pubsub.Bus

	err error
}

func (e *errPublishBus) Publish(ctx context.Context, subject, msgID string, payload any) error {
	return e.err
}

func TestWorkerPool_PublishDispatchOutcome_JetStreamAndFallback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		busMode      string // "success", "nil", "error"
		turnStatus   string
		turnError    string
		chunkID      string
		dispatchID   string
		wantPubTopic string
		wantPubCount int
		wantHTTPCall bool
		wantHTTPEnd  string
	}{
		{
			name:         "jetstream publish success does not invoke http fallback",
			busMode:      "success",
			turnStatus:   "success",
			turnError:    "",
			chunkID:      "chunk-js-success",
			dispatchID:   "disp-js-success",
			wantPubTopic: "aerial.dispatch.completed",
			wantPubCount: 1,
			wantHTTPCall: false,
		},
		{
			name:         "jetstream publish failed status routes to failed topic",
			busMode:      "success",
			turnStatus:   "failed",
			turnError:    "build error",
			chunkID:      "chunk-js-fail",
			dispatchID:   "disp-js-fail",
			wantPubTopic: "aerial.dispatch.failed",
			wantPubCount: 1,
			wantHTTPCall: false,
		},
		{
			name:         "nil event bus invokes legacy http fallback",
			busMode:      "nil",
			turnStatus:   "success",
			turnError:    "",
			chunkID:      "chunk-nil-bus",
			dispatchID:   "disp-nil-bus",
			wantPubCount: 0,
			wantHTTPCall: true,
			wantHTTPEnd:  "/chunks/complete",
		},
		{
			name:         "bus publish error invokes legacy http fallback",
			busMode:      "error",
			turnStatus:   "failed",
			turnError:    "fatal runner crash",
			chunkID:      "chunk-bus-err",
			dispatchID:   "disp-bus-err",
			wantPubCount: 0,
			wantHTTPCall: true,
			wantHTTPEnd:  "/chunks/fail",
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

			var deliveredEvents []pubsub.DispatchOutcomeEvent
			var deliveredMu sync.Mutex
			deliveredCond := sync.NewCond(&deliveredMu)

			if tc.wantPubCount > 0 {
				_, err := memBus.Subscribe(context.Background(), "aerial.dispatch.*", "test-sub", func(msgCtx context.Context, msg pubsub.Message) error {
					var ev pubsub.DispatchOutcomeEvent
					if uErr := json.Unmarshal(msg.Data(), &ev); uErr == nil {
						deliveredMu.Lock()
						deliveredEvents = append(deliveredEvents, ev)
						deliveredCond.Broadcast()
						deliveredMu.Unlock()
					}
					return msg.Ack()
				})
				if err != nil {
					t.Fatalf("failed to subscribe to memory bus: %v", err)
				}
			}

			var bus pubsub.Bus
			switch tc.busMode {
			case "success":
				bus = memBus
			case "nil":
				bus = nil
			case "error":
				bus = &errPublishBus{Bus: memBus, err: errors.New("simulated nats publish failure")}
			}

			poolCtx, cancelPool := context.WithCancel(context.Background())
			defer cancelPool()

			pool := &WorkerPool{
				ctx:      poolCtx,
				cancel:   cancelPool,
				eventBus: bus,
				cfg: WorkerPoolConfig{
					VanadisURL: srv.URL,
					EventBus:   bus,
				},
			}

			disp := db.Dispatch{
				ID:             tc.dispatchID,
				Source:         "vanadis",
				SessionID:      "vanadis-" + tc.chunkID,
				TargetID:       tc.chunkID,
				VanadisChunkID: tc.chunkID,
				Prompt:         "=== Active Chunk (Chunk 1 of 1) ===\nBuild ID: build-test\nChunk ID: " + tc.chunkID,
				Status:         db.StatusPending,
				CreatedAt:      time.Now().UTC(),
				UpdatedAt:      time.Now().UTC(),
			}

			te := &turnExecution{
				pool:        pool,
				dispatch:    &disp,
				threadID:    "dispatch-" + disp.ID,
				triggerType: "dispatch:vanadis",
				execStart:   time.Now().UTC().Add(-100 * time.Millisecond),
				turnStatus:  tc.turnStatus,
				turnError:   tc.turnError,
				turnCtx:     poolCtx,
			}

			cleanup := pool.setupVanadisChunkTurn(disp, te)
			cleanup()

			if tc.wantPubCount > 0 {
				deliveredMu.Lock()
				for len(deliveredEvents) < tc.wantPubCount {
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

			// Check JetStream publish
			if tc.wantPubCount > 0 {
				deliveredMu.Lock()
				defer deliveredMu.Unlock()
				if len(deliveredEvents) != tc.wantPubCount {
					t.Fatalf("delivered events = %d, want %d", len(deliveredEvents), tc.wantPubCount)
				}
				ev := deliveredEvents[0]
				if ev.DispatchID != tc.dispatchID {
					t.Errorf("ev.DispatchID = %q, want %q", ev.DispatchID, tc.dispatchID)
				}
				if ev.VanadisChunkID != tc.chunkID {
					t.Errorf("ev.VanadisChunkID = %q, want %q", ev.VanadisChunkID, tc.chunkID)
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
