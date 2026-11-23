package ticker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/restatetest"
	"github.com/azylman/aerial/services/haro/internal/dispatch"
	"github.com/azylman/aerial/services/haro/internal/model"
	"github.com/azylman/aerial/services/haro/internal/store"
)

type eventWatchTestCase struct {
	name           string
	seedTrigger    bool
	persistent     bool
	preConsume     bool
	closedDB       bool
	nilDispatcher  bool
	defaultClock   bool
	dispatchErr    error
	wantErr        bool
	wantDispatched bool
	wantDeleted    bool
	wantCleared    bool
	wantCode       int
}

func verifyEventWatchOutcome(ctx context.Context, t *testing.T, db *sql.DB, tc eventWatchTestCase, triggerID string, disp dispatch.PromptDispatcher) {
	t.Helper()
	if !tc.nilDispatcher && disp != nil {
		fDisp, ok := disp.(*fakeDispatcher)
		if !ok {
			t.Fatal("expected *fakeDispatcher")
		}
		if tc.wantDispatched && len(fDisp.dispatched) == 0 {
			t.Error("expected prompt dispatch, got none")
		}
		if !tc.wantDispatched && len(fDisp.dispatched) > 0 {
			t.Errorf("expected no prompt dispatch, got %d", len(fDisp.dispatched))
		}
	}

	if tc.seedTrigger {
		_, getErr := store.GetEventTrigger(ctx, db, triggerID)
		if tc.wantDeleted && getErr == nil {
			t.Error("expected trigger to be deleted, still found")
		}
		if !tc.wantDeleted && getErr != nil {
			t.Errorf("expected trigger to exist, got err: %v", getErr)
		}
	}
}

func seedEventWatchTriggerForTest(ctx context.Context, t *testing.T, db *sql.DB, tc eventWatchTestCase, triggerID string, now time.Time) {
	t.Helper()
	if tc.closedDB {
		_ = db.Close()
		return
	}
	if !tc.seedTrigger {
		return
	}
	if err := store.InsertEventTrigger(ctx, db, model.EventTrigger{
		ID:         triggerID,
		ChannelID:  "100000000000000001",
		Source:     "flux",
		Target:     "aerial",
		EventType:  "sync_success",
		Prompt:     "Deploy finished",
		Persistent: tc.persistent,
	}); err != nil {
		t.Fatalf("InsertEventTrigger failed: %v", err)
	}
	if tc.preConsume {
		consumed, cErr := store.ConsumeEventTrigger(ctx, db, triggerID, now)
		if cErr != nil || !consumed {
			t.Fatalf("ConsumeEventTrigger setup failed: consumed=%v, err=%v", consumed, cErr)
		}
	}
}

func TestHaroEventWatch_Fire(t *testing.T) {
	t.Parallel()
	tests := []eventWatchTestCase{
		{
			name:           "missing trigger clears state and ends terminally with 404",
			seedTrigger:    false,
			wantDispatched: false,
			wantCleared:    true,
			wantErr:        true,
			wantCode:       http.StatusNotFound,
		},
		{
			name:           "eri 4xx ends terminally and leaves trigger untouched",
			seedTrigger:    true,
			dispatchErr:    restate.ToTerminalError(errors.New("restate prompt dispatch error (400): bad channel"), restate.WithErrorCode(http.StatusBadRequest)),
			wantErr:        true,
			wantCode:       http.StatusBadRequest,
			wantDispatched: true,
		},
		{
			name:          "nil dispatcher returns error",
			seedTrigger:   true,
			nilDispatcher: true,
			wantErr:       true,
		},
		{
			name:           "dispatch error returns error and leaves trigger untouched",
			seedTrigger:    true,
			dispatchErr:    errors.New("dispatch failed"),
			wantErr:        true,
			wantDispatched: true,
			wantDeleted:    false,
		},
		{
			name:           "non-persistent trigger dispatch success deletes trigger and clears state",
			seedTrigger:    true,
			persistent:     false,
			wantDispatched: true,
			wantDeleted:    true,
			wantCleared:    true,
		},
		{
			name:           "persistent trigger fires on the default clock",
			seedTrigger:    true,
			persistent:     true,
			defaultClock:   true,
			wantDispatched: true,
		},
		{
			name:           "persistent trigger dispatch success updates updated_at without deleting",
			seedTrigger:    true,
			persistent:     true,
			wantDispatched: true,
			wantDeleted:    false,
		},
		{
			name:           "persistent trigger within 5s cooldown skips dispatch",
			seedTrigger:    true,
			persistent:     true,
			preConsume:     true,
			wantDispatched: false,
			wantDeleted:    false,
		},
		{
			name:     "closed db returns error",
			closedDB: true,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openTickerTestDB(t)
			ctx := context.Background()
			triggerID := "trig-" + tc.name

			now := time.Now().UTC()
			seedEventWatchTriggerForTest(ctx, t, db, tc, triggerID, now)

			h := restatetest.NewObjectHarness(t, triggerID)

			var disp dispatch.PromptDispatcher
			if !tc.nilDispatcher {
				disp = &fakeDispatcher{
					response: dispatch.PromptDispatchResponse{
						Status:    model.StatusCompleted,
						ChannelID: "100000000000000001",
						MessageID: "msg-event-1",
					},
					err: tc.dispatchErr,
				}
			}

			w := NewHaroEventWatch(db, disp)
			if !tc.defaultClock {
				w.nowFn = func() time.Time { return now }
			}
			_, err := w.Fire(h.ObjCtx, EventWatchFireRequest{
				TriggerID:     triggerID,
				ContextHeader: "Flux sync",
				EventID:       "ev-1",
			})

			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := restatetest.TerminalCode(err); got != tc.wantCode {
				t.Errorf("terminal code = %d, want %d (err %v)", got, tc.wantCode, err)
			}

			if got := h.ClearedAll(); got != tc.wantCleared {
				t.Errorf("state cleared = %v, want %v", got, tc.wantCleared)
			}
			verifyEventWatchOutcome(ctx, t, db, tc, triggerID, disp)
		})
	}
}

type externalEventTestCase struct {
	name         string
	source       string
	targets      []string
	nilDB        bool
	noIngress    bool
	seedTrigger  bool
	triggerCount int
	statusCode   int
	wantErr      bool
	wantRequest  string
}

func seedExternalEventTriggersForTest(ctx context.Context, t *testing.T, db *sql.DB, tc externalEventTestCase, now time.Time) {
	t.Helper()
	if tc.name == "closed db returns error" && db != nil {
		_ = db.Close()
		return
	}
	if tc.seedTrigger {
		if err := store.InsertEventTrigger(ctx, db, model.EventTrigger{
			ID:        "trig-ext-" + tc.name,
			ChannelID: "100000000000000001",
			Source:    tc.source,
			Target:    "aerial",
			EventType: "sync_success",
			Prompt:    "Prompt",
			CreatedAt: now,
		}); err != nil {
			t.Fatalf("InsertEventTrigger failed: %v", err)
		}
	}
	for i := range tc.triggerCount {
		if err := store.InsertEventTrigger(ctx, db, model.EventTrigger{
			ID:        fmt.Sprintf("trig-ext-%s-%d", tc.name, i),
			ChannelID: "100000000000000001",
			Source:    tc.source,
			Target:    "aerial",
			EventType: "sync_success",
			Prompt:    "Prompt",
			CreatedAt: now,
		}); err != nil {
			t.Fatalf("InsertEventTrigger failed: %v", err)
		}
	}
}

func TestHandleExternalEvent(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()

	tests := []externalEventTestCase{
		{name: "healthcheck source is no-op", source: "healthcheck", targets: []string{"a"}},
		{name: "empty targets is no-op", source: "flux", targets: []string{}},
		{name: "nil db returns error", source: "flux", targets: []string{"a"}, nilDB: true, wantErr: true},
		{name: "closed db returns error", source: "flux", targets: []string{"a"}, statusCode: http.StatusOK, wantErr: true},
		{
			name:        "matching trigger success",
			source:      "flux",
			targets:     []string{"aerial"},
			seedTrigger: true,
			statusCode:  http.StatusAccepted,
			wantErr:     false,
			wantRequest: "/HaroEventWatch/trig-ext-matching trigger success/Fire/send key=haro:event:trig-ext-matching trigger success:ev-123",
		},
		{
			name:        "matching trigger post failure",
			source:      "flux",
			targets:     []string{"aerial"},
			seedTrigger: true,
			statusCode:  http.StatusInternalServerError,
			wantErr:     true,
		},
		{name: "matching trigger without ingress URL errors", source: "flux", targets: []string{"aerial"}, seedTrigger: true, noIngress: true, wantErr: true},
		{
			name:         "multiple triggers continues loop on error",
			source:       "flux",
			targets:      []string{"aerial"},
			triggerCount: 2,
			statusCode:   http.StatusInternalServerError,
			wantErr:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var db *sql.DB
			ctx := context.Background()
			if !tc.nilDB {
				db = openTickerTestDB(t)
			}
			seedExternalEventTriggersForTest(ctx, t, db, tc, now)

			called := 0
			var lastRequest string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called++
				lastRequest = r.URL.Path + " key=" + r.Header.Get("Idempotency-Key")
				w.WriteHeader(tc.statusCode)
			}))
			defer srv.Close()

			ev := pubsub.ExternalEvent{
				EventID:       "ev-123",
				Source:        tc.source,
				Targets:       tc.targets,
				EventType:     "sync_success",
				ContextHeader: "Sync",
			}

			ingressURL := srv.URL
			if tc.noIngress {
				ingressURL = ""
			}
			err := HandleExternalEvent(ctx, db, ingressURL, ev, srv.Client())
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.triggerCount > 0 && called != tc.triggerCount {
				t.Errorf("called = %d, want %d", called, tc.triggerCount)
			}
			if tc.wantRequest != "" && lastRequest != tc.wantRequest {
				t.Errorf("ingress request = %q, want %q", lastRequest, tc.wantRequest)
			}
		})
	}
}
