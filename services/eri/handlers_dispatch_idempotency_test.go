package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
)

func TestHandleDispatch_DuplicateIdempotency(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		seed       *db.Dispatch
		getError   bool
		nilStore   bool
		body       string
		wantStatus int
		wantSubstr string
		absentID   string
	}{
		{name: "lookup error before active gate", getError: true, body: `{"id":"new","source":"heartbeat","prompt":"test","skip_if_active":true}`, wantStatus: 500, wantSubstr: "Failed to check existing dispatch"},
		{name: "nil store", nilStore: true, body: `{"id":"new","source":"heartbeat","prompt":"test","skip_if_active":true}`, wantStatus: 500, wantSubstr: "Database store not configured"},
		{name: "pending replay", seed: &db.Dispatch{ID: "existing", SessionID: "session", Status: db.StatusPending, Prompt: "original"}, body: `{"id":"existing","session_id":"session","source":"vanadis","prompt":"replacement","skip_if_active":true}`, wantStatus: 200, wantSubstr: `"status":"already_exists"`},
		{name: "processing replay", seed: &db.Dispatch{ID: "existing", SessionID: "session", Status: db.StatusProcessing, Prompt: "original"}, body: `{"id":"existing","session_id":"session","source":"vanadis","prompt":"replacement","skip_if_active":true}`, wantStatus: 200, wantSubstr: `"status":"already_exists"`},
		{name: "terminal replay with competing active session", seed: &db.Dispatch{ID: "existing", SessionID: "session", Status: db.StatusCompleted, Prompt: "original"}, body: `{"id":"existing","session_id":"session","source":"vanadis","prompt":"replacement","skip_if_active":true}`, wantStatus: 200, wantSubstr: `"status":"already_exists"`},
		{name: "competing new id", seed: &db.Dispatch{ID: "existing", SessionID: "session", Status: db.StatusProcessing, Prompt: "original"}, body: `{"id":"new","session_id":"session","source":"vanadis","prompt":"test","skip_if_active":true}`, wantStatus: 200, wantSubstr: `"status":"skipped_active"`, absentID: "new"},
		{name: "empty id", body: `{"source":"heartbeat","prompt":"test","skip_if_active":true}`, wantStatus: 202, wantSubstr: `"status":"accepted"`},
		{name: "validation before replay", seed: &db.Dispatch{ID: "existing", Status: db.StatusPending}, body: `{"id":"existing","source":"vanadis","prompt":"","skip_if_active":true}`, wantStatus: 400, wantSubstr: "Invalid payload"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			fake := dbtest.NewFakeStore()
			seedReplayStore(ctx, t, fake, tc.seed)
			if tc.getError {
				fake.FailNext("GetDispatch", errors.New("simulated error"))
			}
			var store db.Store = fake
			if tc.nilStore {
				store = nil
			}
			w := runHTTP(handleDispatch(store, nil), http.MethodPost, "/dispatch", strings.NewReader(tc.body))
			if w.Code != tc.wantStatus || !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("got status=%d body=%q, want status=%d substr=%q", w.Code, w.Body.String(), tc.wantStatus, tc.wantSubstr)
			}
			verifyReplaySeed(ctx, t, fake, tc.seed)

			if tc.absentID != "" {
				if got, err := fake.GetDispatch(ctx, tc.absentID); err != nil || got != nil {
					t.Fatalf("competing dispatch persisted: %+v, %v", got, err)
				}
			}
		})
	}
}

func verifyReplaySeed(ctx context.Context, t *testing.T, store db.Store, seed *db.Dispatch) {
	t.Helper()
	if seed == nil {
		return
	}
	got, err := store.GetDispatch(ctx, seed.ID)
	if err != nil || got == nil || got.Status != seed.Status || got.Prompt != seed.Prompt {
		t.Fatalf("seed changed: %+v, %v", got, err)
	}
}

func seedReplayStore(ctx context.Context, t *testing.T, fake *dbtest.FakeStore, seed *db.Dispatch) {
	t.Helper()
	if seed != nil {
		if _, err := fake.InsertDispatch(ctx, *seed); err != nil {
			t.Fatal(err)
		}
		if seed.Status == db.StatusCompleted {
			if _, err := fake.InsertDispatch(ctx, db.Dispatch{ID: "other", SessionID: "session", Status: db.StatusPending}); err != nil {
				t.Fatal(err)
			}
		}
	}
}
