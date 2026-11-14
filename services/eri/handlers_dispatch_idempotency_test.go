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
	for _, tc := range []struct {
		name       string
		store      func() db.Store
		body       string
		wantStatus int
		wantSubstr string
	}{
		{
			name: "get dispatch error",
			store: func() db.Store {
				s := dbtest.NewFakeStore()
				s.FailNext("GetDispatch", errors.New("simulated error"))
				return s
			},
			body:       `{"id":"disp-err","source":"heartbeat","prompt":"test"}`,
			wantStatus: http.StatusInternalServerError,
			wantSubstr: "Failed to check existing dispatch",
		},
		{
			name: "duplicate dispatch id returns already_exists without enqueueing",
			store: func() db.Store {
				s := dbtest.NewFakeStore()
				_, _ = s.InsertDispatch(context.Background(), db.Dispatch{
					ID:     "disp-dup-1",
					Source: "vanadis",
					Prompt: "execute chunk",
					Status: db.StatusPending,
				})
				return s
			},
			body:       `{"id":"disp-dup-1","source":"vanadis","prompt":"execute chunk"}`,
			wantStatus: http.StatusOK,
			wantSubstr: `"status":"already_exists"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.store()
			w := runHTTP(handleDispatch(s, nil), http.MethodPost, "/dispatch", strings.NewReader(tc.body))
			if w.Code != tc.wantStatus || !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("got status=%d body=%q, want status=%d substr=%q", w.Code, w.Body.String(), tc.wantStatus, tc.wantSubstr)
			}
		})
	}
}
