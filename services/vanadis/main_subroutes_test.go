package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInitPostgres(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	srv := &VanadisServer{}

	// invalid postgres URL
	_, err := initPostgres(ctx, srv, ":::invalid-url")
	if err == nil {
		t.Fatal("expected error on invalid postgres url")
	}
}

func TestVanadisServer_StoreHTTPEndpointErrors(t *testing.T) {
	t.Parallel()

	sErr := newMemoryVanadisStore()
	sErr.getErr = errors.New("simulated get err")

	srv := &VanadisServer{store: sErr}
	mux := srv.Routes()

	doReq := func(method, path, body string) int {
		var r *http.Request
		if body != "" {
			r = httptest.NewRequest(method, path, strings.NewReader(body))
		} else {
			r = httptest.NewRequest(method, path, http.NoBody)
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, r)
		return rec.Code
	}

	if code := doReq(http.MethodPost, "/chunks/attach-pr", `{"repo":"aerial","pr_number":101}`); code != http.StatusInternalServerError {
		t.Errorf("expected 500 on AttachChunkPR err, got %d", code)
	}
	if code := doReq(http.MethodGet, "/chunks/c1", ""); code != http.StatusInternalServerError {
		t.Errorf("expected 500 on GetChunk err, got %d", code)
	}
	if code := doReq(http.MethodGet, "/builds/b1", ""); code != http.StatusInternalServerError {
		t.Errorf("expected 500 on GetBuild err, got %d", code)
	}
	if code := doReq(http.MethodPost, "/builds/b1/cancel", ""); code != http.StatusInternalServerError {
		t.Errorf("expected 500 on CancelBuild err, got %d", code)
	}
}

func TestChunkStatusAndIDValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		status      string
		checkStatus bool
		wantDone    bool
		checkMerged bool
		wantMerged  bool
		id          string
		checkID     bool
		wantValid   bool
	}{
		{
			name:        "merged",
			status:      "merged",
			checkStatus: true,
			wantDone:    true,
			checkMerged: false,
			id:          "valid-chunk_1",
			checkID:     true,
			wantValid:   true,
		},
		{
			name:        "deploying",
			status:      "deploying",
			checkStatus: true,
			wantDone:    false,
			checkMerged: true,
			wantMerged:  true,
			id:          "",
			checkID:     true,
			wantValid:   false,
		},
		{
			name:        "completed",
			status:      "completed",
			checkStatus: true,
			wantDone:    true,
			checkMerged: false,
			checkID:     false,
		},
		{
			name:        "running",
			status:      "",
			checkStatus: false,
			checkMerged: false,
			id:          strings.Repeat("a", 121),
			checkID:     true,
			wantValid:   false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.checkStatus {
				if got := isChunkDoneStatus(tc.status); got != tc.wantDone {
					t.Fatalf("isChunkDoneStatus(%q) = %v, want %v", tc.status, got, tc.wantDone)
				}
			}
			if tc.checkMerged {
				if got := isChunkMergedOrDoneStatus(tc.status); got != tc.wantMerged {
					t.Fatalf("isChunkMergedOrDoneStatus(%q) = %v, want %v", tc.status, got, tc.wantMerged)
				}
			}
			if tc.checkID {
				if got := isValidVanadisChunkID(tc.id); got != tc.wantValid {
					t.Fatalf("isValidVanadisChunkID(%q) = %v, want %v", tc.id, got, tc.wantValid)
				}
			}
		})
	}
}
