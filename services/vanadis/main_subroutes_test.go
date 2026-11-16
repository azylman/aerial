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

func TestValidVanadisChunkID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		id        string
		wantValid bool
	}{
		{
			name:      "valid",
			id:        "valid-chunk_1",
			wantValid: true,
		},
		{
			name:      "empty",
			id:        "",
			wantValid: false,
		},
		{
			name:      "too_long",
			id:        strings.Repeat("a", 121),
			wantValid: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isValidVanadisChunkID(tc.id); got != tc.wantValid {
				t.Fatalf("isValidVanadisChunkID(%q) = %v, want %v", tc.id, got, tc.wantValid)
			}
		})
	}
}
