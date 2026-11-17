package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
)

func TestBuildObject_RecordPRHeadRevision(t *testing.T) {
	t.Parallel()
	if prHeadVerifiedFromContext(nil) {
		t.Fatal("expected false for nil context")
	}
	tests := []struct {
		name, body string
		status     int
		wantHead   string
		wantError  bool
		at         int64
		cold       bool
		repo       string
	}{
		{"redelivered historical head", `{"state":"open","merged":false,"head":{"sha":"head-b"}}`, 200, "head-b", true, 30, false, "owner/repo"},
		{"current head with older delivery timestamp", `{"state":"open","merged":false,"head":{"sha":"head-a"}}`, 200, "head-a", false, 5, false, "owner/repo"},
		{"API unavailable", `{}`, 503, "head-b", true, 30, false, "owner/repo"},
		{"invalid response", `not json`, 200, "head-b", true, 30, false, "owner/repo"},
		{"missing head", `{"state":"open","merged":false}`, 200, "head-b", true, 30, false, "owner/repo"},
		{"unknown stale head", `{"state":"open","merged":false,"head":{"sha":"head-b"}}`, 200, "", true, 30, true, "owner/repo"},
		{"bare repository", `{"state":"open","merged":false,"head":{"sha":"head-a"}}`, 200, "head-a", false, 30, false, "aerial"},
		{"attached fork PR head update", `{"state":"open","merged":false,"head":{"sha":"head-a","repo":{"fork":true,"full_name":"forker/repo"}},"base":{"repo":{"full_name":"owner/repo"}}}`, 200, "head-a", false, 30, false, "owner/repo"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				wantPath := "/repos/owner/repo/pulls/12"
				if tc.repo == "aerial" {
					wantPath = "/repos/azylman/aerial/pulls/12"
				}
				if r.URL.Path != wantPath {
					t.Errorf("path=%s", r.URL.Path)
				}
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer api.Close()
			executor := NewExecutor("", nil, nil, "", api.Client())
			executor.gitHubAPIURL = api.URL
			store := &checkStatusSpyStore{}
			if tc.cold {
				store.getBuildErr = errors.New("read unavailable")
			}
			bo := NewBuildObject("", nil).WithAuthority(store, executor)
			if !tc.cold {
				store.chunks = []VanadisChunk{{ID: "c1", BuildID: "b1", Status: "pr_opened", HeadSHA: "head-b"}}
			}
			err := bo.recordPRHeadRevision(context.Background(), "c1", pubsub.PRLifecycleEvent{Repo: tc.repo, PRNumber: 12, HeadSHA: "head-a", OccurredAt: time.Unix(tc.at, 0)})
			if (err != nil) != tc.wantError {
				t.Fatalf("err=%v", err)
			}
			if tc.wantError && len(store.resets) != 0 {
				t.Fatalf("rejected head mutated storage: %v", store.resets)
			}
			if got := store.chunk("c1").HeadSHA; got != tc.wantHead {
				t.Fatalf("head=%s want=%s", got, tc.wantHead)
			}
		})
	}
}
