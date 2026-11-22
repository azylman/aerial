package prflow

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

func TestFlow_RecordPRHeadRevision(t *testing.T) {
	t.Parallel()
	if store.PrHeadVerifiedFromContext(nil) {
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
			exec := executor.NewExecutor("", nil, nil, "", "azylman", api.Client())
			exec.GitHubAPIURL = api.URL
			spy := &checkStatusSpyStore{}
			if tc.cold {
				spy.getBuildErr = errors.New("read unavailable")
			}
			if !tc.cold {
				spy.chunks = []model.VanadisChunk{{ID: "c1", BuildID: "b1", Status: "pr_opened", HeadSHA: "head-b"}}
			}
			f := NewFlow(spy, exec)
			err := f.RecordPRHeadRevision(context.Background(), "c1", pubsub.PRLifecycleEvent{Repo: tc.repo, PRNumber: 12, HeadSHA: "head-a", OccurredAt: time.Unix(tc.at, 0)})
			if (err != nil) != tc.wantError {
				t.Fatalf("err=%v", err)
			}
			if tc.wantError && len(spy.resets) != 0 {
				t.Fatalf("rejected head mutated storage: %v", spy.resets)
			}
			if got := spy.chunk("c1").HeadSHA; got != tc.wantHead {
				t.Fatalf("head=%s want=%s", got, tc.wantHead)
			}
		})
	}
}

func TestFlow_CanFallbackThreadRepoLookup(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		event pubsub.PRLifecycleEvent
		want  bool
	}{
		{name: "empty target id", event: pubsub.PRLifecycleEvent{Branch: "main"}, want: false},
		{name: "quiet event", event: pubsub.PRLifecycleEvent{TargetID: "th-1", Quiet: true}, want: false},
		{name: "dependabot branch", event: pubsub.PRLifecycleEvent{TargetID: "th-1", Branch: "refs/heads/dependabot/npm/foo-1"}, want: false},
		{name: "threaded event", event: pubsub.PRLifecycleEvent{TargetID: "th-1", Branch: "main"}, want: true},
		{name: "threaded event without branch", event: pubsub.PRLifecycleEvent{TargetID: "th-1"}, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, CanFallbackThreadRepoLookup(tc.event))
		})
	}
}

func TestFlow_SynchronizeValidatesHeadAgainstGitHub(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"state":"open","merged":false,"head":{"sha":"sha-2"}}`)
	}))
	defer api.Close()
	exec := executor.NewExecutor("", nil, nil, "", "azylman", api.Client())
	exec.GitHubAPIURL = api.URL
	spy := &checkStatusSpyStore{}
	f := NewFlow(spy, exec)

	// synchronize validates head against GitHub API and resets check state in store
	if err := f.HandlePREventType(ctx, "b1", "c1", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-2"}); err != nil {
		t.Fatalf("synchronize err=%v", err)
	}
	if len(spy.resets) != 1 || spy.resets[0] != "c1:sha-2:" {
		t.Fatalf("resets=%v", spy.resets)
	}

	// synchronize for superseded sha-1 rejected by GitHub API head validation without calling store reset
	spy.resets = nil
	if err := f.HandlePREventType(ctx, "b1", "c1", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-1"}); !errors.Is(err, ErrPRTransitionSkipped) {
		t.Fatalf("superseded err=%v", err)
	}
	if len(spy.resets) != 0 {
		t.Fatalf("rejected head mutated storage: %v", spy.resets)
	}
}
