package router

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/azylman/aerial/pkg/prwire"
)

func TestCheckOutcomeHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		conclusion string
		action     string
		nilRes     bool
		want       string
	}{
		{name: "nil event maps to no handler", nilRes: true, want: ""},
		{name: "completed success maps to check passed", conclusion: conclusionSuccess, action: ghStatusCompleted, want: "HandleCheckPassed"},
		{name: "completed failure maps to check failed", conclusion: conclusionFailure, action: ghStatusCompleted, want: "HandleCheckFailed"},
		{name: "timed out maps to check failed", conclusion: conclusionTimedOut, action: ghStatusCompleted, want: "HandleCheckFailed"},
		{name: "in progress maps to no handler", conclusion: "", action: "in_progress", want: ""},
		{name: "cancelled maps to no handler", conclusion: "cancelled", action: ghStatusCompleted, want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var res *ResolvedGitHubEvent
			if !tc.nilRes {
				res = &ResolvedGitHubEvent{Conclusion: tc.conclusion, Action: tc.action}
			}
			if got := checkOutcomeHandler(res); got != tc.want {
				t.Errorf("checkOutcomeHandler() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestRestateSend pins the one send core every Restate request goes through:
// a missing restate_url and a transport/5xx failure are retryable, a 4xx is
// terminal, and a 2xx counts as sent.
func TestRestateSend(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		status       int
		noURL        bool
		wantErr      bool
		wantTerminal bool
	}{
		{name: "accepted send succeeds", status: http.StatusAccepted},
		{name: "server error is retryable", status: http.StatusServiceUnavailable, wantErr: true},
		{name: "client error is terminal", status: http.StatusNotFound, wantErr: true, wantTerminal: true},
		{name: "missing restate url is retryable", noURL: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			}))
			t.Cleanup(srv.Close)
			cfg := Config{RestateURL: srv.URL}
			if tc.noURL {
				cfg.RestateURL = ""
			}
			s := NewRouterServer(fixedConfig(cfg), nil)
			err := s.sendOwnedPR(context.Background(), "HandleMerged", prwire.PREvent{Repo: "azylman/aerial", PRNumber: 7, MergeSHA: "abc123"})
			if (err != nil) != tc.wantErr {
				t.Fatalf("sendOwnedPR() err = %v, wantErr %v", err, tc.wantErr)
			}
			failed, terminal := restateSendFailure(err)
			if failed != tc.wantErr || terminal != tc.wantTerminal {
				t.Errorf("restateSendFailure() = %v/%v, want %v/%v", failed, terminal, tc.wantErr, tc.wantTerminal)
			}
		})
	}
}

// TestRestateSendSkipsKeylessEvents pins that PR-less and repo-less events
// have no Restate object key and send nothing.
func TestRestateSendSkipsKeylessEvents(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		pr   prwire.PREvent
		repo prwire.RepoEvent
		sha  string
	}{
		{name: "pr-less events without sha", pr: prwire.PREvent{Repo: "azylman/aerial"}, repo: prwire.RepoEvent{}, sha: ""},
		{name: "repo-less events with zero sha", pr: prwire.PREvent{PRNumber: 7}, repo: prwire.RepoEvent{MainSHA: "abc123"}, sha: "0000000"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusAccepted)
			}))
			t.Cleanup(srv.Close)
			s := NewRouterServer(fixedConfig(Config{RestateURL: srv.URL}), nil)
			ctx := context.Background()
			pr := tc.pr
			pr.PRNumber = 0
			errs := errors.Join(s.sendOwnedPR(ctx, "HandleMerged", tc.pr), s.sendOwnedRepo(ctx, "HandleMainPush", tc.repo),
				s.sendOwnedPROrSHA(ctx, "HandlePushBySHA", "HandleMerged", tc.sha, pr))
			if errs != nil {
				t.Fatalf("sends = %v", errs)
			}
			if got := calls.Load(); got != 0 {
				t.Errorf("restate requests = %d, want 0", got)
			}
		})
	}
}
