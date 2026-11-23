package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

func TestFetchGitHubPullRequest(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		nilExec     bool
		repo        string
		number      int
		status      int
		headerKey   string
		headerVal   string
		body        string
		wantOwner   string
		wantFull    string
		wantErr     bool
		wantSkipped bool
	}{
		{name: "nil executor", nilExec: true, repo: "owner/repo", number: 12, wantErr: true},
		{name: "empty repo", repo: "", number: 12, wantErr: true},
		{name: "zero pr number", repo: "owner/repo", number: 0, wantErr: true},
		{name: "404 skips transition", repo: "owner/repo", number: 12, status: 404, body: `{"message":"Not Found"}`, wantErr: true, wantSkipped: true},
		{name: "410 gone skips transition", repo: "owner/repo", number: 12, status: 410, body: `{"message":"Gone"}`, wantErr: true, wantSkipped: true},
		{name: "plain 403 returns error without skip", repo: "owner/repo", number: 12, status: 403, body: `{"message":"Forbidden"}`, wantErr: true},
		{name: "plain 401 returns error without skip", repo: "owner/repo", number: 12, status: 401, body: `{"message":"Unauthorized"}`, wantErr: true},
		{name: "rate limited 403 returns error without skip", repo: "owner/repo", number: 12, status: 403, headerKey: "X-Ratelimit-Remaining", headerVal: "0", body: `{"message":"rate limit"}`, wantErr: true},
		{name: "rate limited 429 returns error without skip", repo: "owner/repo", number: 12, status: 429, headerKey: "Retry-After", headerVal: "60", body: `{"message":"too many requests"}`, wantErr: true},
		{name: "server 500 error returns error without skip", repo: "owner/repo", number: 12, status: 500, body: `{}`, wantErr: true},
		{name: "invalid json", repo: "owner/repo", number: 12, status: 200, body: `invalid`, wantErr: true},
		{name: "ok", repo: "owner/repo", number: 12, status: 200, body: `{"state":"open"}`, wantOwner: "owner", wantFull: "owner/repo"},
		{name: "bare repo resolves owner", repo: "aerial", number: 12, status: 200, body: `{"state":"open"}`, wantOwner: "example-owner", wantFull: "example-owner/aerial"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.headerKey != "" {
					w.Header().Set(tc.headerKey, tc.headerVal)
				}
				w.WriteHeader(tc.status)
				n, writeErr := w.Write([]byte(tc.body))
				assert.NoError(t, writeErr)
				assert.Positive(t, n)
			}))
			defer srv.Close()
			var exec *Executor
			if !tc.nilExec {
				exec = NewExecutor("", nil, nil, "", "example-owner", srv.Client())
				exec.GitHubAPIURL = srv.URL
			}
			pr, owner, full, err := exec.fetchGitHubPullRequest(context.Background(), tc.repo, tc.number)
			if tc.wantSkipped {
				assert.ErrorIs(t, err, store.ErrPRTransitionSkipped)
				return
			}
			if tc.wantErr {
				assert.Error(t, err)
				assert.NotErrorIs(t, err, store.ErrPRTransitionSkipped)
				return
			}
			assert.NoError(t, err)
			assert.NotNil(t, pr)
			assert.Equal(t, tc.wantOwner, owner)
			assert.Equal(t, tc.wantFull, full)
		})
	}
}

type stubRateLimitTransport struct {
	resetAt time.Time
}

func (*stubRateLimitTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, nil
}

func (s *stubRateLimitTransport) RateLimitResetAt() time.Time {
	return s.resetAt
}

func TestExecutor_RateLimitDelay(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		exec      *Executor
		now       time.Time
		wantDelay time.Duration
	}{
		{
			name:      "nil executor",
			exec:      nil,
			now:       now,
			wantDelay: 0,
		},
		{
			name:      "nil http client",
			exec:      &Executor{},
			now:       now,
			wantDelay: 0,
		},
		{
			name:      "active rate limit delay",
			exec:      &Executor{HTTPClient: &http.Client{Transport: &stubRateLimitTransport{resetAt: now.Add(5 * time.Minute)}}},
			now:       now,
			wantDelay: 5 * time.Minute,
		},
		{
			name:      "past rate limit delay clamps to zero",
			exec:      &Executor{HTTPClient: &http.Client{Transport: &stubRateLimitTransport{resetAt: now.Add(-5 * time.Minute)}}},
			now:       now,
			wantDelay: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.wantDelay, tc.exec.RateLimitDelay(tc.now))
		})
	}
}

func TestValidatePRHead(t *testing.T) {
	t.Parallel()
	openBody := `{"state":"open","merged":false,"head":{"sha":"abc123"}}`
	tests := []struct {
		name        string
		repo        string
		body        string
		status      int
		head        string
		wantErr     bool
		wantSkipped bool
	}{
		{name: "open matching head", repo: "owner/repo", body: openBody, status: 200, head: "abc123"},
		{name: "head match ignores case", repo: "owner/repo", body: openBody, status: 200, head: "ABC123"},
		{name: "bare repo resolves owner", repo: "aerial", body: openBody, status: 200, head: "abc123"},
		{name: "closed pr skipped", repo: "owner/repo", body: `{"state":"closed","merged":true,"head":{"sha":"abc123"}}`, status: 200, head: "abc123", wantErr: true, wantSkipped: true},
		{name: "merged pr skipped", repo: "owner/repo", body: `{"state":"open","merged":true,"head":{"sha":"abc123"}}`, status: 200, head: "abc123", wantErr: true, wantSkipped: true},
		{name: "superseded head skipped", repo: "owner/repo", body: openBody, status: 200, head: "other", wantErr: true, wantSkipped: true},
		{name: "incomplete lifecycle", repo: "owner/repo", body: `{"state":"draft","merged":false,"head":{"sha":"abc123"}}`, status: 200, head: "abc123", wantErr: true},
		{name: "missing merged flag", repo: "owner/repo", body: `{"state":"open","head":{"sha":"abc123"}}`, status: 200, head: "abc123", wantErr: true},
		{name: "empty head", repo: "owner/repo", body: `{"state":"open","merged":false,"head":{}}`, status: 200, head: "abc123", wantErr: true},
		{name: "api unavailable", repo: "owner/repo", body: `{}`, status: 503, head: "abc123", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				n, writeErr := w.Write([]byte(tc.body))
				assert.NoError(t, writeErr)
				assert.Positive(t, n)
			}))
			defer srv.Close()
			exec := NewExecutor("", nil, nil, "", "example-owner", srv.Client())
			exec.GitHubAPIURL = srv.URL
			err := exec.ValidatePRHead(context.Background(), tc.repo, 12, tc.head)
			if tc.wantSkipped {
				assert.ErrorIs(t, err, store.ErrPRTransitionSkipped)
				return
			}
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestLivePRState(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		status   int
		body     string
		wantHead string
		wantOpen bool
		wantErr  bool
	}{
		{"open pr", 200, `{"state":"open","merged":false,"head":{"sha":" abc123 "}}`, "abc123", true, false},
		{"open pr without merged field", 200, `{"state":"open","head":{"sha":"abc123"}}`, "abc123", true, false},
		{"closed pr", 200, `{"state":"closed","merged":false,"head":{"sha":"abc123"}}`, "abc123", false, false},
		{"merged pr", 200, `{"state":"closed","merged":true,"head":{"sha":"abc123"}}`, "abc123", false, false},
		{"missing pr reads closed", 404, `{"message":"Not Found"}`, "", false, false},
		{"server error returned", 500, `{}`, "", false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/repos/example-owner/aerial/pulls/42", r.URL.Path)
				w.WriteHeader(tc.status)
				n, writeErr := w.Write([]byte(tc.body))
				assert.NoError(t, writeErr)
				assert.Positive(t, n)
			}))
			defer srv.Close()
			exec := NewExecutor("", nil, nil, "", "example-owner", srv.Client())
			exec.GitHubAPIURL = srv.URL
			head, open, err := exec.LivePRState(context.Background(), "aerial", 42)
			assert.Equal(t, tc.wantErr, err != nil)
			assert.Equal(t, tc.wantHead, head)
			assert.Equal(t, tc.wantOpen, open)
		})
	}
}

func TestMergedPRState(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		status     int
		body       string
		wantMerged bool
		wantHead   string
		wantMerge  string
		wantErr    bool
	}{
		{"merged pr with head and merge sha", 200, `{"state":"closed","merged":true,"head":{"sha":" abc123 "},"merge_commit_sha":" def456 "}`, true, "abc123", "def456", false},
		{"open unmerged pr", 200, `{"state":"open","merged":false,"head":{"sha":"abc123"}}`, false, "abc123", "", false},
		{"open pr without merged field", 200, `{"state":"open","head":{"sha":"abc123"}}`, false, "abc123", "", false},
		{"closed unmerged pr", 200, `{"state":"closed","merged":false,"head":{"sha":"abc123"}}`, false, "abc123", "", false},
		{"missing pr reads unmerged", 404, `{"message":"Not Found"}`, false, "", "", false},
		{"server error returned", 500, `{}`, false, "", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/repos/example-owner/aerial/pulls/42", r.URL.Path)
				w.WriteHeader(tc.status)
				n, writeErr := w.Write([]byte(tc.body))
				assert.NoError(t, writeErr)
				assert.Positive(t, n)
			}))
			defer srv.Close()
			exec := NewExecutor("", nil, nil, "", "example-owner", srv.Client())
			exec.GitHubAPIURL = srv.URL
			merged, head, merge, err := exec.MergedPRState(context.Background(), "aerial", 42)
			assert.Equal(t, tc.wantErr, err != nil)
			assert.Equal(t, tc.wantMerged, merged)
			assert.Equal(t, tc.wantHead, head)
			assert.Equal(t, tc.wantMerge, merge)
		})
	}
}

func TestHasActiveOrRecoveredChecks(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		status  int
		body    string
		want    bool
		wantErr bool
	}{
		{"active check run", http.StatusOK, `{"check_runs":[{"name":"unit-tests","status":"in_progress"}]}`, true, false},
		{"completed passing check run", http.StatusOK, `{"check_runs":[{"name":"unit-tests","status":"completed","conclusion":"success"}]}`, true, false},
		{"completed failing check run", http.StatusOK, `{"check_runs":[{"name":"unit-tests","status":"completed","conclusion":"failure"}]}`, false, false},
		{"api server error", http.StatusInternalServerError, `{}`, false, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/repos/example-owner/aerial/commits/abc123/check-runs", r.URL.Path)
				assert.Equal(t, "latest", r.URL.Query().Get("filter"))
				assert.Equal(t, "100", r.URL.Query().Get("per_page"))
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			exec := NewExecutor("", nil, nil, "", "example-owner", srv.Client())
			exec.GitHubAPIURL = srv.URL
			got, err := exec.HasActiveOrRecoveredChecks(context.Background(), "aerial", "abc123")
			assert.Equal(t, tc.wantErr, err != nil)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestIsTrustedPRAuthor(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		login       string
		association string
		owner       string
		want        bool
	}{
		{name: "owner login", login: "owner", association: "NONE", owner: "owner", want: true},
		{name: "owner login with padding", login: "  owner  ", association: "", owner: "owner", want: true},
		{name: "member association", login: "teammate", association: "MEMBER", owner: "owner", want: true},
		{name: "collaborator lowercase", login: "teammate", association: "collaborator", owner: "owner", want: true},
		{name: "stranger", login: "stranger", association: "NONE", owner: "owner", want: false},
		{name: "empty login", login: "", association: "NONE", owner: "owner", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, isTrustedPRAuthor(tc.login, tc.association, tc.owner))
		})
	}
}

func TestHasDispatchedChunkAssociation(t *testing.T) {
	t.Parallel()
	trailer := func(thread, chunk string) string {
		return fmt.Sprintf("notes <!-- aerial-pr:{\"target_id\":%q,\"vanadis_chunk_id\":%q} -->", thread, chunk)
	}
	tests := []struct {
		name        string
		body        string
		buildThread string
		chunkID     string
		want        bool
	}{
		{name: "matching trailer", body: trailer("th-1", "c1"), buildThread: "th-1", chunkID: "c1", want: true},
		{name: "empty build thread matches", body: trailer("other", "c1"), buildThread: "", chunkID: "c1", want: true},
		{name: "missing marker", body: "plain notes", buildThread: "th-1", chunkID: "c1", want: false},
		{name: "unclosed marker", body: `notes <!-- aerial-pr:{"a":1}`, buildThread: "th-1", chunkID: "c1", want: false},
		{name: "invalid trailer json", body: `notes <!-- aerial-pr:not-json -->`, buildThread: "th-1", chunkID: "c1", want: false},
		{name: "wrong chunk", body: trailer("th-1", "c9"), buildThread: "th-1", chunkID: "c1", want: false},
		{name: "thread mismatch", body: trailer("other", "c1"), buildThread: "th-1", chunkID: "c1", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			build := model.VanadisBuild{ThreadID: tc.buildThread}
			chunk := model.VanadisChunk{ID: tc.chunkID}
			assert.Equal(t, tc.want, hasDispatchedChunkAssociation(tc.body, build, chunk))
		})
	}
}

func TestValidateChunkPRAttachment(t *testing.T) {
	t.Parallel()
	prBody := func(body, login, assoc, sha, ref string, fork bool, headFull, baseFull string) string {
		return fmt.Sprintf(`{"state":"open","merged":false,"body":%q,"user":{"login":%q},"author_association":%q,"head":{"sha":%q,"ref":%q,"repo":{"fork":%v,"full_name":%q}},"base":{"repo":{"full_name":%q}}}`,
			body, login, assoc, sha, ref, fork, headFull, baseFull)
	}
	validTrailer := `notes <!-- aerial-pr:{"target_id":"th-1","vanadis_chunk_id":"c1"} -->`
	valid := prBody(validTrailer, "owner", "MEMBER", "deadbee", "main", false, "owner/repo", "owner/repo")
	tests := []struct {
		name        string
		prBody      string
		status      int
		branch      string
		headSHA     string
		buildThread string
		wantErr     bool
		wantSkipped bool
	}{
		{name: "valid attachment", prBody: valid, status: 200, branch: "main", headSHA: "deadbee"},
		{name: "empty event sha accepted", prBody: valid, status: 200, branch: "main", headSHA: ""},
		{name: "forked head skipped", prBody: prBody(validTrailer, "owner", "MEMBER", "deadbee", "main", true, "owner/repo", "owner/repo"), status: 200, branch: "main", headSHA: "deadbee", wantErr: true, wantSkipped: true},
		{name: "head repo mismatch skipped", prBody: prBody(validTrailer, "owner", "MEMBER", "deadbee", "main", false, "fork/repo", "owner/repo"), status: 200, branch: "main", headSHA: "deadbee", wantErr: true, wantSkipped: true},
		{name: "untrusted author skipped", prBody: prBody(validTrailer, "stranger", "NONE", "deadbee", "main", false, "owner/repo", "owner/repo"), status: 200, branch: "main", headSHA: "deadbee", wantErr: true, wantSkipped: true},
		{name: "branch mismatch skipped", prBody: valid, status: 200, branch: "other", headSHA: "deadbee", wantErr: true, wantSkipped: true},
		{name: "sha mismatch skipped", prBody: valid, status: 200, branch: "main", headSHA: "cafef00", wantErr: true, wantSkipped: true},
		{name: "missing trailer skipped", prBody: prBody("plain notes", "owner", "MEMBER", "deadbee", "main", false, "owner/repo", "owner/repo"), status: 200, branch: "main", headSHA: "deadbee", wantErr: true, wantSkipped: true},
		{name: "branch chunk match without trailer accepted", prBody: prBody("plain notes", "owner", "MEMBER", "deadbee", "feat/repo-pr-vanadis-c1-012345", false, "owner/repo", "owner/repo"), status: 200, branch: "feat/repo-pr-vanadis-c1-012345", headSHA: "deadbee"},
		{name: "branch chunk match with matching target id accepted", prBody: prBody("plain notes", "owner", "MEMBER", "deadbee", "feat/repo-pr-vanadis-c1-t123456789012345678-012345", false, "owner/repo", "owner/repo"), status: 200, branch: "feat/repo-pr-vanadis-c1-t123456789012345678-012345", headSHA: "deadbee", buildThread: "123456789012345678"},
		{name: "branch chunk match with mismatching target id skipped", prBody: prBody("plain notes", "owner", "MEMBER", "deadbee", "feat/repo-pr-vanadis-c1-t123456789012345678-012345", false, "owner/repo", "owner/repo"), status: 200, branch: "feat/repo-pr-vanadis-c1-t123456789012345678-012345", headSHA: "deadbee", buildThread: "999999999999999999", wantErr: true, wantSkipped: true},
		{name: "api unavailable", prBody: `{}`, status: 500, branch: "main", headSHA: "deadbee", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				n, writeErr := w.Write([]byte(tc.prBody))
				assert.NoError(t, writeErr)
				assert.Positive(t, n)
			}))
			defer srv.Close()
			exec := NewExecutor("", nil, nil, "", "example-owner", srv.Client())
			exec.GitHubAPIURL = srv.URL
			threadID := "th-1"
			if tc.buildThread != "" {
				threadID = tc.buildThread
			}
			build := model.VanadisBuild{ID: "b", ThreadID: threadID}
			chunk := model.VanadisChunk{ID: "c1"}
			ev := pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, Branch: tc.branch, HeadSHA: tc.headSHA}
			err := exec.ValidateChunkPRAttachment(context.Background(), build, chunk, ev)
			if tc.wantSkipped {
				assert.ErrorIs(t, err, store.ErrPRTransitionSkipped)
				return
			}
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestFormatVanadisBuildContext(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		build       model.VanadisBuild
		chunks      []model.VanadisChunk
		activeChunk model.VanadisChunk
		wantSub     []string
	}{
		{
			name: "multi-chunk build context",
			build: model.VanadisBuild{
				ID:        "b-1",
				Title:     "Build Title",
				Objective: "First line\r\nSecond line",
			},
			chunks: []model.VanadisChunk{
				{ID: "c-1", ChunkIndex: 0, Title: "Chunk 1", Mode: "pr", Status: "completed"},
				{ID: "c-2", ChunkIndex: 1, Title: "Chunk 2", Mode: "pr", Status: "running", TargetFiles: []string{"a.go", "b.go"}},
			},
			activeChunk: model.VanadisChunk{ID: "c-2", ChunkIndex: 1, Title: "Chunk 2", Mode: "pr", Status: "running", TargetFiles: []string{"a.go", "b.go"}},
			wantSub: []string{
				"## Vanadis Build Context",
				"- **Build**: b-1 — Build Title",
				"- **Build Objective**: First line Second line",
				"- **Active Chunk (2 of 2)**: c-2 — Chunk 2",
				"- **Active Chunk Target Files**: a.go, b.go",
				"- [Chunk 1/2] c-1: Chunk 1 (pr, completed)",
				"- [Chunk 2/2] c-2: Chunk 2 (pr, running)",
				"- **Scope Boundary (RoboRev & Review Guidance)**:",
			},
		},
		{
			name: "fallback untitled fields",
			build: model.VanadisBuild{
				ID: "b-2",
			},
			chunks: []model.VanadisChunk{
				{ID: "c-1", ChunkIndex: 0},
			},
			activeChunk: model.VanadisChunk{ID: "c-1", ChunkIndex: 0},
			wantSub: []string{
				"- **Build**: b-2 — Untitled Build",
				"- **Active Chunk (1 of 1)**: c-1 — Untitled Chunk",
				"- [Chunk 1/1] c-1: Untitled (unknown, unknown)",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctxStr := FormatVanadisBuildContext(tc.build, tc.chunks, tc.activeChunk)
			for _, sub := range tc.wantSub {
				assert.Contains(t, ctxStr, sub)
			}
		})
	}
}

type autoMergeTestCase struct {
	name          string
	initialBody   string
	nodeID        string
	fetchStatus   int
	patchStatus   int
	graphqlStatus int
	noToken       bool
	build         model.VanadisBuild
	chunks        []model.VanadisChunk
	chunk         model.VanadisChunk
	wantPatched   bool
	wantAutomated bool
	wantErr       bool
}

func handleAutoMergeRequest(t *testing.T, w http.ResponseWriter, r *http.Request, tc autoMergeTestCase, patched, automerged *bool) {
	t.Helper()
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/repos/owner/repo/pulls/10":
		if tc.fetchStatus != 0 {
			w.WriteHeader(tc.fetchStatus)
			return
		}
		w.WriteHeader(http.StatusOK)
		n, err := fmt.Fprintf(w, `{"state":"open","merged":false,"body":%q,"node_id":%q,"user":{"login":"owner"},"author_association":"OWNER","head":{"sha":"abc","ref":"feat/branch","repo":{"fork":false,"full_name":"owner/repo"}},"base":{"repo":{"full_name":"owner/repo"}}}`, tc.initialBody, tc.nodeID)
		assert.NoError(t, err)
		assert.Positive(t, n)
	case r.Method == http.MethodPatch && r.URL.Path == "/repos/owner/repo/pulls/10":
		if tc.patchStatus != 0 {
			w.WriteHeader(tc.patchStatus)
			return
		}
		*patched = true
		w.WriteHeader(http.StatusOK)
		n, err := fmt.Fprintf(w, `{"state":"open"}`)
		assert.NoError(t, err)
		assert.Positive(t, n)
	case r.Method == http.MethodPost && r.URL.Path == "/graphql":
		if tc.graphqlStatus != 0 {
			w.WriteHeader(tc.graphqlStatus)
			return
		}
		*automerged = true
		w.WriteHeader(http.StatusOK)
		n, err := fmt.Fprintf(w, `{"data":{"enablePullRequestAutoMerge":{"pullRequest":{"autoMergeRequest":{"enabledAt":"2026-11-22T00:00:00Z","mergeMethod":"SQUASH"}}}}}`)
		assert.NoError(t, err)
		assert.Positive(t, n)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func TestEnsurePRAutoMergeAndContext(t *testing.T) {
	t.Parallel()
	tests := []autoMergeTestCase{
		{
			name:        "multi-chunk build enriches context and automerges",
			initialBody: "Initial PR description",
			nodeID:      "PR_kwDO12345",
			build:       model.VanadisBuild{ID: "b-1", Title: "Multi Build"},
			chunks: []model.VanadisChunk{
				{ID: "c-1", ChunkIndex: 0, Repo: "owner/repo"},
				{ID: "c-2", ChunkIndex: 1, Repo: "owner/repo"},
			},
			chunk:         model.VanadisChunk{ID: "c-1", ChunkIndex: 0, Repo: "owner/repo"},
			wantPatched:   true,
			wantAutomated: true,
		},
		{
			name:        "fallback repo from sibling chunks",
			initialBody: "Initial PR description",
			nodeID:      "PR_kwDO12345",
			build:       model.VanadisBuild{ID: "b-1", Title: "Multi Build"},
			chunks: []model.VanadisChunk{
				{ID: "c-1", ChunkIndex: 0, Repo: "owner/repo"},
			},
			chunk:         model.VanadisChunk{ID: "c-1", ChunkIndex: 0, Repo: ""},
			wantPatched:   true,
			wantAutomated: true,
		},
		{
			name:        "context already present skips patch but enables automerge",
			initialBody: "Notes\n\n## Vanadis Build Context\n- details",
			nodeID:      "PR_kwDO12345",
			build:       model.VanadisBuild{ID: "b-1"},
			chunks: []model.VanadisChunk{
				{ID: "c-1", ChunkIndex: 0, Repo: "owner/repo"},
				{ID: "c-2", ChunkIndex: 1, Repo: "owner/repo"},
			},
			chunk:         model.VanadisChunk{ID: "c-1", ChunkIndex: 0, Repo: "owner/repo"},
			wantPatched:   false,
			wantAutomated: true,
		},
		{
			name:        "single chunk standalone build skips context",
			initialBody: "Standalone description",
			nodeID:      "PR_kwDO12345",
			build:       model.VanadisBuild{ID: "b-stand"},
			chunks: []model.VanadisChunk{
				{ID: "b-stand-chunk-1", ChunkIndex: 0, Repo: "owner/repo"},
			},
			chunk:         model.VanadisChunk{ID: "b-stand-chunk-1", ChunkIndex: 0, Repo: "owner/repo"},
			wantPatched:   false,
			wantAutomated: true,
		},
		{
			name:        "pr fetch error propagates",
			initialBody: "Notes",
			fetchStatus: http.StatusInternalServerError,
			build:       model.VanadisBuild{ID: "b-1"},
			chunks:      []model.VanadisChunk{{ID: "c-1", Repo: "owner/repo"}},
			chunk:       model.VanadisChunk{ID: "c-1", Repo: "owner/repo"},
			wantErr:     true,
		},
		{
			name:        "patch pr error propagates",
			initialBody: "Notes",
			patchStatus: http.StatusInternalServerError,
			nodeID:      "PR_kwDO12345",
			build:       model.VanadisBuild{ID: "b-1"},
			chunks: []model.VanadisChunk{
				{ID: "c-1", Repo: "owner/repo"},
				{ID: "c-2", Repo: "owner/repo"},
			},
			chunk:   model.VanadisChunk{ID: "c-1", Repo: "owner/repo"},
			wantErr: true,
		},
		{
			name:        "empty node id skips automerge",
			initialBody: "Notes",
			nodeID:      "",
			build:       model.VanadisBuild{ID: "b-stand"},
			chunks:      []model.VanadisChunk{{ID: "b-stand-chunk-1", Repo: "owner/repo"}},
			chunk:       model.VanadisChunk{ID: "b-stand-chunk-1", Repo: "owner/repo"},
		},
		{
			name:          "graphql error is swallowed with warning",
			initialBody:   "Standalone description",
			nodeID:        "PR_kwDO12345",
			graphqlStatus: http.StatusInternalServerError,
			build:         model.VanadisBuild{ID: "b-stand"},
			chunks:        []model.VanadisChunk{{ID: "b-stand-chunk-1", Repo: "owner/repo"}},
			chunk:         model.VanadisChunk{ID: "b-stand-chunk-1", Repo: "owner/repo"},
		},
		{
			name:        "no token on local url skips auth header but automerges",
			initialBody: "Initial PR description",
			nodeID:      "PR_kwDO12345",
			noToken:     true,
			build:       model.VanadisBuild{ID: "b-1", Title: "Multi Build"},
			chunks: []model.VanadisChunk{
				{ID: "c-1", ChunkIndex: 0, Repo: "owner/repo"},
				{ID: "c-2", ChunkIndex: 1, Repo: "owner/repo"},
			},
			chunk:         model.VanadisChunk{ID: "c-1", ChunkIndex: 0, Repo: "owner/repo"},
			wantPatched:   true,
			wantAutomated: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var patched bool
			var automerged bool
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handleAutoMergeRequest(t, w, r, tc, &patched, &automerged)
			}))
			defer srv.Close()
			tok := "test-token"
			if tc.noToken {
				tok = ""
			}
			exec := NewExecutor("", nil, nil, tok, "example-owner", srv.Client())
			exec.GitHubAPIURL = srv.URL
			err := exec.EnsurePRAutoMergeAndContext(context.Background(), tc.build, tc.chunks, tc.chunk, 10)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.wantPatched, patched)
			assert.Equal(t, tc.wantAutomated, automerged)
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestEnablePRAutoMerge(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		nodeID       string
		token        string
		apiURL       string
		respStatus   int
		respBody     string
		transportErr bool
		wantRequests int
		wantAuth     string
		wantErr      bool
	}{
		{
			name:         "remote url without token returns early",
			nodeID:       "PR_123",
			token:        "",
			apiURL:       "https://api.github.com",
			wantRequests: 0,
		},
		{
			name:         "remote url containing localhost query without token returns early",
			nodeID:       "PR_123",
			token:        "",
			apiURL:       "https://example.com/?x=localhost",
			wantRequests: 0,
		},
		{
			name:         "empty node id returns early",
			nodeID:       "",
			token:        "tok",
			apiURL:       "https://api.github.com",
			wantRequests: 0,
		},
		{
			name:         "mutation request with token succeeds",
			nodeID:       "PR_123",
			token:        "secret-token",
			apiURL:       "https://api.github.com",
			respStatus:   http.StatusOK,
			respBody:     `{"data":{"enablePullRequestAutoMerge":{}}}`,
			wantRequests: 1,
			wantAuth:     "Bearer secret-token",
		},
		{
			name:         "graphql response with error array handled",
			nodeID:       "PR_123",
			token:        "secret-token",
			apiURL:       "https://api.github.com",
			respStatus:   http.StatusOK,
			respBody:     `{"errors":[{"message":"auto-merge is not allowed"}]}`,
			wantRequests: 1,
			wantAuth:     "Bearer secret-token",
			wantErr:      true,
		},
		{
			name:         "non-200 status code handled",
			nodeID:       "PR_123",
			token:        "secret-token",
			apiURL:       "https://api.github.com",
			respStatus:   http.StatusInternalServerError,
			respBody:     `{}`,
			wantRequests: 1,
			wantAuth:     "Bearer secret-token",
			wantErr:      true,
		},
		{
			name:         "network transport error handled",
			nodeID:       "PR_123",
			token:        "secret-token",
			apiURL:       "https://api.github.com",
			transportErr: true,
			wantRequests: 1,
			wantAuth:     "Bearer secret-token",
			wantErr:      true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var reqCount int
			var capturedAuth string
			var capturedPath string
			client := &http.Client{
				Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					reqCount++
					capturedAuth = r.Header.Get("Authorization")
					capturedPath = r.URL.Path
					if tc.transportErr {
						return nil, fmt.Errorf("simulated network error")
					}
					status := tc.respStatus
					if status == 0 {
						status = http.StatusOK
					}
					return &http.Response{
						StatusCode: status,
						Header:     make(http.Header),
						Body:       io.NopCloser(strings.NewReader(tc.respBody)),
						Request:    r,
					}, nil
				}),
			}
			exec := NewExecutor("", nil, nil, tc.token, "example-owner", client)
			exec.GitHubAPIURL = tc.apiURL
			err := exec.enablePRAutoMerge(context.Background(), tc.nodeID)
			assert.Equal(t, tc.wantErr, err != nil)
			assert.Equal(t, tc.wantRequests, reqCount)
			if tc.wantRequests > 0 {
				assert.Equal(t, "/graphql", capturedPath)
				assert.Equal(t, tc.wantAuth, capturedAuth)
			}
		})
	}
}

func TestListOpenPullRequests(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		repo      string
		status    int
		body      string
		nilExec   bool
		wantCount int
		wantErr   bool
	}{
		{
			name:      "successful list",
			repo:      "owner/repo",
			status:    200,
			body:      `[{"number":1,"title":"PR 1","head":{"ref":"feat/1","sha":"aaa"}},{"number":2,"title":"PR 2","head":{"ref":"feat/2","sha":"bbb"}}]`,
			wantCount: 2,
		},
		{
			name:      "unqualified repo qualified by owner",
			repo:      "repo",
			status:    200,
			body:      `[{"number":1,"title":"PR 1","head":{"ref":"feat/1","sha":"aaa"}}]`,
			wantCount: 1,
		},
		{
			name:    "server error returns error",
			repo:    "owner/repo",
			status:  500,
			body:    `{"message":"internal error"}`,
			wantErr: true,
		},
		{
			name:    "nil executor returns error",
			repo:    "owner/repo",
			nilExec: true,
			wantErr: true,
		},
		{
			name:    "empty repo returns error",
			repo:    "",
			status:  200,
			body:    `[]`,
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.True(t, r.URL.Path == "/repos/owner/repo/pulls" || r.URL.Path == "/repos/example-owner/repo/pulls")
				assert.Equal(t, "state=open&per_page=100&page=1", r.URL.RawQuery)
				w.WriteHeader(tc.status)
				n, err := w.Write([]byte(tc.body))
				assert.NoError(t, err)
				assert.Positive(t, n)
			}))
			defer srv.Close()
			var exec *Executor
			if !tc.nilExec {
				exec = NewExecutor("", nil, nil, "token", "example-owner", srv.Client())
				exec.GitHubAPIURL = srv.URL
			}
			prs, err := exec.ListOpenPullRequests(context.Background(), tc.repo)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Len(t, prs, tc.wantCount)
		})
	}
}

func TestListOpenPullRequests_Pagination(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		wantPages []string
		wantCount int
	}{
		{
			name:      "multi-page response concatenated until short page",
			wantPages: []string{"1", "2"},
			wantCount: 101,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var requestedPages []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				page := r.URL.Query().Get("page")
				requestedPages = append(requestedPages, page)
				w.WriteHeader(http.StatusOK)
				if page == "1" {
					var prs []map[string]any
					for i := 1; i <= 100; i++ {
						prs = append(prs, map[string]any{"number": i, "title": fmt.Sprintf("PR %d", i)})
					}
					_ = json.NewEncoder(w).Encode(prs)
					return
				}
				_ = json.NewEncoder(w).Encode([]map[string]any{{"number": 101, "title": "PR 101"}})
			}))
			defer srv.Close()
			exec := NewExecutor("", nil, nil, "token", "example-owner", srv.Client())
			exec.GitHubAPIURL = srv.URL
			prs, err := exec.ListOpenPullRequests(context.Background(), "owner/repo")
			assert.NoError(t, err)
			assert.Len(t, prs, tc.wantCount)
			assert.Equal(t, tc.wantPages, requestedPages)
		})
	}
}

func TestIsLoopbackAPIURL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		raw  string
		want bool
	}{
		{name: "ipv4 loopback", raw: "http://127.0.0.1:8080", want: true},
		{name: "ipv4 loopback bare", raw: "http://127.0.0.1", want: true},
		{name: "localhost", raw: "http://localhost:8080", want: true},
		{name: "localhost uppercase", raw: "http://LOCALHOST:8080", want: true},
		{name: "ipv6 loopback", raw: "http://[::1]:8080", want: true},
		{name: "remote public api", raw: "https://api.github.com", want: false},
		{name: "query param containing localhost", raw: "https://example.com/?x=localhost", want: false},
		{name: "subdomain of example", raw: "https://localhost.example.com", want: false},
		{name: "empty url", raw: "", want: false},
		{name: "invalid url", raw: "://invalid", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, isLoopbackAPIURL(tc.raw))
		})
	}
}
