package executor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
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
				_, _ = w.Write([]byte(tc.body))
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
				_, _ = w.Write([]byte(tc.body))
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
		{name: "api unavailable", prBody: `{}`, status: 500, branch: "main", headSHA: "deadbee", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.prBody))
			}))
			defer srv.Close()
			exec := NewExecutor("", nil, nil, "", "example-owner", srv.Client())
			exec.GitHubAPIURL = srv.URL
			build := model.VanadisBuild{ID: "b", ThreadID: "th-1"}
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
