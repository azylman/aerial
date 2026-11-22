package executor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

func TestDispatchAccepted(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		body       string
		code       int
		reqID      string
		wantErr    bool
		wantActive bool
	}{
		{name: "accepted", body: `{"status":"accepted","dispatch_id":"d1"}`, code: 202, reqID: "d1"},
		{name: "already exists", body: `{"status":"already_exists","dispatch_id":"d1"}`, code: 200, reqID: "d1"},
		{name: "skipped active", body: `{"status":"skipped_active","dispatch_id":"d1"}`, code: 200, reqID: "d1", wantErr: true, wantActive: true},
		{name: "wrong dispatch id", body: `{"status":"accepted","dispatch_id":"other"}`, code: 202, reqID: "d1", wantErr: true},
		{name: "empty request id", body: `{"status":"accepted","dispatch_id":"d1"}`, code: 202, reqID: "", wantErr: true},
		{name: "missing status", body: `{}`, code: 200, reqID: "d1", wantErr: true},
		{name: "malformed body", body: `invalid`, code: 200, reqID: "d1", wantErr: true},
		{name: "server failure", body: `{}`, code: 500, reqID: "d1", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.code)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			c := NewHTTPEriClient(srv.URL, srv.Client())
			err := c.DispatchAccepted(context.Background(), eriwire.BrainDispatchRequest{ID: tc.reqID})
			if tc.wantActive {
				assert.ErrorIs(t, err, eriwire.ErrDispatchActive)
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

func TestFetchGitHubPullRequest(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		nilExec   bool
		repo      string
		number    int
		status    int
		body      string
		wantOwner string
		wantFull  string
		wantErr   bool
	}{
		{name: "nil executor", nilExec: true, repo: "owner/repo", number: 12, wantErr: true},
		{name: "empty repo", repo: "", number: 12, wantErr: true},
		{name: "zero pr number", repo: "owner/repo", number: 0, wantErr: true},
		{name: "api error", repo: "owner/repo", number: 12, status: 404, body: `{}`, wantErr: true},
		{name: "invalid json", repo: "owner/repo", number: 12, status: 200, body: `invalid`, wantErr: true},
		{name: "ok", repo: "owner/repo", number: 12, status: 200, body: `{"state":"open"}`, wantOwner: "owner", wantFull: "owner/repo"},
		{name: "bare repo resolves owner", repo: "aerial", number: 12, status: 200, body: `{"state":"open"}`, wantOwner: "azylman", wantFull: "azylman/aerial"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			var exec *Executor
			if !tc.nilExec {
				exec = NewExecutor("", nil, nil, "", srv.Client())
				exec.GitHubAPIURL = srv.URL
			}
			pr, owner, full, err := exec.fetchGitHubPullRequest(context.Background(), tc.repo, tc.number)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.NotNil(t, pr)
			assert.Equal(t, tc.wantOwner, owner)
			assert.Equal(t, tc.wantFull, full)
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
			exec := NewExecutor("", nil, nil, "", srv.Client())
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
			exec := NewExecutor("", nil, nil, "", srv.Client())
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
