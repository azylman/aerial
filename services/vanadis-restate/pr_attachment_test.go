package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
)

func TestFirstAttachmentRecovery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, parent, sibling string
		wantConflict          bool
	}{
		{"failed parent", "failed", "completed", false},
		{"running parent", "running", "pending", false},
		{"failed sibling", "failed", "failed", true},
		{"cancelled sibling", "failed", "cancelled", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := VanadisBuild{ID: "b1", Status: tc.parent}
			store, _ := dbScript{buildRow: &mockRow{b: &b}, chunks: []VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", Repo: "aerial", Status: "failed"}, {ID: "other", Status: tc.sibling}}, routes: []sqlRoute{{match: "UPDATE vanadis_chunks", row: valuesRow("b1", "c1", "")}, {match: "SELECT mode", row: valuesRow("pr")}, {match: "SELECT build_id", row: valuesRow("b1")}}, row: valuesRow("b1", "c1", "")}.open()
			_, _, _, err := store.AttachChunkPR(context.Background(), attachRequest("c1", "aerial"))
			if tc.wantConflict {
				assertStoreErr(t, err, ErrBuildConflict, false)
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func attachmentScript(chunk, build, thread, status, repo string) dbScript {
	return dbScript{routes: []sqlRoute{{match: "SELECT mode", row: valuesRow("pr")}, {match: "SELECT build_id", row: valuesRow(build)}}, buildRow: &mockRow{b: &VanadisBuild{ID: build, ThreadID: thread, Status: status}}, chunks: []VanadisChunk{{ID: chunk, BuildID: build, Mode: "pr", Repo: repo, Status: "running"}}}
}

// This fixture exercises Go selection and error propagation, not SQL effects or row locking.
func TestAttachmentSelectionAndErrors(t *testing.T) {
	t.Parallel()
	writeErr := errors.New("write failed")
	tests := []struct {
		name, parent, status, sibling, failure string
		previous, cap, index                   int
		commitErr, wantErr                     error
	}{
		{name: "first", parent: "failed", status: "failed"},
		{name: "middle", parent: "failed", status: "failed", index: 1},
		{name: "final", parent: "failed", status: "failed", index: 2},
		{name: "duplicate after failure", parent: "failed", status: "failed", previous: 101},
		{name: "cancelled chunk", parent: "running", status: "cancelled", wantErr: ErrBuildConflict},
		{name: "completed different PR", parent: "running", status: "completed", previous: 100, wantErr: ErrBuildConflict},
		{name: "duplicate at cap", parent: "failed", status: "failed", previous: 101, cap: 3},
		{name: "cancelled parent", parent: "cancelled", status: "failed", wantErr: ErrBuildConflict},
		{name: "completed parent", parent: "completed", status: "failed", wantErr: ErrBuildConflict},
		{name: "failed sibling", parent: "failed", status: "failed", sibling: "failed", wantErr: ErrBuildConflict},
		{name: "cancelled sibling", parent: "failed", status: "failed", sibling: "cancelled", wantErr: ErrBuildConflict},
		{name: "chunk write failure", parent: "failed", status: "failed", failure: "UPDATE vanadis_chunks", wantErr: writeErr},
		{name: "parent write failure", parent: "failed", status: "failed", failure: "UPDATE vanadis_builds", wantErr: writeErr},
		{name: "commit failure", parent: "failed", status: "failed", commitErr: writeErr, wantErr: writeErr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chunks := []VanadisChunk{
				{ID: "before", Status: "completed", Repo: "other", ChunkIndex: 0},
				{ID: "between", Status: "completed", Repo: "other", ChunkIndex: 1},
				{ID: "after", Status: "completed", Repo: "other", ChunkIndex: 2},
			}
			chunks[tc.index] = VanadisChunk{ID: "selected", BuildID: "b1", Mode: "pr", Repo: "aerial", Status: tc.status, ChunkIndex: tc.index, PRNumber: tc.previous, CIFailCount: tc.cap}
			if tc.sibling != "" {
				chunks[(tc.index+1)%3].Status = tc.sibling
			}
			script := attachmentScript("selected", "b1", "thread-1", tc.parent, "aerial")
			script.chunks = chunks
			script.failExec, script.execErr, script.commitErr = tc.failure, writeErr, tc.commitErr
			// No failure route means writes succeed; this fake does not apply SQL state changes.
			if tc.failure == "" {
				script.execErr = nil
			}
			store, _ := script.open()
			b, c, th, err := store.AttachChunkPR(context.Background(), attachRequest("selected", "aerial"))
			if assertStoreErr(t, err, tc.wantErr, false) {
				return
			}
			if b != "b1" || c != "selected" || th != "thread-1" {
				t.Fatalf("attachment = %q %q %q", b, c, th)
			}
		})
	}
}

func TestFirstWorkerFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, mode, status string
		previous, failures int
		want               bool
	}{
		{"eligible", "pr", "failed", 0, 0, true},
		{"already attached", "pr", "failed", 101, 0, false},
		{"CI attempts", "pr", "failed", 0, 3, false},
		{"action", "action", "failed", 0, 0, false},
		{"running", "pr", "running", 0, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := firstWorkerFailure(VanadisChunk{Mode: tc.mode, Status: tc.status, PRNumber: tc.previous, CIFailCount: tc.failures})
			if got != tc.want {
				t.Fatalf("eligible = %v, want %v", got, tc.want)
			}
		})
	}
}

// The database supplies an already recovered chunk. This tests the real HTTP
// remediation wake, not persistence of the preceding attachment transaction.
func TestRecoveredChunkRemediationWake(t *testing.T) {
	t.Parallel()
	store, _ := dbScript{buildRow: &mockRow{b: &VanadisBuild{ID: "b1", ThreadID: "thread-1", Status: "running"}}, chunks: []VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", Repo: "aerial", Status: "pr_opened", PRNumber: 101}}}.open()
	payloads := make(chan map[string]string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]string
		if r.URL.Path != "/prompt" || r.Method != http.MethodPost {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		payloads <- payload
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	bo := NewBuildObject("", nil).WithAuthority(store, NewExecutor(t.TempDir(), NewHTTPEriClient(server.URL, server.Client()), nil, "", nil))
	ctx := makeObjectMockContext(t, "b1")
	_, err := restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		return true, bo.handlePREventType(rc, "b1", "c1", "ci_failed", pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, CheckName: "unit", Details: "unit test failed"})
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case payload := <-payloads:
		if payload["channel_id"] != "thread-1" || !strings.Contains(payload["prompt"], "unit test failed") || !strings.Contains(payload["prompt"], "101") {
			t.Fatalf("payload = %+v", payload)
		}
	default:
		t.Fatal("no remediation wake")
	}
}

// Inspect the callback error before the SDK can retry the journaled operation.
func TestExecutePRAttachment(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		req         AttachChunkPRRequest
		db          dbScript
		wantCode    restate.Code
		wantMessage string
	}{
		{"missing action repo", attachRequest("c1", ""), dbScript{routes: attachRoutes("action", true, false)}, 400, "repo is required"},
		{"blank action repo", attachRequest("c1", "  "), dbScript{routes: attachRoutes("action", true, false)}, 400, "repo is required"},
		{"standalone repo", attachRequest("", ""), dbScript{}, 400, "repo is required"},
		{"invalid number", AttachChunkPRRequest{PRNumber: -1}, dbScript{}, 400, "valid pr number"},
		{"transient database error", attachRequest("c1", "aerial"), dbScript{row: errRow(errors.New("connection interrupted"))}, 0, "connection interrupted"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, _ := tc.db.open()
			bo := NewBuildObject("", nil).WithAuthority(store, nil)
			mc := mocks.NewMockContext(t)
			mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(fn func(restate.RunContext) (any, error), _ any, _ ...restate.RunOption) restate.TerminalError {
				_, err := fn(mockRunContext{Context: context.Background()})
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantMessage)
				var terminal restate.TerminalError
				if tc.wantCode > 0 {
					require.True(t, errors.As(err, &terminal), "journal callback must stop retries: %v", err)
					assert.Equal(t, tc.wantCode, terminal.Code())
				} else {
					assert.False(t, errors.As(err, &terminal), "transient failures must remain retryable")
				}
				return restate.ToTerminalError(err)
			})
			recorded := false
			_, err := bo.executePRAttachment(restate.WithMockContext(mc), tc.req, &recorded)
			require.Error(t, err)
		})
	}
}
