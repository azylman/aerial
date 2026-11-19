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
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// The database supplies an already recovered chunk. This tests the real HTTP
// remediation wake, not persistence of the preceding attachment transaction.
func TestRecoveredChunkRemediationWake(t *testing.T) {
	t.Parallel()
	st, _ := dbScript{buildRow: &mockRow{b: &model.VanadisBuild{ID: "b1", ThreadID: "thread-1", Status: "running"}}, chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", Repo: "aerial", Status: "pr_opened", PRNumber: 101}}}.open()
	payloads := make(chan BrainDispatchRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req BrainDispatchRequest
		if r.URL.Path != "/dispatch" || r.Method != http.MethodPost {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		payloads <- req
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	bo := NewBuildObject("", nil).WithAuthority(st, NewExecutor(t.TempDir(), NewHTTPEriClient(server.URL, server.Client()), nil, "", nil))
	ctx := makeObjectMockContext(t, "b1")
	_, err := restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		return true, bo.handlePREventType(rc, "b1", "c1", "ci_failed", pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, CheckName: "unit", Details: "unit test failed"})
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case req := <-payloads:
		if req.TargetID != "thread-1" || req.SessionID != "vanadis-c1" || req.VanadisChunkID != "c1" || !strings.Contains(req.Prompt, "unit test failed") || !strings.Contains(req.Prompt, "101") {
			t.Fatalf("request = %+v", req)
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
		req         store.AttachChunkPRRequest
		db          dbScript
		wantCode    restate.Code
		wantMessage string
	}{
		{"missing action repo", attachRequest("c1", ""), dbScript{routes: attachRoutes("action", true, false)}, 400, "repo is required"},
		{"blank action repo", attachRequest("c1", "  "), dbScript{routes: attachRoutes("action", true, false)}, 400, "repo is required"},
		{"standalone repo", attachRequest("", ""), dbScript{}, 400, "repo is required"},
		{"invalid number", store.AttachChunkPRRequest{PRNumber: -1}, dbScript{}, 400, "valid pr number"},
		{"transient database error", attachRequest("c1", "aerial"), dbScript{row: errRow(errors.New("connection interrupted"))}, 0, "connection interrupted"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := tc.db.open()
			bo := NewBuildObject("", nil).WithAuthority(st, nil)
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
			_, err := bo.executePRAttachment(restate.WithMockContext(mc), tc.req)
			require.Error(t, err)
		})
	}
}
