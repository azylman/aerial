package build

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// The database supplies an already recovered chunk. This tests the real HTTP
// remediation wake, not persistence of the preceding attachment transaction.
func TestRecoveredChunkRemediationWake(t *testing.T) {
	t.Parallel()
	st, _ := dbScript{buildRow: &mockRow{b: &model.VanadisBuild{ID: "b1", ThreadID: "thread-1", Status: "running"}}, chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", Repo: "aerial", Status: "pr_opened", PRNumber: 101}}}.open()
	payloads := make(chan eriwire.BrainDispatchRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/discord/widget" {
			w.Write([]byte(`{}`))
			return
		}
		var req eriwire.BrainDispatchRequest
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
	bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor(t.TempDir(), executor.NewHTTPEriClient(server.URL, server.Client()), nil, "", "azylman", nil))
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
		wantErr     bool
	}{
		{"missing action repo", attachRequest("c1", ""), dbScript{routes: attachRoutes("action", true, false)}, 400, "repo is required", true},
		{"blank action repo", attachRequest("c1", "  "), dbScript{routes: attachRoutes("action", true, false)}, 400, "repo is required", true},
		{"standalone repo", attachRequest("", ""), dbScript{}, 400, "repo is required", true},
		{"invalid number", store.AttachChunkPRRequest{PRNumber: -1}, dbScript{}, 400, "valid pr number", true},
		{"transient database error", attachRequest("c1", "aerial"), dbScript{row: errRow(errors.New("connection interrupted"))}, 0, "connection interrupted", true},
		{
			name: "successful attach skips sync widget during attach step",
			req:  attachRequest("c1", "aerial"),
			db: dbScript{
				routes: []sqlRoute{
					{match: "SELECT status FROM vanadis_builds", row: valuesRow("running")},
					{match: "SELECT mode FROM vanadis_chunks", row: valuesRow("pr")},
					{match: "SELECT build_id FROM vanadis_chunks", row: valuesRow("b1")},
					{match: "RETURNING b.id, c.id, b.thread_id", row: valuesRow("b1", "c1", "th-1")},
					{match: "WHERE c.id = $1", row: failRow("running", "running")},
				},
				row:      valuesRow("running"),
				buildRow: &mockRow{b: &model.VanadisBuild{ID: "b1", ThreadID: "th-1", Status: "running", Title: "Build", StatusMessageID: "msg-123"}},
				chunks:   []model.VanadisChunk{{ID: "c1", BuildID: "b1", Status: "running", Repo: "aerial", Mode: "pr"}},
				countRow: valuesRow(1),
				execTag:  "UPDATE 1",
			},
			wantErr: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := tc.db.open()
			var attachStepWidgetCalls atomic.Int32
			eri := &mockEriClient{
				widgetFn: func(ctx context.Context, req executor.StatusWidgetRequest) (string, error) {
					attachStepWidgetCalls.Add(1)
					return "msg-123", nil
				},
			}
			exec := executor.NewExecutor(t.TempDir(), eri, pubsub.NewMemoryBus(), "token", "azylman", nil)
			fakeRT := newFakeObjectRuntime("b1")
			bo := NewBuildObject("", nil, func(restate.ObjectContext) objectRuntime { return fakeRT }).WithAuthority(st, exec)
			mc := mocks.NewMockContext(t)
			mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(fn func(restate.RunContext) (any, error), output any, _ ...restate.RunOption) restate.TerminalError {
				beforeCalls := attachStepWidgetCalls.Load()
				res, err := fn(mockRunContext{Context: context.Background()})
				if tc.wantErr {
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
				}
				require.NoError(t, err)
				assert.Equal(t, beforeCalls, attachStepWidgetCalls.Load(), "attach-pr-postgres step must not call SyncBuildWidget")
				if output != nil && res != nil {
					outVal := reflect.ValueOf(output)
					resVal := reflect.ValueOf(res)
					if outVal.Kind() == reflect.Pointer && !outVal.IsNil() && outVal.Elem().Type() == resVal.Type() {
						outVal.Elem().Set(resVal)
					}
				}
				return nil
			})
			resp, err := bo.executePRAttachment(restate.WithMockContext(mc), tc.req)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, "attached", resp.Status)
			assert.Equal(t, "b1", resp.BuildID)
		})
	}
}
