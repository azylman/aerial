package build

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

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
			exec := executor.NewExecutor(t.TempDir(), eri, pubsub.NewMemoryBus(), "token", "example-owner", nil)
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

func TestBuildObject_RefreshFailedPRWidget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		build     *model.VanadisBuild
		getErr    error
		syncErr   error
		noExec    bool
		wantCalls int
	}{
		{name: "nil executor no-op", noExec: true, wantCalls: 0},
		{name: "store read error skips widget sync", getErr: errors.New("db error"), wantCalls: 0},
		{name: "non-failed build skips widget sync", build: &model.VanadisBuild{ID: "b1", Status: "running"}, wantCalls: 0},
		{name: "failed build sync widget error logged", build: &model.VanadisBuild{ID: "b1", Status: "failed"}, syncErr: errors.New("sync error"), wantCalls: 1},
		{name: "failed build sync widget success", build: &model.VanadisBuild{ID: "b1", Status: "failed"}, wantCalls: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := dbScript{buildRow: &mockRow{b: tc.build}, queryErr: tc.getErr}.open()
			var widgetCalls int
			var exec *executor.Executor
			if !tc.noExec {
				eri := &mockEriClient{
					widgetFn: func(context.Context, executor.StatusWidgetRequest) (string, error) {
						widgetCalls++
						return "", tc.syncErr
					},
				}
				exec = executor.NewExecutor(t.TempDir(), eri, pubsub.NewMemoryBus(), "token", "example-owner", nil)
			}
			bo := NewBuildObject("", nil).WithAuthority(st, exec)
			bo.refreshFailedPRWidget(context.Background(), "b1")
			assert.Equal(t, tc.wantCalls, widgetCalls)
		})
	}
}
