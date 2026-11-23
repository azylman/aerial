package restatetest_test

import (
	"errors"
	"net/http"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/restatetest"
)

func TestObjectHarness_StateAndSends(t *testing.T) {
	t.Parallel()

	h := restatetest.NewObjectHarness(t, "sched-1",
		restatetest.WithInitialState("gen", int64(10)),
		restatetest.WithKeepTerminalOnly(),
	)

	// Test Initial state and State retrieval
	val, ok := h.State("gen")
	require.True(t, ok)
	require.Equal(t, int64(10), val)

	// Test restate.Get on seeded key
	gotGen, err := restate.Get[int64](h.ObjCtx, "gen")
	require.NoError(t, err)
	require.Equal(t, int64(10), gotGen)

	// Test WithState helper and restate.Set
	restatetest.WithState(h, "label", "alpha")
	gotLabel, err := restate.Get[string](h.ObjCtx, "label")
	require.NoError(t, err)
	require.Equal(t, "alpha", gotLabel)

	restate.Set(h.ObjCtx, "label", "beta")
	gotLabel2, err := restate.Get[string](h.ObjCtx, "label")
	require.NoError(t, err)
	require.Equal(t, "beta", gotLabel2)

	// Test restate.Clear
	restate.Clear(h.ObjCtx, "label")
	gotClearedLabel, err := restate.Get[string](h.ObjCtx, "label")
	require.NoError(t, err)
	require.Empty(t, gotClearedLabel)

	// Test ClearedAll before and after restate.ClearAll
	require.False(t, h.ClearedAll())
	restate.ClearAll(h.ObjCtx)
	require.True(t, h.ClearedAll())
	gotGenAfterClear, err := restate.Get[int64](h.ObjCtx, "gen")
	require.NoError(t, err)
	require.Zero(t, gotGenAfterClear)

	// Test nil state handling (leaves target unchanged and reports present)
	h.SetState("nil-key", nil)
	var gotNil *string
	present, err := h.Context.Get("nil-key", &gotNil)
	require.NoError(t, err)
	require.True(t, present)
	require.Nil(t, gotNil)

	// Test Object & Send interception across arities
	client := h.Context.Object("ServiceA", "key-a", "Method1")
	client.Send("payload-1")

	client2 := h.Context.Object("ServiceB", "key-b", "Method2")
	client2.Send("payload-2", restate.WithDelay(5*time.Second))

	client3 := h.Context.Object("ServiceC", "key-c", "Method3")
	client3.Send("payload-3", restate.WithDelay(time.Minute), restate.WithDelay(2*time.Minute))

	sends := h.Sends()
	require.Len(t, sends, 3)
	require.Equal(t, "ServiceA", sends[0].Service)
	require.Equal(t, "key-a", sends[0].Key)
	require.Equal(t, "Method1", sends[0].Method)
	require.Equal(t, "payload-1", sends[0].Input)
	require.False(t, sends[0].Delayed)

	require.Equal(t, "ServiceB", sends[1].Service)
	require.Equal(t, "payload-2", sends[1].Input)
	require.True(t, sends[1].Delayed)
	require.Len(t, sends[1].Options, 1)

	require.Equal(t, "ServiceC", sends[2].Service)
	require.Equal(t, "payload-3", sends[2].Input)
	require.True(t, sends[2].Delayed)
	require.Len(t, sends[2].Options, 2)
}

func TestObjectHarness_RunInterceptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		keepTerminal  bool
		retVal        string
		retErr        error
		termCode      int
		runOpts       []restate.RunOption
		wantOutput    string
		wantErrCode   int
		wantHasCap    bool
		wantTerminal  bool
		wantRunErrNil bool
	}{
		{
			name:          "success with return value",
			retVal:        "computed-result",
			wantOutput:    "computed-result",
			wantRunErrNil: true,
		},
		{
			name:        "retryable error wrapped with MockRetryableCode by default",
			retErr:      errors.New("transient network issue"),
			wantErrCode: restatetest.MockRetryableCode,
		},
		{
			name:         "retryable error silenced when keepTerminalOnly set",
			keepTerminal: true,
			retErr:       errors.New("transient error to drop"),
			wantOutput:   "",
		},
		{
			name:         "terminal error preserved even when keepTerminalOnly set",
			keepTerminal: true,
			retErr:       errors.New("not found"),
			termCode:     http.StatusNotFound,
			wantErrCode:  http.StatusNotFound,
			wantTerminal: true,
		},
		{
			name:          "max retry cap detected",
			retVal:        "ok",
			runOpts:       []restate.RunOption{restate.WithMaxRetryAttempts(5)},
			wantOutput:    "ok",
			wantHasCap:    true,
			wantRunErrNil: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var opts []restatetest.Option
			if tc.keepTerminal {
				opts = append(opts, restatetest.WithKeepTerminalOnly())
			}
			h := restatetest.NewObjectHarness(t, "run-key", opts...)

			res, err := restate.Run(h.ObjCtx, func(rc restate.RunContext) (string, error) {
				require.NotNil(t, rc.Log())
				require.NotNil(t, rc.Request())
				if tc.termCode != 0 {
					return "", restate.ToTerminalError(tc.retErr, restate.WithErrorCode(restate.Code(tc.termCode)))
				}
				if tc.retErr != nil {
					return "", tc.retErr
				}
				return tc.retVal, nil
			}, tc.runOpts...)
			require.Equal(t, tc.wantOutput, res)
			if tc.wantErrCode != 0 {
				require.Error(t, err)
				require.Equal(t, tc.wantErrCode, int(restate.AsTerminalError(err).Code()))
			} else {
				require.NoError(t, err)
			}

			require.True(t, h.Ran())
			require.Equal(t, tc.wantHasCap, h.HasRetryCap())
			require.Equal(t, tc.wantTerminal, h.IsTerminal())
			if tc.wantRunErrNil {
				require.NoError(t, h.RunErr())
			} else {
				require.Error(t, h.RunErr())
			}
		})
	}
}

func TestTerminalCode(t *testing.T) {
	t.Parallel()

	require.Equal(t, 0, restatetest.TerminalCode(nil))
	require.Equal(t, 0, restatetest.TerminalCode(errors.New("standard error")))
	require.Equal(t, 0, restatetest.TerminalCode(restate.ToTerminalError(errors.New("retryable"), restate.WithErrorCode(restatetest.MockRetryableCode))))
	require.Equal(t, http.StatusNotFound, restatetest.TerminalCode(restate.ToTerminalError(errors.New("missing"), restate.WithErrorCode(http.StatusNotFound))))
	require.Equal(t, http.StatusConflict, restatetest.TerminalCode(restate.ToTerminalError(errors.New("conflict"), restate.WithErrorCode(http.StatusConflict))))
}

func TestObjectHarness_RunErrorOverride(t *testing.T) {
	t.Parallel()

	h := restatetest.NewObjectHarness(t, "override-key", restatetest.WithRunError(errors.New("forced failure")))
	_, err := restate.Run(h.ObjCtx, func(rc restate.RunContext) (string, error) {
		return "ok", nil
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "forced failure")
	require.True(t, h.Ran())
	require.True(t, h.IsTerminal())

	// Test FailRun dynamically updating the error
	h2 := restatetest.NewObjectHarness(t, "override-key-2")
	h2.FailRun(errors.New("runtime forced failure"))
	_, err2 := restate.Run(h2.ObjCtx, func(rc restate.RunContext) (string, error) {
		return "ok", nil
	})
	require.Error(t, err2)
	require.Contains(t, err2.Error(), "runtime forced failure")
}
