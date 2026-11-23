package queue

import (
	"context"
	"log/slog"
	"reflect"
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockRunContext struct {
	context.Context
}

func (mockRunContext) Log() *slog.Logger         { return slog.Default() }
func (mockRunContext) Request() *restate.Request { return &restate.Request{ID: "req-1"} }

func setupActorMockContext(t *testing.T, key, activeInvID string) (*mocks.MockContext, *mocks.MockClient) {
	t.Helper()
	mc := mocks.NewMockContext(t)
	mc.On("Key").Return(key).Maybe()
	mc.On("Request").Return(&restate.Request{ID: "inv-turn-123"}).Maybe()
	mc.On("Value", mock.Anything).Return(nil).Maybe()

	if activeInvID != "" {
		mc.On("Get", "active_invocation_id", mock.Anything).Run(func(args mock.Arguments) {
			if outPtr, ok := args.Get(1).(*string); ok && outPtr != nil {
				*outPtr = activeInvID
			}
		}).Return(true, nil).Maybe()
	} else {
		mc.On("Get", "active_invocation_id", mock.Anything).Return(false, nil).Maybe()
	}

	mc.On("Set", "active_invocation_id", mock.Anything).Return().Maybe()
	mc.On("Clear", "active_invocation_id").Return().Maybe()
	mc.On("ClearAll").Return().Maybe()
	mc.On("CancelInvocation", mock.Anything).Return().Maybe()

	runHandler := func(fn func(restate.RunContext) (any, error), output any, opts ...restate.RunOption) restate.TerminalError {
		res, err := fn(mockRunContext{Context: context.Background()})
		if err != nil {
			return restate.ToTerminalError(err)
		}
		if output != nil && res != nil {
			outVal := reflect.ValueOf(output)
			resVal := reflect.ValueOf(res)
			if outVal.Kind() == reflect.Pointer && !outVal.IsNil() && outVal.Elem().Type() == resVal.Type() {
				outVal.Elem().Set(resVal)
			}
		}
		return nil
	}
	mc.On("Run", mock.Anything, mock.Anything).Return(runHandler).Maybe()
	mc.On("Run", mock.Anything, mock.Anything, mock.Anything).Return(runHandler).Maybe()

	mClient := mocks.NewMockClient(t)
	mInv := mocks.NewMockInvocation(t)
	mc.On("Object", mock.Anything, mock.Anything, mock.Anything).Return(mClient).Maybe()
	mClient.On("Send", mock.Anything).Return(mInv).Maybe()

	return mc, mClient
}

func assertTerminalOrRetriableError(t *testing.T, err error, wantErrorSub string, wantTerminal bool) {
	t.Helper()
	require.Error(t, err)
	assert.Contains(t, err.Error(), wantErrorSub)
	require.True(t, restate.IsTerminalError(err))
	termErr := restate.AsTerminalError(err)
	if wantTerminal {
		assert.Equal(t, restate.Code(400), termErr.Code(), "expected 400 terminal error")
	} else {
		assert.Equal(t, restate.Code(500), termErr.Code(), "expected non-terminal retriable error")
	}
}

func TestDeriveDeterministicIDs(t *testing.T) {
	t.Parallel()

	assert.NotEmpty(t, deriveDeterministicMessageID(nil, "th-1"))
	assert.NotEmpty(t, deriveDeterministicDispatchID(nil, "sess-1"))
}
