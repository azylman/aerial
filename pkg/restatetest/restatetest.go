package restatetest

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/mock"
)

// MockRetryableCode marks a retryable Run error that the object mock surfaces once
// (in production, Restate continues retrying retryable errors).
const MockRetryableCode = 599

// SentCall records a call to mc.Object(...).Send(...).
type SentCall struct {
	Service string
	Key     string
	Method  string
	Input   any
	Delayed bool
	Options []any
}

// Option configures an ObjectHarness during construction.
type Option func(*ObjectHarness)

// WithKeepTerminalOnly configures Run callbacks to return nil on non-terminal errors,
// preserving only explicit terminal errors.
func WithKeepTerminalOnly() Option {
	return func(h *ObjectHarness) {
		h.keepTerminalOnly = true
	}
}

// WithRunError configures Run calls to immediately return a terminal error with err.
func WithRunError(err error) Option {
	return func(h *ObjectHarness) {
		h.runErrOverride = err
	}
}

// WithInitialState seeds a state key in the harness state map upon creation.
func WithInitialState[T any](stateKey string, initial T) Option {
	return func(h *ObjectHarness) {
		WithState(h, stateKey, initial)
	}
}

// WithState seeds or updates stateKey with initial in the harness in-memory state map.
func WithState[T any](h *ObjectHarness, stateKey string, initial T) {
	h.SetState(stateKey, initial)
}

// ObjectHarness wraps Restate mock context, client, and object context with an in-memory
// state store and execution recorders.
type ObjectHarness struct {
	Context *mocks.MockContext
	Client  *mocks.MockClient
	ObjCtx  restate.ObjectContext

	mu               sync.Mutex
	state            map[string]any
	clearedAll       bool
	sends            []SentCall
	ran              bool
	lastRunErr       error
	isTerminal       bool
	hasRetryCap      bool
	keepTerminalOnly bool
	runErrOverride   error
}

// NewRunContext returns a stub restate.RunContext suitable for running step callbacks directly.
func NewRunContext(ctx ...context.Context) restate.RunContext {
	c := context.Background()
	if len(ctx) > 0 && ctx[0] != nil {
		c = ctx[0]
	}
	return stubRunContext{Context: c}
}

//nolint:containedctx // implements restate.RunContext which embeds context.Context
type stubRunContext struct {
	context.Context
}

func (stubRunContext) Log() *slog.Logger         { return slog.Default() }
func (stubRunContext) Request() *restate.Request { return &restate.Request{ID: "req-1"} }

// NewObjectHarness constructs an ObjectHarness for key backed by an in-memory state map.
func NewObjectHarness(t *testing.T, key string, opts ...Option) *ObjectHarness {
	t.Helper()
	mc := mocks.NewMockContext(t)
	mClient := mocks.NewMockClient(t)
	mInv := mocks.NewMockInvocation(t)
	mc.On("Key").Return(key).Maybe()
	mc.On("Request").Return(&restate.Request{ID: "inv-" + key}).Maybe()

	h := &ObjectHarness{
		Context: mc,
		Client:  mClient,
		ObjCtx:  restate.WithMockContext(mc),
		state:   make(map[string]any),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(h)
		}
	}

	bindStateMocks(mc, h)
	bindRunMocks(mc, h)
	bindSendMocks(mc, mClient, mInv, h)

	return h
}

func bindStateMocks(mc *mocks.MockContext, h *ObjectHarness) {
	// State Get
	mc.On("Get", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		stateKey := args.String(0)
		target := args.Get(1)
		h.mu.Lock()
		val, ok := h.state[stateKey]
		h.mu.Unlock()
		if !ok || target == nil || val == nil {
			return
		}
		outVal := reflect.ValueOf(target)
		if outVal.Kind() != reflect.Pointer || outVal.IsNil() {
			return
		}
		vVal := reflect.ValueOf(val)
		if !vVal.IsValid() {
			return
		}
		elem := outVal.Elem()
		if vVal.Type().AssignableTo(elem.Type()) {
			elem.Set(vVal)
		} else if vVal.Type().ConvertibleTo(elem.Type()) {
			elem.Set(vVal.Convert(elem.Type()))
		}
	}).Return(func(stateKey string, _ any, _ ...restate.GetOption) (bool, restate.TerminalError) {
		h.mu.Lock()
		defer h.mu.Unlock()
		_, ok := h.state[stateKey]
		return ok, nil
	}).Maybe()

	// State Set
	mc.On("Set", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		stateKey := args.String(0)
		val := args.Get(1)
		h.SetState(stateKey, val)
	}).Return().Maybe()

	// State Clear
	mc.On("Clear", mock.Anything).Run(func(args mock.Arguments) {
		stateKey := args.String(0)
		h.mu.Lock()
		delete(h.state, stateKey)
		h.mu.Unlock()
	}).Return().Maybe()

	// State ClearAll
	mc.On("ClearAll").Run(func(mock.Arguments) {
		h.mu.Lock()
		h.clearedAll = true
		h.state = make(map[string]any)
		h.mu.Unlock()
	}).Return().Maybe()
}

func bindRunMocks(mc *mocks.MockContext, h *ObjectHarness) {
	runHandler := func(fn func(restate.RunContext) (any, error), output any, runOpts ...restate.RunOption) restate.TerminalError {
		h.mu.Lock()
		h.ran = true
		for _, opt := range runOpts {
			if strings.Contains(fmt.Sprintf("%T", opt), "withMaxRetryAttempts") {
				h.hasRetryCap = true
			}
		}
		override := h.runErrOverride
		h.mu.Unlock()

		if override != nil {
			termErr := restate.TerminalErrorf("%v", override)
			h.mu.Lock()
			h.lastRunErr = override
			h.isTerminal = true
			h.mu.Unlock()
			return termErr
		}

		res, err := fn(NewRunContext())

		h.mu.Lock()
		h.lastRunErr = err
		h.isTerminal = restate.IsTerminalError(err)
		keepTerm := h.keepTerminalOnly
		h.mu.Unlock()

		if err != nil {
			if restate.IsTerminalError(err) {
				return restate.AsTerminalError(err)
			}
			if keepTerm {
				return nil
			}
			return restate.ToTerminalError(err, restate.WithErrorCode(MockRetryableCode))
		}

		if output != nil && res != nil {
			outVal := reflect.ValueOf(output)
			resVal := reflect.ValueOf(res)
			if outVal.Kind() == reflect.Pointer && !outVal.IsNil() {
				elem := outVal.Elem()
				if resVal.Type().AssignableTo(elem.Type()) {
					elem.Set(resVal)
				} else if resVal.Type().ConvertibleTo(elem.Type()) {
					elem.Set(resVal.Convert(elem.Type()))
				}
			}
		}
		return nil
	}

	for arity := 2; arity <= 6; arity++ {
		mc.On("Run", slices.Repeat([]any{mock.Anything}, arity)...).Return(runHandler).Maybe()
	}
}

func bindSendMocks(mc *mocks.MockContext, mClient *mocks.MockClient, mInv *mocks.MockInvocation, h *ObjectHarness) {
	mc.On("Object", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.sends = append(h.sends, SentCall{
			Service: args.String(0),
			Key:     args.String(1),
			Method:  args.String(2),
		})
	}).Return(mClient).Maybe()

	mClient.On("Send", mock.Anything).Run(func(args mock.Arguments) {
		h.completeSend(args.Get(0), false, nil)
	}).Return(mInv).Maybe()

	mClient.On("Send", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		var opts []any
		if len(args) > 1 {
			opts = append(opts, args.Get(1))
		}
		h.completeSend(args.Get(0), true, opts)
	}).Return(mInv).Maybe()

	mClient.On("Send", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		opts := make([]any, 0, len(args)-1)
		for _, a := range args[1:] {
			opts = append(opts, a)
		}
		h.completeSend(args.Get(0), true, opts)
	}).Return(mInv).Maybe()
}

func (h *ObjectHarness) completeSend(input any, delayed bool, options []any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	last := &h.sends[len(h.sends)-1]
	last.Input = input
	last.Delayed = delayed
	last.Options = options
}

// Sends returns a copy of all recorded calls to Object(...).Send(...).
func (h *ObjectHarness) Sends() []SentCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]SentCall, len(h.sends))
	copy(out, h.sends)
	return out
}

// ClearedAll reports whether ClearAll was invoked on this object context.
func (h *ObjectHarness) ClearedAll() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.clearedAll
}

// HasRetryCap reports whether the last Run invocation contained a retry cap option.
func (h *ObjectHarness) HasRetryCap() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.hasRetryCap
}

// RunErr returns the error returned by the most recent Run callback, if any.
func (h *ObjectHarness) RunErr() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastRunErr
}

// Ran reports whether any Run callback was executed.
func (h *ObjectHarness) Ran() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ran
}

// IsTerminal reports whether the most recent Run error was classified terminal.
func (h *ObjectHarness) IsTerminal() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.isTerminal
}

// State returns the value currently stored for key in the harness in-memory map.
func (h *ObjectHarness) State(key string) (any, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	v, ok := h.state[key]
	return v, ok
}

// SetState stores or replaces the value for key in the in-memory map.
func (h *ObjectHarness) SetState(key string, val any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.state[key] = val
}

// FailRun configures subsequent Run invocations to immediately fail with a terminal error.
func (h *ObjectHarness) FailRun(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.runErrOverride = err
}

// TerminalCode returns the Restate terminal code of err, or 0 when err is nil or retryable.
func TerminalCode(err error) int {
	if err == nil || !restate.IsTerminalError(err) {
		return 0
	}
	if code := int(restate.AsTerminalError(err).Code()); code != MockRetryableCode {
		return code
	}
	return 0
}
