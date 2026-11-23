package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
)

type serveBrainHTTPCase struct {
	name       string
	closeLn    bool
	asyncErr   error
	cancelCtx  bool
	wantErr    bool
	wantErrSub string
}

func assertServeBrainHTTPCase(t *testing.T, tc serveBrainHTTPCase) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to bind listener: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var asyncErrCh chan error
	if tc.asyncErr != nil {
		asyncErrCh = make(chan error, 1)
		asyncErrCh <- tc.asyncErr
	}

	if tc.closeLn {
		_ = ln.Close()
	}
	if tc.cancelCtx {
		cancel()
	}

	mux := http.NewServeMux()
	var errResult error
	if asyncErrCh != nil {
		errResult = serveBrainHTTP(ctx, ln, mux, "test-model", 120, asyncErrCh)
	} else {
		errResult = serveBrainHTTP(ctx, ln, mux, "test-model", 120)
	}

	if tc.wantErr {
		if errResult == nil {
			t.Fatalf("expected error, got nil")
		}
		if tc.wantErrSub != "" && !strings.Contains(errResult.Error(), tc.wantErrSub) {
			t.Errorf("expected error containing %q, got %v", tc.wantErrSub, errResult)
		}
		return
	}
	if errResult != nil {
		t.Fatalf("unexpected error: %v", errResult)
	}
}

func TestServeBrainHTTP(t *testing.T) {
	t.Parallel()

	tests := []serveBrainHTTPCase{
		{name: "GracefulShutdownOnContextDone", cancelCtx: true},
		{name: "AsyncLeaderError", asyncErr: errors.New("leader failed"), wantErr: true, wantErrSub: "leader failed"},
		{name: "ListenerClosedServeError", closeLn: true, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertServeBrainHTTPCase(t, tc)
		})
	}
}
