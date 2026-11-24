package runner

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"
)

type museResumeCase struct {
	name        string
	sessionID   string
	resumeCode  int
	resumeMsg   string
	startSessID string
	timeout     time.Duration
	wantErrSub  string
	wantCode    int
	wantTyped   bool
	wantSession string
}

func startMuseHandshake(tc museResumeCase) (*StreamingDaemon, error) {
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	errR, errW := io.Pipe()
	defer closeAll(inR, inW, outR, outW, errR, errW)

	replies := []string{
		`{"jsonrpc":"2.0","id":1,"result":{}}` + "\n",
		`{"jsonrpc":"2.0","id":2,"error":{"code":` + strconv.Itoa(tc.resumeCode) + `,"message":` + strconv.Quote(tc.resumeMsg) + `}}` + "\n",
	}
	if tc.startSessID != "" {
		replies = append(replies, `{"jsonrpc":"2.0","id":4,"result":{"session":{"id":`+strconv.Quote(tc.startSessID)+`}}}`+"\n")
	}
	go runMockCodexHandshakeServer(inR, outW, replies, nil)

	handle := &MockProcessHandle{pid: 779}
	mock := &MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, c DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, ProcessHandle, error) {
			return inW, outR, errR, handle, nil
		},
	}
	cfg := DaemonConfig{Engine: "muse", SessionID: tc.sessionID, ResumeSession: true, Timeout: tc.timeout}
	return StartStreamingDaemon(context.Background(), context.Background(), cfg, mock)
}

func runMuseResumeCase(t *testing.T, tc museResumeCase) {
	t.Helper()
	daemon, err := startMuseHandshake(tc)
	if tc.wantTyped {
		if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
			t.Fatalf("expected error containing %q, got: %v", tc.wantErrSub, err)
		}
		var resumeErr *MuseResumeError
		if !errors.As(err, &resumeErr) {
			t.Fatalf("expected *MuseResumeError, got %T: %v", err, err)
		}
		if resumeErr.Code != tc.wantCode || !strings.Contains(resumeErr.Message, tc.resumeMsg) {
			t.Errorf("MuseResumeError = %+v, want code %d message containing %q", resumeErr, tc.wantCode, tc.resumeMsg)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected handshake error: %v", err)
	}
	defer daemon.Close()
	if daemon.SessionID() != tc.wantSession {
		t.Errorf("expected fresh session %q, got %q", tc.wantSession, daemon.SessionID())
	}
}

func TestStreamingDaemon_Muse_ResumeRetryable_Suite(t *testing.T) {
	tests := []museResumeCase{
		{
			name:      "not found falls back to fresh session",
			sessionID: "018e3f94-2222-7000-8000-000000000002", resumeCode: -32000, resumeMsg: "session not found",
			startSessID: "018e3f94-4444-7000-8000-000000000004", timeout: 2 * time.Second,
			wantSession: "018e3f94-4444-7000-8000-000000000004",
		},
		{
			name:      "expired falls back to fresh session",
			sessionID: "018e3f94-2222-7000-8000-000000000002", resumeCode: -32000, resumeMsg: "session expired",
			startSessID: "018e3f94-5555-7000-8000-000000000005", timeout: 2 * time.Second,
			wantSession: "018e3f94-5555-7000-8000-000000000005",
		},
		{
			name:      "in-use propagates typed retryable error",
			sessionID: "018e3f94-2222-7000-8000-000000000002", resumeCode: -32001, resumeMsg: "session already in use",
			timeout:    300 * time.Millisecond,
			wantErrSub: "already in use", wantCode: -32001, wantTyped: true,
		},
		{
			name:      "unknown rejection propagates typed retryable error",
			sessionID: "018e3f94-2222-7000-8000-000000000002", resumeCode: -32000, resumeMsg: "internal error",
			timeout:    300 * time.Millisecond,
			wantErrSub: "internal error", wantCode: -32000, wantTyped: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runMuseResumeCase(t, tc)
		})
	}
}
