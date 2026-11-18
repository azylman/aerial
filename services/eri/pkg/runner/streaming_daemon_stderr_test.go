package runner

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/tracing"
)

type stderrTestLogHandler struct {
	mu      sync.Mutex
	records []stderrCapturedLog
}

type stderrCapturedLog struct {
	level slog.Level
	msg   string
	attrs map[string]string
}

func (*stderrTestLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *stderrTestLogHandler) Handle(ctx context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	m := make(map[string]string)
	r.Attrs(func(a slog.Attr) bool {
		m[a.Key] = a.Value.String()
		return true
	})
	h.records = append(h.records, stderrCapturedLog{
		level: r.Level,
		msg:   r.Message,
		attrs: m,
	})
	return nil
}

func (h *stderrTestLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *stderrTestLogHandler) WithGroup(string) slog.Handler      { return h }

type readStderrTestCase struct {
	name            string
	engine          string
	stack           string
	target          string
	sessionID       string
	activeTurnSess  string
	attachTurnSpan  bool
	attachSpawnSpan bool
	stderrInput     string
	largeLineLen    int
	wantRecordCount int
	wantLevel       slog.Level
	wantMsgContains string
	wantAttrs       map[string]string
	wantTailContain string
}

func TestStreamingDaemon_ReadStderrLoop(t *testing.T) {
	t.Parallel()
	turnSpan := testTurnSpan(2)
	spawnSpan := testTurnSpan(1)

	tests := []readStderrTestCase{
		{
			name:            "structured glog info with active turn span",
			engine:          "agy",
			stack:           "prod",
			target:          "discord-chan-1",
			sessionID:       "sess-d",
			activeTurnSess:  "sess-turn",
			attachTurnSpan:  true,
			attachSpawnSpan: true,
			stderrInput:     "I1118 09:24:54.123456 12345 client.go:42] Successfully connected to backend\n",
			wantRecordCount: 1,
			wantLevel:       slog.LevelInfo,
			wantMsgContains: "Successfully connected to backend",
			wantAttrs: map[string]string{
				"source":      "agent_stderr",
				"engine":      "agy",
				"stack":       "prod",
				"target":      "discord-chan-1",
				"session_id":  "sess-turn",
				"source_file": "client.go:42",
				"trace_id":    turnSpan.TraceID().String(),
				"span_id":     turnSpan.SpanID().String(),
			},
			wantTailContain: "Successfully connected to backend",
		},
		{
			name:            "spawn span fallback when no active turn",
			engine:          "agy",
			stack:           "dev",
			target:          "t1",
			sessionID:       "sess-spawn",
			attachSpawnSpan: true,
			stderrInput:     "W1118 09:24:54.123456 12345 auth.go:10] Warning message\n",
			wantRecordCount: 1,
			wantLevel:       slog.LevelWarn,
			wantMsgContains: "Warning message",
			wantAttrs: map[string]string{
				"source":      "agent_stderr",
				"engine":      "agy",
				"stack":       "dev",
				"target":      "t1",
				"session_id":  "sess-spawn",
				"source_file": "auth.go:10",
				"trace_id":    spawnSpan.TraceID().String(),
				"span_id":     spawnSpan.SpanID().String(),
			},
			wantTailContain: "Warning message",
		},
		{
			name:            "noise filtering drops benign logs but keeps in tail",
			engine:          "agy",
			stderrInput:     "I1118 09:24:54.123456 12345 client.go:42] You are not logged into Antigravity.\n",
			wantRecordCount: 0,
			wantTailContain: "You are not logged into Antigravity.",
		},
		{
			name:            "long line over 64KB handled without stall",
			engine:          "claude",
			largeLineLen:    70000,
			wantRecordCount: 2,
			wantLevel:       slog.LevelInfo,
			wantTailContain: "AAAA",
		},
		{
			name:            "tail buffer preservation and EOF flush without newline",
			engine:          "codex",
			sessionID:       "sess-codex",
			stderrInput:     "unhandled non-newline message on process exit",
			wantRecordCount: 1,
			wantLevel:       slog.LevelInfo,
			wantMsgContains: "unhandled non-newline message on process exit",
			wantAttrs: map[string]string{
				"source":     "agent_stderr",
				"engine":     "codex",
				"session_id": "sess-codex",
			},
			wantTailContain: "unhandled non-newline message on process exit",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			handler := &stderrTestLogHandler{}
			logger := slog.New(tracing.SlogHandler(handler))
			tail := newTailBuffer(4096)
			d := &StreamingDaemon{
				cfg: DaemonConfig{
					Engine:    tc.engine,
					Stack:     tc.stack,
					ThreadID:  tc.target,
					SessionID: tc.sessionID,
					Logger:    logger,
				},
				stderrTail: tail,
				sessionID:  tc.sessionID,
			}
			if tc.attachSpawnSpan {
				d.spawnSpanCtx = spawnSpan
			}
			if tc.attachTurnSpan || tc.activeTurnSess != "" {
				turnCtx := &TurnContext{SessionID: tc.activeTurnSess}
				if tc.attachTurnSpan {
					turnCtx.Ctx = trace.ContextWithSpanContext(context.Background(), turnSpan)
				}
				d.inflight = []*TurnContext{turnCtx}
			}

			inR, inW := io.Pipe()
			d.stderrCloser = inR
			d.stderrWg.Add(1)
			go d.readStderrLoop(inR)

			if tc.largeLineLen > 0 {
				largeData := bytes.Repeat([]byte("A"), tc.largeLineLen)
				_, _ = inW.Write(append(largeData, '\n'))
			} else {
				_, _ = inW.Write([]byte(tc.stderrInput))
			}
			_ = inW.Close()

			d.stderrWg.Wait()

			handler.mu.Lock()
			records := handler.records
			handler.mu.Unlock()

			assertCapturedStderrLogs(t, records, tail, tc)
		})
	}
}

func assertCapturedStderrLogs(t *testing.T, records []stderrCapturedLog, tail *tailBuffer, tc readStderrTestCase) {
	t.Helper()
	if len(records) != tc.wantRecordCount {
		t.Fatalf("got %d log records, want %d", len(records), tc.wantRecordCount)
	}
	if tc.wantRecordCount > 0 {
		if records[0].level != tc.wantLevel {
			t.Errorf("record level = %v, want %v", records[0].level, tc.wantLevel)
		}
		if tc.wantMsgContains != "" && !strings.Contains(records[0].msg, tc.wantMsgContains) {
			t.Errorf("record msg = %q, want contain %q", records[0].msg, tc.wantMsgContains)
		}
		for k, wantVal := range tc.wantAttrs {
			if gotVal := records[0].attrs[k]; gotVal != wantVal {
				t.Errorf("record attr %q = %q, want %q", k, gotVal, wantVal)
			}
		}
	}
	if tc.wantTailContain != "" && !strings.Contains(tail.String(), tc.wantTailContain) {
		t.Errorf("tail = %q, want contain %q", tail.String(), tc.wantTailContain)
	}
}
