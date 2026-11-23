package session

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type extractTerminalResponseCase struct {
	name           string
	convID         string
	transcript     string
	fullTranscript string
	since          time.Time
	minQuiescence  time.Duration
	useDefault     bool
	recentTaskLog  bool
	taskLogAge     time.Duration
	wantResp       string
}

func runExtractTerminalResponseCase(t *testing.T, tc extractTerminalResponseCase) {
	t.Helper()
	tmp := t.TempDir()
	mgr := New(filepath.Join(tmp, "home"), filepath.Join(tmp, "data"))

	sessDir := filepath.Join(tmp, "data", "brain", tc.convID)
	logsDir := filepath.Join(sessDir, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	if tc.transcript != "" {
		if err := os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), []byte(tc.transcript), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	}
	if tc.fullTranscript != "" {
		if err := os.WriteFile(filepath.Join(logsDir, "transcript_full.jsonl"), []byte(tc.fullTranscript), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	}

	if tc.recentTaskLog {
		tasksDir := filepath.Join(sessDir, ".system_generated", "tasks")
		if err := os.MkdirAll(tasksDir, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		taskLog := filepath.Join(tasksDir, "task-1.log")
		if err := os.WriteFile(taskLog, []byte("running"), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		modTime := time.Now().Add(-tc.taskLogAge)
		if err := os.Chtimes(taskLog, modTime, modTime); err != nil {
			t.Fatalf("Chtimes: %v", err)
		}
	}

	var got string
	if tc.useDefault {
		got = mgr.ExtractDefaultTerminalTurnResponse(context.Background(), tc.convID, tc.since)
	} else {
		got = mgr.ExtractTerminalTurnResponse(context.Background(), tc.convID, tc.since, tc.minQuiescence)
	}
	if got != tc.wantResp {
		t.Fatalf("[%s] ExtractTerminalTurnResponse=%q, want %q", tc.name, got, tc.wantResp)
	}
}

func TestExtractTerminalTurnResponse(t *testing.T) {
	t.Parallel()
	now := time.Now()
	tPast30 := now.Add(-30 * time.Second)
	tPast20 := now.Add(-20 * time.Second)
	tPast5 := now.Add(-5 * time.Second)
	tPast40 := now.Add(-40 * time.Second)

	tests := []extractTerminalResponseCase{
		{
			name:          "quiescent terminal PLANNER_RESPONSE",
			convID:        "s-term-quiescent",
			transcript:    fmt.Sprintf("{\"type\":\"USER_INPUT\",\"content\":\"deploy\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"status\":\"DONE\",\"content\":\"Deployment completed\",\"created_at\":%q,\"tool_calls\":[]}\n", tPast30.Format(time.RFC3339Nano), tPast20.Format(time.RFC3339Nano)),
			since:         tPast30,
			minQuiescence: 15 * time.Second,
			wantResp:      "Deployment completed",
		},
		{
			name:          "non-quiescent step",
			convID:        "s-term-non-quiescent",
			transcript:    fmt.Sprintf("{\"type\":\"USER_INPUT\",\"content\":\"deploy\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"status\":\"DONE\",\"content\":\"Deployment completed\",\"created_at\":%q,\"tool_calls\":[]}\n", tPast30.Format(time.RFC3339Nano), tPast5.Format(time.RFC3339Nano)),
			since:         tPast30,
			minQuiescence: 15 * time.Second,
			wantResp:      "",
		},
		{
			name:          "trailing tool call",
			convID:        "s-term-trailing-tool",
			transcript:    fmt.Sprintf("{\"type\":\"USER_INPUT\",\"content\":\"deploy\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"status\":\"DONE\",\"content\":\"working\",\"created_at\":%q,\"tool_calls\":[{\"name\":\"run_command\"}]}\n", tPast30.Format(time.RFC3339Nano), tPast20.Format(time.RFC3339Nano)),
			since:         tPast30,
			minQuiescence: 15 * time.Second,
			wantResp:      "",
		},
		{
			name:          "pending invoke_subagent without SYSTEM_MESSAGE",
			convID:        "s-term-pending-async",
			transcript:    fmt.Sprintf("{\"type\":\"USER_INPUT\",\"content\":\"research\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"status\":\"DONE\",\"content\":\"[Tool Call Requested]: invoke_subagent\",\"created_at\":%q,\"tool_calls\":[{\"name\":\"invoke_subagent\"}]}\n{\"type\":\"PLANNER_RESPONSE\",\"status\":\"DONE\",\"content\":\"Interim text\",\"created_at\":%q,\"tool_calls\":[]}\n", tPast40.Format(time.RFC3339Nano), tPast30.Format(time.RFC3339Nano), tPast20.Format(time.RFC3339Nano)),
			since:         tPast40,
			minQuiescence: 15 * time.Second,
			wantResp:      "",
		},
		{
			name:          "completed invoke_subagent followed by SYSTEM_MESSAGE and terminal PLANNER_RESPONSE",
			convID:        "s-term-completed-async",
			transcript:    fmt.Sprintf("{\"type\":\"USER_INPUT\",\"content\":\"research\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"status\":\"DONE\",\"content\":\"[Tool Call Requested]: invoke_subagent\",\"created_at\":%q,\"tool_calls\":[{\"name\":\"invoke_subagent\"}]}\n{\"type\":\"SYSTEM_MESSAGE\",\"content\":\"Subagent done\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"status\":\"DONE\",\"content\":\"Research findings complete\",\"created_at\":%q,\"tool_calls\":[]}\n", tPast40.Format(time.RFC3339Nano), tPast30.Format(time.RFC3339Nano), tPast20.Format(time.RFC3339Nano), tPast20.Format(time.RFC3339Nano)),
			since:         tPast40,
			minQuiescence: 15 * time.Second,
			wantResp:      "Research findings complete",
		},
		{
			name:          "recent task log activity",
			convID:        "s-term-task-log",
			transcript:    fmt.Sprintf("{\"type\":\"USER_INPUT\",\"content\":\"task\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"status\":\"DONE\",\"content\":\"Task output\",\"created_at\":%q,\"tool_calls\":[]}\n", tPast30.Format(time.RFC3339Nano), tPast20.Format(time.RFC3339Nano)),
			since:         tPast30,
			minQuiescence: 15 * time.Second,
			recentTaskLog: true,
			taskLogAge:    2 * time.Second,
			wantResp:      "",
		},
		{
			name:          "since cutoff",
			convID:        "s-term-since-cutoff",
			transcript:    fmt.Sprintf("{\"type\":\"USER_INPUT\",\"content\":\"task\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"status\":\"DONE\",\"content\":\"Old output\",\"created_at\":%q,\"tool_calls\":[]}\n", tPast40.Format(time.RFC3339Nano), tPast30.Format(time.RFC3339Nano)),
			since:         tPast20,
			minQuiescence: 15 * time.Second,
			wantResp:      "",
		},
		{
			name:       "default quiescence helper",
			convID:     "s-term-default-quiescence",
			transcript: fmt.Sprintf("{\"type\":\"USER_INPUT\",\"content\":\"task\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"status\":\"DONE\",\"content\":\"Default quiescence output\",\"created_at\":%q,\"tool_calls\":[]}\n", tPast40.Format(time.RFC3339Nano), tPast20.Format(time.RFC3339Nano)),
			since:      tPast40,
			useDefault: true,
			wantResp:   "Default quiescence output",
		},
		{
			name:          "terminal PLANNER_RESPONSE containing agy background abort notice rejected",
			convID:        "s-term-agy-abort",
			transcript:    fmt.Sprintf("{\"type\":\"USER_INPUT\",\"content\":\"deploy\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"status\":\"DONE\",\"content\":\"Turn is aborted because model did not make any tool calls or return any response text\",\"created_at\":%q,\"tool_calls\":[]}\n", tPast30.Format(time.RFC3339Nano), tPast20.Format(time.RFC3339Nano)),
			since:         tPast30,
			minQuiescence: 15 * time.Second,
			wantResp:      "",
		},
		{
			name:          "pending background task without SYSTEM_MESSAGE",
			convID:        "s-term-pending-bg-task",
			transcript:    fmt.Sprintf("{\"type\":\"USER_INPUT\",\"content\":\"test\",\"created_at\":%q}\n{\"type\":\"GENERIC\",\"content\":\"Tool is running as a background task with task id: task-1\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"status\":\"DONE\",\"content\":\"Running command\",\"created_at\":%q,\"tool_calls\":[]}\n", tPast40.Format(time.RFC3339Nano), tPast30.Format(time.RFC3339Nano), tPast20.Format(time.RFC3339Nano)),
			since:         tPast40,
			minQuiescence: 15 * time.Second,
			wantResp:      "",
		},
		{
			name:          "completed background task followed by SYSTEM_MESSAGE and terminal PLANNER_RESPONSE",
			convID:        "s-term-completed-bg-task",
			transcript:    fmt.Sprintf("{\"type\":\"USER_INPUT\",\"content\":\"test\",\"created_at\":%q}\n{\"type\":\"GENERIC\",\"content\":\"Tool is running as a background task with task id: task-1\",\"created_at\":%q}\n{\"type\":\"SYSTEM_MESSAGE\",\"content\":\"Task completed\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"status\":\"DONE\",\"content\":\"Verification complete\",\"created_at\":%q,\"tool_calls\":[]}\n", tPast40.Format(time.RFC3339Nano), tPast30.Format(time.RFC3339Nano), tPast20.Format(time.RFC3339Nano), tPast20.Format(time.RFC3339Nano)),
			since:         tPast40,
			minQuiescence: 15 * time.Second,
			wantResp:      "Verification complete",
		},
		{
			name:          "pending background task cleared by server restart SYSTEM_MESSAGE",
			convID:        "s-term-bg-task-restart",
			transcript:    fmt.Sprintf("{\"type\":\"USER_INPUT\",\"content\":\"test\",\"created_at\":%q}\n{\"type\":\"GENERIC\",\"content\":\"Tool is running as a background task with task id: task-1\",\"created_at\":%q}\n{\"type\":\"SYSTEM_MESSAGE\",\"content\":\"All your subagents and background tasks have been stopped due to server restart\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"status\":\"DONE\",\"content\":\"Recovered from restart\",\"created_at\":%q,\"tool_calls\":[]}\n", tPast40.Format(time.RFC3339Nano), tPast30.Format(time.RFC3339Nano), tPast20.Format(time.RFC3339Nano), tPast20.Format(time.RFC3339Nano)),
			since:         tPast40,
			minQuiescence: 15 * time.Second,
			wantResp:      "Recovered from restart",
		},
		{
			name:          "PLANNER_RESPONSE echoing background task marker does not increment pending",
			convID:        "s-term-planner-echo-bg-marker",
			transcript:    fmt.Sprintf("{\"type\":\"USER_INPUT\",\"content\":\"status\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"status\":\"DONE\",\"content\":\"Tool is running as a background task with task id: 123\",\"created_at\":%q,\"tool_calls\":[]}\n", tPast40.Format(time.RFC3339Nano), tPast20.Format(time.RFC3339Nano)),
			since:         tPast40,
			minQuiescence: 15 * time.Second,
			wantResp:      "Tool is running as a background task with task id: 123",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runExtractTerminalResponseCase(t, tc)
		})
	}
}
