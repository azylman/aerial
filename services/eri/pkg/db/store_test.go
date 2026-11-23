package db

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestResolveAgentName(t *testing.T) {
	tests := []struct {
		name      string
		candidate string
		envVal    string
		want      string
	}{
		{
			name:      "explicit candidate takes precedence",
			candidate: "custom-agent",
			envVal:    "ignored-agent",
			want:      "custom-agent",
		},
		{
			name:      "whitespace candidate falls back to env",
			candidate: "   ",
			envVal:    "env-agent",
			want:      "env-agent",
		},
		{
			name:      "empty candidate and empty env defaults to DefaultAgent",
			candidate: "",
			envVal:    "",
			want:      DefaultAgent,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("AGENT_NAME", tc.envVal)
			got := ResolveAgentName(tc.candidate)
			if got != tc.want {
				t.Errorf("ResolveAgentName(%q) with env=%q: got %q, want %q", tc.candidate, tc.envVal, got, tc.want)
			}
		})
	}
}

func assertIsolatedMessageContent(ctx context.Context, t *testing.T, s Store, msgID, wantContent, wantAgent string) {
	t.Helper()
	msg, err := s.GetMessage(ctx, msgID)
	if err != nil || msg == nil {
		t.Fatalf("GetMessage(%q) failed: %v", msgID, err)
	}
	if msg.Content != wantContent || msg.Agent != wantAgent {
		t.Errorf("message %q mismatch: got content %q agent %q, want %q / %q", msgID, msg.Content, msg.Agent, wantContent, wantAgent)
	}
}

func TestStore_MultiAgentIsolation_Messages(t *testing.T) {
	t.Parallel()
	database := newTestDB(t)
	storeAlpha := NewSQLStore(database).WithAgent("agent-alpha")
	storeBeta := NewSQLStore(database).WithAgent("agent-beta")
	ctx := context.Background()

	tests := []struct {
		name         string
		sharedMsgID  string
		alphaOnlyID  string
		threadID     string
		alphaContent string
		betaContent  string
	}{
		{
			name:         "shared message id in same thread without collision",
			sharedMsgID:  "msg-collision-100",
			alphaOnlyID:  "msg-alpha-only-100",
			threadID:     "thread-shared-200",
			alphaContent: "Alpha instruction turn",
			betaContent:  "Beta instruction turn",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := storeAlpha.InsertMessage(ctx, Message{
				ID:        tc.sharedMsgID,
				ThreadID:  tc.threadID,
				Content:   tc.alphaContent,
				Status:    StatusPending,
				CreatedAt: time.Now().UTC(),
			}); err != nil {
				t.Fatalf("storeAlpha.InsertMessage failed: %v", err)
			}

			if err := storeAlpha.InsertMessage(ctx, Message{
				ID:        tc.alphaOnlyID,
				ThreadID:  tc.threadID,
				Content:   "Alpha only message",
				Status:    StatusPending,
				CreatedAt: time.Now().UTC(),
			}); err != nil {
				t.Fatalf("storeAlpha.InsertMessage alpha-only failed: %v", err)
			}
			if existsAlpha, err := storeAlpha.MessageExists(ctx, tc.alphaOnlyID); err != nil || !existsAlpha {
				t.Fatalf("storeAlpha.MessageExists alpha-only: got (%v, %v), want (true, nil)", existsAlpha, err)
			}
			if existsBeta, err := storeBeta.MessageExists(ctx, tc.alphaOnlyID); err != nil || existsBeta {
				t.Fatalf("storeBeta.MessageExists alpha-only: got (%v, %v), want (false, nil)", existsBeta, err)
			}

			if err := storeBeta.InsertMessage(ctx, Message{
				ID:        tc.sharedMsgID,
				ThreadID:  tc.threadID,
				Content:   tc.betaContent,
				Status:    StatusPending,
				CreatedAt: time.Now().UTC(),
			}); err != nil {
				t.Fatalf("storeBeta.InsertMessage failed: %v", err)
			}

			assertIsolatedMessageContent(ctx, t, storeAlpha, tc.sharedMsgID, tc.alphaContent, "agent-alpha")
			assertIsolatedMessageContent(ctx, t, storeBeta, tc.sharedMsgID, tc.betaContent, "agent-beta")
		})
	}
}

func TestStore_MultiAgentIsolation_Sessions(t *testing.T) {
	t.Parallel()
	database := newTestDB(t)
	storeAlpha := NewSQLStore(database).WithAgent("agent-alpha")
	storeBeta := NewSQLStore(database).WithAgent("agent-beta")
	ctx := context.Background()

	tests := []struct {
		name         string
		threadID     string
		alphaConvID  string
		betaConvID   string
		alphaSummary string
		betaSummary  string
	}{
		{
			name:         "independent session and summaries on same thread",
			threadID:     "thread-sess-iso-1",
			alphaConvID:  "conv-alpha-001",
			betaConvID:   "conv-beta-001",
			alphaSummary: "Alpha summary 1",
			betaSummary:  "Beta summary 1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if saveErr := storeAlpha.SaveSessionID(ctx, tc.threadID, tc.alphaConvID); saveErr != nil {
				t.Fatalf("storeAlpha.SaveSessionID failed: %v", saveErr)
			}
			if saveErr := storeBeta.SaveSessionID(ctx, tc.threadID, tc.betaConvID); saveErr != nil {
				t.Fatalf("storeBeta.SaveSessionID failed: %v", saveErr)
			}

			sessA, getErrA := storeAlpha.GetSessionID(ctx, tc.threadID)
			if getErrA != nil || sessA != tc.alphaConvID {
				t.Errorf("storeAlpha.GetSessionID = %q (err: %v), want %q", sessA, getErrA, tc.alphaConvID)
			}
			sessB, getErrB := storeBeta.GetSessionID(ctx, tc.threadID)
			if getErrB != nil || sessB != tc.betaConvID {
				t.Errorf("storeBeta.GetSessionID = %q (err: %v), want %q", sessB, getErrB, tc.betaConvID)
			}

			if sumErr := storeAlpha.SaveThreadSummary(ctx, tc.threadID, tc.alphaSummary, "msg-1"); sumErr != nil {
				t.Fatalf("storeAlpha.SaveThreadSummary failed: %v", sumErr)
			}
			if sumErr := storeBeta.SaveThreadSummary(ctx, tc.threadID, tc.betaSummary, "msg-1"); sumErr != nil {
				t.Fatalf("storeBeta.SaveThreadSummary failed: %v", sumErr)
			}

			sumA, _, getSumErrA := storeAlpha.GetThreadSummary(ctx, tc.threadID)
			if getSumErrA != nil || sumA != tc.alphaSummary {
				t.Errorf("storeAlpha.GetThreadSummary = %q, want %q", sumA, tc.alphaSummary)
			}
			sumB, _, getSumErrB := storeBeta.GetThreadSummary(ctx, tc.threadID)
			if getSumErrB != nil || sumB != tc.betaSummary {
				t.Errorf("storeBeta.GetThreadSummary = %q, want %q", sumB, tc.betaSummary)
			}
		})
	}
}

func TestStore_MultiAgentIsolation_TurnCounts(t *testing.T) {
	t.Parallel()
	database := newTestDB(t)
	storeAlpha := NewSQLStore(database).WithAgent("agent-alpha")
	storeBeta := NewSQLStore(database).WithAgent("agent-beta")
	ctx := context.Background()

	tests := []struct {
		name          string
		threadID      string
		alphaSteps    int
		betaSteps     int
		wantAlphaTurn int
		wantBetaTurn  int
	}{
		{
			name:          "independent turn increments",
			threadID:      "thread-turns-1",
			alphaSteps:    2,
			betaSteps:     1,
			wantAlphaTurn: 2,
			wantBetaTurn:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for range tc.alphaSteps {
				if _, incErr := storeAlpha.IncrementSessionTurnCount(ctx, tc.threadID); incErr != nil {
					t.Fatalf("storeAlpha increment failed: %v", incErr)
				}
			}
			for range tc.betaSteps {
				if _, incErr := storeBeta.IncrementSessionTurnCount(ctx, tc.threadID); incErr != nil {
					t.Fatalf("storeBeta increment failed: %v", incErr)
				}
			}

			countA, errA := storeAlpha.GetSessionTurnCount(ctx, tc.threadID)
			if errA != nil || countA != tc.wantAlphaTurn {
				t.Errorf("storeAlpha turn count = %d (err: %v), want %d", countA, errA, tc.wantAlphaTurn)
			}
			countB, errB := storeBeta.GetSessionTurnCount(ctx, tc.threadID)
			if errB != nil || countB != tc.wantBetaTurn {
				t.Errorf("storeBeta turn count = %d (err: %v), want %d", countB, errB, tc.wantBetaTurn)
			}
		})
	}
}

func TestStore_MultiAgentIsolation_Records(t *testing.T) {
	t.Parallel()
	database := newTestDB(t)
	storeAlpha := NewSQLStore(database).WithAgent("agent-alpha")
	storeBeta := NewSQLStore(database).WithAgent("agent-beta")
	ctx := context.Background()

	tests := []struct {
		name, msgID, threadID string
	}{
		{name: "by-id record ops never touch another agent's rows", msgID: "msg-rec-1", threadID: "thread-rec-1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := storeAlpha.InsertMessage(ctx, Message{ID: tc.msgID, ThreadID: tc.threadID, Status: StatusProcessing}); err != nil {
				t.Fatalf("InsertMessage: %v", err)
			}
			if ids, err := storeBeta.FailMessagesIfActive(ctx, []string{tc.msgID}, "x"); err != nil || len(ids) != 0 {
				t.Errorf("beta FailMessagesIfActive = (%v, %v), want none", ids, err)
			}
			if ids, err := storeBeta.RequeueProcessingMessages(ctx, []string{tc.msgID}, "x"); err != nil || len(ids) != 0 {
				t.Errorf("beta RequeueProcessingMessages = (%v, %v), want none", ids, err)
			}
			if _, err := storeBeta.BeginMessageAttempt(ctx, tc.msgID, 1); !errors.Is(err, ErrRecordNotFound) {
				t.Errorf("beta BeginMessageAttempt err = %v, want ErrRecordNotFound", err)
			}
			if msgs, err := storeBeta.GetMessagesByID(ctx, []string{tc.msgID}); err != nil || len(msgs) != 0 {
				t.Errorf("beta GetMessagesByID = (%v, %v), want none", msgs, err)
			}
			assertIsolatedMessageContent(ctx, t, storeAlpha, tc.msgID, "", "agent-alpha")
			msg, err := storeAlpha.GetMessage(ctx, tc.msgID)
			if err != nil || msg == nil {
				t.Fatalf("storeAlpha.GetMessage(%q) = (%v, %v), want non-nil and nil error", tc.msgID, msg, err)
			}
			if msg.Status != StatusProcessing || msg.RetryCount != 0 || msg.RestartCount != 0 {
				t.Errorf("alpha row changed: status %s retry %d restart %d", msg.Status, msg.RetryCount, msg.RestartCount)
			}
		})
	}
}
