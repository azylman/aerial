package queue

import (
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
)

func TestPlanBurstExecution(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	botID := "bot-aerial-123"
	roleIDs := []string{"role-aerial-managed"}

	tests := []struct {
		name              string
		input             BurstPlanInput
		expectedWakeIdx   int
		expectedAllAmb    bool
		expectedHasActive bool
		classifierMode    string
		classifierScore   float64
		classifierReason  string
		wantActiveID      string
		wantLeadingIDs    []string
		wantTrailingIDs   []string
		wantTrailingWake  bool
		wantFirstWake     bool
		wantAllWakeFalse  bool
		wantWakeReason    string
		wantFirstScore    float64
	}{
		{
			name: "Empty burst returns negative index and no active message",
			input: BurstPlanInput{
				Burst: nil,
			},
			expectedWakeIdx:   -1,
			expectedAllAmb:    false,
			expectedHasActive: false,
		},
		{
			name: "Hook override wake forces wake index 0 and partitions trailing",
			input: BurstPlanInput{
				Burst: []db.Message{
					{ID: "m1", Content: "hello"},
					{ID: "m2", Content: "world"},
				},
				HookOverride: "wake",
			},
			expectedWakeIdx:   0,
			expectedAllAmb:    false,
			expectedHasActive: true,
			wantActiveID:      "m1",
			wantTrailingIDs:   []string{"m2"},
			wantFirstWake:     true,
		},
		{
			name: "Hook override drop marks all messages ambient",
			input: BurstPlanInput{
				Burst: []db.Message{
					{ID: "m1", Content: "hello"},
					{ID: "m2", Content: "world"},
				},
				HookOverride: "drop",
			},
			expectedWakeIdx:   -1,
			expectedAllAmb:    true,
			expectedHasActive: false,
			wantAllWakeFalse:  true,
			wantWakeReason:    "hook_drop_override",
		},
		{
			name: "Tier-1 direct mention at head wakes immediately",
			input: BurstPlanInput{
				Burst: []db.Message{
					{ID: "m1", Content: "<@bot-aerial-123> help me plan dinner", CreatedAt: now},
					{ID: "m2", Content: "also grab milk", CreatedAt: now.Add(time.Second)},
				},
				WakeMode:   "mention",
				BotUserID:  botID,
				BotRoleIDs: roleIDs,
			},
			expectedWakeIdx:   0,
			expectedAllAmb:    false,
			expectedHasActive: true,
			wantTrailingIDs:   []string{"m2"},
		},
		{
			name: "Tier-1 direct mention after ambient chatter partitions leading ambient",
			input: BurstPlanInput{
				Burst: []db.Message{
					{ID: "m1", Content: "what a nice day", CreatedAt: now},
					{ID: "m2", Content: "yeah totally", CreatedAt: now.Add(time.Second)},
					{ID: "m3", Content: "<@bot-aerial-123> summarize this channel", CreatedAt: now.Add(2 * time.Second)},
					{ID: "m4", Content: "and send a dm", CreatedAt: now.Add(3 * time.Second)},
				},
				WakeMode:   "mention",
				BotUserID:  botID,
				BotRoleIDs: roleIDs,
			},
			expectedWakeIdx:   2,
			expectedAllAmb:    false,
			expectedHasActive: true,
			wantLeadingIDs:    []string{"m1", "m2"},
			wantActiveID:      "m3",
			wantTrailingIDs:   []string{"m4"},
		},
		{
			name: "Mention mode with no mentions drops all messages as ambient without running classifier",
			input: BurstPlanInput{
				Burst: []db.Message{
					{ID: "m1", Content: "talking about general stuff", CreatedAt: now},
					{ID: "m2", Content: "aerial is cool but not tagged", CreatedAt: now.Add(time.Second)},
				},
				WakeMode: "mention",
			},
			expectedWakeIdx:   -1,
			expectedAllAmb:    true,
			expectedHasActive: false,
			classifierMode:    "panic",
		},
		{
			name: "All mode always wakes on first message and partitions trailing",
			input: BurstPlanInput{
				Burst: []db.Message{
					{ID: "m1", Content: "msg 1", CreatedAt: now},
					{ID: "m2", Content: "msg 2", CreatedAt: now.Add(time.Second)},
				},
				WakeMode: "all",
			},
			expectedWakeIdx:   0,
			expectedAllAmb:    false,
			expectedHasActive: true,
			wantActiveID:      "m1",
			wantTrailingIDs:   []string{"m2"},
		},
		{
			name: "Classifier mode with threshold <= 0 drops all as ambient",
			input: BurstPlanInput{
				Burst: []db.Message{
					{ID: "m1", Content: "msg 1", CreatedAt: now},
				},
				WakeMode:  "classifier",
				Threshold: 0.0,
			},
			expectedWakeIdx:   -1,
			expectedAllAmb:    true,
			expectedHasActive: false,
		},
		{
			name: "Classifier mode with heuristic skip bypasses classifier and drops",
			input: BurstPlanInput{
				Burst: []db.Message{
					{ID: "m1", Content: "lol", CreatedAt: now},
					{ID: "m2", Content: "👍", CreatedAt: now.Add(time.Second)},
				},
				WakeMode:  "classifier",
				Threshold: 0.8,
			},
			expectedWakeIdx:   -1,
			expectedAllAmb:    true,
			expectedHasActive: false,
			classifierMode:    "panic",
		},
		{
			name: "Classifier mode with high score wakes turn",
			input: BurstPlanInput{
				Burst: []db.Message{
					{ID: "m1", Content: "Can someone help me troubleshoot this Docker container?", CreatedAt: now},
				},
				WakeMode:  "classifier",
				Threshold: 0.75,
			},
			expectedWakeIdx:   0,
			expectedAllAmb:    false,
			expectedHasActive: true,
			classifierMode:    "static",
			classifierScore:   0.92,
			classifierReason:  "direct question to assistant",
			wantFirstScore:    0.92,
			wantWakeReason:    "direct question to assistant",
		},
		{
			name: "Classifier mode with low score drops as ambient",
			input: BurstPlanInput{
				Burst: []db.Message{
					{ID: "m1", Content: "I had tacos for lunch yesterday at that new food truck", CreatedAt: now},
				},
				WakeMode:  "classifier",
				Threshold: 0.75,
			},
			expectedWakeIdx:   -1,
			expectedAllAmb:    true,
			expectedHasActive: false,
			classifierMode:    "static",
			classifierScore:   0.15,
			classifierReason:  "unrelated conversation",
		},
		{
			name: "Nil classifierFn defaults safely without panicking",
			input: BurstPlanInput{
				Burst: []db.Message{
					{ID: "m1", Content: "I need assistance with a server issue", CreatedAt: now},
				},
				WakeMode:  "classifier",
				Threshold: 0.5,
			},
			expectedWakeIdx:   -1,
			expectedAllAmb:    true,
			expectedHasActive: false,
		},
		{
			name: "Trailing message with direct mention in multi-message burst is marked wake for re-enqueuing",
			input: BurstPlanInput{
				Burst: []db.Message{
					{ID: "m1", Content: "<@bot-aerial-123> task 1", CreatedAt: now},
					{ID: "m2", Content: "<@bot-aerial-123> task 2", CreatedAt: now.Add(time.Second)},
				},
				WakeMode:   "mention",
				BotUserID:  botID,
				BotRoleIDs: roleIDs,
			},
			expectedWakeIdx:   0,
			expectedAllAmb:    false,
			expectedHasActive: true,
			wantTrailingIDs:   []string{"m2"},
			wantTrailingWake:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			in := tc.input
			switch tc.classifierMode {
			case "panic":
				in.ClassifierFn = func([]db.Message) (float64, string) {
					t.Fatalf("classifier should NOT be called")
					return 1.0, "should not be called"
				}
			case "static":
				in.ClassifierFn = func([]db.Message) (float64, string) {
					return tc.classifierScore, tc.classifierReason
				}
			}

			plan := PlanBurstExecution(in)
			if plan.WakeIndex != tc.expectedWakeIdx {
				t.Errorf("expected WakeIndex %d, got %d", tc.expectedWakeIdx, plan.WakeIndex)
			}
			if plan.IsAllAmbient != tc.expectedAllAmb {
				t.Errorf("expected IsAllAmbient %t, got %t", tc.expectedAllAmb, plan.IsAllAmbient)
			}
			if plan.HasActiveMessage != tc.expectedHasActive {
				t.Errorf("expected HasActiveMessage %t, got %t", tc.expectedHasActive, plan.HasActiveMessage)
			}
			if tc.wantActiveID != "" && plan.ActiveMessage.ID != tc.wantActiveID {
				t.Errorf("expected active message %s, got %s", tc.wantActiveID, plan.ActiveMessage.ID)
			}
			if tc.wantLeadingIDs != nil {
				if len(plan.LeadingAmbient) != len(tc.wantLeadingIDs) {
					t.Fatalf("expected %d leading ambient, got %d", len(tc.wantLeadingIDs), len(plan.LeadingAmbient))
				}
				for i, id := range tc.wantLeadingIDs {
					if plan.LeadingAmbient[i].ID != id {
						t.Errorf("expected leading[%d]=%s, got %s", i, id, plan.LeadingAmbient[i].ID)
					}
				}
			}
			if tc.wantTrailingIDs != nil {
				if len(plan.TrailingMessages) != len(tc.wantTrailingIDs) {
					t.Fatalf("expected %d trailing messages, got %d", len(tc.wantTrailingIDs), len(plan.TrailingMessages))
				}
				for i, id := range tc.wantTrailingIDs {
					if plan.TrailingMessages[i].ID != id {
						t.Errorf("expected trailing[%d]=%s, got %s", i, id, plan.TrailingMessages[i].ID)
					}
				}
			}
			if tc.wantTrailingWake {
				if len(plan.TrailingInfos) == 0 || !plan.TrailingInfos[0].IsWake {
					t.Errorf("expected trailing message to be marked IsWake = true")
				}
			}
			if tc.wantFirstWake {
				if len(plan.WakeInfos) == 0 || !plan.WakeInfos[0].IsWake {
					t.Errorf("expected wakeInfos[0].IsWake to be true")
				}
			}
			if tc.wantAllWakeFalse {
				for i, info := range plan.WakeInfos {
					if info.IsWake {
						t.Errorf("expected wakeInfo[%d].IsWake = false", i)
					}
				}
			}
			if tc.wantWakeReason != "" {
				for i, info := range plan.WakeInfos {
					if info.Reason != tc.wantWakeReason {
						t.Errorf("expected wakeInfo[%d].Reason = %q, got %q", i, tc.wantWakeReason, info.Reason)
					}
				}
			}
			if tc.wantFirstScore != 0 {
				if len(plan.WakeInfos) == 0 || plan.WakeInfos[0].Score != tc.wantFirstScore {
					t.Errorf("expected wakeInfos[0].Score %f", tc.wantFirstScore)
				}
			}
		})
	}
}

func TestAssembleTurnPrompt(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 12, 14, 0, 0, 0, time.UTC)

	tests := []struct {
		name                 string
		input                TurnPromptInput
		wantExact            string
		wantPrefix           string
		wantSuffix           string
		wantContains         []string
		wantExcludes         []string
		wantTagCounts        map[string]int
		wantOrderedSubstrings []string
	}{
		{
			name:      "Empty input produces empty prompt",
			input:     TurnPromptInput{},
			wantExact: "",
		},
		{
			name: "Single message burst formats directly",
			input: TurnPromptInput{
				Burst: []db.Message{
					{Content: "What is the capital of France?"},
				},
			},
			wantExact: "What is the capital of France?",
		},
		{
			name: "Multi-message burst produces coalesced USER_REQUEST envelope",
			input: TurnPromptInput{
				Burst: []db.Message{
					{AuthorName: "alice", Content: "step 1", CreatedAt: now},
					{AuthorName: "bob", Content: "step 2", CreatedAt: now.Add(time.Second)},
				},
			},
			wantPrefix: "<USER_REQUEST>",
			wantSuffix: "</USER_REQUEST>",
			wantContains: []string{
				"--- Message 1 (by @alice at 14:00:00) ---",
				"--- Message 2 (by @bob at 14:00:01) ---",
			},
		},
		{
			name: "Tag idempotency avoids double-wrapping of coordination context and thread summary",
			input: TurnPromptInput{
				Burst: []db.Message{
					{Content: "run pipeline"},
				},
				InjectedHookContext: "<COORDINATION_CONTEXT>\nalready tagged\n</COORDINATION_CONTEXT>",
				ThreadSummary:       "<THREAD_SUMMARY>\nalready summarized\n</THREAD_SUMMARY>",
			},
			wantTagCounts: map[string]int{
				"<COORDINATION_CONTEXT>": 1,
				"<THREAD_SUMMARY>":       1,
			},
		},
		{
			name: "Previous session omitted if not cold start",
			input: TurnPromptInput{
				Burst: []db.Message{
					{Content: "next step"},
				},
				PreviousSessionID: "sess-abc-123",
				IsColdStart:       false,
			},
			wantExcludes: []string{"sess-abc-123"},
		},
		{
			name: "Exact layer hierarchy verification in top-to-bottom document order",
			input: TurnPromptInput{
				Burst: []db.Message{
					{AuthorName: "alice", Content: "user prompt utterance", CreatedAt: now},
				},
				InjectedHookContext: "coordinator-override: allow",
				ChannelInstructions: "Be concise and clear.",
				PreviousSessionID:   "sess-old-456",
				IsColdStart:         true,
				LookbackHistory: []HistoryMessage{
					{AuthorName: "charlie", Content: "previous message in channel", CreatedAt: time.Now().UTC().Add(-time.Minute)},
				},
				ThreadSummary: "Discussion about server maintenance",
			},
			wantOrderedSubstrings: []string{
				"<COORDINATION_CONTEXT>",
				"<CHANNEL_INSTRUCTIONS>",
				"<PREVIOUS_SESSION>",
				"previous message in channel",
				"<THREAD_SUMMARY>",
				"user prompt utterance",
			},
		},
		{
			name: "Scope instructions produces SCOPE_INSTRUCTIONS block",
			input: TurnPromptInput{
				Burst: []db.Message{
					{AuthorName: "alice", Content: "turn on lights", CreatedAt: now},
				},
				ScopeInstructions: "Spoken brevity for touch-kiosk-kitchen.",
			},
			wantContains: []string{
				"<SCOPE_INSTRUCTIONS>",
				"Spoken brevity for touch-kiosk-kitchen.",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := AssembleTurnPrompt(tc.input)
			if tc.wantExact != "" && result != tc.wantExact {
				t.Errorf("expected %q, got %q", tc.wantExact, result)
			}
			if tc.wantPrefix != "" && !strings.HasPrefix(result, tc.wantPrefix) {
				t.Errorf("expected prefix %q, got:\n%s", tc.wantPrefix, result)
			}
			if tc.wantSuffix != "" && !strings.HasSuffix(result, tc.wantSuffix) {
				t.Errorf("expected suffix %q, got:\n%s", tc.wantSuffix, result)
			}
			for _, substr := range tc.wantContains {
				if !strings.Contains(result, substr) {
					t.Errorf("expected result to contain %q, got:\n%s", substr, result)
				}
			}
			for _, substr := range tc.wantExcludes {
				if strings.Contains(result, substr) {
					t.Errorf("expected result to exclude %q, got:\n%s", substr, result)
				}
			}
			for tag, count := range tc.wantTagCounts {
				if actual := strings.Count(result, tag); actual != count {
					t.Errorf("expected %d occurrences of %q, got %d", count, tag, actual)
				}
			}
			if len(tc.wantOrderedSubstrings) > 0 {
				lastIdx := -1
				for _, sub := range tc.wantOrderedSubstrings {
					idx := strings.Index(result, sub)
					if idx == -1 {
						t.Fatalf("expected substring %q not found in prompt:\n%s", sub, result)
					}
					if idx <= lastIdx {
						t.Errorf("substring %q at index %d out of order (previous at %d)", sub, idx, lastIdx)
					}
					lastIdx = idx
				}
			}
		})
	}
}

func TestEvaluateBurstStaleness(t *testing.T) {
	t.Parallel()
	baseTime := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	ttl := 30 * time.Minute

	tests := []struct {
		name                 string
		burst                []db.Message
		ttl                  time.Duration
		isCold               bool
		lastActivity         time.Time
		now                  time.Time
		expectedStale        bool
		expectedReasonSubstr string
	}{
		{
			name:          "Empty burst returns not stale",
			burst:         nil,
			expectedStale: false,
		},
		{
			name: "Zero timestamp message is retained safely",
			burst: []db.Message{
				{ID: "m1", CreatedAt: time.Time{}},
			},
			expectedStale:        false,
			expectedReasonSubstr: "zero_timestamp_retained",
		},
		{
			name: "Message exceeding MaxMessageAbsoluteAge is dropped regardless of recovery or thread state",
			burst: []db.Message{
				{
					ID:           "m1",
					CreatedAt:    baseTime,
					RetryCount:   5,
					RestartCount: 2,
				},
			},
			ttl:                  ttl,
			isCold:               false,
			lastActivity:         baseTime.Add(2*time.Hour + 5*time.Minute),
			now:                  baseTime.Add(2*time.Hour + 10*time.Minute),
			expectedStale:        true,
			expectedReasonSubstr: "hard absolute age cap",
		},
		{
			name: "Fresh message under TTL is not stale",
			burst: []db.Message{
				{ID: "m1", CreatedAt: baseTime},
			},
			ttl:           ttl,
			isCold:        true,
			now:           baseTime.Add(10 * time.Minute),
			expectedStale: false,
		},
		{
			name: "Message older than TTL in new thread is dropped as stale",
			burst: []db.Message{
				{ID: "m1", CreatedAt: baseTime},
			},
			ttl:                  ttl,
			isCold:               true,
			now:                  baseTime.Add(35 * time.Minute),
			expectedStale:        true,
			expectedReasonSubstr: "new thread and message age",
		},
		{
			name: "Message older than TTL in existing thread with recent session activity is retained",
			burst: []db.Message{
				{ID: "m1", CreatedAt: baseTime},
			},
			ttl:                  ttl,
			isCold:               false,
			lastActivity:         baseTime.Add(30 * time.Minute),
			now:                  baseTime.Add(35 * time.Minute),
			expectedStale:        false,
			expectedReasonSubstr: "recent activity",
		},
		{
			name: "Message older than TTL in existing thread with inactive session is dropped",
			burst: []db.Message{
				{ID: "m1", CreatedAt: baseTime},
			},
			ttl:                  ttl,
			isCold:               false,
			lastActivity:         baseTime.Add(2 * time.Minute),
			now:                  baseTime.Add(35 * time.Minute),
			expectedStale:        true,
			expectedReasonSubstr: "existing session inactive",
		},
		{
			name: "Recovered message (RetryCount > 0) older than TTL is retained",
			burst: []db.Message{
				{ID: "m1", CreatedAt: baseTime, RetryCount: 1},
			},
			ttl:           ttl,
			isCold:        true,
			now:           baseTime.Add(40 * time.Minute),
			expectedStale: false,
		},
		{
			name: "Recovered message (RestartCount > 0) older than TTL is retained",
			burst: []db.Message{
				{ID: "m1", CreatedAt: baseTime, RestartCount: 1},
			},
			ttl:           ttl,
			isCold:        true,
			now:           baseTime.Add(40 * time.Minute),
			expectedStale: false,
		},
		{
			name: "Burst atomicity: if any message in burst has recovered, whole burst is retained",
			burst: []db.Message{
				{ID: "m1", CreatedAt: baseTime, RetryCount: 0},
				{ID: "m2", CreatedAt: baseTime.Add(time.Second), RetryCount: 1},
			},
			ttl:           ttl,
			isCold:        true,
			now:           baseTime.Add(45 * time.Minute),
			expectedStale: false,
		},
		{
			name: "Multi-message burst evaluates against newest message timestamp",
			burst: []db.Message{
				{ID: "m1", CreatedAt: baseTime},
				{ID: "m2", CreatedAt: baseTime.Add(20 * time.Minute)},
			},
			ttl:           ttl,
			isCold:        true,
			now:           baseTime.Add(35 * time.Minute),
			expectedStale: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stale, reason := EvaluateBurstStaleness(tc.burst, tc.ttl, tc.isCold, tc.lastActivity, tc.now)
			if stale != tc.expectedStale {
				t.Errorf("expected stale=%t, got %t (reason: %s)", tc.expectedStale, stale, reason)
			}
			if tc.expectedReasonSubstr != "" && !strings.Contains(reason, tc.expectedReasonSubstr) {
				t.Errorf("expected reason to contain %q, got %q", tc.expectedReasonSubstr, reason)
			}
		})
	}
}

func TestEvaluateMessageStaleness(t *testing.T) {
	t.Parallel()
	baseTime := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	msg := db.Message{ID: "single-1", CreatedAt: baseTime}

	stale, reason := EvaluateMessageStaleness(msg, 10*time.Minute, true, time.Time{}, baseTime.Add(5*time.Minute))
	if stale {
		t.Errorf("expected fresh single message not to be stale: %s", reason)
	}

	stale, reason = EvaluateMessageStaleness(msg, 10*time.Minute, true, time.Time{}, baseTime.Add(15*time.Minute))
	if !stale {
		t.Errorf("expected expired single message in cold thread to be stale")
	}
}
