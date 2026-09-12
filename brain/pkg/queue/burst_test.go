package queue

import (
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
)

func TestPlanBurstExecution(t *testing.T) {
	now := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	botID := "bot-aerial-123"
	roleIDs := []string{"role-aerial-managed"}

	tests := []struct {
		name              string
		input             BurstPlanInput
		expectedWakeIdx   int
		expectedAllAmb    bool
		expectedHasActive bool
		check             func(t *testing.T, plan BurstPlan)
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
			check: func(t *testing.T, plan BurstPlan) {
				if plan.ActiveMessage.ID != "m1" {
					t.Errorf("expected active message m1, got %s", plan.ActiveMessage.ID)
				}
				if len(plan.TrailingMessages) != 1 || plan.TrailingMessages[0].ID != "m2" {
					t.Errorf("expected trailing message m2, got %v", plan.TrailingMessages)
				}
				if len(plan.WakeInfos) != 2 || !plan.WakeInfos[0].IsWake {
					t.Errorf("expected wakeInfos[0].IsWake to be true")
				}
			},
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
			check: func(t *testing.T, plan BurstPlan) {
				for i, info := range plan.WakeInfos {
					if info.IsWake {
						t.Errorf("expected wakeInfo[%d].IsWake = false", i)
					}
					if info.Reason != "hook_drop_override" {
						t.Errorf("expected reason hook_drop_override, got %s", info.Reason)
					}
				}
			},
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
			check: func(t *testing.T, plan BurstPlan) {
				if len(plan.LeadingAmbient) != 0 {
					t.Errorf("expected 0 leading ambient messages, got %d", len(plan.LeadingAmbient))
				}
				if len(plan.TrailingMessages) != 1 || plan.TrailingMessages[0].ID != "m2" {
					t.Errorf("expected 1 trailing message m2")
				}
			},
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
			check: func(t *testing.T, plan BurstPlan) {
				if len(plan.LeadingAmbient) != 2 {
					t.Fatalf("expected 2 leading ambient messages, got %d", len(plan.LeadingAmbient))
				}
				if plan.LeadingAmbient[0].ID != "m1" || plan.LeadingAmbient[1].ID != "m2" {
					t.Errorf("unexpected leading ambient IDs: %s, %s", plan.LeadingAmbient[0].ID, plan.LeadingAmbient[1].ID)
				}
				if plan.ActiveMessage.ID != "m3" {
					t.Errorf("expected active message m3, got %s", plan.ActiveMessage.ID)
				}
				if len(plan.TrailingMessages) != 1 || plan.TrailingMessages[0].ID != "m4" {
					t.Errorf("expected 1 trailing message m4, got %v", plan.TrailingMessages)
				}
			},
		},
		{
			name: "Mention mode with no mentions drops all messages as ambient without running classifier",
			input: BurstPlanInput{
				Burst: []db.Message{
					{ID: "m1", Content: "talking about general stuff", CreatedAt: now},
					{ID: "m2", Content: "aerial is cool but not tagged", CreatedAt: now.Add(time.Second)},
				},
				WakeMode: "mention",
				ClassifierFn: func([]db.Message) (float64, string) {
					t.Fatalf("classifier should NOT be called in mention mode")
					return 1.0, "should not be called"
				},
			},
			expectedWakeIdx:   -1,
			expectedAllAmb:    true,
			expectedHasActive: false,
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
			check: func(t *testing.T, plan BurstPlan) {
				if plan.ActiveMessage.ID != "m1" {
					t.Errorf("expected active message m1, got %s", plan.ActiveMessage.ID)
				}
				if len(plan.TrailingMessages) != 1 || plan.TrailingMessages[0].ID != "m2" {
					t.Errorf("expected trailing message m2")
				}
			},
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
				ClassifierFn: func([]db.Message) (float64, string) {
					t.Fatalf("classifier should NOT be called for heuristic skip banter")
					return 1.0, "should not run"
				},
			},
			expectedWakeIdx:   -1,
			expectedAllAmb:    true,
			expectedHasActive: false,
		},
		{
			name: "Classifier mode with high score wakes turn",
			input: BurstPlanInput{
				Burst: []db.Message{
					{ID: "m1", Content: "Can someone help me troubleshoot this Docker container?", CreatedAt: now},
				},
				WakeMode:  "classifier",
				Threshold: 0.75,
				ClassifierFn: func(msgs []db.Message) (float64, string) {
					return 0.92, "direct question to assistant"
				},
			},
			expectedWakeIdx:   0,
			expectedAllAmb:    false,
			expectedHasActive: true,
			check: func(t *testing.T, plan BurstPlan) {
				if plan.WakeInfos[0].Score != 0.92 {
					t.Errorf("expected score 0.92, got %f", plan.WakeInfos[0].Score)
				}
				if plan.WakeInfos[0].Reason != "direct question to assistant" {
					t.Errorf("expected reason 'direct question to assistant', got %q", plan.WakeInfos[0].Reason)
				}
			},
		},
		{
			name: "Classifier mode with low score drops as ambient",
			input: BurstPlanInput{
				Burst: []db.Message{
					{ID: "m1", Content: "I had tacos for lunch yesterday at that new food truck", CreatedAt: now},
				},
				WakeMode:  "classifier",
				Threshold: 0.75,
				ClassifierFn: func(msgs []db.Message) (float64, string) {
					return 0.15, "unrelated conversation"
				},
			},
			expectedWakeIdx:   -1,
			expectedAllAmb:    true,
			expectedHasActive: false,
		},
		{
			name: "Nil classifierFn defaults safely without panicking",
			input: BurstPlanInput{
				Burst: []db.Message{
					{ID: "m1", Content: "I need assistance with a server issue", CreatedAt: now},
				},
				WakeMode:     "classifier",
				Threshold:    0.5,
				ClassifierFn: nil,
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
			check: func(t *testing.T, plan BurstPlan) {
				if len(plan.TrailingMessages) != 1 {
					t.Fatalf("expected 1 trailing message, got %d", len(plan.TrailingMessages))
				}
				if len(plan.TrailingInfos) != 1 || !plan.TrailingInfos[0].IsWake {
					t.Errorf("expected trailing message to be marked IsWake = true")
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plan := PlanBurstExecution(tc.input)
			if plan.WakeIndex != tc.expectedWakeIdx {
				t.Errorf("expected WakeIndex %d, got %d", tc.expectedWakeIdx, plan.WakeIndex)
			}
			if plan.IsAllAmbient != tc.expectedAllAmb {
				t.Errorf("expected IsAllAmbient %t, got %t", tc.expectedAllAmb, plan.IsAllAmbient)
			}
			if plan.HasActiveMessage != tc.expectedHasActive {
				t.Errorf("expected HasActiveMessage %t, got %t", tc.expectedHasActive, plan.HasActiveMessage)
			}
			if tc.check != nil {
				tc.check(t, plan)
			}
		})
	}
}

func TestAssembleTurnPrompt(t *testing.T) {
	now := time.Date(2026, 9, 12, 14, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		input    TurnPromptInput
		expected string
		check    func(t *testing.T, prompt string)
	}{
		{
			name:     "Empty input produces empty prompt",
			input:    TurnPromptInput{},
			expected: "",
		},
		{
			name: "Single message burst formats directly",
			input: TurnPromptInput{
				Burst: []db.Message{
					{Content: "What is the capital of France?"},
				},
			},
			expected: "What is the capital of France?",
		},
		{
			name: "Multi-message burst produces coalesced USER_REQUEST envelope",
			input: TurnPromptInput{
				Burst: []db.Message{
					{AuthorName: "alice", Content: "step 1", CreatedAt: now},
					{AuthorName: "bob", Content: "step 2", CreatedAt: now.Add(time.Second)},
				},
			},
			check: func(t *testing.T, prompt string) {
				if !strings.HasPrefix(prompt, "<USER_REQUEST>") || !strings.HasSuffix(prompt, "</USER_REQUEST>") {
					t.Errorf("expected <USER_REQUEST> envelope wrapping coalesced messages")
				}
				if !strings.Contains(prompt, "--- Message 1 (by @alice at 14:00:00) ---") {
					t.Errorf("expected header for message 1")
				}
				if !strings.Contains(prompt, "--- Message 2 (by @bob at 14:00:01) ---") {
					t.Errorf("expected header for message 2")
				}
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
			check: func(t *testing.T, prompt string) {
				if strings.Count(prompt, "<COORDINATION_CONTEXT>") != 1 {
					t.Errorf("expected exactly 1 <COORDINATION_CONTEXT> opening tag, found %d", strings.Count(prompt, "<COORDINATION_CONTEXT>"))
				}
				if strings.Count(prompt, "<THREAD_SUMMARY>") != 1 {
					t.Errorf("expected exactly 1 <THREAD_SUMMARY> opening tag, found %d", strings.Count(prompt, "<THREAD_SUMMARY>"))
				}
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
			check: func(t *testing.T, prompt string) {
				if strings.Contains(prompt, "sess-abc-123") {
					t.Errorf("previous session ID should be omitted when IsColdStart is false")
				}
			},
		},
		{
			name: "Exact 7-layer hierarchy verification in top-to-bottom document order",
			input: TurnPromptInput{
				Burst: []db.Message{
					{AuthorName: "alice", Content: "user prompt utterance", CreatedAt: now},
				},
				InjectedHookContext: "coordinator-override: allow",
				ChannelInstructions: "Be concise and clear.",
				SemanticMemoryFacts: []db.Fact{
					{ID: 1, FactText: "Host IP is 192.168.1.14"},
				},
				PreviousSessionID: "sess-old-456",
				IsColdStart:       true,
				LookbackHistory: []HistoryMessage{
					{AuthorName: "charlie", Content: "previous message in channel", CreatedAt: time.Now().UTC().Add(-time.Minute)},
				},
				ThreadSummary: "Discussion about server maintenance",
			},
			check: func(t *testing.T, prompt string) {
				// Verify document order:
				// Layer 1: COORDINATION_CONTEXT
				idxCoord := strings.Index(prompt, "<COORDINATION_CONTEXT>")
				// Layer 2: CHANNEL_INSTRUCTIONS
				idxInst := strings.Index(prompt, "<CHANNEL_INSTRUCTIONS>")
				// Layer 3: SEMANTIC_MEMORY
				idxMem := strings.Index(prompt, "192.168.1.14")
				// Layer 4: PREVIOUS_SESSION
				idxPrev := strings.Index(prompt, "sess-old-456")
				// Layer 5: CHANNEL_HISTORY
				idxHist := strings.Index(prompt, "previous message in channel")
				// Layer 6: THREAD_SUMMARY
				idxSum := strings.Index(prompt, "<THREAD_SUMMARY>")
				// Layer 7: User Request
				idxUser := strings.Index(prompt, "user prompt utterance")

				if idxCoord == -1 || idxInst == -1 || idxMem == -1 || idxPrev == -1 || idxHist == -1 || idxSum == -1 || idxUser == -1 {
					t.Fatalf("missing layer in prompt:\ncoord=%d inst=%d mem=%d prev=%d hist=%d sum=%d user=%d\nFull prompt:\n%s",
						idxCoord, idxInst, idxMem, idxPrev, idxHist, idxSum, idxUser, prompt)
				}

				if !(idxCoord < idxInst && idxInst < idxMem && idxMem < idxPrev && idxPrev < idxHist && idxHist < idxSum && idxSum < idxUser) {
					t.Errorf("layer ordering violation:\ncoord=%d\ninst=%d\nmem=%d\nprev=%d\nhist=%d\nsum=%d\nuser=%d",
						idxCoord, idxInst, idxMem, idxPrev, idxHist, idxSum, idxUser)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := AssembleTurnPrompt(tc.input)
			if tc.expected != "" && result != tc.expected {
				t.Errorf("expected:\n%s\ngot:\n%s", tc.expected, result)
			}
			if tc.check != nil {
				tc.check(t, result)
			}
		})
	}
}

func TestEvaluateBurstStaleness(t *testing.T) {
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
			expectedStale: false, // m2 age is 15 min <= 30 min TTL
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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
