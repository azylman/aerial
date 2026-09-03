package classifier

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
)

func TestClassifier_Defaults(t *testing.T) {
	c := NewClassifier()
	if c.Model != "gemini-2.5-flash" {
		t.Errorf("expected default model 'gemini-2.5-flash', got %q", c.Model)
	}
	if c.Timeout != 1500*time.Millisecond {
		t.Errorf("expected default timeout 1500ms, got %v", c.Timeout)
	}
	if c.FailureThreshold != 3 {
		t.Errorf("expected default failure threshold 3, got %d", c.FailureThreshold)
	}
	if c.CooldownDuration != 60*time.Second {
		t.Errorf("expected default cooldown duration 60s, got %v", c.CooldownDuration)
	}
	if c.Clock == nil {
		t.Errorf("expected non-nil default clock")
	}
}

func TestClassifier_PromptFormatting(t *testing.T) {
	t1 := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 9, 2, 12, 1, 0, 0, time.UTC)
	tTarget := time.Date(2026, 9, 2, 12, 2, 0, 0, time.UTC)

	recentContext := []db.Message{
		{
			ID:         "msg-1",
			AuthorName: "Alice",
			Content:    "Hello everyone",
			CreatedAt:  t1,
		},
		{
			ID:         "msg-2",
			AuthorName: "Bob",
			Content:    "Hey Alice, did you check the build?",
			CreatedAt:  t2,
		},
	}

	target := db.Message{
		ID:         "msg-3",
		AuthorName: "Charlie",
		Content:    "Aerial, what is the status of the deployment?",
		CreatedAt:  tTarget,
	}

	var capturedPrompt string
	c := NewClassifier(
		WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
			capturedPrompt = prompt
			return `{"confidence": 0.9, "reason": "direct question"}`, nil
		}),
	)

	res := c.Classify(context.Background(), target, recentContext)
	if res.Confidence != 0.9 {
		t.Fatalf("expected confidence 0.9, got %f", res.Confidence)
	}

	// Verify context layout [@AuthorName] (timestamp): Content
	expectedAlice := "[@Alice] (2026-09-02T12:00:00Z): Hello everyone"
	expectedBob := "[@Bob] (2026-09-02T12:01:00Z): Hey Alice, did you check the build?"
	expectedTarget := "[@Charlie] (2026-09-02T12:02:00Z): Aerial, what is the status of the deployment?"

	if !strings.Contains(capturedPrompt, expectedAlice) {
		t.Errorf("prompt missing Alice format, prompt:\n%s", capturedPrompt)
	}
	if !strings.Contains(capturedPrompt, expectedBob) {
		t.Errorf("prompt missing Bob format, prompt:\n%s", capturedPrompt)
	}
	if !strings.Contains(capturedPrompt, expectedTarget) {
		t.Errorf("prompt missing Target format, prompt:\n%s", capturedPrompt)
	}

	// Verify chronological order: Alice appears before Bob
	aliceIdx := strings.Index(capturedPrompt, expectedAlice)
	bobIdx := strings.Index(capturedPrompt, expectedBob)
	targetIdx := strings.Index(capturedPrompt, expectedTarget)

	if aliceIdx == -1 || bobIdx == -1 || targetIdx == -1 {
		t.Fatalf("one or more messages not found in prompt")
	}
	if !(aliceIdx < bobIdx && bobIdx < targetIdx) {
		t.Errorf("expected chronological order Alice < Bob < Target, got indices %d, %d, %d", aliceIdx, bobIdx, targetIdx)
	}

	// Verify JSON instruction
	if !strings.Contains(capturedPrompt, `"confidence"`) || !strings.Contains(capturedPrompt, `"reason"`) {
		t.Errorf("prompt missing JSON output instructions")
	}
}

func TestClassifier_AuthorFallbacks(t *testing.T) {
	ts := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)

	// AuthorName empty, has AuthorID
	m1 := db.Message{
		AuthorID:  "user_12345",
		Content:   "test author ID fallback",
		CreatedAt: ts,
	}
	formatted1 := FormatMessage(m1)
	if formatted1 != "[@user_12345] (2026-09-02T12:00:00Z): test author ID fallback" {
		t.Errorf("unexpected fallback to author ID: %q", formatted1)
	}

	// Both empty
	m2 := db.Message{
		Content:   "test unknown fallback",
		CreatedAt: ts,
	}
	formatted2 := FormatMessage(m2)
	if formatted2 != "[@unknown] (2026-09-02T12:00:00Z): test unknown fallback" {
		t.Errorf("unexpected fallback to unknown: %q", formatted2)
	}
}

func TestClassifier_JSONParsing(t *testing.T) {
	tests := []struct {
		name           string
		llmOutput      string
		wantConfidence float64
		wantReason     string
	}{
		{
			name:           "raw JSON",
			llmOutput:      `{"confidence": 0.85, "reason": "direct question directed at assistant"}`,
			wantConfidence: 0.85,
			wantReason:     "direct question directed at assistant",
		},
		{
			name: "markdown fenced JSON",
			llmOutput: "```json\n" +
				"{\n" +
				`  "confidence": 0.72,` + "\n" +
				`  "reason": "relevant technical inquiry"` + "\n" +
				"}\n" +
				"```",
			wantConfidence: 0.72,
			wantReason:     "relevant technical inquiry",
		},
		{
			name: "markdown fence without json tag",
			llmOutput: "```\n" +
				`{"confidence": 0.65, "reason": "partial match"}` + "\n" +
				"```",
			wantConfidence: 0.65,
			wantReason:     "partial match",
		},
		{
			name:           "extra whitespace and surrounding commentary",
			llmOutput:      "  \n\tHere is the evaluation:\n\t{\"confidence\": 0.45, \"reason\": \"general chatter\"}\nHope this helps!  \n",
			wantConfidence: 0.45,
			wantReason:     "general chatter",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClassifier(
				WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
					return tc.llmOutput, nil
				}),
			)
			res := c.Classify(context.Background(), db.Message{Content: "test"}, nil)
			if res.Confidence != tc.wantConfidence {
				t.Errorf("expected confidence %f, got %f", tc.wantConfidence, res.Confidence)
			}
			if res.Reason != tc.wantReason {
				t.Errorf("expected reason %q, got %q", tc.wantReason, res.Reason)
			}
		})
	}
}

func TestClassifier_InvalidJSON(t *testing.T) {
	c := NewClassifier(
		WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
			return `This is not valid json at all`, nil
		}),
	)
	res := c.Classify(context.Background(), db.Message{Content: "hello"}, nil)
	if res.Confidence != 0.0 {
		t.Errorf("expected confidence 0.0 on invalid json, got %f", res.Confidence)
	}
	if !strings.Contains(res.Reason, "classifier error") {
		t.Errorf("expected reason to contain 'classifier error', got %q", res.Reason)
	}
}

func TestClassifier_ConfidenceClamping(t *testing.T) {
	tests := []struct {
		name           string
		llmOutput      string
		wantConfidence float64
	}{
		{
			name:           "negative clamped to 0.0",
			llmOutput:      `{"confidence": -0.5, "reason": "negative"}`,
			wantConfidence: 0.0,
		},
		{
			name:           "greater than 1.0 clamped to 1.0",
			llmOutput:      `{"confidence": 1.75, "reason": "high"}`,
			wantConfidence: 1.0,
		},
		{
			name:           "zero stays 0.0",
			llmOutput:      `{"confidence": 0.0, "reason": "zero"}`,
			wantConfidence: 0.0,
		},
		{
			name:           "one stays 1.0",
			llmOutput:      `{"confidence": 1.0, "reason": "one"}`,
			wantConfidence: 1.0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClassifier(
				WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
					return tc.llmOutput, nil
				}),
			)
			res := c.Classify(context.Background(), db.Message{Content: "test"}, nil)
			if res.Confidence != tc.wantConfidence {
				t.Errorf("expected confidence %f, got %f", tc.wantConfidence, res.Confidence)
			}
		})
	}
}

func TestClassifier_TimeoutHandling(t *testing.T) {
	c := NewClassifier(
		WithTimeout(30*time.Millisecond),
		WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
			select {
			case <-time.After(200 * time.Millisecond):
				return `{"confidence": 0.95, "reason": "slow response"}`, nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}),
	)

	start := time.Now()
	res := c.Classify(context.Background(), db.Message{Content: "ping"}, nil)
	duration := time.Since(start)

	if duration > 150*time.Millisecond {
		t.Errorf("expected call to abort near timeout (30ms), took %v", duration)
	}
	if res.Confidence != 0.0 {
		t.Errorf("expected confidence 0.0 on timeout, got %f", res.Confidence)
	}
	if !strings.Contains(res.Reason, "classifier error") {
		t.Errorf("expected reason to contain 'classifier error', got %q", res.Reason)
	}
}

func TestClassifier_CircuitBreaker(t *testing.T) {
	currTime := time.Date(2026, 9, 2, 10, 0, 0, 0, time.UTC)
	var timeMu sync.Mutex
	getTime := func() time.Time {
		timeMu.Lock()
		defer timeMu.Unlock()
		return currTime
	}
	advanceTime := func(d time.Duration) {
		timeMu.Lock()
		defer timeMu.Unlock()
		currTime = currTime.Add(d)
	}

	var callCount int32
	var shouldFail int32 = 1

	c := NewClassifier(
		WithFailureThreshold(3),
		WithCooldownDuration(60*time.Second),
		WithClock(getTime),
		WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
			atomic.AddInt32(&callCount, 1)
			if atomic.LoadInt32(&shouldFail) == 1 {
				return "", errors.New("simulated LLM failure")
			}
			return `{"confidence": 0.88, "reason": "healthy now"}`, nil
		}),
	)

	target := db.Message{Content: "hello"}

	// 1st failure
	res1 := c.Classify(context.Background(), target, nil)
	if res1.Confidence != 0.0 || !strings.Contains(res1.Reason, "classifier error") {
		t.Fatalf("call 1: expected classifier error, got %v", res1)
	}
	if atomic.LoadInt32(&callCount) != 1 {
		t.Fatalf("expected 1 call, got %d", atomic.LoadInt32(&callCount))
	}
	if c.ConsecutiveFailures() != 1 {
		t.Errorf("expected 1 consecutive failure, got %d", c.ConsecutiveFailures())
	}
	if c.IsCircuitOpen() {
		t.Errorf("circuit should not be open after 1 failure")
	}

	// 2nd failure
	res2 := c.Classify(context.Background(), target, nil)
	if res2.Confidence != 0.0 || !strings.Contains(res2.Reason, "classifier error") {
		t.Fatalf("call 2: expected classifier error, got %v", res2)
	}
	if atomic.LoadInt32(&callCount) != 2 {
		t.Fatalf("expected 2 calls, got %d", atomic.LoadInt32(&callCount))
	}
	if c.ConsecutiveFailures() != 2 {
		t.Errorf("expected 2 consecutive failures, got %d", c.ConsecutiveFailures())
	}
	if c.IsCircuitOpen() {
		t.Errorf("circuit should not be open after 2 failures")
	}

	// 3rd failure - this should trip the circuit breaker
	res3 := c.Classify(context.Background(), target, nil)
	if res3.Confidence != 0.0 || !strings.Contains(res3.Reason, "classifier error") {
		t.Fatalf("call 3: expected classifier error, got %v", res3)
	}
	if atomic.LoadInt32(&callCount) != 3 {
		t.Fatalf("expected 3 calls, got %d", atomic.LoadInt32(&callCount))
	}
	if c.ConsecutiveFailures() != 3 {
		t.Errorf("expected 3 consecutive failures, got %d", c.ConsecutiveFailures())
	}
	if !c.IsCircuitOpen() {
		t.Errorf("circuit breaker should be open after 3 failures")
	}

	// 4th call: circuit breaker is open! Should return immediately without calling LLM
	res4 := c.Classify(context.Background(), target, nil)
	if res4.Confidence != 0.0 {
		t.Errorf("call 4: expected confidence 0.0, got %f", res4.Confidence)
	}
	if res4.Reason != "circuit breaker open" {
		t.Errorf("call 4: expected reason 'circuit breaker open', got %q", res4.Reason)
	}
	if atomic.LoadInt32(&callCount) != 3 {
		t.Fatalf("expected callCount to remain 3 because circuit is open, got %d", atomic.LoadInt32(&callCount))
	}

	// Advance time within cooldown (30s out of 60s)
	advanceTime(30 * time.Second)
	res4b := c.Classify(context.Background(), target, nil)
	if res4b.Reason != "circuit breaker open" {
		t.Errorf("call 4b: expected circuit to still be open at +30s, got %q", res4b.Reason)
	}
	if atomic.LoadInt32(&callCount) != 3 {
		t.Fatalf("expected callCount to still be 3, got %d", atomic.LoadInt32(&callCount))
	}

	// Advance time past cooldown (advance by another 31s, total 61s > 60s)
	advanceTime(31 * time.Second)
	// Now heal the LLM service
	atomic.StoreInt32(&shouldFail, 0)

	// 5th call: half-open probe should call LLM and succeed
	res5 := c.Classify(context.Background(), target, nil)
	if atomic.LoadInt32(&callCount) != 4 {
		t.Fatalf("expected callCount to be 4 after recovery probe, got %d", atomic.LoadInt32(&callCount))
	}
	if res5.Confidence != 0.88 {
		t.Errorf("call 5: expected confidence 0.88, got %f", res5.Confidence)
	}
	if res5.Reason != "healthy now" {
		t.Errorf("call 5: expected reason 'healthy now', got %q", res5.Reason)
	}
	if c.IsCircuitOpen() {
		t.Errorf("circuit breaker should be closed after successful call")
	}
	if c.ConsecutiveFailures() != 0 {
		t.Errorf("consecutive failures should be reset to 0, got %d", c.ConsecutiveFailures())
	}

	// 6th call: circuit breaker should be fully reset and continue to succeed
	res6 := c.Classify(context.Background(), target, nil)
	if atomic.LoadInt32(&callCount) != 5 {
		t.Fatalf("expected callCount to be 5, got %d", atomic.LoadInt32(&callCount))
	}
	if res6.Confidence != 0.88 {
		t.Errorf("call 6: expected confidence 0.88, got %f", res6.Confidence)
	}
}

func TestClassifier_ConcurrentAccess(t *testing.T) {
	c := NewClassifier(
		WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
			return `{"confidence": 0.5, "reason": "concurrent"}`, nil
		}),
	)

	var wg sync.WaitGroup
	const goroutines = 20
	const iterations = 10

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = c.Classify(context.Background(), db.Message{Content: "ping"}, nil)
				_ = c.IsCircuitOpen()
				_ = c.ConsecutiveFailures()
			}
		}()
	}

	wg.Wait()
}
