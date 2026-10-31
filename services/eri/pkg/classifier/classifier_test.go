package classifier

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

// WithModel sets the LLM model identifier.
func WithModel(model string) Option { return func(c *Classifier) { c.Model = model } }

// WithLLMFunc sets the LLM invocation function.
func WithLLMFunc(fn func(ctx context.Context, model, prompt string) (string, error)) Option {
	return func(c *Classifier) { c.LLMFunc = fn }
}

// NewClassifier constructs a Classifier with defaults (compatibility wrapper for New).
func NewClassifier(opts ...Option) *Classifier {
	return New(nil, opts...)
}

type classifierMockProcessHandle struct{ pid int }

func (m *classifierMockProcessHandle) Pid() int    { return m.pid }
func (m *classifierMockProcessHandle) Kill() error { return nil }
func (m *classifierMockProcessHandle) Wait() error { return nil }

type classifierMockSpawner struct {
	spawnFn func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error)
}

func (m *classifierMockSpawner) Spawn(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
	return m.spawnFn(ctx, cfg)
}

func mkMsg(author, content string, ts time.Time) db.Message {
	return db.Message{AuthorName: author, Content: content, CreatedAt: ts}
}

func mkReplyMsg(author, replyTo, content string, ts time.Time) db.Message {
	return db.Message{AuthorName: author, Content: content, CreatedAt: ts, ReplyingToAuthor: replyTo}
}

func TestClassifier_DefaultsAndOptions(t *testing.T) {
	tests := []struct {
		name           string
		cfg            *config.Config
		llmOut         string
		customModel    string
		customThresh   int
		customCooldown time.Duration
		customTimeout  time.Duration
		titleOut       string
		wantModel      string
		wantThresh     int
		wantCooldown   time.Duration
		wantTimeout    time.Duration
		wantSysOne     bool
		wantOutput     string
		wantTitle      string
		wantErr        bool
	}{
		{name: "nil config defaults with nil llm", wantModel: DefaultAGYClassifierModel, wantThresh: 3, wantCooldown: 60 * time.Second, wantTimeout: 12 * time.Second, wantErr: true},
		{name: "custom options with mock llm", cfg: config.NewFromData(&config.ConfigData{AgyBin: "/usr/bin/agy", APIKey: "key-123"}), llmOut: "runner-ok", customModel: "custom-model", customThresh: 5, customCooldown: 30 * time.Second, customTimeout: 5 * time.Second, titleOut: "title-fn", wantModel: "custom-model", wantThresh: 5, wantCooldown: 30 * time.Second, wantTimeout: 5 * time.Second, wantOutput: "runner-ok", wantTitle: "title-fn"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var opts []Option
			if tc.customModel != "" {
				opts = append(opts, WithModel(tc.customModel))
			}
			if tc.llmOut != "" {
				opts = append(opts, WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
					return tc.llmOut, nil
				}))
			}
			c := New(tc.cfg, opts...)
			if tc.customThresh > 0 {
				c.FailureThreshold = tc.customThresh
				c.CooldownDuration = tc.customCooldown
				c.Timeout = tc.customTimeout
				c.Clock = time.Now
			}
			if tc.titleOut != "" {
				c.TitleLLMFunc = func(ctx context.Context, model, prompt string) (string, error) {
					return tc.titleOut, nil
				}
			}
			if c == nil || c.Model != tc.wantModel || c.FailureThreshold != tc.wantThresh || c.CooldownDuration != tc.wantCooldown || c.Timeout != tc.wantTimeout || c.isSystemOne() != tc.wantSysOne {
				t.Fatalf("defaults mismatch: %+v", c)
			}
			out, err := c.LLMFunc(context.Background(), "m", "p")
			if (err != nil) != tc.wantErr || (!tc.wantErr && out != tc.wantOutput) {
				t.Fatalf("LLMFunc got (%q, %v), wantErr %v", out, err, tc.wantErr)
			}
			tOut, tErr := c.TitleLLMFunc(context.Background(), "m", "p")
			if (tErr != nil) != tc.wantErr || (!tc.wantErr && tOut != tc.wantTitle) {
				t.Fatalf("TitleLLMFunc got (%q, %v), want (%q, %v)", tOut, tErr, tc.wantTitle, tc.wantErr)
			}
		})
	}

	compat := NewClassifier()
	if compat == nil || compat.Model != DefaultAGYClassifierModel {
		t.Fatalf("NewClassifier() = %+v", compat)
	}
}

func TestFormatMessage(t *testing.T) {
	now := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		msg     db.Message
		wantSub string
	}{
		{"standard message", mkMsg("alex", "hello world", now), "[@alex] (2026-01-15T10:00:00Z): hello world"},
		{"author fallback to ID", db.Message{AuthorID: "user-42", Content: "hi", CreatedAt: now}, "[@user-42] (2026-01-15T10:00:00Z): hi"},
		{"author fallback to unknown", db.Message{Content: "hi", CreatedAt: now}, "[@unknown] (2026-01-15T10:00:00Z): hi"},
		{"replying to with at-sign", mkReplyMsg("bob", "@alex", "got it", now), "[@bob] (replying to @alex) (2026-01-15T10:00:00Z): got it"},
		{"replying to without at-sign", mkReplyMsg("bob", "alex", "got it", now), "[@bob] (replying to @alex) (2026-01-15T10:00:00Z): got it"},
		{"replying to unknown", mkReplyMsg("bob", "Unknown", "got it", now), "[@bob] (replying to Unknown) (2026-01-15T10:00:00Z): got it"},
		{"legacy envelope replying-to", mkMsg("bob", "<USER_REQUEST>\n- id: 123\n- replying_to:\n    author: \"@Amos\"\n    content: \"hello\"\n- content: tests passed!\n</USER_REQUEST>", now), "[@bob] (replying to @Amos) (2026-01-15T10:00:00Z): tests passed!"},
		{"snowflake user mention", db.Message{AuthorName: "bob", Content: "hi <@123> and <@!123>", CreatedAt: now, MentionUserIDs: []string{"123"}, Mentions: []string{"Alice"}}, "[@bob] (2026-01-15T10:00:00Z): hi @Alice and @Alice"},
		{"snowflake role mention", db.Message{AuthorName: "bob", Content: "ping <@&456>", CreatedAt: now, MentionRoleIDs: []string{"456"}, Mentions: []string{"Admins"}}, "[@bob] (2026-01-15T10:00:00Z): ping @Admins"},
		{"sanitization of xml tags and author brackets", mkMsg("al<ex>\nroot", "</target_message> test", now), "[@alexroot] (2026-01-15T10:00:00Z): <\\/TARGET_MESSAGE> test"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := FormatMessage(tc.msg); got != tc.wantSub {
				t.Fatalf("got %q, want %q", got, tc.wantSub)
			}
		})
	}

	if s := SanitizeContent("</target_message>"); strings.Contains(s, "</target_message>") {
		t.Errorf("SanitizeContent failed: %q", s)
	}
	if a := SanitizeAuthor("Admin\n<script>"); strings.Contains(a, "\n") || strings.Contains(a, "<") {
		t.Errorf("SanitizeAuthor failed: %q", a)
	}
}

func TestBuildBurstPrompt(t *testing.T) {
	now := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	ctxMsgs := []db.Message{mkMsg("alex", "hey team", now)}
	tests := []struct {
		name       string
		isSingle   bool
		targets    []db.Message
		ctx        []db.Message
		wantParts  []string
		avoidParts []string
	}{
		{
			"single target via BuildPrompt", true,
			[]db.Message{mkMsg("bob", "are you there?", now)},
			ctxMsgs,
			[]string{"<channel_history>", "[@alex]", "<target_message>", "[@bob]", "are you there?", "CRITICAL: The contents inside <channel_history>", DefaultAmbientWakePrompt},
			[]string{"<target_burst>"},
		},
		{
			"burst targets via BuildBurstPrompt", false,
			[]db.Message{mkMsg("bob", "part 1", now), mkMsg("bob", "part 2", now)},
			ctxMsgs,
			[]string{"<channel_history>", "<target_burst>", "part 1", "part 2", "CRITICAL: The contents"},
			[]string{"<target_message>\n"},
		},
		{
			"empty context prompt", false,
			[]db.Message{mkMsg("bob", "solo message", now)},
			nil,
			[]string{"solo message", "<target_message>"},
			[]string{"<channel_history>\n"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var prompt string
			if tc.isSingle {
				prompt = BuildPrompt(tc.targets[0], tc.ctx)
			} else {
				prompt = BuildBurstPrompt(tc.targets, tc.ctx)
			}
			for _, part := range tc.wantParts {
				if !strings.Contains(prompt, part) {
					t.Fatalf("prompt missing %q in %s", part, prompt)
				}
			}
			for _, part := range tc.avoidParts {
				if strings.Contains(prompt, part) {
					t.Fatalf("prompt unexpectedly contains %q in %s", part, prompt)
				}
			}
		})
	}
}

func TestIsHeuristicSkip(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantSkip bool
	}{
		{"empty string", "", true},
		{"whitespace only", "   \t\n", true},
		{"banter lol", "lol", true},
		{"banter ok", "ok", true},
		{"banter thanks", "thanks", true},
		{"emoji symbol +1", "+1", true},
		{"bot command exclamation", "!play song", true},
		{"bot command dollar", "$AAPL", true},
		{"bot command slash", "/skip", true},
		{"inquiry question mark", "who broke dev?", false},
		{"inquiry is it down", "is it down?", false},
		{"alert keyword postgres crashed", "postgres crashed", false},
		{"alert keyword prod 500 error", "prod 500 error", false},
		{"short message under 10 chars", "short msg", true},
		{"message exactly 10 chars", "0123456789", false},
		{"longer substantive discussion", "I wonder if we can use raft consensus for distributed locking", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsHeuristicSkip(tc.input); got != tc.wantSkip {
				t.Fatalf("IsHeuristicSkip(%q) = %v, want %v", tc.input, got, tc.wantSkip)
			}
		})
	}
}

func TestClassify_LLM(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		llmOut      string
		nilLLM      bool
		blankModel  bool
		timeoutWait bool
		burst       []db.Message
		useBurst    bool
		useNilCtx   bool
		checkParse  bool
		wantConf    float64
		wantReason  string
	}{
		{name: "RawJSON", llmOut: `{"confidence": 0.85, "reason": "direct question"}`, wantConf: 0.85, wantReason: "direct question"},
		{name: "RawJSONWithWhitespace", llmOut: "  \n\t{\"confidence\": 0.45, \"reason\": \"general chatter\"}\n\t  ", wantConf: 0.45, wantReason: "general chatter"},
		{name: "MarkdownFencedFails", llmOut: "```json\n{\"confidence\": 0.72, \"reason\": \"test\"}\n```", wantConf: 0.0, wantReason: "classifier error"},
		{name: "MarkdownFenceNoTagFails", llmOut: "```\n{\"confidence\": 0.65, \"reason\": \"test\"}\n```", wantConf: 0.0, wantReason: "classifier error"},
		{name: "SurroundingCommentaryFails", llmOut: "Here is evaluation:\n{\"confidence\": 0.45, \"reason\": \"test\"}", wantConf: 0.0, wantReason: "classifier error"},
		{name: "TrailingCommentaryFails", llmOut: "{\"confidence\": 0.85, \"reason\": \"ok\"}\nNote: {confidence}", wantConf: 0.0, wantReason: "classifier error"},
		{name: "ClampedNegative", llmOut: `{"confidence": -0.5, "reason": "negative"}`, wantConf: 0.0, wantReason: "negative"},
		{name: "ClampedHigh", llmOut: `{"confidence": 1.5, "reason": "high"}`, wantConf: 1.0, wantReason: "high"},
		{name: "EmptyBurstReturnsZero", useBurst: true, wantConf: 0.0, wantReason: "empty target burst"},
		{name: "ClassifyBurstValid", llmOut: `{"confidence": 0.9, "reason": "burst query"}`, burst: []db.Message{{Content: "m1"}, {Content: "m2"}}, useBurst: true, wantConf: 0.9, wantReason: "burst query"},
		{name: "NilContextGraceful", llmOut: `{"confidence": 0.8, "reason": "ok"}`, burst: []db.Message{{Content: "msg"}}, useNilCtx: true, wantConf: 0.8, wantReason: "ok"},
		{name: "NilLLMFuncError", nilLLM: true, burst: []db.Message{{Content: "msg"}}, wantConf: 0.0, wantReason: "no LLMFunc configured"},
		{name: "EmptyModelError", blankModel: true, burst: []db.Message{{Content: "msg"}}, wantConf: 0.0, wantReason: "model is not configured"},
		{name: "TimeoutHandling", timeoutWait: true, burst: []db.Message{{Content: "msg"}}, wantConf: 0.0, wantReason: "classifier error"},
		{name: "ParseErrorCallback", llmOut: "not valid json {", checkParse: true, wantConf: 0.0, wantReason: "classifier error"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var capturedModel, capturedRaw string
			var capturedErr error

			var cls *Classifier
			switch {
			case tc.nilLLM:
				cls = &Classifier{Model: "test-model"}
			case tc.blankModel:
				cls = &Classifier{Model: "   ", cfg: config.NewFromData(&config.ConfigData{LowEffortModel: "   "}), LLMFunc: func(ctx context.Context, model, prompt string) (string, error) { return "", nil }}
			case tc.timeoutWait:
				cls = NewClassifier(WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
					<-ctx.Done()
					return "", ctx.Err()
				}))
				cls.Timeout = 10 * time.Millisecond
			default:
				cls = NewClassifier(WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) { return tc.llmOut, nil }))
			}

			if tc.checkParse {
				cls.OnParseError = func(model, raw string, err error) {
					capturedModel = model
					capturedRaw = raw
					capturedErr = err
				}
			}

			var ctx context.Context
			if !tc.useNilCtx {
				ctx = context.Background()
			}
			var res ClassificationResult
			if tc.useBurst {
				res = cls.ClassifyBurst(ctx, tc.burst, nil)
			} else if len(tc.burst) > 0 {
				res = cls.Classify(ctx, tc.burst[0], nil)
			} else {
				res = cls.Classify(ctx, db.Message{Content: "test"}, nil)
			}

			if res.Confidence != tc.wantConf || !strings.Contains(res.Reason, tc.wantReason) {
				t.Fatalf("got %+v, want conf %f, reason substring %q", res, tc.wantConf, tc.wantReason)
			}
			if tc.checkParse {
				if capturedRaw != "not valid json {" || capturedErr == nil || capturedModel != DefaultAGYClassifierModel {
					t.Fatalf("OnParseError failed: model=%q, raw=%q, err=%v", capturedModel, capturedRaw, capturedErr)
				}
			}
		})
	}
}

func TestCircuitBreaker(t *testing.T) {
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

	isCircuitOpen := func(cls *Classifier) bool {
		cls.mu.Lock()
		defer cls.mu.Unlock()
		if !cls.circuitOpen {
			return false
		}
		now := time.Now()
		if cls.Clock != nil {
			now = cls.Clock()
		}
		return now.Before(cls.circuitOpenUntil)
	}
	getFailures := func(cls *Classifier) int {
		cls.mu.Lock()
		defer cls.mu.Unlock()
		return cls.consecutiveFailures
	}

	var callCount, shouldFail int32
	atomic.StoreInt32(&shouldFail, 1)

	c := NewClassifier(
		WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
			atomic.AddInt32(&callCount, 1)
			if atomic.LoadInt32(&shouldFail) == 1 {
				return "", errors.New("simulated failure")
			}
			return `{"confidence": 0.88, "reason": "healthy now"}`, nil
		}),
	)
	c.FailureThreshold = 3
	c.CooldownDuration = 60 * time.Second
	c.Clock = getTime

	target := db.Message{Content: "hello"}
	for i := 1; i <= 2; i++ {
		res := c.Classify(context.Background(), target, nil)
		if res.Confidence != 0.0 || getFailures(c) != i || isCircuitOpen(c) {
			t.Fatalf("call %d: failures=%d, open=%v", i, getFailures(c), isCircuitOpen(c))
		}
	}

	res3 := c.Classify(context.Background(), target, nil)
	if res3.Confidence != 0.0 || getFailures(c) != 3 || !isCircuitOpen(c) {
		t.Fatalf("call 3: failures=%d, open=%v", getFailures(c), isCircuitOpen(c))
	}

	res4 := c.Classify(context.Background(), target, nil)
	if res4.Confidence != 0.0 || res4.Reason != "circuit breaker open" || atomic.LoadInt32(&callCount) != 3 {
		t.Fatalf("call 4: expected circuit open, got %+v, calls=%d", res4, atomic.LoadInt32(&callCount))
	}

	advanceTime(61 * time.Second)
	atomic.StoreInt32(&shouldFail, 0)
	res5 := c.Classify(context.Background(), target, nil)
	if res5.Confidence != 0.88 || isCircuitOpen(c) || getFailures(c) != 0 {
		t.Fatalf("call 5: expected recovery, got %+v, failures=%d, open=%v", res5, getFailures(c), isCircuitOpen(c))
	}

	cParse := NewClassifier(WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) { return "bad-json", nil }))
	cParse.FailureThreshold = 2
	cParse.CooldownDuration = 60 * time.Second
	cParse.Classify(context.Background(), target, nil)
	cParse.Classify(context.Background(), target, nil)
	if getFailures(cParse) != 2 || !isCircuitOpen(cParse) {
		t.Fatalf("expected 2 parse failures and open circuit, got %d, open=%v", getFailures(cParse), isCircuitOpen(cParse))
	}

	cConc := NewClassifier()
	cConc.FailureThreshold = 5
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if idx%2 == 0 {
				cConc.recordFailure()
			} else {
				cConc.recordSuccess()
			}
			_ = isCircuitOpen(cConc)
			_ = getFailures(cConc)
		}(i)
	}
	wg.Wait()
}

func TestResolveModelAndTitleModel(t *testing.T) {
	t.Parallel()
	defaultLow := config.DefaultConfigData().LowEffortModel

	tests := []struct {
		name      string
		cls       *Classifier
		isTitle   bool
		wantModel string
	}{
		{"ClassifierURLWithModel", &Classifier{cfg: config.NewFromData(&config.ConfigData{ClassifierURL: "http://test", ClassifierModel: "ollama-m"})}, false, "ollama-m"},
		{"ClassifierURLWithoutModel", &Classifier{cfg: config.NewFromData(&config.ConfigData{ClassifierURL: "http://test"})}, false, DefaultAGYClassifierModel},
		{"ClassifierURLWithCustomModel", &Classifier{Model: "custom-qwen", cfg: config.NewFromData(&config.ConfigData{ClassifierURL: "http://test"})}, false, "custom-qwen"},
		{"LowEffortModelConfigured", &Classifier{cfg: config.NewFromData(&config.ConfigData{LowEffortModel: "gemini-low"})}, false, "gemini-low"},
		{"CustomModelWithoutURL", &Classifier{Model: "custom-flash", cfg: config.NewFromData(&config.ConfigData{LowEffortModel: "gemini-default"})}, false, "custom-flash"},
		{"NilConfigDefault", &Classifier{}, false, defaultLow},
		{"TitleURLWithModel", &Classifier{cfg: config.NewFromData(&config.ConfigData{ThreadTitleURL: "http://test", ThreadTitleModel: "title-m"})}, true, "title-m"},
		{"TitleURLWithoutModel", &Classifier{cfg: config.NewFromData(&config.ConfigData{ThreadTitleURL: "http://test"})}, true, DefaultAGYClassifierModel},
		{"TitleURLWithCustomModel", &Classifier{Model: "custom-title-m", cfg: config.NewFromData(&config.ConfigData{ThreadTitleURL: "http://test"})}, true, "custom-title-m"},
		{"FallbackClassifierURLWithModel", &Classifier{cfg: config.NewFromData(&config.ConfigData{ClassifierURL: "http://test", ClassifierModel: "cls-m"})}, true, "cls-m"},
		{"FallbackClassifierURLWithoutModel", &Classifier{cfg: config.NewFromData(&config.ConfigData{ClassifierURL: "http://test"})}, true, DefaultAGYClassifierModel},
		{"FallbackClassifierURLWithCustomModel", &Classifier{Model: "custom-cls-m", cfg: config.NewFromData(&config.ConfigData{ClassifierURL: "http://test"})}, true, "custom-cls-m"},
		{"TitleLowEffortModel", &Classifier{cfg: config.NewFromData(&config.ConfigData{LowEffortModel: "gemini-low"})}, true, "gemini-low"},
		{"TitleCustomModel", &Classifier{Model: "custom-model"}, true, "custom-model"},
		{"TitleNilConfigDefault", &Classifier{}, true, defaultLow},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var got string
			if tc.isTitle {
				got = tc.cls.resolveTitleModel()
			} else {
				got = tc.cls.resolveModel()
			}
			if got != tc.wantModel {
				t.Fatalf("got %q, want %q", got, tc.wantModel)
			}
		})
	}
}

func TestCleanThreadTitle(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"CleanSimpleTitle", `"Fixing Database Latency"`, "Fixing Database Latency"},
		{"StripMentionsAndBackticks", "`` <@123> `<#456>` @everyone Discussion on Go concurrency. ``", "Discussion on Go concurrency"},
		{"MultilineSelectFirstNonEmpty", "\n\n  \n  Optimizing Redis Cache Keys \n Second line", "Optimizing Redis Cache Keys"},
		{"RuneTruncationAt80", "This is a extremely long thread title designed to test rune truncation functionality when titles exceed maximum length limits", "This is a extremely long thread title designed to test rune truncation functi..."},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := CleanThreadTitle(tc.input); got != tc.expected {
				t.Fatalf("CleanThreadTitle(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestSummarizeThreadTitle(t *testing.T) {
	t.Parallel()
	dl := time.Now().Add(5 * time.Minute)
	ctxWithDL, cancelDL := context.WithDeadline(context.Background(), dl)
	t.Cleanup(cancelDL)

	tests := []struct {
		name       string
		nilCls     bool
		nilLLM     bool
		llmOut     string
		llmErr     error
		titleOut   string
		primaryOut string
		sysOneOut  string
		checkDL    bool
		ctx        context.Context
		q          string
		wantTitle  string
		wantErr    bool
	}{
		{name: "NilClassifierError", nilCls: true, ctx: context.Background(), q: "question", wantErr: true},
		{name: "NilLLMFunc", nilLLM: true, ctx: context.Background(), q: "question", wantErr: true},
		{name: "EmptyQuestion", llmOut: "ok", ctx: context.Background(), q: "   ", wantErr: true},
		{name: "LLMReturnsError", llmErr: errors.New("timeout"), ctx: context.Background(), q: "question", wantErr: true},
		{name: "LLMReturnsEmptyAfterClean", llmOut: "   \n\t  ", ctx: context.Background(), q: "question", wantErr: true},
		{name: "SuccessfulViaLLMFunc", llmOut: `"Database Latency Investigation."`, ctx: context.Background(), q: "why is db slow?", wantTitle: "Database Latency Investigation"},
		{name: "SuccessfulViaTitleLLMFunc", titleOut: "Title From TitleLLM", ctx: context.Background(), q: "why is db slow?", wantTitle: "Title From TitleLLM"},
		{name: "PrimaryTitleLLMFunc", primaryOut: "Primary Title Result", ctx: context.Background(), q: "question", wantTitle: "Primary Title Result"},
		{name: "SystemOneWithoutPrimaryTitleErrors", sysOneOut: "systemone", ctx: context.Background(), q: "question", wantErr: true},
		{name: "PreExistingContextDeadlinePreserved", checkDL: true, ctx: ctxWithDL, q: "question", wantTitle: "Title With DL"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var cls *Classifier
			switch {
			case tc.nilCls:
				cls = nil
			case tc.nilLLM:
				cls = &Classifier{Model: "m"}
			case tc.primaryOut != "":
				cls = New(nil)
				cls.primaryTitleLLMFunc = func(ctx context.Context, model, prompt string) (string, error) { return tc.primaryOut, nil }
			case tc.sysOneOut != "":
				cls = New(config.NewFromData(&config.ConfigData{ClassifierProtocol: "systemone", AgyBin: "echo", APIKey: "secret"}))
			case tc.checkDL:
				cls = &Classifier{Model: "gemini", LLMFunc: func(ctx context.Context, model, prompt string) (string, error) {
					if gotDL, ok := ctx.Deadline(); !ok || !gotDL.Equal(dl) {
						return "", fmt.Errorf("expected deadline %v, got %v", dl, gotDL)
					}
					return "Title With DL", nil
				}}
			case tc.titleOut != "":
				cls = &Classifier{TitleLLMFunc: func(ctx context.Context, model, prompt string) (string, error) { return tc.titleOut, nil }}
			default:
				cls = &Classifier{Model: "gemini", LLMFunc: func(ctx context.Context, model, prompt string) (string, error) { return tc.llmOut, tc.llmErr }}
			}
			got, err := cls.SummarizeThreadTitle(tc.ctx, tc.q)
			if (err != nil) != tc.wantErr || (!tc.wantErr && got != tc.wantTitle) {
				t.Fatalf("SummarizeThreadTitle got (%q, %v), want (%q, %v)", got, err, tc.wantTitle, tc.wantErr)
			}
		})
	}
}

func TestClassifier_ProcessPools(t *testing.T) {
	if cNil := New(nil, WithProcessPool(nil)); cNil == nil {
		t.Fatal("expected non-nil classifier for WithProcessPool(nil)")
	}

	var calledPrimary bool
	clsPrimary := New(nil)
	clsPrimary.primaryLLMFunc = func(ctx context.Context, model, prompt string) (string, error) {
		calledPrimary = true
		return `{"confidence":0.7,"reason":"from primary"}`, nil
	}
	resPrimary := clsPrimary.Classify(context.Background(), db.Message{Content: "test message"}, nil)
	if !calledPrimary || resPrimary.Confidence != 0.7 {
		t.Fatalf("primaryLLMFunc failed: called=%v, conf=%f", calledPrimary, resPrimary.Confidence)
	}

	mockSpawner := &classifierMockSpawner{
		spawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			outR, outW := io.Pipe()
			inR, inW := io.Pipe()
			errR, _ := io.Pipe()
			go func() {
				_, _ = fmt.Fprintf(outW, "{\"event\":\"init\",\"session_id\":\"00000000-0000-0000-0000-000000000099\"}\n")
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
					line := scanner.Text()
					if strings.Contains(line, "summarize") || strings.Contains(line, "nginx") {
						_, _ = fmt.Fprintf(outW, "{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"Summary from pool\"}}\n")
					} else {
						_, _ = fmt.Fprintf(outW, "{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"{\\\"confidence\\\":0.85,\\\"reason\\\":\\\"needs help\\\"}\"}}\n")
					}
				}
			}()
			return inW, outR, errR, &classifierMockProcessHandle{pid: 999}, nil
		},
	}

	pool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "test-model"}, mockSpawner)
	defer pool.Close()

	clsPool := New(nil, WithProcessPool(pool))
	resPool := clsPool.Classify(context.Background(), db.Message{Content: "Help needed"}, nil)
	if resPool.Confidence != 0.85 || resPool.Reason != "needs help" {
		t.Fatalf("unexpected pool classification: %+v", resPool)
	}
	titlePool, errTitle := clsPool.SummarizeThreadTitle(context.Background(), "How do I configure nginx?")
	if errTitle != nil || titlePool != "Summary from pool" {
		t.Fatalf("unexpected pool thread title: %v, %q", errTitle, titlePool)
	}

	interPool := runner.NewInterchangeablePool(pool, runner.InterchangeablePoolConfig{WorkerCount: 1})
	defer interPool.Close()

	clsInter := New(nil, WithProcessPool(interPool))
	resInter := clsInter.Classify(context.Background(), db.Message{Content: "Help needed"}, nil)
	if resInter.Confidence != 0.85 {
		t.Fatalf("unexpected interchangeable pool result: %+v", resInter)
	}
}

func TestBuildSystemOneState(t *testing.T) {
	msgMention := db.Message{
		AuthorID: "123456", Content: "<@111> check <@&222>",
		MentionUserIDs: []string{"111"}, MentionRoleIDs: []string{"222"}, Mentions: []string{"Alice", "Admins"}, ReplyingToAuthor: "bob",
	}

	historySix := []db.Message{
		{AuthorName: "user1", Content: strings.Repeat("1", 600)},
		{AuthorName: "user2", Content: strings.Repeat("2", 600)},
		{AuthorName: "user3", Content: strings.Repeat("3", 600)},
		{AuthorName: "user4", Content: strings.Repeat("4", 600)},
		{AuthorName: "user5", Content: strings.Repeat("5", 600)},
		{AuthorName: "user6", Content: strings.Repeat("6", 600)},
	}

	burstEight := []db.Message{
		{AuthorName: "target1", Content: strings.Repeat("a", 600)},
		{AuthorName: "target2", Content: strings.Repeat("b", 600)},
		{AuthorName: "target3", Content: strings.Repeat("c", 600)},
		{AuthorName: "target4", Content: strings.Repeat("d", 600)},
		{AuthorName: "target5", Content: strings.Repeat("e", 600)},
		{AuthorName: "target6", Content: strings.Repeat("f", 600)},
		{AuthorName: "target7", Content: strings.Repeat("g", 600)},
		{AuthorName: "target8", Content: strings.Repeat("h", 600)},
	}

	tests := []struct {
		name         string
		targets      []db.Message
		context      []db.Message
		wantExact    string
		wantPrefix   string
		wantContains []string
		wantExcluded []string
		checkRuneCap int
		checkLineLen int
	}{
		{
			name:      "nil messages",
			targets:   nil,
			context:   nil,
			wantExact: "",
		},
		{
			name:      "empty slice messages",
			targets:   []db.Message{},
			context:   []db.Message{},
			wantExact: "",
		},
		{
			name:      "formatted target with mentions and reply",
			targets:   []db.Message{msgMention},
			context:   nil,
			wantExact: "Target message:\n123456 (replying to @bob): @Alice check @Admins",
		},
		{
			name:         "single long message capped at 500 runes",
			targets:      []db.Message{{AuthorName: "alice", Content: "START_OF_MESSAGE " + strings.Repeat("x", 600) + " END_OF_MESSAGE"}},
			context:      nil,
			wantPrefix:   "Target message:\n",
			wantContains: []string{"START_OF_MESSAGE", "END_OF_MESSAGE", " ... [snip] ... "},
			checkLineLen: 500,
		},
		{
			name:         "context capped at MaxSystemOneContextMessages and remaining budget",
			targets:      []db.Message{{AuthorName: "user7", Content: strings.Repeat("7", 600)}},
			context:      historySix,
			wantPrefix:   "Recent context:\n",
			wantContains: []string{"\n\nTarget message:\n", "user2:", "user3:", "user4:", "user5:", "user6:", "user7:"},
			wantExcluded: []string{"user1:"},
			checkRuneCap: MaxSystemOneStateRunes,
		},
		{
			name:         "target burst capped within MaxSystemOneStateRunes tail style",
			targets:      burstEight,
			context:      nil,
			wantPrefix:   "Target message:\n",
			wantContains: []string{"target3:", "target4:", "target5:", "target6:", "target7:", "target8:"},
			wantExcluded: []string{"target1:", "target2:"},
			checkRuneCap: MaxSystemOneStateRunes,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildSystemOneState(tc.targets, tc.context)
			if tc.wantExact != "" || (len(tc.targets) == 0 && len(tc.context) == 0) {
				if got != tc.wantExact {
					t.Fatalf("BuildSystemOneState() = %q, want %q", got, tc.wantExact)
				}
				return
			}
			if tc.wantPrefix != "" && !strings.HasPrefix(got, tc.wantPrefix) {
				t.Fatalf("BuildSystemOneState() missing prefix %q in: %q", tc.wantPrefix, got)
			}
			for _, sub := range tc.wantContains {
				if !strings.Contains(got, sub) {
					t.Fatalf("BuildSystemOneState() missing substring %q in: %q", sub, got)
				}
			}
			for _, sub := range tc.wantExcluded {
				if strings.Contains(got, sub) {
					t.Fatalf("BuildSystemOneState() unexpectedly contains %q in: %q", sub, got)
				}
			}
			if tc.checkRuneCap > 0 {
				if runeCount := utf8.RuneCountInString(got); runeCount > tc.checkRuneCap {
					t.Fatalf("BuildSystemOneState() runes = %d, exceeds cap %d", runeCount, tc.checkRuneCap)
				}
			}
			if tc.checkLineLen > 0 && tc.wantPrefix != "" {
				line := strings.TrimPrefix(got, tc.wantPrefix)
				if runeCount := utf8.RuneCountInString(line); runeCount != tc.checkLineLen {
					t.Fatalf("message line runes = %d, want %d: %q", runeCount, tc.checkLineLen, line)
				}
			}
		})
	}
}

func TestClassify_SystemOne(t *testing.T) {
	tests := []struct {
		name          string
		emptyURL      bool
		customURL     string
		customModel   string
		statusCode    int
		respBody      string
		retryFailures int
		cancelSleep   bool
		isBurst       bool
		wantConf      float64
		wantReasonSub string
		wantAlert     bool
	}{
		{"empty ClassifierURL", true, "", "", 0, "", 0, false, false, 0.0, "classifier_url is empty", false},
		{"success with noul", false, "", "", 200, `{"model":"laya-cuda","answers":{"should_wake":{"type":"noul","noul":0.88}}}`, 0, false, false, 0.88, "systemone", false},
		{"confidence fallback", false, "", "", 200, `{"answers":{"should_wake":{"type":"noul","confidence":0.65}}}`, 0, false, false, 0.65, "systemone", false},
		{"clamp below zero", false, "", "", 200, `{"answers":{"should_wake":{"confidence":-0.5}}}`, 0, false, false, 0.0, "systemone", false},
		{"clamp above one", false, "", "", 200, `{"answers":{"should_wake":{"confidence":1.5}}}`, 0, false, false, 1.0, "systemone", false},
		{"missing should_wake", false, "", "", 200, `{"answers":{}}`, 0, false, false, 0.0, "missing 'should_wake'", true},
		{"response error field", false, "", "", 200, `{"error":"model out of memory"}`, 0, false, false, 0.0, "model out of memory", true},
		{"invalid json", false, "", "", 200, `not valid json {`, 0, false, false, 0.0, "failed to parse systemone json", true},
		{"non-200 truncation", false, "", "", 503, strings.Repeat("error detail string ", 20), 0, false, false, 0.0, "HTTP 503", true},
		{"3-attempt recovery", false, "", "", 200, `{"answers":{"should_wake":{"confidence":0.95}}}`, 2, false, false, 0.95, "systemone", false},
		{"cancel during sleep", false, "", "", 500, "fail", 0, true, false, 0.0, "context cancelled", true},
		{"burst model override", false, "", "custom-modernbert", 200, `{"answers":{"should_wake":{"confidence":0.77}}}`, 0, false, true, 0.77, "systemone", false},
		{"malformed URL aborts immediately", false, "://bad-url", "", 0, "", 0, false, false, 0.0, "failed to create http request", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var attempts int32
			var lastReq systemOneRequest
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewDecoder(r.Body).Decode(&lastReq)
				curr := atomic.AddInt32(&attempts, 1)
				if int(curr) <= tc.retryFailures {
					http.Error(w, "transient error", http.StatusInternalServerError)
					return
				}
				if tc.statusCode != 0 && tc.statusCode != http.StatusOK {
					http.Error(w, tc.respBody, tc.statusCode)
					return
				}
				_, _ = w.Write([]byte(tc.respBody))
			}))
			defer ts.Close()

			serverURL := ts.URL
			if tc.emptyURL {
				serverURL = ""
			} else if tc.customURL != "" {
				serverURL = tc.customURL
			}

			cfg := config.NewFromData(&config.ConfigData{
				ClassifierURL:      serverURL,
				ClassifierProtocol: "systemone",
				ClassifierModel:    tc.customModel,
			})

			var alerted bool
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			cls := New(cfg)
			cls.systemOneHTTPClient = ts.Client()
			cls.retryDelayFunc = func(attempt int) time.Duration { return 0 }
			cls.retrySleepFunc = func(c context.Context, d time.Duration) error {
				if tc.cancelSleep {
					cancel()
					return c.Err()
				}
				return nil
			}
			cls.OnSystemAlert = func(endpoint string, err error) { alerted = true }

			var res ClassificationResult
			if tc.emptyURL {
				res = cls.classifySystemOne(ctx, []db.Message{{AuthorName: "alex", Content: "test msg"}}, nil)
			} else if tc.isBurst {
				res = cls.ClassifyBurst(ctx, []db.Message{{AuthorName: "alex", Content: "burst msg 1"}, {AuthorName: "alex", Content: "burst msg 2"}}, nil)
			} else {
				res = cls.Classify(ctx, db.Message{AuthorName: "alex", Content: "test msg"}, nil)
			}

			if res.Confidence != tc.wantConf || (tc.wantReasonSub != "" && !strings.Contains(res.Reason, tc.wantReasonSub)) || alerted != tc.wantAlert {
				t.Fatalf("got res=%+v, alerted=%v; want conf=%f, reasonSub=%q, alert=%v", res, alerted, tc.wantConf, tc.wantReasonSub, tc.wantAlert)
			}
			if !tc.emptyURL && !tc.cancelSleep && tc.statusCode == 200 && tc.wantConf > 0 {
				q, ok := lastReq.Questions["should_wake"]
				if !ok || q.Type != "noul" || q.Instructions != DefaultSystemOneWakePrompt ||
					q.Criteria["true"] != DefaultSystemOneTrueCriteria ||
					q.Criteria["false"] != DefaultSystemOneFalseCriteria {
					t.Fatalf("unexpected request structure: %+v", lastReq)
				}
			}
		})
	}
}
