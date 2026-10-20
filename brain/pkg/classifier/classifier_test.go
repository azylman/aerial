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

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/runner"
)

func mkMsg(author, content string, ts time.Time) db.Message {
	return db.Message{AuthorName: author, Content: content, CreatedAt: ts}
}

func mkReplyMsg(author, replyTo, content string, ts time.Time) db.Message {
	return db.Message{AuthorName: author, Content: content, CreatedAt: ts, Metadata: db.MessageMetadata{ReplyingToAuthor: replyTo}}
}

func TestClassifier_DefaultsAndOptions(t *testing.T) {
	tests := []struct {
		name           string
		cfg            *config.Config
		runnerStdout   string
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
		{name: "nil config defaults with nil runner", wantModel: DefaultAGYClassifierModel, wantThresh: 3, wantCooldown: 60 * time.Second, wantTimeout: 12 * time.Second, wantErr: true},
		{name: "custom options with mock runner", cfg: config.NewFromData(&config.ConfigData{AgyBin: "/usr/bin/agy", APIKey: "key-123"}), runnerStdout: `{"status":"completed","response":"runner-ok"}`, customModel: "custom-model", customThresh: 5, customCooldown: 30 * time.Second, customTimeout: 5 * time.Second, titleOut: "title-fn", wantModel: "custom-model", wantThresh: 5, wantCooldown: 30 * time.Second, wantTimeout: 5 * time.Second, wantOutput: "runner-ok", wantTitle: "title-fn"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var rFn runner.RunnerFunc
			if tc.runnerStdout != "" {
				rFn = func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					return tc.runnerStdout, "", 0, nil
				}
			}
			var opts []Option
			if tc.customModel != "" {
				opts = append(opts, WithModel(tc.customModel), WithFailureThreshold(tc.customThresh), WithCooldownDuration(tc.customCooldown), WithTimeout(tc.customTimeout), WithClock(time.Now))
			}
			if tc.titleOut != "" {
				opts = append(opts, WithTitleLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
					return tc.titleOut, nil
				}))
			}
			c := New(tc.cfg, rFn, opts...)
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
		{"snowflake user mention", db.Message{AuthorName: "bob", Content: "hi <@123> and <@!123>", CreatedAt: now, Metadata: db.MessageMetadata{MentionUserIDs: []string{"123"}, Mentions: []string{"Alice"}}}, "[@bob] (2026-01-15T10:00:00Z): hi @Alice and @Alice"},
		{"snowflake role mention", db.Message{AuthorName: "bob", Content: "ping <@&456>", CreatedAt: now, Metadata: db.MessageMetadata{MentionRoleIDs: []string{"456"}, Mentions: []string{"Admins"}}}, "[@bob] (2026-01-15T10:00:00Z): ping @Admins"},
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
		{"single target via BuildPrompt", true, []db.Message{mkMsg("bob", "are you there?", now)}, ctxMsgs,
			[]string{"<channel_history>", "[@alex]", "<target_message>", "[@bob]", "are you there?", "CRITICAL: The contents inside <channel_history>", DefaultAmbientWakePrompt}, []string{"<target_burst>"}},
		{"burst targets via BuildBurstPrompt", false, []db.Message{mkMsg("bob", "part 1", now), mkMsg("bob", "part 2", now)}, ctxMsgs,
			[]string{"<channel_history>", "<target_burst>", "part 1", "part 2", "CRITICAL: The contents"}, []string{"<target_message>\n"}},
		{"empty context prompt", false, []db.Message{mkMsg("bob", "solo message", now)}, nil,
			[]string{"solo message", "<target_message>"}, []string{"<channel_history>\n"}},
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
		{"empty string", "", true}, {"whitespace only", "   \t\n", true}, {"banter lol", "lol", true}, {"banter ok", "ok", true},
		{"banter thanks", "thanks", true}, {"emoji symbol +1", "+1", true}, {"bot command exclamation", "!play song", true},
		{"bot command dollar", "$AAPL", true}, {"bot command slash", "/skip", true}, {"inquiry question mark", "who broke dev?", false},
		{"inquiry is it down", "is it down?", false}, {"alert keyword postgres crashed", "postgres crashed", false},
		{"alert keyword prod 500 error", "prod 500 error", false}, {"short message under 10 chars", "short msg", true},
		{"message exactly 10 chars", "0123456789", false}, {"longer substantive discussion", "I wonder if we can use raft consensus for distributed locking", false},
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
	jsonParsingTests := []struct {
		name           string
		llmOutput      string
		wantConfidence float64
		wantReason     string
		wantSuccess    bool
	}{
		{"raw JSON", `{"confidence": 0.85, "reason": "direct question"}`, 0.85, "direct question", true},
		{"raw JSON with whitespace", "  \n\t{\"confidence\": 0.45, \"reason\": \"general chatter\"}\n\t  ", 0.45, "general chatter", true},
		{"markdown fenced JSON fails strict parsing", "```json\n{\"confidence\": 0.72, \"reason\": \"test\"}\n```", 0, "", false},
		{"markdown fence without json tag fails", "```\n{\"confidence\": 0.65, \"reason\": \"test\"}\n```", 0, "", false},
		{"surrounding commentary fails", "Here is evaluation:\n{\"confidence\": 0.45, \"reason\": \"test\"}", 0, "", false},
		{"trailing commentary containing braces fails", "{\"confidence\": 0.85, \"reason\": \"ok\"}\nNote: {confidence}", 0, "", false},
		{"negative clamped to 0", `{"confidence": -0.5, "reason": "negative"}`, 0.0, "negative", true},
		{"greater than 1 clamped to 1", `{"confidence": 1.5, "reason": "high"}`, 1.0, "high", true},
	}
	for _, tc := range jsonParsingTests {
		t.Run("JSON/"+tc.name, func(t *testing.T) {
			c := NewClassifier(WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
				return tc.llmOutput, nil
			}))
			res := c.Classify(context.Background(), db.Message{Content: "test"}, nil)
			if tc.wantSuccess {
				if res.Confidence != tc.wantConfidence || res.Reason != tc.wantReason {
					t.Fatalf("got %+v, want conf %f, reason %q", res, tc.wantConfidence, tc.wantReason)
				}
			} else if res.Confidence != 0.0 || !strings.Contains(res.Reason, "classifier error") {
				t.Fatalf("expected failure, got %+v", res)
			}
		})
	}

	edgeCaseTests := []struct {
		name        string
		llmOut      string
		nilLLM      bool
		blankModel  bool
		timeoutWait bool
		burst       []db.Message
		useBurst    bool
		useNilCtx   bool
		wantConf    float64
		wantReason  string
	}{
		{name: "empty burst returns 0 confidence", useBurst: true, wantConf: 0.0, wantReason: "empty target burst"},
		{name: "ClassifyBurst with valid output", llmOut: `{"confidence": 0.9, "reason": "burst query"}`, burst: []db.Message{{Content: "m1"}, {Content: "m2"}}, useBurst: true, wantConf: 0.9, wantReason: "burst query"},
		{name: "nil context handled gracefully", llmOut: `{"confidence": 0.8, "reason": "ok"}`, burst: []db.Message{{Content: "msg"}}, useNilCtx: true, wantConf: 0.8, wantReason: "ok"},
		{name: "nil LLMFunc error", nilLLM: true, burst: []db.Message{{Content: "msg"}}, wantConf: 0.0, wantReason: "no LLMFunc configured"},
		{name: "empty model error", blankModel: true, burst: []db.Message{{Content: "msg"}}, wantConf: 0.0, wantReason: "model is not configured"},
		{name: "timeout handling", timeoutWait: true, burst: []db.Message{{Content: "msg"}}, wantConf: 0.0, wantReason: "classifier error"},
	}
	for _, tc := range edgeCaseTests {
		t.Run("Edge/"+tc.name, func(t *testing.T) {
			var cls *Classifier
			switch {
			case tc.nilLLM:
				cls = &Classifier{Model: "test-model"}
			case tc.blankModel:
				cls = &Classifier{Model: "   ", cfg: config.NewFromData(&config.ConfigData{LowEffortModel: "   "}), LLMFunc: func(ctx context.Context, model, prompt string) (string, error) { return "", nil }}
			case tc.timeoutWait:
				cls = NewClassifier(WithTimeout(10*time.Millisecond), WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) { <-ctx.Done(); return "", ctx.Err() }))
			case tc.llmOut != "":
				cls = NewClassifier(WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) { return tc.llmOut, nil }))
			default:
				cls = NewClassifier()
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
				res = cls.classifyWithPrompt(ctx, "prompt")
			}
			if res.Confidence != tc.wantConf || !strings.Contains(res.Reason, tc.wantReason) {
				t.Fatalf("got %+v, want conf %f, reason substring %q", res, tc.wantConf, tc.wantReason)
			}
		})
	}

	var capturedModel, capturedRaw string
	var capturedErr error
	cParse := NewClassifier(
		WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) { return "not valid json {", nil }),
		WithOnParseError(func(model, raw string, err error) { capturedModel = model; capturedRaw = raw; capturedErr = err }),
	)
	cParse.Classify(context.Background(), db.Message{Content: "test"}, nil)
	if capturedRaw != "not valid json {" || capturedErr == nil || capturedModel != DefaultAGYClassifierModel {
		t.Fatalf("OnParseError failed: model=%q, raw=%q, err=%v", capturedModel, capturedRaw, capturedErr)
	}
}

func TestCircuitBreaker(t *testing.T) {
	currTime := time.Date(2026, 9, 2, 10, 0, 0, 0, time.UTC)
	var timeMu sync.Mutex
	getTime := func() time.Time { timeMu.Lock(); defer timeMu.Unlock(); return currTime }
	advanceTime := func(d time.Duration) { timeMu.Lock(); defer timeMu.Unlock(); currTime = currTime.Add(d) }

	var callCount, shouldFail int32
	atomic.StoreInt32(&shouldFail, 1)

	c := NewClassifier(
		WithFailureThreshold(3), WithCooldownDuration(60*time.Second), WithClock(getTime),
		WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
			atomic.AddInt32(&callCount, 1)
			if atomic.LoadInt32(&shouldFail) == 1 {
				return "", errors.New("simulated failure")
			}
			return `{"confidence": 0.88, "reason": "healthy now"}`, nil
		}),
	)

	target := db.Message{Content: "hello"}
	for i := 1; i <= 2; i++ {
		res := c.Classify(context.Background(), target, nil)
		if res.Confidence != 0.0 || c.ConsecutiveFailures() != i || c.IsCircuitOpen() {
			t.Fatalf("call %d: failures=%d, open=%v", i, c.ConsecutiveFailures(), c.IsCircuitOpen())
		}
	}

	res3 := c.Classify(context.Background(), target, nil)
	if res3.Confidence != 0.0 || c.ConsecutiveFailures() != 3 || !c.IsCircuitOpen() {
		t.Fatalf("call 3: failures=%d, open=%v", c.ConsecutiveFailures(), c.IsCircuitOpen())
	}

	res4 := c.Classify(context.Background(), target, nil)
	if res4.Confidence != 0.0 || res4.Reason != "circuit breaker open" || atomic.LoadInt32(&callCount) != 3 {
		t.Fatalf("call 4: expected circuit open, got %+v, calls=%d", res4, atomic.LoadInt32(&callCount))
	}

	advanceTime(61 * time.Second)
	atomic.StoreInt32(&shouldFail, 0)
	res5 := c.Classify(context.Background(), target, nil)
	if res5.Confidence != 0.88 || c.IsCircuitOpen() || c.ConsecutiveFailures() != 0 {
		t.Fatalf("call 5: expected recovery, got %+v, failures=%d, open=%v", res5, c.ConsecutiveFailures(), c.IsCircuitOpen())
	}

	cParse := NewClassifier(WithFailureThreshold(2), WithCooldownDuration(60*time.Second), WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) { return "bad-json", nil }))
	cParse.Classify(context.Background(), target, nil)
	cParse.Classify(context.Background(), target, nil)
	if cParse.ConsecutiveFailures() != 2 || !cParse.IsCircuitOpen() {
		t.Fatalf("expected 2 parse failures and open circuit, got %d, open=%v", cParse.ConsecutiveFailures(), cParse.IsCircuitOpen())
	}

	cConc := NewClassifier(WithFailureThreshold(5))
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
			_ = cConc.IsCircuitOpen()
			_ = cConc.ConsecutiveFailures()
		}(i)
	}
	wg.Wait()
}

func TestNewAgyLLMFunc(t *testing.T) {
	tests := []struct {
		name       string
		nilRunner  bool
		stdout     string
		stderr     string
		exitCode   int
		wantSub    string
		wantErrSub string
	}{
		{name: "success with response field", stdout: `{"conversation_id":"ambient-eval-123","status":"SUCCESS","response":"{\"confidence\": 0.95}"}`, wantSub: "0.95"},
		{name: "nil runner error", nilRunner: true, wantErrSub: "no runner function configured"},
		{name: "runner returns non-zero exit code", stderr: "process died", exitCode: 1, wantErrSub: "agy classification failed (exit 1)"},
		{name: "runner returns bad JSON stdout", stdout: "not valid json", wantErrSub: "failed to parse agy json output"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var rFn runner.RunnerFunc
			if !tc.nilRunner {
				rFn = func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					return tc.stdout, tc.stderr, tc.exitCode, nil
				}
			}
			fn := NewAgyLLMFunc("agy", "key", rFn)
			out, err := fn(context.Background(), "gemini-3.8-flash-low", "prompt")
			if tc.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErrSub, err)
				}
			} else if err != nil || !strings.Contains(out, tc.wantSub) {
				t.Fatalf("expected output containing %q, got (%q, %v)", tc.wantSub, out, err)
			}
		})
	}
}

func TestResolveModelAndTitleModel(t *testing.T) {
	defaultLow := config.DefaultConfigData().LowEffortModel

	resolveModelTests := []struct {
		name      string
		cls       *Classifier
		wantModel string
	}{
		{"classifier URL with model", &Classifier{cfg: config.NewFromData(&config.ConfigData{ClassifierURL: "http://test", ClassifierModel: "ollama-m"})}, "ollama-m"},
		{"classifier URL without model", &Classifier{cfg: config.NewFromData(&config.ConfigData{ClassifierURL: "http://test"})}, DefaultAGYClassifierModel},
		{"classifier URL set with custom c.Model", &Classifier{Model: "custom-qwen", cfg: config.NewFromData(&config.ConfigData{ClassifierURL: "http://test"})}, "custom-qwen"},
		{"low effort model configured", &Classifier{cfg: config.NewFromData(&config.ConfigData{LowEffortModel: "gemini-low"})}, "gemini-low"},
		{"custom c.Model without URL", &Classifier{Model: "custom-flash", cfg: config.NewFromData(&config.ConfigData{LowEffortModel: "gemini-default"})}, "custom-flash"},
		{"nil config default", &Classifier{}, defaultLow},
	}
	for _, tc := range resolveModelTests {
		t.Run("resolveModel/"+tc.name, func(t *testing.T) {
			if got := tc.cls.resolveModel(); got != tc.wantModel {
				t.Fatalf("got %q, want %q", got, tc.wantModel)
			}
		})
	}

	resolveTitleTests := []struct {
		name      string
		cls       *Classifier
		wantModel string
	}{
		{"thread title URL with model", &Classifier{cfg: config.NewFromData(&config.ConfigData{ThreadTitleURL: "http://test", ThreadTitleModel: "title-m"})}, "title-m"},
		{"thread title URL without model", &Classifier{cfg: config.NewFromData(&config.ConfigData{ThreadTitleURL: "http://test"})}, DefaultAGYClassifierModel},
		{"thread title URL with custom c.Model", &Classifier{Model: "custom-title-m", cfg: config.NewFromData(&config.ConfigData{ThreadTitleURL: "http://test"})}, "custom-title-m"},
		{"fallback classifier URL with model", &Classifier{cfg: config.NewFromData(&config.ConfigData{ClassifierURL: "http://test", ClassifierModel: "cls-m"})}, "cls-m"},
		{"fallback classifier URL without model", &Classifier{cfg: config.NewFromData(&config.ConfigData{ClassifierURL: "http://test"})}, DefaultAGYClassifierModel},
		{"fallback classifier URL with custom c.Model", &Classifier{Model: "custom-cls-m", cfg: config.NewFromData(&config.ConfigData{ClassifierURL: "http://test"})}, "custom-cls-m"},
		{"low effort model", &Classifier{cfg: config.NewFromData(&config.ConfigData{LowEffortModel: "gemini-low"})}, "gemini-low"},
		{"custom c.Model", &Classifier{Model: "custom-model"}, "custom-model"},
		{"nil config default", &Classifier{}, defaultLow},
	}
	for _, tc := range resolveTitleTests {
		t.Run("resolveTitleModel/"+tc.name, func(t *testing.T) {
			if got := tc.cls.resolveTitleModel(); got != tc.wantModel {
				t.Fatalf("got %q, want %q", got, tc.wantModel)
			}
		})
	}
}

func TestCleanAndSummarizeThreadTitle(t *testing.T) {
	cleanTests := []struct {
		name     string
		input    string
		expected string
	}{
		{"clean simple title", `"Fixing Database Latency"`, "Fixing Database Latency"},
		{"strip mentions and backticks", "`` <@123> `<#456>` @everyone Discussion on Go concurrency. ``", "Discussion on Go concurrency"},
		{"multiline select first non-empty", "\n\n  \n  Optimizing Redis Cache Keys \n Second line", "Optimizing Redis Cache Keys"},
		{"rune truncation at 80", "This is a extremely long thread title designed to test rune truncation functionality when titles exceed maximum length limits", "This is a extremely long thread title designed to test rune truncation functi..."},
	}
	for _, tc := range cleanTests {
		t.Run("Clean/"+tc.name, func(t *testing.T) {
			if got := CleanThreadTitle(tc.input); got != tc.expected {
				t.Fatalf("CleanThreadTitle(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}

	dl := time.Now().Add(5 * time.Minute)
	ctxWithDL, cancelDL := context.WithDeadline(context.Background(), dl)
	defer cancelDL()

	summarizeTests := []struct {
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
		{name: "nil classifier error", nilCls: true, ctx: context.Background(), q: "question", wantErr: true},
		{name: "nil LLMFunc", nilLLM: true, ctx: context.Background(), q: "question", wantErr: true},
		{name: "empty question", llmOut: "ok", ctx: context.Background(), q: "   ", wantErr: true},
		{name: "LLM returns error", llmErr: errors.New("timeout"), ctx: context.Background(), q: "question", wantErr: true},
		{name: "LLM returns empty after clean", llmOut: "   \n\t  ", ctx: context.Background(), q: "question", wantErr: true},
		{name: "successful via LLMFunc", llmOut: `"Database Latency Investigation."`, ctx: context.Background(), q: "why is db slow?", wantTitle: "Database Latency Investigation"},
		{name: "successful via TitleLLMFunc", titleOut: "Title From TitleLLM", ctx: context.Background(), q: "why is db slow?", wantTitle: "Title From TitleLLM"},
		{name: "primary title LLMFunc", primaryOut: "Primary Title Result", ctx: context.Background(), q: "question", wantTitle: "Primary Title Result"},
		{name: "systemone fallback to runnerFn", sysOneOut: `{"status":"completed","response":"Generated Title"}`, ctx: context.Background(), q: "question", wantTitle: "Generated Title"},
		{name: "pre-existing context deadline preserved", checkDL: true, ctx: ctxWithDL, q: "question", wantTitle: "Title With DL"},
	}
	for _, tc := range summarizeTests {
		t.Run("Summarize/"+tc.name, func(t *testing.T) {
			var cls *Classifier
			switch {
			case tc.nilCls:
				cls = nil
			case tc.nilLLM:
				cls = &Classifier{Model: "m"}
			case tc.primaryOut != "":
				cls = New(nil, nil, WithPrimaryTitleLLMFunc(func(ctx context.Context, model, prompt string) (string, error) { return tc.primaryOut, nil }))
			case tc.sysOneOut != "":
				cls = New(config.NewFromData(&config.ConfigData{ClassifierProtocol: "systemone", AgyBin: "echo", APIKey: "secret"}), func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
					return tc.sysOneOut, "", 0, nil
				})
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
	if cNil := New(nil, nil, WithProcessPool(nil)); cNil == nil {
		t.Fatal("expected non-nil classifier for WithProcessPool(nil)")
	}

	var calledPrimary bool
	clsPrimary := New(nil, nil, WithPrimaryLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
		calledPrimary = true
		return `{"confidence":0.7,"reason":"from primary"}`, nil
	}))
	resPrimary := clsPrimary.Classify(context.Background(), db.Message{Content: "test message"}, nil)
	if !calledPrimary || resPrimary.Confidence != 0.7 {
		t.Fatalf("primaryLLMFunc failed: called=%v, conf=%f", calledPrimary, resPrimary.Confidence)
	}

	mockSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
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
			return inW, outR, errR, runner.NewMockProcessHandle(999), nil
		},
	}

	pool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "test-model"}, mockSpawner)
	defer pool.Close()

	clsPool := New(nil, nil, WithProcessPool(pool))
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

	clsInter := New(nil, nil, WithProcessPool(interPool))
	resInter := clsInter.Classify(context.Background(), db.Message{Content: "Help needed"}, nil)
	if resInter.Confidence != 0.85 {
		t.Fatalf("unexpected interchangeable pool result: %+v", resInter)
	}
}

func TestBuildSystemOneState(t *testing.T) {
	if got := BuildSystemOneState(nil, nil); got != "" {
		t.Fatalf("expected empty string for nil messages, got %q", got)
	}
	if got := BuildSystemOneState([]db.Message{}, []db.Message{}); got != "" {
		t.Fatalf("expected empty string for empty slice messages, got %q", got)
	}

	msg := db.Message{
		AuthorID: "123456", Content: "<@111> check <@&222>",
		Metadata: db.MessageMetadata{MentionUserIDs: []string{"111"}, MentionRoleIDs: []string{"222"}, Mentions: []string{"Alice", "Admins"}, ReplyingToAuthor: "bob"},
	}
	state := BuildSystemOneState([]db.Message{msg}, nil)
	if !strings.Contains(state, "123456 (replying to @bob): @Alice check @Admins") {
		t.Fatalf("unexpected state formatting: %q", state)
	}

	longMsg := db.Message{AuthorName: "alex", Content: "START_OF_MESSAGE " + strings.Repeat("x", 500) + " END_OF_MESSAGE"}
	longState := BuildSystemOneState([]db.Message{longMsg}, nil)
	if len([]rune(longState)) != MaxSystemOneMessageRunes || !strings.Contains(longState, "START_OF_MESSAGE") || !strings.Contains(longState, "END_OF_MESSAGE") || !strings.Contains(longState, ModernBERTTruncationIndicator) {
		t.Fatalf("expected head, tail, and indicator capped at %d runes: %q", MaxSystemOneMessageRunes, longState)
	}

	historyMulti := []db.Message{
		{AuthorName: "user1", Content: strings.Repeat("1", 300)}, {AuthorName: "user2", Content: strings.Repeat("2", 300)},
		{AuthorName: "user3", Content: strings.Repeat("3", 300)}, {AuthorName: "user4", Content: strings.Repeat("4", 300)},
	}
	burstMulti := []db.Message{{AuthorName: "user5", Content: strings.Repeat("5", 300)}}
	multiState := BuildSystemOneState(burstMulti, historyMulti)
	if len([]rune(multiState)) > MaxSystemOneStateRunes || strings.Contains(multiState, "user1:") || strings.Contains(multiState, "user2:") ||
		!strings.Contains(multiState, "user3:") || !strings.Contains(multiState, "user4:") || !strings.Contains(multiState, "user5:") {
		t.Fatalf("unexpected tail-style multi-message state: %q", multiState)
	}
}

func TestClassify_SystemOne(t *testing.T) {
	tests := []struct {
		name          string
		emptyURL      bool
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
		{"empty ClassifierURL", true, "", 0, "", 0, false, false, 0.0, "classifier_url is empty", false},
		{"success with noul", false, "", 200, `{"model":"orin-laya","answers":{"should_wake":{"type":"noul","noul":0.88}}}`, 0, false, false, 0.88, "systemone", false},
		{"confidence fallback", false, "", 200, `{"answers":{"should_wake":{"type":"noul","confidence":0.65}}}`, 0, false, false, 0.65, "systemone", false},
		{"clamp below zero", false, "", 200, `{"answers":{"should_wake":{"confidence":-0.5}}}`, 0, false, false, 0.0, "systemone", false},
		{"clamp above one", false, "", 200, `{"answers":{"should_wake":{"confidence":1.5}}}`, 0, false, false, 1.0, "systemone", false},
		{"missing should_wake", false, "", 200, `{"answers":{}}`, 0, false, false, 0.0, "missing 'should_wake'", true},
		{"response error field", false, "", 200, `{"error":"model out of memory"}`, 0, false, false, 0.0, "model out of memory", true},
		{"invalid json", false, "", 200, `not valid json {`, 0, false, false, 0.0, "failed to parse systemone json", true},
		{"non-200 truncation", false, "", 503, strings.Repeat("error detail string ", 20), 0, false, false, 0.0, "HTTP 503", true},
		{"3-attempt recovery", false, "", 200, `{"answers":{"should_wake":{"confidence":0.95}}}`, 2, false, false, 0.95, "systemone", false},
		{"cancel during sleep", false, "", 500, "fail", 0, true, false, 0.0, "context cancelled", true},
		{"burst model override", false, "custom-modernbert", 200, `{"answers":{"should_wake":{"confidence":0.77}}}`, 0, false, true, 0.77, "systemone", false},
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
			}

			cfg := config.NewFromData(&config.ConfigData{
				ClassifierURL:      serverURL,
				ClassifierProtocol: "systemone",
				ClassifierModel:    tc.customModel,
			})

			var alerted bool
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			cls := New(cfg, nil,
				WithSystemOneHTTPClient(ts.Client()),
				WithRetryDelayFunc(func(attempt int) time.Duration { return 0 }),
				WithRetrySleepFunc(func(c context.Context, d time.Duration) error {
					if tc.cancelSleep {
						cancel()
						return c.Err()
					}
					return nil
				}),
				WithOnSystemAlert(func(endpoint string, err error) { alerted = true }),
			)

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
				if !ok || q.Type != "noul" || q.Instructions != DefaultAmbientWakePrompt {
					t.Fatalf("unexpected request structure: %+v", lastReq)
				}
			}
		})
	}
}
