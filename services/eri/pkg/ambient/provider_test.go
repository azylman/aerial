package ambient

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/config"
)

// SetNowFunc overrides the clock function in tests.
func (p *Provider) SetNowFunc(fn func() time.Time) {
	if fn == nil {
		p.nowFunc = time.Now
	} else {
		p.nowFunc = fn
	}
}

// mockInvoker answers Execute from declarative per-tool fields; execFunc is only for the
// concurrency tests that need to block or count inside the call.
type mockInvoker struct {
	mu       sync.Mutex
	calls    int
	results  map[string]string // tool name -> response text
	errs     map[string]error  // tool name -> error
	waitCtx  bool              // block until ctx is done and return its error
	execFunc func(ctx context.Context, name string, args map[string]any) (string, error)
}

func (m *mockInvoker) Execute(ctx context.Context, name string, args map[string]any) (string, error) {
	m.mu.Lock()
	m.calls++
	fn := m.execFunc
	m.mu.Unlock()
	switch {
	case fn != nil:
		return fn(ctx, name, args)
	case m.waitCtx:
		<-ctx.Done()
		return "", ctx.Err()
	case m.errs[name] != nil:
		return "", m.errs[name]
	case m.results[name] != "":
		return m.results[name], nil
	}
	return "{}", nil
}

func (m *mockInvoker) CallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

// providerErrMatched fails on an unexpected or missing error and reports whether the row
// expected one (so the caller stops).
func providerErrMatched(t *testing.T, op string, err error, wantErr string) bool {
	t.Helper()
	if wantErr == "" {
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return false
	}
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("%s error = %v, want %q", op, err, wantErr)
	}
	return true
}

func TestProvider_Retrieve(t *testing.T) {
	t.Parallel()
	tools := func(pairs ...string) []config.AmbientToolConfig {
		var out []config.AmbientToolConfig
		for i := 0; i+1 < len(pairs); i += 2 {
			out = append(out, config.AmbientToolConfig{Name: pairs[i], Tool: pairs[i+1]})
		}
		return out
	}
	wrap := func(body string) string { return "<ambient_context>\n" + body + "\n</ambient_context>" }
	tests := []struct {
		name       string
		template   string
		tools      []config.AmbientToolConfig
		inv        *mockInvoker // nil means a nil MCPInvoker
		cancelCtx  bool
		want       string
		wantNewErr string // NewProvider error substring
		wantErr    string // Retrieve error substring
	}{
		{name: "nil invoker renders tool error", template: "Error: {{.ha.error}}", tools: tools("ha", "get_state"), want: wrap("Error: nil mcp invoker")},
		{name: "empty tools renders now", template: "Time: {{.now}}", tools: []config.AmbientToolConfig{}, want: wrap("Time: 12:00:00")},
		{name: "empty template renders nothing", want: ""},
		{
			name:     "multi tool json and text helpers",
			template: "Music: {{upper .music.status}} - {{.music.artist}}\nWeather: {{trim .weather.text}}\nJSON: {{json .music}}\nTime: {{.now}}",
			tools:    tools("music", "music_status", "weather", "weather_report"),
			inv:      &mockInvoker{results: map[string]string{"music_status": `{"status": "playing", "artist": "lofi"}`, "weather_report": "72F Sunny with clouds"}},
			want:     wrap("Music: PLAYING - lofi\nWeather: 72F Sunny with clouds\nJSON: {\"artist\":\"lofi\",\"status\":\"playing\"}\nTime: 12:00:00"),
		},
		{
			name: "partial tool failure still renders", template: "Failed: {{.tool1.error}}\nTemp: {{.tool2.temperature}}", tools: tools("tool1", "failing_tool", "tool2", "healthy_tool"),
			inv:  &mockInvoker{results: map[string]string{"healthy_tool": `{"temperature": 68}`}, errs: map[string]error{"failing_tool": errors.New("connection timed out")}},
			want: wrap("Failed: connection timed out\nTemp: 68"),
		},
		{
			name:     "missing keys render as empty strings",
			template: "Missing: '{{.nonexistent}}'\nDefaulted: '{{default \"fallback_val\" .missing_key}}'\nLower: '{{lower .not_found}}'\nUpper: '{{upper .not_found}}'\nTrim: '{{trim .not_found}}'",
			want:     wrap("Missing: ''\nDefaulted: 'fallback_val'\nLower: ''\nUpper: ''\nTrim: ''"),
		},
		{name: "cancelled context surfaces as tool error", template: "Ambient: {{.tool.error}}", tools: tools("tool", "slow_tool"), inv: &mockInvoker{waitCtx: true}, cancelCtx: true, want: wrap("Ambient: context canceled")},
		{name: "template syntax error", template: "Unclosed {{.foo", wantNewErr: "failed to parse ambient template"},
		{name: "template execution error", template: "{{call .now}}", tools: tools("tool", "test_tool"), inv: &mockInvoker{}, wantErr: "ambient template execution error"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var inv MCPInvoker
			if tc.inv != nil {
				inv = tc.inv
			}
			p, err := NewProvider(config.AmbientContextConfig{Template: tc.template, Tools: tc.tools, TimeoutMs: 1000}, inv)
			if providerErrMatched(t, "NewProvider", err, tc.wantNewErr) {
				return
			}
			p.SetNowFunc(func() time.Time { return time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC) })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelCtx {
				cancel()
			}
			got, err := p.Retrieve(ctx)
			if providerErrMatched(t, "Retrieve", err, tc.wantErr) {
				return
			}
			if got != tc.want {
				t.Errorf("Retrieve = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestProvider_CacheLifecycle drives one provider (5s TTL, counting tool) through a
// sequence of Retrieve calls per row, advancing the fake clock before each step.
func TestProvider_CacheLifecycle(t *testing.T) {
	t.Parallel()
	type step struct {
		advanceBy time.Duration
		want      string
		wantCalls int
	}
	tests := []struct {
		name  string
		steps []step
	}{
		{"RAM cache hit within TTL", []step{{0, "Val: 1", 1}, {2 * time.Second, "Val: 1", 1}}},
		{"refresh after hard expiry", []step{{0, "Val: 1", 1}, {12 * time.Second, "Val: 2", 2}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var counter atomic.Int32
			inv := &mockInvoker{execFunc: func(context.Context, string, map[string]any) (string, error) {
				return fmt.Sprintf(`{"val": %d}`, counter.Add(1)), nil
			}}
			p, err := NewProvider(config.AmbientContextConfig{CacheTTL: "5s", Template: "Val: {{.tool.val}}", Tools: []config.AmbientToolConfig{{Name: "tool", Tool: "getter"}}}, inv)
			if err != nil {
				t.Fatalf("NewProvider: %v", err)
			}
			now := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
			p.SetNowFunc(func() time.Time { return now })
			for i, st := range tc.steps {
				now = now.Add(st.advanceBy)
				got, err := p.Retrieve(context.Background())
				if err != nil {
					t.Fatalf("step %d: Retrieve: %v", i, err)
				}
				if want := "<ambient_context>\n" + st.want + "\n</ambient_context>"; got != want {
					t.Errorf("step %d: Retrieve = %q, want %q", i, got, want)
				}
				if calls := inv.CallCount(); calls != st.wantCalls {
					t.Errorf("step %d: invoker calls = %d, want %d", i, calls, st.wantCalls)
				}
			}
		})
	}
}

func TestProvider_SingleflightDeduplication(t *testing.T) {
	t.Parallel()

	var toolInvocations int32
	inv := &mockInvoker{
		execFunc: func(ctx context.Context, name string, args map[string]any) (string, error) {
			atomic.AddInt32(&toolInvocations, 1)
			// Hold briefly via timer channel so concurrent callers overlap in singleflight
			<-time.After(30 * time.Millisecond)
			return `{"status": "ok"}`, nil
		},
	}

	cfg := config.AmbientContextConfig{
		CacheTTL:  "10s",
		TimeoutMs: 1000,
		Template:  "Status: {{.check.status}}",
		Tools: []config.AmbientToolConfig{
			{Name: "check", Tool: "health"},
		},
	}

	p, err := NewProvider(cfg, inv)
	if err != nil {
		t.Fatalf("NewProvider failed: %v", err)
	}

	const concurrentCallers = 10
	var wg sync.WaitGroup
	results := make([]string, concurrentCallers)
	errorsList := make([]error, concurrentCallers)

	for i := range concurrentCallers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errorsList[idx] = p.Retrieve(context.Background())
		}(i)
	}

	wg.Wait()

	for i, err := range errorsList {
		if err != nil {
			t.Errorf("caller %d failed with error: %v", i, err)
		}
	}

	firstRes := results[0]
	for i, res := range results {
		if res != firstRes {
			t.Errorf("caller %d got different result: %q vs %q", i, res, firstRes)
		}
	}

	// Singleflight should collapse all concurrent requests to 1 tool invocation
	if toolInvocations != 1 {
		t.Errorf("expected singleflight to collapse calls into 1 tool invocation, got %d", toolInvocations)
	}
}

func TestProvider_TemplatePathLoading(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmplFile := filepath.Join(tmpDir, "ambient.tmpl")
	content := "From file: {{.now}}"
	if err := os.WriteFile(tmplFile, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write tmpl file: %v", err)
	}

	// Successful file loading
	cfg := config.AmbientContextConfig{
		TemplatePath: tmplFile,
	}
	p, err := NewProvider(cfg, nil)
	if err != nil {
		t.Fatalf("NewProvider failed to load template from path: %v", err)
	}

	fixedTime := time.Date(2026, 10, 1, 9, 15, 0, 0, time.UTC)
	p.SetNowFunc(func() time.Time { return fixedTime })
	res, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}
	if !strings.Contains(res, "From file: 09:15:00") {
		t.Errorf("expected 'From file: 09:15:00', got: %q", res)
	}

	// Non-existent template path error
	cfgBadPath := config.AmbientContextConfig{
		TemplatePath: filepath.Join(tmpDir, "does_not_exist.tmpl"),
	}
	_, errBad := NewProvider(cfgBadPath, nil)
	if errBad == nil {
		t.Fatal("expected error for non-existent TemplatePath, got nil")
	}
}

func TestProvider_TemplateHelpers(t *testing.T) {
	t.Parallel()

	tmpl := `
DefaultStr: {{default "default_val" .missing_str}}
DefaultEmptyStr: {{default "def_empty" .empty_str}}
DefaultZeroInt: {{default 42 .tool.count}}
DefaultFalseBool: {{default true .tool.active}}
DefaultTrueBool: {{default false .tool.is_true}}
DefaultEmptyList: {{default "non_empty" .tool.empty_list}}
DefaultEmptyMap: {{default "non_empty_map" .tool.empty_map}}
DefaultValidStr: {{default "def" .tool.valid_str}}
DefaultNonZero: {{default 999 .tool.non_zero}}
TrimNil: {{trim .missing_nil}}
TrimNoVal: {{trim "<no value>"}}
TrimVal: {{trim "  hello world  "}}
LowerNil: {{lower .missing_nil}}
LowerNoVal: {{lower "<no value>"}}
LowerVal: {{lower "HELLO WORLD"}}
UpperNil: {{upper .missing_nil}}
UpperNoVal: {{upper "<no value>"}}
UpperVal: {{upper "hello world"}}
DefaultNoVal: {{default "def_noval" "<no value>"}}
JsonNil: {{json .missing_nil}}
JsonVal: {{json .tool.nested}}
RawText: {{.raw.text}}
`

	cfg := config.AmbientContextConfig{
		Template: tmpl,
		CacheTTL: "1m",
		Tools: []config.AmbientToolConfig{
			{Name: "tool", Tool: "json_tool"},
			{Name: "raw", Tool: "raw_tool"},
		},
	}

	inv := &mockInvoker{
		execFunc: func(ctx context.Context, name string, args map[string]any) (string, error) {
			if name == "json_tool" {
				return `{"nested": {"foo": "bar"}, "count": 0, "active": false, "is_true": true, "valid_str": "custom", "non_zero": 123, "empty_list": [], "empty_map": {}}`, nil
			}
			if name == "raw_tool" {
				return "plain text response", nil
			}
			return `{}`, nil
		},
	}

	p, err := NewProvider(cfg, inv)
	if err != nil {
		t.Fatalf("NewProvider failed: %v", err)
	}

	p.SetNowFunc(nil) // test resetting to time.Now
	res, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}

	if !strings.Contains(res, "DefaultStr: default_val") {
		t.Errorf("expected DefaultStr fallback, got %q", res)
	}
	if !strings.Contains(res, "DefaultZeroInt: 42") {
		t.Errorf("expected DefaultZeroInt fallback, got %q", res)
	}
	if !strings.Contains(res, "DefaultFalseBool: true") {
		t.Errorf("expected DefaultFalseBool fallback, got %q", res)
	}
	if !strings.Contains(res, "DefaultTrueBool: true") {
		t.Errorf("expected DefaultTrueBool to retain true, got %q", res)
	}
	if !strings.Contains(res, "DefaultValidStr: custom") {
		t.Errorf("expected DefaultValidStr to retain custom, got %q", res)
	}
	if !strings.Contains(res, "DefaultNonZero: 123") {
		t.Errorf("expected DefaultNonZero to retain 123, got %q", res)
	}
	if !strings.Contains(res, "DefaultNoVal: def_noval") {
		t.Errorf("expected DefaultNoVal fallback, got %q", res)
	}
	if !strings.Contains(res, "DefaultEmptyList: non_empty") {
		t.Errorf("expected DefaultEmptyList fallback, got %q", res)
	}
	if !strings.Contains(res, "DefaultEmptyMap: non_empty_map") {
		t.Errorf("expected DefaultEmptyMap fallback, got %q", res)
	}
	if !strings.Contains(res, "TrimVal: hello world") {
		t.Errorf("expected TrimVal, got %q", res)
	}
	if !strings.Contains(res, "LowerVal: hello world") {
		t.Errorf("expected LowerVal, got %q", res)
	}
	if !strings.Contains(res, "UpperVal: HELLO WORLD") {
		t.Errorf("expected UpperVal, got %q", res)
	}
	if !strings.Contains(res, `JsonVal: {"foo":"bar"}`) {
		t.Errorf("expected JsonVal, got %q", res)
	}
	if !strings.Contains(res, "RawText: plain text response") {
		t.Errorf("expected RawText, got %q", res)
	}
}

func TestProvider_Prime(t *testing.T) {
	t.Parallel()

	inv := &mockInvoker{
		execFunc: func(ctx context.Context, name string, args map[string]any) (string, error) {
			return `{"state": "primed"}`, nil
		},
	}

	cfg := config.AmbientContextConfig{
		CacheTTL: "30s",
		Template: "State: {{.tool.state}}",
		Tools: []config.AmbientToolConfig{
			{Name: "tool", Tool: "state_tool"},
		},
	}

	p, err := NewProvider(cfg, inv)
	if err != nil {
		t.Fatalf("NewProvider failed: %v", err)
	}

	// Prime upfront
	if err = p.Prime(context.Background()); err != nil {
		t.Fatalf("Prime failed: %v", err)
	}
	// Second Prime while cache is still fresh hits fast-path inside fetchAndRender
	if err = p.Prime(context.Background()); err != nil {
		t.Fatalf("second Prime failed: %v", err)
	}
	if inv.CallCount() != 1 {
		t.Fatalf("expected 1 call after Prime, got %d", inv.CallCount())
	}

	// Retrieve should hit cache immediately without extra tool invocation
	res, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}
	if !strings.Contains(res, "State: primed") {
		t.Errorf("expected 'State: primed', got %q", res)
	}
	if inv.CallCount() != 1 {
		t.Errorf("expected invoker call count to remain 1, got %d", inv.CallCount())
	}
}

func TestProvider_StaleWhileRevalidate(t *testing.T) {
	t.Parallel()

	var counter int
	var counterMu sync.Mutex
	refreshDone := make(chan struct{})

	inv := &mockInvoker{
		execFunc: func(ctx context.Context, name string, args map[string]any) (string, error) {
			counterMu.Lock()
			counter++
			c := counter
			counterMu.Unlock()
			if c == 2 {
				defer func() {
					select {
					case <-refreshDone:
					default:
						close(refreshDone)
					}
				}()
			}
			return fmt.Sprintf(`{"val": %d}`, c), nil
		},
	}

	cfg := config.AmbientContextConfig{
		CacheTTL:  "5s",
		TimeoutMs: 1000,
		Template:  "Val: {{.tool.val}}",
		Tools: []config.AmbientToolConfig{
			{Name: "tool", Tool: "val_tool"},
		},
	}

	p, err := NewProvider(cfg, inv)
	if err != nil {
		t.Fatalf("NewProvider failed: %v", err)
	}

	simulatedTime := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	var timeMu sync.Mutex
	p.SetNowFunc(func() time.Time {
		timeMu.Lock()
		defer timeMu.Unlock()
		return simulatedTime
	})

	// 1. Initial Retrieve populates cache
	res1, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("first Retrieve failed: %v", err)
	}
	if !strings.Contains(res1, "Val: 1") {
		t.Fatalf("expected 'Val: 1', got %q", res1)
	}
	if inv.CallCount() != 1 {
		t.Fatalf("expected 1 call, got %d", inv.CallCount())
	}

	// 2. Advance time past expiresAt (5s) but within hardExpiresAt (10s) -> 6s
	timeMu.Lock()
	simulatedTime = simulatedTime.Add(6 * time.Second)
	timeMu.Unlock()

	// 3. Second Retrieve should return stale cache ("Val: 1") immediately
	start := time.Now()
	res2, err := p.Retrieve(context.Background())
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("second Retrieve failed: %v", err)
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("Retrieve blocked for %v, expected non-blocking SWR return", elapsed)
	}
	if !strings.Contains(res2, "Val: 1") {
		t.Errorf("expected stale 'Val: 1', got %q", res2)
	}

	// 4. Wait for background refresh to finish
	select {
	case <-refreshDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for background refresh")
	}

	deadline := time.Now().Add(2 * time.Second)
	for p.refreshing.Load() != 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}

	if inv.CallCount() != 2 {
		t.Fatalf("expected 2 calls after background refresh, got %d", inv.CallCount())
	}

	// 5. Advance simulated clock to match when the background refresh completed
	timeMu.Lock()
	simulatedTime = simulatedTime.Add(1 * time.Second)
	timeMu.Unlock()

	// 6. Subsequent Retrieve should return updated cache ("Val: 2")
	res3, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("third Retrieve failed: %v", err)
	}
	if !strings.Contains(res3, "Val: 2") {
		t.Errorf("expected refreshed 'Val: 2', got %q", res3)
	}
}

func TestProvider_AtomicCAS_PreventsConcurrentRefresh(t *testing.T) {
	t.Parallel()

	var toolCalls atomic.Int32
	toolStarted := make(chan struct{})
	blockTool := make(chan struct{})

	inv := &mockInvoker{
		execFunc: func(ctx context.Context, name string, args map[string]any) (string, error) {
			count := toolCalls.Add(1)
			if count == 2 {
				// Signal second invocation (background refresh) started
				close(toolStarted)
				<-blockTool
			}
			return fmt.Sprintf(`{"val": %d}`, count), nil
		},
	}

	cfg := config.AmbientContextConfig{
		CacheTTL:  "5s",
		TimeoutMs: 2000,
		Template:  "Val: {{.tool.val}}",
		Tools: []config.AmbientToolConfig{
			{Name: "tool", Tool: "val_tool"},
		},
	}

	p, err := NewProvider(cfg, inv)
	if err != nil {
		t.Fatalf("NewProvider failed: %v", err)
	}

	simulatedTime := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	var timeMu sync.Mutex
	p.SetNowFunc(func() time.Time {
		timeMu.Lock()
		defer timeMu.Unlock()
		return simulatedTime
	})

	// Warm cache
	res1, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("first Retrieve failed: %v", err)
	}
	if !strings.Contains(res1, "Val: 1") {
		t.Fatalf("expected 'Val: 1', got %q", res1)
	}

	// Advance past TTL (5s) but within hard TTL (10s) -> 6s
	timeMu.Lock()
	simulatedTime = simulatedTime.Add(6 * time.Second)
	timeMu.Unlock()

	// Launch multiple concurrent Retrieve calls while in SWR window
	const concurrent = 10
	var wg sync.WaitGroup
	results := make([]string, concurrent)

	for i := range concurrent {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r, rErr := p.Retrieve(context.Background())
			if rErr != nil {
				t.Errorf("concurrent Retrieve %d failed: %v", idx, rErr)
			}
			results[idx] = r
		}(i)
	}

	// Wait for the background refresh to begin
	select {
	case <-toolStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for background refresh to start")
	}

	// All concurrent callers should immediately receive the stale cache
	wg.Wait()
	for i, r := range results {
		if !strings.Contains(r, "Val: 1") {
			t.Errorf("caller %d expected stale 'Val: 1', got %q", i, r)
		}
	}

	// Unblock the tool execution
	close(blockTool)

	// Wait for refreshing flag to clear and cache to update
	deadline := time.Now().Add(2 * time.Second)
	for p.refreshing.Load() != 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}

	// Only 2 total calls to the tool: 1 initial + 1 background refresh (no duplicates spawned)
	if total := toolCalls.Load(); total != 2 {
		t.Errorf("expected exactly 2 tool calls (1 initial + 1 bg refresh), got %d", total)
	}
}

type preservesCacheTestCase struct {
	name          string
	prePrime      bool
	advanceBy     time.Duration
	cancelContext bool
	wantSubstring string
	wantRefreshes bool
}

func runPreservesCacheCase(t *testing.T, tc preservesCacheTestCase) {
	t.Helper()
	var toolCalls atomic.Int32
	refreshDone := make(chan struct{})

	inv := &mockInvoker{
		execFunc: func(ctx context.Context, name string, args map[string]any) (string, error) {
			c := toolCalls.Add(1)
			if c == 1 && tc.prePrime {
				return fmt.Sprintf(`{"val": %d}`, c), nil
			}
			if ctx.Err() != nil {
				return "", ctx.Err()
			}
			select {
			case <-refreshDone:
			default:
				close(refreshDone)
			}
			return fmt.Sprintf(`{"val": %d}`, c), nil
		},
	}

	cfg := config.AmbientContextConfig{
		CacheTTL:  "5s",
		TimeoutMs: 1000,
		Template:  "Val: {{default .tool.error .tool.val}}",
		Tools: []config.AmbientToolConfig{
			{Name: "tool", Tool: "val_tool"},
		},
	}

	p, err := NewProvider(cfg, inv)
	if err != nil {
		t.Fatalf("NewProvider failed: %v", err)
	}

	simulatedTime := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	var timeMu sync.Mutex
	p.SetNowFunc(func() time.Time {
		timeMu.Lock()
		defer timeMu.Unlock()
		return simulatedTime
	})

	if tc.prePrime {
		resPrime, pErr := p.Retrieve(context.Background())
		if pErr != nil {
			t.Fatalf("pre-prime Retrieve failed: %v", pErr)
		}
		if !strings.Contains(resPrime, "Val: 1") {
			t.Fatalf("expected initial 'Val: 1', got %q", resPrime)
		}
	}

	if tc.advanceBy > 0 {
		timeMu.Lock()
		simulatedTime = simulatedTime.Add(tc.advanceBy)
		timeMu.Unlock()
	}

	reqCtx := context.Background()
	if tc.cancelContext {
		cCtx, cancel := context.WithCancel(context.Background())
		cancel()
		reqCtx = cCtx
	}

	res, err := p.Retrieve(reqCtx)
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}
	if !strings.Contains(res, tc.wantSubstring) {
		t.Errorf("Retrieve = %q, want substring %q", res, tc.wantSubstring)
	}

	if tc.wantRefreshes {
		select {
		case <-refreshDone:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for background refresh to complete")
		}

		deadline := time.Now().Add(2 * time.Second)
		for p.refreshing.Load() != 0 && time.Now().Before(deadline) {
			runtime.Gosched()
		}
	}
}

func TestProvider_Retrieve_PreservesCacheOnDeadlineExceeded(t *testing.T) {
	t.Parallel()

	tests := []preservesCacheTestCase{
		{
			name:          "primed cache preserved on canceled context past hard TTL",
			prePrime:      true,
			advanceBy:     15 * time.Second,
			cancelContext: true,
			wantSubstring: "Val: 1",
			wantRefreshes: true,
		},
		{
			name:          "unprimed cold cache surfaces canceled error without panic",
			prePrime:      false,
			advanceBy:     0,
			cancelContext: true,
			wantSubstring: "context canceled",
			wantRefreshes: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runPreservesCacheCase(t, tc)
		})
	}
}
