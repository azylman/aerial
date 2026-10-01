package ambient

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
)

type mockInvoker struct {
	mu       sync.Mutex
	calls    int
	execFunc func(ctx context.Context, name string, args map[string]interface{}) (string, error)
}

func (m *mockInvoker) Execute(ctx context.Context, name string, args map[string]interface{}) (string, error) {
	m.mu.Lock()
	m.calls++
	fn := m.execFunc
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, name, args)
	}
	return "{}", nil
}

func (m *mockInvoker) CallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func TestProvider_NilInvokerAndEmptyTools(t *testing.T) {
	t.Parallel()

	// 1. Nil invoker with configured tool
	cfgWithTool := config.AmbientContextConfig{
		Template: "Error: {{.ha.error}}",
		Tools: []config.AmbientToolConfig{
			{Name: "ha", Tool: "get_state"},
		},
	}
	p, err := NewProvider(cfgWithTool, nil)
	if err != nil {
		t.Fatalf("unexpected NewProvider error: %v", err)
	}
	res, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("unexpected Retrieve error: %v", err)
	}
	expected := "<ambient_context>\nError: nil mcp invoker\n</ambient_context>"
	if res != expected {
		t.Errorf("got %q, want %q", res, expected)
	}

	// 2. Empty tools list
	cfgEmptyTools := config.AmbientContextConfig{
		Template: "Time: {{.now}}",
		Tools:    []config.AmbientToolConfig{},
	}
	p2, err := NewProvider(cfgEmptyTools, nil)
	if err != nil {
		t.Fatalf("unexpected NewProvider error: %v", err)
	}
	fixedTime := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	p2.SetNowFunc(func() time.Time { return fixedTime })
	res2, err := p2.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("unexpected Retrieve error: %v", err)
	}
	expected2 := "<ambient_context>\nTime: 12:00:00\n</ambient_context>"
	if res2 != expected2 {
		t.Errorf("got %q, want %q", res2, expected2)
	}

	// 3. Empty template should return empty string
	cfgEmptyTmpl := config.AmbientContextConfig{
		Template: "",
	}
	p3, err := NewProvider(cfgEmptyTmpl, nil)
	if err != nil {
		t.Fatalf("unexpected NewProvider error: %v", err)
	}
	res3, err := p3.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("unexpected Retrieve error: %v", err)
	}
	if res3 != "" {
		t.Errorf("expected empty string for empty template, got %q", res3)
	}
}

func TestProvider_SuccessfulMultiToolExecution(t *testing.T) {
	t.Parallel()

	inv := &mockInvoker{
		execFunc: func(ctx context.Context, name string, args map[string]interface{}) (string, error) {
			switch name {
			case "music_status":
				return `{"status": "playing", "artist": "lofi girl"}`, nil
			case "weather_report":
				return "72F Sunny with clouds", nil
			default:
				return "{}", nil
			}
		},
	}

	cfg := config.AmbientContextConfig{
		Template: `Music: {{upper .music.status}} - {{.music.artist}}
Weather: {{trim .weather.text}}
JSON: {{json .music}}
Time: {{.now}}`,
		Tools: []config.AmbientToolConfig{
			{Name: "music", Tool: "music_status"},
			{Name: "weather", Tool: "weather_report"},
		},
	}

	p, err := NewProvider(cfg, inv)
	if err != nil {
		t.Fatalf("NewProvider failed: %v", err)
	}
	fixedTime := time.Date(2026, 10, 1, 15, 30, 0, 0, time.UTC)
	p.SetNowFunc(func() time.Time { return fixedTime })

	res, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}

	if !strings.HasPrefix(res, "<ambient_context>\n") || !strings.HasSuffix(res, "\n</ambient_context>") {
		t.Errorf("response not properly wrapped in ambient_context tags: %q", res)
	}
	if !strings.Contains(res, "Music: PLAYING - lofi girl") {
		t.Errorf("missing formatted music data: %q", res)
	}
	if !strings.Contains(res, "Weather: 72F Sunny with clouds") {
		t.Errorf("missing formatted weather data: %q", res)
	}
	if !strings.Contains(res, `JSON: {"artist":"lofi girl","status":"playing"}`) {
		t.Errorf("missing json helper output: %q", res)
	}
	if !strings.Contains(res, "Time: 15:30:00") {
		t.Errorf("missing time output: %q", res)
	}
}

func TestProvider_PartialFailureResilience(t *testing.T) {
	t.Parallel()

	inv := &mockInvoker{
		execFunc: func(ctx context.Context, name string, args map[string]interface{}) (string, error) {
			if name == "failing_tool" {
				return "", errors.New("connection timed out")
			}
			return `{"temperature": 68}`, nil
		},
	}

	cfg := config.AmbientContextConfig{
		Template: `Failed: {{.tool1.error}}
Temp: {{.tool2.temperature}}`,
		Tools: []config.AmbientToolConfig{
			{Name: "tool1", Tool: "failing_tool"},
			{Name: "tool2", Tool: "healthy_tool"},
		},
	}

	p, err := NewProvider(cfg, inv)
	if err != nil {
		t.Fatalf("NewProvider failed: %v", err)
	}

	res, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("expected Retrieve to succeed on partial failure, got err: %v", err)
	}

	if !strings.Contains(res, "Failed: connection timed out") {
		t.Errorf("expected error string in rendered output, got: %q", res)
	}
	if !strings.Contains(res, "Temp: 68") {
		t.Errorf("expected healthy tool data in rendered output, got: %q", res)
	}
}

func TestProvider_MissingKeysZeroString(t *testing.T) {
	t.Parallel()

	cfg := config.AmbientContextConfig{
		Template: `Missing: '{{.nonexistent}}'
Defaulted: '{{default "fallback_val" .missing_key}}'
Lower: '{{lower .not_found}}'
Upper: '{{upper .not_found}}'
Trim: '{{trim .not_found}}'`,
	}

	p, err := NewProvider(cfg, nil)
	if err != nil {
		t.Fatalf("NewProvider failed: %v", err)
	}

	res, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}

	if strings.Contains(res, "<no value>") {
		t.Errorf("rendered template contains '<no value>': %q", res)
	}
	if !strings.Contains(res, "Missing: ''") {
		t.Errorf("expected missing key to render as empty string, got: %q", res)
	}
	if !strings.Contains(res, "Defaulted: 'fallback_val'") {
		t.Errorf("expected default helper to provide fallback, got: %q", res)
	}
	if !strings.Contains(res, "Lower: ''") || !strings.Contains(res, "Upper: ''") || !strings.Contains(res, "Trim: ''") {
		t.Errorf("expected string helpers on nil to render as empty string, got: %q", res)
	}
}

func TestProvider_RAMCacheHit(t *testing.T) {
	t.Parallel()

	inv := &mockInvoker{
		execFunc: func(ctx context.Context, name string, args map[string]interface{}) (string, error) {
			return `{"count": 1}`, nil
		},
	}

	cfg := config.AmbientContextConfig{
		CacheTTL: "30s",
		Template: "Count: {{.tool.count}}",
		Tools: []config.AmbientToolConfig{
			{Name: "tool", Tool: "counter"},
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

	// First call - cache miss
	res1, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("first Retrieve failed: %v", err)
	}
	if inv.CallCount() != 1 {
		t.Fatalf("expected 1 invoker call, got %d", inv.CallCount())
	}

	// Advance time within TTL (+15s)
	timeMu.Lock()
	simulatedTime = simulatedTime.Add(15 * time.Second)
	timeMu.Unlock()

	// Second call - should hit cache
	res2, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("second Retrieve failed: %v", err)
	}
	if res2 != res1 {
		t.Errorf("cached result mismatch: %q vs %q", res2, res1)
	}
	if inv.CallCount() != 1 {
		t.Errorf("expected invoker to NOT be called on cache hit, call count: %d", inv.CallCount())
	}
}

func TestProvider_SingleflightDeduplication(t *testing.T) {
	t.Parallel()

	var toolInvocations int32
	inv := &mockInvoker{
		execFunc: func(ctx context.Context, name string, args map[string]interface{}) (string, error) {
			atomic.AddInt32(&toolInvocations, 1)
			// Small sleep so concurrent calls overlap
			time.Sleep(50 * time.Millisecond)
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

	for i := 0; i < concurrentCallers; i++ {
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

func TestProvider_ExpiredCacheRefresh(t *testing.T) {
	t.Parallel()

	var counter int
	var counterMu sync.Mutex

	inv := &mockInvoker{
		execFunc: func(ctx context.Context, name string, args map[string]interface{}) (string, error) {
			counterMu.Lock()
			counter++
			val := counter
			counterMu.Unlock()
			return `{"val": ` + string(rune('0'+val)) + `}`, nil
		},
	}

	cfg := config.AmbientContextConfig{
		CacheTTL: "5s",
		Template: "Val: {{.tool.val}}",
		Tools: []config.AmbientToolConfig{
			{Name: "tool", Tool: "getter"},
		},
	}

	p, err := NewProvider(cfg, inv)
	if err != nil {
		t.Fatalf("NewProvider failed: %v", err)
	}

	simulatedTime := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
	var timeMu sync.Mutex
	p.SetNowFunc(func() time.Time {
		timeMu.Lock()
		defer timeMu.Unlock()
		return simulatedTime
	})

	// Initial call
	res1, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("first Retrieve failed: %v", err)
	}
	if !strings.Contains(res1, "Val: 1") {
		t.Errorf("expected 'Val: 1', got: %q", res1)
	}

	// Advance past TTL (6s > 5s)
	timeMu.Lock()
	simulatedTime = simulatedTime.Add(6 * time.Second)
	timeMu.Unlock()

	// Call after expiration
	res2, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("second Retrieve failed: %v", err)
	}
	if !strings.Contains(res2, "Val: 2") {
		t.Errorf("expected refreshed 'Val: 2', got: %q", res2)
	}
	if inv.CallCount() != 2 {
		t.Errorf("expected 2 invoker calls after cache expiration, got %d", inv.CallCount())
	}
}

func TestProvider_TemplateSyntaxError(t *testing.T) {
	t.Parallel()

	cfg := config.AmbientContextConfig{
		Template: "Unclosed {{.foo",
	}
	_, err := NewProvider(cfg, nil)
	if err == nil {
		t.Fatal("expected error on template syntax error, got nil")
	}
}

func TestProvider_TemplatePathLoading(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tmplFile := filepath.Join(tmpDir, "ambient.tmpl")
	content := "From file: {{.now}}"
	if err := os.WriteFile(tmplFile, []byte(content), 0644); err != nil {
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
