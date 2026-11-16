package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/pubsub"
)

type mockEriClient struct {
	dispatchFn func(ctx context.Context, req BrainDispatchRequest) error
	cancelFn   func(ctx context.Context, chunkID string) error
	promptFn   func(ctx context.Context, threadID, prompt string) error
	widgetFn   func(ctx context.Context, req StatusWidgetRequest) (string, error)
	messageFn  func(ctx context.Context, channelID, content string) error

	lastDispatch *BrainDispatchRequest
	lastCancel   string
	lastThreadID string
	lastPrompt   string
	lastWidget   *StatusWidgetRequest
	lastMsgChan  string
	lastMsgCont  string
}

func (m *mockEriClient) Dispatch(ctx context.Context, req BrainDispatchRequest) error {
	m.lastDispatch = &req
	if m.dispatchFn != nil {
		return m.dispatchFn(ctx, req)
	}
	return nil
}

func (m *mockEriClient) CancelDispatch(ctx context.Context, chunkID string) error {
	m.lastCancel = chunkID
	if m.cancelFn != nil {
		return m.cancelFn(ctx, chunkID)
	}
	return nil
}

func (m *mockEriClient) Prompt(ctx context.Context, threadID, prompt string) error {
	m.lastThreadID = threadID
	m.lastPrompt = prompt
	if m.promptFn != nil {
		return m.promptFn(ctx, threadID, prompt)
	}
	return nil
}

func (m *mockEriClient) Widget(ctx context.Context, req StatusWidgetRequest) (string, error) {
	m.lastWidget = &req
	if m.widgetFn != nil {
		return m.widgetFn(ctx, req)
	}
	return "msg-123", nil
}

func (m *mockEriClient) Message(ctx context.Context, channelID, content string) error {
	m.lastMsgChan = channelID
	m.lastMsgCont = content
	if m.messageFn != nil {
		return m.messageFn(ctx, channelID, content)
	}
	return nil
}

func TestHTTPEriClient(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dispatch":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/dispatch/cancel":
			w.WriteHeader(http.StatusOK)
		case "/prompt":
			w.WriteHeader(http.StatusOK)
		case "/discord/widget":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"message_id":"widget-msg-999"}`))
		case "/discord/message":
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	client := NewHTTPEriClient(srv.URL, srv.Client())

	ctx := context.Background()
	if err := client.Dispatch(ctx, BrainDispatchRequest{ID: "d1"}); err != nil {
		t.Fatalf("Dispatch err = %v", err)
	}
	if err := client.CancelDispatch(ctx, "c1"); err != nil {
		t.Fatalf("CancelDispatch err = %v", err)
	}
	if err := client.Prompt(ctx, "th1", "wake up"); err != nil {
		t.Fatalf("Prompt err = %v", err)
	}
	msgID, err := client.Widget(ctx, StatusWidgetRequest{ID: "b1"})
	if err != nil || msgID != "widget-msg-999" {
		t.Fatalf("Widget msgID = %q err = %v", msgID, err)
	}
	if err := client.Message(ctx, "ch1", "hello"); err != nil {
		t.Fatalf("Message err = %v", err)
	}

	// Test errors
	badClient := NewHTTPEriClient("http://invalid.localhost:99", srv.Client())
	if err := badClient.Dispatch(ctx, BrainDispatchRequest{}); err == nil {
		t.Error("expected error on bad client dispatch, got nil")
	}
}

func TestExecutor_PromptAndDispatch(t *testing.T) {
	t.Parallel()
	eri := &mockEriClient{}
	bus := pubsub.NewMemoryBus()
	exec := NewExecutor(t.TempDir(), eri, bus, "test-token", nil)

	b := VanadisBuild{ID: "b1", Title: "Build 1", Objective: "Make everything awesome", ThreadID: "th-1"}
	chunks := []VanadisChunk{
		{ID: "c1", Title: "Chunk 1", Repo: "aerial", Instructions: "Do step 1", Effort: "high"},
		{ID: "c2", Title: "Chunk 2", Repo: "aerial", Instructions: "Do step 2", Mode: chunkModeAction},
	}

	prompt1, err := exec.FormatChunkPrompt(b, chunks, 0)
	if err != nil || !strings.Contains(prompt1, "Chunk 1") {
		t.Fatalf("FormatChunkPrompt 0 failed: %v, prompt: %s", err, prompt1)
	}

	prompt2, err := exec.FormatChunkPrompt(b, chunks, 1)
	if err != nil || !strings.Contains(prompt2, "Chunk 2") {
		t.Fatalf("FormatChunkPrompt 1 failed: %v, prompt: %s", err, prompt2)
	}

	// Test chunk out of bounds
	if _, err := exec.FormatChunkPrompt(b, chunks, 9); err == nil {
		t.Error("expected error on out of bounds chunk index, got nil")
	}

	store, _ := dbScript{}.open()
	if err := exec.ExecuteLaunchChunk(context.Background(), store, b, chunks, 0, "[NOTICE]\n"); err != nil {
		t.Fatalf("ExecuteLaunchChunk failed: %v", err)
	}
	if eri.lastDispatch == nil || eri.lastDispatch.VanadisChunkID != "c1" {
		t.Errorf("lastDispatch chunkID = %+v, want c1", eri.lastDispatch)
	}

	if err := exec.CancelChunkDispatch(context.Background(), "c1"); err != nil || eri.lastCancel != "c1" {
		t.Errorf("CancelChunkDispatch failed: %v", err)
	}
	if err := exec.DispatchWakePrompt(context.Background(), "th-1", "wake"); err != nil || eri.lastPrompt != "wake" {
		t.Errorf("DispatchWakePrompt failed: %v", err)
	}
	if err := exec.DispatchDirectMessage(context.Background(), "ch-1", "msg"); err != nil || eri.lastMsgCont != "msg" {
		t.Errorf("DispatchDirectMessage failed: %v", err)
	}
}

func TestExecutor_SyncBuildWidget(t *testing.T) {
	t.Parallel()
	eri := &mockEriClient{
		widgetFn: func(ctx context.Context, req StatusWidgetRequest) (string, error) {
			return "widget-msg-456", nil
		},
	}
	exec := NewExecutor("", eri, nil, "", nil)
	store, _ := dbScript{execTag: "UPDATE 1"}.open()

	b := VanadisBuild{ID: "b1", Title: "Build 1", Status: "running", ThreadID: "th-1"}
	chunks := []VanadisChunk{{ID: "c1", Title: "Chunk 1", Status: "running"}}

	msgID, err := exec.SyncBuildWidget(context.Background(), store, b, chunks, "Notice!")
	if err != nil || msgID != "widget-msg-456" {
		t.Fatalf("SyncBuildWidget msgID = %q err = %v", msgID, err)
	}
	if eri.lastWidget == nil || eri.lastWidget.Banner != "Notice!" {
		t.Errorf("expected banner 'Notice!', got %v", eri.lastWidget)
	}

	// nil executor or eri client
	if id, err := (*Executor)(nil).SyncBuildWidget(context.Background(), nil, b, chunks, ""); id != "" || err != nil {
		t.Error("expected empty string and nil error")
	}

	// eri client error
	eriErr := &mockEriClient{
		widgetFn: func(ctx context.Context, req StatusWidgetRequest) (string, error) {
			return "", errors.New("eri widget failure")
		},
	}
	execErr := NewExecutor("", eriErr, nil, "", nil)
	if _, err := execErr.SyncBuildWidget(context.Background(), store, b, chunks, ""); err == nil {
		t.Error("expected error on eri failure")
	}

	// update message id error branch
	storeUpdateErr, _ := dbScript{failExec: "UPDATE vanadis_builds", execErr: errors.New("update err")}.open()
	_, _ = exec.SyncBuildWidget(context.Background(), storeUpdateErr, b, chunks, "")

	// truncatePromptRunes coverage
	// normalizeSymbolName
	if sym := normalizeSymbolName("pkg.FooBar()"); sym != "FooBar" {
		t.Errorf("expected FooBar, got %q", sym)
	}
	if sym := normalizeSymbolName("  MyFunc  "); sym != "MyFunc" {
		t.Errorf("expected MyFunc, got %q", sym)
	}

	shortStr := "hello"
	if got := truncatePromptRunes(shortStr, 10); got != shortStr {
		t.Errorf("expected %q, got %q", shortStr, got)
	}
	longStr := "hello world from vanadis"
	if got := truncatePromptRunes(longStr, 5); !strings.HasSuffix(got, "... (truncated)") {
		t.Errorf("expected truncation, got %q", got)
	}
}

func TestExecutor_PublishExternalEvent(t *testing.T) {
	t.Parallel()
	bus := pubsub.NewMemoryBus()
	exec := NewExecutor("", nil, bus, "", nil)

	ctx := context.Background()
	if err := exec.PublishExternalEvent(ctx, "vanadis_build", "completed", []string{"b1"}, "all green"); err != nil {
		t.Fatalf("PublishExternalEvent failed: %v", err)
	}
}

func TestSymbolResolution(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	repoDir := filepath.Join(tmpDir, "aerial")
	if err := os.MkdirAll(repoDir, 0o755); err != nil {
		t.Fatal(err)
	}

	code := `package test

func DoAwesomeThing() {
	println("awesome")
}

type AwesomeStruct struct {
	Field int
}
`
	if err := os.WriteFile(filepath.Join(repoDir, "awesome.go"), []byte(code), 0o644); err != nil {
		t.Fatal(err)
	}

	spans, ok := ResolveSymbolSpans(tmpDir, "aerial", []string{"awesome.go"}, []string{"DoAwesomeThing", "AwesomeStruct"})
	if !ok || len(spans) == 0 {
		t.Fatalf("ResolveSymbolSpans failed, spans = %v, ok = %v", spans, ok)
	}
	if !strings.Contains(spans[0], "DoAwesomeThing") || !strings.Contains(spans[0], "AwesomeStruct") {
		t.Errorf("spans missing symbols: %v", spans)
	}
}

func TestCIFailureLogExtraction(t *testing.T) {
	t.Parallel()
	rawLog := `2026-11-15T23:00:00.000Z ##[group]Run tests
2026-11-15T23:00:01.000Z === RUN   TestSomething
2026-11-15T23:00:02.000Z --- FAIL: TestSomething (0.01s)
2026-11-15T23:00:02.000Z     main_test.go:42: something failed
2026-11-15T23:00:03.000Z FAIL
2026-11-15T23:00:04.000Z ##[endgroup]
2026-11-15T23:00:05.000Z Post job cleanup.
`
	extracted := ExtractCIFailureDetails(rawLog)
	if !strings.Contains(extracted, "FAIL: TestSomething") {
		t.Errorf("ExtractCIFailureDetails missing failure: %s", extracted)
	}
	if strings.Contains(extracted, "##[group]") || strings.Contains(extracted, "Post job cleanup.") {
		t.Errorf("ExtractCIFailureDetails contains plumbing lines: %s", extracted)
	}

	ev := pubsub.PRLifecycleEvent{
		Repo:      "azylman/aerial",
		PRNumber:  123,
		CheckName: "test-unit",
		Branch:    "feat/test",
		HeadSHA:   "abcdef1",
	}
	prompt := FormatCIFailurePrompt(ev, "extracted failure details")
	if !strings.Contains(prompt, "PR #123") || !strings.Contains(prompt, "extracted failure details") {
		t.Errorf("FormatCIFailurePrompt missing details: %s", prompt)
	}

	conflictPrompt := FormatConflictPrompt(ev)
	if !strings.Contains(conflictPrompt, "Merge conflict") || !strings.Contains(conflictPrompt, "PR #123") {
		t.Errorf("FormatConflictPrompt missing details: %s", conflictPrompt)
	}
}

func TestWidgetFormatting(t *testing.T) {
	t.Parallel()
	c := VanadisChunk{
		ID:     "c1",
		Status: "open",
		CIChecks: map[string]string{
			"lint": "passed",
			"test": "failed",
		},
	}
	step := FormatChunkActiveStep(c)
	if !strings.Contains(step, "1/2 passed") {
		t.Errorf("FormatChunkActiveStep = %q, want '1/2 passed'", step)
	}

	cDeploying := VanadisChunk{ID: "c2", Status: "deploying"}
	stepDeploy := FormatChunkActiveStep(cDeploying)
	if stepDeploy != "Deploying to cluster" {
		t.Errorf("FormatChunkActiveStep deploying = %q, want 'Deploying to cluster'", stepDeploy)
	}

	cEmpty := VanadisChunk{ID: "c3", Status: "pending"}
	if stepEmpty := FormatChunkActiveStep(cEmpty); stepEmpty != "" {
		t.Errorf("FormatChunkActiveStep pending = %q, want empty", stepEmpty)
	}
}

func TestNormalizeWidgetStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{input: "coding", want: "running"},
		{input: "running", want: "running"},
		{input: "pr_opened", want: "open"},
		{input: "open", want: "open"},
		{input: "completed", want: "completed"},
		{input: "deployed", want: "completed"},
		{input: "merged", want: "merged"},
		{input: "failed", want: "failed"},
		{input: "crashed", want: "failed"},
		{input: "timeout", want: "failed"},
		{input: "cancelled", want: "cancelled"},
		{input: "custom_status", want: "custom_status"},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			got := normalizeWidgetStatus(tc.input)
			if got != tc.want {
				t.Errorf("normalizeWidgetStatus(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestExecutor_ResolveCIFailureDetails(t *testing.T) {
	t.Parallel()
	ghServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/issues/10/comments"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"id":1,"body":"some regular comment"},{"id":2,"body":"## roborev:\n` + "`abc1234`" + `\nFound potential issue on line 10"}]`))
		case strings.Contains(r.URL.Path, "/actions/jobs/555/logs"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("=== RUN TestA\n--- FAIL: TestA (0.01s)\nFAIL\n"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(ghServer.Close)

	exec := NewExecutor("", nil, nil, "gh-token", ghServer.Client())
	exec.gitHubAPIURL = ghServer.URL

	tests := []struct {
		name      string
		repo      string
		pr        int
		check     string
		sha       string
		url       string
		details   string
		wantMatch string
	}{
		{
			name:      "roborev comment matched",
			repo:      "aerial",
			pr:        10,
			check:     "roborev",
			sha:       "abc123456789",
			details:   "failure",
			wantMatch: "roborev:",
		},
		{
			name:      "job logs extracted",
			repo:      "aerial",
			pr:        10,
			check:     "unit-tests",
			url:       "https://github.com/azylman/aerial/actions/runs/123/job/555",
			details:   "failure",
			wantMatch: "FAIL: TestA",
		},
		{
			name:      "fallback details when check url has no job id",
			repo:      "aerial",
			pr:        10,
			check:     "unit-tests",
			url:       "https://github.com/azylman/aerial/actions",
			details:   "timed_out",
			wantMatch: "timed_out",
		},
		{
			name:      "custom details preserved",
			repo:      "aerial",
			pr:        10,
			check:     "unit-tests",
			details:   "already specific error details",
			wantMatch: "already specific error details",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := exec.ResolveCIFailureDetails(context.Background(), tc.repo, tc.pr, tc.check, tc.sha, tc.url, tc.details)
			if !strings.Contains(got, tc.wantMatch) {
				t.Errorf("ResolveCIFailureDetails = %q, want containing %q", got, tc.wantMatch)
			}
		})
	}
}

func TestFormatChunkPromptWithShareRoot(t *testing.T) {
	t.Parallel()
	b := VanadisBuild{
		ID:            "b-1",
		Title:         "Build Title",
		Objective:     "Build Objective",
		SharedContext: "Shared Context",
		Tier:          2,
	}
	chunks := []VanadisChunk{
		{
			ID:           "c-1",
			Title:        "Chunk 1",
			Mode:         "pr",
			Repo:         "aerial",
			Instructions: "Do chunk 1",
			TargetFiles:  []string{"services/vanadis-restate/main.go"},
		},
		{
			ID:           "c-2",
			Title:        "Action Chunk 2",
			Mode:         "action",
			Instructions: "Verify deployment",
		},
	}

	tests := []struct {
		name      string
		chunks    []VanadisChunk
		activeIdx int
		wantErr   bool
		wantPart  string
	}{
		{name: "empty chunks error", chunks: nil, activeIdx: 0, wantErr: true},
		{name: "negative index error", chunks: chunks, activeIdx: -1, wantErr: true},
		{name: "out of range index error", chunks: chunks, activeIdx: 5, wantErr: true},
		{name: "first chunk prompt", chunks: chunks, activeIdx: 0, wantPart: "=== Active Chunk (Chunk 1 of 2) ==="},
		{name: "action chunk prompt", chunks: chunks, activeIdx: 1, wantPart: "=== Active Chunk (Chunk 2 of 2) ==="},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			prompt, err := formatChunkPromptWithShareRoot(b, tc.chunks, tc.activeIdx, "/tmp")
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !strings.Contains(prompt, tc.wantPart) {
				t.Errorf("prompt missing %q:\n%s", tc.wantPart, prompt)
			}
		})
	}
}

func TestIsHighSignalLine(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		line string
		want bool
	}{
		{name: "fail test line", line: "--- FAIL: TestFoo (0.01s)", want: true},
		{name: "fail tab", line: "FAIL\tgithub.com/azylman/aerial", want: true},
		{name: "panic line", line: "panic: runtime error", want: true},
		{name: "error line", line: "Error: something failed", want: true},
		{name: "gha error", line: "##[error] Process failed", want: true},
		{name: "alert emoji", line: "🚨 test failure", want: true},
		{name: "go coordinate", line: "pkg/test.go:42:15: syntax error", want: true},
		{name: "regular log", line: "just some regular output line", want: false},
		{name: "empty line", line: "", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isHighSignalLine(tc.line)
			if got != tc.want {
				t.Errorf("isHighSignalLine(%q) = %v, want %v", tc.line, got, tc.want)
			}
		})
	}
}

func TestIsBoundedIdent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		line  string
		start int
		end   int
		want  bool
	}{
		{name: "middle bounded", line: " foo ", start: 1, end: 4, want: true},
		{name: "prefixed", line: "afoo ", start: 1, end: 5, want: false},
		{name: "suffixed", line: " fooa", start: 1, end: 4, want: false},
		{name: "start of line", line: "foo ", start: 0, end: 3, want: true},
		{name: "end of line", line: " foo", start: 1, end: 4, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isBoundedIdent(tc.line, tc.start, tc.end)
			if got != tc.want {
				t.Errorf("isBoundedIdent(%q, %d, %d) = %v, want %v", tc.line, tc.start, tc.end, got, tc.want)
			}
		})
	}
}

func TestExecutor_NilEriAndDirectHelpers(t *testing.T) {
	t.Parallel()
	execNil := NewExecutor("", nil, nil, "", nil)
	ctx := context.Background()
	if err := execNil.CancelChunkDispatch(ctx, "c1"); err != nil {
		t.Fatal(err)
	}
	if err := execNil.DispatchWakePrompt(ctx, "th1", "prompt"); err != nil {
		t.Fatal(err)
	}
	if err := execNil.DispatchDirectMessage(ctx, "ch1", "msg"); err != nil {
		t.Fatal(err)
	}

	eri := &mockEriClient{}
	exec := NewExecutor("", eri, nil, "", nil)
	if err := exec.CancelChunkDispatch(ctx, "c1"); err != nil {
		t.Fatal(err)
	}
	if eri.lastCancel != "c1" {
		t.Errorf("lastCancel = %q, want c1", eri.lastCancel)
	}
	if err := exec.DispatchWakePrompt(ctx, "th1", "prompt"); err != nil {
		t.Fatal(err)
	}
	if eri.lastPrompt != "prompt" {
		t.Errorf("lastPrompt = %q, want prompt", eri.lastPrompt)
	}
	if err := exec.DispatchDirectMessage(ctx, "ch1", "msg"); err != nil {
		t.Fatal(err)
	}
	if eri.lastMsgCont != "msg" {
		t.Errorf("lastMsgCont = %q, want msg", eri.lastMsgCont)
	}
}

func TestContainsIdent(t *testing.T) {
	t.Parallel()
	if containsIdent("any", "") {
		t.Error("expected false for empty ident")
	}
	if !containsIdent("xfoo foo", "foo") {
		t.Error("expected true when second occurrence matches")
	}
}

func TestRepoCheckoutDir_Cases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		repo     string
		wantPath string
		wantOK   bool
	}{
		{repo: "", wantOK: false},
		{repo: ".", wantOK: false},
		{repo: "..", wantOK: false},
		{repo: "aerial", wantPath: "/share/aerial", wantOK: true},
		{repo: "azylman/aerial", wantPath: "/share/aerial", wantOK: true},
	}
	for _, tc := range tests {
		t.Run(tc.repo, func(t *testing.T) {
			t.Parallel()
			path, ok := repoCheckoutDir("/share", tc.repo)
			if ok != tc.wantOK {
				t.Fatalf("repoCheckoutDir(%q) ok = %v, want %v", tc.repo, ok, tc.wantOK)
			}
			if ok && path != tc.wantPath {
				t.Errorf("repoCheckoutDir(%q) path = %q, want %q", tc.repo, path, tc.wantPath)
			}
		})
	}
}

func TestStripLineComment(t *testing.T) {
	t.Parallel()
	tests := []struct {
		line string
		want string
	}{
		{line: "func Foo() { // comment", want: "func Foo() { "},
		{line: "func Foo() {}", want: "func Foo() {}"},
	}
	for _, tc := range tests {
		t.Run(tc.line, func(t *testing.T) {
			t.Parallel()
			got := stripLineComment(tc.line)
			if got != tc.want {
				t.Errorf("stripLineComment(%q) = %q, want %q", tc.line, got, tc.want)
			}
		})
	}
}

func TestHTTPEriClient_Extended(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Nil receiver
	var nilClient *HTTPEriClient
	if err := nilClient.Dispatch(ctx, BrainDispatchRequest{}); err == nil {
		t.Error("expected error from nil HTTPEriClient")
	}

	// Empty URL
	emptyURLClient := NewHTTPEriClient("", nil)
	if err := emptyURLClient.Dispatch(ctx, BrainDispatchRequest{}); err == nil {
		t.Error("expected error from empty URL HTTPEriClient")
	}

	// Empty arguments are no-ops
	if err := emptyURLClient.CancelDispatch(ctx, ""); err != nil {
		t.Errorf("unexpected error on empty CancelDispatch: %v", err)
	}
	if err := emptyURLClient.Prompt(ctx, "", "prompt"); err != nil {
		t.Errorf("unexpected error on empty Prompt: %v", err)
	}
	if err := emptyURLClient.Prompt(ctx, "th", ""); err != nil {
		t.Errorf("unexpected error on empty Prompt: %v", err)
	}
	if err := emptyURLClient.Message(ctx, "", "content"); err != nil {
		t.Errorf("unexpected error on empty Message: %v", err)
	}
	if err := emptyURLClient.Message(ctx, "ch", ""); err != nil {
		t.Errorf("unexpected error on empty Message: %v", err)
	}

	// Server returning 500
	srv500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer srv500.Close()

	c500 := NewHTTPEriClient(srv500.URL, srv500.Client())
	if err := c500.Dispatch(ctx, BrainDispatchRequest{}); err == nil {
		t.Error("expected error from 500 status")
	}
	if _, err := c500.Widget(ctx, StatusWidgetRequest{}); err == nil {
		t.Error("expected error from 500 status on Widget")
	}

	// Marshal error
	if _, err := c500.postEriJSON(ctx, "/path", make(chan int), 1024); err == nil {
		t.Error("expected marshal error")
	}

	// Bad URL request error
	badURLClient := NewHTTPEriClient("http://[::1]:namedport", srv500.Client())
	if _, err := badURLClient.postEriJSON(ctx, "/path", "test", 1024); err == nil {
		t.Error("expected create request error")
	}
}

func TestFindSymbolSpan_Cases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		content   string
		symbol    string
		wantStart int
		wantEnd   int
		wantFound bool
	}{
		{name: "empty symbol", content: "func Foo() {}", symbol: "", wantFound: false},
		{name: "empty content", content: "", symbol: "Foo", wantFound: false},
		{name: "not found", content: "func Foo() {}", symbol: "Bar", wantFound: false},
		{name: "decl prefix with closing brace", content: "func Foo() {\n\treturn\n}", symbol: "Foo", wantStart: 1, wantEnd: 3, wantFound: true},
		{name: "decl prefix without closing brace", content: "type Foo struct {\n\tX int", symbol: "Foo", wantStart: 1, wantEnd: 1, wantFound: true},
		{name: "fallback non-decl line", content: "\n\tFoo := 123", symbol: "Foo", wantStart: 2, wantEnd: 2, wantFound: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			start, end, found := FindSymbolSpan(tc.content, tc.symbol)
			if found != tc.wantFound || start != tc.wantStart || end != tc.wantEnd {
				t.Errorf("FindSymbolSpan(%q, %q) = (%d, %d, %v), want (%d, %d, %v)", tc.content, tc.symbol, start, end, found, tc.wantStart, tc.wantEnd, tc.wantFound)
			}
		})
	}
}

func TestResolveSymbolSpans_Extended(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	repoDir := filepath.Join(tmpDir, "aerial")
	if err := os.MkdirAll(repoDir, 0o755); err != nil {
		t.Fatal(err)
	}
	testFile := filepath.Join(repoDir, "test.go")
	if err := os.WriteFile(testFile, []byte("package main\n\nfunc MyTargetFunc() {\n}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name          string
		shareRoot     string
		repo          string
		files         []string
		symbols       []string
		wantResolved  bool
		wantSpanCount int
	}{
		{
			name:         "invalid repo path",
			shareRoot:    tmpDir,
			repo:         ".",
			files:        []string{"test.go"},
			symbols:      []string{"MyTargetFunc"},
			wantResolved: false,
		},
		{
			name:         "nonexistent file",
			shareRoot:    tmpDir,
			repo:         "aerial",
			files:        []string{"nonexistent.go"},
			symbols:      []string{"MyTargetFunc"},
			wantResolved: false,
		},
		{
			name:         "file exists symbol missing",
			shareRoot:    tmpDir,
			repo:         "aerial",
			files:        []string{"test.go"},
			symbols:      []string{"MissingSymbol"},
			wantResolved: false,
		},
		{
			name:          "success resolving symbol",
			shareRoot:     tmpDir,
			repo:          "aerial",
			files:         []string{"test.go"},
			symbols:       []string{"MyTargetFunc"},
			wantResolved:  true,
			wantSpanCount: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spans, ok := ResolveSymbolSpans(tc.shareRoot, tc.repo, tc.files, tc.symbols)
			if ok != tc.wantResolved {
				t.Fatalf("ResolveSymbolSpans ok = %v, want %v", ok, tc.wantResolved)
			}
			if len(spans) != tc.wantSpanCount {
				t.Errorf("spans count = %d, want %d", len(spans), tc.wantSpanCount)
			}
		})
	}
}

func TestResolveCIFailureDetails_RoboRevAndLogsBranches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Server returning RoboRev comment with non-matching SHA and matching SHA
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/comments"):
			w.WriteHeader(http.StatusOK)
			comments := []gitHubIssueComment{
				{ID: 1, Body: "## roborev: findings for `other12`\nSome finding"},
				{ID: 2, Body: "## roborev: findings for `abcdef1`\nVerified finding"},
			}
			data, _ := json.Marshal(comments)
			_, _ = w.Write(data)
		case strings.Contains(r.URL.Path, "/logs"):
			w.WriteHeader(http.StatusOK)
			largeLog := strings.Repeat("2026-11-16T00:00:00Z line of log\n", 1000) +
				"2026-11-16T00:00:00Z FAIL: TestX\nPost job cleanup.\nmore logs\n"
			_, _ = w.Write([]byte(largeLog))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	exec := NewExecutor("", nil, nil, "token", srv.Client())
	exec.gitHubAPIURL = srv.URL

	// 1. RoboRev match
	details := exec.ResolveCIFailureDetails(ctx, "aerial", 101, "roborev", "abcdef123456", "", "failure")
	if !strings.Contains(details, "Verified finding") {
		t.Errorf("expected Verified finding, got %q", details)
	}

	// 2. Fetch logs with actions URL
	actionsURL := "https://github.com/azylman/aerial/actions/runs/123/job/456789"
	logDetails := exec.ResolveCIFailureDetails(ctx, "aerial", 0, "test", "abcdef1", actionsURL, "failure")
	if !strings.Contains(logDetails, "FAIL: TestX") {
		t.Errorf("expected FAIL: TestX in logs, got %q", logDetails)
	}

	// 3. FormatCIFailurePrompt with RoboRev
	prEv := pubsub.PRLifecycleEvent{
		Repo:      "aerial",
		PRNumber:  101,
		CheckName: "roborev",
		HeadSHA:   "abcdef1",
		Branch:    "feat/test",
		CheckURL:  actionsURL,
	}
	prompt := FormatCIFailurePrompt(prEv, "details")
	if !strings.Contains(prompt, "critically evaluate each RoboRev finding") {
		t.Errorf("expected RoboRev directive, got %q", prompt)
	}

	// 4. FormatConflictPrompt
	conflictPrompt := FormatConflictPrompt(prEv)
	if !strings.Contains(conflictPrompt, "Merge conflict detected on PR #101") {
		t.Errorf("expected conflict prompt, got %q", conflictPrompt)
	}
}

func TestFetchCheckJobFailureLogsAndRoboRevCases(t *testing.T) {
	t.Parallel()
	ghServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/actions/jobs/999/logs"):
			w.WriteHeader(http.StatusInternalServerError)
		case strings.Contains(r.URL.Path, "/actions/jobs/888/logs"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(""))
		case strings.Contains(r.URL.Path, "/issues/99/comments"):
			w.WriteHeader(http.StatusInternalServerError)
		case strings.Contains(r.URL.Path, "/issues/88/comments"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"id":3,"body":"no roborev here"}]`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ghServer.Close()

	exec := NewExecutor("", nil, nil, "gh-token", ghServer.Client())
	exec.gitHubAPIURL = ghServer.URL

	ctx := context.Background()

	// fetchCheckJobFailureLogs nil or invalid
	if res := (*Executor)(nil).fetchCheckJobFailureLogs(ctx, "aerial", 123); res != "" {
		t.Errorf("expected empty string, got %q", res)
	}
	if res := exec.fetchCheckJobFailureLogs(ctx, "", 123); res != "" {
		t.Errorf("expected empty string for empty repo, got %q", res)
	}
	if res := exec.fetchCheckJobFailureLogs(ctx, "aerial", 0); res != "" {
		t.Errorf("expected empty string for zero jobID, got %q", res)
	}
	if res := exec.fetchCheckJobFailureLogs(ctx, "aerial", 999); res != "" {
		t.Errorf("expected empty string for 500 status, got %q", res)
	}
	if res := exec.fetchCheckJobFailureLogs(ctx, "aerial", 888); res != "" {
		t.Errorf("expected empty for logs without high signal, got %q", res)
	}

	// fetchLatestRoboRevComment nil or invalid
	if res := (*Executor)(nil).fetchLatestRoboRevComment(ctx, "aerial", 10, "sha"); res != "" {
		t.Errorf("expected empty string, got %q", res)
	}
	if res := exec.fetchLatestRoboRevComment(ctx, "", 10, "sha"); res != "" {
		t.Errorf("expected empty string for empty repo, got %q", res)
	}
	if res := exec.fetchLatestRoboRevComment(ctx, "aerial", 0, "sha"); res != "" {
		t.Errorf("expected empty string for zero pr, got %q", res)
	}
	if res := exec.fetchLatestRoboRevComment(ctx, "aerial", 99, "sha"); res != "" {
		t.Errorf("expected empty string for 500 status, got %q", res)
	}
	if res := exec.fetchLatestRoboRevComment(ctx, "aerial", 88, "sha"); res != "" {
		t.Errorf("expected empty string when no roborev comment found, got %q", res)
	}

	// Huge logs > 256KB
	ghServerHuge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		prefix := strings.Repeat("log line\n", 30000)
		_, _ = w.Write([]byte(prefix + "\n--- FAIL: TestHuge (0.01s)\n"))
	}))
	defer ghServerHuge.Close()
	execHuge := NewExecutor("", nil, nil, "token", ghServerHuge.Client())
	execHuge.gitHubAPIURL = ghServerHuge.URL
	if res := execHuge.fetchCheckJobFailureLogs(ctx, "aerial", 111); !strings.Contains(res, "FAIL: TestHuge") {
		t.Errorf("expected FAIL: TestHuge, got %q", res)
	}

	// RoboRev non-matching SHA
	ghServerRoboMismatch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		comments := []gitHubIssueComment{
			{ID: 1, Body: "## roborev: findings for `other12345`\nFinding"},
		}
		data, _ := json.Marshal(comments)
		_, _ = w.Write(data)
	}))
	defer ghServerRoboMismatch.Close()
	execRoboMismatch := NewExecutor("", nil, nil, "token", ghServerRoboMismatch.Client())
	execRoboMismatch.gitHubAPIURL = ghServerRoboMismatch.URL
	if res := execRoboMismatch.fetchLatestRoboRevComment(ctx, "aerial", 10, "abcdef1"); res != "" {
		t.Errorf("expected empty string for non-matching SHA, got %q", res)
	}
}

func TestExecuteLaunchChunk_Errors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// nil executor or eri client
	if err := (*Executor)(nil).ExecuteLaunchChunk(ctx, nil, VanadisBuild{}, nil, 0, ""); err == nil {
		t.Error("expected error for nil executor")
	}

	exec := NewExecutor("", &mockEriClient{}, nil, "", nil)

	// out of bounds
	if err := exec.ExecuteLaunchChunk(ctx, nil, VanadisBuild{}, nil, 0, ""); err == nil {
		t.Error("expected error for out of bounds chunk")
	}

	// activeIdx out of bounds
	chunks := []VanadisChunk{{ID: "c1"}}
	if err := exec.ExecuteLaunchChunk(ctx, nil, VanadisBuild{ID: "b1"}, chunks, -1, ""); err == nil {
		t.Error("expected error for negative activeIdx")
	}
	if err := exec.ExecuteLaunchChunk(ctx, nil, VanadisBuild{ID: "b1"}, chunks, 2, ""); err == nil {
		t.Error("expected error for out of range activeIdx")
	}

	// store reserve error branch
	mockStore := &mockStoreForLaunch{reserveErr: errors.New("database locked")}
	execWithStore := NewExecutor("", &mockEriClient{}, nil, "", nil)
	chunksForLaunch := []VanadisChunk{{ID: "c1", Title: "Chunk 1", Mode: "pr", Repo: "aerial"}}
	if err := execWithStore.ExecuteLaunchChunk(ctx, mockStore, VanadisBuild{ID: "b1"}, chunksForLaunch, 0, ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// TargetSymbols span rendering in FormatChunkPrompt
	tmpShare := t.TempDir()
	aerialDir := filepath.Join(tmpShare, "aerial")
	_ = os.MkdirAll(aerialDir, 0o755)
	_ = os.WriteFile(filepath.Join(aerialDir, "main.go"), []byte("package main\n\nfunc MySpecialSym() {\n}\n"), 0o644)
	execShare := NewExecutor(tmpShare, &mockEriClient{}, nil, "", nil)
	promptWithSpans, err := execShare.FormatChunkPrompt(VanadisBuild{ID: "b1"}, []VanadisChunk{{
		ID:            "c1",
		Title:         "Chunk 1",
		Mode:          "pr",
		Repo:          "aerial",
		TargetFiles:   []string{"main.go"},
		TargetSymbols: []string{"MySpecialSym"},
	}}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(promptWithSpans, "Target Symbol Spans:") {
		t.Errorf("expected Target Symbol Spans in prompt, got:\n%s", promptWithSpans)
	}

	// PublishExternalEvent
	mockB := &mockBusExecutor{}
	execBus := NewExecutor("", nil, mockB, "", nil)
	if err := execBus.PublishExternalEvent(ctx, "source", "type", []string{"t1"}, "ctx"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mockB.lastSubj != "aerial.events.source.type" {
		t.Errorf("lastSubj = %q, want aerial.events.source.type", mockB.lastSubj)
	}
}

type mockStoreForLaunch struct {
	VanadisStore

	reserveErr error
}

func (m *mockStoreForLaunch) ReserveChunkForLaunch(ctx context.Context, chunkID string) error {
	return m.reserveErr
}

type mockBusExecutor struct {
	pubsub.Bus

	lastSubj string
}

func (m *mockBusExecutor) Publish(ctx context.Context, subject, msgID string, data any) error {
	m.lastSubj = subject
	return nil
}
