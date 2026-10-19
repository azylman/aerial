package guard

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type batchHookInput struct {
	tool string
	path string
	cmd  string
}

func TestCheckBatch(t *testing.T) {
	// Not t.Parallel() because it exercises SetBatchDir and t.Setenv
	now := time.Now()

	turn := func(items ...batchHookInput) []batchHookInput { return items }
	turns := func(ts ...[]batchHookInput) [][]batchHookInput { return ts }

	stepTests := []struct {
		name             string
		steps            [][]batchHookInput
		wantLastDecision DecisionType
		wantReasonSubstr string
	}{
		{"unscoped go test", turns(turn(batchHookInput{tool: "run_command", cmd: "go test ./..."})), DecisionDeny, "Unscoped test sweep"},
		{"unscoped test brain/...", turns(turn(batchHookInput{tool: "run_command", cmd: "go test brain/..."})), DecisionDeny, "Unscoped test sweep"},
		{"unscoped pytest", turns(turn(batchHookInput{tool: "run_command", cmd: "pytest"})), DecisionDeny, "Unscoped test sweep"},
		{"unscoped verify.sh", turns(turn(batchHookInput{tool: "run_command", cmd: "./scripts/verify.sh"})), DecisionDeny, "Unscoped test sweep"},
		{"scoped test allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "go test ./pkg/guard"})), DecisionAllow, ""},
		{"scoped verify staged", turns(turn(batchHookInput{tool: "run_command", cmd: "./scripts/verify.sh --staged"})), DecisionAllow, ""},
		{"unbatched git add", turns(turn(batchHookInput{tool: "run_command", cmd: "git add main.go"})), DecisionDeny, "Unbatched git lifecycle"},
		{"unbatched git commit", turns(turn(batchHookInput{tool: "run_command", cmd: "git commit -m 'fix'"})), DecisionDeny, "Unbatched git lifecycle"},
		{"safe git status", turns(turn(batchHookInput{tool: "run_command", cmd: "git status"})), DecisionAllow, ""},
		{"micro inspection thrashing", turns(turn(batchHookInput{tool: "run_command", cmd: "cat a.txt"}), turn(batchHookInput{tool: "run_command", cmd: "head b.txt"}), turn(batchHookInput{tool: "run_command", cmd: "tail c.txt"}), turn(batchHookInput{tool: "run_command", cmd: "which d.txt"})), DecisionDeny, "Micro-command thrashing detected"},
		{"micro read thrashing same file", turns(turn(batchHookInput{tool: "view_file", path: "/tmp/foo.go"}), turn(batchHookInput{tool: "view_file", path: "/tmp/foo.go"}), turn(batchHookInput{tool: "view_file", path: "/tmp/foo.go"})), DecisionDeny, "Iterative micro-read thrashing"},
		{"sequential single file reads", turns(turn(batchHookInput{tool: "view_file", path: "/tmp/a.go"}), turn(batchHookInput{tool: "view_file", path: "/tmp/b.go"}), turn(batchHookInput{tool: "view_file", path: "/tmp/c.go"})), DecisionDeny, "Parallel Tool Batching Invariant Violated"},
		{"parallel reads allowed", turns(turn(batchHookInput{tool: "view_file", path: "/tmp/a.go"}, batchHookInput{tool: "view_file", path: "/tmp/b.go"}, batchHookInput{tool: "view_file", path: "/tmp/c.go"})), DecisionAllow, ""},
		{"micro edit thrashing", turns(turn(batchHookInput{tool: "replace_file_content", path: "/tmp/mod.go"}), turn(batchHookInput{tool: "replace_file_content", path: "/tmp/mod.go"}), turn(batchHookInput{tool: "replace_file_content", path: "/tmp/mod.go"})), DecisionDeny, "Coarse-Grained Code Editing Invariant Violated"},
		{"read different file allowed", turns(turn(batchHookInput{tool: "view_file", path: "/tmp/foo.go"}, batchHookInput{tool: "view_file", path: "/tmp/extra1.go"}), turn(batchHookInput{tool: "view_file", path: "/tmp/foo.go"}, batchHookInput{tool: "view_file", path: "/tmp/extra2.go"}), turn(batchHookInput{tool: "view_file", path: "/tmp/other.go"}, batchHookInput{tool: "view_file", path: "/tmp/extra1.go"})), DecisionAllow, ""},
	}

	for _, tc := range stepTests {
		t.Run("step_"+tc.name, func(t *testing.T) {
			dir := t.TempDir()
			convID := "test-session"
			var lastDec *Decision

			stepIdx := 0
			for turnIdx, turn := range tc.steps {
				stepTime := now.Add(time.Duration(turnIdx) * 2 * time.Second)
				stepIdx++
				for _, inv := range turn {
					payload := Payload{
						ConversationID: convID,
						StepIdx:        stepIdx,
						ToolCall:       ToolCall{Name: inv.tool},
					}
					args := ToolArgs{
						TargetFile:  inv.path,
						CommandLine: inv.cmd,
					}
					lastDec = CheckBatchWithDir(payload, args, dir, stepTime)
				}
			}

			if lastDec == nil {
				t.Fatalf("no decisions returned")
			}
			if lastDec.Decision != tc.wantLastDecision {
				t.Fatalf("got decision=%q, want %q (reason: %s)", lastDec.Decision, tc.wantLastDecision, lastDec.Reason)
			}
			if tc.wantReasonSubstr != "" && !strings.Contains(lastDec.Reason, tc.wantReasonSubstr) {
				t.Fatalf("expected reason containing %q, got %q", tc.wantReasonSubstr, lastDec.Reason)
			}
		})
	}

	// Fault Injection: Corrupted state file recovery
	dir := t.TempDir()
	convID := "corrupted-session"
	statePath := filepath.Join(dir, fmt.Sprintf("state-%s.json", sanitizeID(convID)))
	if err := os.WriteFile(statePath, []byte("{invalid-json-data"), 0o600); err != nil {
		t.Fatalf("failed to write corrupted state: %v", err)
	}
	dec := CheckBatchWithDir(Payload{ConversationID: convID, StepIdx: 1, ToolCall: ToolCall{Name: "view_file"}}, ToolArgs{TargetFile: "/tmp/foo.txt"}, dir, now)
	if dec.Decision != DecisionAllow {
		t.Fatalf("expected allow after corrupt state recovery, got: %v", dec)
	}

	// Concurrency test: 10 concurrent goroutines calling CheckBatchWithDir
	var wg sync.WaitGroup
	concurrentSession := "concurrent-session"
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			p := Payload{
				ConversationID: concurrentSession,
				StepIdx:        idx + 1,
				ToolCall:       ToolCall{Name: "view_file"},
			}
			d := CheckBatchWithDir(p, ToolArgs{TargetFile: fmt.Sprintf("/tmp/concurrent_%d.go", idx)}, dir, now)
			if d == nil {
				t.Errorf("got nil decision in concurrent call")
			}
		}(i)
	}
	wg.Wait()

	// Max records truncation (DefaultMaxRecords = 30)
	truncDir := t.TempDir()
	truncSession := "trunc-session"
	for i := 0; i < 35; i++ {
		p := Payload{
			ConversationID: truncSession,
			StepIdx:        i + 1,
			ToolCall:       ToolCall{Name: "run_command"},
		}
		d := CheckBatchWithDir(p, ToolArgs{CommandLine: fmt.Sprintf("echo rec-%d && true", i)}, truncDir, now)
		if d.Decision != DecisionAllow {
			t.Fatalf("call %d expected allow, got %v", i, d)
		}
	}
	st := loadState(truncDir, truncSession, now)
	if len(st.Records) > DefaultMaxRecords {
		t.Fatalf("records length = %d, want <= %d", len(st.Records), DefaultMaxRecords)
	}

	// Uncreatable directory fail-open
	dFail := CheckBatchWithDir(Payload{ToolCall: ToolCall{Name: "view_file"}}, ToolArgs{TargetFile: "/a.go"}, "/dev/null/impossible", now)
	if dFail.Decision != DecisionAllow {
		t.Fatalf("expected allow on uncreatable dir, got %v", dFail)
	}

	// Wrapper and Env checks
	t.Setenv("AERIAL_BATCH_GUARD_DIR", "/tmp/custom-batch-guard-dir")
	if envDir := GetBatchDir(); envDir != "/tmp/custom-batch-guard-dir" {
		t.Fatalf("GetBatchDir() = %q, want /tmp/custom-batch-guard-dir", envDir)
	}
	os.Unsetenv("AERIAL_BATCH_GUARD_DIR")

	customDir := t.TempDir()
	SetBatchDir(customDir)
	defer SetBatchDir(DefaultBatchDir)
	dDefault := CheckBatch(Payload{ConversationID: "custom-batch-session", StepIdx: 1, ToolCall: ToolCall{Name: "view_file"}}, ToolArgs{TargetFile: "/tmp/fresh.txt"})
	if dDefault == nil || dDefault.Decision != DecisionAllow {
		t.Fatalf("expected allow from CheckBatch, got %v", dDefault)
	}

	// Prune state files and opportunistic GC
	gcDir := t.TempDir()
	oldFile := filepath.Join(gcDir, "state-old-session.json")
	newFile := filepath.Join(gcDir, "state-new-session.json")
	if err := os.WriteFile(oldFile, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(newFile, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	oldTime := now.Add(-3 * time.Hour)
	if err := os.Chtimes(oldFile, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(gcDir, "subdir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gcDir, "ignore.txt"), []byte("txt"), 0o600); err != nil {
		t.Fatal(err)
	}

	pruneStateFiles(gcDir, now)
	if _, err := os.Stat(oldFile); !os.IsNotExist(err) {
		t.Fatalf("expected old file to be pruned")
	}
	if _, err := os.Stat(newFile); err != nil {
		t.Fatalf("expected new file to remain")
	}
	opportunisticGC(gcDir, now)
}

func TestBatchHelpers(t *testing.T) {
	t.Parallel()

	unscopedTests := []struct {
		cmd  string
		want bool
	}{
		{"go test ./...", true},
		{"go test brain/...", true},
		{"go test", true},
		{"pytest", true},
		{"python3 -m pytest", true},
		{"scripts/verify.sh", true},
		{"./scripts/verify.sh", true},
		{"go test ./pkg/guard", false},
		{"scripts/verify.sh --staged", false},
		{"scripts/verify.sh -h", false},
		{"", false},
	}
	for _, tc := range unscopedTests {
		t.Run("unscoped_"+tc.cmd, func(t *testing.T) {
			t.Parallel()
			if got := isUnscopedTest(tc.cmd); got != tc.want {
				t.Fatalf("isUnscopedTest(%q) = %v, want %v", tc.cmd, got, tc.want)
			}
		})
	}

	gitMutationTests := []struct {
		cmd  string
		want bool
	}{
		{"git add .", true},
		{"git commit -m 'msg'", true},
		{"git status", false},
		{"git diff", false},
		{"git log", false},
		{"echo git add", false},
		{"", false},
	}
	for _, tc := range gitMutationTests {
		t.Run("git_mutation_"+tc.cmd, func(t *testing.T) {
			t.Parallel()
			if got := isUnbatchedGitMutation(tc.cmd); got != tc.want {
				t.Fatalf("isUnbatchedGitMutation(%q) = %v, want %v", tc.cmd, got, tc.want)
			}
		})
	}

	singleInspectionTests := []struct {
		cmd  string
		want bool
	}{
		{"cat file.go", true},
		{"head -n 10 file.go", true},
		{"tail file.go", true},
		{"which agy", true},
		{"stat file.go", true},
		{"file binary", true},
		{"echo hello", true},
		{"cat a.txt && cat b.txt", false},
		{"cat a.txt | grep foo", false},
		{"", false},
		{"   ", false},
	}
	for _, tc := range singleInspectionTests {
		t.Run("inspection_"+tc.cmd, func(t *testing.T) {
			t.Parallel()
			if got := isSingleInspection(tc.cmd); got != tc.want {
				t.Fatalf("isSingleInspection(%q) = %v, want %v", tc.cmd, got, tc.want)
			}
		})
	}

	categoryTests := []struct {
		name     string
		tool     string
		args     ToolArgs
		wantCat  string
		wantTgt  string
	}{
		{"view file", "view_file", ToolArgs{TargetFile: "/tmp/a.go"}, "read", "/tmp/a.go"},
		{"replace content", "replace_file_content", ToolArgs{TargetFile: "/tmp/a.go"}, "edit", "/tmp/a.go"},
		{"write to file", "write_to_file", ToolArgs{TargetFile: "/tmp/a.go"}, "edit", "/tmp/a.go"},
		{"unscoped test cmd", "run_command", ToolArgs{CommandLine: "go test ./..."}, "unscoped_test", "go test ./..."},
		{"git mutation cmd", "run_command", ToolArgs{CommandLine: "git commit -m 'a'"}, "git_mutation", "git commit -m 'a'"},
		{"inspection cmd", "run_command", ToolArgs{CommandLine: "cat a.txt"}, "inspection", "cat a.txt"},
		{"generic cmd", "run_command", ToolArgs{CommandLine: "make build"}, "command", "make build"},
		{"other tool", "custom_mcp_tool", ToolArgs{}, "other", "custom_mcp_tool"},
		{"default_api prefix", "default_api:run_command", ToolArgs{CommandLine: "cat a.txt"}, "inspection", "cat a.txt"},
	}
	for _, tc := range categoryTests {
		t.Run("category_"+tc.name, func(t *testing.T) {
			t.Parallel()
			cat, tgt := categorizeInvocation(tc.tool, tc.args)
			if cat != tc.wantCat || tgt != tc.wantTgt {
				t.Fatalf("categorizeInvocation = (%q, %q), want (%q, %q)", cat, tgt, tc.wantCat, tc.wantTgt)
			}
		})
	}

	sanitizeTests := []struct {
		input string
		want  string
	}{
		{"", "default"},
		{"   ", "default"},
		{"session-123_abc", "session-123_abc"},
		{"session/../../../etc/passwd", "sessionetcpasswd"},
		{"!@#$%^&*()", "default"},
	}
	for _, tc := range sanitizeTests {
		t.Run("sanitize_"+tc.input, func(t *testing.T) {
			t.Parallel()
			if got := sanitizeID(tc.input); got != tc.want {
				t.Fatalf("sanitizeID(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}

	// Step grouping and timestamp check
	rec := ToolInvocation{Timestamp: 100, StepIdx: 0}
	if !isSameStep(rec, 0, 99) {
		t.Fatalf("expected true for within 1s diff")
	}
	if isSameStep(rec, 0, 50) {
		t.Fatalf("expected false for diff > 1s")
	}

	if steps := groupSteps(nil); steps != nil {
		t.Fatalf("expected nil for empty records")
	}
	invs := []ToolInvocation{
		{Timestamp: 100, StepIdx: 0},
		{Timestamp: 99, StepIdx: 0},
		{Timestamp: 50, StepIdx: 0},
	}
	steps := groupSteps(invs)
	if len(steps) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(steps))
	}

	// Filtering and counting helpers
	list := []ToolInvocation{
		{Category: "read", Target: "a.go", StepIdx: 1, Timestamp: 100},
		{Category: "read", Target: "a.go", StepIdx: 2, Timestamp: 200},
		{Category: "read", Target: "b.go", StepIdx: 3, Timestamp: 300},
		{Category: "edit", Target: "a.go", StepIdx: 4, Timestamp: 400},
	}
	if count := countTargetInvocations(list, "read", "a.go"); count != 2 {
		t.Fatalf("countTargetInvocations = %d, want 2", count)
	}
	afterRemove := removeTargetInvocations(list, "a.go")
	if len(afterRemove) != 1 || afterRemove[0].Target != "b.go" {
		t.Fatalf("removeTargetInvocations failed: %+v", afterRemove)
	}
	afterFilter := filterOutCategory(list, "read")
	if len(afterFilter) != 1 || afterFilter[0].Category != "edit" {
		t.Fatalf("filterOutCategory failed: %+v", afterFilter)
	}
}
