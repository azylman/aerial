package guard

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type batchHookInput struct {
	tool  string
	path  string
	cmd   string
	start int
	end   int
}

func TestCheckBatch(t *testing.T) {
	// Not t.Parallel() because it exercises SetBatchDir and t.Setenv
	now := time.Now()

	turn := func(items ...batchHookInput) []batchHookInput { return items }
	turns := func(ts ...[]batchHookInput) [][]batchHookInput { return ts }

	stepTests := []struct {
		name               string
		steps              [][]batchHookInput
		wantLastDecision   DecisionType
		wantReasonSubstr   string
		wantOverwriteCmd   string
		wantOverwriteStart int
		wantOverwriteEnd   int
	}{
		{"unscoped go test", turns(turn(batchHookInput{tool: "run_command", cmd: "go test ./..."})), DecisionDeny, "Unscoped test sweep", "", 0, 0},
		{"unscoped test brain/...", turns(turn(batchHookInput{tool: "run_command", cmd: "go test brain/..."})), DecisionDeny, "Unscoped test sweep", "", 0, 0},
		{"unscoped test services/eri/...", turns(turn(batchHookInput{tool: "run_command", cmd: "go test services/eri/..."})), DecisionDeny, "Unscoped test sweep", "", 0, 0},
		{"unscoped pytest", turns(turn(batchHookInput{tool: "run_command", cmd: "pytest"})), DecisionDeny, "Unscoped test sweep", "", 0, 0},
		{"unscoped verify.sh", turns(turn(batchHookInput{tool: "run_command", cmd: "./scripts/verify.sh"})), DecisionDeny, "Unscoped test sweep", "", 0, 0},
		{"scoped test allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "go test ./pkg/guard"})), DecisionAllow, "", "", 0, 0},
		{"verbose test stripped and allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "go test -v ./pkg/guard"})), DecisionAllow, "", "go test ./pkg/guard", 0, 0},
		{"verbose test with run stripped and allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "go test -v -run TestCheckBatch ./pkg/guard"})), DecisionAllow, "", "go test -run TestCheckBatch ./pkg/guard", 0, 0},
		{"compound unscoped test denied", turns(turn(batchHookInput{tool: "run_command", cmd: "cd /tmp && go test ./..."})), DecisionDeny, "Unscoped test sweep", "", 0, 0},
		{"scoped verify staged", turns(turn(batchHookInput{tool: "run_command", cmd: "./scripts/verify.sh --staged"})), DecisionAllow, "", "", 0, 0},
		{"verify help allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "./scripts/verify.sh --help"})), DecisionAllow, "", "", 0, 0},
		{"empty command in batch allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "   "})), DecisionAllow, "", "", 0, 0},
		{"unbatched git add", turns(turn(batchHookInput{tool: "run_command", cmd: "git add main.go"})), DecisionDeny, "Unbatched git lifecycle", "", 0, 0},
		{"unbatched git commit", turns(turn(batchHookInput{tool: "run_command", cmd: "git commit -m 'fix'"})), DecisionDeny, "Unbatched git lifecycle", "", 0, 0},
		{"bare git status rewritten to -sb", turns(turn(batchHookInput{tool: "run_command", cmd: "git status"})), DecisionAllow, "", "git status -sb", 0, 0},
		{"git -C status rewritten to -sb", turns(turn(batchHookInput{tool: "run_command", cmd: "git -C /tmp/repo status pkg/guard"})), DecisionAllow, "", "git -C /tmp/repo status -sb pkg/guard", 0, 0},
		{"subshell git status rewritten to -sb", turns(turn(batchHookInput{tool: "run_command", cmd: "(cd /tmp/repo && git status)"})), DecisionAllow, "", "(cd /tmp/repo && git status -sb)", 0, 0},
		{"git status -s unchanged", turns(turn(batchHookInput{tool: "run_command", cmd: "git status -s"})), DecisionAllow, "", "", 0, 0},
		{"git status --porcelain unchanged", turns(turn(batchHookInput{tool: "run_command", cmd: "git status --porcelain"})), DecisionAllow, "", "", 0, 0},
		{"unbounded git log denied", turns(turn(batchHookInput{tool: "run_command", cmd: "git log"})), DecisionDeny, "Unbounded git log detected", "", 0, 0},
		{"unbounded git log --oneline denied", turns(turn(batchHookInput{tool: "run_command", cmd: "git log --oneline"})), DecisionDeny, "Unbounded git log detected", "", 0, 0},
		{"bounded git log -n 5 allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "git log -n 5 --oneline"})), DecisionAllow, "", "", 0, 0},
		{"bounded git log -n5 allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "git log -n5"})), DecisionAllow, "", "", 0, 0},
		{"bounded git log -3 allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "git log -3"})), DecisionAllow, "", "", 0, 0},
		{"unscoped bare git diff denied", turns(turn(batchHookInput{tool: "run_command", cmd: "git diff"})), DecisionDeny, "Unscoped full-patch git diff detected", "", 0, 0},
		{"unscoped git diff --cached denied", turns(turn(batchHookInput{tool: "run_command", cmd: "git diff --cached"})), DecisionDeny, "Unscoped full-patch git diff detected", "", 0, 0},
		{"unscoped git diff HEAD denied", turns(turn(batchHookInput{tool: "run_command", cmd: "git diff HEAD"})), DecisionDeny, "Unscoped full-patch git diff detected", "", 0, 0},
		{"unscoped git diff origin/main denied", turns(turn(batchHookInput{tool: "run_command", cmd: "git --no-pager diff origin/main"})), DecisionDeny, "Unscoped full-patch git diff detected", "", 0, 0},
		{"unscoped git diff dot denied", turns(turn(batchHookInput{tool: "run_command", cmd: "git diff -- ."})), DecisionDeny, "Unscoped full-patch git diff detected", "", 0, 0},
		{"git version flag without subcommand allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "git --version"})), DecisionAllow, "", "", 0, 0},
		{"git diff --stat allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "git diff --stat"})), DecisionAllow, "", "", 0, 0},
		{"git diff --name-only allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "git diff --cached --name-only"})), DecisionAllow, "", "", 0, 0},
		{"git diff scoped with dash-dash allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "git diff HEAD -- services/eri/pkg/guard/batch.go"})), DecisionAllow, "", "", 0, 0},
		{"git diff scoped with file path allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "git diff -U 3 services/eri/pkg/guard/batch.go"})), DecisionAllow, "", "", 0, 0},
		{"read_file on transcript.jsonl denied", turns(turn(batchHookInput{tool: "read_file", path: "/data/runtimes/discord/.gemini/antigravity-cli/brain/361f6998/.system_generated/logs/transcript.jsonl"})), DecisionDeny, "transcript-search", "", 0, 0},
		{"view_file on transcript_full.jsonl denied", turns(turn(batchHookInput{tool: "view_file", path: ".system_generated/logs/transcript_full.jsonl"})), DecisionDeny, "transcript-search", "", 0, 0},
		{"docker exec go test -v stripped and allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "docker exec -i eri go test -v ./pkg/guard"})), DecisionAllow, "", "docker exec -i eri go test ./pkg/guard", 0, 0},
		{"docker exec unscoped go test denied", turns(turn(batchHookInput{tool: "run_command", cmd: "docker exec eri go test ./..."})), DecisionDeny, "Unscoped test sweep", "", 0, 0},
		{"sequential inspection commands allowed", turns(turn(batchHookInput{tool: "run_command", cmd: "cat a.txt"}), turn(batchHookInput{tool: "run_command", cmd: "head b.txt"}), turn(batchHookInput{tool: "run_command", cmd: "tail c.txt"}), turn(batchHookInput{tool: "run_command", cmd: "which d.txt"})), DecisionAllow, "", "", 0, 0},
		{"unbounded read_file allowed without overwrite", turns(turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go"})), DecisionAllow, "", "", 0, 0},
		{"open-ended StartLine read_file allowed without overwrite", turns(turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go", start: 801})), DecisionAllow, "", "", 0, 0},
		{"inverted read_file range allowed without overwrite", turns(turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go", start: 100, end: 50})), DecisionAllow, "", "", 0, 0},
		{"empty path read_file allowed without overwrite", turns(turn(batchHookInput{tool: "read_file", path: "", start: 10, end: 20})), DecisionAllow, "", "", 0, 0},
		{"full-page 800-line read_file allowed without overwrite", turns(turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go", start: 1, end: 800})), DecisionAllow, "", "", 0, 0},
		{"120-line anchor read_file allowed without overwrite", turns(turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go", start: 101, end: 220})), DecisionAllow, "", "", 0, 0},
		{"tight read_file padded to 120-line anchor window", turns(turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go", start: 100, end: 130})), DecisionAllow, "", "", 55, 174},
		{"tight read_file near top clamped to line 1", turns(turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go", start: 5, end: 25})), DecisionAllow, "", "", 1, 120},
		{"second disjoint read_file padded to 120-line anchor window", turns(turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go", start: 100, end: 130}), turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go", start: 900, end: 930})), DecisionAllow, "", "", 855, 974},
		{"contiguous forward read promoted to 800-line page", turns(turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go", start: 440, end: 560}), turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go", start: 561, end: 620})), DecisionAllow, "", "", 440, 1239},
		{"contiguous forward read after full page 1 promoted to page 2", turns(turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go", start: 1, end: 800}), turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go", start: 801, end: 860})), DecisionAllow, "", "", 801, 1600},
		{"contiguous backward read promoted to 800-line page clamped to 1", turns(turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go", start: 600, end: 750}), turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go", start: 450, end: 600})), DecisionAllow, "", "", 1, 800},
		{"contiguous backward read at high lines promoted to 800-line page", turns(turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go", start: 1500, end: 1650}), turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go", start: 1400, end: 1499})), DecisionAllow, "", "", 851, 1650},
		{"contiguous backward read exceeding 800-line union anchored at start", turns(turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go", start: 1000, end: 1750}), turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go", start: 900, end: 999})), DecisionAllow, "", "", 900, 1699},
		{"third disjoint read on same file promoted to centered 800-line page", turns(turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go", start: 100, end: 220}), turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go", start: 500, end: 620}), turn(batchHookInput{tool: "read_file", path: "/tmp/foo.go", start: 1000, end: 1040})), DecisionAllow, "", "", 620, 1419},
		{"cross-path share and scratch reads normalized and promoted", turns(turn(batchHookInput{tool: "read_file", path: "/share/aerial/pkg/foo.go", start: 100, end: 220}), turn(batchHookInput{tool: "read_file", path: "/data/scratch/aerial-code-scratch.abc123/pkg/foo.go", start: 221, end: 300})), DecisionAllow, "", "", 100, 899},
		{"edit invalidates prior reads on same file", turns(turn(batchHookInput{tool: "read_file", path: "/share/aerial/pkg/foo.go", start: 100, end: 220}), turn(batchHookInput{tool: "read_file", path: "/share/aerial/pkg/foo.go", start: 500, end: 620}), turn(batchHookInput{tool: "edit_file", path: "/data/scratch/aerial-code-scratch.abc123/pkg/foo.go"}), turn(batchHookInput{tool: "read_file", path: "/data/scratch/aerial-code-scratch.abc123/pkg/foo.go", start: 1000, end: 1030})), DecisionAllow, "", "", 955, 1074},
		{"sequential single file reads across 4 files allowed", turns(turn(batchHookInput{tool: "read_file", path: "/tmp/a.go"}), turn(batchHookInput{tool: "read_file", path: "/tmp/b.go"}), turn(batchHookInput{tool: "read_file", path: "/tmp/c.go"}), turn(batchHookInput{tool: "read_file", path: "/tmp/d.go"})), DecisionAllow, "", "", 0, 0},
		{"parallel reads allowed", turns(turn(batchHookInput{tool: "view_file", path: "/tmp/a.go"}, batchHookInput{tool: "view_file", path: "/tmp/b.go"}, batchHookInput{tool: "view_file", path: "/tmp/c.go"})), DecisionAllow, "", "", 0, 0},
		{"multiple replace_file_content edits allowed", turns(turn(batchHookInput{tool: "replace_file_content", path: "/tmp/mod.go"}), turn(batchHookInput{tool: "replace_file_content", path: "/tmp/mod.go"}), turn(batchHookInput{tool: "replace_file_content", path: "/tmp/mod.go"})), DecisionAllow, "", "", 0, 0},
		{"multiple edit_file and write_file calls allowed", turns(turn(batchHookInput{tool: "edit_file", path: "/tmp/mod.go"}), turn(batchHookInput{tool: "write_file", path: "/tmp/mod.go"}), turn(batchHookInput{tool: "edit_file", path: "/tmp/mod.go"})), DecisionAllow, "", "", 0, 0},
		{"post-test edit after go test allowed", turns(turn(batchHookInput{tool: "write_file", path: "/tmp/mod.go"}), turn(batchHookInput{tool: "run_command", cmd: "go test ./pkg/guard"}), turn(batchHookInput{tool: "edit_file", path: "/tmp/mod.go"}), turn(batchHookInput{tool: "edit_file", path: "/tmp/mod.go"})), DecisionAllow, "", "", 0, 0},
	}

	for _, tc := range stepTests {
		t.Run("step_"+tc.name, func(t *testing.T) {
			dir := t.TempDir()
			convID := "test-session"
			var lastDec *Decision

			stepIdx := 0
			for turnIdx, turn := range tc.steps {
				stepTime := now.Add(time.Duration(turnIdx) * 2 * time.Second)
				for _, inv := range turn {
					stepIdx++
					payload := Payload{
						ConversationID: convID,
						StepIdx:        stepIdx,
						ToolCall:       ToolCall{Name: inv.tool},
					}
					args := ToolArgs{
						TargetFile:  inv.path,
						StartLine:   inv.start,
						EndLine:     inv.end,
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
			if strings.Contains(lastDec.Reason, " -v ") {
				t.Fatalf("unexpected -v recommendation in reason: %q", lastDec.Reason)
			}
			gotOverwrite, _ := lastDec.Overwrite["CommandLine"].(string)
			if gotOverwrite != tc.wantOverwriteCmd {
				t.Fatalf("Overwrite[CommandLine] = %q, want %q", gotOverwrite, tc.wantOverwriteCmd)
			}
			gotStart, _ := lastDec.Overwrite["StartLine"].(int)
			gotEnd, _ := lastDec.Overwrite["EndLine"].(int)
			if gotStart != tc.wantOverwriteStart || gotEnd != tc.wantOverwriteEnd {
				t.Fatalf("Overwrite[StartLine,EndLine] = (%d, %d), want (%d, %d)", gotStart, gotEnd, tc.wantOverwriteStart, tc.wantOverwriteEnd)
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
	for i := range 10 {
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
	for i := range 35 {
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
	if defDir := GetBatchDir(); defDir != DefaultBatchDir {
		t.Fatalf("GetBatchDir() default = %q, want %q", defDir, DefaultBatchDir)
	}
	t.Setenv("AERIAL_BATCH_GUARD_DIR", "/tmp/custom-batch-guard-dir")
	if envDir := GetBatchDir(); envDir != "/tmp/custom-batch-guard-dir" {
		t.Fatalf("GetBatchDir() = %q, want /tmp/custom-batch-guard-dir", envDir)
	}

	customDir := t.TempDir()
	t.Setenv("AERIAL_BATCH_GUARD_DIR", customDir)
	dDefault := CheckBatch(Payload{ConversationID: "custom-batch-session", StepIdx: 1, ToolCall: ToolCall{Name: "view_file"}}, ToolArgs{TargetFile: "/tmp/fresh.txt"})
	if dDefault == nil || dDefault.Decision != DecisionAllow {
		t.Fatalf("expected allow from CheckBatch, got %v", dDefault)
	}

	// Tunable environment variable overrides and SCRATCH_DIR resolution
	t.Setenv("AERIAL_BATCH_MIN_ANCHOR_LINES", "80")
	t.Setenv("SCRATCH_DIR", "/tmp/custom-worktree")
	if got := minAnchorReadLines(); got != 80 {
		t.Fatalf("minAnchorReadLines() = %d, want 80", got)
	}
	if got := normalizeBatchTarget("/tmp/custom-worktree/pkg/a.go"); got != "/share/aerial/pkg/a.go" {
		t.Fatalf("normalizeBatchTarget(custom-worktree) = %q, want /share/aerial/pkg/a.go", got)
	}
	if got := normalizeBatchTarget("$SCRATCH_DIR/pkg/a.go"); got != "/share/aerial/pkg/a.go" {
		t.Fatalf("normalizeBatchTarget($SCRATCH_DIR) = %q, want /share/aerial/pkg/a.go", got)
	}
	t.Setenv("AERIAL_BATCH_MIN_ANCHOR_LINES", "invalid")
	if got := minAnchorReadLines(); got != DefaultMinAnchorReadLines {
		t.Fatalf("minAnchorReadLines() fallback = %d, want %d", got, DefaultMinAnchorReadLines)
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

	// PreInvocation turn grouping (parallel multi-region edit_file taking >1s each)
	invDir := t.TempDir()
	t.Setenv("AERIAL_BATCH_GUARD_DIR", invDir)
	_ = RecordPreInvocation(Payload{ConversationID: "inv-s", InvocationNum: 1})
	for i := range 4 {
		if d := CheckBatchWithDir(Payload{ConversationID: "inv-s", StepIdx: 10 + i, ToolCall: ToolCall{Name: "edit_file"}}, ToolArgs{TargetFile: "/tmp/same_turn.go"}, invDir, now.Add(time.Duration(i*3)*time.Second)); d.Decision != DecisionAllow {
			t.Fatalf("parallel edit %d in same invocationNum=1 should be allowed, got: %+v", i, d)
		}
	}
	for i := 1; i <= 3; i++ {
		_ = RecordPreInvocationWithDir(Payload{ConversationID: "post-s", InvocationNum: i}, invDir, now.Add(time.Duration(i*2)*time.Second))
		if d := CheckBatchWithDir(Payload{ConversationID: "post-s", StepIdx: i, ToolCall: ToolCall{Name: "edit_file"}}, ToolArgs{TargetFile: "/tmp/rb.go"}, invDir, now.Add(time.Duration(i*2)*time.Second)); d.Decision != DecisionAllow {
			t.Fatalf("turn %d expected allow after rolling back turn 2 failed edit, got: %+v", i, d)
		}
		if i == 2 {
			_ = RecordPostBatch(Payload{ConversationID: "post-s", StepIdx: 2, Error: "not found", ToolCall: ToolCall{Name: "edit_file"}}, ToolArgs{TargetFile: "/tmp/rb.go"})
		}
	}
	_ = RecordPostBatchWithDir(Payload{ConversationID: "post-s", StepIdx: 99, Error: "failed", ToolCall: ToolCall{Name: "edit_file"}}, ToolArgs{TargetFile: "/tmp/rb.go"}, invDir, now)
	_ = RecordPostBatchWithDir(Payload{ConversationID: "post-s", StepIdx: 3, Error: "", ToolCall: ToolCall{Name: "edit_file"}}, ToolArgs{TargetFile: "/tmp/rb.go"}, invDir, now)
	_ = RecordPostBatchWithDir(Payload{ConversationID: "post-s", StepIdx: 4, Error: "", ToolCall: ToolCall{Name: "run_command"}}, ToolArgs{CommandLine: "scripts/aerial-pr.sh submit /data/scratch/x 'feat: test'"}, invDir, now)
}

func TestBatchHelpers(t *testing.T) {
	t.Parallel()

	unscopedTests := []struct {
		cmd  string
		want bool
	}{
		{"go test ./...", true},
		{"go test brain/...", true},
		{"go test services/eri/...", true},
		{"go test", true},
		{"pytest", true},
		{"python3 -m pytest", true},
		{"scripts/verify.sh", true},
		{"./scripts/verify.sh", true},
		{"go test ./pkg/guard", false},
		{"go test -v ./pkg/guard", false},
		{"go test -v -run TestFoo ./pkg/guard", false},
		{"go test -v=true -run=TestFoo -timeout 30s ./pkg/guard", false},
		{"cd services/eri && go test ./...", true},
		{"(cd services/eri && go test ./...)", true},
		{"CGO_ENABLED=0 go test ./...", true},
		{"go test -C services/eri ./...", true},
		{"go test -C services/synapse ./...", false},
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

	stripVerboseTests := []struct {
		cmd  string
		want string
	}{
		{"go test ./pkg/guard", "go test ./pkg/guard"},
		{"go test -v ./pkg/guard", "go test ./pkg/guard"},
		{"go test -race ./pkg/guard", "go test -race ./pkg/guard"},
		{"go test -test.v ./pkg/guard", "go test ./pkg/guard"},
		{"go test -v=true -test.v=true --v --test.v ./pkg/guard", "go test ./pkg/guard"},
		{"go test -v -run TestFoo ./pkg/guard", "go test -run TestFoo ./pkg/guard"},
		{"cd /tmp && CGO_ENABLED=1 go test -v -race ./pkg/guard", "cd /tmp && CGO_ENABLED=1 go test -race ./pkg/guard"},
		{"go test -race ./services/eri/pkg/...", "go test -C services/eri -race ./pkg/..."},
		{"go test -v -race -run TestFoo ./services/eri/pkg/guard", "go test -C services/eri -race -run TestFoo ./pkg/guard"},
		{"go test ./services/eri/pkg/guard ./services/eri/pkg/queue", "go test -C services/eri ./pkg/guard ./pkg/queue"},
		{"go test ./services/synapse/...", "go test -C services/synapse ./..."},
		{"go test ./services/synapse", "go test -C services/synapse ."},
		{"go test ./mcp/discord-mcp/...", "go test -C mcp/discord-mcp ./..."},
		{"go test ./scripts/code-health/...", "go test -C scripts/code-health ./..."},
		{"go test ./scripts/code-health", "go test -C scripts/code-health ."},
		{"go test ./pkg/httpkit/...", "go test -C pkg ./httpkit/..."},
		{"go test -C services/eri ./services/eri/pkg/guard", "go test -C services/eri ./services/eri/pkg/guard"},
		{"go test ./services/eri/pkg/guard ./services/haro", "go test ./services/eri/pkg/guard ./services/haro"},
		{"go test -count=1 ./services/eri/pkg/guard", "go test -C services/eri -count=1 ./pkg/guard"},
		{"(cd /tmp && go test ./pkg/guard -v)", "(cd /tmp && go test ./pkg/guard)"},
		{"(cd /tmp && go test -race ./services/eri/pkg/guard)", "(cd /tmp && go test -C services/eri -race ./pkg/guard)"},
		{"(go test -v ./pkg/guard)", "(go test ./pkg/guard)"},
		{"go test -v ./pkg/guard -- -v", "go test ./pkg/guard -- -v"},
		{"go test -run -v -v ./pkg/guard", "go test -run -v ./pkg/guard"},
	}
	for _, tc := range stripVerboseTests {
		t.Run("strip_verbose_"+tc.cmd, func(t *testing.T) {
			t.Parallel()
			if got := stripGoTestVerbose(tc.cmd); got != tc.want {
				t.Fatalf("stripGoTestVerbose(%q) = %q, want %q", tc.cmd, got, tc.want)
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

	categoryTests := []struct {
		name    string
		tool    string
		args    ToolArgs
		wantCat string
		wantTgt string
	}{
		{"view file", "view_file", ToolArgs{TargetFile: "/tmp/a.go"}, "read", "/tmp/a.go"},
		{"view file empty path", "view_file", ToolArgs{}, "read", ""},
		{"view file config scratch", "view_file", ToolArgs{TargetFile: "/data/scratch/aerial-config-scratch.999999/config.yaml"}, "read", "/share/aerial-config/config.yaml"},
		{"view file aerial-pr scratch", "view_file", ToolArgs{TargetFile: "/tmp/aerial-pr-123456/main.go"}, "read", "/share/aerial/main.go"},
		{"replace content", "replace_file_content", ToolArgs{TargetFile: "/tmp/a.go"}, "edit", "/tmp/a.go"},
		{"write to file", "write_to_file", ToolArgs{TargetFile: "/tmp/a.go"}, "edit", "/tmp/a.go"},
		{"unscoped test cmd", "run_command", ToolArgs{CommandLine: "go test ./..."}, "unscoped_test", "go test ./..."},
		{"git mutation cmd", "run_command", ToolArgs{CommandLine: "git commit -m 'a'"}, "git_mutation", "git commit -m 'a'"},
		{"inspection cmd", "run_command", ToolArgs{CommandLine: "cat a.txt"}, "command", "cat a.txt"},
		{"generic cmd", "run_command", ToolArgs{CommandLine: "make build"}, "command", "make build"},
		{"other tool", "custom_mcp_tool", ToolArgs{}, "other", "custom_mcp_tool"},
		{"default_api prefix", "default_api:run_command", ToolArgs{CommandLine: "cat a.txt"}, "command", "cat a.txt"},
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

	// Filtering helpers
	list := []ToolInvocation{
		{Category: "read", Target: "a.go", StepIdx: 1, Timestamp: 100, StartLine: 1, EndLine: 120},
		{Category: "read", Target: "a.go", StepIdx: 2, Timestamp: 200, StartLine: 121, EndLine: 240},
		{Category: "read", Target: "b.go", StepIdx: 3, Timestamp: 300, StartLine: 1, EndLine: 120},
		{Category: "edit", Target: "a.go", StepIdx: 4, Timestamp: 400},
	}
	afterRemove := removeTargetInvocations(list, "read", "a.go")
	if len(afterRemove) != 2 || afterRemove[0].Target != "b.go" || afterRemove[1].Category != "edit" {
		t.Fatalf("removeTargetInvocations failed: %+v", afterRemove)
	}

	submitTests := []struct {
		cmd  string
		want bool
	}{
		{"scripts/aerial-pr.sh submit /data/scratch/x 'feat: x'", true},
		{"/share/aerial/scripts/aerial-pr.sh --repo aerial submit /data/scratch/x", true},
		{"scripts/aerial-pr.sh override-roborev 123 'rebuttal'", true},
		{"scripts/aerial-pr.sh dispute-roborev 123 'rebuttal'", true},
		{"cd /share/aerial && scripts/aerial-build.sh enqueue /tmp/plan.json", true},
		{"scripts/aerial-pr.sh init aerial", false},
		{"scripts/aerial-build.sh validate /tmp/plan.json", false},
		{"scripts/aerial-pr.sh", false},
		{"echo hello", false},
		{"", false},
	}
	for _, tc := range submitTests {
		t.Run("submit_"+tc.cmd, func(t *testing.T) {
			t.Parallel()
			if got := isTerminalSubmitCommand(tc.cmd); got != tc.want {
				t.Fatalf("isTerminalSubmitCommand(%q) = %v, want %v", tc.cmd, got, tc.want)
			}
		})
	}

	statTests := []struct {
		name     string
		stat     string
		wantPPID int
		wantPGRP int
		wantOK   bool
	}{
		{"valid bash child", "1425 (bash) S 209 1425 1425 34816 1425 4194304", 209, 1425, true},
		{"comm with parens", "1426 (my (cmd)) R 209 209 1 0", 209, 209, true},
		{"missing closing paren", "1425 (bash S 209 1425", 0, 0, false},
		{"too few fields", "1425 (bash) S 209", 0, 0, false},
		{"non-numeric ppid", "1425 (bash) S abc 1425", 0, 0, false},
		{"non-numeric pgrp", "1425 (bash) S 209 xyz", 0, 0, false},
	}
	for _, tc := range statTests {
		t.Run("stat_"+tc.name, func(t *testing.T) {
			t.Parallel()
			ppid, pgrp, ok := parseProcStatPPIDAndPGRP(tc.stat)
			if ok != tc.wantOK || ppid != tc.wantPPID || pgrp != tc.wantPGRP {
				t.Fatalf("parseProcStatPPIDAndPGRP(%q) = (%d, %d, %v), want (%d, %d, %v)", tc.stat, ppid, pgrp, ok, tc.wantPPID, tc.wantPGRP, tc.wantOK)
			}
		})
	}

	if got := resolveProcDir(DefaultBatchDir); got != "/proc" {
		t.Fatalf("resolveProcDir(DefaultBatchDir) = %q, want /proc", got)
	}
	if got := resolveProcDir("/tmp/custom"); got != "/tmp/custom/proc" {
		t.Fatalf("resolveProcDir(/tmp/custom) = %q, want /tmp/custom/proc", got)
	}

	fakeProc := filepath.Join(t.TempDir(), "proc")
	for _, entry := range []struct {
		dir  string
		stat string
	}{
		{"not-a-pid", ""},
		{"1", "1 (init) S 0 1 1"},
		{"209", "209 (agy) S 7 209 1"},
		{"1500", "1500 (hook-guard) R 209 209 1"},
		{"1425", "1425 (bash) S 209 1425 1425"},
		{"1426", "1426 (bash) S 209 1426 1426"},
		{"1427", "1427 (grep) R 1425 1425 1425"},
		{"1499", "1499 (hook) S 209 209 1"},
		{"1600", ""},
	} {
		dirPath := filepath.Join(fakeProc, entry.dir)
		if err := os.MkdirAll(dirPath, 0o755); err != nil {
			t.Fatal(err)
		}
		if entry.stat != "" {
			if err := os.WriteFile(filepath.Join(dirPath, "stat"), []byte(entry.stat), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}

	var signaled []int
	mockSig := func(pid int, sig syscall.Signal) error {
		if sig == syscall.SIGKILL {
			signaled = append(signaled, pid)
		}
		if pid == -1425 {
			return syscall.ESRCH
		}
		return nil
	}
	reapOrphanedChildProcessGroups(fakeProc, 0, 1500, mockSig)
	reapOrphanedChildProcessGroups(fakeProc, 209, 1500, nil)
	reapOrphanedChildProcessGroups(filepath.Join(fakeProc, "nonexistent"), 209, 1500, mockSig)
	reapOrphanedChildProcessGroups(fakeProc, 209, 1500, mockSig)
	if len(signaled) != 2 || signaled[0] != -1425 || signaled[1] != -1426 {
		t.Fatalf("signaled = %v, want [-1425 -1426]", signaled)
	}
}
