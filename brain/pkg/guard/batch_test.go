package guard

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestUnscopedTestRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cmd      string
		wantDeny bool
	}{
		{"go test ./...", true},
		{"go test brain/...", true},
		{"go test", true},
		{"go test -v", true},
		{"go test -v ./...", true},
		{"pytest", true},
		{"python3 -m pytest", true},
		{"python -m pytest -v", true},
		{"./scripts/verify.sh", true},
		{"scripts/verify.sh", true},
		// Scoped commands allowed:
		{"go test -v ./pkg/guard/...", false},
		{"go test ./pkg/guard", false},
		{"./scripts/verify.sh --staged", false},
		{"./scripts/verify.sh -h", false},
		{"pytest tests/test_batch.py", false},
		{"ls -la", false},
		{"", false},
	}

	dir := t.TempDir()
	now := time.Now()

	for _, tt := range tests {
		payload := Payload{
			ToolCall: ToolCall{
				Name: "run_command",
			},
		}
		args := ToolArgs{
			CommandLine: tt.cmd,
		}

		dec := CheckBatchWithDir(payload, args, dir, now)
		if tt.wantDeny && dec.Decision != DecisionDeny {
			t.Errorf("cmd %q: expected deny, got: %v", tt.cmd, dec)
		}
		if !tt.wantDeny && dec.Decision != DecisionAllow {
			t.Errorf("cmd %q: expected allow, got: %v", tt.cmd, dec)
		}
	}
}

func TestUnbatchedGitMutationRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cmd      string
		wantDeny bool
	}{
		{"git add .", true},
		{"git add src/main.go", true},
		{"git commit -m 'feat: update'", true},
		{"git commit", true},
		// Chained commands allowed:
		{"git add . && git commit -m 'feat' && git push", false},
		{"git add . ; git commit -m 'feat'", false},
		{"git commit -m 'feat' || echo 'failed'", false},
		{"git commit -m 'feat' | tee log.txt", false},
		// Read-only / PR scripts allowed:
		{"git status", false},
		{"git diff", false},
		{"git log -n 5", false},
		{"git branch -a", false},
		{"scripts/aerial-pr.sh submit /path 'msg'", false},
		{"", false},
	}

	dir := t.TempDir()
	now := time.Now()

	for _, tt := range tests {
		payload := Payload{
			ToolCall: ToolCall{
				Name: "run_command",
			},
		}
		args := ToolArgs{
			CommandLine: tt.cmd,
		}

		dec := CheckBatchWithDir(payload, args, dir, now)
		if tt.wantDeny && dec.Decision != DecisionDeny {
			t.Errorf("cmd %q: expected deny, got: %v", tt.cmd, dec)
		}
		if !tt.wantDeny && dec.Decision != DecisionAllow {
			t.Errorf("cmd %q: expected allow, got: %v", tt.cmd, dec)
		}
	}
}

func TestMicroCommandInspectionThrashing(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	now := time.Now()
	convID := "test-conv-inspection"

	// 3 single inspection commands across 3 distinct steps
	stepCommands := []string{"ls -la", "cat /data/file1.txt", "head -n 20 /data/file2.txt"}
	for i, cmd := range stepCommands {
		p := Payload{
			ConversationID: convID,
			StepIdx:        i + 1,
			ToolCall:       ToolCall{Name: "run_command"},
		}
		d := CheckBatchWithDir(p, ToolArgs{CommandLine: cmd}, dir, now)
		if d.Decision != DecisionAllow {
			t.Fatalf("step %d (%s) expected allow, got %v", i+1, cmd, d)
		}
	}

	// 4th single inspection in step 4 should be denied
	p4 := Payload{
		ConversationID: convID,
		StepIdx:        4,
		ToolCall:       ToolCall{Name: "run_command"},
	}
	d4 := CheckBatchWithDir(p4, ToolArgs{CommandLine: "find . -name '*.go'"}, dir, now)
	if d4.Decision != DecisionDeny {
		t.Fatalf("expected step 4 single inspection to be denied, got %v", d4)
	}

	// After denial, recovery step with chained command or scratch script should be allowed
	p5 := Payload{
		ConversationID: convID,
		StepIdx:        5,
		ToolCall:       ToolCall{Name: "run_command"},
	}
	d5 := CheckBatchWithDir(p5, ToolArgs{CommandLine: "ls -la && cat /data/file1.txt"}, dir, now)
	if d5.Decision != DecisionAllow {
		t.Fatalf("expected chained command in step 5 to be allowed, got %v", d5)
	}
}

func TestMicroReadThrashingSameFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	now := time.Now()
	convID := "test-conv-read-thrash"
	target := "/data/main.go"

	// Step 1: read main.go
	d1 := CheckBatchWithDir(Payload{
		ConversationID: convID,
		StepIdx:        1,
		ToolCall:       ToolCall{Name: "view_file"},
	}, ToolArgs{AbsolutePath: target}, dir, now)
	if d1.Decision != DecisionAllow {
		t.Fatalf("step 1 expected allow, got %v", d1)
	}

	// Step 2: read main.go again
	d2 := CheckBatchWithDir(Payload{
		ConversationID: convID,
		StepIdx:        2,
		ToolCall:       ToolCall{Name: "view_file"},
	}, ToolArgs{AbsolutePath: target}, dir, now)
	if d2.Decision != DecisionAllow {
		t.Fatalf("step 2 expected allow, got %v", d2)
	}

	// Step 3: read main.go a 3rd time (should be denied)
	d3 := CheckBatchWithDir(Payload{
		ConversationID: convID,
		StepIdx:        3,
		ToolCall:       ToolCall{Name: "view_file"},
	}, ToolArgs{AbsolutePath: target}, dir, now)
	if d3.Decision != DecisionDeny {
		t.Fatalf("step 3 expected deny for same file micro-read thrash, got %v", d3)
	}

	// Step 4: reading a DIFFERENT file should be allowed
	d4 := CheckBatchWithDir(Payload{
		ConversationID: convID,
		StepIdx:        4,
		ToolCall:       ToolCall{Name: "view_file"},
	}, ToolArgs{AbsolutePath: "/data/other.go"}, dir, now)
	if d4.Decision != DecisionAllow {
		t.Fatalf("step 4 reading different file expected allow, got %v", d4)
	}
}

func TestSequentialSingleFileReadsAcrossTurns(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	now := time.Now()
	convID := "test-conv-sequential-reads"

	// Step 1: 1 read
	d1 := CheckBatchWithDir(Payload{
		ConversationID: convID,
		StepIdx:        1,
		ToolCall:       ToolCall{Name: "view_file"},
	}, ToolArgs{AbsolutePath: "/data/f1.go"}, dir, now)
	if d1.Decision != DecisionAllow {
		t.Fatalf("step 1 expected allow, got %v", d1)
	}

	// Step 2: 1 read
	d2 := CheckBatchWithDir(Payload{
		ConversationID: convID,
		StepIdx:        2,
		ToolCall:       ToolCall{Name: "view_file"},
	}, ToolArgs{AbsolutePath: "/data/f2.go"}, dir, now)
	if d2.Decision != DecisionAllow {
		t.Fatalf("step 2 expected allow, got %v", d2)
	}

	// Step 3: 1 read (3rd consecutive turn with single read -> should be denied)
	d3 := CheckBatchWithDir(Payload{
		ConversationID: convID,
		StepIdx:        3,
		ToolCall:       ToolCall{Name: "view_file"},
	}, ToolArgs{AbsolutePath: "/data/f3.go"}, dir, now)
	if d3.Decision != DecisionDeny {
		t.Fatalf("step 3 expected deny, got %v", d3)
	}

	// Step 4: Now agent batches 3 reads in the SAME turn (same StepIdx: 4)
	// All calls sharing StepIdx: 4 should be allowed!
	pBatch1 := Payload{
		ConversationID: convID,
		StepIdx:        4,
		ToolCall:       ToolCall{Name: "view_file"},
	}
	pBatch2 := Payload{
		ConversationID: convID,
		StepIdx:        4,
		ToolCall:       ToolCall{Name: "view_file"},
	}
	pBatch3 := Payload{
		ConversationID: convID,
		StepIdx:        4,
		ToolCall:       ToolCall{Name: "view_file"},
	}

	if b1 := CheckBatchWithDir(pBatch1, ToolArgs{AbsolutePath: "/data/a.go"}, dir, now); b1.Decision != DecisionAllow {
		t.Fatalf("batch call 1 expected allow, got %v", b1)
	}
	if b2 := CheckBatchWithDir(pBatch2, ToolArgs{AbsolutePath: "/data/b.go"}, dir, now); b2.Decision != DecisionAllow {
		t.Fatalf("batch call 2 expected allow, got %v", b2)
	}
	if b3 := CheckBatchWithDir(pBatch3, ToolArgs{AbsolutePath: "/data/c.go"}, dir, now); b3.Decision != DecisionAllow {
		t.Fatalf("batch call 3 expected allow, got %v", b3)
	}
}

func TestMicroEditThrashingSameFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	now := time.Now()
	convID := "test-conv-edit-thrash"
	target := "/data/main.go"

	// Step 1: edit main.go
	d1 := CheckBatchWithDir(Payload{
		ConversationID: convID,
		StepIdx:        1,
		ToolCall:       ToolCall{Name: "replace_file_content"},
	}, ToolArgs{TargetFile: target}, dir, now)
	if d1.Decision != DecisionAllow {
		t.Fatalf("step 1 expected allow, got %v", d1)
	}

	// Step 2: edit main.go again
	d2 := CheckBatchWithDir(Payload{
		ConversationID: convID,
		StepIdx:        2,
		ToolCall:       ToolCall{Name: "replace_file_content"},
	}, ToolArgs{TargetFile: target}, dir, now)
	if d2.Decision != DecisionAllow {
		t.Fatalf("step 2 expected allow, got %v", d2)
	}

	// Step 3: edit main.go a 3rd time (should be denied)
	d3 := CheckBatchWithDir(Payload{
		ConversationID: convID,
		StepIdx:        3,
		ToolCall:       ToolCall{Name: "replace_file_content"},
	}, ToolArgs{TargetFile: target}, dir, now)
	if d3.Decision != DecisionDeny {
		t.Fatalf("step 3 expected deny for same file micro-edit thrash, got %v", d3)
	}

	// Step 4: editing a different file should be allowed
	d4 := CheckBatchWithDir(Payload{
		ConversationID: convID,
		StepIdx:        4,
		ToolCall:       ToolCall{Name: "write_to_file"},
	}, ToolArgs{TargetFile: "/data/other.go"}, dir, now)
	if d4.Decision != DecisionAllow {
		t.Fatalf("step 4 editing different file expected allow, got %v", d4)
	}
}

func TestStateFileCorruptionRecovery(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	now := time.Now()
	convID := "corrupted-session"

	// Pre-create corrupt state file
	statePath := filepath.Join(dir, fmt.Sprintf("state-%s.json", convID))
	if err := os.WriteFile(statePath, []byte(`{invalid-json`), 0600); err != nil {
		t.Fatalf("failed to write corrupt file: %v", err)
	}

	payload := Payload{
		ConversationID: convID,
		StepIdx:        1,
		ToolCall:       ToolCall{Name: "view_file"},
	}
	dec := CheckBatchWithDir(payload, ToolArgs{AbsolutePath: "/data/test.go"}, dir, now)
	if dec.Decision != DecisionAllow {
		t.Fatalf("expected allow on corrupt state recovery, got %v", dec)
	}

	// Subsequent call should work cleanly with recovered state
	payload2 := Payload{
		ConversationID: convID,
		StepIdx:        2,
		ToolCall:       ToolCall{Name: "view_file"},
	}
	dec2 := CheckBatchWithDir(payload2, ToolArgs{AbsolutePath: "/data/test2.go"}, dir, now)
	if dec2.Decision != DecisionAllow {
		t.Fatalf("expected allow on subsequent call, got %v", dec2)
	}
}

func TestConcurrentStateAccess(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	now := time.Now()
	convID := "concurrent-session"

	const workers = 10
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func(workerID int) {
			defer wg.Done()
			payload := Payload{
				ConversationID: convID,
				StepIdx:        workerID + 1,
				ToolCall:       ToolCall{Name: "view_file"},
			}
			args := ToolArgs{AbsolutePath: fmt.Sprintf("/data/file-%d.go", workerID)}
			dec := CheckBatchWithDir(payload, args, dir, now)
			if dec == nil {
				t.Errorf("worker %d got nil decision", workerID)
			}
		}(i)
	}

	wg.Wait()
}

func TestOpportunisticGC(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	now := time.Now()

	oldFile := filepath.Join(dir, "state-old.json")
	if err := os.WriteFile(oldFile, []byte(`{}`), 0600); err != nil {
		t.Fatalf("failed to write old file: %v", err)
	}
	// Set mtime to 2 hours ago
	oldTime := now.Add(-2 * time.Hour)
	if err := os.Chtimes(oldFile, oldTime, oldTime); err != nil {
		t.Fatalf("failed to chtimes: %v", err)
	}

	newFile := filepath.Join(dir, "state-new.json")
	if err := os.WriteFile(newFile, []byte(`{}`), 0600); err != nil {
		t.Fatalf("failed to write new file: %v", err)
	}

	// Trigger pruneStateFiles directly
	pruneStateFiles(dir, now)

	if _, err := os.Stat(oldFile); !os.IsNotExist(err) {
		t.Errorf("expected old state file to be purged, but it still exists")
	}
	if _, err := os.Stat(newFile); err != nil {
		t.Errorf("expected new state file to remain, but got error: %v", err)
	}

	// Trigger opportunisticGC to verify rate-limited wrapper executes cleanly
	opportunisticGC(dir, now)
}

func TestSanitizeID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{"", "default"},
		{"   ", "default"},
		{"session-123_abc", "session-123_abc"},
		{"session/../../../etc/passwd", "sessionetcpasswd"},
		{"!@#$%^&*()", "default"},
	}

	for _, tt := range tests {
		got := sanitizeID(tt.input)
		if got != tt.want {
			t.Errorf("sanitizeID(%q) = %q; want %q", tt.input, got, tt.want)
		}
	}
}

func TestCheckBatchDefault(t *testing.T) {
	SetBatchDir(t.TempDir())
	defer SetBatchDir(DefaultBatchDir)

	payload := Payload{
		ToolCall: ToolCall{Name: "view_file"},
	}
	dec := CheckBatch(payload, ToolArgs{AbsolutePath: "/tmp/foo.txt"})
	if dec == nil || dec.Decision != DecisionAllow {
		t.Fatalf("expected allow from CheckBatch, got %v", dec)
	}
}

func TestGetBatchDirEnv(t *testing.T) {
	// Cannot use t.Parallel() with t.Setenv()
	t.Setenv("AERIAL_BATCH_GUARD_DIR", "/tmp/custom-batch-guard-dir")
	if dir := GetBatchDir(); dir != "/tmp/custom-batch-guard-dir" {
		t.Errorf("GetBatchDir() = %q, want /tmp/custom-batch-guard-dir", dir)
	}
}

func TestBatchEdgeCases(t *testing.T) {
	t.Parallel()

	// 2. isSingleInspection edge cases
	if isSingleInspection("") {
		t.Errorf("expected false on empty command")
	}
	if isSingleInspection("   ") {
		t.Errorf("expected false on whitespace command")
	}

	// 3. categorizeInvocation branches
	catTest, _ := categorizeInvocation("run_command", ToolArgs{CommandLine: "go test ./..."})
	if catTest != "unscoped_test" {
		t.Errorf("expected unscoped_test, got %q", catTest)
	}
	catOther, tgtOther := categorizeInvocation("custom_mcp_tool", ToolArgs{})
	if catOther != "other" || tgtOther != "custom_mcp_tool" {
		t.Errorf("expected other / custom_mcp_tool, got %q / %q", catOther, tgtOther)
	}

	// 4. isSameStep timestamp inversion
	rec := ToolInvocation{Timestamp: 100, StepIdx: 0}
	if !isSameStep(rec, 0, 99) {
		t.Errorf("expected true for within 1s diff")
	}
	if isSameStep(rec, 0, 50) {
		t.Errorf("expected false for diff > 1s")
	}

	// 5. groupSteps edge cases
	if steps := groupSteps(nil); steps != nil {
		t.Errorf("expected nil for empty records")
	}
	invs := []ToolInvocation{
		{Timestamp: 100, StepIdx: 0},
		{Timestamp: 99, StepIdx: 0}, // diff < 0
		{Timestamp: 50, StepIdx: 0}, // distinct step
	}
	steps := groupSteps(invs)
	if len(steps) != 2 {
		t.Errorf("expected 2 groups, got %d", len(steps))
	}

	// 6. Max records truncation in CheckBatchWithDir
	dir := t.TempDir()
	now := time.Now()
	convID := "max-records-session"
	for i := 0; i < 35; i++ {
		p := Payload{
			ConversationID: convID,
			StepIdx:        i + 1,
			ToolCall:       ToolCall{Name: "run_command"},
		}
		// Alternating non-inspection commands
		cmd := fmt.Sprintf("echo record-%d && true", i)
		d := CheckBatchWithDir(p, ToolArgs{CommandLine: cmd}, dir, now)
		if d.Decision != DecisionAllow {
			t.Fatalf("call %d expected allow, got %v", i, d)
		}
	}
	// Verify state file records count is clamped to DefaultMaxRecords (30)
	st := loadState(dir, convID, now)
	if len(st.Records) > DefaultMaxRecords {
		t.Errorf("records length = %d; want <= %d", len(st.Records), DefaultMaxRecords)
	}

	// 7. Fail-open with invalid directory
	dFail := CheckBatchWithDir(Payload{ToolCall: ToolCall{Name: "view_file"}}, ToolArgs{AbsolutePath: "/a.go"}, "/dev/null/impossible", now)
	if dFail.Decision != DecisionAllow {
		t.Errorf("expected allow on uncreatable directory, got %v", dFail)
	}

	// 8. pruneStateFiles directory skipping
	gcDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(gcDir, "subdir"), 0755); err != nil {
		t.Fatalf("failed to create subdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(gcDir, "non-state.txt"), []byte("hi"), 0600); err != nil {
		t.Fatalf("failed to write non-state file: %v", err)
	}
	pruneStateFiles(gcDir, now)
}
