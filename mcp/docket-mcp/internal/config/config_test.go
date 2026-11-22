package config

import (
	"cmp"
	"context"
	"errors"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/ollama"
)

func int64Ptr(v int64) *int64 {
	p := new(int64)
	*p = v
	return p
}

func TestResolveTaskStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		done       bool
		resolution string
		want       string
	}{
		{"open when done is false with empty resolution", false, "", "open"},
		{"open when done is false even if resolution is completed", false, "completed", "open"},
		{"completed when done is true and resolution is empty", true, "", "completed"},
		{"completed when done is true and resolution is completed", true, "completed", "completed"},
		{"wont_do when done is true and resolution is wont_do", true, "wont_do", "wont_do"},
		{"wont_do with surrounding whitespace", true, "  wont_do  ", "wont_do"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ResolveTaskStatus(tc.done, tc.resolution)
			if got != tc.want {
				t.Fatalf("ResolveTaskStatus(%v, %q) = %q; want %q", tc.done, tc.resolution, got, tc.want)
			}
		})
	}
}

// mapNodeSource is an in-memory NodeSource; absent ids report ok=false.
type mapNodeSource map[int64]TaskNode

func (m mapNodeSource) Node(_ context.Context, id int64) (TaskNode, bool, error) {
	n, ok := m[id]
	return n, ok, nil
}

// failingNodeSource fails every fetch, proving a code path fetches nothing when it succeeds.
type failingNodeSource struct{}

func (failingNodeSource) Node(_ context.Context, id int64) (TaskNode, bool, error) {
	return TaskNode{}, false, errors.New("fetch failed")
}

func cycleGraph() mapNodeSource {
	return mapNodeSource{
		1:  {ID: 1, SubtaskIDs: []int64{2, 3}, Blocks: []int64{4, 8}},
		2:  {ID: 2, ParentID: int64Ptr(1), SubtaskIDs: []int64{5}},
		3:  {ID: 3, ParentID: int64Ptr(1)},
		4:  {ID: 4, BlockedBy: []int64{1}, Blocks: []int64{6}},
		5:  {ID: 5, ParentID: int64Ptr(2)},
		6:  {ID: 6, BlockedBy: []int64{4, 8}},
		7:  {ID: 7},
		8:  {ID: 8, BlockedBy: []int64{1}, Blocks: []int64{6}},
		10: {ID: 10, ParentID: int64Ptr(11)},
		11: {ID: 11, ParentID: int64Ptr(12)},
		12: {ID: 12, ParentID: int64Ptr(13)},
		13: {ID: 13},
		20: {ID: 20, ParentID: int64Ptr(21)},
		21: {ID: 21, ParentID: int64Ptr(20)},
		30: {ID: 30, Blocks: []int64{99}},
	}
}

func TestDetectGraphCycle(t *testing.T) {
	t.Parallel()

	nodes := cycleGraph()

	tests := []struct {
		name     string
		src      NodeSource
		sourceID int64
		targetID int64
		edgeType string
		wantErr  string
	}{
		{name: "self-link rejected", src: nodes, sourceID: 1, targetID: 1, edgeType: "parent", wantErr: "self-link cycle: task 1 cannot link to itself"},
		{name: "valid parent link", src: nodes, sourceID: 7, targetID: 1, edgeType: "parent"},
		{name: "parent cycle direct child linking to parent", src: nodes, sourceID: 1, targetID: 2, edgeType: "parent", wantErr: "cycle detected: task 1 is already an ancestor of task 2"},
		{name: "parent cycle deep descendant linking to ancestor", src: nodes, sourceID: 1, targetID: 5, edgeType: "parent", wantErr: "cycle detected: task 1 is already an ancestor of task 5"},
		{name: "parent cycle through multi-level chain", src: nodes, sourceID: 13, targetID: 10, edgeType: "parent", wantErr: "cycle detected: task 13 is already an ancestor of task 10"},
		{name: "parent link to sibling subtree is valid", src: nodes, sourceID: 3, targetID: 5, edgeType: "parent"},
		{name: "parent walk terminates on corrupt parent loop", src: nodes, sourceID: 7, targetID: 20, edgeType: "parent"},
		{name: "parent target missing from source", src: nodes, sourceID: 1, targetID: 99, edgeType: "parent"},
		{name: "parent fetch failure propagates", src: failingNodeSource{}, sourceID: 1, targetID: 2, edgeType: "parent", wantErr: "fetch failed"},
		{name: "valid blocks link", src: nodes, sourceID: 7, targetID: 1, edgeType: "blocks"},
		{name: "blocks cycle direct", src: nodes, sourceID: 4, targetID: 1, edgeType: "blocks", wantErr: "cycle detected: task 1 already transitively blocks task 4"},
		{name: "blocks cycle through diamond", src: nodes, sourceID: 6, targetID: 1, edgeType: "blocks", wantErr: "cycle detected: task 1 already transitively blocks task 6"},
		{name: "blocks walk past node missing from source", src: nodes, sourceID: 7, targetID: 30, edgeType: "blocks"},
		{name: "blocks fetch failure propagates", src: failingNodeSource{}, sourceID: 4, targetID: 1, edgeType: "blocks", wantErr: "fetch failed"},
		{name: "related never fetches", src: failingNodeSource{}, sourceID: 1, targetID: 6, edgeType: "related"},
		{name: "unknown edge type rejected", src: nodes, sourceID: 1, targetID: 7, edgeType: "invalid_edge", wantErr: `unknown edge type: "invalid_edge"`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertErrString(t, DetectGraphCycle(context.Background(), tc.src, tc.sourceID, tc.targetID, tc.edgeType), tc.wantErr)
		})
	}
}

func TestDetectCreateCycle(t *testing.T) {
	t.Parallel()

	nodes := cycleGraph()

	tests := []struct {
		name      string
		src       NodeSource
		blocks    []int64
		blockedBy []int64
		wantErr   string
	}{
		{name: "no relations", src: failingNodeSource{}},
		{name: "blocks only never fetches", src: failingNodeSource{}, blocks: []int64{1}},
		{name: "blocked_by only never fetches", src: failingNodeSource{}, blockedBy: []int64{1}},
		{name: "unrelated blocks and blockers", src: nodes, blocks: []int64{6}, blockedBy: []int64{7}},
		{name: "blocked task directly blocks blocker", src: nodes, blocks: []int64{4}, blockedBy: []int64{6}, wantErr: "cycle detected: task 4 already transitively blocks task 6"},
		{name: "blocked task transitively blocks blocker through diamond", src: nodes, blocks: []int64{7, 1}, blockedBy: []int64{3, 6}, wantErr: "cycle detected: task 1 already transitively blocks task 6"},
		{name: "same id in both lists", src: nodes, blocks: []int64{7}, blockedBy: []int64{7}, wantErr: "cycle detected: task 7 already transitively blocks task 7"},
		{name: "blocker reachable only against edge direction", src: nodes, blocks: []int64{6}, blockedBy: []int64{1}},
		{name: "fetch failure propagates", src: failingNodeSource{}, blocks: []int64{1}, blockedBy: []int64{6}, wantErr: "fetch failed"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertErrString(t, DetectCreateCycle(context.Background(), tc.src, tc.blocks, tc.blockedBy), tc.wantErr)
		})
	}
}

func TestLoadCloseNeighborhood(t *testing.T) {
	t.Parallel()

	nodes := mapNodeSource{
		1: {ID: 1, SubtaskIDs: []int64{2}, BlockedBy: []int64{9}},
		2: {ID: 2, ParentID: int64Ptr(1), SubtaskIDs: []int64{3, 404}, Blocks: []int64{4, 405}},
		3: {ID: 3, ParentID: int64Ptr(2)},
		4: {ID: 4, BlockedBy: []int64{2, 5}},
		5: {ID: 5, BlockedBy: []int64{6}},
		6: {ID: 6},
		7: {ID: 7, ParentID: int64Ptr(8)},
		8: {ID: 8, SubtaskIDs: []int64{7}},
		9: {ID: 9, BlockedBy: []int64{10}},
	}

	tests := []struct {
		name    string
		src     NodeSource
		id      int64
		wantIDs []int64
		wantErr string
	}{
		{name: "subtree plus blockers and co-blockers of blocked tasks", src: nodes, id: 1, wantIDs: []int64{1, 2, 3, 4, 5, 9}},
		{name: "leaf excludes parent and siblings", src: nodes, id: 7, wantIDs: []int64{7}},
		{name: "missing task reports not found", src: nodes, id: 99, wantErr: "task 99 not found in graph"},
		{name: "fetch failure propagates", src: failingNodeSource{}, id: 1, wantErr: "fetch failed"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := LoadCloseNeighborhood(context.Background(), tc.src, tc.id)
			assertErrString(t, err, tc.wantErr)
			gotIDs := slices.Sorted(maps.Keys(got))
			if !slices.Equal(gotIDs, tc.wantIDs) {
				t.Fatalf("neighborhood ids = %v, want %v", gotIDs, tc.wantIDs)
			}
		})
	}
}

func TestHydrateTask(t *testing.T) {
	t.Parallel()

	created := time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC)
	doneAt := time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC)
	full := VikunjaTask{
		ID: 10, Title: "Root", Description: "desc", Done: true, DoneAt: &doneAt, Created: created, Updated: doneAt, ProjectID: 1,
		RelatedTasks: map[string][]VikunjaTask{
			"parenttask": {{ID: 1, Title: "Parent"}, {ID: 2, Title: "Second Parent"}},
			"subtask":    {{ID: 12, Title: "Sub B", Done: true}, {ID: 11, Title: "Sub A"}},
			"blocking":   {{ID: 20, Title: "Blocked"}},
			"blocked":    {{ID: 30, Title: "Blocker", Done: true}},
			"related":    {{ID: 40, Title: "Rel"}},
		},
	}
	comment := VikunjaComment{ID: 7, Comment: "hi", Created: created}
	comment.Author.Username = "aerial"

	parentOfRoot := int64(1)
	root := int64(10)
	neighbors := map[int64]TaskNode{
		1:  {ID: 1, Title: "Parent", Status: "open", SubtaskIDs: []int64{10}},
		12: {ID: 12, Title: "Sub B", Status: "wont_do", ParentID: &root, Blocks: []int64{20}},
		11: {ID: 11, Title: "Sub A", Status: "open", ParentID: &root},
		20: {ID: 20, Title: "Blocked", Status: "open", BlockedBy: []int64{10, 12}},
		30: {ID: 30, Title: "Blocker", Status: "completed", ParentID: &parentOfRoot, Blocks: []int64{10}},
	}

	tests := []struct {
		name      string
		task      VikunjaTask
		status    string
		comments  []VikunjaComment
		neighbors map[int64]TaskNode
		want      HydratedTask
	}{
		{
			name:      "present neighbors get full summaries and absent ones fall back to embedded rows",
			task:      full,
			status:    "wont_do",
			comments:  []VikunjaComment{comment},
			neighbors: neighbors,
			want: HydratedTask{
				ID: 10, Title: "Root", Status: "wont_do", Description: "desc", CreatedAt: created, UpdatedAt: doneAt, ClosedAt: &doneAt,
				Parent:    &TaskSummary{ID: 1, Title: "Parent", Status: "open"},
				Subtasks:  []TaskSummary{{ID: 12, Title: "Sub B", Status: "wont_do", ParentID: &root, Blocks: []int64{20}}, {ID: 11, Title: "Sub A", Status: "open", ParentID: &root}},
				Blocks:    []TaskSummary{{ID: 20, Title: "Blocked", Status: "open", BlockedBy: []int64{10, 12}}},
				BlockedBy: []TaskSummary{{ID: 30, Title: "Blocker", Status: "completed", ParentID: &parentOfRoot, Blocks: []int64{10}}},
				Related:   []TaskSummary{{ID: 40, Title: "Rel", Status: "open"}},
				Comments:  []TaskComment{{ID: 7, Author: "aerial", Comment: "hi", CreatedAt: created}},
			},
		},
		{
			name:   "no neighbor nodes falls back to embedded rows with done status",
			task:   full,
			status: "completed",
			want: HydratedTask{
				ID: 10, Title: "Root", Status: "completed", Description: "desc", CreatedAt: created, UpdatedAt: doneAt, ClosedAt: &doneAt,
				Parent:    &TaskSummary{ID: 1, Title: "Parent", Status: "open"},
				Subtasks:  []TaskSummary{{ID: 12, Title: "Sub B", Status: "completed"}, {ID: 11, Title: "Sub A", Status: "open"}},
				Blocks:    []TaskSummary{{ID: 20, Title: "Blocked", Status: "open"}},
				BlockedBy: []TaskSummary{{ID: 30, Title: "Blocker", Status: "completed"}},
				Related:   []TaskSummary{{ID: 40, Title: "Rel", Status: "open"}},
			},
		},
		{
			name:   "task without relations or comments",
			task:   VikunjaTask{ID: 5, Title: "Lone", Created: created, Updated: created},
			status: "open",
			want:   HydratedTask{ID: 5, Title: "Lone", Status: "open", CreatedAt: created, UpdatedAt: created},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := HydrateTask(tc.task, tc.status, tc.comments, tc.neighbors)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("HydrateTask() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func assertErrString(t *testing.T, err error, wantErr string) {
	t.Helper()
	if wantErr == "" {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return
	}
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("error = %v, want substring %q", err, wantErr)
	}
}

// renderReadyTree flattens a ready tree to "id(sub sub(subsub)) id" so a row
// asserts every level, including an empty tree.
func renderReadyTree(tasks []ReadyTask) string {
	parts := make([]string, 0, len(tasks))
	for _, task := range tasks {
		s := strconv.FormatInt(task.ID, 10)
		if len(task.ReadySubtasks) > 0 {
			s += "(" + renderReadyTree(task.ReadySubtasks) + ")"
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, " ")
}

func TestBuildReadyTree(t *testing.T) {
	t.Parallel()

	nodes := map[int64]TaskNode{
		1:  {ID: 1, Title: "Top Ready 1", Status: "open", SubtaskIDs: []int64{2, 11, 3, 9999}},
		2:  {ID: 2, Title: "Sub Ready 2", Status: "open", ParentID: int64Ptr(1), SubtaskIDs: []int64{9}},
		11: {ID: 11, Title: "Sub Ready 11", Status: "open", ParentID: int64Ptr(1)},
		3:  {ID: 3, Title: "Sub Blocked 3", Status: "open", ParentID: int64Ptr(1), BlockedBy: []int64{4}},
		4:  {ID: 4, Title: "Blocker Open 4", Status: "open"},
		5:  {ID: 5, Title: "Top Blocked 5", Status: "open", BlockedBy: []int64{4}},
		6:  {ID: 6, Title: "Top Completed 6", Status: "completed"},
		7:  {ID: 7, Title: "Top Ready with Closed Blocker 7", Status: "open", BlockedBy: []int64{6}},
		8:  {ID: 8, Title: "Top with Nonexistent Parent", Status: "open", ParentID: int64Ptr(999)},
		9:  {ID: 9, Title: "Deep Sub Ready 9", Status: "open", ParentID: int64Ptr(2)},
		10: {ID: 10, Title: "Done Subtask 10", Status: "completed", ParentID: int64Ptr(1)},
		12: {ID: 12, Title: "Top WontDo Blocker 12", Status: "wont_do"},
		13: {ID: 13, Title: "Top Blocked by WontDo 13", Status: "open", BlockedBy: []int64{12}},
	}

	tests := []struct {
		name           string
		scopedParentID *int64
		want           string
	}{
		{"top level ready tree", nil, "1(2(9) 11) 4 7 8"},
		{"scoped parent 1 ready tree", int64Ptr(1), "2(9) 11"},
		{"scoped parent with no ready subtasks", int64Ptr(7), ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := renderReadyTree(BuildReadyTree(nodes, tc.scopedParentID)); got != tc.want {
				t.Fatalf("BuildReadyTree = %q; want %q", got, tc.want)
			}
		})
	}
}

func TestValidateCloseGuard(t *testing.T) {
	t.Parallel()

	nodes := map[int64]TaskNode{
		1:  {ID: 1, Status: "open", BlockedBy: []int64{2}, SubtaskIDs: []int64{3}},
		2:  {ID: 2, Status: "open"},
		3:  {ID: 3, Status: "open"},
		4:  {ID: 4, Status: "open", BlockedBy: []int64{5}, SubtaskIDs: []int64{6}},
		5:  {ID: 5, Status: "completed"},
		6:  {ID: 6, Status: "wont_do"},
		7:  {ID: 7, Status: "open", BlockedBy: []int64{2}},
		8:  {ID: 8, Status: "open", SubtaskIDs: []int64{3}},
		9:  {ID: 9, Status: "open", Blocks: []int64{10}},
		10: {ID: 10, Status: "open"},
		11: {ID: 11, Status: "open", SubtaskIDs: []int64{12}},
		12: {ID: 12, Status: "open", Blocks: []int64{13}},
		13: {ID: 13, Status: "open"},
		14: {ID: 14, Status: "open", Blocks: []int64{15, 16, 17}, SubtaskIDs: []int64{17}},
		15: {ID: 15, Status: "completed"},
		16: {ID: 16, Status: "wont_do"},
		17: {ID: 17, Status: "open"},
	}

	tests := []struct {
		name       string
		id         int64
		resolution string
		wantErr    string
	}{
		{"wont_do allows close even with open blockers and subtasks", 1, "wont_do", ""},
		{"wont_do rejected when target task blocks open task", 9, "wont_do", "cannot close task 9 as wont_do: blocks open tasks: [10]"},
		{"wont_do rejected when cascaded subtask blocks external open task", 11, "wont_do", "cannot close task 11 as wont_do: blocks open tasks: [13]"},
		{"wont_do allowed when blocked tasks are closed or in same cascaded subtree", 14, "wont_do", ""},
		{"wont_do on nonexistent task returns error", 999, "wont_do", "task 999 not found in graph"},
		{"completed allowed when all blockers and subtasks are closed", 4, "completed", ""},
		{"completed rejected when blocker is open", 7, "completed", "cannot complete task 7: open blockers: [2], open subtasks: []"},
		{"completed rejected when subtask is open", 8, "completed", "cannot complete task 8: open blockers: [], open subtasks: [3]"},
		{"completed rejected when both are open", 1, "completed", "cannot complete task 1: open blockers: [2], open subtasks: [3]"},
		{"task not found in graph returns error", 999, "completed", "task 999 not found in graph"},
		{"invalid resolution string rejected", 4, "invalid_res", `invalid resolution: "invalid_res" (must be 'completed' or 'wont_do')`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertErrString(t, ValidateCloseGuard(nodes, tc.id, tc.resolution), tc.wantErr)
		})
	}
}

func TestCollectOpenSubtreeIDs(t *testing.T) {
	t.Parallel()

	nodes := map[int64]TaskNode{
		1: {ID: 1, SubtaskIDs: []int64{2, 3}},
		2: {ID: 2, Status: "open", SubtaskIDs: []int64{4, 5}},
		3: {ID: 3, Status: "completed", SubtaskIDs: []int64{4, 6}}, // Diamond: 2 and 3 both reference 4
		4: {ID: 4, Status: "open"},
		5: {ID: 5, Status: "wont_do"},
		6: {ID: 6, Status: "open"}, // Open descendant under closed node 3
		7: {ID: 7, Status: "open"}, // Unrelated
	}

	tests := []struct {
		name   string
		rootID int64
		want   []int64
	}{
		{"collect transitive open subtasks under root 1", 1, []int64{2, 4, 6}},
		{"leaf node has empty subtree", 4, []int64{}},
		{"nonexistent root ID returns empty slice", 999, []int64{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := CollectOpenSubtreeIDs(nodes, tc.rootID); !slices.Equal(got, tc.want) {
				t.Fatalf("CollectOpenSubtreeIDs(%d) = %v; want %v", tc.rootID, got, tc.want)
			}
		})
	}
}

func TestComputeUnblockedIDs(t *testing.T) {
	t.Parallel()

	before := map[int64]TaskNode{
		1: {ID: 1, Status: "open", Blocks: []int64{2, 3}},
		2: {ID: 2, Status: "open", BlockedBy: []int64{1}},
		3: {ID: 3, Status: "open", BlockedBy: []int64{1, 4}},
		4: {ID: 4, Status: "open", Blocks: []int64{3}},
		5: {ID: 5, Status: "open"}, // Already unblocked
	}

	after := map[int64]TaskNode{
		1: {ID: 1, Status: "completed", Blocks: []int64{2, 3}},
		2: {ID: 2, Status: "open", BlockedBy: []int64{1}},    // Now unblocked because 1 is closed
		3: {ID: 3, Status: "open", BlockedBy: []int64{1, 4}}, // Still blocked by 4
		4: {ID: 4, Status: "open", Blocks: []int64{3}},
		5: {ID: 5, Status: "open"},
		6: {ID: 6, Status: "completed"}, // Closed task in after
		7: {ID: 7, Status: "open"},      // New task not in before
	}

	afterWontDo := map[int64]TaskNode{
		1: {ID: 1, Status: "wont_do", Blocks: []int64{2, 3}},
		2: {ID: 2, Status: "open", BlockedBy: []int64{1}},
		3: {ID: 3, Status: "open", BlockedBy: []int64{1, 4}},
		4: {ID: 4, Status: "open", Blocks: []int64{3}},
		5: {ID: 5, Status: "open"},
	}

	tests := []struct {
		name      string
		after     map[int64]TaskNode
		closedIDs []int64
		want      []int64
	}{
		{"task 2 newly unblocked after task 1 completed", after, []int64{1}, []int64{2}},
		{"blocker transitions to wont_do leaves dependent blocked", afterWontDo, []int64{1}, []int64{}},
		{"no transitions when no dependencies change", before, []int64{}, []int64{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ComputeUnblockedIDs(before, tc.after, tc.closedIDs); !slices.Equal(got, tc.want) {
				t.Fatalf("ComputeUnblockedIDs = %v; want %v", got, tc.want)
			}
		})
	}
}

// loadConfigFileLayer is what testdata config.yaml sets in TestLoadConfig.
var loadConfigFileLayer = Config{Port: "9090", VikunjaURL: "http://custom-vikunja:3456", DefaultProjectID: 42}

// cfgWith returns LoadConfig's built-in defaults with the non-zero fields of
// each layer applied in order, the way the YAML file and env overrides stack.
func cfgWith(layers ...Config) *Config {
	cfg := &Config{
		Port:            "8080",
		VikunjaUsername: "aerial",
		VikunjaPassword: "aerial-password",
		Ollama:          ollama.Config{Model: "all-minilm", NumCtx: 512},
	}
	for _, l := range layers {
		cfg.Port = cmp.Or(l.Port, cfg.Port)
		cfg.VikunjaURL = cmp.Or(l.VikunjaURL, cfg.VikunjaURL)
		cfg.VikunjaUsername = cmp.Or(l.VikunjaUsername, cfg.VikunjaUsername)
		cfg.VikunjaPassword = cmp.Or(l.VikunjaPassword, cfg.VikunjaPassword)
		cfg.DefaultProjectID = cmp.Or(l.DefaultProjectID, cfg.DefaultProjectID)
		cfg.DatabaseURL = cmp.Or(l.DatabaseURL, cfg.DatabaseURL)
		cfg.Ollama.BaseURL = cmp.Or(l.Ollama.BaseURL, cfg.Ollama.BaseURL)
		cfg.Ollama.Model = cmp.Or(l.Ollama.Model, cfg.Ollama.Model)
	}
	return cfg
}

func TestLoadConfig(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	files := map[string]string{
		"config.yaml": "port: \"9090\"\nvikunja_url: \"http://custom-vikunja:3456\"\ndefault_project_id: 42\n",
		"bad.yaml":    "port: [unclosed",
		"empty.yaml":  "",
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(tempDir, name), []byte(body), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	file := loadConfigFileLayer

	// path is relative to tempDir ("" stays empty); env is one KEY=value pair.
	tests := []struct {
		name    string
		path    string
		env     string
		want    *Config
		wantErr string
	}{
		{"defaults applied when path is empty", "", "", cfgWith(), ""},
		{"empty file returns defaults", "empty.yaml", "", cfgWith(), ""},
		{"nonexistent file returns defaults", "nonexistent.yaml", "", cfgWith(), ""},
		{"valid yaml loaded successfully", "config.yaml", "", cfgWith(file), ""},
		{"invalid yaml returns error", "bad.yaml", "", nil, "failed to parse config"},
		{"directory path returns error", ".", "", nil, "config path is a directory"},
		{"stat error on file as directory returns error", "config.yaml/subfile.yaml", "", nil, "not a directory"},
		{"env PORT override", "config.yaml", "PORT=7777", cfgWith(file, Config{Port: "7777"}), ""},
		{"env DEFAULT_PROJECT_ID override", "config.yaml", "DEFAULT_PROJECT_ID=100", cfgWith(file, Config{DefaultProjectID: 100}), ""},
		{"env DEFAULT_PROJECT_ID non-positive ignored", "config.yaml", "DEFAULT_PROJECT_ID=-1", cfgWith(file), ""},
		{"env VIKUNJA_USERNAME override", "config.yaml", "VIKUNJA_USERNAME=testadmin", cfgWith(file, Config{VikunjaUsername: "testadmin"}), ""},
		{"env VIKUNJA_PASSWORD override", "config.yaml", "VIKUNJA_PASSWORD=supersecret", cfgWith(file, Config{VikunjaPassword: "supersecret"}), ""},
		{
			name: "env VIKUNJA_URL override", path: "config.yaml", env: "VIKUNJA_URL=http://remote-vikunja:8080",
			want: cfgWith(file, Config{VikunjaURL: "http://remote-vikunja:8080"}),
		},
		{
			name: "env POSTGRES_URL override", path: "config.yaml", env: "POSTGRES_URL=postgres://pguser:pgpass@pghost:5432/docket",
			want: cfgWith(file, Config{DatabaseURL: "postgres://pguser:pgpass@pghost:5432/docket"}),
		},
		{
			name: "env DATABASE_URL override", path: "config.yaml", env: "DATABASE_URL=postgres://dbuser:dbpass@dbhost:5432/docket",
			want: cfgWith(file, Config{DatabaseURL: "postgres://dbuser:dbpass@dbhost:5432/docket"}),
		},
		{
			name: "env POSTGRES_PASSWORD override", path: "config.yaml", env: "POSTGRES_PASSWORD=secretpass",
			want: cfgWith(file, Config{DatabaseURL: "postgres://aerial:secretpass@postgres:5432/docket?sslmode=disable"}),
		},
		{
			name: "env OLLAMA_BASE_URL override", path: "config.yaml", env: "OLLAMA_BASE_URL=http://ollama-remote:11434",
			want: cfgWith(file, Config{Ollama: ollama.Config{BaseURL: "http://ollama-remote:11434"}}),
		},
		{
			name: "env OLLAMA_MODEL override", path: "config.yaml", env: "OLLAMA_MODEL=nomic-embed-text",
			want: cfgWith(file, Config{Ollama: ollama.Config{Model: "nomic-embed-text"}}),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := tc.path
			if path != "" {
				path = filepath.Join(tempDir, path)
			}
			envKey, envVal, _ := strings.Cut(tc.env, "=")
			getenv := func(key string) string {
				if tc.env != "" && key == envKey {
					return envVal
				}
				return ""
			}
			got, err := LoadConfig(path, getenv)
			assertErrString(t, err, tc.wantErr)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("LoadConfig = %+v; want %+v", got, tc.want)
			}
		})
	}
}

func assertQueryParseTable[T any](t *testing.T, tests []struct {
	name    string
	raw     string
	want    T
	wantErr string
}, parseFn func(url.Values) (T, error),
) {
	t.Helper()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			values, err := url.ParseQuery(tc.raw)
			if err != nil {
				t.Fatalf("ParseQuery: %v", err)
			}
			got, err := parseFn(values)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestParseTaskListQuery(t *testing.T) {
	t.Parallel()

	assertQueryParseTable(t, []struct {
		name    string
		raw     string
		want    TaskListQuery
		wantErr string
	}{
		{name: "defaults to open", raw: "", want: TaskListQuery{Status: "open"}},
		{name: "completed accepted", raw: "status=completed", want: TaskListQuery{Status: "completed"}},
		{name: "wont_do accepted", raw: "status=wont_do", want: TaskListQuery{Status: "wont_do"}},
		{name: "all accepted", raw: "status=all", want: TaskListQuery{Status: "all"}},
		{name: "unknown status rejected", raw: "status=pending", wantErr: `invalid status "pending"`},
	}, ParseTaskListQuery)
}

func TestParseTaskSearchQuery(t *testing.T) {
	t.Parallel()

	assertQueryParseTable(t, []struct {
		name    string
		raw     string
		want    TaskSearchQuery
		wantErr string
	}{
		{name: "defaults status and limit", raw: "q=deploy", want: TaskSearchQuery{Query: "deploy", Status: "open", Limit: 20}},
		{name: "explicit status and limit", raw: "q=deploy&status=all&limit=5", want: TaskSearchQuery{Query: "deploy", Status: "all", Limit: 5}},
		{name: "limit clamped to 50", raw: "q=deploy&limit=500", want: TaskSearchQuery{Query: "deploy", Status: "open", Limit: 50}},
		{name: "limit of one accepted", raw: "q=x&limit=1", want: TaskSearchQuery{Query: "x", Status: "open", Limit: 1}},
		{name: "missing q rejected", raw: "status=open", wantErr: "parameter 'q' is required"},
		{name: "blank q rejected", raw: "q=%20%20", wantErr: "parameter 'q' is required"},
		{name: "bad status rejected", raw: "q=x&status=nope", wantErr: `invalid status "nope"`},
		{name: "non-numeric limit rejected", raw: "q=x&limit=ten", wantErr: `invalid limit "ten"`},
		{name: "zero limit rejected", raw: "q=x&limit=0", wantErr: `invalid limit "0"`},
		{name: "negative limit rejected", raw: "q=x&limit=-3", wantErr: `invalid limit "-3"`},
	}, ParseTaskSearchQuery)
}

func TestParseTaskID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		want    int64
		wantErr string
	}{
		{name: "positive id accepted", raw: "42", want: 42},
		{name: "empty rejected", raw: "", wantErr: `invalid task id ""`},
		{name: "zero rejected", raw: "0", wantErr: `invalid task id "0"`},
		{name: "negative rejected", raw: "-7", wantErr: `invalid task id "-7"`},
		{name: "non-numeric rejected", raw: "abc", wantErr: `invalid task id "abc"`},
		{name: "nested path rejected", raw: "4/comments", wantErr: `invalid task id "4/comments"`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseTaskID(tc.raw)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %d, want %d", got, tc.want)
			}
		})
	}
}

func TestTaskToNode_And_SummarizeNode(t *testing.T) {
	t.Parallel()
	parentID := int64(9)
	vt := VikunjaTask{
		ID: 1, Title: "Test", Description: "Desc", Done: true,
		RelatedTasks: map[string][]VikunjaTask{
			"parenttask":    {{ID: parentID}},
			"subtask":       {{ID: 2}},
			"blocking":      {{ID: 3}},
			"blocked":       {{ID: 4}},
			RelationRelated: {{ID: 5}},
		},
	}
	node := TaskToNode(vt, "completed")
	if node.ParentID == nil || *node.ParentID != parentID {
		t.Fatalf("node.ParentID = %v, want %d", node.ParentID, parentID)
	}
	if node.Status != TaskStatusCompleted {
		t.Fatalf("node.Status = %q, want %q", node.Status, TaskStatusCompleted)
	}
	if !slices.Equal(node.SubtaskIDs, []int64{2}) {
		t.Fatalf("node.SubtaskIDs = %v, want [2]", node.SubtaskIDs)
	}
	if !slices.Equal(node.Blocks, []int64{3}) {
		t.Fatalf("node.Blocks = %v, want [3]", node.Blocks)
	}
	if !slices.Equal(node.BlockedBy, []int64{4}) {
		t.Fatalf("node.BlockedBy = %v, want [4]", node.BlockedBy)
	}
	if !slices.Equal(node.Related, []int64{5}) {
		t.Fatalf("node.Related = %v, want [5]", node.Related)
	}
	summary := SummarizeNode(node)
	if summary.ID != 1 || summary.Title != "Test" || summary.Status != TaskStatusCompleted {
		t.Fatalf("SummarizeNode() = %+v", summary)
	}
	if summary.ParentID == nil || *summary.ParentID != parentID {
		t.Fatalf("summary.ParentID = %v, want %d", summary.ParentID, parentID)
	}
	if !slices.Equal(summary.Blocks, []int64{3}) {
		t.Fatalf("summary.Blocks = %v, want [3]", summary.Blocks)
	}
	if !slices.Equal(summary.BlockedBy, []int64{4}) {
		t.Fatalf("summary.BlockedBy = %v, want [4]", summary.BlockedBy)
	}
}
