package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
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
		{
			name:       "open when done is false with empty resolution",
			done:       false,
			resolution: "",
			want:       "open",
		},
		{
			name:       "open when done is false even if resolution is completed",
			done:       false,
			resolution: "completed",
			want:       "open",
		},
		{
			name:       "completed when done is true and resolution is empty",
			done:       true,
			resolution: "",
			want:       "completed",
		},
		{
			name:       "completed when done is true and resolution is completed",
			done:       true,
			resolution: "completed",
			want:       "completed",
		},
		{
			name:       "wont_do when done is true and resolution is wont_do",
			done:       true,
			resolution: "wont_do",
			want:       "wont_do",
		},
		{
			name:       "wont_do with surrounding whitespace",
			done:       true,
			resolution: "  wont_do  ",
			want:       "wont_do",
		},
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

func TestDetectGraphCycle(t *testing.T) {
	t.Parallel()

	nodes := map[int64]TaskNode{
		1: {ID: 1, SubtaskIDs: []int64{2, 3}, Blocks: []int64{4, 8}},
		2: {ID: 2, ParentID: int64Ptr(1), SubtaskIDs: []int64{5}},
		3: {ID: 3, ParentID: int64Ptr(1), SubtaskIDs: []int64{5}}, // Diamond: 2 and 3 both have 5
		4: {ID: 4, BlockedBy: []int64{1}, Blocks: []int64{6}},
		5: {ID: 5, ParentID: int64Ptr(2)},
		6: {ID: 6, BlockedBy: []int64{4}},
		7: {ID: 7},
		8: {ID: 8, Blocks: []int64{6}}, // Diamond blocks: 4 and 8 both block 6
	}

	tests := []struct {
		name     string
		nodes    map[int64]TaskNode
		sourceID int64
		targetID int64
		edgeType string
		wantErr  string
	}{
		{
			name:     "self-link rejected",
			nodes:    nodes,
			sourceID: 1,
			targetID: 1,
			edgeType: "parent",
			wantErr:  "self-link cycle: task 1 cannot link to itself",
		},
		{
			name:     "valid parent link",
			nodes:    nodes,
			sourceID: 7,
			targetID: 1,
			edgeType: "parent",
			wantErr:  "",
		},
		{
			name:     "parent cycle direct child linking to parent",
			nodes:    nodes,
			sourceID: 1,
			targetID: 2,
			edgeType: "parent",
			wantErr:  "cycle detected: task 1 is already an ancestor of task 2",
		},
		{
			name:     "parent cycle deep descendant linking to ancestor",
			nodes:    nodes,
			sourceID: 1,
			targetID: 5,
			edgeType: "parent",
			wantErr:  "cycle detected: task 1 is already an ancestor of task 5",
		},
		{
			name:     "parent search drains diamond queue without cycle",
			nodes:    nodes,
			sourceID: 1,
			targetID: 7,
			edgeType: "parent",
			wantErr:  "",
		},
		{
			name:     "valid blocks link",
			nodes:    nodes,
			sourceID: 7,
			targetID: 1,
			edgeType: "blocks",
			wantErr:  "",
		},
		{
			name:     "blocks cycle direct",
			nodes:    nodes,
			sourceID: 4,
			targetID: 1,
			edgeType: "blocks",
			wantErr:  "cycle detected: task 1 already transitively blocks task 4",
		},
		{
			name:     "blocks cycle transitive",
			nodes:    nodes,
			sourceID: 6,
			targetID: 1,
			edgeType: "blocks",
			wantErr:  "cycle detected: task 1 already transitively blocks task 6",
		},
		{
			name:     "related edges never produce cycles",
			nodes:    nodes,
			sourceID: 1,
			targetID: 6,
			edgeType: "related",
			wantErr:  "",
		},
		{
			name:     "unknown edge type rejected",
			nodes:    nodes,
			sourceID: 1,
			targetID: 7,
			edgeType: "invalid_edge",
			wantErr:  `unknown edge type: "invalid_edge"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := DetectGraphCycle(tc.nodes, tc.sourceID, tc.targetID, tc.edgeType)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
				}
			}
		})
	}
}

func TestBuildReadyTree(t *testing.T) {
	t.Parallel()

	nodes := map[int64]TaskNode{
		1:  {ID: 1, Title: "Top Ready 1", Status: "open", SubtaskIDs: []int64{2, 11, 3, 9999}},
		2:  {ID: 2, Title: "Sub Ready 2", Status: "open", ParentID: int64Ptr(1)},
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

	// Update node 2 subtask IDs
	node2 := nodes[2]
	node2.SubtaskIDs = []int64{9}
	nodes[2] = node2

	tests := []struct {
		name           string
		nodes          map[int64]TaskNode
		scopedParentID *int64
		wantIDs        []int64
		wantNestedIDs  map[int64][]int64
	}{
		{
			name:           "top level ready tree",
			nodes:          nodes,
			scopedParentID: nil,
			wantIDs:        []int64{1, 4, 7, 8},
			wantNestedIDs: map[int64][]int64{
				1: {2, 11},
				2: {9},
			},
		},
		{
			name:           "scoped parent 1 ready tree",
			nodes:          nodes,
			scopedParentID: int64Ptr(1),
			wantIDs:        []int64{2, 11},
			wantNestedIDs: map[int64][]int64{
				2: {9},
			},
		},
		{
			name:           "scoped parent with no ready subtasks",
			nodes:          nodes,
			scopedParentID: int64Ptr(7),
			wantIDs:        nil,
			wantNestedIDs:  nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := BuildReadyTree(tc.nodes, tc.scopedParentID)
			var gotIDs []int64
			for _, item := range got {
				gotIDs = append(gotIDs, item.ID)
			}
			if len(tc.wantIDs) == 0 && len(gotIDs) == 0 {
				return
			}
			if !reflect.DeepEqual(gotIDs, tc.wantIDs) {
				t.Fatalf("BuildReadyTree top IDs = %v; want %v", gotIDs, tc.wantIDs)
			}
			for parentID, expectedNested := range tc.wantNestedIDs {
				for _, top := range got {
					if top.ID == parentID {
						var nestedIDs []int64
						for _, sub := range top.ReadySubtasks {
							nestedIDs = append(nestedIDs, sub.ID)
						}
						if !reflect.DeepEqual(nestedIDs, expectedNested) {
							t.Fatalf("Nested subtasks for %d = %v; want %v", parentID, nestedIDs, expectedNested)
						}
					}
				}
			}
		})
	}
}

func TestValidateCloseGuard(t *testing.T) {
	t.Parallel()

	nodes := map[int64]TaskNode{
		1:  {ID: 1, Status: "open", BlockedBy: []int64{2}, SubtaskIDs: []int64{3}},
		2:  {ID: 2, Status: "open", Blocks: []int64{1, 7}},
		3:  {ID: 3, Status: "open"},
		4:  {ID: 4, Status: "open", BlockedBy: []int64{5}, SubtaskIDs: []int64{6}, Blocks: []int64{5, 6}},
		5:  {ID: 5, Status: "completed"},
		6:  {ID: 6, Status: "wont_do"},
		7:  {ID: 7, Status: "open", BlockedBy: []int64{2}},
		8:  {ID: 8, Status: "open", SubtaskIDs: []int64{3}},
		9:  {ID: 9, Status: "open", SubtaskIDs: []int64{10}},
		10: {ID: 10, Status: "open", ParentID: int64Ptr(9), Blocks: []int64{7, 7}},
		11: {ID: 11, Status: "open", SubtaskIDs: []int64{12}, Blocks: []int64{12}},
		12: {ID: 12, Status: "open", ParentID: int64Ptr(11), BlockedBy: []int64{11}},
	}

	tests := []struct {
		name       string
		nodes      map[int64]TaskNode
		id         int64
		resolution string
		wantErr    string
	}{
		{
			name:       "wont_do allows close even with open blockers and subtasks",
			nodes:      nodes,
			id:         1,
			resolution: "wont_do",
			wantErr:    "",
		},
		{
			name:       "wont_do allowed when blocked tasks are already closed",
			nodes:      nodes,
			id:         4,
			resolution: "wont_do",
			wantErr:    "",
		},
		{
			name:       "wont_do allowed when blocked task is inside cascaded open subtree",
			nodes:      nodes,
			id:         11,
			resolution: "wont_do",
			wantErr:    "",
		},
		{
			name:       "wont_do rejected when task blocks open tasks",
			nodes:      nodes,
			id:         2,
			resolution: "wont_do",
			wantErr:    "cannot close task 2 as wont_do: blocks open tasks: [1 7]",
		},
		{
			name:       "wont_do rejected when cascaded open subtask blocks external open task",
			nodes:      nodes,
			id:         9,
			resolution: "wont_do",
			wantErr:    "cannot close task 9 as wont_do: blocks open tasks: [7]",
		},
		{
			name:       "wont_do task not found in graph returns error",
			nodes:      nodes,
			id:         999,
			resolution: "wont_do",
			wantErr:    "task 999 not found in graph",
		},
		{
			name:       "completed allowed when all blockers and subtasks are closed",
			nodes:      nodes,
			id:         4,
			resolution: "completed",
			wantErr:    "",
		},
		{
			name:       "completed rejected when blocker is open",
			nodes:      nodes,
			id:         7,
			resolution: "completed",
			wantErr:    "cannot complete task 7: open blockers: [2], open subtasks: []",
		},
		{
			name:       "completed rejected when subtask is open",
			nodes:      nodes,
			id:         8,
			resolution: "completed",
			wantErr:    "cannot complete task 8: open blockers: [], open subtasks: [3]",
		},
		{
			name:       "completed rejected when both blocker and subtask are open",
			nodes:      nodes,
			id:         1,
			resolution: "completed",
			wantErr:    "cannot complete task 1: open blockers: [2], open subtasks: [3]",
		},
		{
			name:       "task not found in graph returns error",
			nodes:      nodes,
			id:         999,
			resolution: "completed",
			wantErr:    "task 999 not found in graph",
		},
		{
			name:       "invalid resolution string rejected",
			nodes:      nodes,
			id:         4,
			resolution: "invalid_res",
			wantErr:    `invalid resolution: "invalid_res"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateCloseGuard(tc.nodes, tc.id, tc.resolution)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
				}
			}
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
		name    string
		nodes   map[int64]TaskNode
		rootID  int64
		wantIDs []int64
	}{
		{
			name:    "collect transitive open subtasks under root 1",
			nodes:   nodes,
			rootID:  1,
			wantIDs: []int64{2, 4, 6},
		},
		{
			name:    "leaf node has empty subtree",
			nodes:   nodes,
			rootID:  4,
			wantIDs: nil,
		},
		{
			name:    "nonexistent root ID returns empty slice",
			nodes:   nodes,
			rootID:  999,
			wantIDs: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := CollectOpenSubtreeIDs(tc.nodes, tc.rootID)
			if len(tc.wantIDs) == 0 && len(got) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.wantIDs) {
				t.Fatalf("CollectOpenSubtreeIDs(%d) = %v; want %v", tc.rootID, got, tc.wantIDs)
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

	tests := []struct {
		name      string
		before    map[int64]TaskNode
		after     map[int64]TaskNode
		closedIDs []int64
		wantIDs   []int64
	}{
		{
			name:      "task 2 newly unblocked after task 1 completed",
			before:    before,
			after:     after,
			closedIDs: []int64{1},
			wantIDs:   []int64{2},
		},
		{
			name:      "no transitions when no dependencies change",
			before:    before,
			after:     before,
			closedIDs: []int64{},
			wantIDs:   nil,
		},
		{
			name:   "wont_do blocker does not unblock dependent task",
			before: before,
			after: map[int64]TaskNode{
				1: {ID: 1, Status: "wont_do", Blocks: []int64{2, 3}},
				2: {ID: 2, Status: "open", BlockedBy: []int64{1}},
				3: {ID: 3, Status: "open", BlockedBy: []int64{1, 4}},
				4: {ID: 4, Status: "open", Blocks: []int64{3}},
				5: {ID: 5, Status: "open"},
			},
			closedIDs: []int64{1},
			wantIDs:   nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ComputeUnblockedIDs(tc.before, tc.after, tc.closedIDs)
			if len(tc.wantIDs) == 0 && len(got) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.wantIDs) {
				t.Fatalf("ComputeUnblockedIDs = %v; want %v", got, tc.wantIDs)
			}
		})
	}
}

func TestLoadConfig(t *testing.T) {
	tempDir := t.TempDir()
	validYAML := filepath.Join(tempDir, "config.yaml")
	if err := os.WriteFile(validYAML, []byte("port: \"9090\"\nvikunja_url: \"http://custom-vikunja:3456\"\ndefault_project_id: 42\n"), 0o600); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	invalidYAML := filepath.Join(tempDir, "bad.yaml")
	if err := os.WriteFile(invalidYAML, []byte("port: [unclosed"), 0o600); err != nil {
		t.Fatalf("failed to write bad config: %v", err)
	}

	emptyYAML := filepath.Join(tempDir, "empty.yaml")
	if err := os.WriteFile(emptyYAML, []byte(""), 0o600); err != nil {
		t.Fatalf("failed to write empty config: %v", err)
	}

	tests := []struct {
		name            string
		path            string
		envKey          string
		envVal          string
		wantPort        string
		wantProjID      int64
		wantVikunjaURL  string
		wantUsername    string
		wantPassword    string
		wantDatabaseURL string
		wantOllamaBase  string
		wantOllamaModel string
		wantErr         bool
	}{
		{
			name:       "defaults applied when path is empty",
			path:       "",
			wantPort:   "8080",
			wantProjID: 0,
			wantErr:    false,
		},
		{
			name:       "empty file returns defaults",
			path:       emptyYAML,
			wantPort:   "8080",
			wantProjID: 0,
			wantErr:    false,
		},
		{
			name:       "valid yaml loaded successfully",
			path:       validYAML,
			wantPort:   "9090",
			wantProjID: 42,
			wantErr:    false,
		},
		{
			name:    "invalid yaml returns error",
			path:    invalidYAML,
			wantErr: true,
		},
		{
			name:    "directory path returns error",
			path:    tempDir,
			wantErr: true,
		},
		{
			name:    "stat error on file as directory returns error",
			path:    filepath.Join(validYAML, "subfile.yaml"),
			wantErr: true,
		},
		{
			name:       "env PORT override",
			path:       validYAML,
			envKey:     "PORT",
			envVal:     "7777",
			wantPort:   "7777",
			wantProjID: 42,
			wantErr:    false,
		},
		{
			name:       "env DEFAULT_PROJECT_ID override",
			path:       validYAML,
			envKey:     "DEFAULT_PROJECT_ID",
			envVal:     "100",
			wantPort:   "9090",
			wantProjID: 100,
			wantErr:    false,
		},
		{
			name:           "env VIKUNJA_URL override",
			path:           validYAML,
			envKey:         "VIKUNJA_URL",
			envVal:         "http://remote-vikunja:8080",
			wantPort:       "9090",
			wantProjID:     42,
			wantVikunjaURL: "http://remote-vikunja:8080",
			wantErr:        false,
		},
		{
			name:         "env VIKUNJA_USERNAME override",
			path:         validYAML,
			envKey:       "VIKUNJA_USERNAME",
			envVal:       "testadmin",
			wantPort:     "9090",
			wantProjID:   42,
			wantUsername: "testadmin",
			wantErr:      false,
		},
		{
			name:         "env VIKUNJA_PASSWORD override",
			path:         validYAML,
			envKey:       "VIKUNJA_PASSWORD",
			envVal:       "supersecret",
			wantPort:     "9090",
			wantProjID:   42,
			wantPassword: "supersecret",
			wantErr:      false,
		},
		{
			name:            "env POSTGRES_URL override",
			path:            validYAML,
			envKey:          "POSTGRES_URL",
			envVal:          "postgres://pguser:pgpass@pghost:5432/docket",
			wantPort:        "9090",
			wantProjID:      42,
			wantDatabaseURL: "postgres://pguser:pgpass@pghost:5432/docket",
			wantErr:         false,
		},
		{
			name:            "env DATABASE_URL override",
			path:            validYAML,
			envKey:          "DATABASE_URL",
			envVal:          "postgres://dbuser:dbpass@dbhost:5432/docket",
			wantPort:        "9090",
			wantProjID:      42,
			wantDatabaseURL: "postgres://dbuser:dbpass@dbhost:5432/docket",
			wantErr:         false,
		},
		{
			name:            "env POSTGRES_PASSWORD override",
			path:            validYAML,
			envKey:          "POSTGRES_PASSWORD",
			envVal:          "secretpass",
			wantPort:        "9090",
			wantProjID:      42,
			wantDatabaseURL: "postgres://aerial:secretpass@postgres:5432/docket?sslmode=disable",
			wantErr:         false,
		},
		{
			name:           "env OLLAMA_BASE_URL override",
			path:           validYAML,
			envKey:         "OLLAMA_BASE_URL",
			envVal:         "http://ollama-remote:11434",
			wantPort:       "9090",
			wantProjID:     42,
			wantOllamaBase: "http://ollama-remote:11434",
			wantErr:        false,
		},
		{
			name:            "env OLLAMA_MODEL override",
			path:            validYAML,
			envKey:          "OLLAMA_MODEL",
			envVal:          "nomic-embed-text",
			wantPort:        "9090",
			wantProjID:      42,
			wantOllamaModel: "nomic-embed-text",
			wantErr:         false,
		},
		{
			name:       "nonexistent file returns defaults",
			path:       filepath.Join(tempDir, "nonexistent.yaml"),
			wantPort:   "8080",
			wantProjID: 0,
			wantErr:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("PORT", "")
			t.Setenv("VIKUNJA_URL", "")
			t.Setenv("VIKUNJA_USERNAME", "")
			t.Setenv("VIKUNJA_PASSWORD", "")
			t.Setenv("DEFAULT_PROJECT_ID", "")
			t.Setenv("POSTGRES_URL", "")
			t.Setenv("DATABASE_URL", "")
			t.Setenv("POSTGRES_PASSWORD", "")
			t.Setenv("POSTGRES_HOST", "")
			t.Setenv("POSTGRES_PORT", "")
			t.Setenv("POSTGRES_USER", "")
			t.Setenv("POSTGRES_DB", "")
			t.Setenv("OLLAMA_BASE_URL", "")
			t.Setenv("OLLAMA_MODEL", "")

			if tc.envKey != "" {
				t.Setenv(tc.envKey, tc.envVal)
			}

			cfg, err := LoadConfig(tc.path)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.Port != tc.wantPort {
				t.Fatalf("Port = %q; want %q", cfg.Port, tc.wantPort)
			}
			if cfg.DefaultProjectID != tc.wantProjID {
				t.Fatalf("DefaultProjectID = %d; want %d", cfg.DefaultProjectID, tc.wantProjID)
			}
			if tc.wantVikunjaURL != "" && cfg.VikunjaURL != tc.wantVikunjaURL {
				t.Fatalf("VikunjaURL = %q; want %q", cfg.VikunjaURL, tc.wantVikunjaURL)
			}
			if tc.wantUsername != "" && cfg.VikunjaUsername != tc.wantUsername {
				t.Fatalf("VikunjaUsername = %q; want %q", cfg.VikunjaUsername, tc.wantUsername)
			}
			if tc.wantPassword != "" && cfg.VikunjaPassword != tc.wantPassword {
				t.Fatalf("VikunjaPassword = %q; want %q", cfg.VikunjaPassword, tc.wantPassword)
			}
			if tc.wantDatabaseURL != "" && cfg.DatabaseURL != tc.wantDatabaseURL {
				t.Fatalf("DatabaseURL = %q; want %q", cfg.DatabaseURL, tc.wantDatabaseURL)
			}
			if tc.wantOllamaBase != "" && cfg.Ollama.BaseURL != tc.wantOllamaBase {
				t.Fatalf("Ollama.BaseURL = %q; want %q", cfg.Ollama.BaseURL, tc.wantOllamaBase)
			}
			if tc.wantOllamaModel != "" && cfg.Ollama.Model != tc.wantOllamaModel {
				t.Fatalf("Ollama.Model = %q; want %q", cfg.Ollama.Model, tc.wantOllamaModel)
			}
		})
	}
}
