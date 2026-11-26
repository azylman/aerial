package transcript

import (
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestParseTranscriptTimestamp(t *testing.T) {
	fallback := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		rawTime  string
		wantTime time.Time
	}{
		{name: "empty timestamp", rawTime: "", wantTime: fallback},
		{name: "invalid timestamp", rawTime: "invalid-date", wantTime: fallback},
		{name: "rfc3339nano", rawTime: "2026-10-01T10:00:00.123456789Z", wantTime: time.Date(2026, 10, 1, 10, 0, 0, 123456789, time.UTC)},
		{name: "rfc3339 standard", rawTime: "2026-10-01T10:00:00Z", wantTime: time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := parseTranscriptTimestamp(tc.rawTime, fallback)
			if !got.Equal(tc.wantTime) {
				t.Fatalf("parseTranscriptTimestamp(%q) = %v, want %v", tc.rawTime, got, tc.wantTime)
			}
		})
	}
}

func TestExtractClaudeBlockText(t *testing.T) {
	tests := []struct {
		name, raw, wantStr string
	}{
		{name: "empty raw message", raw: "", wantStr: ""},
		{name: "quoted string", raw: `"simple text output"`, wantStr: "simple text output"},
		{name: "text blocks array", raw: `[{"type":"text","text":"first line"},{"type":"text","text":"second line"}]`, wantStr: "first line\nsecond line"},
		{name: "fallback raw json", raw: `{"key":"value"}`, wantStr: `{"key":"value"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := extractClaudeBlockText(json.RawMessage(tc.raw))
			if got != tc.wantStr {
				t.Fatalf("extractClaudeBlockText(%q) = %q, want %q", tc.raw, got, tc.wantStr)
			}
		})
	}
}

func TestFindTranscriptFile_PathResolution(t *testing.T) {
	tests := []struct {
		name, relPath, sessionID string
		wantFound                bool
	}{
		{name: "not found", sessionID: "s-missing", wantFound: false},
		{name: "dot system generated logs", relPath: "s-dot/.system_generated/logs/transcript.jsonl", sessionID: "s-dot", wantFound: true},
		{name: "no dot system generated logs", relPath: "s-nodot/system_generated/logs/transcript.jsonl", sessionID: "s-nodot", wantFound: true},
		{name: "direct under session dir", relPath: "s-direct/transcript.jsonl", sessionID: "s-direct", wantFound: true},
		{name: "session id jsonl in brain", relPath: "s-brain.jsonl", sessionID: "s-brain", wantFound: true},
		{name: "claude relative 1", relPath: "../claude/projects/p1/s-claude1.jsonl", sessionID: "s-claude1", wantFound: true},
		{name: "claude relative 2", relPath: "../../../claude/projects/p2/s-claude2.jsonl", sessionID: "s-claude2", wantFound: true},
		{name: "claude relative dot claude", relPath: "../.claude/projects/p3/s-claude3.jsonl", sessionID: "s-claude3", wantFound: true},
		{name: "claude voice runtime isolated", relPath: "../../../runtimes/voice/.claude/projects/p4/s-claude-voice.jsonl", sessionID: "s-claude-voice", wantFound: true},
		{name: "claude ephemeral runtime excluded", relPath: "../../../runtimes/ephemeral/.claude/projects/p5/s-claude-eph.jsonl", sessionID: "s-claude-eph", wantFound: false},
		{name: "codex relative direct", relPath: "../codex/sessions/s-codex1.jsonl", sessionID: "s-codex1", wantFound: true},
		{name: "codex relative rollout", relPath: "../codex/sessions/rollout-s-codex2.jsonl", sessionID: "s-codex2", wantFound: true},
		{name: "codex relative nested", relPath: "../../../codex/sessions/2026/11/15/rollout-t-s-codex3.jsonl", sessionID: "s-codex3", wantFound: true},
		{name: "codex relative dot codex", relPath: "../.codex/sessions/rollout-s-codex4.jsonl", sessionID: "s-codex4", wantFound: true},
		{name: "codex voice runtime isolated", relPath: "../../../runtimes/voice/.codex/sessions/rollout-s-codex-voice.jsonl", sessionID: "s-codex-voice", wantFound: true},
		{name: "codex ephemeral runtime excluded", relPath: "../../../runtimes/ephemeral/.codex/sessions/rollout-s-codex-eph.jsonl", sessionID: "s-codex-eph", wantFound: false},
		{name: "muse voice runtime isolated", relPath: "../../../runtimes/voice/.local/share/muse/sessions/s-muse-voice.jsonl", sessionID: "s-muse-voice", wantFound: true},
		{name: "muse ephemeral runtime excluded", relPath: "../../../runtimes/ephemeral/.local/share/muse/sessions/s-muse-eph.jsonl", sessionID: "s-muse-eph", wantFound: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			brainDir := filepath.Join(tmp, "sub1", "sub2", "brain")
			if err := os.MkdirAll(brainDir, 0o755); err != nil {
				t.Fatalf("MkdirAll(brainDir): %v", err)
			}

			if tc.relPath != "" {
				fullPath := filepath.Join(brainDir, tc.relPath)
				if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
					t.Fatalf("MkdirAll: %v", err)
				}
				if err := os.WriteFile(fullPath, []byte("content"), 0o644); err != nil {
					t.Fatalf("WriteFile: %v", err)
				}
			}

			got := FindTranscriptFile(brainDir, tc.sessionID)
			if tc.wantFound && got == "" {
				t.Fatalf("expected to find transcript file for %q", tc.sessionID)
			} else if !tc.wantFound && got != "" {
				t.Fatalf("expected not to find transcript file, got %q", got)
			}
		})
	}
}

func TestEngineForPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, brainDir, path, want string
	}{
		{name: "agy transcript inside brain dir", brainDir: "/data/brain", path: "/data/brain/sess-1/.system_generated/logs/transcript.jsonl", want: "agy"},
		{name: "brain dir with trailing slash", brainDir: "/data/brain/", path: "/data/brain/sess-1.jsonl", want: "agy"},
		{name: "claude project transcript resolved for a brain entry", brainDir: "/data/brain", path: "/data/claude/projects/-proj/sess-1.jsonl", want: "claude"},
		{name: "sibling dir sharing the brain prefix", brainDir: "/data/brain", path: "/data/brain-old/sess-1.jsonl", want: "claude"},
		{name: "codex sessions dir", brainDir: "/data/brain", path: "/data/codex/sessions/2026/11/15/rollout-sess-1.jsonl", want: "codex"},
		{name: "dot codex sessions dir", brainDir: "/data/brain", path: "/data/runtimes/discord/.codex/sessions/sess-1.jsonl", want: "codex"},
		{name: "rollout prefix anywhere", brainDir: "/data/brain", path: "/some/path/rollout-2026-sess-1.jsonl", want: "codex"},
		{name: "agy voice runtime transcript", brainDir: "/data/runtimes/voice/.gemini/antigravity-cli/brain", path: "/data/runtimes/voice/.gemini/antigravity-cli/brain/sess-1/transcript.jsonl", want: "agy"},
		{name: "claude voice runtime transcript", brainDir: "/data/runtimes/voice/.gemini/antigravity-cli/brain", path: "/data/runtimes/voice/.claude/projects/proj1/sess-1.jsonl", want: "claude"},
		{name: "codex voice runtime transcript", brainDir: "/data/runtimes/voice/.gemini/antigravity-cli/brain", path: "/data/runtimes/voice/.codex/sessions/sess-1.jsonl", want: "codex"},
		{name: "muse voice runtime transcript", brainDir: "/data/runtimes/voice/.gemini/antigravity-cli/brain", path: "/data/runtimes/voice/.local/share/muse/sessions/sess-1.jsonl", want: "muse"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := engineForPath(tc.brainDir, tc.path); got != tc.want {
				t.Errorf("engineForPath(%q, %q) = %q, want %q", tc.brainDir, tc.path, got, tc.want)
			}
		})
	}
}

func TestScanSessionTranscriptFile(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		content    string
		missing    bool
		wantErr    bool
		wantSteps  []string // Type|Status|ToolName|Content|Thinking
		wantErrors int
	}{
		{
			name: "claude prompt, tool use, id-paired results and plain reply",
			content: `{"type":"user","timestamp":"2026-11-07T10:00:00Z","message":{"role":"user","content":"Hello"}}
{"type":"assistant","timestamp":"2026-11-07T10:00:01Z","message":{"role":"assistant","content":[{"type":"thinking","thinking":"plan"},{"type":"text","text":"Running"},{"type":"tool_use","id":"tu-1","name":"Bash","input":{"command":"ls"}}]}}
{"type":"user","timestamp":"2026-11-07T10:00:02Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tu-1","content":"file.txt"}]}}
{"type":"user","timestamp":"2026-11-07T10:00:03Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tu-1","is_error":true,"content":[{"type":"text","text":"boom"}]}]}}
{"type":"assistant","timestamp":"2026-11-07T10:00:04Z","message":{"role":"assistant","content":"Done"}}
{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","name":"Read","input":{}}]}}
{"type":"user","message":{"role":"user","content":[{"type":"text","text":"not a tool result"}]}}
`,
			wantSteps: []string{"USER_INPUT|DONE||Hello|", "PLANNER_RESPONSE|DONE|Bash|Running|plan", "TOOL|DONE|Bash|file.txt|", "TOOL|ERROR|Bash|boom|", "PLANNER_RESPONSE|DONE||Done|", "PLANNER_RESPONSE|DONE|Read||", "user||||"},
		},
		{
			name: "codex tool calls resolve output tool names by call id",
			content: `{"type":"response_item","payload":{"type":"local_shell_call","id":"ls-1","cmd":"pwd"}}
{"type":"response_item","payload":{"type":"function_call_output","call_id":"ls-1","output":"/root"}}
{"type":"response_item","payload":{"type":"local_shell_call","call_id":"ls-2","arguments":{"command":["ls"]}}}
{"type":"response_item","payload":{"type":"local_shell_call","call_id":"ls-3","input":{"command":"ls"}}}
{"type":"response_item","payload":{"type":"web_search_call","call_id":"ws-1","query":"go"}}
{"type":"response_item","payload":{"type":"web_search_call","id":"ws-2","input":{"q":"x"}}}
{"type":"response_item","payload":{"type":"web_search_call","call_id":"ws-3","arguments":{"q":"y"}}}
{"type":"response_item","payload":{"type":"custom_tool_call_output","call_id":"ws-3","status":"failed","output":{"e":1}}}
{"type":"response_item","payload":{"type":"function_call","name":"shell","call_id":"fc-1","arguments":"{\"cmd\":\"ls\"}"}}
{"type":"response_item","payload":{"type":"custom_tool_call","name":"apply_patch","id":"ct-1","input":"patch"}}
`,
			wantSteps: []string{"PLANNER_RESPONSE|DONE|local_shell||", "TOOL|DONE|local_shell|/root|", "PLANNER_RESPONSE|DONE|local_shell||", "PLANNER_RESPONSE|DONE|local_shell||", "PLANNER_RESPONSE|DONE|web_search||", "PLANNER_RESPONSE|DONE|web_search||", "PLANNER_RESPONSE|DONE|web_search||", `TOOL|ERROR|web_search|{"e":1}|`, "PLANNER_RESPONSE|DONE|shell||", "PLANNER_RESPONSE|DONE|apply_patch||"},
		},
		{
			name: "codex messages, reasoning summary and raw content fallback",
			content: `{"type":"session_meta","payload":{"id":"codex-1"}}
{"type":"response_item","timestamp":"2026-11-07T10:00:00Z","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"Build it"},{"type":"image","text":"ignored"}]}}
{"type":"response_item","payload":{"type":"message","role":"developer","content":"skipped"}}
{"type":"response_item","payload":{"type":"reasoning","summary":"thinking hard"}}
{"type":"response_item","payload":{"type":"message","role":"assistant","content":"Plain"}}
{"type":"response_item","payload":{"type":"message","role":"assistant","content":{"k":1}}}
`,
			wantSteps: []string{"USER_INPUT|DONE||Build it|", "PLANNER_RESPONSE|DONE|||thinking hard", "PLANNER_RESPONSE|DONE||Plain|", `PLANNER_RESPONSE|DONE||{"k":1}|`},
		},
		{
			name: "agy steps count a malformed line and ignore a partial last line",
			content: `{"step_index":0,"type":"USER_INPUT","content":"Hi"}

not-json
{"step_index":1,"type":"PLANNER_RESPONSE","content":"Yo"}
{"step_inde`,
			wantSteps:  []string{"USER_INPUT|||Hi|", "PLANNER_RESPONSE|||Yo|"},
			wantErrors: 1,
		},
		{name: "missing file errors", missing: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "transcript.jsonl")
			if !tc.missing {
				if err := os.WriteFile(path, []byte(tc.content), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			var stats SyncStats
			st, err := scanSessionTranscriptFile(path, &stats)
			if (err != nil) != tc.wantErr {
				t.Fatalf("scanSessionTranscriptFile() err = %v, wantErr %v", err, tc.wantErr)
			}
			got := make([]string, 0, len(st.parsedSteps))
			for _, s := range st.parsedSteps {
				got = append(got, s.Type+"|"+s.Status+"|"+s.ToolName+"|"+s.Content+"|"+s.Thinking)
			}
			if !slices.Equal(got, tc.wantSteps) {
				t.Errorf("steps = %q, want %q", got, tc.wantSteps)
			}
			if stats.Errors != tc.wantErrors {
				t.Errorf("stats.Errors = %d, want %d", stats.Errors, tc.wantErrors)
			}
		})
	}
}

func TestDiscoverSessions(t *testing.T) {
	t.Parallel()
	// Paths are relative to the test root; the brain dir is root/x/y/brain, so "x/y/<engine>"
	// and the three-levels-up "<engine>" locations are both default search bases.
	tests := []struct {
		name       string
		files      map[string]string
		customDirs bool
		want       map[string]string
	}{
		{
			name: "default bases for every engine, brain dir wins collisions",
			files: map[string]string{
				"x/y/brain/agy-1/transcript.jsonl":                         "",
				"x/y/brain/.hidden/transcript.jsonl":                       "",
				"x/y/brain/empty-dir/notes.txt":                            "",
				"x/y/brain/stray.txt":                                      "",
				"x/y/claude/projects/p1/claude-1.jsonl":                    "",
				"x/y/claude/projects/p1/agy-1.jsonl":                       "",
				"x/y/claude/projects/p1/.dot.jsonl":                        "",
				"x/y/claude/projects/p1/notes.txt":                         "",
				"x/y/claude/projects/stray.jsonl":                          "",
				"claude/projects/p2/claude-2.jsonl":                        "",
				"x/y/codex/sessions/2026/11/15/rollout-test-codex-1.jsonl": "",
				"x/y/codex/sessions/plain-codex.jsonl":                     "",
				"x/y/codex/sessions/notes.txt":                             "",
				"x/y/codex/sessions/rollout-meta.jsonl":                    `{"type":"session_meta","payload":{"id":"codex-meta"}}` + "\n",
				"x/y/muse/sessions/muse-1.jsonl":                           "",
				"x/y/muse/sessions/m2/session.jsonl":                       "",
			},
			want: map[string]string{"agy-1": "agy", "claude-1": "claude", "claude-2": "claude", "codex-1": "codex", "plain-codex": "codex", "codex-meta": "codex", "muse-1": "muse", "m2": "muse"},
		},
		{
			name: "custom claude, codex and muse dirs",
			files: map[string]string{
				"custom-claude/p/claude-c.jsonl":     "",
				"custom-codex/rollout-codex-c.jsonl": "",
				"custom-muse/muse-c.jsonl":           "",
			},
			customDirs: true,
			want:       map[string]string{"claude-c": "claude", "codex-c": "codex", "muse-c": "muse"},
		},
		{name: "nothing on disk", want: map[string]string{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			brainDir := filepath.Join(root, "x", "y", "brain")
			for rel, content := range tc.files {
				full := filepath.Join(root, rel)
				if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			entries, err := os.ReadDir(brainDir)
			if err != nil && !os.IsNotExist(err) {
				t.Fatalf("ReadDir(%s): %v", brainDir, err)
			}
			var claudeDir string
			var dirs []string
			if tc.customDirs {
				claudeDir = filepath.Join(root, "custom-claude")
				dirs = []string{filepath.Join(root, "custom-codex"), filepath.Join(root, "custom-muse")}
			}
			got := map[string]string{}
			for id, s := range discoverSessions(brainDir, entries, claudeDir, dirs...) {
				got[id] = s.engine
			}
			if !maps.Equal(got, tc.want) {
				t.Errorf("discoverSessions() engines = %v, want %v", got, tc.want)
			}
		})
	}
}
