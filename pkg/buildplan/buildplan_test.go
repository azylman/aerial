package buildplan

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
)

var (
	jsonCreatedAt = time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	jsonUpdatedAt = time.Date(2026, 11, 21, 11, 30, 0, 0, time.UTC)
)

const fullChunkJSON = `{"id":"c1","build_id":"build-1","chunk_index":2,"mode":"pr","repo":"aerial",` +
	`"title":"T","instructions":"do it","target_files":["a.go"],"target_symbols":["Sym"],"effort":"low",` +
	`"acceptance_criteria":["green"],"status":"merged","pr_number":42,"stall_nudges":1,"depends_on":["c0"],` +
	`"ci_fail_count":3,"ci_checks":{"test":"success"},"head_sha":"h1","remediated_sha":"r1","merge_sha":"m1",` +
	`"rollout_state":{"expected_jobs":["eri"],"started_jobs":["eri"],"deployed_jobs":["eri"],` +
	`"job_states":{"eri":"deployed"},"kustomization_statuses":{"core":"ready"}},"banner":"b",` +
	`"merge_awakeable_id":"ma","deploy_awakeable_id":"da","started_at":"2026-11-21T10:00:00Z",` +
	`"completed_at":"2026-11-21T11:30:00Z","created_at":"2026-11-21T10:00:00Z","updated_at":"2026-11-21T11:30:00Z"}`

const zeroChunkJSON = `{"id":"","build_id":"","chunk_index":0,"mode":"","title":"","instructions":"",` +
	`"target_files":null,"target_symbols":null,"effort":"","acceptance_criteria":null,"status":"",` +
	`"depends_on":null,"ci_fail_count":0,"started_at":"0001-01-01T00:00:00Z","completed_at":"0001-01-01T00:00:00Z",` +
	`"created_at":"0001-01-01T00:00:00Z","updated_at":"0001-01-01T00:00:00Z"}`

func TestVanadisChunkJSON(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		chunk    VanadisChunk
		wantJSON string
	}{
		{
			name: "fully populated",
			chunk: VanadisChunk{
				ID: "c1", BuildID: "build-1", ChunkIndex: 2, Mode: "pr", Repo: "aerial", Title: "T",
				Instructions: "do it", TargetFiles: []string{"a.go"}, TargetSymbols: []string{"Sym"}, Effort: "low",
				AcceptanceCriteria: []string{"green"}, Status: "merged", PRNumber: 42, StallNudges: 1,
				DependsOn: []string{"c0"}, CIFailCount: 3, CIChecks: map[string]string{"test": "success"},
				HeadSHA: "h1", RemediatedSHA: "r1", MergeSHA: "m1",
				RolloutState: &pubsub.PRRolloutState{
					ExpectedJobs: []string{"eri"}, StartedJobs: []string{"eri"}, DeployedJobs: []string{"eri"},
					JobStates: map[string]string{"eri": "deployed"}, KustomizationStatuses: map[string]string{"core": "ready"},
				},
				Banner: "b", MergeAwakeableID: "ma", DeployAwakeableID: "da",
				StartedAt: jsonCreatedAt, CompletedAt: jsonUpdatedAt, CreatedAt: jsonCreatedAt, UpdatedAt: jsonUpdatedAt,
			},
			wantJSON: fullChunkJSON,
		},
		{name: "zero value omits omitempty fields", chunk: VanadisChunk{}, wantJSON: zeroChunkJSON},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := json.Marshal(tc.chunk)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if string(got) != tc.wantJSON {
				t.Fatalf("Marshal =\n%s\nwant\n%s", got, tc.wantJSON)
			}
			var back VanadisChunk
			if err := json.Unmarshal([]byte(tc.wantJSON), &back); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if !reflect.DeepEqual(back, tc.chunk) {
				t.Fatalf("round trip = %#v, want %#v", back, tc.chunk)
			}
		})
	}
}

func TestVanadisBuildJSON(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		build    VanadisBuild
		wantJSON string
	}{
		{
			name: "fully populated",
			build: VanadisBuild{
				ID: "build-1", Agent: "beta", RunnerAgent: "aerial", ThreadID: "123", Tier: 3, Title: "T",
				Objective: "O", SharedContext: "S", Status: "running", StatusMessageID: "456", Quiet: true,
				CreatedAt: jsonCreatedAt, UpdatedAt: jsonUpdatedAt,
			},
			wantJSON: `{"id":"build-1","agent":"beta","runner_agent":"aerial","thread_id":"123","tier":3,"title":"T",` +
				`"objective":"O","shared_context":"S","status":"running","status_message_id":"456","quiet":true,` +
				`"created_at":"2026-11-21T10:00:00Z","updated_at":"2026-11-21T11:30:00Z"}`,
		},
		{
			name:  "zero value keeps runner_agent",
			build: VanadisBuild{},
			wantJSON: `{"id":"","runner_agent":"","thread_id":"","tier":0,"title":"","objective":"","status":"",` +
				`"quiet":false,"created_at":"0001-01-01T00:00:00Z","updated_at":"0001-01-01T00:00:00Z"}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := json.Marshal(tc.build)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if string(got) != tc.wantJSON {
				t.Fatalf("Marshal =\n%s\nwant\n%s", got, tc.wantJSON)
			}
			var back VanadisBuild
			if err := json.Unmarshal([]byte(tc.wantJSON), &back); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if back != tc.build {
				t.Fatalf("round trip = %#v, want %#v", back, tc.build)
			}
		})
	}
}

func TestIsValidVanadisChunkID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		id    string
		valid bool
	}{
		{name: "simple", id: "abc", valid: true},
		{name: "complex", id: "Abc-123_XZ", valid: true},
		{name: "padded", id: "  abc  ", valid: true},
		{name: "empty", id: "", valid: false},
		{name: "blank", id: "   ", valid: false},
		{name: "too long", id: strings.Repeat("a", 121), valid: false},
		{name: "max length", id: strings.Repeat("a", 120), valid: true},
		{name: "slash", id: "a/b", valid: false},
		{name: "space", id: "a b", valid: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsValidVanadisChunkID(tc.id); got != tc.valid {
				t.Errorf("IsValidVanadisChunkID(%q) = %v, want %v", tc.id, got, tc.valid)
			}
		})
	}
}

func TestGenerateBuildID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, wantPrefix string
		wantParts        int
		wantSuffixLen    int
	}{
		{name: "build-<unix>-<8hex>", wantPrefix: "build-", wantParts: 3, wantSuffixLen: 8},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			id := GenerateBuildID()
			parts := strings.Split(id, "-")
			if !strings.HasPrefix(id, tc.wantPrefix) || len(parts) != tc.wantParts || len(parts[len(parts)-1]) != tc.wantSuffixLen {
				t.Fatalf("GenerateBuildID() = %q, want %s<unix>-<%d hex>", id, tc.wantPrefix, tc.wantSuffixLen)
			}
		})
	}
}

func TestNormalizeRepo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, repo, want string
	}{
		{name: "empty", repo: "", want: ""},
		{name: "trims", repo: "  aerial  ", want: "aerial"},
		{name: "keeps owner", repo: "azylman/aerial", want: "azylman/aerial"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := TrimRepo(tc.repo); got != tc.want {
				t.Errorf("TrimRepo(%q) = %q, want %q", tc.repo, got, tc.want)
			}
		})
	}
}

func TestNormalizeChunkMode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, raw, want, wantErr string
	}{
		{name: "empty defaults to pr", raw: "", want: "pr"},
		{name: "pr", raw: "pr", want: "pr"},
		{name: "pr case insensitive", raw: "PR", want: "pr"},
		{name: "pr trims", raw: "  pr  ", want: "pr"},
		{name: "action", raw: "action", want: "action"},
		{name: "action mixed case", raw: "Action", want: "action"},
		{name: "invalid", raw: "nope", wantErr: `invalid chunk mode "nope": must be 'pr' or 'action'`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := NormalizeChunkMode(tc.raw)
			if gotErr := errString(err); gotErr != tc.wantErr || got != tc.want {
				t.Errorf("NormalizeChunkMode(%q) = (%q, %q), want (%q, %q)", tc.raw, got, gotErr, tc.want, tc.wantErr)
			}
		})
	}
}

func TestNormalizeChunkEffort(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, raw, want, wantErr string
	}{
		{name: "empty defaults to medium", raw: "", want: "medium"},
		{name: "medium", raw: "medium", want: "medium"},
		{name: "high caps to medium", raw: "high", want: "medium"},
		{name: "low mixed case", raw: " LOW ", want: "low"},
		{name: "invalid", raw: "bogus", wantErr: `invalid chunk effort "bogus": must be 'low', 'medium', or 'high'`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := NormalizeChunkEffort(tc.raw)
			if gotErr := errString(err); gotErr != tc.wantErr || got != tc.want {
				t.Errorf("NormalizeChunkEffort(%q) = (%q, %q), want (%q, %q)", tc.raw, got, gotErr, tc.want, tc.wantErr)
			}
		})
	}
}

func TestValidateChunkModeAndRepo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		mode, repo     string
		files, symbols []string
		wantErr        string
	}{
		{name: "default mode with repo", mode: "", repo: "aerial"},
		{name: "pr mode valid", mode: "pr", repo: "aerial"},
		{name: "action mode valid", mode: "action"},
		{name: "invalid mode", mode: "invalid", repo: "aerial", wantErr: `chunk[0] invalid chunk mode "invalid": must be 'pr' or 'action'`},
		{name: "pr blank repo", mode: "pr", repo: "   ", wantErr: "chunk[0] repo is required for pr mode"},
		{name: "action with repo", mode: "action", repo: "aerial", wantErr: "chunk[0] action chunk must have empty repo"},
		{name: "action with files", mode: "action", files: []string{"a.go"}, wantErr: "chunk[0] action chunk must not specify target_files"},
		{name: "action with symbols", mode: "action", symbols: []string{"Sym"}, wantErr: "chunk[0] action chunk must not specify target_symbols"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := errString(ValidateChunkModeAndRepo(0, tc.mode, tc.repo, tc.files, tc.symbols))
			if got != tc.wantErr {
				t.Errorf("ValidateChunkModeAndRepo(%q, %q) error = %q, want %q", tc.mode, tc.repo, got, tc.wantErr)
			}
		})
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestNonNilSlice(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    []string
		wantLen  int
		wantVals []string
	}{
		{name: "nil becomes empty", input: nil, wantLen: 0, wantVals: []string{}},
		{name: "empty stays empty", input: []string{}, wantLen: 0, wantVals: []string{}},
		{name: "values preserved", input: []string{"a", "b"}, wantLen: 2, wantVals: []string{"a", "b"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := NonNilSlice(tc.input)
			if got == nil || len(got) != tc.wantLen {
				t.Fatalf("NonNilSlice(%v) = %v, want len %d non-nil", tc.input, got, tc.wantLen)
			}
			for i, want := range tc.wantVals {
				if got[i] != want {
					t.Errorf("NonNilSlice(%v)[%d] = %q, want %q", tc.input, i, got[i], want)
				}
			}
		})
	}
}

func TestResolveChunkRef(t *testing.T) {
	t.Parallel()
	chunks := []VanadisChunk{
		{ID: "chunk-a", ChunkIndex: 0},
		{ID: "chunk-b", ChunkIndex: 1},
		{ID: "10", ChunkIndex: 2},
	}
	tests := []struct {
		name     string
		chunks   []VanadisChunk
		chunkRef string
		wantIdx  int
	}{
		{"empty_ref", chunks, "", -1},
		{"whitespace_ref", chunks, "   ", -1},
		{"match_id_exact", chunks, "chunk-a", 0},
		{"match_id_with_whitespace", chunks, "  chunk-b  ", 1},
		{"match_index_string", chunks, "0", 0},
		{"match_index_second", chunks, "1", 1},
		{"match_id_takes_precedence_over_index", chunks, "10", 2},
		{"no_match_index_out_of_bounds", chunks, "99", -1},
		{"no_match_unknown_id", chunks, "unknown", -1},
		{"empty_slice", nil, "chunk-a", -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ResolveChunkRef(tc.chunks, tc.chunkRef); got != tc.wantIdx {
				t.Errorf("ResolveChunkRef() = %d, want %d", got, tc.wantIdx)
			}
		})
	}
}
