package store

import (
	"database/sql"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

func TestValidateChunkModeAndRepo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		mode          string
		repo          string
		targetFiles   []string
		targetSymbols []string
		wantErr       bool
		wantSubstr    string
	}{
		{name: "invalid mode", mode: "invalid", repo: "aerial", wantErr: true, wantSubstr: "invalid chunk mode"},
		{name: "pr mode empty repo", mode: "pr", repo: "", wantErr: true, wantSubstr: "repo is required"},
		{name: "pr mode valid", mode: "pr", repo: "aerial", wantErr: false},
		{name: "action mode with repo", mode: "action", repo: "aerial", wantErr: true, wantSubstr: "action chunk must have empty repo"},
		{name: "action mode with target files", mode: "action", repo: "", targetFiles: []string{"a.go"}, wantErr: true, wantSubstr: "action chunk must not specify target_files"},
		{name: "action mode with target symbols", mode: "action", repo: "", targetSymbols: []string{"Sym"}, wantErr: true, wantSubstr: "action chunk must not specify target_symbols"},
		{name: "action mode valid", mode: "action", repo: "", wantErr: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateChunkModeAndRepo(0, tc.mode, tc.repo, tc.targetFiles, tc.targetSymbols)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantSubstr)
				}
				if tc.wantSubstr != "" && !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantSubstr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestRepoMatchCandidates(t *testing.T) {
	t.Parallel()
	tests := []struct {
		repo string
		want []string
	}{
		{repo: "", want: nil},
		{repo: "aerial", want: []string{"aerial", "azylman/aerial"}},
		{repo: "azylman/aerial", want: []string{"azylman/aerial", "aerial"}},
		{repo: "other/repo", want: []string{"other/repo"}},
	}
	for _, tc := range tests {
		t.Run(tc.repo, func(t *testing.T) {
			t.Parallel()
			got := RepoMatchCandidates(tc.repo)
			if !slices.Equal(got, tc.want) {
				t.Errorf("RepoMatchCandidates(%q) = %v, want %v", tc.repo, got, tc.want)
			}
		})
	}
}

func TestPostgresVanadisStore_ParsingHelpers(t *testing.T) {
	t.Parallel()

	if eff, effErr := NormalizeChunkEffort("low"); effErr != nil || eff != "low" {
		t.Errorf("NormalizeChunkEffort(low) = (%q, %v)", eff, effErr)
	}
	if eff, effErr := NormalizeChunkEffort("high"); effErr != nil || eff != "medium" {
		t.Errorf("NormalizeChunkEffort(high) = (%q, %v)", eff, effErr)
	}
	if _, effErr := NormalizeChunkEffort("invalid"); effErr == nil {
		t.Error("expected error for invalid effort")
	}
	if eff := CapChunkEffort("invalid"); eff != "medium" {
		t.Errorf("CapChunkEffort(invalid) = %q, want medium", eff)
	}
	if eff := CapChunkEffort("low"); eff != "low" {
		t.Errorf("CapChunkEffort(low) = %q, want low", eff)
	}
}

func TestFirstNonEmpty(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		vals []string
		want string
	}{
		{name: "empty", vals: nil, want: ""},
		{name: "blank only", vals: []string{"", "   "}, want: ""},
		{name: "first wins", vals: []string{"a", "b"}, want: "a"},
		{name: "skips blanks", vals: []string{"", "  ", "a", "b"}, want: "a"},
		{name: "trims result", vals: []string{"  a  "}, want: "a"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := FirstNonEmpty(tc.vals...); got != tc.want {
				t.Errorf("FirstNonEmpty(%q) = %q, want %q", tc.vals, got, tc.want)
			}
		})
	}
}

func TestNonNilSlice(t *testing.T) {
	t.Parallel()
	if got := NonNilSlice[string](nil); got == nil || len(got) != 0 {
		t.Errorf("NonNilSlice(nil) = %#v, want empty non-nil slice", got)
	}
	if got := NonNilSlice([]string{"a"}); !slices.Equal(got, []string{"a"}) {
		t.Errorf("NonNilSlice([a]) = %v, want [a]", got)
	}
}

func TestNormalizeRepo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		repo string
		want string
	}{
		{name: "empty", repo: "", want: ""},
		{name: "trims", repo: "  aerial  ", want: "aerial"},
		{name: "keeps owner", repo: "azylman/aerial", want: "azylman/aerial"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := NormalizeRepo(tc.repo); got != tc.want {
				t.Errorf("NormalizeRepo(%q) = %q, want %q", tc.repo, got, tc.want)
			}
		})
	}
}

func TestNormalizeChunkMode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{name: "empty defaults to pr", raw: "", want: "pr"},
		{name: "pr", raw: "pr", want: "pr"},
		{name: "pr case insensitive", raw: "PR", want: "pr"},
		{name: "pr trims", raw: "  pr  ", want: "pr"},
		{name: "action", raw: "action", want: "action"},
		{name: "action case insensitive", raw: "ACTION", want: "action"},
		{name: "invalid", raw: "nope", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := NormalizeChunkMode(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("NormalizeChunkMode(%q) expected error, got nil", tc.raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("NormalizeChunkMode(%q) unexpected error: %v", tc.raw, err)
			}
			if got != tc.want {
				t.Errorf("NormalizeChunkMode(%q) = %q, want %q", tc.raw, got, tc.want)
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

type stubScanRow struct {
	vals []any
	err  error
}

func (r stubScanRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(r.vals) > len(dest) {
		return errors.New("stub row holds more values than destinations")
	}
	for i, v := range r.vals {
		if v == nil {
			continue
		}
		ptr, val := reflect.ValueOf(dest[i]), reflect.ValueOf(v)
		if ptr.Kind() != reflect.Pointer || ptr.IsNil() || ptr.Elem().Type() != val.Type() {
			return errors.New("stub row value type mismatch")
		}
		ptr.Elem().Set(val)
	}
	return nil
}

func TestScanBuild(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	row := stubScanRow{vals: []any{"b1", "thread-1", 2, "Title", "Objective", "ctx", "running", "msg-1", true, now, now}}
	got, err := ScanBuild(row)
	if err != nil {
		t.Fatalf("ScanBuild unexpected error: %v", err)
	}
	if got.ID != "b1" || got.ThreadID != "thread-1" || got.Tier != 2 || !got.Quiet {
		t.Errorf("ScanBuild decoded %+v", got)
	}
	if _, err := ScanBuild(stubScanRow{err: errors.New("scan boom")}); err == nil {
		t.Error("ScanBuild expected propagated scan error, got nil")
	}
}

func TestApplyRawChunkCIChecks(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		raw        string
		wantHead   string
		wantMerge  string
		wantRemed  string
		wantChecks map[string]string
	}{
		{name: "empty", raw: ""},
		{name: "null", raw: "null"},
		{name: "empty object", raw: "{}"},
		{name: "invalid json", raw: "{oops"},
		{name: "full", raw: `{"_head_sha":"h","_merge_sha":"m","_remediated_sha":"r","lint":"passed"}`, wantHead: "h", wantMerge: "m", wantRemed: "r", wantChecks: map[string]string{"lint": "passed"}},
		{name: "internal only", raw: `{"_head_sha":"h"}`, wantHead: "h"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var c model.VanadisChunk
			ApplyRawChunkCIChecks(&c, []byte(tc.raw))
			if c.HeadSHA != tc.wantHead || c.MergeSHA != tc.wantMerge || c.RemediatedSHA != tc.wantRemed {
				t.Errorf("ApplyRawChunkCIChecks(%q) SHAs = %q/%q/%q", tc.raw, c.HeadSHA, c.MergeSHA, c.RemediatedSHA)
			}
			if len(c.CIChecks) != len(tc.wantChecks) {
				t.Errorf("ApplyRawChunkCIChecks(%q) checks = %v", tc.raw, c.CIChecks)
			}
			for k, v := range tc.wantChecks {
				if c.CIChecks[k] != v {
					t.Errorf("ApplyRawChunkCIChecks(%q) checks[%q] = %q, want %q", tc.raw, k, c.CIChecks[k], v)
				}
			}
		})
	}
}

type stubChunkRows struct {
	pgx.Rows

	chunks  []model.VanadisChunk
	checks  []string
	states  []string
	idx     int
	scanErr error
	rowsErr error
}

func (*stubChunkRows) Close() {}

func (r *stubChunkRows) Err() error { return r.rowsErr }

func (r *stubChunkRows) Next() bool {
	r.idx++
	return r.idx <= len(r.chunks)
}

func (r *stubChunkRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	c := r.chunks[r.idx-1]
	checks, state := []byte("{}"), []byte("{}")
	if r.idx-1 < len(r.checks) {
		checks = []byte(r.checks[r.idx-1])
	}
	if r.idx-1 < len(r.states) {
		state = []byte(r.states[r.idx-1])
	}
	vals := []any{c.ID, c.BuildID, c.ChunkIndex, c.Mode, c.Repo, c.Title, c.Instructions, c.TargetFiles, c.TargetSymbols, c.Effort, c.AcceptanceCriteria, c.Status, c.PRNumber, c.StallNudges, c.DependsOn, c.CIFailCount, checks, state, c.Banner, c.MergeAwakeableID, c.DeployAwakeableID, sql.NullTime{Time: c.StartedAt, Valid: !c.StartedAt.IsZero()}, sql.NullTime{Time: c.CompletedAt, Valid: !c.CompletedAt.IsZero()}, c.CreatedAt, c.UpdatedAt}
	if len(vals) != len(dest) {
		return errors.New("stub chunk row arity mismatch")
	}
	for i, v := range vals {
		ptr, val := reflect.ValueOf(dest[i]), reflect.ValueOf(v)
		if ptr.Kind() != reflect.Pointer || ptr.IsNil() || ptr.Elem().Type() != val.Type() {
			return errors.New("stub chunk row value type mismatch")
		}
		ptr.Elem().Set(val)
	}
	return nil
}

func TestScanChunks(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	withTimes := model.VanadisChunk{ID: "c1", BuildID: "b1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Title: "T1", Instructions: "I1", Effort: "high", Status: "pending", StartedAt: now, CompletedAt: now, CreatedAt: now, UpdatedAt: now}
	bare := model.VanadisChunk{ID: "c2", BuildID: "b1", ChunkIndex: 1, Mode: "pr", Repo: "aerial", Title: "T2", Instructions: "I2", Status: "pending", CreatedAt: now, UpdatedAt: now}
	t.Run("mixed rows decode", func(t *testing.T) {
		t.Parallel()
		rows := &stubChunkRows{
			chunks: []model.VanadisChunk{withTimes, bare},
			checks: []string{`{"_head_sha":"h","lint":"passed"}`, "null"},
			states: []string{`{"expected_jobs":["eri"]}`, "{}"},
		}
		got, err := ScanChunks(rows)
		if err != nil {
			t.Fatalf("ScanChunks unexpected error: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("ScanChunks decoded %d chunks, want 2", len(got))
		}
		if got[0].HeadSHA != "h" || got[0].CIChecks["lint"] != "passed" {
			t.Errorf("ScanChunks chunk 0 CI = %+v", got[0])
		}
		if got[0].RolloutState == nil {
			t.Error("ScanChunks chunk 0 expected rollout state")
		}
		if got[0].Effort != "medium" {
			t.Errorf("ScanChunks chunk 0 effort = %q, want medium", got[0].Effort)
		}
		if !got[0].StartedAt.Equal(now) || !got[0].CompletedAt.Equal(now) {
			t.Errorf("ScanChunks chunk 0 times = %v/%v", got[0].StartedAt, got[0].CompletedAt)
		}
		if len(got[1].TargetFiles) != 0 || got[1].TargetFiles == nil {
			t.Errorf("ScanChunks chunk 1 target files = %#v, want empty non-nil", got[1].TargetFiles)
		}
		if !got[1].StartedAt.IsZero() {
			t.Errorf("ScanChunks chunk 1 started = %v, want zero", got[1].StartedAt)
		}
	})
	t.Run("scan error", func(t *testing.T) {
		t.Parallel()
		rows := &stubChunkRows{chunks: []model.VanadisChunk{withTimes}, scanErr: errors.New("scan boom")}
		if _, err := ScanChunks(rows); err == nil {
			t.Error("ScanChunks expected scan error, got nil")
		}
	})
	t.Run("rows error", func(t *testing.T) {
		t.Parallel()
		rows := &stubChunkRows{chunks: []model.VanadisChunk{withTimes}, rowsErr: errors.New("rows boom")}
		if _, err := ScanChunks(rows); err == nil {
			t.Error("ScanChunks expected rows error, got nil")
		}
	})
}

func sweepChunk(id string, idx int, mode, repo, status string, completedAt time.Time) model.VanadisChunk {
	return model.VanadisChunk{ID: id, BuildID: "b1", ChunkIndex: idx, Mode: mode, Repo: repo, Status: status, CompletedAt: completedAt}
}

func TestEarlierDeploymentSweepIDs(t *testing.T) {
	t.Parallel()
	mergeBase := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	mergeLater := mergeBase.Add(time.Hour)
	mergeLater2 := mergeBase.Add(2 * time.Hour)
	tests := []struct {
		name     string
		chunks   []model.VanadisChunk
		deployID string
		want     []string
	}{
		{
			name: "earlier merged same repo completes",
			chunks: []model.VanadisChunk{
				sweepChunk("earlier", 0, "pr", "aerial", "merged", mergeBase),
				sweepChunk("deploying", 1, "pr", "aerial", "deploying", mergeLater),
			},
			deployID: "deploying",
			want:     []string{"earlier"},
		},
		{
			name: "earlier deploying and synced same repo complete",
			chunks: []model.VanadisChunk{
				sweepChunk("early-deploying", 0, "pr", "aerial", "deploying", mergeBase),
				sweepChunk("early-synced", 1, "pr", "aerial", "synced", mergeBase),
				sweepChunk("deploying", 2, "pr", "aerial", "deploying", mergeLater),
			},
			deployID: "deploying",
			want:     []string{"early-deploying", "early-synced"},
		},
		{
			name: "later merged same repo stays untouched",
			chunks: []model.VanadisChunk{
				sweepChunk("earlier", 0, "pr", "aerial", "merged", mergeBase),
				sweepChunk("deploying", 1, "pr", "aerial", "deploying", mergeLater),
				sweepChunk("later", 2, "pr", "aerial", "merged", mergeLater2),
			},
			deployID: "deploying",
			want:     []string{"earlier"},
		},
		{
			name: "earlier merged different repo stays untouched",
			chunks: []model.VanadisChunk{
				sweepChunk("other-repo", 0, "pr", "mirrormere", "merged", mergeBase),
				sweepChunk("earlier", 1, "pr", "aerial", "merged", mergeBase),
				sweepChunk("deploying", 2, "pr", "aerial", "deploying", mergeLater),
			},
			deployID: "deploying",
			want:     []string{"earlier"},
		},
		{
			name: "earlier terminal chunks stay untouched",
			chunks: []model.VanadisChunk{
				sweepChunk("failed", 0, "pr", "aerial", "failed", mergeBase),
				sweepChunk("cancelled", 1, "pr", "aerial", "cancelled", mergeBase),
				sweepChunk("completed", 2, "pr", "aerial", "completed", mergeBase),
				sweepChunk("earlier", 3, "pr", "aerial", "merged", mergeBase),
				sweepChunk("deploying", 4, "pr", "aerial", "deploying", mergeLater),
			},
			deployID: "deploying",
			want:     []string{"earlier"},
		},
		{
			name: "earlier never merged chunks stay untouched",
			chunks: []model.VanadisChunk{
				sweepChunk("opened", 0, "pr", "aerial", "pr_opened", time.Time{}),
				sweepChunk("coding", 1, "pr", "aerial", "coding", time.Time{}),
				sweepChunk("earlier", 2, "pr", "aerial", "merged", mergeBase),
				sweepChunk("deploying", 3, "pr", "aerial", "deploying", mergeLater),
			},
			deployID: "deploying",
			want:     []string{"earlier"},
		},
		{
			name: "merge stamp tie breaks by lower chunk index",
			chunks: []model.VanadisChunk{
				sweepChunk("tie-early", 0, "pr", "aerial", "merged", mergeBase),
				sweepChunk("deploying", 1, "pr", "aerial", "deploying", mergeBase),
				sweepChunk("tie-late", 2, "pr", "aerial", "merged", mergeBase),
			},
			deployID: "deploying",
			want:     []string{"tie-early"},
		},
		{
			name: "owner qualified repo spelling matches",
			chunks: []model.VanadisChunk{
				sweepChunk("earlier", 0, "pr", "aerial", "merged", mergeBase),
				sweepChunk("deploying", 1, "pr", "azylman/aerial", "deploying", mergeLater),
			},
			deployID: "deploying",
			want:     []string{"earlier"},
		},
		{
			name: "unknown deploying stamp skips earlier merged candidate",
			chunks: []model.VanadisChunk{
				sweepChunk("earlier", 0, "pr", "aerial", "merged", mergeBase),
				sweepChunk("deploying", 1, "pr", "aerial", "deploying", time.Time{}),
			},
			deployID: "deploying",
			want:     nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := EarlierDeploymentSweepIDs(tc.chunks, tc.deployID); !slices.Equal(got, tc.want) {
				t.Errorf("EarlierDeploymentSweepIDs(%q) = %q, want %q", tc.deployID, got, tc.want)
			}
		})
	}
}
