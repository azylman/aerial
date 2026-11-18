package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

func TestInsertBuildChunkCases(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	pgErr := func(code, constraint string) error {
		return fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: code, ConstraintName: constraint})
	}
	tests := []struct {
		name      string
		chunk     model.VanadisChunk
		execErr   error
		wantErrIs error
		wantErr   bool
	}{
		{name: "invalid mode", chunk: model.VanadisChunk{ID: "c1", Mode: "bogus"}, wantErr: true},
		{name: "pr requires repo", chunk: model.VanadisChunk{ID: "c1", Mode: "pr"}, wantErr: true},
		{name: "action rejects repo", chunk: model.VanadisChunk{ID: "c1", Mode: "action", Repo: "aerial"}, wantErr: true},
		{name: "action rejects target files", chunk: model.VanadisChunk{ID: "c1", Mode: "action", TargetFiles: []string{"f.go"}}, wantErr: true},
		{name: "action rejects target symbols", chunk: model.VanadisChunk{ID: "c1", Mode: "action", TargetSymbols: []string{"S"}}, wantErr: true},
		{name: "invalid effort", chunk: model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial", Effort: "ultra"}, wantErr: true},
		{name: "pr success", chunk: model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial", Status: "pending", Effort: "medium"}},
		{name: "action running success", chunk: model.VanadisChunk{ID: "c2", Mode: "action", Status: "running"}},
		{name: "terminal status success", chunk: model.VanadisChunk{ID: "c3", Mode: "pr", Repo: "aerial", Status: "completed"}},
		{name: "explicit times success", chunk: model.VanadisChunk{ID: "c4", Mode: "pr", Repo: "aerial", Status: "running", PRNumber: 7, StartedAt: now, CompletedAt: now}},
		{name: "chunk id conflict", chunk: model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial"}, execErr: pgErr("23505", "vanadis_chunks_pkey"), wantErrIs: ErrBuildConflict},
		{name: "chunk index conflict", chunk: model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial"}, execErr: pgErr("23505", "uq_vanadis_chunks_build_index"), wantErrIs: ErrBuildConflict},
		{name: "unknown unique constraint", chunk: model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial"}, execErr: pgErr("23505", "other_constraint"), wantErr: true},
		{name: "non-unique pg error", chunk: model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial"}, execErr: pgErr("40P01", "vanadis_chunks_pkey"), wantErr: true},
		{name: "generic exec error", chunk: model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial"}, execErr: errors.New("exec failed"), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tx := &mockTx{}
			if tc.execErr != nil {
				tx.execFn = func(context.Context, string, ...any) (pgconn.CommandTag, error) {
					return pgconn.CommandTag{}, tc.execErr
				}
			}
			err := InsertBuildChunk(context.Background(), tx, "b1", tc.chunk, InsertChunkSQL)
			assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
		})
	}
}

func TestValidateCreateBuildInputsCases(t *testing.T) {
	t.Parallel()
	prChunk := func(id string, files []string, deps []string) model.VanadisChunk {
		return model.VanadisChunk{ID: id, Title: "T", Instructions: "I", Mode: "pr", Repo: "aerial", TargetFiles: files, DependsOn: deps}
	}
	tests := []struct {
		name      string
		buildID   string
		threadID  string
		objective string
		chunks    []model.VanadisChunk
		wantErrIs error
		wantErr   bool
	}{
		{name: "empty identifiers", wantErr: true},
		{name: "chunk missing title", buildID: "b1", threadID: "t1", objective: "o", chunks: []model.VanadisChunk{{ID: "c1", Instructions: "I", DependsOn: []string{}}}, wantErr: true},
		{name: "invalid chunk id", buildID: "b1", threadID: "t1", objective: "o", chunks: []model.VanadisChunk{{ID: "!!", Title: "T", Instructions: "I", DependsOn: []string{}}}, wantErr: true},
		{name: "chunk build mismatch", buildID: "b1", threadID: "t1", objective: "o", chunks: []model.VanadisChunk{{ID: "c1", BuildID: "other", Title: "T", Instructions: "I", DependsOn: []string{}}}, wantErr: true},
		{name: "invalid mode", buildID: "b1", threadID: "t1", objective: "o", chunks: []model.VanadisChunk{{ID: "c1", Title: "T", Instructions: "I", Mode: "bogus", DependsOn: []string{}}}, wantErr: true},
		{name: "action with repo", buildID: "b1", threadID: "t1", objective: "o", chunks: []model.VanadisChunk{{ID: "c1", Title: "T", Instructions: "I", Mode: "action", Repo: "aerial", DependsOn: []string{}}}, wantErr: true},
		{name: "invalid effort", buildID: "b1", threadID: "t1", objective: "o", chunks: []model.VanadisChunk{{ID: "c1", Title: "T", Instructions: "I", Mode: "action", Effort: "ultra", DependsOn: []string{}}}, wantErr: true},
		{name: "duplicate ids", buildID: "b1", threadID: "t1", objective: "o", chunks: []model.VanadisChunk{prChunk("c1", []string{"a.go"}, []string{}), prChunk("c1", []string{"b.go"}, []string{})}, wantErrIs: ErrInvalidBuildInput},
		{name: "nil depends", buildID: "b1", threadID: "t1", objective: "o", chunks: []model.VanadisChunk{prChunk("c1", []string{"a.go"}, nil)}, wantErrIs: ErrInvalidBuildInput},
		{name: "unknown dep", buildID: "b1", threadID: "t1", objective: "o", chunks: []model.VanadisChunk{prChunk("c1", []string{"a.go"}, []string{"nope"})}, wantErrIs: ErrInvalidBuildInput},
		{name: "empty dep", buildID: "b1", threadID: "t1", objective: "o", chunks: []model.VanadisChunk{prChunk("c1", []string{"a.go"}, []string{" "})}, wantErrIs: ErrInvalidBuildInput},
		{name: "self dep", buildID: "b1", threadID: "t1", objective: "o", chunks: []model.VanadisChunk{prChunk("c1", []string{"a.go"}, []string{"c1"})}, wantErrIs: ErrInvalidBuildInput},
		{name: "cycle", buildID: "b1", threadID: "t1", objective: "o", chunks: []model.VanadisChunk{prChunk("c1", []string{"a.go"}, []string{"c2"}), prChunk("c2", []string{"b.go"}, []string{"c1"})}, wantErrIs: ErrInvalidBuildInput},
		{name: "overlapping files", buildID: "b1", threadID: "t1", objective: "o", chunks: []model.VanadisChunk{prChunk("c1", []string{"a.go"}, []string{}), prChunk("c2", []string{"a.go"}, []string{})}, wantErrIs: ErrInvalidBuildInput},
		{name: "missing target files", buildID: "b1", threadID: "t1", objective: "o", chunks: []model.VanadisChunk{prChunk("c1", nil, []string{}), prChunk("c2", nil, []string{})}, wantErrIs: ErrInvalidBuildInput},
		{name: "terminal skips disjoint", buildID: "b1", threadID: "t1", objective: "o", chunks: []model.VanadisChunk{
			{ID: "c1", Title: "T", Instructions: "I", Mode: "pr", Repo: "aerial", TargetFiles: []string{"a.go"}, Status: "completed", DependsOn: []string{}},
			prChunk("c2", []string{"a.go"}, []string{}),
		}},
		{name: "disjoint valid", buildID: "b1", threadID: "t1", objective: "o", chunks: []model.VanadisChunk{prChunk("c1", []string{"a.go"}, []string{}), prChunk("c2", []string{"b.go"}, []string{"c1"})}},
		{name: "numeric dep valid", buildID: "b1", threadID: "t1", objective: "o", chunks: []model.VanadisChunk{prChunk("c1", []string{"a.go"}, []string{"1"}), prChunk("c2", []string{"b.go"}, []string{})}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateCreateBuildInputs(tc.buildID, tc.threadID, tc.objective, tc.chunks)
			assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
		})
	}
}

func TestKnownCreationConflictCases(t *testing.T) {
	t.Parallel()
	pgErr := &pgconn.PgError{Code: "23505", ConstraintName: "vanadis_chunks_pkey"}
	tests := []struct {
		name        string
		err         error
		constraints []string
		want        bool
	}{
		{name: "nil", err: nil, constraints: []string{"vanadis_chunks_pkey"}, want: false},
		{name: "generic", err: errors.New("boom"), constraints: []string{"vanadis_chunks_pkey"}, want: false},
		{name: "wrong code", err: &pgconn.PgError{Code: "40P01", ConstraintName: "vanadis_chunks_pkey"}, constraints: []string{"vanadis_chunks_pkey"}, want: false},
		{name: "unknown constraint", err: pgErr, constraints: []string{"other"}, want: false},
		{name: "match", err: pgErr, constraints: []string{"other", "vanadis_chunks_pkey"}, want: true},
		{name: "wrapped match", err: fmt.Errorf("wrapped: %w", pgErr), constraints: []string{"vanadis_chunks_pkey"}, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := knownCreationConflict(tc.err, tc.constraints...); got != tc.want {
				t.Errorf("knownCreationConflict(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestSameBuildCreationCases(t *testing.T) {
	t.Parallel()
	base := model.VanadisBuild{ID: "b1", ThreadID: "t1", Tier: 3, Title: "Build", Objective: "Obj", SharedContext: "ctx", Quiet: true}
	chunk := sameCreationChunk
	withTitle := base
	withTitle.Title = "Changed"
	withObjective := base
	withObjective.Objective = "Other"
	withQuiet := base
	withQuiet.Quiet = false
	trimmed := base
	trimmed.ID = "  b1  "
	badMode := chunk("c1", "T", "bogus")
	renamed := chunk("c9", "T", "pr")
	changed := chunk("c1", "Changed", "pr")
	good := []model.VanadisChunk{chunk("c1", "T", "pr")}
	tests := []struct {
		name string
		a    model.VanadisBuild
		ac   []model.VanadisChunk
		b    model.VanadisBuild
		bc   []model.VanadisChunk
		want bool
	}{
		{name: "identical", a: base, ac: good, b: base, bc: good, want: true},
		{name: "trimmed equal", a: trimmed, ac: good, b: base, bc: good, want: true},
		{name: "different objective", a: base, ac: good, b: withObjective, bc: good},
		{name: "different title", a: base, ac: good, b: withTitle, bc: good},
		{name: "different quiet", a: base, ac: good, b: withQuiet, bc: good},
		{name: "different count", a: base, ac: good, b: base, bc: append(append([]model.VanadisChunk{}, good...), chunk("c2", "T", "pr"))},
		{name: "changed chunk", a: base, ac: good, b: base, bc: []model.VanadisChunk{changed}},
		{name: "renamed chunk", a: base, ac: good, b: base, bc: []model.VanadisChunk{renamed}},
		{name: "invalid mode", a: base, ac: good, b: base, bc: []model.VanadisChunk{badMode}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := sameBuildCreation(tc.a, tc.ac, tc.b, tc.bc); got != tc.want {
				t.Errorf("sameBuildCreation = %v, want %v", got, tc.want)
			}
		})
	}
}

func sameCreationChunk(id, title, mode string) model.VanadisChunk {
	return model.VanadisChunk{ID: id, BuildID: "b1", Title: title, Instructions: "I", Mode: mode, Repo: "aerial", Effort: "medium", TargetFiles: []string{"a.go"}, DependsOn: []string{}}
}

func TestIsTerminalChunkStatusCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		status string
		want   bool
	}{
		{status: "pending", want: false},
		{status: "running", want: false},
		{status: "deploying", want: false},
		{status: "", want: false},
		{status: "other", want: false},
		{status: "merged", want: true},
		{status: "completed", want: true},
		{status: "failed", want: true},
		{status: "cancelled", want: true},
		{status: "  FAILED  ", want: true},
	}
	for _, tc := range tests {
		t.Run(tc.status, func(t *testing.T) {
			t.Parallel()
			if got := isTerminalChunkStatus(tc.status); got != tc.want {
				t.Errorf("isTerminalChunkStatus(%q) = %v, want %v", tc.status, got, tc.want)
			}
		})
	}
}

func TestMarshalJSONBOrEmptyCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input any
		want  string
	}{
		{name: "nil", input: nil, want: "{}"},
		{name: "empty struct", input: struct{}{}, want: "{}"},
		{name: "map", input: map[string]string{"a": "b"}, want: `{"a":"b"}`},
		{name: "unmarshalable", input: func() {}, want: "{}"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := string(marshalJSONBOrEmpty(tc.input)); got != tc.want {
				t.Errorf("marshalJSONBOrEmpty = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNormalizeAndValidateBuildDAGCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		chunks   []model.VanadisChunk
		wantErr  bool
		wantDeps []string
	}{
		{name: "empty", chunks: nil},
		{name: "duplicate id", chunks: []model.VanadisChunk{{ID: "c1"}, {ID: "c1"}}, wantErr: true},
		{name: "empty id", chunks: []model.VanadisChunk{{ID: " "}}, wantErr: true},
		{
			name: "numeric dep normalized",
			chunks: []model.VanadisChunk{
				{ID: "c1", DependsOn: []string{"1"}},
				{ID: "c2", DependsOn: []string{}},
			},
			wantDeps: []string{"c2"},
		},
		{name: "unknown dep", chunks: []model.VanadisChunk{{ID: "c1", DependsOn: []string{"zz"}}}, wantErr: true},
		{
			name: "chain ok",
			chunks: []model.VanadisChunk{
				{ID: "c1", DependsOn: []string{"c2"}},
				{ID: "c2", DependsOn: []string{"c3"}},
				{ID: "c3", DependsOn: []string{}},
			},
			wantDeps: []string{"c2"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out, err := normalizeAndValidateBuildDAG(tc.chunks)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantDeps != nil && len(out) > 0 {
				got := NonNilSlice(out[0].DependsOn)
				if len(got) != len(tc.wantDeps) || (len(got) > 0 && got[0] != tc.wantDeps[0]) {
					t.Errorf("normalized deps = %v, want %v", got, tc.wantDeps)
				}
			}
		})
	}
}

func TestGetBuildGuards(t *testing.T) {
	t.Parallel()
	b1 := model.VanadisBuild{ID: "b1", ThreadID: "th1", Title: "Build 1"}
	c1 := []model.VanadisChunk{{ID: "c1", BuildID: "b1", Title: "C1", Status: "running", Mode: "pr", Repo: "aerial", TargetFiles: []string{"f.go"}, DependsOn: []string{}, Effort: "medium"}}
	t.Run("empty id", func(t *testing.T) {
		t.Parallel()
		st, _ := dbScript{}.open()
		if _, _, err := st.GetBuild(context.Background(), "  "); err == nil {
			t.Fatal("expected error, got nil")
		}
	})
	t.Run("scan error", func(t *testing.T) {
		t.Parallel()
		st, _ := dbScript{row: &mockRow{b: &b1}, chunks: c1, scanErr: errors.New("scan failed")}.open()
		if _, _, err := st.GetBuild(context.Background(), "b1"); err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestStoreNilGuards(t *testing.T) {
	t.Parallel()
	var nilStore *Store
	if err := nilStore.CreateBuild(context.Background(), model.VanadisBuild{}, nil); err == nil {
		t.Error("expected CreateBuild nil error, got nil")
	}
	if _, _, err := nilStore.GetBuild(context.Background(), "b1"); err == nil {
		t.Error("expected GetBuild nil error, got nil")
	}
	empty := New(&mockDB{})
	empty.db = nil
	if err := empty.CreateBuild(context.Background(), model.VanadisBuild{ID: "b1"}, nil); err == nil {
		t.Error("expected CreateBuild nil-db error, got nil")
	}
}

func TestValidateConcurrentDisjointBranches(t *testing.T) {
	t.Parallel()
	pr := func(id, mode, repo string, files []string, deps []string) model.VanadisChunk {
		return model.VanadisChunk{ID: id, Mode: mode, Repo: repo, TargetFiles: files, DependsOn: deps}
	}
	tests := []struct {
		name    string
		chunks  []model.VanadisChunk
		wantErr bool
	}{
		{name: "invalid first mode skipped", chunks: []model.VanadisChunk{pr("c1", "bogus", "aerial", []string{"a.go"}, []string{}), pr("c2", "pr", "aerial", []string{"a.go"}, []string{})}},
		{name: "invalid second mode skipped", chunks: []model.VanadisChunk{pr("c1", "pr", "aerial", []string{"a.go"}, []string{}), pr("c2", "bogus", "aerial", []string{"a.go"}, []string{})}},
		{name: "different repos skipped", chunks: []model.VanadisChunk{pr("c1", "pr", "aerial", []string{"a.go"}, []string{}), pr("c2", "pr", "other", []string{"a.go"}, []string{})}},
		{name: "ordered overlap skipped", chunks: []model.VanadisChunk{pr("c1", "pr", "aerial", []string{"a.go"}, []string{"c2"}), pr("c2", "pr", "aerial", []string{"a.go"}, []string{})}},
		{name: "second missing files", chunks: []model.VanadisChunk{pr("c1", "pr", "aerial", []string{"a.go"}, []string{}), pr("c2", "pr", "aerial", nil, []string{})}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := normalizeAndValidateBuildDAG(tc.chunks)
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestSameBuildCreationEffortMismatch(t *testing.T) {
	t.Parallel()
	b := model.VanadisBuild{ID: "b1", ThreadID: "t1", Objective: "o"}
	bad := model.VanadisChunk{ID: "c1", Title: "T", Instructions: "I", Mode: "pr", Repo: "aerial", Effort: "ultra", DependsOn: []string{}}
	if sameBuildCreation(b, []model.VanadisChunk{bad}, b, []model.VanadisChunk{bad}) {
		t.Error("expected false for invalid effort, got true")
	}
	good := model.VanadisChunk{ID: "c1", Title: "T", Instructions: "I", Mode: "pr", Repo: "aerial", Effort: "medium", DependsOn: []string{}}
	other := model.VanadisChunk{ID: "c2", Title: "T", Instructions: "I", Mode: "pr", Repo: "aerial", Effort: "medium", DependsOn: []string{}}
	if sameBuildCreation(b, []model.VanadisChunk{good}, b, []model.VanadisChunk{other}) {
		t.Error("expected false for disjoint ids, got true")
	}
}

func TestBuildTransitiveClosureChain(t *testing.T) {
	t.Parallel()
	out := []model.VanadisChunk{
		{ID: "a", DependsOn: []string{"b"}},
		{ID: "b", DependsOn: []string{"c"}},
		{ID: "c", DependsOn: []string{}},
		{ID: "d", DependsOn: []string{}},
	}
	idMap := map[string]int{"a": 0, "b": 1, "c": 2, "d": 3}
	reach := buildTransitiveClosure(out, idMap)
	if !reach[1][0] || !reach[2][0] || !reach[2][1] {
		t.Errorf("expected a reachable from b and c, got %v", reach)
	}
	if reach[0][1] || reach[3][0] || reach[0][3] {
		t.Errorf("unexpected reachability, got %v", reach)
	}
}
