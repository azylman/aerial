package store

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/sqltest"
)

// ch builds a mutation fixture chunk; pr chunks get the default repo.
func ch(id, mode, status string, deps ...string) model.VanadisChunk {
	c := model.VanadisChunk{ID: id, BuildID: "b1", Title: "C", Mode: mode, Status: status, DependsOn: append([]string{}, deps...)}
	if mode == "pr" {
		c.Repo = "aerial"
	}
	return c
}

func mutateChunks(cs ...model.VanadisChunk) []model.VanadisChunk { return cs }

// assertMutateErr checks errors.Is against wantIs and a literal substring;
// both empty means success.
func assertMutateErr(t *testing.T, err, wantIs error, wantSub string) {
	t.Helper()
	if wantIs == nil && wantSub == "" {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return
	}
	if err == nil {
		t.Fatalf("err = nil, want %v containing %q", wantIs, wantSub)
	}
	if wantIs != nil && !errors.Is(err, wantIs) {
		t.Fatalf("err = %v, want errors.Is %v", err, wantIs)
	}
	if !strings.Contains(err.Error(), wantSub) {
		t.Fatalf("err = %v, want containing %q", err, wantSub)
	}
}

func TestValidateAppendChunksInputs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		buildID string
		inputs  []CreateChunkInput
		wantErr string
	}{
		{name: "empty build id", inputs: []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial"}}, wantErr: "build id and at least one chunk are required"},
		{name: "empty inputs", buildID: "b1", wantErr: "build id and at least one chunk are required"},
		{name: "invalid chunk mode", buildID: "b1", inputs: []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial", Mode: "invalid"}}, wantErr: "invalid chunk mode"},
		{name: "invalid chunk effort", buildID: "b1", inputs: []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial", Effort: "gigantic"}}, wantErr: "invalid chunk effort"},
		{name: "empty chunk title", buildID: "b1", inputs: []CreateChunkInput{{Instructions: "I", Repo: "aerial"}}, wantErr: "title and instructions cannot be empty"},
		{name: "empty chunk instructions", buildID: "b1", inputs: []CreateChunkInput{{Title: "T", Repo: "aerial"}}, wantErr: "title and instructions cannot be empty"},
		{name: "valid inputs", buildID: "b1", inputs: []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertMutateErr(t, validateAppendChunksInputs(tc.buildID, tc.inputs), nil, tc.wantErr)
		})
	}
}

func TestResolveChunkRef(t *testing.T) {
	t.Parallel()
	chunks := []model.VanadisChunk{
		{ID: "c-first", ChunkIndex: 0},
		{ID: "c-second", ChunkIndex: 1},
	}
	tests := []struct {
		ref  string
		want int
	}{
		{ref: "", want: -1},
		{ref: "c-first", want: 0},
		{ref: "c-second", want: 1},
		{ref: "0", want: 0},
		{ref: "1", want: 1},
		{ref: "5", want: -1},
		{ref: "c-unknown", want: -1},
	}
	for _, tc := range tests {
		t.Run(tc.ref, func(t *testing.T) {
			t.Parallel()
			if got := resolveChunkRef(chunks, tc.ref); got != tc.want {
				t.Errorf("resolveChunkRef(%q) = %d, want %d", tc.ref, got, tc.want)
			}
		})
	}
}

func TestPostgresVanadisStore_UpdateBuild_Errors(t *testing.T) {
	t.Parallel()
	running := sqlRoute{Match: "SELECT status FROM vanadis_builds", Row: sqltest.ValuesRow("running")}
	updated := func(status string) sqlRoute {
		return sqlRoute{Match: "FROM vanadis_builds WHERE id = $1", Row: &sqltest.Row{Build: &model.VanadisBuild{ID: "b1", Title: "Updated Title", Status: status}}}
	}
	title := UpdateBuildRequest{Title: "Title"}
	full := UpdateBuildRequest{Title: "Updated Title", Objective: "Updated Obj", SharedContext: "Shared"}
	tests := []struct {
		name      string
		buildID   string
		req       UpdateBuildRequest
		db        dbScript
		wantErrIs error
		wantErr   string
	}{
		{name: "empty input", buildID: "b1", wantErrIs: ErrInvalidBuildInput, wantErr: "build id and at least one non-empty field"},
		{name: "empty build id", req: title, wantErrIs: ErrInvalidBuildInput, wantErr: "build id and at least one non-empty field"},
		{name: "build not found", buildID: "b1", req: title, db: dbScript{Row: sqltest.ErrRow(pgx.ErrNoRows)}, wantErrIs: ErrBuildNotFound},
		{name: "build not running", buildID: "b1", req: title, db: dbScript{Row: sqltest.ValuesRow("completed")}, wantErrIs: ErrBuildConflict, wantErr: "must be running"},
		{name: "nil db", buildID: "b1", req: title, db: nilDB, wantErr: "database connection is nil"},
		{name: "begin error", buildID: "b1", req: title, db: dbScript{BeginErr: errors.New("begin fail")}, wantErr: "begin update build tx"},
		{
			name: "check status error", buildID: "b1", req: UpdateBuildRequest{Title: "T"},
			db: dbScript{Routes: []sqlRoute{{Match: "SELECT status FROM vanadis_builds", Row: sqltest.ErrRow(errors.New("boom"))}}}, wantErr: "check build status",
		},
		{
			name: "exec error", buildID: "b1", req: title,
			db: dbScript{Routes: []sqlRoute{running}, FailExec: "UPDATE vanadis_builds", ExecErr: errors.New("exec fail")}, wantErr: "update build",
		},
		{
			name: "fetch error", buildID: "b1", req: UpdateBuildRequest{Title: "T"},
			db: dbScript{Routes: []sqlRoute{running, {Match: "FROM vanadis_builds WHERE id = $1", Row: sqltest.ErrRow(errors.New("boom"))}}}, wantErr: "fetch updated build",
		},
		{name: "commit error", buildID: "b1", req: title, db: dbScript{Routes: []sqlRoute{running, updated("running")}, CommitErr: errors.New("commit fail")}, wantErr: "commit update build tx"},
		{name: "update build success", buildID: "b1", req: full, db: dbScript{Routes: []sqlRoute{running, updated("running")}}},
		{
			name: "update build success on failed build", buildID: "b1", req: full,
			db: dbScript{Routes: []sqlRoute{{Match: "SELECT status FROM vanadis_builds", Row: sqltest.ValuesRow("failed")}, updated("failed")}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := open(tc.db)
			_, err := st.UpdateBuild(context.Background(), tc.buildID, tc.req)
			assertMutateErr(t, err, tc.wantErrIs, tc.wantErr)
		})
	}
}

func TestValidateUpdateChunkInput_Validation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, buildID, ref string
		req                UpdateChunkRequest
		want               string
		wantErr            string
	}{
		{name: "empty build id", ref: "c1", wantErr: "build id and chunk ref are required"},
		{name: "empty chunk ref", buildID: "b1", wantErr: "build id and chunk ref are required"},
		{name: "no updated fields", buildID: "b1", ref: "c1", wantErr: "at least one updated field are required"},
		{name: "blank mode", buildID: "b1", ref: "c1", req: UpdateChunkRequest{Mode: new("  ")}, wantErr: "mode cannot be empty string"},
		{name: "invalid mode", buildID: "b1", ref: "c1", req: UpdateChunkRequest{Mode: new("invalid-mode")}, wantErr: "invalid chunk mode"},
		{name: "invalid effort", buildID: "b1", ref: "c1", req: UpdateChunkRequest{Effort: "huge"}, wantErr: "invalid chunk effort"},
		{name: "effort capped to medium", buildID: "b1", ref: "c1", req: UpdateChunkRequest{Effort: "high"}, want: "medium"},
		{name: "no effort", buildID: "b1", ref: "c1", req: UpdateChunkRequest{Title: "New Title"}, want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := validateUpdateChunkInput(tc.buildID, tc.ref, tc.req)
			var wantIs error
			if tc.wantErr != "" {
				wantIs = ErrInvalidBuildInput
			}
			assertMutateErr(t, err, wantIs, tc.wantErr)
			if got != tc.want {
				t.Errorf("effort = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestApplyChunkUpdates_Validation(t *testing.T) {
	t.Parallel()
	base := model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "example-org/aerial", Title: "Old Title", Instructions: "Old Instr", TargetFiles: []string{"main.go"}, TargetSymbols: []string{"OldSym"}}
	noRepo := base
	noRepo.Repo = ""
	old := model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial", Title: "Old", Instructions: "Old", TargetFiles: []string{"old.go"}, TargetSymbols: []string{"Old"}, Effort: "low", DependsOn: []string{}}
	tests := []struct {
		name    string
		chunk   model.VanadisChunk
		req     UpdateChunkRequest
		effort  string
		want    model.VanadisChunk
		wantErr string
	}{
		{name: "pr mode with empty repo", chunk: noRepo, wantErr: "repo is required for pr mode"},
		{name: "action mode with repo", chunk: base, req: UpdateChunkRequest{Mode: new("action"), Repo: "example-org/aerial"}, wantErr: "action chunk must have empty repo"},
		{name: "action mode with target files", chunk: base, req: UpdateChunkRequest{Mode: new("action"), TargetFiles: []string{"a.go"}}, wantErr: "action chunk must not specify target_files"},
		{name: "action mode with target symbols", chunk: base, req: UpdateChunkRequest{Mode: new("action"), TargetSymbols: []string{"Sym"}}, wantErr: "action chunk must not specify target_symbols"},
		{
			name: "valid action mode switch", chunk: base, effort: "low",
			req:  UpdateChunkRequest{Mode: new("action"), Title: "New Action Title", Instructions: "New Action Instr"},
			want: model.VanadisChunk{ID: "c1", Mode: "action", Title: "New Action Title", Instructions: "New Action Instr", TargetFiles: []string{}, TargetSymbols: []string{}, Effort: "low"},
		},
		{
			name: "valid pr update relinks dependencies", chunk: old, effort: "medium",
			req: UpdateChunkRequest{
				Title: "New", Instructions: "New", TargetFiles: []string{"new.go"}, TargetSymbols: []string{"New"},
				Effort: "high", DependsOn: &[]string{}, Repo: "aerial", Mode: new("pr"),
			},
			want: model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial", Title: "New", Instructions: "New", TargetFiles: []string{"new.go"}, TargetSymbols: []string{"New"}, Effort: "medium", DependsOn: []string{}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.chunk
			got.TargetFiles = append([]string(nil), tc.chunk.TargetFiles...)
			err := applyChunkUpdates(&got, tc.req, tc.effort)
			var wantIs error
			if tc.wantErr != "" {
				wantIs = ErrInvalidBuildInput
			}
			assertMutateErr(t, err, wantIs, tc.wantErr)
			if tc.wantErr == "" && !reflect.DeepEqual(got, tc.want) {
				t.Errorf("applyChunkUpdates chunk = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestPostgresVanadisStore_AppendChunks(t *testing.T) {
	t.Parallel()
	in := []CreateChunkInput{{Mode: "action", Title: "New Chunk", Instructions: "Inst", DependsOn: []string{}}}
	one := mutateChunks(ch("c1", "action", "running"))
	pending := mutateChunks(ch("c1", "action", "pending"))
	cycle := []CreateChunkInput{
		{ID: "c2", Mode: "action", Title: "T", Instructions: "I", DependsOn: []string{"c3"}},
		{ID: "c3", Mode: "action", Title: "T", Instructions: "I", DependsOn: []string{"c2"}},
	}
	tests := []struct {
		name      string
		db        dbScript
		in        []CreateChunkInput
		wantErrIs error
		wantErr   string
	}{
		{name: "success", db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: one, ExecTag: "INSERT 0 1"}, in: in},
		{name: "success on failed build", db: dbScript{Row: sqltest.ValuesRow("failed"), Chunks: one, ExecTag: "INSERT 0 1"}, in: in},
		{name: "nil db", db: nilDB, in: in, wantErr: "database connection is nil"},
		{name: "invalid input", in: []CreateChunkInput{{Mode: "action", Instructions: "I"}}, wantErr: "title and instructions cannot be empty"},
		{name: "invalid id", in: []CreateChunkInput{{ID: "bad id!", Mode: "action", Title: "T", Instructions: "I"}}, wantErr: "invalid id"},
		{name: "begin error", db: dbScript{BeginErr: errors.New("begin fail")}, in: in, wantErr: "begin append chunks tx"},
		{name: "build not found", db: dbScript{Row: sqltest.ErrRow(pgx.ErrNoRows)}, in: in, wantErrIs: ErrBuildNotFound},
		{name: "build not running", db: dbScript{Row: sqltest.ValuesRow("completed")}, in: in, wantErrIs: ErrBuildConflict},
		{name: "lock query error", db: dbScript{Row: sqltest.ValuesRow("running"), QueryErr: errors.New("query fail")}, in: in, wantErr: "lock chunks for append"},
		{name: "lock scan error", db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: pending, ScanErr: errors.New("scan fail")}, in: in, wantErr: "scan fail"},
		{
			name: "duplicate id", db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: mutateChunks(ch("c-dup", "action", "running"))},
			in: []CreateChunkInput{{ID: "c-dup", Mode: "action", Title: "New Chunk", Instructions: "Inst", DependsOn: []string{}}}, wantErrIs: ErrBuildConflict,
		},
		{name: "dag cycle", db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: pending}, in: cycle, wantErr: "cycle"},
		{
			name: "insert error", db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: pending, FailExec: "INSERT INTO vanadis_chunks", ExecErr: errors.New("exec fail")},
			in: in, wantErr: "failed to insert",
		},
		{name: "commit error", db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: one, ExecTag: "INSERT 0 1", CommitErr: errors.New("commit fail")}, in: in, wantErr: "commit append chunks tx"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := open(tc.db)
			_, err := st.AppendChunks(context.Background(), "b1", tc.in)
			assertMutateErr(t, err, tc.wantErrIs, tc.wantErr)
		})
	}
}

func TestPostgresVanadisStore_UpdateChunk(t *testing.T) {
	t.Parallel()
	title := UpdateChunkRequest{Title: "Updated"}
	pr := mutateChunks(ch("c1", "pr", "pending"))
	tests := []struct {
		name      string
		db        dbScript
		req       UpdateChunkRequest
		wantErrIs error
		wantErr   string
	}{
		{name: "success", db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: pr, ExecTag: "UPDATE 1"}, req: title},
		{name: "success on failed build", db: dbScript{Row: sqltest.ValuesRow("failed"), Chunks: pr, ExecTag: "UPDATE 1"}, req: title},
		{name: "nil db", db: nilDB, req: title, wantErr: "database connection is nil"},
		{name: "empty request", db: dbScript{ExecTag: "UPDATE 0"}, wantErrIs: ErrInvalidBuildInput},
		{name: "begin error", db: dbScript{BeginErr: errors.New("begin fail")}, req: title, wantErr: "begin update chunk tx"},
		{name: "build not found", db: dbScript{Row: sqltest.ErrRow(pgx.ErrNoRows)}, req: title, wantErrIs: ErrBuildNotFound},
		{name: "build not running", db: dbScript{Row: sqltest.ValuesRow("completed")}, req: title, wantErrIs: ErrBuildConflict},
		{name: "lock query error", db: dbScript{Row: sqltest.ValuesRow("running"), QueryErr: errors.New("query fail")}, req: title, wantErr: "lock chunks for update"},
		{name: "lock scan error", db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: mutateChunks(ch("c1", "action", "pending")), ScanErr: errors.New("scan fail")}, req: title, wantErr: "scan fail"},
		{name: "chunk not found", db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: mutateChunks()}, req: title, wantErrIs: ErrChunkNotFound},
		{name: "chunk not pending", db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: mutateChunks(ch("c1", "pr", "running"))}, req: title, wantErrIs: ErrBuildConflict},
		{
			name: "action with repo", db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: mutateChunks(ch("c1", "action", "pending"))},
			req: UpdateChunkRequest{Mode: new("action"), Repo: "aerial"}, wantErr: "action chunk must have empty repo",
		},
		{
			name: "dag cycle", db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: mutateChunks(ch("c1", "action", "pending"), ch("c2", "action", "pending", "c1"))},
			req: UpdateChunkRequest{Title: "T", DependsOn: &[]string{"c2"}}, wantErr: "cycle",
		},
		{
			name: "exec error", db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: pr, FailExec: "UPDATE vanadis_chunks SET mode", ExecErr: errors.New("exec fail")},
			req: title, wantErr: "execute update chunk",
		},
		{name: "commit error", db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: pr, ExecTag: "UPDATE 1", CommitErr: errors.New("commit fail")}, req: title, wantErr: "commit update chunk tx"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := open(tc.db)
			_, err := st.UpdateChunk(context.Background(), "b1", "c1", tc.req)
			assertMutateErr(t, err, tc.wantErrIs, tc.wantErr)
		})
	}
}

func TestPostgresVanadisStore_RemoveChunk(t *testing.T) {
	t.Parallel()
	two := mutateChunks(ch("c1", "action", "pending"), ch("c2", "action", "pending"))
	prFiles := func(id string) model.VanadisChunk {
		c := ch(id, "pr", "pending")
		c.TargetFiles = []string{"a.go"}
		return c
	}
	tests := []struct {
		name, buildID, ref string
		db                 dbScript
		wantErrIs          error
		wantErr            string
	}{
		{name: "success", buildID: "b1", ref: "c1", db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: two, ExecTag: "DELETE 1"}},
		{name: "success on failed build", buildID: "b1", ref: "c1", db: dbScript{Row: sqltest.ValuesRow("failed"), Chunks: two, ExecTag: "DELETE 1"}},
		{name: "nil db", buildID: "b1", ref: "c1", db: nilDB, wantErr: "database connection is nil"},
		{name: "empty build id", ref: "c1", db: dbScript{ExecTag: "UPDATE 0"}, wantErrIs: ErrInvalidBuildInput},
		{name: "begin error", buildID: "b1", ref: "c1", db: dbScript{BeginErr: errors.New("begin fail")}, wantErr: "begin remove chunk tx"},
		{name: "build not found", buildID: "b1", ref: "c1", db: dbScript{Row: sqltest.ErrRow(pgx.ErrNoRows)}, wantErrIs: ErrBuildNotFound},
		{name: "build not running", buildID: "b1", ref: "c1", db: dbScript{Row: sqltest.ValuesRow("completed")}, wantErrIs: ErrBuildConflict},
		{name: "lock query error", buildID: "b1", ref: "c1", db: dbScript{Row: sqltest.ValuesRow("running"), QueryErr: errors.New("query fail")}, wantErr: "lock chunks"},
		{name: "lock scan error", buildID: "b1", ref: "c1", db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: two, ScanErr: errors.New("scan fail")}, wantErr: "scan fail"},
		{name: "chunk not found", buildID: "b1", ref: "c1", db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: mutateChunks()}, wantErrIs: ErrChunkNotFound},
		{name: "chunk not pending", buildID: "b1", ref: "c1", db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: mutateChunks(ch("c1", "action", "running"))}, wantErrIs: ErrBuildConflict},
		{name: "only chunk", buildID: "b1", ref: "c1", db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: mutateChunks(ch("c1", "action", "pending"))}, wantErrIs: ErrBuildConflict},
		{
			name: "dag invalid", buildID: "b1", ref: "c3",
			db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: mutateChunks(prFiles("c1"), prFiles("c2"), ch("c3", "action", "pending"))}, wantErr: "invalid DAG",
		},
		{
			name: "delete error", buildID: "b1", ref: "c1",
			db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: two, FailExec: "DELETE FROM vanadis_chunks", ExecErr: errors.New("exec fail")}, wantErr: "delete chunk",
		},
		{
			name: "reindex error", buildID: "b1", ref: "c1",
			db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: two, ExecTag: "DELETE 1", FailExec: "SET chunk_index", ExecErr: errors.New("exec fail")}, wantErr: "reindex chunk",
		},
		{
			name: "commit error", buildID: "b1", ref: "c1",
			db: dbScript{Row: sqltest.ValuesRow("running"), Chunks: two, ExecTag: "DELETE 1", CommitErr: errors.New("commit fail")}, wantErr: "commit remove chunk tx",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := open(tc.db)
			_, _, err := st.RemoveChunk(context.Background(), tc.buildID, tc.ref)
			assertMutateErr(t, err, tc.wantErrIs, tc.wantErr)
		})
	}
}
