package store

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
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

// bmFixture is the declarative database state one mutation row starts from:
// seeded rows, optional sabotage DDL in the private database, a closed pool
// or a Store without a database.
type bmFixture struct {
	builds   []model.VanadisBuild
	chunks   []model.VanadisChunk
	sabotage string
	closed   bool
	nilDB    bool
}

// bmBuilds returns the single b1 build fixture with status.
func bmBuilds(status string) []model.VanadisBuild {
	return []model.VanadisBuild{{ID: "b1", ThreadID: "thread-1", Title: "Build", Objective: "Obj", SharedContext: "Ctx", Status: status}}
}

// bmOpen materializes f into a Store and returns its pool (nil when the
// row uses no live database).
func bmOpen(t *testing.T, f bmFixture) (*Store, *pgxpool.Pool) {
	t.Helper()
	switch {
	case f.nilDB:
		return &Store{}, nil
	case f.closed:
		return closedStore(t), nil
	}
	st, pool := openStore(t)
	seed(t, pool, f.builds, f.chunks)
	mustExec(t, pool, f.sabotage)
	return st, pool
}

// bmIndexed sets a fixture chunk's index.
func bmIndexed(c model.VanadisChunk, idx int) model.VanadisChunk {
	c.ChunkIndex = idx
	return c
}

// bmSkipDelete makes DELETEs on vanadis_chunks silently affect no row.
const bmSkipDelete = `CREATE FUNCTION bm_skip() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
CREATE TRIGGER bm_skip_delete BEFORE DELETE ON vanadis_chunks FOR EACH ROW EXECUTE FUNCTION bm_skip();`

// bmBreakBuildStatus makes the build status lock fail to prepare.
const bmBreakBuildStatus = `ALTER TABLE vanadis_builds RENAME COLUMN status TO status_gone;`

// bmBreakChunkSelect makes the chunk lock query fail to prepare.
const bmBreakChunkSelect = `ALTER TABLE vanadis_chunks RENAME COLUMN banner TO banner_gone;`

// bmStoredChunk is the stored projection mutation rows assert on.
type bmStoredChunk struct {
	ID, Mode, Repo, Title, Status string
	Index                         int
	DependsOn                     []string
}

// bmStoredChunks reads every chunk of b1 in index order.
func bmStoredChunks(t *testing.T, pool *pgxpool.Pool) []bmStoredChunk {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT id, mode, repo, title, status, chunk_index, depends_on FROM vanadis_chunks WHERE build_id = 'b1' ORDER BY chunk_index`)
	if err != nil {
		t.Fatalf("read chunks: %v", err)
	}
	defer rows.Close()
	var out []bmStoredChunk
	for rows.Next() {
		var c bmStoredChunk
		if err := rows.Scan(&c.ID, &c.Mode, &c.Repo, &c.Title, &c.Status, &c.Index, &c.DependsOn); err != nil {
			t.Fatalf("scan chunk: %v", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate chunks: %v", err)
	}
	return out
}

// bmAssertStored compares b1's stored chunks with want; nil skips the check.
func bmAssertStored(t *testing.T, pool *pgxpool.Pool, want []bmStoredChunk) {
	t.Helper()
	if want == nil {
		return
	}
	if got := bmStoredChunks(t, pool); !reflect.DeepEqual(got, want) {
		t.Errorf("stored chunks = %+v, want %+v", got, want)
	}
}

func TestPostgresVanadisStore_UpdateBuild_Errors(t *testing.T) {
	t.Parallel()
	running, failed := bmBuilds("running"), bmBuilds("failed")
	title := UpdateBuildRequest{Title: "Title"}
	full := UpdateBuildRequest{Title: "Updated Title", Objective: "Updated Obj", SharedContext: "Shared"}
	tests := []struct {
		name      string
		buildID   string
		req       UpdateBuildRequest
		db        bmFixture
		wantErrIs error
		wantErr   string
		// want is b1's stored title/objective/context/status after the call.
		want *model.VanadisBuild
	}{
		{name: "empty input", buildID: "b1", wantErrIs: ErrInvalidBuildInput, wantErr: "build id and at least one non-empty field"},
		{name: "empty build id", req: title, wantErrIs: ErrInvalidBuildInput, wantErr: "build id and at least one non-empty field"},
		{name: "build not found", buildID: "b1", req: title, wantErrIs: ErrBuildNotFound},
		{name: "build not running", buildID: "b1", req: title, db: bmFixture{builds: bmBuilds("completed")}, wantErrIs: ErrBuildConflict, wantErr: "must be running"},
		{name: "nil db", buildID: "b1", req: title, db: bmFixture{nilDB: true}, wantErr: "database connection is nil"},
		{name: "begin error", buildID: "b1", req: title, db: bmFixture{closed: true}, wantErr: "begin update build tx"},
		{name: "check status error", buildID: "b1", req: UpdateBuildRequest{Title: "T"}, db: bmFixture{builds: running, sabotage: bmBreakBuildStatus}, wantErr: "check build status"},
		{name: "exec error", buildID: "b1", req: title, db: bmFixture{builds: running, sabotage: failStatement("vanadis_builds", "UPDATE")}, wantErr: "update build"},
		{
			name: "fetch error", buildID: "b1", req: UpdateBuildRequest{Title: "T"},
			db: bmFixture{builds: running, sabotage: `ALTER TABLE vanadis_builds RENAME COLUMN quiet TO quiet_gone;`}, wantErr: "fetch updated build",
		},
		{
			name: "commit error", buildID: "b1", req: title, db: bmFixture{builds: running, sabotage: failCommit("vanadis_builds", "UPDATE")}, wantErr: "commit update build tx",
			want: &model.VanadisBuild{Title: "Build", Objective: "Obj", SharedContext: "Ctx", Status: "running"},
		},
		{
			name: "update build success", buildID: " b1 ", req: full, db: bmFixture{builds: running},
			want: &model.VanadisBuild{Title: "Updated Title", Objective: "Updated Obj", SharedContext: "Shared", Status: "running"},
		},
		{
			name: "update build success on failed build", buildID: "b1", req: full, db: bmFixture{builds: failed},
			want: &model.VanadisBuild{Title: "Updated Title", Objective: "Updated Obj", SharedContext: "Shared", Status: "failed"},
		},
		{
			name: "blank fields keep stored values", buildID: "b1", req: UpdateBuildRequest{Objective: " New Obj "}, db: bmFixture{builds: running},
			want: &model.VanadisBuild{Title: "Build", Objective: "New Obj", SharedContext: "Ctx", Status: "running"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := bmOpen(t, tc.db)
			got, err := st.UpdateBuild(context.Background(), tc.buildID, tc.req)
			assertMutateErr(t, err, tc.wantErrIs, tc.wantErr)
			if tc.want == nil {
				return
			}
			stored := loadBuild(t, pool, "b1")
			if gotStored := (model.VanadisBuild{Title: stored.Title, Objective: stored.Objective, SharedContext: stored.SharedContext, Status: stored.Status}); gotStored != *tc.want {
				t.Errorf("stored build = %+v, want %+v", gotStored, *tc.want)
			}
			if err == nil && (got.ID != "b1" || got.Title != tc.want.Title || got.Status != tc.want.Status) {
				t.Errorf("returned build = %+v, want stored %+v", got, *tc.want)
			}
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
	running := bmBuilds("running")
	cycle := []CreateChunkInput{
		{ID: "c2", Mode: "action", Title: "T", Instructions: "I", DependsOn: []string{"c3"}},
		{ID: "c3", Mode: "action", Title: "T", Instructions: "I", DependsOn: []string{"c2"}},
	}
	appended := []bmStoredChunk{
		{ID: "c1", Mode: "action", Title: "C", Status: "running", Index: 0, DependsOn: []string{}},
		{ID: "b1-chunk-2", Mode: "action", Title: "New Chunk", Status: "pending", Index: 1, DependsOn: []string{}},
	}
	untouched := []bmStoredChunk{{ID: "c1", Mode: "action", Title: "C", Status: "running", Index: 0, DependsOn: []string{}}}
	tests := []struct {
		name       string
		db         bmFixture
		in         []CreateChunkInput
		wantErrIs  error
		wantErr    string
		wantStored []bmStoredChunk
	}{
		{name: "success", db: bmFixture{builds: running, chunks: one}, in: in, wantStored: appended},
		{name: "success on failed build", db: bmFixture{builds: bmBuilds("failed"), chunks: one}, in: in, wantStored: appended},
		{name: "nil db", db: bmFixture{nilDB: true}, in: in, wantErr: "database connection is nil"},
		{name: "invalid input", in: []CreateChunkInput{{Mode: "action", Instructions: "I"}}, wantErr: "title and instructions cannot be empty"},
		{name: "invalid id", in: []CreateChunkInput{{ID: "bad id!", Mode: "action", Title: "T", Instructions: "I"}}, wantErr: "invalid id"},
		{name: "begin error", db: bmFixture{closed: true}, in: in, wantErr: "begin append chunks tx"},
		{name: "build not found", in: in, wantErrIs: ErrBuildNotFound},
		{name: "build not running", db: bmFixture{builds: bmBuilds("completed")}, in: in, wantErrIs: ErrBuildConflict},
		{name: "lock build error", db: bmFixture{builds: running, sabotage: bmBreakBuildStatus}, in: in, wantErr: "lock build for append chunks"},
		{name: "lock query error", db: bmFixture{builds: running, sabotage: bmBreakChunkSelect}, in: in, wantErr: "lock chunks for append"},
		{name: "lock scan error", db: bmFixture{builds: running, chunks: pending, sabotage: breakChunkScanSQL}, in: in, wantErr: "failed to scan vanadis chunk"},
		{
			name: "duplicate id", db: bmFixture{builds: running, chunks: mutateChunks(ch("c-dup", "action", "running"))},
			in: []CreateChunkInput{{ID: "c-dup", Mode: "action", Title: "New Chunk", Instructions: "Inst", DependsOn: []string{}}}, wantErrIs: ErrBuildConflict,
		},
		{name: "dag cycle", db: bmFixture{builds: running, chunks: pending}, in: cycle, wantErr: "cycle"},
		{name: "insert error", db: bmFixture{builds: running, chunks: pending, sabotage: failStatement("vanadis_chunks", "INSERT")}, in: in, wantErr: "failed to insert"},
		{name: "commit error", db: bmFixture{builds: running, chunks: one, sabotage: failCommit("vanadis_chunks", "INSERT")}, in: in, wantErr: "commit append chunks tx", wantStored: untouched},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := bmOpen(t, tc.db)
			_, err := st.AppendChunks(context.Background(), "b1", tc.in)
			assertMutateErr(t, err, tc.wantErrIs, tc.wantErr)
			bmAssertStored(t, pool, tc.wantStored)
		})
	}
}

func TestPostgresVanadisStore_UpdateChunk(t *testing.T) {
	t.Parallel()
	title := UpdateChunkRequest{Title: "Updated"}
	pr := mutateChunks(ch("c1", "pr", "pending"))
	running := bmBuilds("running")
	updated := []bmStoredChunk{{ID: "c1", Mode: "pr", Repo: "aerial", Title: "Updated", Status: "pending", Index: 0, DependsOn: []string{}}}
	original := []bmStoredChunk{{ID: "c1", Mode: "pr", Repo: "aerial", Title: "C", Status: "pending", Index: 0, DependsOn: []string{}}}
	tests := []struct {
		name       string
		db         bmFixture
		req        UpdateChunkRequest
		wantErrIs  error
		wantErr    string
		wantStored []bmStoredChunk
	}{
		{name: "success", db: bmFixture{builds: running, chunks: pr}, req: title, wantStored: updated},
		{name: "success on failed build", db: bmFixture{builds: bmBuilds("failed"), chunks: pr}, req: title, wantStored: updated},
		{
			name: "mode switch to action clears repo", db: bmFixture{builds: running, chunks: pr}, req: UpdateChunkRequest{Mode: new("action")},
			wantStored: []bmStoredChunk{{ID: "c1", Mode: "action", Title: "C", Status: "pending", Index: 0, DependsOn: []string{}}},
		},
		{name: "nil db", db: bmFixture{nilDB: true}, req: title, wantErr: "database connection is nil"},
		{name: "empty request", wantErrIs: ErrInvalidBuildInput},
		{name: "begin error", db: bmFixture{closed: true}, req: title, wantErr: "begin update chunk tx"},
		{name: "build not found", req: title, wantErrIs: ErrBuildNotFound},
		{name: "build not running", db: bmFixture{builds: bmBuilds("completed")}, req: title, wantErrIs: ErrBuildConflict},
		{name: "lock build error", db: bmFixture{builds: running, sabotage: bmBreakBuildStatus}, req: title, wantErr: "lock build for update chunk"},
		{name: "lock query error", db: bmFixture{builds: running, sabotage: bmBreakChunkSelect}, req: title, wantErr: "lock chunks for update"},
		{name: "lock scan error", db: bmFixture{builds: running, chunks: mutateChunks(ch("c1", "action", "pending")), sabotage: breakChunkScanSQL}, req: title, wantErr: "failed to scan vanadis chunk"},
		{name: "chunk not found", db: bmFixture{builds: running}, req: title, wantErrIs: ErrChunkNotFound},
		{name: "chunk not pending", db: bmFixture{builds: running, chunks: mutateChunks(ch("c1", "pr", "running"))}, req: title, wantErrIs: ErrBuildConflict},
		{
			name: "action with repo", db: bmFixture{builds: running, chunks: mutateChunks(ch("c1", "action", "pending"))},
			req: UpdateChunkRequest{Mode: new("action"), Repo: "aerial"}, wantErr: "action chunk must have empty repo",
		},
		{
			name: "dag cycle", db: bmFixture{builds: running, chunks: mutateChunks(ch("c1", "action", "pending"), bmIndexed(ch("c2", "action", "pending", "c1"), 1))},
			req: UpdateChunkRequest{Title: "T", DependsOn: &[]string{"c2"}}, wantErr: "cycle",
		},
		{name: "exec error", db: bmFixture{builds: running, chunks: pr, sabotage: failStatement("vanadis_chunks", "UPDATE")}, req: title, wantErr: "execute update chunk"},
		{name: "commit error", db: bmFixture{builds: running, chunks: pr, sabotage: failCommit("vanadis_chunks", "UPDATE")}, req: title, wantErr: "commit update chunk tx", wantStored: original},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := bmOpen(t, tc.db)
			_, err := st.UpdateChunk(context.Background(), "b1", "c1", tc.req)
			assertMutateErr(t, err, tc.wantErrIs, tc.wantErr)
			bmAssertStored(t, pool, tc.wantStored)
		})
	}
}

// bmPRFiles is a pending pr chunk touching a.go.
func bmPRFiles(id string, idx int) model.VanadisChunk {
	c := bmIndexed(ch(id, "pr", "pending"), idx)
	c.TargetFiles = []string{"a.go"}
	return c
}

func TestPostgresVanadisStore_RemoveChunk(t *testing.T) {
	t.Parallel()
	two := mutateChunks(ch("c1", "action", "pending"), bmIndexed(ch("c2", "action", "pending"), 1))
	running := bmBuilds("running")
	reindexed := []bmStoredChunk{{ID: "c2", Mode: "action", Title: "C", Status: "pending", Index: 0, DependsOn: []string{}}}
	both := []bmStoredChunk{
		{ID: "c1", Mode: "action", Title: "C", Status: "pending", Index: 0, DependsOn: []string{}},
		{ID: "c2", Mode: "action", Title: "C", Status: "pending", Index: 1, DependsOn: []string{}},
	}
	tests := []struct {
		name, buildID, ref string
		db                 bmFixture
		wantErrIs          error
		wantErr            string
		wantStored         []bmStoredChunk
	}{
		{name: "success", buildID: "b1", ref: "c1", db: bmFixture{builds: running, chunks: two}, wantStored: reindexed},
		{name: "success on failed build", buildID: "b1", ref: "c1", db: bmFixture{builds: bmBuilds("failed"), chunks: two}, wantStored: reindexed},
		{
			name: "removal by index relinks dependents", buildID: "b1", ref: "1",
			db: bmFixture{builds: running, chunks: mutateChunks(ch("c1", "action", "completed"), bmIndexed(ch("c2", "action", "pending", "c1"), 1), bmIndexed(ch("c3", "action", "pending", "c2"), 2))},
			wantStored: []bmStoredChunk{
				{ID: "c1", Mode: "action", Title: "C", Status: "completed", Index: 0, DependsOn: []string{}},
				{ID: "c3", Mode: "action", Title: "C", Status: "pending", Index: 1, DependsOn: []string{"c1"}},
			},
		},
		{name: "nil db", buildID: "b1", ref: "c1", db: bmFixture{nilDB: true}, wantErr: "database connection is nil"},
		{name: "empty build id", ref: "c1", wantErrIs: ErrInvalidBuildInput},
		{name: "begin error", buildID: "b1", ref: "c1", db: bmFixture{closed: true}, wantErr: "begin remove chunk tx"},
		{name: "build not found", buildID: "b1", ref: "c1", wantErrIs: ErrBuildNotFound},
		{name: "build not running", buildID: "b1", ref: "c1", db: bmFixture{builds: bmBuilds("completed")}, wantErrIs: ErrBuildConflict},
		{name: "lock build error", buildID: "b1", ref: "c1", db: bmFixture{builds: running, sabotage: bmBreakBuildStatus}, wantErr: "lock build"},
		{name: "lock query error", buildID: "b1", ref: "c1", db: bmFixture{builds: running, sabotage: bmBreakChunkSelect}, wantErr: "lock chunks"},
		{name: "lock scan error", buildID: "b1", ref: "c1", db: bmFixture{builds: running, chunks: two, sabotage: breakChunkScanSQL}, wantErr: "failed to scan vanadis chunk"},
		{name: "chunk not found", buildID: "b1", ref: "c1", db: bmFixture{builds: running}, wantErrIs: ErrChunkNotFound},
		{name: "chunk not pending", buildID: "b1", ref: "c1", db: bmFixture{builds: running, chunks: mutateChunks(ch("c1", "action", "running"))}, wantErrIs: ErrBuildConflict},
		{name: "only chunk", buildID: "b1", ref: "c1", db: bmFixture{builds: running, chunks: mutateChunks(ch("c1", "action", "pending"))}, wantErrIs: ErrBuildConflict},
		{
			name: "dag invalid", buildID: "b1", ref: "c3",
			db: bmFixture{builds: running, chunks: mutateChunks(bmPRFiles("c1", 0), bmPRFiles("c2", 1), bmIndexed(ch("c3", "action", "pending"), 2))}, wantErr: "invalid DAG",
		},
		{name: "delete error", buildID: "b1", ref: "c1", db: bmFixture{builds: running, chunks: two, sabotage: failStatement("vanadis_chunks", "DELETE")}, wantErr: "delete chunk"},
		{name: "delete affects no row", buildID: "b1", ref: "c1", db: bmFixture{builds: running, chunks: two, sabotage: bmSkipDelete}, wantErrIs: ErrChunkNotFound, wantStored: both},
		{name: "reindex error", buildID: "b1", ref: "c1", db: bmFixture{builds: running, chunks: two, sabotage: failStatement("vanadis_chunks", "UPDATE")}, wantErr: "reindex chunk"},
		{name: "commit error", buildID: "b1", ref: "c1", db: bmFixture{builds: running, chunks: two, sabotage: failCommit("vanadis_chunks", "DELETE")}, wantErr: "commit remove chunk tx", wantStored: both},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := bmOpen(t, tc.db)
			_, _, err := st.RemoveChunk(context.Background(), tc.buildID, tc.ref)
			assertMutateErr(t, err, tc.wantErrIs, tc.wantErr)
			bmAssertStored(t, pool, tc.wantStored)
		})
	}
}
