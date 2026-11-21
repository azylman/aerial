package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

var nilDB = &bareStore{store: &Store{}}

func TestValidateAppendChunksInputs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		buildID    string
		inputs     []CreateChunkInput
		wantErr    bool
		wantSubstr string
	}{
		{
			name:       "empty build id",
			buildID:    "",
			inputs:     []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial"}},
			wantErr:    true,
			wantSubstr: "build id and at least one chunk are required",
		},
		{
			name:       "empty inputs",
			buildID:    "b1",
			inputs:     nil,
			wantErr:    true,
			wantSubstr: "build id and at least one chunk are required",
		},
		{
			name:       "invalid chunk mode",
			buildID:    "b1",
			inputs:     []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial", Mode: "invalid"}},
			wantErr:    true,
			wantSubstr: "invalid chunk mode",
		},
		{
			name:       "empty chunk effort",
			buildID:    "b1",
			inputs:     []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial", Effort: "gigantic"}},
			wantErr:    true,
			wantSubstr: "invalid chunk effort",
		},
		{
			name:       "empty chunk title",
			buildID:    "b1",
			inputs:     []CreateChunkInput{{Title: "", Instructions: "I", Repo: "aerial"}},
			wantErr:    true,
			wantSubstr: "title and instructions cannot be empty",
		},
		{
			name:       "empty chunk instructions",
			buildID:    "b1",
			inputs:     []CreateChunkInput{{Title: "T", Instructions: "", Repo: "aerial"}},
			wantErr:    true,
			wantSubstr: "title and instructions cannot be empty",
		},
		{
			name:    "valid inputs",
			buildID: "b1",
			inputs:  []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial"}},
			wantErr: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateAppendChunksInputs(tc.buildID, tc.inputs)
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
	tests := []struct {
		name       string
		buildID    string
		req        UpdateBuildRequest
		db         dbScript
		wantErr    bool
		wantErrIs  error
		wantSubstr string
	}{
		{
			name:       "empty input",
			buildID:    "b1",
			req:        UpdateBuildRequest{},
			wantErr:    true,
			wantSubstr: "build id and at least one non-empty field",
		},
		{
			name:       "empty build id",
			buildID:    "",
			req:        UpdateBuildRequest{Title: "Title"},
			wantErr:    true,
			wantSubstr: "build id and at least one non-empty field",
		},
		{
			name:      "build not found",
			buildID:   "b1",
			req:       UpdateBuildRequest{Title: "Title"},
			db:        dbScript{row: errRow(pgx.ErrNoRows)},
			wantErrIs: ErrBuildNotFound,
		},
		{
			name:       "build not running",
			buildID:    "b1",
			req:        UpdateBuildRequest{Title: "Title"},
			db:         dbScript{row: valuesRow("completed")},
			wantErrIs:  ErrBuildConflict,
			wantSubstr: "must be running",
		},
		{
			name:       "nil db",
			buildID:    "b1",
			req:        UpdateBuildRequest{Title: "Title"},
			db:         dbScript{bare: nilDB},
			wantErr:    true,
			wantSubstr: "database connection is nil",
		},
		{
			name:       "begin error",
			buildID:    "b1",
			req:        UpdateBuildRequest{Title: "Title"},
			db:         dbScript{beginErr: errors.New("begin fail")},
			wantErr:    true,
			wantSubstr: "begin update build tx",
		},
		{
			name:    "exec error",
			buildID: "b1",
			req:     UpdateBuildRequest{Title: "Title"},
			db: dbScript{
				routes: []sqlRoute{
					{match: "SELECT status FROM vanadis_builds", row: valuesRow("running")},
				},
				failExec: "UPDATE vanadis_builds",
				execErr:  errors.New("exec fail"),
			},
			wantErr:    true,
			wantSubstr: "update build",
		},
		{
			name:    "commit error",
			buildID: "b1",
			req:     UpdateBuildRequest{Title: "Title"},
			db: dbScript{
				routes: []sqlRoute{
					{match: "SELECT status FROM vanadis_builds", row: valuesRow("running")},
					{match: "FROM vanadis_builds WHERE id = $1", row: &mockRow{b: &model.VanadisBuild{ID: "b1", Title: "Updated Title", Status: "running"}}},
				},
				commitErr: errors.New("commit fail"),
			},
			wantErr:    true,
			wantSubstr: "commit update build tx",
		},
		{
			name:    "update build success",
			buildID: "b1",
			req:     UpdateBuildRequest{Title: "Updated Title", Objective: "Updated Obj", SharedContext: "Shared"},
			db: dbScript{
				routes: []sqlRoute{
					{match: "SELECT status FROM vanadis_builds", row: valuesRow("running")},
					{match: "FROM vanadis_builds WHERE id = $1", row: &mockRow{b: &model.VanadisBuild{ID: "b1", Title: "Updated Title", Status: "running"}}},
				},
			},
			wantErr: false,
		},
		{
			name:    "update build success on failed build",
			buildID: "b1",
			req:     UpdateBuildRequest{Title: "Updated Title", Objective: "Updated Obj", SharedContext: "Shared"},
			db: dbScript{
				routes: []sqlRoute{
					{match: "SELECT status FROM vanadis_builds", row: valuesRow("failed")},
					{match: "FROM vanadis_builds WHERE id = $1", row: &mockRow{b: &model.VanadisBuild{ID: "b1", Title: "Updated Title", Status: "failed"}}},
				},
			},
			wantErr: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := tc.db.open()
			_, err := st.UpdateBuild(context.Background(), tc.buildID, tc.req)
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("UpdateBuild err = %v, wantErrIs %v", err, tc.wantErrIs)
				}
				return
			}
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

func TestInsertBuildChunk_ModesAndValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		chunk      model.VanadisChunk
		failExec   string
		wantErr    bool
		wantErrSub string
	}{
		{
			name:       "pr mode missing repo",
			chunk:      model.VanadisChunk{ID: "c1", Mode: "pr", Repo: ""},
			wantErr:    true,
			wantErrSub: "repo is required for pr mode",
		},
		{
			name:       "action mode with non-empty repo",
			chunk:      model.VanadisChunk{ID: "c1", Mode: "action", Repo: "aerial"},
			wantErr:    true,
			wantErrSub: "action chunk must have empty repo",
		},
		{
			name:       "action mode with target files",
			chunk:      model.VanadisChunk{ID: "c1", Mode: "action", TargetFiles: []string{"a.go"}},
			wantErr:    true,
			wantErrSub: "action chunk must not specify target files or symbols",
		},
		{
			name:       "invalid effort",
			chunk:      model.VanadisChunk{ID: "c1", Mode: "action", Effort: "gigantic"},
			wantErr:    true,
			wantErrSub: "invalid chunk effort",
		},
		{
			name:     "insert exec error",
			chunk:    model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial", Status: "running", PRNumber: 123},
			failExec: "INSERT INTO vanadis_chunks",
			wantErr:  true,
		},
		{
			name:    "insert success with completed status and action mode",
			chunk:   model.VanadisChunk{ID: "c1", Mode: "action", Status: "completed"},
			wantErr: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tx := &mockTx{
				execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
					if tc.failExec != "" && strings.Contains(sql, tc.failExec) {
						return pgconn.CommandTag{}, errors.New("exec error")
					}
					return pgconn.NewCommandTag("INSERT 1"), nil
				},
			}
			err := InsertBuildChunk(context.Background(), tx, "b1", tc.chunk, "INSERT INTO vanadis_chunks VALUES (...)")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErrSub)
				}
				if tc.wantErrSub != "" && !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErrSub, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateUpdateChunkInput_Validation(t *testing.T) {
	t.Parallel()

	strPtr := func(s string) *string { return &s }

	// validateUpdateChunkInput tests
	_, err := validateUpdateChunkInput("", "c1", UpdateChunkRequest{})
	if err == nil || !strings.Contains(err.Error(), "build id and chunk ref are required") {
		t.Fatalf("expected error on empty buildID, got %v", err)
	}
	_, err = validateUpdateChunkInput("b1", "", UpdateChunkRequest{})
	if err == nil || !strings.Contains(err.Error(), "build id and chunk ref are required") {
		t.Fatalf("expected error on empty chunkRef, got %v", err)
	}
	_, err = validateUpdateChunkInput("b1", "c1", UpdateChunkRequest{})
	if err == nil || !strings.Contains(err.Error(), "at least one updated field are required") {
		t.Fatalf("expected error on empty fields, got %v", err)
	}
	_, err = validateUpdateChunkInput("b1", "c1", UpdateChunkRequest{Mode: strPtr("  ")})
	if err == nil || !strings.Contains(err.Error(), "mode cannot be empty string") {
		t.Fatalf("expected error on empty mode string, got %v", err)
	}
	_, err = validateUpdateChunkInput("b1", "c1", UpdateChunkRequest{Mode: strPtr("invalid-mode")})
	if err == nil {
		t.Fatal("expected error on invalid mode")
	}
	_, err = validateUpdateChunkInput("b1", "c1", UpdateChunkRequest{Effort: "huge"})
	if err == nil {
		t.Fatal("expected error on invalid effort")
	}
	effort, err := validateUpdateChunkInput("b1", "c1", UpdateChunkRequest{Effort: "high"})
	if err != nil || effort != "medium" {
		t.Fatalf("expected medium effort, got %q (err: %v)", effort, err)
	}
	effort, err = validateUpdateChunkInput("b1", "c1", UpdateChunkRequest{Title: "New Title"})
	if err != nil || effort != "" {
		t.Fatalf("expected empty effort, got %q (err: %v)", effort, err)
	}
}

func TestApplyChunkUpdates_Validation(t *testing.T) {
	t.Parallel()

	strPtr := func(s string) *string { return &s }

	// applyChunkUpdates tests
	c := model.VanadisChunk{
		ID:            "c1",
		Mode:          "pr",
		Repo:          "azylman/aerial",
		Title:         "Old Title",
		Instructions:  "Old Instr",
		TargetFiles:   []string{"main.go"},
		TargetSymbols: []string{"OldSym"},
	}

	// PR mode with empty repo error
	cEmptyRepo := c
	cEmptyRepo.Repo = ""
	err := applyChunkUpdates(&cEmptyRepo, UpdateChunkRequest{}, "")
	if err == nil || !strings.Contains(err.Error(), "repo is required for pr mode") {
		t.Fatalf("expected error on pr mode with empty repo, got %v", err)
	}

	// Action mode with repo error
	cActionWithRepo := c
	err = applyChunkUpdates(&cActionWithRepo, UpdateChunkRequest{Mode: strPtr("action"), Repo: "azylman/aerial"}, "")
	if err == nil || !strings.Contains(err.Error(), "action chunk must have empty repo") {
		t.Fatalf("expected error on action chunk with repo, got %v", err)
	}

	// Action mode with target_files error
	cActionWithFiles := c
	err = applyChunkUpdates(&cActionWithFiles, UpdateChunkRequest{Mode: strPtr("action"), TargetFiles: []string{"a.go"}}, "")
	if err == nil || !strings.Contains(err.Error(), "action chunk must not specify target_files") {
		t.Fatalf("expected error on action chunk with target_files, got %v", err)
	}

	// Action mode with target_symbols error
	cActionWithSymbols := c
	err = applyChunkUpdates(&cActionWithSymbols, UpdateChunkRequest{Mode: strPtr("action"), TargetSymbols: []string{"Sym"}}, "")
	if err == nil || !strings.Contains(err.Error(), "action chunk must not specify target_symbols") {
		t.Fatalf("expected error on action chunk with target_symbols, got %v", err)
	}

	// Valid action mode switch
	cValidAction := c
	err = applyChunkUpdates(&cValidAction, UpdateChunkRequest{
		Mode:         strPtr("action"),
		Title:        "New Action Title",
		Instructions: "New Action Instr",
	}, "low")
	if err != nil {
		t.Fatalf("unexpected error on valid action update: %v", err)
	}
	if cValidAction.Mode != "action" || cValidAction.Repo != "" || len(cValidAction.TargetFiles) != 0 || len(cValidAction.TargetSymbols) != 0 || cValidAction.Effort != "low" {
		t.Fatalf("unexpected updated action chunk: %+v", cValidAction)
	}
}

func TestPostgresVanadisStore_MutateChunks(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		db        dbScript
		wantErr   bool
		wantErrIs error
	}{
		{name: "append chunks success", db: dbScript{
			row:     valuesRow("running"),
			chunks:  []model.VanadisChunk{{ID: "c1", BuildID: "b1", Title: "C1", Status: "running", Mode: "action"}},
			execTag: "INSERT 0 1",
		}},
		{name: "append chunks build not found", db: dbScript{row: errRow(pgx.ErrNoRows)}, wantErrIs: ErrBuildNotFound},
		{name: "append chunks build not running", db: dbScript{row: valuesRow("completed")}, wantErrIs: ErrBuildConflict},
		{name: "append chunks duplicate id", db: dbScript{
			row:    valuesRow("running"),
			chunks: []model.VanadisChunk{{ID: "c-dup", BuildID: "b1", Title: "C1", Status: "running", Mode: "action"}},
		}, wantErrIs: ErrBuildConflict},
		{name: "append chunks begin error", db: dbScript{beginErr: errors.New("begin fail")}, wantErr: true},
		{name: "append chunks commit error", db: dbScript{
			row:       valuesRow("running"),
			chunks:    []model.VanadisChunk{{ID: "c1", BuildID: "b1", Title: "C1", Status: "running", Mode: "action"}},
			execTag:   "INSERT 0 1",
			commitErr: errors.New("commit fail"),
		}, wantErr: true},
		{name: "append chunks success on failed build", db: dbScript{
			row:     valuesRow("failed"),
			chunks:  []model.VanadisChunk{{ID: "c1", BuildID: "b1", Title: "C1", Status: "running", Mode: "action"}},
			execTag: "INSERT 0 1",
		}},
		{name: "remove chunk success", db: dbScript{row: valuesRow("running"), chunks: []model.VanadisChunk{{ID: "c1", Status: "pending", Mode: "action"}, {ID: "c2", Status: "pending", Mode: "action"}}, execTag: "DELETE 1"}},
		{name: "remove chunk success on failed build", db: dbScript{row: valuesRow("failed"), chunks: []model.VanadisChunk{{ID: "c1", Status: "pending", Mode: "action"}, {ID: "c2", Status: "pending", Mode: "action"}}, execTag: "DELETE 1"}},
		{name: "remove chunk only chunk", db: dbScript{row: valuesRow("running"), chunks: []model.VanadisChunk{{ID: "c1", Status: "pending", Mode: "action"}}}, wantErrIs: ErrBuildConflict},
		{name: "remove chunk not found", db: dbScript{row: valuesRow("running"), chunks: []model.VanadisChunk{}}, wantErrIs: ErrChunkNotFound},
		{name: "remove chunk build not running", db: dbScript{row: valuesRow("completed")}, wantErrIs: ErrBuildConflict},
		{name: "remove chunk not pending", db: dbScript{row: valuesRow("running"), chunks: []model.VanadisChunk{{ID: "c1", Status: "running"}}}, wantErrIs: ErrBuildConflict},
		{name: "remove chunk begin error", db: dbScript{beginErr: errors.New("begin fail")}, wantErr: true},
		{name: "remove chunk commit error", db: dbScript{row: valuesRow("running"), chunks: []model.VanadisChunk{{ID: "c1", Status: "pending", Mode: "action"}, {ID: "c2", Status: "pending", Mode: "action"}}, execTag: "DELETE 1", commitErr: errors.New("commit fail")}, wantErr: true},
		{name: "update chunk success", db: dbScript{row: valuesRow("running"), chunks: []model.VanadisChunk{{ID: "c1", Status: "pending", Repo: "aerial", Mode: "pr"}}, execTag: "UPDATE 1"}},
		{name: "update chunk success on failed build", db: dbScript{row: valuesRow("failed"), chunks: []model.VanadisChunk{{ID: "c1", Status: "pending", Repo: "aerial", Mode: "pr"}}, execTag: "UPDATE 1"}},
		{name: "update chunk not found", db: dbScript{row: valuesRow("running"), chunks: []model.VanadisChunk{}}, wantErrIs: ErrChunkNotFound},
		{name: "update chunk not pending", db: dbScript{row: valuesRow("running"), chunks: []model.VanadisChunk{{ID: "c1", Status: "running", Repo: "aerial", Mode: "pr"}}}, wantErrIs: ErrBuildConflict},
		{name: "update chunk begin error", db: dbScript{beginErr: errors.New("begin fail")}, wantErr: true},
		{name: "update chunk commit error", db: dbScript{row: valuesRow("running"), chunks: []model.VanadisChunk{{ID: "c1", Status: "pending", Repo: "aerial", Mode: "pr"}}, execTag: "UPDATE 1", commitErr: errors.New("commit fail")}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := tc.db.open()
			switch {
			case strings.HasPrefix(tc.name, "append chunks"):
				inputID := ""
				if tc.name == "append chunks duplicate id" {
					inputID = "c-dup"
				}
				_, err := st.AppendChunks(context.Background(), "b1", []CreateChunkInput{{ID: inputID, Mode: "action", Title: "New Chunk", Instructions: "Inst", DependsOn: []string{}}})
				assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
			case strings.HasPrefix(tc.name, "remove chunk"):
				_, _, err := st.RemoveChunk(context.Background(), "b1", "c1")
				assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
			case strings.HasPrefix(tc.name, "update chunk"):
				_, err := st.UpdateChunk(context.Background(), "b1", "c1", UpdateChunkRequest{Title: "Updated"})
				assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
			}
		})
	}
}

func TestPostgresVanadisStore_MutateGuards(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storeNil, _ := dbScript{bare: nilDB}.open()
	storeActive, _ := dbScript{execTag: "UPDATE 0"}.open()
	storeUpdateChunkNotFound, _ := dbScript{row: errRow(pgx.ErrNoRows)}.open()
	storeUpdateChunkNotRunning, _ := dbScript{row: valuesRow("completed")}.open()
	if _, err := storeNil.UpdateChunk(ctx, "b1", "c1", UpdateChunkRequest{Title: "New Title"}); err == nil {
		t.Error("expected error with nil db")
	}
	if _, err := storeActive.UpdateChunk(ctx, "b1", "c1", UpdateChunkRequest{}); err == nil {
		t.Error("expected error with empty request")
	}
	if _, err := storeUpdateChunkNotFound.UpdateChunk(ctx, "b1", "c1", UpdateChunkRequest{Title: "New Title"}); !errors.Is(err, ErrBuildNotFound) {
		t.Errorf("expected ErrBuildNotFound, got %v", err)
	}
	if _, err := storeUpdateChunkNotRunning.UpdateChunk(ctx, "b1", "c1", UpdateChunkRequest{Title: "New Title"}); !errors.Is(err, ErrBuildConflict) {
		t.Errorf("expected ErrBuildConflict, got %v", err)
	}
	if _, _, err := storeNil.RemoveChunk(ctx, "b1", "c1"); err == nil {
		t.Error("expected error with nil db")
	}
	if _, _, err := storeActive.RemoveChunk(ctx, "", "c1"); err == nil {
		t.Error("expected error with empty buildID")
	}
	if _, _, err := storeUpdateChunkNotFound.RemoveChunk(ctx, "b1", "c1"); !errors.Is(err, ErrBuildNotFound) {
		t.Errorf("expected ErrBuildNotFound, got %v", err)
	}
	if _, _, err := storeUpdateChunkNotRunning.RemoveChunk(ctx, "b1", "c1"); !errors.Is(err, ErrBuildConflict) {
		t.Errorf("expected ErrBuildConflict, got %v", err)
	}
}

func TestBuildMutateBranches(t *testing.T) {
	t.Parallel()
	strPtr := func(s string) *string { return &s }
	cycleAppend := []CreateChunkInput{
		{ID: "c2", Mode: "action", Title: "T", Instructions: "I", DependsOn: []string{"c3"}},
		{ID: "c3", Mode: "action", Title: "T", Instructions: "I", DependsOn: []string{"c2"}},
	}
	tests := []struct {
		name       string
		op         string
		db         dbScript
		req        UpdateChunkRequest
		inputs     []CreateChunkInput
		chunkRef   string
		wantSubstr string
	}{
		{name: "append nil db", op: "append", db: dbScript{bare: nilDB}, inputs: []CreateChunkInput{{Mode: "action", Title: "T", Instructions: "I", DependsOn: []string{}}}, wantSubstr: "database connection is nil"},
		{name: "append invalid input", op: "append", inputs: []CreateChunkInput{{Mode: "action", Title: "", Instructions: "I"}}, wantSubstr: "title and instructions cannot be empty"},
		{name: "append invalid id", op: "append", inputs: []CreateChunkInput{{ID: "bad id!", Mode: "action", Title: "T", Instructions: "I"}}, wantSubstr: "invalid id"},
		{name: "append dag cycle", op: "append", db: dbScript{row: valuesRow("running"), chunks: []model.VanadisChunk{{ID: "c1", Mode: "action", Status: "pending", DependsOn: []string{}}}}, inputs: cycleAppend, wantSubstr: "cycle"},
		{name: "append insert error", op: "append", db: dbScript{row: valuesRow("running"), chunks: []model.VanadisChunk{{ID: "c1", Mode: "action", Status: "pending", DependsOn: []string{}}}, failExec: "INSERT INTO vanadis_chunks", execErr: errors.New("exec fail")}, inputs: []CreateChunkInput{{Mode: "action", Title: "T", Instructions: "I", DependsOn: []string{}}}, wantSubstr: "failed to insert"},
		{name: "append lock query error", op: "append", db: dbScript{row: valuesRow("running"), queryErr: errors.New("query fail")}, inputs: []CreateChunkInput{{Mode: "action", Title: "T", Instructions: "I", DependsOn: []string{}}}, wantSubstr: "lock chunks for append"},
		{name: "append lock scan error", op: "append", db: dbScript{row: valuesRow("running"), chunks: []model.VanadisChunk{{ID: "c1", Mode: "action", Status: "pending", DependsOn: []string{}}}, scanErr: errors.New("scan fail")}, inputs: []CreateChunkInput{{Mode: "action", Title: "T", Instructions: "I", DependsOn: []string{}}}, wantSubstr: "scan fail"},
		{name: "update lock query error", op: "update", db: dbScript{row: valuesRow("running"), queryErr: errors.New("query fail")}, req: UpdateChunkRequest{Title: "T"}, wantSubstr: "lock chunks for update"},
		{name: "update lock scan error", op: "update", db: dbScript{row: valuesRow("running"), chunks: []model.VanadisChunk{{ID: "c1", Mode: "action", Status: "pending", DependsOn: []string{}}}, scanErr: errors.New("scan fail")}, req: UpdateChunkRequest{Title: "T"}, wantSubstr: "scan fail"},
		{name: "update dag cycle", op: "update", db: dbScript{row: valuesRow("running"), chunks: []model.VanadisChunk{{ID: "c1", Mode: "action", Status: "pending", DependsOn: []string{}}, {ID: "c2", Mode: "action", Status: "pending", DependsOn: []string{"c1"}}}}, req: UpdateChunkRequest{Title: "T", DependsOn: &[]string{"c2"}}, wantSubstr: "cycle"},
		{name: "update exec error", op: "update", db: dbScript{row: valuesRow("running"), chunks: []model.VanadisChunk{{ID: "c1", Mode: "pr", Repo: "aerial", Status: "pending", DependsOn: []string{}}}, failExec: "UPDATE vanadis_chunks SET mode", execErr: errors.New("exec fail")}, req: UpdateChunkRequest{Title: "T"}, wantSubstr: "execute update chunk"},
		{name: "update action with repo", op: "update", db: dbScript{row: valuesRow("running"), chunks: []model.VanadisChunk{{ID: "c1", Mode: "action", Status: "pending", DependsOn: []string{}}}}, req: UpdateChunkRequest{Mode: strPtr("action"), Repo: "aerial"}, wantSubstr: "action chunk must have empty repo"},
		{name: "remove lock query error", op: "remove", db: dbScript{row: valuesRow("running"), queryErr: errors.New("query fail")}, wantSubstr: "lock chunks"},
		{name: "remove lock scan error", op: "remove", db: dbScript{row: valuesRow("running"), chunks: []model.VanadisChunk{{ID: "c1", Mode: "action", Status: "pending", DependsOn: []string{}}, {ID: "c2", Mode: "action", Status: "pending", DependsOn: []string{}}}, scanErr: errors.New("scan fail")}, wantSubstr: "scan fail"},
		{name: "remove delete error", op: "remove", db: dbScript{row: valuesRow("running"), chunks: []model.VanadisChunk{{ID: "c1", Mode: "action", Status: "pending", DependsOn: []string{}}, {ID: "c2", Mode: "action", Status: "pending", DependsOn: []string{}}}, failExec: "DELETE FROM vanadis_chunks", execErr: errors.New("exec fail")}, wantSubstr: "delete chunk"},
		{name: "remove reindex error", op: "remove", db: dbScript{row: valuesRow("running"), chunks: []model.VanadisChunk{{ID: "c1", Mode: "action", Status: "pending", DependsOn: []string{}}, {ID: "c2", Mode: "action", Status: "pending", DependsOn: []string{}}}, execTag: "DELETE 1", failExec: "SET chunk_index", execErr: errors.New("exec fail")}, wantSubstr: "reindex chunk"},
		{name: "remove dag invalid", op: "remove", chunkRef: "c3", db: dbScript{row: valuesRow("running"), chunks: []model.VanadisChunk{{ID: "c1", Mode: "pr", Repo: "aerial", Status: "pending", TargetFiles: []string{"a.go"}, DependsOn: []string{}}, {ID: "c2", Mode: "pr", Repo: "aerial", Status: "pending", TargetFiles: []string{"a.go"}, DependsOn: []string{}}, {ID: "c3", Mode: "action", Status: "pending", DependsOn: []string{}}}}, wantSubstr: "invalid DAG"},
		{name: "update build check status error", op: "updateBuild", db: dbScript{routes: []sqlRoute{{match: "SELECT status FROM vanadis_builds", row: errRow(errors.New("boom"))}}}, wantSubstr: "check build status"},
		{name: "update build fetch error", op: "updateBuild", db: dbScript{routes: []sqlRoute{{match: "SELECT status FROM vanadis_builds", row: valuesRow("running")}, {match: "FROM vanadis_builds WHERE id = $1", row: errRow(errors.New("boom"))}}}, wantSubstr: "fetch updated build"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := tc.db.open()
			var err error
			switch tc.op {
			case "append":
				_, err = st.AppendChunks(context.Background(), "b1", tc.inputs)
			case "update":
				_, err = st.UpdateChunk(context.Background(), "b1", "c1", tc.req)
			case "remove":
				ref := tc.chunkRef
				if ref == "" {
					ref = "c1"
				}
				_, _, err = st.RemoveChunk(context.Background(), "b1", ref)
			case "updateBuild":
				_, err = st.UpdateBuild(context.Background(), "b1", UpdateBuildRequest{Title: "T"})
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantSubstr)
			}
			if !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantSubstr, err)
			}
		})
	}
}

func TestBuildMutateRelinkAndApplySuccess(t *testing.T) {
	t.Parallel()

	c := model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial", Title: "Old", Instructions: "Old", TargetFiles: []string{"old.go"}, TargetSymbols: []string{"Old"}, Effort: "low", DependsOn: []string{}}
	strPtr := func(s string) *string { return &s }
	err := applyChunkUpdates(&c, UpdateChunkRequest{
		Title: "New", Instructions: "New", TargetFiles: []string{"new.go"},
		TargetSymbols: []string{"New"}, Effort: "high", DependsOn: &[]string{},
		Repo: "aerial", Mode: strPtr("pr"),
	}, "medium")
	if err != nil {
		t.Fatalf("unexpected error on valid pr update: %v", err)
	}
	if c.Title != "New" || c.Instructions != "New" || len(c.TargetFiles) != 1 || c.TargetFiles[0] != "new.go" || len(c.TargetSymbols) != 1 || c.Effort != "medium" || c.DependsOn == nil {
		t.Fatalf("unexpected updated pr chunk: %+v", c)
	}
}
