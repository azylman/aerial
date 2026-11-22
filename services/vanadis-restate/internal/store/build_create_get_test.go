package store

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// cgSkipBuildInsertSQL makes every build INSERT affect zero rows without a
// stored row, as an ON CONFLICT race whose winner vanished would.
const cgSkipBuildInsertSQL = `CREATE FUNCTION pgtest_skip() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
CREATE TRIGGER pgtest_skip BEFORE INSERT ON vanadis_builds FOR EACH ROW EXECUTE FUNCTION pgtest_skip();`

// cgIndexSquatterSQL inserts a chunk at index 0 of every new build inside
// the creating transaction, so the request's first chunk hits
// uq_vanadis_chunks_build_index.
const cgIndexSquatterSQL = `CREATE FUNCTION pgtest_squat() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	INSERT INTO vanadis_chunks (id, build_id, chunk_index, mode, repo, title, instructions) VALUES ('squatter', NEW.id, 0, 'pr', 'aerial', 'S', 'S');
	RETURN NULL;
END $$;
CREATE TRIGGER pgtest_squat AFTER INSERT ON vanadis_builds FOR EACH ROW EXECUTE FUNCTION pgtest_squat();`

// cgUnknownUniqueSQL adds a unique index build creation does not own.
const cgUnknownUniqueSQL = `CREATE UNIQUE INDEX pgtest_unknown_unique ON vanadis_chunks (title);`

// cgDeadlockSQL makes chunk inserts fail with a non-unique SQLSTATE that
// still names a creation-owned constraint.
const cgDeadlockSQL = `CREATE FUNCTION pgtest_deadlock() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE EXCEPTION 'deadlock detected' USING ERRCODE = '40P01', CONSTRAINT = 'vanadis_chunks_pkey';
END $$;
CREATE TRIGGER pgtest_deadlock BEFORE INSERT ON vanadis_chunks FOR EACH ROW EXECUTE FUNCTION pgtest_deadlock();`

// cgBreakChunkPrepareSQL renames a selected chunk column so chunk reads fail
// to prepare while build reads keep working.
const cgBreakChunkPrepareSQL = `ALTER TABLE vanadis_chunks RENAME COLUMN banner TO banner_gone;`

// cgBreakBuildPrepareSQL renames a selected build column so build reads fail.
const cgBreakBuildPrepareSQL = `ALTER TABLE vanadis_builds RENAME COLUMN quiet TO quiet_gone;`

// cgOtherBuild owns rows that collide with the build under test.
var cgOtherBuild = model.VanadisBuild{ID: "b0", ThreadID: "th0"}

// cgStore opens a private database unless the row asks for a nil receiver or
// a Store without a database.
func cgStore(t *testing.T, nilReceiver, nilDB bool) (*Store, *pgxpool.Pool) {
	t.Helper()
	st, pool := openStore(t)
	switch {
	case nilReceiver:
		return nil, pool
	case nilDB:
		return &Store{}, pool
	}
	return st, pool
}

// cgStoredChunkIDs lists the chunk IDs stored for buildID in index order;
// a missing build yields nil.
func cgStoredChunkIDs(t *testing.T, pool *pgxpool.Pool, buildID string) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT id FROM vanadis_chunks WHERE build_id = $1 ORDER BY chunk_index`, buildID)
	if err != nil {
		t.Fatalf("list chunks: %v", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan chunk id: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate chunk ids: %v", err)
	}
	return ids
}

// cgBuildExists reports whether a build row with id is stored.
func cgBuildExists(t *testing.T, pool *pgxpool.Pool, id string) bool {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(context.Background(), `SELECT EXISTS (SELECT 1 FROM vanadis_builds WHERE id = $1)`, id).Scan(&exists); err != nil {
		t.Fatalf("check build: %v", err)
	}
	return exists
}

func TestPostgresVanadisStore_BuildCRUD(t *testing.T) {
	t.Parallel()
	b1 := model.VanadisBuild{ID: "b1", ThreadID: "th1", Title: "Build 1", Objective: "Obj 1"}
	c1 := []model.VanadisChunk{{ID: "c1", BuildID: "b1", Repo: "aerial", Title: "Chunk 1", Instructions: "Inst 1", DependsOn: []string{}}}

	tests := []struct {
		name        string
		nilReceiver bool
		nilDB       bool
		closed      bool
		build       model.VanadisBuild
		// chunks replaces the c1 request chunks when set.
		chunks []model.VanadisChunk
		// precreate commits b1/c1 through CreateBuild before setup runs.
		precreate bool
		// collide seeds b0 with chunk c1 so the request's chunk ID collides.
		collide       bool
		setup         string
		wantErr       bool
		wantErrIs     error
		wantObjective string
		wantChunks    []string
	}{
		{name: "create build success", build: b1, wantObjective: "Obj 1", wantChunks: []string{"c1"}},
		{name: "nil receiver", nilReceiver: true, build: b1, wantErr: true},
		{name: "nil db", nilDB: true, build: b1, wantErr: true},
		{name: "invalid input", build: model.VanadisBuild{ID: "b1", Objective: "Obj 1"}, wantErr: true},
		{name: "invalid dag", build: b1, chunks: []model.VanadisChunk{{ID: "c1", Repo: "aerial", Title: "Chunk 1", Instructions: "Inst 1", DependsOn: []string{"c2"}}, {ID: "c2", Repo: "aerial", Title: "Chunk 2", Instructions: "Inst 2", DependsOn: []string{"c1"}}}, wantErr: true},
		{name: "replay build read failure", build: b1, setup: cgSkipBuildInsertSQL, wantErr: true},
		{name: "replay chunks read failure", build: b1, precreate: true, setup: cgBreakChunkPrepareSQL, wantErr: true},
		{name: "replay chunks scan failure", build: b1, precreate: true, setup: breakChunkScanSQL, wantErr: true},
		{name: "identical committed replay", build: b1, precreate: true, wantObjective: "Obj 1", wantChunks: []string{"c1"}},
		{
			name: "evolved committed replay", build: b1, precreate: true, setup: `UPDATE vanadis_builds SET status = 'running', status_message_id = 'widget'`,
			wantObjective: "Obj 1", wantChunks: []string{"c1"},
		},
		{name: "different objective replay", build: b1, precreate: true, setup: `UPDATE vanadis_builds SET objective = 'different'`, wantErrIs: ErrBuildConflict},
		{name: "partial replay", build: b1, precreate: true, setup: `DELETE FROM vanadis_chunks`, wantErrIs: ErrBuildConflict},
		{name: "different chunk replay", build: b1, precreate: true, setup: `UPDATE vanadis_chunks SET title = 'changed'`, wantErrIs: ErrBuildConflict},
		{name: "chunk id conflict", build: b1, collide: true, wantErrIs: ErrBuildConflict},
		{name: "chunk index conflict", build: b1, setup: cgIndexSquatterSQL, wantErrIs: ErrBuildConflict},
		{name: "unknown unique", build: b1, setup: cgUnknownUniqueSQL + ` INSERT INTO vanadis_builds (id, thread_id, objective) VALUES ('b0', 'th0', 'O'); INSERT INTO vanadis_chunks (id, build_id, chunk_index, repo, title, instructions) VALUES ('c0', 'b0', 0, 'aerial', 'Chunk 1', 'I');`, wantErr: true},
		{name: "deadlock", build: b1, setup: cgDeadlockSQL, wantErr: true},
		{name: "create build begin error", build: b1, closed: true, wantErr: true},
		{name: "create build exec build error", build: b1, setup: failStatement("vanadis_builds", "INSERT"), wantErr: true},
		{name: "create build exec chunk error", build: b1, setup: failStatement("vanadis_chunks", "INSERT"), wantErr: true},
		{name: "create build commit error", build: b1, setup: failCommit("vanadis_builds", "INSERT"), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := cgStore(t, tc.nilReceiver, tc.nilDB)
			cgArrange(t, st, pool, b1, c1, tc.precreate, tc.collide, tc.setup, tc.closed)
			chunks := c1
			if tc.chunks != nil {
				chunks = tc.chunks
			}
			err := st.CreateBuild(context.Background(), tc.build, chunks)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				cgAssertCreateFailure(t, pool, err, tc.wantErrIs == nil, !tc.precreate && !tc.closed)
				return
			}
			if got := loadBuild(t, pool, "b1"); got.Objective != tc.wantObjective || got.Title != "Build 1" || got.ThreadID != "th1" {
				t.Errorf("stored build = %+v, want objective %q title Build 1 thread th1", got, tc.wantObjective)
			}
			if got := cgStoredChunkIDs(t, pool, "b1"); !slices.Equal(got, tc.wantChunks) {
				t.Errorf("stored chunks = %v, want %v", got, tc.wantChunks)
			}
		})
	}
}

// cgArrange prepares a BuildCRUD row's database: an optional committed
// b1/c1, a colliding chunk c1 in b0, setup SQL, and an optionally closed pool.
func cgArrange(t *testing.T, st *Store, pool *pgxpool.Pool, b model.VanadisBuild, chunks []model.VanadisChunk, precreate, collide bool, setup string, closed bool) {
	t.Helper()
	if precreate {
		if err := st.CreateBuild(context.Background(), b, chunks); err != nil {
			t.Fatalf("precreate: %v", err)
		}
	}
	if collide {
		seed(t, pool, []model.VanadisBuild{cgOtherBuild}, []model.VanadisChunk{{ID: "c1", BuildID: "b0"}})
	}
	mustExec(t, pool, setup)
	if closed {
		pool.Close()
	}
}

// cgAssertCreateFailure checks a failed CreateBuild: a plain failure must not
// be classified as a conflict, and a rolled-back creation leaves no b1.
func cgAssertCreateFailure(t *testing.T, pool *pgxpool.Pool, err error, wantPlain, wantRolledBack bool) {
	t.Helper()
	if wantPlain && errors.Is(err, ErrBuildConflict) {
		t.Errorf("err = %v, want a non-conflict failure", err)
	}
	if wantRolledBack && cgBuildExists(t, pool, "b1") {
		t.Error("failed creation left build b1 stored, want rolled back")
	}
}

// TestPostgresVanadisStore_CreateBuildConflictAtomicity pins rollback on a
// chunk unique conflict: the build insert never becomes durable.
func TestPostgresVanadisStore_CreateBuildConflictAtomicity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		collide bool
		setup   string
	}{
		{name: "chunk ID", collide: true},
		{name: "chunk index", setup: cgIndexSquatterSQL},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := openStore(t)
			if tc.collide {
				seed(t, pool, []model.VanadisBuild{cgOtherBuild}, []model.VanadisChunk{{ID: "c1", BuildID: "b0"}})
			}
			mustExec(t, pool, tc.setup)
			err := st.CreateBuild(context.Background(), model.VanadisBuild{ID: "b1", ThreadID: "th1", Objective: "Obj"}, []model.VanadisChunk{{ID: "c1", Title: "Chunk", Instructions: "Work", Repo: "aerial", DependsOn: []string{}}})
			assertStoreErr(t, err, ErrBuildConflict, false)
			if cgBuildExists(t, pool, "b1") {
				t.Fatal("build b1 is durable after the chunk conflict, want rolled back")
			}
			if got := cgStoredChunkIDs(t, pool, "b1"); got != nil {
				t.Fatalf("chunks for b1 = %v, want none", got)
			}
		})
	}
}

func TestPostgresVanadisStore_GetBuild(t *testing.T) {
	t.Parallel()
	stamp := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	b1 := model.VanadisBuild{ID: "b1", ThreadID: "th1", Title: "Build 1"}
	full := model.VanadisChunk{
		ID: "c1", BuildID: "b1", Title: "C1", Status: "running", Mode: "pr", Repo: "aerial",
		StartedAt: stamp, CompletedAt: stamp,
		TargetFiles: []string{"f.go"}, TargetSymbols: []string{"S"}, AcceptanceCriteria: []string{"AC"},
		DependsOn: []string{"dep"}, Effort: "medium",
	}
	boom := model.VanadisChunk{ID: "c1", BuildID: "b1", Banner: "boom"}
	tests := []struct {
		name        string
		nilReceiver bool
		buildID     string
		builds      []model.VanadisBuild
		chunks      []model.VanadisChunk
		sabotage    string
		wantErr     bool
		wantErrIs   error
		wantChunk   model.VanadisChunk
	}{
		{name: "nil receiver", nilReceiver: true, buildID: "b1", wantErr: true},
		{name: "empty id", buildID: "  ", wantErr: true},
		{name: "get build not found", buildID: "b1", wantErrIs: ErrBuildNotFound},
		{name: "get build query error", buildID: "b1", builds: []model.VanadisBuild{b1}, sabotage: cgBreakBuildPrepareSQL, wantErr: true},
		{name: "get build chunks query error", buildID: "b1", builds: []model.VanadisBuild{b1}, sabotage: cgBreakChunkPrepareSQL, wantErr: true},
		{name: "get build chunks scan error", buildID: "b1", builds: []model.VanadisBuild{b1}, chunks: []model.VanadisChunk{full}, sabotage: breakChunkScanSQL, wantErr: true},
		{name: "get build chunks rows error", buildID: "b1", builds: []model.VanadisBuild{b1}, chunks: []model.VanadisChunk{boom}, sabotage: failChunkRowsSQL, wantErr: true},
		{
			name: "get build success", buildID: " b1 ", builds: []model.VanadisBuild{b1}, chunks: []model.VanadisChunk{full},
			wantChunk: model.VanadisChunk{
				ID: "c1", BuildID: "b1", Title: "C1", Status: "running", Mode: "pr", Repo: "aerial",
				StartedAt: stamp, CompletedAt: stamp,
				TargetFiles: []string{"f.go"}, TargetSymbols: []string{"S"}, AcceptanceCriteria: []string{"AC"},
				DependsOn: []string{"dep"}, Effort: "medium",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := cgStore(t, tc.nilReceiver, false)
			seed(t, pool, tc.builds, tc.chunks)
			mustExec(t, pool, tc.sabotage)
			b, chunks, err := st.GetBuild(context.Background(), tc.buildID)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				return
			}
			if b.ID != "b1" || b.ThreadID != "th1" || b.Title != "Build 1" || b.Status != "running" || b.Agent != "aerial" {
				t.Errorf("GetBuild build = %+v", b)
			}
			if len(chunks) != 1 {
				t.Fatalf("GetBuild chunks = %+v, want one", chunks)
			}
			got := chunks[0]
			got.StartedAt, got.CompletedAt = got.StartedAt.UTC(), got.CompletedAt.UTC()
			got.CreatedAt, got.UpdatedAt = time.Time{}, time.Time{}
			if !cgChunkEqual(got, tc.wantChunk) {
				t.Errorf("GetBuild chunk = %+v\nwant %+v", got, tc.wantChunk)
			}
		})
	}
}

// cgChunkEqual compares chunks field by field (slices by value).
func cgChunkEqual(a, b model.VanadisChunk) bool {
	return a.ID == b.ID && a.BuildID == b.BuildID && a.ChunkIndex == b.ChunkIndex && a.Mode == b.Mode && a.Repo == b.Repo &&
		a.Title == b.Title && a.Status == b.Status && a.Effort == b.Effort && a.PRNumber == b.PRNumber &&
		a.StartedAt.Equal(b.StartedAt) && a.CompletedAt.Equal(b.CompletedAt) &&
		slices.Equal(a.TargetFiles, b.TargetFiles) && slices.Equal(a.TargetSymbols, b.TargetSymbols) &&
		slices.Equal(a.AcceptanceCriteria, b.AcceptanceCriteria) && slices.Equal(a.DependsOn, b.DependsOn)
}
