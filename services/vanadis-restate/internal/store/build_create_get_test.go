package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/sqltest"
)

type (
	dbScript = sqltest.Script
	sqlRoute = sqltest.Route
)

// nilReceiver and nilDB are sentinel scripts open turns into a nil *Store and
// a Store without a database.
var (
	nilReceiver = dbScript{ExecTag: "nil receiver"}
	nilDB       = dbScript{ExecTag: "nil db"}
)

func open(s dbScript) (*Store, *[]string) {
	switch db, candidates := s.DB(); s.ExecTag {
	case nilReceiver.ExecTag:
		return nil, candidates
	case nilDB.ExecTag:
		return &Store{}, candidates
	default:
		return New(db, "example-org"), candidates
	}
}

func assertStoreErr(t *testing.T, err, wantErrIs error, wantErr bool) bool {
	t.Helper()
	switch {
	case wantErrIs != nil:
		if !errors.Is(err, wantErrIs) {
			t.Fatalf("err = %v, want %v", err, wantErrIs)
		}
		return true
	case wantErr:
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		return true
	case err != nil:
		t.Fatalf("unexpected error: %v", err)
	}
	return false
}

func TestPostgresVanadisStore_BuildCRUD(t *testing.T) {
	t.Parallel()
	b1 := model.VanadisBuild{ID: "b1", ThreadID: "th1", Title: "Build 1", Objective: "Obj 1"}
	c1 := []model.VanadisChunk{{ID: "c1", BuildID: "b1", Repo: "aerial", Title: "Chunk 1", Instructions: "Inst 1", DependsOn: []string{}}}

	tests := []struct {
		name      string
		db        dbScript
		wantErr   bool
		wantErrIs error
	}{
		{name: "create build success", db: dbScript{}},
		{name: "nil receiver", db: nilReceiver, wantErr: true},
		{name: "nil db", db: nilDB, wantErr: true},
		{name: "replay build read failure", db: dbScript{ExecTag: "INSERT 0 0", BuildRow: sqltest.ErrRow(errors.New("read failed"))}, wantErr: true},
		{name: "replay chunks read failure", db: dbScript{ExecTag: "INSERT 0 0", BuildRow: &sqltest.Row{Build: &b1}, QueryErr: errors.New("read failed")}, wantErr: true},
		{name: "replay chunks scan failure", db: dbScript{ExecTag: "INSERT 0 0", BuildRow: &sqltest.Row{Build: &b1}, Chunks: c1, ScanErr: errors.New("scan failed")}, wantErr: true},
		{name: "identical committed replay", db: dbScript{ExecTag: "INSERT 0 0", BuildRow: &sqltest.Row{Build: &b1}, Chunks: c1}},
		{name: "evolved committed replay", db: dbScript{ExecTag: "INSERT 0 0", BuildRow: &sqltest.Row{Build: &model.VanadisBuild{ID: "b1", ThreadID: "th1", Title: "Build 1", Objective: "Obj 1", Status: "running", StatusMessageID: "widget"}}, Chunks: c1}},
		{name: "different objective replay", db: dbScript{ExecTag: "INSERT 0 0", BuildRow: &sqltest.Row{Build: &model.VanadisBuild{ID: "b1", ThreadID: "th1", Title: "Build 1", Objective: "different"}}, Chunks: c1}, wantErrIs: ErrBuildConflict},
		{name: "partial replay", db: dbScript{ExecTag: "INSERT 0 0", BuildRow: &sqltest.Row{Build: &b1}, Chunks: nil}, wantErrIs: ErrBuildConflict},
		{name: "different chunk replay", db: dbScript{ExecTag: "INSERT 0 0", BuildRow: &sqltest.Row{Build: &b1}, Chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b1", Repo: "aerial", Title: "changed", DependsOn: []string{}}}}, wantErrIs: ErrBuildConflict},
		{name: "chunk id conflict", db: dbScript{FailExec: "INSERT INTO vanadis_chunks", ExecErr: fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "23505", ConstraintName: "vanadis_chunks_pkey", Detail: "private"})}, wantErrIs: ErrBuildConflict},
		{name: "chunk index conflict", db: dbScript{FailExec: "INSERT INTO vanadis_chunks", ExecErr: fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "23505", ConstraintName: "uq_vanadis_chunks_build_index", Detail: "private"})}, wantErrIs: ErrBuildConflict},
		{name: "unknown unique", db: dbScript{FailExec: "INSERT INTO vanadis_chunks", ExecErr: fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "23505", ConstraintName: "foreign_constraint", Detail: "private"})}, wantErr: true},
		{name: "deadlock", db: dbScript{FailExec: "INSERT INTO vanadis_chunks", ExecErr: fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "40P01", ConstraintName: "vanadis_chunks_pkey", Detail: "private"})}, wantErr: true},
		{name: "create build begin error", db: dbScript{BeginErr: errors.New("begin error")}, wantErr: true},
		{name: "create build exec build error", db: dbScript{FailExec: "INSERT INTO vanadis_builds", ExecErr: errors.New("exec build error")}, wantErr: true},
		{name: "create build exec chunk error", db: dbScript{FailExec: "INSERT INTO vanadis_chunks", ExecErr: errors.New("exec chunk error")}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := open(tc.db)
			err := st.CreateBuild(context.Background(), b1, c1)
			assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
		})
	}
}

// createConflictTx models atomic visibility at the database boundary:
// inserts remain private until Commit and disappear on Rollback.
type createConflictTx struct {
	sqltest.Tx

	pending []string
	durable []string
	failure error
}

func (tx *createConflictTx) Exec(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	if strings.Contains(query, "INSERT INTO vanadis_chunks") {
		return pgconn.CommandTag{}, tx.failure
	}
	id, ok := args[0].(string)
	if !ok {
		return pgconn.CommandTag{}, errors.New("expected build ID")
	}
	tx.pending = append(tx.pending, id)
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (tx *createConflictTx) Commit(context.Context) error {
	tx.durable = append(tx.durable, tx.pending...)
	tx.pending = nil
	return nil
}

func (tx *createConflictTx) Rollback(context.Context) error {
	tx.pending = nil
	return nil
}

// TestPostgresVanadisStore_CreateBuildConflictAtomicity pins rollback on a
// chunk unique conflict: the build insert never becomes durable.
func TestPostgresVanadisStore_CreateBuildConflictAtomicity(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, constraint string }{
		{"chunk ID", "vanadis_chunks_pkey"},
		{"chunk index", "uq_vanadis_chunks_build_index"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tx := &createConflictTx{failure: &pgconn.PgError{Code: "23505", ConstraintName: tc.constraint}}
			st := New(sqltest.DB{BeginFn: func(context.Context) (pgx.Tx, error) { return tx, nil }}, "example-org")
			err := st.CreateBuild(context.Background(), model.VanadisBuild{ID: "b1", ThreadID: "th1", Objective: "Obj"}, []model.VanadisChunk{{ID: "c1", Title: "Chunk", Instructions: "Work", Repo: "aerial", DependsOn: []string{}}})
			if !errors.Is(err, ErrBuildConflict) {
				t.Fatalf("err = %v, want %v", err, ErrBuildConflict)
			}
			if len(tx.durable) != 0 || len(tx.pending) != 0 {
				t.Fatalf("durable=%v pending=%v, want both empty", tx.durable, tx.pending)
			}
		})
	}
}

func TestPostgresVanadisStore_GetBuild(t *testing.T) {
	t.Parallel()
	b1 := model.VanadisBuild{ID: "b1", ThreadID: "th1", Title: "Build 1"}
	tests := []struct {
		name      string
		buildID   string
		db        dbScript
		wantErr   bool
		wantErrIs error
	}{
		{name: "nil receiver", buildID: "b1", db: nilReceiver, wantErr: true},
		{name: "empty id", buildID: "  ", wantErr: true},
		{name: "get build not found", buildID: "b1", db: dbScript{Row: sqltest.ErrRow(pgx.ErrNoRows)}, wantErrIs: ErrBuildNotFound},
		{name: "get build query error", buildID: "b1", db: dbScript{Row: sqltest.ErrRow(errors.New("db error"))}, wantErr: true},
		{name: "get build chunks query error", buildID: "b1", db: dbScript{Row: &sqltest.Row{Build: &b1}, QueryErr: errors.New("chunks err")}, wantErr: true},
		{name: "get build chunks scan error", buildID: "b1", db: dbScript{Row: &sqltest.Row{Build: &b1}, Chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", Repo: "aerial"}}, ScanErr: errors.New("scan failed")}, wantErr: true},
		{name: "get build success", buildID: "b1", db: dbScript{
			Row: &sqltest.Row{Build: &b1},
			Chunks: []model.VanadisChunk{{
				ID: "c1", BuildID: "b1", Title: "C1", Status: "running", Mode: "pr", Repo: "aerial",
				StartedAt: time.Now(), CompletedAt: time.Now(),
				TargetFiles: []string{"f.go"}, TargetSymbols: []string{"S"}, AcceptanceCriteria: []string{"AC"},
				DependsOn: []string{"dep"}, Effort: "medium",
			}},
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := open(tc.db)
			_, _, err := st.GetBuild(context.Background(), tc.buildID)
			assertStoreErr(t, err, tc.wantErrIs, tc.wantErr)
		})
	}
}
