package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

func TestInsertBuildChunkCases(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	tests := []struct {
		name  string
		chunk model.VanadisChunk
		// seedChunks are stored (in b0 or b1) before the insert runs.
		seedChunks    []model.VanadisChunk
		setup         string
		wantErrIs     error
		wantErr       bool
		wantErrSub    string
		wantStatus    string
		wantStarted   bool
		wantCompleted bool
	}{
		{name: "invalid mode", chunk: model.VanadisChunk{ID: "c1", Mode: "bogus"}, wantErr: true},
		{name: "pr requires repo", chunk: model.VanadisChunk{ID: "c1", Mode: "pr"}, wantErr: true, wantErrSub: "repo is required for pr mode"},
		{name: "action rejects repo", chunk: model.VanadisChunk{ID: "c1", Mode: "action", Repo: "aerial"}, wantErr: true, wantErrSub: "action chunk must have empty repo"},
		{name: "action rejects target files", chunk: model.VanadisChunk{ID: "c1", Mode: "action", TargetFiles: []string{"f.go"}}, wantErr: true, wantErrSub: "action chunk must not specify target files or symbols"},
		{name: "action rejects target symbols", chunk: model.VanadisChunk{ID: "c1", Mode: "action", TargetSymbols: []string{"S"}}, wantErr: true},
		{name: "invalid effort", chunk: model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial", Effort: "ultra"}, wantErr: true, wantErrSub: "invalid chunk effort"},
		{name: "pr success", chunk: model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial", Status: "pending", Effort: "medium"}, wantStatus: "pending"},
		{name: "action running success", chunk: model.VanadisChunk{ID: "c2", Mode: "action", Status: "running"}, wantStatus: "running", wantStarted: true},
		{name: "terminal status success", chunk: model.VanadisChunk{ID: "c3", Mode: "pr", Repo: "aerial", Status: "completed"}, wantStatus: "completed", wantCompleted: true},
		{name: "explicit times success", chunk: model.VanadisChunk{ID: "c4", Mode: "pr", Repo: "aerial", Status: "running", PRNumber: 7, StartedAt: now, CompletedAt: now}, wantStatus: "running", wantStarted: true, wantCompleted: true},
		{name: "chunk id conflict", chunk: model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial"}, seedChunks: []model.VanadisChunk{{ID: "c1", BuildID: "b0"}}, wantErrIs: ErrBuildConflict},
		{name: "chunk index conflict", chunk: model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial"}, seedChunks: []model.VanadisChunk{{ID: "other", BuildID: "b1"}}, wantErrIs: ErrBuildConflict},
		{name: "unknown unique constraint", chunk: model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial"}, seedChunks: []model.VanadisChunk{{ID: "c0", BuildID: "b0"}}, setup: cgUnknownUniqueSQL, wantErr: true},
		{name: "non-unique pg error", chunk: model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial"}, setup: cgDeadlockSQL, wantErr: true},
		{name: "generic exec error", chunk: model.VanadisChunk{ID: "c1", Mode: "pr", Repo: "aerial"}, setup: failStatement("vanadis_chunks", "INSERT"), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, pool := openStore(t)
			seed(t, pool, []model.VanadisBuild{cgOtherBuild, {ID: "b1", ThreadID: "th1"}}, tc.seedChunks)
			mustExec(t, pool, tc.setup)
			ctx := context.Background()
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			err = InsertBuildChunk(ctx, tx, "b1", tc.chunk, InsertChunkSQL)
			if assertStoreErr(t, err, tc.wantErrIs, tc.wantErr) {
				if tc.wantErrIs == nil && errors.Is(err, ErrBuildConflict) {
					t.Errorf("err = %v, want a non-conflict failure", err)
				}
				if !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("err = %v, want containing %q", err, tc.wantErrSub)
				}
				return
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatalf("commit: %v", err)
			}
			got := loadChunk(t, pool, tc.chunk.ID)
			if got.BuildID != "b1" || got.Status != tc.wantStatus || got.PRNumber != tc.chunk.PRNumber {
				t.Errorf("stored chunk = %+v, want build b1 status %q pr %d", got, tc.wantStatus, tc.chunk.PRNumber)
			}
			if !got.StartedAt.IsZero() != tc.wantStarted || !got.CompletedAt.IsZero() != tc.wantCompleted {
				t.Errorf("stored started=%v completed=%v, want started %v completed %v", got.StartedAt, got.CompletedAt, tc.wantStarted, tc.wantCompleted)
			}
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
	defaultAgent := base
	defaultAgent.Agent = " Aerial "
	badMode := chunk("c1", "T", "bogus")
	renamed := chunk("c9", "T", "pr")
	changed := chunk("c1", "Changed", "pr")
	badEffort := chunk("c1", "T", "pr")
	badEffort.Effort = "ultra"
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
		{name: "empty and default agent equal", a: base, ac: good, b: defaultAgent, bc: good, want: true},
		{name: "different count", a: base, ac: good, b: base, bc: append(append([]model.VanadisChunk{}, good...), chunk("c2", "T", "pr"))},
		{name: "changed chunk", a: base, ac: good, b: base, bc: []model.VanadisChunk{changed}},
		{name: "renamed chunk", a: base, ac: good, b: base, bc: []model.VanadisChunk{renamed}},
		{name: "invalid mode", a: base, ac: good, b: base, bc: []model.VanadisChunk{badMode}},
		{name: "invalid effort on both sides", a: base, ac: []model.VanadisChunk{badEffort}, b: base, bc: []model.VanadisChunk{badEffort}},
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
