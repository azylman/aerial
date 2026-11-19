package build

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// observeJournalCallback inspects the callback error before the SDK mock adapter
// terminates execution. It does not simulate Restate scheduling or retries.
func observeJournalCallback(t *testing.T, mc *mocks.MockContext, want error, code restate.Code) {
	t.Helper()
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(fn func(restate.RunContext) (any, error), output any, opts ...restate.RunOption) restate.TerminalError {
		res, err := fn(mockRunContext{Context: context.Background()})
		if code != 0 {
			var terminal restate.TerminalError
			require.ErrorAs(t, err, &terminal)
			assert.Equal(t, code, terminal.Code())
		} else {
			assert.False(t, restate.IsTerminalError(err))
		}
		switch {
		case want != nil && code != 0:
			assert.EqualError(t, err, want.Error())
		case want != nil:
			assert.ErrorIs(t, err, want)
		default:
			assert.NoError(t, err)
		}
		// Only adapt after observing the raw callback error above.
		return runMockRunContext(func(restate.RunContext) (any, error) { return res, err }, output, opts...)
	})
}

func TestRunJournaledCallback(t *testing.T) {
	t.Parallel()
	transient := errors.New("storage unavailable")
	terminal := restate.ToTerminalError(errors.New("already terminal"), restate.WithErrorCode(422))
	tests := []struct {
		name string
		err  error
		code restate.Code
	}{
		{"wrapped invalid", fmt.Errorf("validation: %w", store.ErrInvalidBuildInput), 400},
		{"missing build", store.ErrBuildNotFound, 404},
		{"missing chunk", ErrChunkNotFound, 404},
		{"conflict", store.ErrBuildConflict, 409},
		{"already terminal", terminal, 422},
		{"transient", transient, 0},
		{"success", nil, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc := mocks.NewMockContext(t)
			observeJournalCallback(t, mc, tc.err, tc.code)
			got, err := RunJournaled(restate.WithMockContext(mc), labelUpdateBuild, "update-build-postgres", func(restate.RunContext) (string, error) {
				return "payload", tc.err
			})
			if tc.err == nil {
				require.NoError(t, err)
				assert.Equal(t, "payload", got)
			}
		})
	}
}

type metadataMutationStore struct {
	store.VanadisStore

	err error
}

func (s *metadataMutationStore) UpdateBuild(_ context.Context, id string, req store.UpdateBuildRequest) (*model.VanadisBuild, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &model.VanadisBuild{ID: id, Title: req.Title}, nil
}

//nolint:tparallel // Requests must run in order against the same object after a conflict.
func TestUpdateBuildConflictThenValid(t *testing.T) {
	t.Parallel()
	st := &metadataMutationStore{err: store.ErrBuildConflict}
	bo := NewBuildObject("", nil).WithAuthority(st, nil)
	rt := newFakeObjectRuntime("build")
	bo.runtimeFactory = func(restate.ObjectContext) objectRuntime { return rt }
	tests := []struct {
		name string
		err  error
		code restate.Code
	}{
		{"conflict", store.ErrBuildConflict, 409},
		{"valid", nil, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st.err = tc.err
			mc := mocks.NewMockContext(t)
			mc.EXPECT().Key().Return("build")
			observeJournalCallback(t, mc, tc.err, tc.code)
			got, err := bo.UpdateBuild(restate.WithMockContext(mc), store.UpdateBuildRequest{Title: "Updated"})
			if tc.err == nil {
				require.NoError(t, err)
				assert.Equal(t, model.VanadisBuild{ID: "build", Title: "Updated"}, got)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func (s *metadataMutationStore) LookupChunkBuildID(context.Context, string) (string, error) {
	return "", s.err
}

func (s *metadataMutationStore) LookupPRChunk(context.Context, string, int) (string, string, error) {
	return "", "", s.err
}

func TestCreateBuildJournalCallback(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		code       string
		constraint string
		terminal   restate.Code
	}{
		{"chunk ID", "23505", "vanadis_chunks_pkey", 409},
		{"chunk index", "23505", "uq_vanadis_chunks_build_index", 409},
		{"unknown unique", "23505", "unknown", 0},
		{"serialization", "40001", "vanadis_chunks_pkey", 0},
		{"deadlock", "40P01", "", 0},
		{"connection", "08006", "", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dbErr := fmt.Errorf("database: %w", &pgconn.PgError{Code: tc.code, ConstraintName: tc.constraint, Detail: "private database details"})
			st, _ := (dbScript{failExec: "INSERT INTO vanadis_chunks", execErr: dbErr}).open()
			bo := NewBuildObject("", nil).WithAuthority(st, nil)
			mc := mocks.NewMockContext(t)
			want := dbErr
			if tc.terminal != 0 {
				want = fmt.Errorf("%w: chunk %q in build %q already exists", store.ErrBuildConflict, "c1", "b1")
			}
			observeJournalCallback(t, mc, want, tc.terminal)
			_, _ = RunJournaled(restate.WithMockContext(mc), labelCreate, "create-build-postgres", func(rc restate.RunContext) (bool, error) {
				return bo.persistBuildCreation(rc, model.VanadisBuild{ID: "b1", ThreadID: "th1", Objective: "Obj"}, []model.VanadisChunk{{ID: "c1", Title: "Chunk", Instructions: "Do work", Repo: "aerial", DependsOn: []string{}}})
			})
		})
	}
}

func TestCreateBuildCommitAcknowledgementReplay(t *testing.T) {
	t.Parallel()
	lostAck := errors.New("connection lost after commit")
	build := model.VanadisBuild{ID: "b1", ThreadID: "th1", Objective: "Obj"}
	chunks := []model.VanadisChunk{{ID: "c1", BuildID: "b1", Title: "Chunk", Instructions: "Do work", Repo: "aerial", DependsOn: []string{}}}
	tests := []struct {
		name string
		db   dbScript
		want error
	}{
		{"acknowledgement lost", dbScript{commitErr: lostAck}, lostAck},
		{"committed request replay", dbScript{execTag: "INSERT 0 0", buildRow: &mockRow{b: &build}, chunks: chunks}, nil},
	}
	// The second database fixture represents the durable state after the first
	// commit succeeded at PostgreSQL but its response failed at the connection.
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, _ := tc.db.open()
			bo := NewBuildObject("", nil).WithAuthority(st, nil)
			accepted, err := bo.persistBuildCreation(mockRunContext{Context: context.Background()}, build, chunks)
			assert.ErrorIs(t, err, tc.want)
			assert.Equal(t, tc.want == nil, accepted)
		})
	}
}

// creationTransaction models atomic visibility at the database boundary:
// inserts remain private until Commit and disappear on Rollback.
type creationTransaction struct {
	mockTx

	pending []string
	durable []string
	failure error
}

func (tx *creationTransaction) Exec(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
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

func (tx *creationTransaction) Commit(context.Context) error {
	tx.durable = append(tx.durable, tx.pending...)
	tx.pending = nil
	return nil
}

func (tx *creationTransaction) Rollback(context.Context) error {
	tx.pending = nil
	return nil
}

func TestCreateBuildConflictAtomicity(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, constraint string }{
		{"chunk ID", "vanadis_chunks_pkey"},
		{"chunk index", "uq_vanadis_chunks_build_index"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tx := &creationTransaction{failure: &pgconn.PgError{Code: "23505", ConstraintName: tc.constraint}}
			st := newMockStore(mockDB{beginFn: func(context.Context) (pgx.Tx, error) { return tx, nil }})
			err := st.CreateBuild(context.Background(), model.VanadisBuild{ID: "b1", ThreadID: "th1", Objective: "Obj"}, []model.VanadisChunk{{ID: "c1", Title: "Chunk", Instructions: "Work", Repo: "aerial", DependsOn: []string{}}})
			require.ErrorIs(t, err, store.ErrBuildConflict)
			assert.Empty(t, tx.durable)
			assert.Empty(t, tx.pending)
		})
	}
}

func TestPlanInputsToChunks_JournaledClockIdentity(t *testing.T) {
	t.Parallel()
	fixed := time.Date(2026, time.November, 19, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		buildID string
		chunkID string
		title   string
	}{
		{name: "first chunk timestamp identity", buildID: "b1", chunkID: "c1", title: "Chunk One"},
		{name: "second chunk timestamp identity", buildID: "b1", chunkID: "c2", title: "Chunk Two"},
		{name: "other build timestamp identity", buildID: "b2", chunkID: "c1", title: "Chunk One"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inputs := []store.CreateChunkInput{{ID: tc.chunkID, Title: tc.title, Repo: "aerial", Instructions: "Do work"}}
			first := planInputsToChunks(tc.buildID, inputs, fixed)
			second := planInputsToChunks(tc.buildID, inputs, fixed)
			require.Len(t, first, 1)
			require.Len(t, second, 1)
			assert.Equal(t, tc.chunkID, first[0].ID)
			assert.Equal(t, fixed, first[0].CreatedAt)
			assert.Equal(t, fixed, first[0].UpdatedAt)
			assert.Equal(t, first[0].CreatedAt, second[0].CreatedAt)
			assert.Equal(t, first[0].UpdatedAt, second[0].UpdatedAt)
		})
	}
}
