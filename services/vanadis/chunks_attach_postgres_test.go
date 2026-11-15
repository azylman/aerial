package main

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestVanadisStore_AttachChunkPR_ExistingErrors_MockDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s := newMockStore(mockDB{})
	// Invalid args
	if _, _, _, err := s.AttachChunkPR(ctx, AttachChunkPRRequest{ChunkID: "", PRNumber: 0, Repo: "aerial"}); err == nil {
		t.Error("expected error on empty chunk ID and zero pr number")
	}

	// No rows: chunk does not exist
	sNotExist := newMockStore(mockDB{
		queryRowFn: func(_ context.Context, sql string, _ ...any) pgx.Row {
			if strings.Contains(sql, "SELECT mode") {
				return &mockRow{err: pgx.ErrNoRows}
			}
			if strings.Contains(sql, "SELECT EXISTS") {
				return &genericMockRow{scanFn: func(dest ...any) error {
					*dest[0].(*bool) = false
					return nil
				}}
			}
			return &mockRow{err: pgx.ErrNoRows}
		},
	})
	if _, _, _, err := sNotExist.AttachChunkPR(ctx, AttachChunkPRRequest{ChunkID: "c1", PRNumber: 101, Repo: "aerial"}); !errors.Is(err, ErrChunkNotFound) {
		t.Errorf("expected ErrChunkNotFound, got %v", err)
	}

	// No rows: chunk exists but condition failed (e.g. mismatched)
	sMismatched := newMockStore(mockDB{
		queryRowFn: func(_ context.Context, sql string, _ ...any) pgx.Row {
			if strings.Contains(sql, "SELECT mode") {
				return &genericMockRow{scanFn: func(dest ...any) error {
					*dest[0].(*string) = "pr"
					return nil
				}}
			}
			if strings.Contains(sql, "c.status = 'cancelled'") {
				return &genericMockRow{scanFn: func(dest ...any) error {
					*dest[0].(*bool) = false
					return nil
				}}
			}
			if strings.Contains(sql, "SELECT EXISTS") {
				return &genericMockRow{scanFn: func(dest ...any) error {
					*dest[0].(*bool) = true
					return nil
				}}
			}
			return &mockRow{err: pgx.ErrNoRows}
		},
	})
	if _, _, _, err := sMismatched.AttachChunkPR(ctx, AttachChunkPRRequest{ChunkID: "c1", PRNumber: 101, Repo: "aerial"}); err == nil || errors.Is(err, ErrChunkNotFound) || errors.Is(err, ErrBuildConflict) {
		t.Errorf("expected mismatch error, got %v", err)
	}

	// General error
	sGeneralErr := newMockStore(mockDB{
		queryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return &mockRow{err: errors.New("attach db err")}
		},
	})
	if _, _, _, err := sGeneralErr.AttachChunkPR(ctx, AttachChunkPRRequest{ChunkID: "c1", PRNumber: 101, Repo: "aerial"}); err == nil {
		t.Error("expected error on general queryRow error")
	}

	// Action chunk requires repo
	sActionNoRepo := newMockStore(mockDB{
		queryRowFn: func(_ context.Context, sql string, _ ...any) pgx.Row {
			if strings.Contains(sql, "SELECT mode") {
				return &genericMockRow{scanFn: func(dest ...any) error {
					*dest[0].(*string) = "action"
					return nil
				}}
			}
			return &mockRow{err: pgx.ErrNoRows}
		},
	})
	if _, _, _, err := sActionNoRepo.AttachChunkPR(ctx, AttachChunkPRRequest{ChunkID: "c1", PRNumber: 101, Repo: ""}); err == nil || !strings.Contains(err.Error(), "repo is required") {
		t.Errorf("expected repo is required error on action chunk attach PR with empty repo, got %v", err)
	}
}

func TestVanadisStore_AttachChunkPR_ExistingConflict_MockDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Conflict scenarios: cancelled chunk/build or completed parent build returns ErrBuildConflict
	conflictTests := []struct {
		name        string
		repo        string
		isCancelled bool
		wantErr     error
	}{
		{name: "cancelled chunk or build with repo returns ErrBuildConflict", repo: "aerial", isCancelled: true, wantErr: ErrBuildConflict},
		{name: "cancelled chunk or build without repo returns ErrBuildConflict", repo: "", isCancelled: true, wantErr: ErrBuildConflict},
	}
	for _, tc := range conflictTests {
		sConflict := newMockStore(mockDB{
			queryRowFn: func(_ context.Context, sql string, _ ...any) pgx.Row {
				if strings.Contains(sql, "SELECT mode") {
					return &genericMockRow{scanFn: func(dest ...any) error {
						*dest[0].(*string) = "pr"
						return nil
					}}
				}
				if strings.Contains(sql, "c.status = 'cancelled'") {
					return &genericMockRow{scanFn: func(dest ...any) error {
						*dest[0].(*bool) = tc.isCancelled
						return nil
					}}
				}
				if strings.Contains(sql, "SELECT EXISTS") {
					return &genericMockRow{scanFn: func(dest ...any) error {
						*dest[0].(*bool) = true
						return nil
					}}
				}
				return &mockRow{err: pgx.ErrNoRows}
			},
		})
		if _, _, _, err := sConflict.AttachChunkPR(ctx, AttachChunkPRRequest{ChunkID: "c1", PRNumber: 101, Repo: tc.repo}); !errors.Is(err, tc.wantErr) {
			t.Errorf("%s: expected ErrBuildConflict, got %v", tc.name, err)
		}
	}
}

func newAttachSuccessStore(mode string, captureCandidates *[]string) *PostgresVanadisStore {
	return newMockStore(mockDB{
		queryRowFn: func(_ context.Context, sql string, args ...any) pgx.Row {
			if strings.Contains(sql, "SELECT mode") {
				return &genericMockRow{scanFn: func(dest ...any) error {
					*dest[0].(*string) = mode
					return nil
				}}
			}
			if captureCandidates != nil && len(args) >= 3 {
				if cands, ok := args[2].([]string); ok {
					*captureCandidates = cands
				}
			}
			return &genericMockRow{scanFn: func(dest ...any) error {
				if len(dest) == 3 {
					*dest[0].(*string) = "b-" + mode
					*dest[1].(*string) = "c-" + mode
					*dest[2].(*string) = "thread-" + mode
					return nil
				}
				*dest[0].(*string) = "thread-" + mode
				return nil
			}}
		},
	})
}

func TestVanadisStore_AttachChunkPR_ExistingSuccess_MockDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Success without repo
	sNoRepo := newAttachSuccessStore("pr", nil)
	bID, cID, threadID, err := sNoRepo.AttachChunkPR(ctx, AttachChunkPRRequest{ChunkID: "c-pr", PRNumber: 101, Repo: ""})
	if err != nil || bID != "b-pr" || cID != "c-pr" || threadID != "thread-pr" {
		t.Errorf("unexpected AttachChunkPR result: bID=%q cID=%q threadID=%q err=%v", bID, cID, threadID, err)
	}

	// Success with repo candidates
	var capturedAttachCandidates []string
	sWithRepo := newAttachSuccessStore("pr", &capturedAttachCandidates)
	bIDRepo, cIDRepo, threadIDWithRepo, err := sWithRepo.AttachChunkPR(ctx, AttachChunkPRRequest{ChunkID: "c-pr", PRNumber: 101, Repo: "aerial"})
	if err != nil || bIDRepo != "b-pr" || cIDRepo != "c-pr" || threadIDWithRepo != "thread-pr" {
		t.Errorf("unexpected AttachChunkPR with repo result: bID=%q cID=%q threadID=%q err=%v", bIDRepo, cIDRepo, threadIDWithRepo, err)
	}
	if !slices.Equal(capturedAttachCandidates, []string{"aerial", "azylman/aerial"}) {
		t.Errorf("AttachChunkPR candidates = %v, want [aerial azylman/aerial]", capturedAttachCandidates)
	}
}

func mockTxWithExecErr(matchSQL, errMsg string) *PostgresVanadisStore {
	return newMockStore(mockDB{
		beginFn: func(context.Context) (pgx.Tx, error) {
			return &mockTx{
				queryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
					return &mockRow{err: pgx.ErrNoRows}
				},
				execFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
					if strings.Contains(sql, matchSQL) {
						return pgconn.CommandTag{}, errors.New(errMsg)
					}
					return pgconn.NewCommandTag("INSERT 0 1"), nil
				},
			}, nil
		},
	})
}

func TestVanadisStore_AttachChunkPR_Standalone_MockDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Standalone attach: empty repo error
	sEmptyRepo := newMockStore(mockDB{})
	if _, _, _, err := sEmptyRepo.AttachChunkPR(ctx, AttachChunkPRRequest{ChunkID: "", PRNumber: 101, Repo: ""}); err == nil || !strings.Contains(err.Error(), "repo is required") {
		t.Errorf("expected error when repo is empty in standalone attach, got %v", err)
	}

	// Standalone attach: existing PR lookup matches
	sExistingMatch := newMockStore(mockDB{
		beginFn: func(context.Context) (pgx.Tx, error) {
			return &mockTx{
				queryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
					return &genericMockRow{scanFn: func(dest ...any) error {
						*dest[0].(*string) = "b-existing"
						*dest[1].(*string) = "c-existing"
						*dest[2].(*string) = "thread-existing"
						return nil
					}}
				},
			}, nil
		},
	})
	bIDMatch, cIDMatch, thMatch, err := sExistingMatch.AttachChunkPR(ctx, AttachChunkPRRequest{ChunkID: "", PRNumber: 101, Repo: "aerial"})
	if err != nil || bIDMatch != "b-existing" || cIDMatch != "c-existing" || thMatch != "thread-existing" {
		t.Errorf("unexpected existing match standalone attach result: %s, %s, %s, %v", bIDMatch, cIDMatch, thMatch, err)
	}

	// Standalone attach: existing PR lookup error (not ErrNoRows)
	sExistingErr := newMockStore(mockDB{
		beginFn: func(context.Context) (pgx.Tx, error) {
			return &mockTx{
				queryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
					return &mockRow{err: errors.New("lookup err")}
				},
			}, nil
		},
	})
	if _, _, _, err := sExistingErr.AttachChunkPR(ctx, AttachChunkPRRequest{ChunkID: "", PRNumber: 101, Repo: "aerial"}); err == nil {
		t.Error("expected error on existing lookup db failure")
	}

	// Standalone attach: begin tx error
	sStandaloneBeginErr := newMockStore(mockDB{
		beginFn: func(context.Context) (pgx.Tx, error) {
			return nil, errors.New("begin standalone tx err")
		},
	})
	if _, _, _, err := sStandaloneBeginErr.AttachChunkPR(ctx, AttachChunkPRRequest{ChunkID: "", PRNumber: 101, Repo: "aerial"}); err == nil {
		t.Error("expected error on begin tx failure in standalone attach")
	}

	// Standalone attach: advisory lock error
	sLockErr := newMockStore(mockDB{
		beginFn: func(context.Context) (pgx.Tx, error) {
			return &mockTx{
				execFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
					if strings.Contains(sql, "pg_advisory_xact_lock") {
						return pgconn.CommandTag{}, errors.New("advisory lock boom")
					}
					return pgconn.NewCommandTag("INSERT 0 1"), nil
				},
			}, nil
		},
	})
	if _, _, _, err := sLockErr.AttachChunkPR(ctx, AttachChunkPRRequest{ChunkID: "", PRNumber: 101, Repo: "aerial"}); err == nil {
		t.Error("expected error on advisory lock failure")
	}

	// Standalone attach: insert build error
	sInsertBuildErr := mockTxWithExecErr("INSERT INTO vanadis_builds", "insert build err")
	if _, _, _, err := sInsertBuildErr.AttachChunkPR(ctx, AttachChunkPRRequest{ChunkID: "", PRNumber: 101, Repo: "aerial"}); err == nil {
		t.Error("expected error on insert build failure in standalone attach")
	}

	// Standalone attach: insert chunk error
	sInsertChunkErr := mockTxWithExecErr("INSERT INTO vanadis_chunks", "insert chunk err")
	if _, _, _, err := sInsertChunkErr.AttachChunkPR(ctx, AttachChunkPRRequest{ChunkID: "", PRNumber: 101, Repo: "aerial"}); err == nil {
		t.Error("expected error on insert chunk failure in standalone attach")
	}

	// Standalone attach: commit error
	sStandaloneCommitErr := newMockStore(mockDB{
		beginFn: func(context.Context) (pgx.Tx, error) {
			return &mockTx{
				queryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
					return &mockRow{err: pgx.ErrNoRows}
				},
				commitErr: errors.New("commit standalone err"),
			}, nil
		},
	})
	if _, _, _, err := sStandaloneCommitErr.AttachChunkPR(ctx, AttachChunkPRRequest{ChunkID: "", PRNumber: 101, Repo: "aerial"}); err == nil {
		t.Error("expected error on commit failure in standalone attach")
	}

	// Standalone attach: success
	sStandaloneSuccess := newMockStore(mockDB{
		beginFn: func(context.Context) (pgx.Tx, error) {
			return &mockTx{
				queryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
					return &mockRow{err: pgx.ErrNoRows}
				},
			}, nil
		},
	})
	bIDNew, cIDNew, thNew, err := sStandaloneSuccess.AttachChunkPR(ctx, AttachChunkPRRequest{
		ChunkID:  "",
		PRNumber: 101,
		Repo:     "aerial",
		TargetID: "123456789012345678",
		Branch:   "dependabot/npm_and_yarn/foo",
		Title:    "Bump foo",
	})
	if err != nil || bIDNew == "" || cIDNew == "" || thNew != "123456789012345678" {
		t.Errorf("unexpected standalone attach success: got (%s, %s, %s, %v)", bIDNew, cIDNew, thNew, err)
	}
}
