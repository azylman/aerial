package main

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPostgresVanadisStore_ChunkMethods(t *testing.T) {
	ctx := context.Background()

	t.Run("NilReceiverAndDB", func(t *testing.T) {
		var nilStore *PostgresVanadisStore
		emptyStore := &PostgresVanadisStore{}

		for _, s := range []*PostgresVanadisStore{nilStore, emptyStore} {
			if _, _, err := s.GetChunk(ctx, "c1"); err == nil {
				t.Error("expected error on GetChunk")
			}
			if _, err := s.GetChunkByPR(ctx, "aerial", 1); err == nil {
				t.Error("expected error on GetChunkByPR")
			}
			if _, err := s.AttachChunkPR(ctx, "c1", 1, "aerial"); err == nil {
				t.Error("expected error on AttachChunkPR")
			}
			if _, err := s.AdvanceChunkByPR(ctx, "aerial", 1); err == nil {
				t.Error("expected error on AdvanceChunkByPR")
			}
			if _, _, _, err := s.FailChunkByPR(ctx, "aerial", 1); err == nil {
				t.Error("expected error on FailChunkByPR")
			}
			if _, err := s.UpdateBuild(ctx, "b1", UpdateBuildRequest{Title: "T"}); err == nil {
				t.Error("expected error on UpdateBuild")
			}
			if _, err := s.UpdateChunk(ctx, "b1", "c1", UpdateChunkRequest{Title: "T"}); err == nil {
				t.Error("expected error on UpdateChunk")
			}
			if _, err := s.AppendChunks(ctx, "b1", []CreateChunkInput{{Title: "T", Instructions: "I"}}); err == nil {
				t.Error("expected error on AppendChunks")
			}
			if _, _, err := s.RemoveChunk(ctx, "b1", "c1"); err == nil {
				t.Error("expected error on RemoveChunk")
			}
		}
	})

	t.Run("GetChunk_MockDB", func(t *testing.T) {
		// Empty chunk ID
		s := newMockStore(mockDB{})
		if _, _, err := s.GetChunk(ctx, "  "); err == nil {
			t.Error("expected error on empty chunk ID")
		}

		// Query error
		sQueryErr := newMockStore(mockDB{
			queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
				return nil, errors.New("query err")
			},
		})
		if _, _, err := sQueryErr.GetChunk(ctx, "c1"); err == nil {
			t.Error("expected error on query failure")
		}

		// Rows error
		sRowsErr := newMockStore(mockDB{
			queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
				return &mockRows{rowsErr: errors.New("rows err"), items: []VanadisChunk{{ID: "c1"}}}, nil
			},
		})
		if _, _, err := sRowsErr.GetChunk(ctx, "c1"); err == nil {
			t.Error("expected error on rows error")
		}

		// Chunk not found (0 items)
		sNotFound := newMockStore(mockDB{
			queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
				return &mockRows{items: []VanadisChunk{}}, nil
			},
		})
		if _, _, err := sNotFound.GetChunk(ctx, "c1"); !errors.Is(err, ErrChunkNotFound) {
			t.Errorf("expected ErrChunkNotFound, got %v", err)
		}

		// Build not found
		sBuildNotFound := newMockStore(mockDB{
			queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
				return &mockRows{items: []VanadisChunk{{ID: "c1", BuildID: "b1"}}}, nil
			},
			queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return &mockRow{err: pgx.ErrNoRows}
			},
		})
		if _, _, err := sBuildNotFound.GetChunk(ctx, "c1"); !errors.Is(err, ErrBuildNotFound) {
			t.Errorf("expected ErrBuildNotFound, got %v", err)
		}

		// Build query error
		sBuildErr := newMockStore(mockDB{
			queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
				return &mockRows{items: []VanadisChunk{{ID: "c1", BuildID: "b1"}}}, nil
			},
			queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return &mockRow{err: errors.New("build query err")}
			},
		})
		if _, _, err := sBuildErr.GetChunk(ctx, "c1"); err == nil {
			t.Error("expected error on build query error")
		}

		// Success
		sSuccess := newMockStore(mockDB{
			queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
				return &mockRows{items: []VanadisChunk{{ID: "c1", BuildID: "b1", Title: "Chunk Title"}}}, nil
			},
			queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return &mockRow{b: &VanadisBuild{ID: "b1", Title: "Build Title"}}
			},
		})
		c, b, err := sSuccess.GetChunk(ctx, "c1")
		if err != nil || c.ID != "c1" || b.ID != "b1" {
			t.Errorf("unexpected GetChunk success: c=%+v b=%+v err=%v", c, b, err)
		}
	})

	t.Run("AttachChunkPR_MockDB", func(t *testing.T) {
		s := newMockStore(mockDB{})
		// Invalid args
		if _, err := s.AttachChunkPR(ctx, "", 0, "aerial"); err == nil {
			t.Error("expected error on empty chunk ID")
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
		if _, err := sNotExist.AttachChunkPR(ctx, "c1", 101, "aerial"); !errors.Is(err, ErrChunkNotFound) {
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
		if _, err := sMismatched.AttachChunkPR(ctx, "c1", 101, "aerial"); err == nil || errors.Is(err, ErrChunkNotFound) || errors.Is(err, ErrBuildConflict) {
			t.Errorf("expected mismatch error, got %v", err)
		}

		// Conflict scenarios: cancelled chunk/build or completed parent build returns ErrBuildConflict
		conflictTests := []struct {
			name        string
			repo        string
			isCancelled bool
			wantErr     error
		}{
			{name: "cancelled chunk or build with repo returns ErrBuildConflict", repo: "aerial", isCancelled: true, wantErr: ErrBuildConflict},
			{name: "completed parent build with repo returns ErrBuildConflict", repo: "aerial", isCancelled: true, wantErr: ErrBuildConflict},
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
			if _, err := sConflict.AttachChunkPR(ctx, "c1", 101, tc.repo); !errors.Is(err, tc.wantErr) {
				t.Errorf("%s: expected ErrBuildConflict, got %v", tc.name, err)
			}
		}

		// General error
		sGeneralErr := newMockStore(mockDB{
			queryRowFn: func(_ context.Context, _ string, _ ...any) pgx.Row {
				return &mockRow{err: errors.New("attach db err")}
			},
		})
		if _, err := sGeneralErr.AttachChunkPR(ctx, "c1", 101, "aerial"); err == nil {
			t.Error("expected error on general queryRow error")
		}

		// Action chunk rejected
		sActionReject := newMockStore(mockDB{
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
		if _, err := sActionReject.AttachChunkPR(ctx, "c1", 101, "aerial"); err == nil || !errors.Is(err, ErrBuildConflict) {
			t.Errorf("expected ErrBuildConflict on action chunk attach PR, got %v", err)
		}

		// Success without repo
		sNoRepo := newMockStore(mockDB{
			queryRowFn: func(_ context.Context, sql string, _ ...any) pgx.Row {
				if strings.Contains(sql, "SELECT mode") {
					return &genericMockRow{scanFn: func(dest ...any) error {
						*dest[0].(*string) = "pr"
						return nil
					}}
				}
				return &genericMockRow{scanFn: func(dest ...any) error {
					*dest[0].(*string) = "thread-123"
					return nil
				}}
			},
		})
		threadID, err := sNoRepo.AttachChunkPR(ctx, "c1", 101, "")
		if err != nil || threadID != "thread-123" {
			t.Errorf("unexpected AttachChunkPR result: threadID=%q err=%v", threadID, err)
		}

		// Success with repo candidates
		var capturedAttachCandidates []string
		sWithRepo := newMockStore(mockDB{
			queryRowFn: func(_ context.Context, sql string, args ...any) pgx.Row {
				if strings.Contains(sql, "SELECT mode") {
					return &genericMockRow{scanFn: func(dest ...any) error {
						*dest[0].(*string) = "pr"
						return nil
					}}
				}
				if len(args) >= 3 {
					if cands, ok := args[2].([]string); ok {
						capturedAttachCandidates = cands
					}
				}
				return &genericMockRow{scanFn: func(dest ...any) error {
					*dest[0].(*string) = "thread-123"
					return nil
				}}
			},
		})
		threadIDWithRepo, err := sWithRepo.AttachChunkPR(ctx, "c1", 101, "aerial")
		if err != nil || threadIDWithRepo != "thread-123" {
			t.Errorf("unexpected AttachChunkPR with repo result: threadID=%q err=%v", threadIDWithRepo, err)
		}
		if !slices.Equal(capturedAttachCandidates, []string{"aerial", "azylman/aerial"}) {
			t.Errorf("AttachChunkPR candidates = %v, want [aerial azylman/aerial]", capturedAttachCandidates)
		}
	})

	t.Run("AdvanceChunkByPR_MockDB", func(t *testing.T) {
		s := newMockStore(mockDB{})
		// Invalid args
		res, err := s.AdvanceChunkByPR(ctx, "", 0)
		if err != nil || res.Matched {
			t.Errorf("expected unmatched for empty args, got %+v", res)
		}

		// Begin error
		sBeginErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return nil, errors.New("begin tx err")
			},
		})
		if _, err := sBeginErr.AdvanceChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on begin tx failure")
		}

		// Select chunk no rows
		sNoRows := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return &mockRow{err: pgx.ErrNoRows}
					},
				}, nil
			},
		})
		resNoRows, err := sNoRows.AdvanceChunkByPR(ctx, "aerial", 101)
		if err != nil || resNoRows.Matched {
			t.Errorf("expected unmatched on no rows, got %+v", resNoRows)
		}

		// Select chunk db error
		sSelectErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return &mockRow{err: errors.New("select chunk err")}
					},
				}, nil
			},
		})
		if _, err := sSelectErr.AdvanceChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on select chunk error")
		}

		mockAdvRow := func(mode, cTitle, cStatus, bStatus string) *genericMockRow {
			return &genericMockRow{scanFn: func(dest ...any) error {
				*dest[0].(*string) = "c1"
				*dest[1].(*string) = "b1"
				*dest[2].(*int) = 0
				*dest[3].(*string) = mode
				*dest[4].(*string) = cTitle
				*dest[5].(*string) = cStatus
				*dest[6].(*string) = "thread-1"
				*dest[7].(*int) = 2
				*dest[8].(*string) = "Build 1"
				*dest[9].(*string) = "Objective 1"
				*dest[10].(*string) = bStatus
				*dest[11].(*string) = "msg-1"
				return nil
			}}
		}

		// Already terminal
		sTerminal := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return mockAdvRow("pr", "Chunk 1", "merged", "cancelled")
					},
				}, nil
			},
		})
		resTerm, err := sTerminal.AdvanceChunkByPR(ctx, "aerial", 101)
		if err != nil || !resTerm.Matched || resTerm.Advanced || resTerm.NextChunkIndex != -1 {
			t.Errorf("expected matched but not advanced for already terminal build, got %+v", resTerm)
		}

		// Already merged chunk on running build computes next chunk
		sMergedRunning := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return mockAdvRow("pr", "Chunk 1", "merged", "running")
					},
					queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
						return &mockRows{items: []VanadisChunk{
							{ID: "c1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Status: "merged"},
							{ID: "c2", ChunkIndex: 1, Mode: "pr", Repo: "aerial", Status: "pending"},
						}}, nil
					},
				}, nil
			},
		})
		resMergedRunning, err := sMergedRunning.AdvanceChunkByPR(ctx, "aerial", 101)
		if err != nil || !resMergedRunning.Matched || resMergedRunning.Advanced || resMergedRunning.NextChunkIndex != 1 {
			t.Errorf("expected matched, unadvanced, nextChunkIdx 1 for already merged chunk on running build, got %+v", resMergedRunning)
		}

		// Update chunk error
		sUpdateErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return mockAdvRow("pr", "Chunk 1", "pr_opened", "running")
					},
					execFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
						if strings.Contains(sql, "UPDATE vanadis_chunks") {
							return pgconn.CommandTag{}, errors.New("update chunk err")
						}
						return pgconn.NewCommandTag("UPDATE 1"), nil
					},
				}, nil
			},
		})
		if _, err := sUpdateErr.AdvanceChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on update chunk error")
		}

		// Query chunks error
		sQueryChunksErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return mockAdvRow("pr", "Chunk 1", "pr_opened", "running")
					},
					queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
						return nil, errors.New("query chunks err")
					},
				}, nil
			},
		})
		if _, err := sQueryChunksErr.AdvanceChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on query chunks error")
		}

		// Iterate chunks error
		sIterErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return mockAdvRow("pr", "Chunk 1", "pr_opened", "running")
					},
					queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
						return &mockRows{rowsErr: errors.New("chunks iter err"), items: []VanadisChunk{{ID: "c1", Mode: "pr", Repo: "aerial"}}}, nil
					},
				}, nil
			},
		})
		if _, err := sIterErr.AdvanceChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on chunks iter error")
		}

		// Update next chunk error
		sUpdateNextErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return mockAdvRow("pr", "Chunk 1", "pr_opened", "running")
					},
					queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
						return &mockRows{items: []VanadisChunk{
							{ID: "c1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Status: "merged", StartedAt: time.Now()},
							{ID: "c2", ChunkIndex: 1, Mode: "pr", Repo: "aerial", Status: "pending"},
						}}, nil
					},
					execFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
						if strings.Contains(sql, "UPDATE vanadis_chunks") && strings.Contains(sql, "status = 'running'") {
							return pgconn.CommandTag{}, errors.New("update next chunk err")
						}
						return pgconn.NewCommandTag("UPDATE 1"), nil
					},
				}, nil
			},
		})
		if _, err := sUpdateNextErr.AdvanceChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on update next chunk error")
		}

		// Commit error with next chunk
		sCommitErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					commitErr: errors.New("commit err"),
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return mockAdvRow("pr", "Chunk 1", "pr_opened", "running")
					},
					queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
						return &mockRows{items: []VanadisChunk{
							{ID: "c1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Status: "merged", StartedAt: time.Now()},
							{ID: "c2", ChunkIndex: 1, Mode: "pr", Repo: "aerial", Status: "pending"},
						}}, nil
					},
				}, nil
			},
		})
		if _, err := sCommitErr.AdvanceChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on commit error")
		}

		// Build completion: update build error
		sBuildUpdateErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return mockAdvRow("pr", "Chunk 1", "pr_opened", "running")
					},
					queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
						// Only 1 chunk, which is merged -> complete build
						return &mockRows{items: []VanadisChunk{
							{ID: "c1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Status: "merged"},
						}}, nil
					},
					execFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
						if strings.Contains(sql, "UPDATE vanadis_builds") {
							return pgconn.CommandTag{}, errors.New("update build err")
						}
						return pgconn.NewCommandTag("UPDATE 1"), nil
					},
				}, nil
			},
		})
		if _, err := sBuildUpdateErr.AdvanceChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on build update error")
		}

		// Build completion: commit error
		sBuildCommitErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					commitErr: errors.New("build commit err"),
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return mockAdvRow("pr", "Chunk 1", "pr_opened", "running")
					},
					queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
						return &mockRows{items: []VanadisChunk{
							{ID: "c1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Status: "merged"},
						}}, nil
					},
				}, nil
			},
		})
		if _, err := sBuildCommitErr.AdvanceChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on build commit error")
		}

		// Success advancing to next chunk
		var capturedAdvanceCandidates []string
		sAdvanceNextSuccess := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
						if len(args) > 0 {
							if cands, ok := args[0].([]string); ok {
								capturedAdvanceCandidates = cands
							}
						}
						return mockAdvRow("pr", "Chunk 1", "failed", "failed")
					},
					queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
						return &mockRows{items: []VanadisChunk{
							{ID: "c1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Status: "merged"},
							{ID: "c2", ChunkIndex: 1, Mode: "pr", Repo: "aerial", Status: "pending"},
						}}, nil
					},
				}, nil
			},
		})
		resNext, err := sAdvanceNextSuccess.AdvanceChunkByPR(ctx, "aerial", 101)
		if err != nil || !resNext.Matched || !resNext.Advanced || resNext.NextChunkIndex != 1 || resNext.BuildStatus != "running" {
			t.Errorf("expected success advancing and recovering build to next chunk, got %+v err=%v", resNext, err)
		}
		if !slices.Equal(capturedAdvanceCandidates, []string{"aerial", "azylman/aerial"}) {
			t.Errorf("AdvanceChunkByPR candidates = %v, want [aerial azylman/aerial]", capturedAdvanceCandidates)
		}

		// Recover build running error when advancing to next chunk
		sRecoverBuildErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return mockAdvRow("pr", "Chunk 1", "failed", "failed")
					},
					queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
						return &mockRows{items: []VanadisChunk{
							{ID: "c1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Status: "merged"},
							{ID: "c2", ChunkIndex: 1, Mode: "pr", Repo: "aerial", Status: "pending"},
						}}, nil
					},
					execFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
						if strings.Contains(sql, "UPDATE vanadis_builds SET status = 'running'") {
							return pgconn.CommandTag{}, errors.New("recover build err")
						}
						return pgconn.NewCommandTag("UPDATE 1"), nil
					},
				}, nil
			},
		})
		if _, err := sRecoverBuildErr.AdvanceChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on recover build running failure")
		}

		// Success completing build
		sCompleteSuccess := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return mockAdvRow("pr", "Chunk 1", "pr_opened", "running")
					},
					queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
						return &mockRows{items: []VanadisChunk{
							{ID: "c1", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Status: "merged"},
						}}, nil
					},
				}, nil
			},
		})
		resComp, err := sCompleteSuccess.AdvanceChunkByPR(ctx, "aerial", 101)
		if err != nil || !resComp.Matched || !resComp.Advanced || !resComp.BuildCompleted || resComp.NextChunkIndex != -1 {
			t.Errorf("expected success completing build, got %+v err=%v", resComp, err)
		}
	})

	t.Run("FailChunkByPR_MockDB", func(t *testing.T) {
		s := newMockStore(mockDB{})
		// Invalid args
		_, _, failed, err := s.FailChunkByPR(ctx, "", 0)
		if err != nil || failed {
			t.Errorf("expected not failed for invalid args, got failed=%v, err=%v", failed, err)
		}

		// No rows
		sNoRows := newMockStore(mockDB{
			queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return &mockRow{err: pgx.ErrNoRows}
			},
		})
		_, _, failedNoRows, err := sNoRows.FailChunkByPR(ctx, "aerial", 101)
		if err != nil || failedNoRows {
			t.Errorf("expected not failed on NoRows, got failed=%v, err=%v", failedNoRows, err)
		}

		// Query row error
		sErr := newMockStore(mockDB{
			queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return &mockRow{err: errors.New("fail query err")}
			},
		})
		if _, _, _, err := sErr.FailChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on query row failure")
		}

		// Update build error
		sUpdateBuildErr := newMockStore(mockDB{
			queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return &genericMockRow{scanFn: func(dest ...any) error {
					*dest[0].(*string) = "b1"
					*dest[1].(*string) = "c1"
					return nil
				}}
			},
			execFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
				if strings.Contains(sql, "UPDATE vanadis_builds") {
					return pgconn.CommandTag{}, errors.New("update build err")
				}
				return pgconn.NewCommandTag("UPDATE 1"), nil
			},
		})
		if _, _, _, err := sUpdateBuildErr.FailChunkByPR(ctx, "aerial", 101); err == nil {
			t.Error("expected error on update build failure")
		}

		// Success
		var capturedFailCandidates []string
		sFailSuccess := newMockStore(mockDB{
			queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
				if len(args) > 0 {
					if cands, ok := args[0].([]string); ok {
						capturedFailCandidates = cands
					}
				}
				return &genericMockRow{scanFn: func(dest ...any) error {
					*dest[0].(*string) = "b1"
					*dest[1].(*string) = "c1"
					return nil
				}}
			},
		})
		bID, cID, failed, err := sFailSuccess.FailChunkByPR(ctx, "aerial", 101)
		if err != nil || !failed || bID != "b1" || cID != "c1" {
			t.Errorf("unexpected FailChunkByPR success: got (%s, %s, %v, %v)", bID, cID, failed, err)
		}
		if !slices.Equal(capturedFailCandidates, []string{"aerial", "azylman/aerial"}) {
			t.Errorf("FailChunkByPR candidates = %v, want [aerial azylman/aerial]", capturedFailCandidates)
		}
	})

	t.Run("CompleteActionChunk_MockDB", func(t *testing.T) {
		s := newMockStore(mockDB{})
		// Empty chunk ID
		if _, err := s.CompleteActionChunk(ctx, "  "); err == nil {
			t.Error("expected error on empty chunk ID")
		}

		// Begin error
		sBeginErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return nil, errors.New("begin tx err")
			},
		})
		if _, err := sBeginErr.CompleteActionChunk(ctx, "c1"); err == nil {
			t.Error("expected error on begin tx failure")
		}

		// Select chunk not found
		sNoRows := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return &mockRow{err: pgx.ErrNoRows}
					},
				}, nil
			},
		})
		if _, err := sNoRows.CompleteActionChunk(ctx, "c1"); !errors.Is(err, ErrChunkNotFound) {
			t.Errorf("expected ErrChunkNotFound, got %v", err)
		}

		// Select chunk db error
		sSelectErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return &mockRow{err: errors.New("select chunk err")}
					},
				}, nil
			},
		})
		if _, err := sSelectErr.CompleteActionChunk(ctx, "c1"); err == nil {
			t.Error("expected error on select chunk error")
		}

		mockActChunkRow := func(mode, cStatus string) *genericMockRow {
			return &genericMockRow{scanFn: func(dest ...any) error {
				*dest[0].(*string) = "c1"
				*dest[1].(*string) = "b1"
				*dest[2].(*int) = 0
				*dest[3].(*string) = mode
				*dest[4].(*string) = "Action Chunk"
				*dest[5].(*string) = cStatus
				return nil
			}}
		}
		mockActBuildRow := func(bStatus string) *genericMockRow {
			return &genericMockRow{scanFn: func(dest ...any) error {
				*dest[0].(*string) = "b1"
				*dest[1].(*string) = "thread-1"
				*dest[2].(*int) = 2
				*dest[3].(*string) = "Build 1"
				*dest[4].(*string) = "Objective 1"
				*dest[5].(*string) = bStatus
				*dest[6].(*string) = "msg-1"
				return nil
			}}
		}
		makeMockQueryRow := func(mode, cStatus, bStatus string) func(context.Context, string, ...any) pgx.Row {
			return func(_ context.Context, sql string, _ ...any) pgx.Row {
				if strings.Contains(sql, "FROM vanadis_chunks") {
					return mockActChunkRow(mode, cStatus)
				}
				return mockActBuildRow(bStatus)
			}
		}

		// Select build not found
		sBuildNoRows := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(_ context.Context, sql string, _ ...any) pgx.Row {
						if strings.Contains(sql, "FROM vanadis_chunks") {
							return mockActChunkRow("action", "running")
						}
						return &mockRow{err: pgx.ErrNoRows}
					},
				}, nil
			},
		})
		if _, err := sBuildNoRows.CompleteActionChunk(ctx, "c1"); !errors.Is(err, ErrBuildNotFound) {
			t.Errorf("expected ErrBuildNotFound, got %v", err)
		}

		// Non-action mode (mode = "pr") -> no-op returns matched: true, advanced: false
		sPRMode := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: makeMockQueryRow("pr", "running", "running"),
				}, nil
			},
		})
		resPR, err := sPRMode.CompleteActionChunk(ctx, "c1")
		if err != nil || !resPR.Matched || resPR.Advanced {
			t.Errorf("expected no-op for pr mode, got %+v, err: %v", resPR, err)
		}

		// Action mode not running (status = "completed") -> no-op
		sNotRunning := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: makeMockQueryRow("action", "completed", "running"),
				}, nil
			},
		})
		resNotRun, err := sNotRunning.CompleteActionChunk(ctx, "c1")
		if err != nil || !resNotRun.Matched || resNotRun.Advanced {
			t.Errorf("expected no-op for completed chunk, got %+v, err: %v", resNotRun, err)
		}

		// Update chunk error
		sUpdateErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: makeMockQueryRow("action", "running", "running"),
					execFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
						if strings.Contains(sql, "UPDATE vanadis_chunks") {
							return pgconn.CommandTag{}, errors.New("update action chunk err")
						}
						return pgconn.NewCommandTag("UPDATE 1"), nil
					},
				}, nil
			},
		})
		if _, err := sUpdateErr.CompleteActionChunk(ctx, "c1"); err == nil {
			t.Error("expected error on update action chunk failure")
		}

		// Success completing and advancing
		sSuccess := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: makeMockQueryRow("action", "running", "running"),
					queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
						return &mockRows{items: []VanadisChunk{
							{ID: "c1", ChunkIndex: 0, Mode: "action", Status: "completed"},
							{ID: "c2", ChunkIndex: 1, Mode: "pr", Repo: "aerial", Status: "pending"},
						}}, nil
					},
					execFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
						return pgconn.NewCommandTag("UPDATE 1"), nil
					},
				}, nil
			},
		})
		resSuccess, err := sSuccess.CompleteActionChunk(ctx, "c1")
		if err != nil || !resSuccess.Matched || !resSuccess.Advanced || resSuccess.NextChunkIndex != 1 {
			t.Errorf("expected advancement to next chunk, got %+v, err: %v", resSuccess, err)
		}
	})

	t.Run("FailChunk_MockDB", func(t *testing.T) {
		s := newMockStore(mockDB{})
		// Empty chunk ID
		if _, _, _, err := s.FailChunk(ctx, "", "reason"); err == nil {
			t.Error("expected error on empty chunk ID")
		}

		// Begin error
		sBeginErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return nil, errors.New("begin tx err")
			},
		})
		if _, _, _, err := sBeginErr.FailChunk(ctx, "c1", "reason"); err == nil {
			t.Error("expected error on begin tx failure")
		}

		// Select chunk not found
		sNoRows := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return &mockRow{err: pgx.ErrNoRows}
					},
				}, nil
			},
		})
		if _, _, _, err := sNoRows.FailChunk(ctx, "c1", "reason"); !errors.Is(err, ErrChunkNotFound) {
			t.Errorf("expected ErrChunkNotFound, got %v", err)
		}

		// Select chunk db error
		sSelectErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return &mockRow{err: errors.New("select chunk fail err")}
					},
				}, nil
			},
		})
		if _, _, _, err := sSelectErr.FailChunk(ctx, "c1", "reason"); err == nil {
			t.Error("expected error on select chunk error")
		}

		mockFailRow := func(cStatus, bStatus string) *genericMockRow {
			return &genericMockRow{scanFn: func(dest ...any) error {
				*dest[0].(*string) = "c1"
				*dest[1].(*string) = "b1"
				*dest[2].(*int) = 0
				*dest[3].(*string) = "action"
				*dest[4].(*string) = ""
				*dest[5].(*string) = "Fail Chunk Title"
				*dest[6].(*string) = "Instructions"
				*dest[7].(*[]string) = []string{}
				*dest[8].(*[]string) = []string{}
				*dest[9].(*string) = "medium"
				*dest[10].(*[]string) = []string{}
				*dest[11].(*string) = cStatus
				*dest[12].(*int) = 0
				*dest[13].(*sql.NullTime) = sql.NullTime{}
				*dest[14].(*sql.NullTime) = sql.NullTime{}
				*dest[15].(*time.Time) = time.Now()
				*dest[16].(*time.Time) = time.Now()
				*dest[17].(*string) = "b1"
				*dest[18].(*string) = "thread-1"
				*dest[19].(*int) = 2
				*dest[20].(*string) = "Build 1"
				*dest[21].(*string) = "Objective 1"
				*dest[22].(*string) = "Context"
				*dest[23].(*string) = bStatus
				*dest[24].(*string) = "msg-1"
				*dest[25].(*time.Time) = time.Now()
				*dest[26].(*time.Time) = time.Now()
				return nil
			}}
		}

		// Already terminal status -> no-op returns failed=false
		sTerm := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return mockFailRow("failed", "failed")
					},
				}, nil
			},
		})
		_, _, termFailed, err := sTerm.FailChunk(ctx, "c1", "reason")
		if err != nil || termFailed {
			t.Errorf("expected no-op for already terminal chunk, got %v, err: %v", termFailed, err)
		}

		// Update chunk error
		sUpdateChunkErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return mockFailRow("running", "running")
					},
					execFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
						if strings.Contains(sql, "UPDATE vanadis_chunks SET status = 'failed'") {
							return pgconn.CommandTag{}, errors.New("update chunk fail err")
						}
						return pgconn.NewCommandTag("UPDATE 1"), nil
					},
				}, nil
			},
		})
		if _, _, _, err := sUpdateChunkErr.FailChunk(ctx, "c1", "reason"); err == nil {
			t.Error("expected error on update chunk failure")
		}

		// Update build error
		sUpdateBuildErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return mockFailRow("running", "running")
					},
					execFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
						if strings.Contains(sql, "UPDATE vanadis_builds SET status = 'failed'") {
							return pgconn.CommandTag{}, errors.New("update build fail err")
						}
						return pgconn.NewCommandTag("UPDATE 1"), nil
					},
				}, nil
			},
		})
		if _, _, _, err := sUpdateBuildErr.FailChunk(ctx, "c1", "reason"); err == nil {
			t.Error("expected error on update build failure")
		}

		// Commit error
		sCommitErr := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					commitErr: errors.New("commit fail err"),
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return mockFailRow("running", "running")
					},
					execFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
						return pgconn.NewCommandTag("UPDATE 1"), nil
					},
				}, nil
			},
		})
		if _, _, _, err := sCommitErr.FailChunk(ctx, "c1", "reason"); err == nil {
			t.Error("expected error on commit failure")
		}

		// Success
		sSuccess := newMockStore(mockDB{
			beginFn: func(context.Context) (pgx.Tx, error) {
				return &mockTx{
					queryRowFn: func(context.Context, string, ...any) pgx.Row {
						return mockFailRow("running", "running")
					},
					execFn: func(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
						return pgconn.NewCommandTag("UPDATE 1"), nil
					},
				}, nil
			},
		})
		bGot, cGot, ok, err := sSuccess.FailChunk(ctx, "c1", "unrecoverable failure")
		if err != nil || !ok || bGot.Status != "failed" || cGot.Status != "failed" {
			t.Fatalf("expected success on FailChunk, got b=%+v c=%+v ok=%v err=%v", bGot, cGot, ok, err)
		}
	})
}

type genericMockRow struct {
	scanFn func(dest ...any) error
}

func (r *genericMockRow) Scan(dest ...any) error {
	if r.scanFn != nil {
		return r.scanFn(dest...)
	}
	return nil
}

func TestPostgresVanadisStore_UpdateBuild(t *testing.T) {
	tests := []struct {
		name        string
		buildID     string
		req         UpdateBuildRequest
		beginErr    error
		scanErr     error
		buildStatus string
		execErr     error
		commitErr   error
		wantErrIs   error
		wantErr     bool
	}{
		{"empty_build_id", "", UpdateBuildRequest{Title: "T"}, nil, nil, "", nil, nil, ErrInvalidBuildInput, true},
		{"all_empty_fields", "b1", UpdateBuildRequest{Title: "   "}, nil, nil, "", nil, nil, ErrInvalidBuildInput, true},
		{"begin_error", "b1", UpdateBuildRequest{Title: "T"}, errors.New("begin err"), nil, "", nil, nil, nil, true},
		{"build_not_found", "b1", UpdateBuildRequest{Title: "T"}, nil, pgx.ErrNoRows, "", nil, nil, ErrBuildNotFound, true},
		{"build_query_error", "b1", UpdateBuildRequest{Title: "T"}, nil, errors.New("query err"), "", nil, nil, nil, true},
		{"build_conflict_status", "b1", UpdateBuildRequest{Title: "T"}, nil, nil, "completed", nil, nil, ErrBuildConflict, true},
		{"exec_error", "b1", UpdateBuildRequest{Title: "T"}, nil, nil, "running", errors.New("exec err"), nil, nil, true},
		{"commit_error", "b1", UpdateBuildRequest{Title: "T"}, nil, nil, "running", nil, errors.New("commit err"), nil, true},
		{"success", "b1", UpdateBuildRequest{Title: "New Title", Objective: "New Obj", SharedContext: "New Ctx"}, nil, nil, "running", nil, nil, nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := &mockTx{
				commitErr: tc.commitErr,
				queryRowFn: func(context.Context, string, ...any) pgx.Row {
					if tc.scanErr != nil {
						return &mockRow{err: tc.scanErr}
					}
					return &mockRow{b: &VanadisBuild{ID: tc.buildID, Status: tc.buildStatus}}
				},
				execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
					if tc.execErr != nil {
						return pgconn.CommandTag{}, tc.execErr
					}
					return pgconn.NewCommandTag("UPDATE 1"), nil
				},
			}
			db := mockDB{
				beginFn: func(context.Context) (pgx.Tx, error) {
					if tc.beginErr != nil {
						return nil, tc.beginErr
					}
					return tx, nil
				},
			}
			store := NewPostgresVanadisStore(&db)
			got, err := store.UpdateBuild(context.Background(), tc.buildID, tc.req)
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("expected error %v, got %v", tc.wantErrIs, err)
				}
				return
			}
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got == nil || got.Title != tc.req.Title {
				t.Fatalf("unexpected updated build: %+v", got)
			}
		})
	}
}

func TestPostgresVanadisStore_UpdateChunk(t *testing.T) {
	modeBad := "bad-mode"
	modePR := "pr"
	modeAction := "action"
	tests := []struct {
		name          string
		buildID       string
		chunkRef      string
		req           UpdateChunkRequest
		beginErr      error
		buildScanErr  error
		buildStatus   string
		chunksErr     error
		scanChunksErr error
		chunks        []VanadisChunk
		execErr       error
		commitErr     error
		wantErrIs     error
		wantErr       bool
	}{
		{"empty_build_id", "", "c1", UpdateChunkRequest{Title: "T"}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true},
		{"empty_chunk_ref", "b1", "", UpdateChunkRequest{Title: "T"}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true},
		{"no_fields", "b1", "c1", UpdateChunkRequest{}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true},
		{"invalid_effort", "b1", "c1", UpdateChunkRequest{Effort: "extreme"}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true},
		{"begin_error", "b1", "c1", UpdateChunkRequest{Title: "T"}, errors.New("begin err"), nil, "", nil, nil, nil, nil, nil, nil, true},
		{"build_not_found", "b1", "c1", UpdateChunkRequest{Title: "T"}, nil, pgx.ErrNoRows, "", nil, nil, nil, nil, nil, ErrBuildNotFound, true},
		{"build_query_error", "b1", "c1", UpdateChunkRequest{Title: "T"}, nil, errors.New("query err"), "", nil, nil, nil, nil, nil, nil, true},
		{"build_conflict_status", "b1", "c1", UpdateChunkRequest{Title: "T"}, nil, nil, "completed", nil, nil, nil, nil, nil, ErrBuildConflict, true},
		{"chunks_query_error", "b1", "c1", UpdateChunkRequest{Title: "T"}, nil, nil, "running", errors.New("chunks query err"), nil, nil, nil, nil, nil, true},
		{"chunks_scan_error", "b1", "c1", UpdateChunkRequest{Title: "T"}, nil, nil, "running", nil, errors.New("chunks scan err"), []VanadisChunk{{ID: "c1"}}, nil, nil, nil, true},
		{"chunk_not_found", "b1", "c999", UpdateChunkRequest{Title: "T"}, nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", Status: "pending"}}, nil, nil, ErrChunkNotFound, true},
		{"chunk_conflict_status", "b1", "c1", UpdateChunkRequest{Title: "T"}, nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", Status: "running"}}, nil, nil, ErrBuildConflict, true},
		{"exec_error", "b1", "c1", UpdateChunkRequest{Title: "T"}, nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", Status: "pending"}}, errors.New("exec err"), nil, nil, true},
		{"commit_error", "b1", "c1", UpdateChunkRequest{Title: "T"}, nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", Status: "pending"}}, nil, errors.New("commit err"), nil, true},
		{"invalid_mode", "b1", "c1", UpdateChunkRequest{Mode: &modeBad}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true},
		{"pr_mode_empty_repo", "b1", "c1", UpdateChunkRequest{Mode: &modePR}, nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", Mode: "action", Repo: "", Status: "pending"}}, nil, nil, ErrInvalidBuildInput, true},
		{"action_mode_with_repo", "b1", "c1", UpdateChunkRequest{Mode: &modeAction, Repo: "aerial"}, nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", Mode: "pr", Repo: "aerial", Status: "pending"}}, nil, nil, ErrInvalidBuildInput, true},
		{"action_mode_with_files", "b1", "c1", UpdateChunkRequest{TargetFiles: []string{"a.go"}}, nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", Mode: "action", Repo: "", Status: "pending"}}, nil, nil, ErrInvalidBuildInput, true},
		{"action_mode_with_symbols", "b1", "c1", UpdateChunkRequest{TargetSymbols: []string{"A"}}, nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", Mode: "action", Repo: "", Status: "pending"}}, nil, nil, ErrInvalidBuildInput, true},
		{"transition_pr_to_action_mode_success", "b1", "c1", UpdateChunkRequest{Mode: &modeAction}, nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", Mode: "pr", Repo: "aerial", TargetFiles: []string{"a.go"}, TargetSymbols: []string{"A"}, Status: "pending"}}, nil, nil, nil, false},
		{"success", "b1", "c1", UpdateChunkRequest{Repo: "mirrormere", Title: "New Title", Instructions: "New Instr", Effort: "low", TargetFiles: []string{"a.go"}, TargetSymbols: []string{"A"}}, nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", Status: "pending"}}, nil, nil, nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := &mockTx{
				commitErr: tc.commitErr,
				queryRowFn: func(context.Context, string, ...any) pgx.Row {
					if tc.buildScanErr != nil {
						return &mockRow{err: tc.buildScanErr}
					}
					return &mockRow{b: &VanadisBuild{ID: tc.buildID, Status: tc.buildStatus}}
				},
				queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
					if tc.chunksErr != nil {
						return nil, tc.chunksErr
					}
					return &mockRows{items: tc.chunks, scanErr: tc.scanChunksErr}, nil
				},
				execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
					if tc.execErr != nil {
						return pgconn.CommandTag{}, tc.execErr
					}
					return pgconn.NewCommandTag("UPDATE 1"), nil
				},
			}
			db := mockDB{
				beginFn: func(context.Context) (pgx.Tx, error) {
					if tc.beginErr != nil {
						return nil, tc.beginErr
					}
					return tx, nil
				},
			}
			store := NewPostgresVanadisStore(&db)
			got, err := store.UpdateChunk(context.Background(), tc.buildID, tc.chunkRef, tc.req)
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("expected error %v, got %v", tc.wantErrIs, err)
				}
				return
			}
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got == nil || got.Title != tc.req.Title {
				t.Fatalf("unexpected updated chunk: %+v", got)
			}
			if tc.req.Mode != nil && *tc.req.Mode == "action" {
				if got.Mode != "action" || got.Repo != "" || len(got.TargetFiles) != 0 || len(got.TargetSymbols) != 0 {
					t.Fatalf("expected action mode with cleared repo/files/symbols, got %+v", got)
				}
			}
		})
	}
}

func TestPostgresVanadisStore_AppendChunks(t *testing.T) {
	tests := []struct {
		name          string
		buildID       string
		inputs        []CreateChunkInput
		beginErr      error
		buildScanErr  error
		buildStatus   string
		chunksErr     error
		scanChunksErr error
		existing      []VanadisChunk
		execErr       error
		commitErr     error
		wantErrIs     error
		wantErr       bool
		wantCount     int
	}{
		{"empty_build_id", "", []CreateChunkInput{{Title: "T", Instructions: "I"}}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true, 0},
		{"empty_inputs", "b1", nil, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true, 0},
		{"empty_title", "b1", []CreateChunkInput{{Title: "", Instructions: "I"}}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true, 0},
		{"empty_instructions", "b1", []CreateChunkInput{{Title: "T", Instructions: ""}}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true, 0},
		{"invalid_id", "b1", []CreateChunkInput{{ID: "bad:id", Title: "T", Instructions: "I"}}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true, 0},
		{"invalid_effort", "b1", []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial", Effort: "super"}}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true, 0},
		{"invalid_mode", "b1", []CreateChunkInput{{Mode: "bad", Title: "T", Instructions: "I"}}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true, 0},
		{"pr_mode_empty_repo", "b1", []CreateChunkInput{{Mode: "pr", Repo: "", Title: "T", Instructions: "I"}}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true, 0},
		{"action_mode_with_repo", "b1", []CreateChunkInput{{Mode: "action", Repo: "aerial", Title: "T", Instructions: "I"}}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true, 0},
		{"action_mode_with_files", "b1", []CreateChunkInput{{Mode: "action", TargetFiles: []string{"a.go"}, Title: "T", Instructions: "I"}}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true, 0},
		{"action_mode_with_symbols", "b1", []CreateChunkInput{{Mode: "action", TargetSymbols: []string{"A"}, Title: "T", Instructions: "I"}}, nil, nil, "", nil, nil, nil, nil, nil, ErrInvalidBuildInput, true, 0},
		{"begin_error", "b1", []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial"}}, errors.New("begin err"), nil, "", nil, nil, nil, nil, nil, nil, true, 0},
		{"build_not_found", "b1", []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial"}}, nil, pgx.ErrNoRows, "", nil, nil, nil, nil, nil, ErrBuildNotFound, true, 0},
		{"build_query_error", "b1", []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial"}}, nil, errors.New("query err"), "", nil, nil, nil, nil, nil, nil, true, 0},
		{"build_conflict_status", "b1", []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial"}}, nil, nil, "completed", nil, nil, nil, nil, nil, ErrBuildConflict, true, 0},
		{"chunks_query_error", "b1", []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial"}}, nil, nil, "running", errors.New("chunks err"), nil, nil, nil, nil, nil, true, 0},
		{"chunks_scan_error", "b1", []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial"}}, nil, nil, "running", nil, errors.New("scan err"), []VanadisChunk{{ID: "c1"}}, nil, nil, nil, true, 0},
		{"duplicate_id_conflict", "b1", []CreateChunkInput{{ID: "c1", Title: "T", Instructions: "I", Repo: "aerial"}}, nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1"}}, nil, nil, ErrBuildConflict, true, 0},
		{"exec_insert_error", "b1", []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial"}}, nil, nil, "running", nil, nil, nil, errors.New("insert err"), nil, nil, true, 0},
		{"commit_error", "b1", []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial"}}, nil, nil, "running", nil, nil, nil, nil, errors.New("commit err"), nil, true, 0},
		{"success_with_id_collision_resolution", "b1", []CreateChunkInput{{Title: "T", Instructions: "I", Repo: "aerial"}}, nil, nil, "running", nil, nil, []VanadisChunk{{ID: "b1-chunk-1", ChunkIndex: 0, Repo: "mirrormere"}}, nil, nil, nil, false, 1},
		{"success_append_action_chunk", "b1", []CreateChunkInput{{Mode: "action", Title: "Act", Instructions: "Do Act"}}, nil, nil, "running", nil, nil, nil, nil, nil, nil, false, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := &mockTx{
				commitErr: tc.commitErr,
				queryRowFn: func(context.Context, string, ...any) pgx.Row {
					if tc.buildScanErr != nil {
						return &mockRow{err: tc.buildScanErr}
					}
					return &mockRow{b: &VanadisBuild{ID: tc.buildID, Status: tc.buildStatus}}
				},
				queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
					if tc.chunksErr != nil {
						return nil, tc.chunksErr
					}
					return &mockRows{items: tc.existing, scanErr: tc.scanChunksErr}, nil
				},
				execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
					if tc.execErr != nil {
						return pgconn.CommandTag{}, tc.execErr
					}
					return pgconn.NewCommandTag("INSERT 0 1"), nil
				},
			}
			db := mockDB{
				beginFn: func(context.Context) (pgx.Tx, error) {
					if tc.beginErr != nil {
						return nil, tc.beginErr
					}
					return tx, nil
				},
			}
			store := NewPostgresVanadisStore(&db)
			got, err := store.AppendChunks(context.Background(), tc.buildID, tc.inputs)
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("expected error %v, got %v", tc.wantErrIs, err)
				}
				return
			}
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != tc.wantCount {
				t.Fatalf("got %d chunks, want %d", len(got), tc.wantCount)
			}
		})
	}
}

func TestPostgresVanadisStore_RemoveChunk(t *testing.T) {
	tests := []struct {
		name           string
		buildID        string
		chunkRef       string
		beginErr       error
		buildScanErr   error
		buildStatus    string
		chunksErr      error
		scanChunksErr  error
		existing       []VanadisChunk
		delExecErr     error
		compactExecErr error
		commitErr      error
		wantErrIs      error
		wantErr        bool
		wantRemainLen  int
	}{
		{"empty_build_id", "", "c1", nil, nil, "", nil, nil, nil, nil, nil, nil, ErrInvalidBuildInput, true, 0},
		{"empty_chunk_ref", "b1", "", nil, nil, "", nil, nil, nil, nil, nil, nil, ErrInvalidBuildInput, true, 0},
		{"begin_error", "b1", "c1", errors.New("begin err"), nil, "", nil, nil, nil, nil, nil, nil, nil, true, 0},
		{"build_not_found", "b1", "c1", nil, pgx.ErrNoRows, "", nil, nil, nil, nil, nil, nil, ErrBuildNotFound, true, 0},
		{"build_query_error", "b1", "c1", nil, errors.New("query err"), "", nil, nil, nil, nil, nil, nil, nil, true, 0},
		{"build_conflict_status", "b1", "c1", nil, nil, "completed", nil, nil, nil, nil, nil, nil, ErrBuildConflict, true, 0},
		{"chunks_query_error", "b1", "c1", nil, nil, "running", errors.New("chunks err"), nil, nil, nil, nil, nil, nil, true, 0},
		{"chunks_scan_error", "b1", "c1", nil, nil, "running", nil, errors.New("scan err"), []VanadisChunk{{ID: "c1"}}, nil, nil, nil, nil, true, 0},
		{"chunk_not_found", "b1", "c999", nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", Status: "pending"}}, nil, nil, nil, ErrChunkNotFound, true, 0},
		{"chunk_conflict_status", "b1", "c1", nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", Status: "running"}, {ID: "c2", Status: "pending"}}, nil, nil, nil, ErrBuildConflict, true, 0},
		{"cannot_remove_only_chunk", "b1", "c1", nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", Status: "pending"}}, nil, nil, nil, ErrBuildConflict, true, 0},
		{"delete_exec_error", "b1", "c2", nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", ChunkIndex: 0, Status: "running"}, {ID: "c2", ChunkIndex: 1, Status: "pending"}}, errors.New("del err"), nil, nil, nil, true, 0},
		{"compact_exec_error", "b1", "c1", nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", ChunkIndex: 0, Status: "pending"}, {ID: "c2", ChunkIndex: 1, Status: "pending"}}, nil, errors.New("compact err"), nil, nil, true, 0},
		{"commit_error", "b1", "c2", nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", ChunkIndex: 0, Status: "running"}, {ID: "c2", ChunkIndex: 1, Status: "pending"}}, nil, nil, errors.New("commit err"), nil, true, 0},
		{"success", "b1", "1", nil, nil, "running", nil, nil, []VanadisChunk{{ID: "c1", ChunkIndex: 0, Status: "running"}, {ID: "c2", ChunkIndex: 1, Status: "pending"}, {ID: "c3", ChunkIndex: 2, Status: "pending"}}, nil, nil, nil, nil, false, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := &mockTx{
				commitErr: tc.commitErr,
				queryRowFn: func(context.Context, string, ...any) pgx.Row {
					if tc.buildScanErr != nil {
						return &mockRow{err: tc.buildScanErr}
					}
					return &mockRow{b: &VanadisBuild{ID: tc.buildID, Status: tc.buildStatus}}
				},
				queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
					if tc.chunksErr != nil {
						return nil, tc.chunksErr
					}
					return &mockRows{items: tc.existing, scanErr: tc.scanChunksErr}, nil
				},
				execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
					if tc.delExecErr != nil {
						return pgconn.CommandTag{}, tc.delExecErr
					}
					if tc.compactExecErr != nil {
						return pgconn.CommandTag{}, tc.compactExecErr
					}
					return pgconn.NewCommandTag("DELETE 1"), nil
				},
			}
			db := mockDB{
				beginFn: func(context.Context) (pgx.Tx, error) {
					if tc.beginErr != nil {
						return nil, tc.beginErr
					}
					return tx, nil
				},
			}
			store := NewPostgresVanadisStore(&db)
			removed, remaining, err := store.RemoveChunk(context.Background(), tc.buildID, tc.chunkRef)
			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("expected error %v, got %v", tc.wantErrIs, err)
				}
				return
			}
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if removed == nil {
				t.Fatal("expected removed chunk, got nil")
			}
			if len(remaining) != tc.wantRemainLen {
				t.Fatalf("got %d remaining chunks, want %d", len(remaining), tc.wantRemainLen)
			}
		})
	}
}

func TestPostgresVanadisStore_ClaimStalledChunk(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                                  string
		chunkID, mode, status, buildStatus    string
		prNumber, nudges                      int
		beginErr, scanErr, execErr, commitErr error
		wantVerdict                           StalledChunkVerdict
		wantErr                               error
		wantAnyErr, wantNudgeExec             bool
	}{
		{name: "pr chunk without pr first stall nudges", chunkID: "c1", mode: "pr", status: "running", buildStatus: "running", wantVerdict: StalledChunkVerdictNudge, wantNudgeExec: true},
		{name: "pr chunk without pr second stall fails", chunkID: "c1", mode: "pr", status: "running", buildStatus: "running", nudges: 1, wantVerdict: StalledChunkVerdictFail},
		{name: "pr opened chunk is not stalled", chunkID: "c1", mode: "pr", status: "pr_opened", buildStatus: "running", prNumber: 42, wantVerdict: StalledChunkVerdictNone},
		{name: "running chunk with linked pr is not stalled", chunkID: "c1", mode: "pr", status: "running", buildStatus: "running", prNumber: 42, wantVerdict: StalledChunkVerdictNone},
		{name: "action chunk is never stalled", chunkID: "c1", mode: "action", status: "running", buildStatus: "running", wantVerdict: StalledChunkVerdictNone},
		{name: "build not running is not stalled", chunkID: "c1", mode: "pr", status: "running", buildStatus: "failed", wantVerdict: StalledChunkVerdictNone},
		{name: "empty chunk id rejected", chunkID: "  ", wantAnyErr: true},
		{name: "begin error", chunkID: "c1", beginErr: errors.New("begin boom"), wantAnyErr: true},
		{name: "missing chunk", chunkID: "c1", scanErr: pgx.ErrNoRows, wantErr: ErrChunkNotFound},
		{name: "select error", chunkID: "c1", scanErr: errors.New("select boom"), wantAnyErr: true},
		{name: "nudge update error", chunkID: "c1", mode: "pr", status: "running", buildStatus: "running", execErr: errors.New("exec boom"), wantAnyErr: true, wantNudgeExec: true},
		{name: "nudge commit error", chunkID: "c1", mode: "pr", status: "running", buildStatus: "running", commitErr: errors.New("commit boom"), wantAnyErr: true, wantNudgeExec: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var nudgeExecs []any
			tx := &mockTx{
				commitErr: tc.commitErr,
				queryRowFn: func(context.Context, string, ...any) pgx.Row {
					return &genericMockRow{scanFn: func(dest ...any) error {
						if tc.scanErr != nil {
							return tc.scanErr
						}
						*dest[0].(*string), *dest[1].(*string), *dest[3].(*string), *dest[4].(*string), *dest[5].(*string) = tc.chunkID, "b1", tc.mode, "Stalled", tc.status
						*dest[6].(*int), *dest[7].(*int) = tc.prNumber, tc.nudges
						*dest[8].(*string), *dest[9].(*string), *dest[10].(*string), *dest[11].(*string) = "b1", "thread-1", "Objective", tc.buildStatus
						return nil
					}}
				},
				execFn: func(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
					if strings.Contains(sql, "stall_nudges = stall_nudges + 1") {
						nudgeExecs = append(nudgeExecs, args...)
					}
					return pgconn.NewCommandTag("UPDATE 1"), tc.execErr
				},
			}
			store := newMockStore(mockDB{beginFn: func(context.Context) (pgx.Tx, error) { return tx, tc.beginErr }})
			verdict, build, chunk, err := store.ClaimStalledChunk(context.Background(), tc.chunkID)
			if gotNudge := len(nudgeExecs) == 1 && nudgeExecs[0] == "c1"; gotNudge != tc.wantNudgeExec {
				t.Fatalf("nudge exec = %v (args %v), want %v", gotNudge, nudgeExecs, tc.wantNudgeExec)
			}
			if tc.wantErr != nil || tc.wantAnyErr {
				if err == nil || (tc.wantErr != nil && !errors.Is(err, tc.wantErr)) {
					t.Fatalf("err = %v, want %v (any=%v)", err, tc.wantErr, tc.wantAnyErr)
				}
				return
			}
			if err != nil || verdict != tc.wantVerdict || build == nil || build.ID != "b1" || build.ThreadID != "thread-1" || chunk == nil || chunk.ID != "c1" {
				t.Fatalf("got verdict=%q build=%+v chunk=%+v err=%v, want verdict %q", verdict, build, chunk, err, tc.wantVerdict)
			}
		})
	}
	var nilStore *PostgresVanadisStore
	if _, _, _, err := nilStore.ClaimStalledChunk(context.Background(), "c1"); err == nil {
		t.Fatal("expected error for nil store")
	}
}
