package main

// Shared retry/recovery test fixtures retained in the root package while the
// remaining consumers complete their moves: action_retry_test.go (handler
// tests, staying) uses retryFixtureStore; store_pr_recovery_test.go (chunk 8)
// uses buildRow and retryTransaction. The Test functions that lived in this
// file moved to internal/store in chunk 7; these helpers are temporarily
// duplicated across both packages until chunk 8 consolidates them.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// retryTransaction models the external database's atomic commit and parent lock.
// State changes remain private until Commit; Rollback discards them.
type retryTransaction struct {
	*mockTx

	finish func(bool) error
}

func (tx *retryTransaction) Commit(context.Context) error   { return tx.finish(true) }
func (tx *retryTransaction) Rollback(context.Context) error { return tx.finish(false) }

func retryFixtureStore(mu *sync.Mutex, current *model.VanadisChunk, buildStatus *string, sibling model.VanadisChunk, failBuild bool, lost *bool) *PostgresVanadisStore {
	return newMockStore(mockDB{beginFn: func(context.Context) (pgx.Tx, error) {
		var local model.VanadisChunk
		var parent string
		locked := false
		closed := false
		base := &mockTx{
			queryRowFn: func(_ context.Context, query string, _ ...any) pgx.Row {
				if !strings.Contains(query, "FROM vanadis_builds") || !strings.Contains(query, "FOR UPDATE") {
					return errRow(errors.New("missing parent lock"))
				}
				mu.Lock()
				locked = true
				local = *current
				parent = *buildStatus
				return buildRow("b1", parent)
			},
			queryFn: func(_ context.Context, query string, _ ...any) (pgx.Rows, error) {
				if !locked || !strings.Contains(query, "ORDER BY chunk_index ASC FOR UPDATE") {
					return nil, errors.New("missing ordered chunk locks")
				}
				return &mockRows{items: []model.VanadisChunk{sibling, local}}, nil
			},
			execFn: func(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
				return retryFixtureExec(query, &local, &parent, failBuild, args...)
			},
		}
		return &retryTransaction{mockTx: base, finish: func(commit bool) error {
			if closed {
				return pgx.ErrTxClosed
			}
			closed = true
			if !locked {
				return errors.New("transaction never locked parent")
			}
			defer mu.Unlock()
			if commit {
				*current = local
				*buildStatus = parent
				if *lost {
					*lost = false
					return errors.New("commit response lost")
				}
			}
			return nil
		}}, nil
	}})
}

func retryFixtureExec(query string, local *model.VanadisChunk, parent *string, failBuild bool, args ...any) (pgconn.CommandTag, error) {
	switch {
	case strings.Contains(query, "UPDATE vanadis_chunks"):
		for _, fragment := range []string{"status = 'pending'", "stall_nudges = stall_nudges + 1", "started_at = NULL", "completed_at = NULL", "ci_fail_count = 0", "ci_checks = '{}'::jsonb", "rollout_state = '{}'::jsonb", "banner = ''", "merge_awakeable_id = ''", "deploy_awakeable_id = ''"} {
			if !strings.Contains(query, fragment) {
				return pgconn.CommandTag{}, fmt.Errorf("missing reset %s", fragment)
			}
		}
		if strings.Contains(query, "instructions = COALESCE($2, instructions)") && len(args) == 2 {
			if replacement, ok := args[1].(*string); ok && replacement != nil {
				local.Instructions = *replacement
			}
		}
		local.Status = "pending"
		local.StallNudges++
	case strings.Contains(query, "UPDATE vanadis_builds SET status = 'running'"):
		if failBuild {
			return pgconn.CommandTag{}, errors.New("build write failed")
		}
		*parent = "running"
	default:
		return pgconn.CommandTag{}, errors.New("unexpected mutation")
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func buildRow(id, status string) pgx.Row {
	return valuesRow(id, "thread", 3, "title", "objective", "", status, "", false, time.Time{}, time.Time{})
}
