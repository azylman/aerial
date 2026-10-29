package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type mockVanadisPgxRow struct {
	scanFn func(dest ...any) error
}

func (r mockVanadisPgxRow) Scan(dest ...any) error {
	if r.scanFn != nil {
		return r.scanFn(dest...)
	}
	return nil
}

type mockVanadisPgxRows struct {
	pgx.Rows
	items     []VanadisChunkSummary
	intItems  []int
	idx       int
	scanErr   error
	rowsErr   error
	totalRows int
	scanRowFn func(idx int, dest ...any) error
}

func (r *mockVanadisPgxRows) Close()     {}
func (r *mockVanadisPgxRows) Err() error { return r.rowsErr }
func (r *mockVanadisPgxRows) Next() bool {
	limit := len(r.items)
	if len(r.intItems) > limit {
		limit = len(r.intItems)
	}
	if r.totalRows > limit {
		limit = r.totalRows
	}
	if r.idx < limit {
		r.idx++
		return true
	}
	return false
}

func (r *mockVanadisPgxRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if r.scanRowFn != nil {
		return r.scanRowFn(r.idx-1, dest...)
	}
	if len(r.intItems) > 0 {
		*dest[0].(*int) = r.intItems[r.idx-1]
		return nil
	}
	it := r.items[r.idx-1]
	*dest[0].(*string) = it.ID
	*dest[1].(*int) = it.ChunkIndex
	*dest[2].(*string) = it.Repo
	*dest[3].(*string) = it.Title
	*dest[4].(*string) = it.Status
	*dest[5].(*int) = it.PRNumber
	if len(dest) > 6 {
		if !it.StartedAt.IsZero() {
			*dest[6].(*sql.NullTime) = sql.NullTime{Time: it.StartedAt, Valid: true}
		} else {
			*dest[6].(*sql.NullTime) = sql.NullTime{}
		}
	}
	if len(dest) > 7 {
		*dest[7].(*time.Time) = it.UpdatedAt
	}
	return nil
}

type mockVanadisPgxTx struct {
	pgx.Tx
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
	execFn     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	queryFn    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	commitErr  error
	rbErr      error
}

func (t *mockVanadisPgxTx) Commit(ctx context.Context) error { return t.commitErr }
func (t *mockVanadisPgxTx) Rollback(ctx context.Context) error {
	if t.rbErr != nil {
		return t.rbErr
	}
	return pgx.ErrTxClosed
}

func (t *mockVanadisPgxTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if t.queryRowFn != nil {
		return t.queryRowFn(ctx, sql, args...)
	}
	return mockVanadisPgxRow{}
}

func (t *mockVanadisPgxTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if t.execFn != nil {
		return t.execFn(ctx, sql, args...)
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (t *mockVanadisPgxTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if t.queryFn != nil {
		return t.queryFn(ctx, sql, args...)
	}
	return &mockVanadisPgxRows{}, nil
}

type mockVanadisPgxPool struct {
	beginFn    func(ctx context.Context) (pgx.Tx, error)
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
	execFn     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	queryFn    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func (p *mockVanadisPgxPool) Begin(ctx context.Context) (pgx.Tx, error) {
	if p.beginFn != nil {
		return p.beginFn(ctx)
	}
	return &mockVanadisPgxTx{}, nil
}

func (p *mockVanadisPgxPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if p.queryRowFn != nil {
		return p.queryRowFn(ctx, sql, args...)
	}
	return mockVanadisPgxRow{}
}

func (p *mockVanadisPgxPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if p.execFn != nil {
		return p.execFn(ctx, sql, args...)
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (p *mockVanadisPgxPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if p.queryFn != nil {
		return p.queryFn(ctx, sql, args...)
	}
	return &mockVanadisPgxRows{}, nil
}

func TestPostgresPRRegistry_Unit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name           string
		op             string
		nilPool        bool
		repo           string
		prNumber       int
		sha            string
		jobName        string
		jobs           []string
		targetID       string
		limit          int
		mockSelectErr  error
		mockScanErr    error
		mockExecErr    error
		mockBeginErr   error
		mockCommitErr  error
		mockRowsErr    error
		rowsAffected   int64
		targetIDReturn string
		branchReturn   string
		mergeSHAReturn string
		metaJSON       string
		totalRows      int
		vanadisStatus  int
		vanadisBody    string
		wantErr        bool
		wantUpdated    bool
		wantTarget     string
		wantPRNum      int
		wantCount      int
		wantSQLSubstr  string
	}{
		// Single-row transitions: UpdatePRMerged
		{name: "UpdatePRMerged success", op: "UpdatePRMerged", repo: "azylman/aerial", prNumber: 42, sha: "merge123", targetIDReturn: "1555405874565091380", wantTarget: "1555405874565091380", wantSQLSubstr: "CASE WHEN status IN ('synced', 'deploying', 'deployed', 'deploy_failed') THEN status ELSE 'merged' END"},
		{name: "UpdatePRMerged not found", op: "UpdatePRMerged", repo: "azylman/aerial", prNumber: 42, sha: "merge123", mockSelectErr: pgx.ErrNoRows, wantTarget: ""},
		{name: "UpdatePRMerged query error", op: "UpdatePRMerged", repo: "azylman/aerial", prNumber: 42, sha: "merge123", mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "UpdatePRMerged nil pool guard", op: "UpdatePRMerged", nilPool: true, repo: "azylman/aerial", prNumber: 42, sha: "merge123"},
		{name: "UpdatePRMerged empty repo guard", op: "UpdatePRMerged", repo: "", prNumber: 42},
		{name: "UpdatePRMerged prNumber zero guard", op: "UpdatePRMerged", repo: "azylman/aerial", prNumber: 0},

		// UpdatePRClosedUnmerged
		{name: "UpdatePRClosedUnmerged success", op: "UpdatePRClosedUnmerged", repo: "azylman/aerial", prNumber: 42, targetIDReturn: "1555405874565091380", wantTarget: "1555405874565091380"},
		{name: "UpdatePRClosedUnmerged not found", op: "UpdatePRClosedUnmerged", repo: "azylman/aerial", prNumber: 42, mockSelectErr: pgx.ErrNoRows, wantTarget: ""},
		{name: "UpdatePRClosedUnmerged db error", op: "UpdatePRClosedUnmerged", repo: "azylman/aerial", prNumber: 42, mockSelectErr: errors.New("db error"), wantErr: true},

		// UpdatePRSync
		{name: "UpdatePRSync success", op: "UpdatePRSync", repo: "azylman/aerial", prNumber: 42, sha: "head123", targetIDReturn: "1555405874565091380", wantTarget: "1555405874565091380", wantSQLSubstr: "(COALESCE(metadata, '{}'::jsonb) - 'banner') - 'ci_checks'"},
		{name: "UpdatePRSync not found", op: "UpdatePRSync", repo: "azylman/aerial", prNumber: 42, sha: "head123", mockSelectErr: pgx.ErrNoRows, wantTarget: ""},
		{name: "UpdatePRSync db error", op: "UpdatePRSync", repo: "azylman/aerial", prNumber: 42, sha: "head123", mockSelectErr: errors.New("db error"), wantErr: true},

		// BackfillPushMergeSHA
		{name: "BackfillPushMergeSHA success", op: "BackfillPushMergeSHA", repo: "azylman/aerial", prNumber: 42, sha: "merge123", targetIDReturn: "1555405874565091380", wantTarget: "1555405874565091380", wantSQLSubstr: "CASE WHEN status IN ('synced', 'deploying', 'deployed', 'deploy_failed') THEN status ELSE 'merged' END"},
		{name: "BackfillPushMergeSHA not found", op: "BackfillPushMergeSHA", repo: "azylman/aerial", prNumber: 42, sha: "merge123", mockSelectErr: pgx.ErrNoRows, wantTarget: ""},
		{name: "BackfillPushMergeSHA db error", op: "BackfillPushMergeSHA", repo: "azylman/aerial", prNumber: 42, sha: "merge123", mockSelectErr: errors.New("db error"), wantErr: true},

		// IncrementCIFailCount
		{name: "IncrementCIFailCount success", op: "IncrementCIFailCount", repo: "azylman/aerial", prNumber: 42, wantPRNum: 2},
		{name: "IncrementCIFailCount not found", op: "IncrementCIFailCount", repo: "azylman/aerial", prNumber: 42, mockSelectErr: pgx.ErrNoRows, wantPRNum: 0},
		{name: "IncrementCIFailCount db error", op: "IncrementCIFailCount", repo: "azylman/aerial", prNumber: 42, mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "IncrementCIFailCount nil pool guard", op: "IncrementCIFailCount", nilPool: true, repo: "azylman/aerial", prNumber: 42},
		{name: "IncrementCIFailCount empty repo guard", op: "IncrementCIFailCount", repo: "", prNumber: 42},
		{name: "IncrementCIFailCount prNumber zero guard", op: "IncrementCIFailCount", repo: "azylman/aerial", prNumber: 0},

		// RecordPRCheckStatus
		{name: "RecordPRCheckStatus success", op: "RecordPRCheckStatus", repo: "azylman/aerial", prNumber: 42, sha: "head123", jobName: "Verify Codebase", targetID: "passed", wantUpdated: true, wantSQLSubstr: "ci_checks"},
		{name: "RecordPRCheckStatus distinct unchanged (not found)", op: "RecordPRCheckStatus", repo: "azylman/aerial", prNumber: 42, sha: "head123", jobName: "Verify Codebase", targetID: "passed", mockSelectErr: pgx.ErrNoRows, wantUpdated: false},
		{name: "RecordPRCheckStatus db error", op: "RecordPRCheckStatus", repo: "azylman/aerial", prNumber: 42, sha: "head123", jobName: "Verify Codebase", targetID: "passed", mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "RecordPRCheckStatus nil pool guard", op: "RecordPRCheckStatus", nilPool: true, repo: "azylman/aerial", prNumber: 42, sha: "head123", jobName: "Verify Codebase", targetID: "passed"},
		{name: "RecordPRCheckStatus empty repo guard", op: "RecordPRCheckStatus", repo: "", prNumber: 42, sha: "head123", jobName: "Verify Codebase", targetID: "passed"},
		{name: "RecordPRCheckStatus prNumber zero guard", op: "RecordPRCheckStatus", repo: "azylman/aerial", prNumber: 0, sha: "head123", jobName: "Verify Codebase", targetID: "passed"},
		{name: "RecordPRCheckStatus empty headSHA guard", op: "RecordPRCheckStatus", repo: "azylman/aerial", prNumber: 42, sha: "", jobName: "Verify Codebase", targetID: "passed"},
		{name: "RecordPRCheckStatus zero headSHA guard", op: "RecordPRCheckStatus", repo: "azylman/aerial", prNumber: 42, sha: "0000000000000000000000000000000000000000", jobName: "Verify Codebase", targetID: "passed"},
		{name: "RecordPRCheckStatus empty checkName guard", op: "RecordPRCheckStatus", repo: "azylman/aerial", prNumber: 42, sha: "head123", jobName: "", targetID: "passed"},
		{name: "RecordPRCheckStatus empty checkState guard", op: "RecordPRCheckStatus", repo: "azylman/aerial", prNumber: 42, sha: "head123", jobName: "Verify Codebase", targetID: ""},

		// AtomicTransitionCIFailed
		{name: "AtomicTransitionCIFailed success", op: "AtomicTransitionCIFailed", repo: "azylman/aerial", prNumber: 42, targetIDReturn: "1555405874565091380", wantTarget: "1555405874565091380", wantUpdated: true, wantSQLSubstr: "COALESCE(merge_sha, '') = '' AND status NOT IN ('ci_failed', 'merged', 'synced', 'closed', 'deploying', 'deployed', 'deploy_failed')"},
		{name: "AtomicTransitionCIFailed not found", op: "AtomicTransitionCIFailed", repo: "azylman/aerial", prNumber: 42, mockSelectErr: pgx.ErrNoRows, wantTarget: "", wantUpdated: false},
		{name: "AtomicTransitionCIFailed db error", op: "AtomicTransitionCIFailed", repo: "azylman/aerial", prNumber: 42, mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "AtomicTransitionCIFailed empty repo guard", op: "AtomicTransitionCIFailed", repo: "", prNumber: 42},
		{name: "AtomicTransitionCIFailed prNumber zero guard", op: "AtomicTransitionCIFailed", repo: "azylman/aerial", prNumber: 0},

		// AtomicTransitionConflict
		{name: "AtomicTransitionConflict success", op: "AtomicTransitionConflict", repo: "azylman/aerial", prNumber: 42, targetIDReturn: "1555405874565091380", wantTarget: "1555405874565091380", wantUpdated: true, wantSQLSubstr: "COALESCE(merge_sha, '') = '' AND status NOT IN ('conflict', 'merged', 'synced', 'closed', 'deploying', 'deployed', 'deploy_failed')"},
		{name: "AtomicTransitionConflict not found", op: "AtomicTransitionConflict", repo: "azylman/aerial", prNumber: 42, mockSelectErr: pgx.ErrNoRows, wantTarget: "", wantUpdated: false},
		{name: "AtomicTransitionConflict db error", op: "AtomicTransitionConflict", repo: "azylman/aerial", prNumber: 42, mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "AtomicTransitionConflict empty repo guard", op: "AtomicTransitionConflict", repo: "", prNumber: 42},
		{name: "AtomicTransitionConflict prNumber zero guard", op: "AtomicTransitionConflict", repo: "azylman/aerial", prNumber: 0},

		// AtomicTransitionDeployFailed
		{name: "AtomicTransitionDeployFailed success", op: "AtomicTransitionDeployFailed", repo: "azylman/aerial", prNumber: 42, targetIDReturn: "1555405874565091380", wantTarget: "1555405874565091380", wantUpdated: true},
		{name: "AtomicTransitionDeployFailed not found", op: "AtomicTransitionDeployFailed", repo: "azylman/aerial", prNumber: 42, mockSelectErr: pgx.ErrNoRows, wantTarget: "", wantUpdated: false},
		{name: "AtomicTransitionDeployFailed db error", op: "AtomicTransitionDeployFailed", repo: "azylman/aerial", prNumber: 42, mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "AtomicTransitionDeployFailed empty repo guard", op: "AtomicTransitionDeployFailed", repo: "", prNumber: 42},
		{name: "AtomicTransitionDeployFailed prNumber zero guard", op: "AtomicTransitionDeployFailed", repo: "azylman/aerial", prNumber: 0},

		// AtomicTransitionCIFailedByHeadSHA
		{name: "AtomicTransitionCIFailedByHeadSHA success", op: "AtomicTransitionCIFailedByHeadSHA", repo: "azylman/aerial", sha: "sha123", targetIDReturn: "1555405874565091380", wantTarget: "1555405874565091380", wantPRNum: 42, wantUpdated: true, wantSQLSubstr: "COALESCE(merge_sha, '') = '' AND status NOT IN ('ci_failed', 'merged', 'synced', 'closed', 'deploying', 'deployed', 'deploy_failed')"},
		{name: "AtomicTransitionCIFailedByHeadSHA not found", op: "AtomicTransitionCIFailedByHeadSHA", repo: "azylman/aerial", sha: "sha123", mockSelectErr: pgx.ErrNoRows, wantUpdated: false},
		{name: "AtomicTransitionCIFailedByHeadSHA db error", op: "AtomicTransitionCIFailedByHeadSHA", repo: "azylman/aerial", sha: "sha123", mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "AtomicTransitionCIFailedByHeadSHA empty sha guard", op: "AtomicTransitionCIFailedByHeadSHA", repo: "azylman/aerial", sha: ""},
		{name: "AtomicTransitionCIFailedByHeadSHA zero sha guard", op: "AtomicTransitionCIFailedByHeadSHA", repo: "azylman/aerial", sha: "0000000000000000000000000000000000000000"},

		// ResolvePRBySHA
		{name: "ResolvePRBySHA success", op: "ResolvePRBySHA", repo: "azylman/aerial", sha: "sha123", wantPRNum: 42, branchReturn: "feat/sha", targetIDReturn: "1555405874565091380", wantTarget: "1555405874565091380"},
		{name: "ResolvePRBySHA not found", op: "ResolvePRBySHA", repo: "azylman/aerial", sha: "sha123", mockSelectErr: pgx.ErrNoRows, wantPRNum: 0},
		{name: "ResolvePRBySHA db error", op: "ResolvePRBySHA", repo: "azylman/aerial", sha: "sha123", mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "ResolvePRBySHA zero sha guard", op: "ResolvePRBySHA", repo: "azylman/aerial", sha: "0000000000000000000000000000000000000000", wantPRNum: 0},

		// ResolvePRByNumber
		{name: "ResolvePRByNumber success", op: "ResolvePRByNumber", repo: "azylman/aerial", prNumber: 42, targetIDReturn: "1555405874565091380", branchReturn: "feat/by-num", mergeSHAReturn: "merge123", wantTarget: "1555405874565091380"},
		{name: "ResolvePRByNumber not found", op: "ResolvePRByNumber", repo: "azylman/aerial", prNumber: 42, mockSelectErr: pgx.ErrNoRows, wantTarget: ""},
		{name: "ResolvePRByNumber db error", op: "ResolvePRByNumber", repo: "azylman/aerial", prNumber: 42, mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "ResolvePRByNumber zero pr guard", op: "ResolvePRByNumber", repo: "azylman/aerial", prNumber: 0, wantTarget: ""},

		// TransitionDeploying
		{name: "TransitionDeploying empty jobs guard", op: "TransitionDeploying", repo: "azylman/aerial", prNumber: 42, jobs: nil, wantTarget: ""},
		{name: "TransitionDeploying empty repo guard", op: "TransitionDeploying", repo: "", prNumber: 42, jobs: []string{"brain"}},
		{name: "TransitionDeploying zero prNumber guard", op: "TransitionDeploying", repo: "azylman/aerial", prNumber: 0, jobs: []string{"brain"}},
		{name: "TransitionDeploying success", op: "TransitionDeploying", repo: "azylman/aerial", prNumber: 42, jobs: []string{"brain", "webhooks-router"}, targetIDReturn: "1555405874565091380", wantTarget: "1555405874565091380"},
		{name: "TransitionDeploying db error", op: "TransitionDeploying", repo: "azylman/aerial", prNumber: 42, jobs: []string{"brain"}, mockSelectErr: errors.New("db error"), wantErr: true},

		// ListOpenPRs
		{name: "ListOpenPRs success", op: "ListOpenPRs", repo: "azylman/aerial", totalRows: 2, wantCount: 2, wantSQLSubstr: "COALESCE(merge_sha, '') = '' AND status NOT IN ('merged', 'synced', 'closed', 'conflict', 'deploying', 'deployed', 'deploy_failed')"},
		{name: "ListOpenPRs query error", op: "ListOpenPRs", repo: "azylman/aerial", mockSelectErr: errors.New("query err"), wantErr: true},
		{name: "ListOpenPRs scan error", op: "ListOpenPRs", repo: "azylman/aerial", totalRows: 1, mockScanErr: errors.New("scan err"), wantErr: true},
		{name: "ListOpenPRs empty repo guard", op: "ListOpenPRs", repo: ""},

		// ListDeployingPRs
		{name: "ListDeployingPRs success", op: "ListDeployingPRs", totalRows: 2, metaJSON: `{"jobs":["brain"]}`, wantCount: 2},
		{name: "ListDeployingPRs invalid metadata JSON warning still succeeds", op: "ListDeployingPRs", totalRows: 1, metaJSON: `{invalid json`, wantCount: 1},
		{name: "ListDeployingPRs query error", op: "ListDeployingPRs", mockSelectErr: errors.New("query err"), wantErr: true},

		// AtomicTransitionDeployedByJob
		{name: "AtomicTransitionDeployedByJob empty job guard", op: "AtomicTransitionDeployedByJob", jobName: "", wantUpdated: false},
		{name: "AtomicTransitionDeployedByJob begin tx error", op: "AtomicTransitionDeployedByJob", jobName: "brain", mockBeginErr: errors.New("begin err"), wantErr: true},
		{name: "AtomicTransitionDeployedByJob select ErrNoRows", op: "AtomicTransitionDeployedByJob", jobName: "brain", mockSelectErr: pgx.ErrNoRows, wantUpdated: false},
		{name: "AtomicTransitionDeployedByJob select error", op: "AtomicTransitionDeployedByJob", jobName: "brain", mockSelectErr: errors.New("select err"), wantErr: true},
		{name: "AtomicTransitionDeployedByJob remaining jobs update success", op: "AtomicTransitionDeployedByJob", jobName: "cadvisor", metaJSON: `{"jobs":["cadvisor","node-exporter"]}`, wantUpdated: false},
		{name: "AtomicTransitionDeployedByJob remaining jobs update exec error", op: "AtomicTransitionDeployedByJob", jobName: "cadvisor", metaJSON: `{"jobs":["cadvisor","node-exporter"]}`, mockExecErr: errors.New("exec err"), wantErr: true},
		{name: "AtomicTransitionDeployedByJob remaining jobs update commit error", op: "AtomicTransitionDeployedByJob", jobName: "cadvisor", metaJSON: `{"jobs":["cadvisor","node-exporter"]}`, mockCommitErr: errors.New("commit err"), wantErr: true},
		{name: "AtomicTransitionDeployedByJob final job deployed RowsAffected 1", op: "AtomicTransitionDeployedByJob", jobName: "node-exporter", repo: "azylman/aerial", metaJSON: `{"jobs":["node-exporter"],"started_jobs":["node-exporter"]}`, targetIDReturn: "1555405874565091380", wantPRNum: 42, mergeSHAReturn: "merge123", rowsAffected: 1, wantUpdated: true, wantTarget: "1555405874565091380", wantSQLSubstr: "metadata->'jobs' ? $1 AND metadata->'started_jobs' ? $1"},
		{name: "AtomicTransitionDeployedByJob final job deployed RowsAffected 0", op: "AtomicTransitionDeployedByJob", jobName: "node-exporter", metaJSON: `{"jobs":["node-exporter"]}`, rowsAffected: 0, wantUpdated: false},
		{name: "AtomicTransitionDeployedByJob final job exec error", op: "AtomicTransitionDeployedByJob", jobName: "node-exporter", metaJSON: `{"jobs":["node-exporter"]}`, mockExecErr: errors.New("exec err"), wantErr: true},
		{name: "AtomicTransitionDeployedByJob final job commit error", op: "AtomicTransitionDeployedByJob", jobName: "node-exporter", metaJSON: `{"jobs":["node-exporter"]}`, rowsAffected: 1, mockCommitErr: errors.New("commit err"), wantErr: true},

		// AtomicTransitionDeployFailedByJob
		{name: "AtomicTransitionDeployFailedByJob empty job guard", op: "AtomicTransitionDeployFailedByJob", jobName: "", wantUpdated: false},
		{name: "AtomicTransitionDeployFailedByJob select ErrNoRows", op: "AtomicTransitionDeployFailedByJob", jobName: "brain", mockSelectErr: pgx.ErrNoRows, wantUpdated: false, wantSQLSubstr: "deployed_jobs"},
		{name: "AtomicTransitionDeployFailedByJob select error", op: "AtomicTransitionDeployFailedByJob", jobName: "brain", mockSelectErr: errors.New("select err"), wantErr: true},
		{name: "AtomicTransitionDeployFailedByJob exec error", op: "AtomicTransitionDeployFailedByJob", jobName: "brain", mockExecErr: errors.New("exec err"), wantErr: true},
		{name: "AtomicTransitionDeployFailedByJob RowsAffected 0", op: "AtomicTransitionDeployFailedByJob", jobName: "brain", repo: "azylman/aerial", targetIDReturn: "1555405874565091380", wantPRNum: 42, mergeSHAReturn: "merge123", rowsAffected: 0, wantUpdated: false, wantTarget: "1555405874565091380"},
		{name: "AtomicTransitionDeployFailedByJob success", op: "AtomicTransitionDeployFailedByJob", jobName: "brain", repo: "azylman/aerial", targetIDReturn: "1555405874565091380", wantPRNum: 42, mergeSHAReturn: "merge123", rowsAffected: 1, wantUpdated: true, wantTarget: "1555405874565091380"},

		// EnsurePRRegistered
		{name: "EnsurePRRegistered empty repo guard", op: "EnsurePRRegistered", repo: "", prNumber: 692, wantTarget: ""},
		{name: "EnsurePRRegistered chunk query error", op: "EnsurePRRegistered", repo: "azylman/mirrormere", prNumber: 692, jobName: "chunk-2", vanadisStatus: http.StatusInternalServerError, vanadisBody: `{"error":"internal"}`, wantErr: true},
		{name: "EnsurePRRegistered chunk resolves thread_id and upserts pr_registry", op: "EnsurePRRegistered", repo: "azylman/mirrormere", prNumber: 692, jobName: "chunk-2", vanadisStatus: http.StatusOK, vanadisBody: `{"status":"attached","thread_id":"1562162630997581946"}`, targetIDReturn: "1562162630997581946", wantTarget: "1562162630997581946"},
		{name: "EnsurePRRegistered invalid targetID after chunk miss skips upsert", op: "EnsurePRRegistered", repo: "azylman/mirrormere", prNumber: 692, jobName: "chunk-2", vanadisStatus: http.StatusNotFound, vanadisBody: `{"error":"not found"}`, wantTarget: ""},
		{name: "EnsurePRRegistered upsert exec error", op: "EnsurePRRegistered", repo: "azylman/mirrormere", prNumber: 692, targetID: "1562162630997581946", mockExecErr: errors.New("upsert err"), wantErr: true},

		// RegisterPR
		{name: "RegisterPR nil pool guard", op: "RegisterPR", nilPool: true, repo: "azylman/aerial", prNumber: 42, targetID: "1555405874565091380", wantErr: true},
		{name: "RegisterPR empty repo guard", op: "RegisterPR", repo: "", prNumber: 42, targetID: "1555405874565091380", wantErr: true},
		{name: "RegisterPR zero pr guard", op: "RegisterPR", repo: "azylman/aerial", prNumber: 0, targetID: "1555405874565091380", wantErr: true},
		{name: "RegisterPR empty branch guard", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, sha: "head123", targetID: "1555405874565091380", branchReturn: "", wantErr: true},
		{name: "RegisterPR empty head_sha guard", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, sha: "", targetID: "1555405874565091380", branchReturn: "feat/foo", wantErr: true},
		{name: "RegisterPR chunk query error", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, branchReturn: "feat/foo", sha: "head123", jobName: "chunk-1", vanadisStatus: http.StatusInternalServerError, vanadisBody: `{"error":"internal"}`, wantErr: true},
		{name: "RegisterPR chunk not found returns ErrVanadisChunkNotFound", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, branchReturn: "feat/foo", sha: "head123", jobName: "chunk-1", vanadisStatus: http.StatusNotFound, vanadisBody: `{"error":"not found"}`, wantErr: true},
		{name: "RegisterPR chunk resolves thread_id and upserts pr_registry", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, branchReturn: "feat/foo", sha: "head123", jobName: "chunk-1", vanadisStatus: http.StatusOK, vanadisBody: `{"status":"attached","thread_id":"1555405874565091380"}`, targetIDReturn: "1555405874565091380", wantTarget: "1555405874565091380", wantSQLSubstr: "ON CONFLICT (repo, pr_number)"},
		{name: "RegisterPR invalid targetID without chunk returns error", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, branchReturn: "feat/foo", sha: "head123", targetID: "invalid-target", wantErr: true},
		{name: "RegisterPR empty targetID without chunk returns error", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, branchReturn: "feat/foo", sha: "head123", targetID: "", wantErr: true},
		{name: "RegisterPR upsert exec error", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, branchReturn: "feat/foo", sha: "head123", targetID: "1555405874565091380", mockExecErr: errors.New("upsert err"), wantErr: true},
		{name: "RegisterPR success direct targetID", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, branchReturn: "feat/foo", sha: "head123", targetID: "1555405874565091380", wantTarget: "1555405874565091380", wantSQLSubstr: "INSERT INTO pr_registry"},
		{name: "RegisterPR success discord prefixed targetID", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, branchReturn: "feat/foo", sha: "head123", targetID: "discord-1555405874565091380", wantTarget: "1555405874565091380", wantSQLSubstr: "INSERT INTO pr_registry"},
		{name: "RegisterPR chunk overrides dispatch session key targetID", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, branchReturn: "feat/foo", sha: "head123", targetID: "dispatch-8d23879c", jobName: "chunk-1", vanadisStatus: http.StatusOK, vanadisBody: `{"status":"attached","thread_id":"discord-1555405874565091380"}`, targetIDReturn: "1555405874565091380", wantTarget: "1555405874565091380", wantSQLSubstr: "INSERT INTO pr_registry"},

		// RecordExpectedJobs
		{name: "RecordExpectedJobs empty repo guard", op: "RecordExpectedJobs", repo: "", prNumber: 42, jobs: []string{"coven"}, wantUpdated: false},
		{name: "RecordExpectedJobs zero pr guard", op: "RecordExpectedJobs", repo: "azylman/aerial", prNumber: 0, jobs: []string{"coven"}, wantUpdated: false},
		{name: "RecordExpectedJobs nil pool guard", op: "RecordExpectedJobs", nilPool: true, repo: "azylman/aerial", prNumber: 42, jobs: []string{"coven"}, wantUpdated: false},
		{name: "RecordExpectedJobs empty jobs delegates to HasExpectedDeployJobs", op: "RecordExpectedJobs", repo: "azylman/aerial", prNumber: 42, jobs: nil, wantUpdated: true, wantSQLSubstr: "FROM pr_registry"},
		{name: "RecordExpectedJobs non-empty jobs updates metadata", op: "RecordExpectedJobs", repo: "azylman/aerial", prNumber: 42, jobs: []string{"coven", "db-migrate"}, wantUpdated: true, wantSQLSubstr: "UPDATE pr_registry"},
		{name: "RecordExpectedJobs not found returns false", op: "RecordExpectedJobs", repo: "azylman/aerial", prNumber: 42, jobs: []string{"coven"}, mockSelectErr: pgx.ErrNoRows, wantUpdated: false},
		{name: "RecordExpectedJobs db error", op: "RecordExpectedJobs", repo: "azylman/aerial", prNumber: 42, jobs: []string{"coven"}, mockSelectErr: errors.New("db error"), wantErr: true},

		// HasExpectedDeployJobs
		{name: "HasExpectedDeployJobs empty repo guard", op: "HasExpectedDeployJobs", repo: "", prNumber: 42, wantUpdated: false},
		{name: "HasExpectedDeployJobs zero pr guard", op: "HasExpectedDeployJobs", repo: "azylman/aerial", prNumber: 0, wantUpdated: false},
		{name: "HasExpectedDeployJobs nil pool guard", op: "HasExpectedDeployJobs", nilPool: true, repo: "azylman/aerial", prNumber: 42, wantUpdated: false},
		{name: "HasExpectedDeployJobs true when jobs exist", op: "HasExpectedDeployJobs", repo: "azylman/aerial", prNumber: 42, wantUpdated: true, wantSQLSubstr: "deployed_jobs"},
		{name: "HasExpectedDeployJobs not found returns false", op: "HasExpectedDeployJobs", repo: "azylman/aerial", prNumber: 42, mockSelectErr: pgx.ErrNoRows, wantUpdated: false},
		{name: "HasExpectedDeployJobs db error", op: "HasExpectedDeployJobs", repo: "azylman/aerial", prNumber: 42, mockSelectErr: errors.New("db error"), wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var gotSQL, gotExecSQL string
			var pool *mockVanadisPgxPool
			if !tc.nilPool {
				pool = &mockVanadisPgxPool{
					queryRowFn: func(qCtx context.Context, sql string, args ...any) pgx.Row {
						gotSQL = sql
						return mockVanadisPgxRow{scanFn: func(dest ...any) error {
							if tc.mockSelectErr != nil {
								return tc.mockSelectErr
							}
							if tc.mockScanErr != nil {
								return tc.mockScanErr
							}
							for _, d := range dest {
								switch ptr := d.(type) {
								case *string:
									*ptr = tc.targetIDReturn
								case *int:
									*ptr = tc.wantPRNum
								case *int64:
									*ptr = 100
								case *bool:
									*ptr = tc.wantUpdated
								case *[]byte:
									if tc.metaJSON != "" {
										*ptr = []byte(tc.metaJSON)
									}
								}
							}
							if len(dest) >= 2 {
								if sPtr, ok := dest[0].(*string); ok && tc.targetIDReturn != "" {
									*sPtr = tc.targetIDReturn
								}
								if iPtr, ok := dest[1].(*int); ok && tc.wantPRNum != 0 {
									*iPtr = tc.wantPRNum
								}
								if bPtr, ok := dest[1].(*string); ok && tc.branchReturn != "" {
									*bPtr = tc.branchReturn
								}
							}
							if len(dest) >= 3 {
								if tPtr, ok := dest[2].(*string); ok && tc.targetIDReturn != "" {
									*tPtr = tc.targetIDReturn
								}
							}
							if len(dest) >= 4 {
								if mPtr, ok := dest[3].(*string); ok && tc.mergeSHAReturn != "" {
									*mPtr = tc.mergeSHAReturn
								}
							}
							if len(dest) == 5 {
								*dest[0].(*int64) = 100
								*dest[1].(*string) = tc.targetIDReturn
								*dest[2].(*int) = tc.wantPRNum
								*dest[3].(*string) = tc.mergeSHAReturn
								*dest[4].(*string) = tc.repo
							}
							return nil
						}}
					},
					execFn: func(qCtx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
						gotExecSQL = sql
						if tc.mockExecErr != nil {
							return pgconn.CommandTag{}, tc.mockExecErr
						}
						rowsAff := tc.rowsAffected
						if rowsAff == 0 && tc.wantUpdated {
							rowsAff = 1
						}
						return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", rowsAff)), nil
					},
					beginFn: func(qCtx context.Context) (pgx.Tx, error) {
						if tc.mockBeginErr != nil {
							return nil, tc.mockBeginErr
						}
						return &mockVanadisPgxTx{
							commitErr: tc.mockCommitErr,
							execFn: func(txCtx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
								gotExecSQL = sql
								if tc.mockExecErr != nil {
									return pgconn.CommandTag{}, tc.mockExecErr
								}
								rowsAff := tc.rowsAffected
								if rowsAff == 0 && tc.wantUpdated {
									rowsAff = 1
								}
								return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", rowsAff)), nil
							},
							queryRowFn: func(txCtx context.Context, sql string, args ...any) pgx.Row {
								gotSQL = sql
								return mockVanadisPgxRow{scanFn: func(dest ...any) error {
									if tc.mockSelectErr != nil {
										return tc.mockSelectErr
									}
									if len(dest) == 6 {
										*dest[0].(*int64) = 101
										*dest[1].(*string) = tc.targetIDReturn
										*dest[2].(*int) = tc.wantPRNum
										*dest[3].(*string) = tc.mergeSHAReturn
										*dest[4].(*string) = tc.repo
										if tc.metaJSON != "" {
											*dest[5].(*[]byte) = []byte(tc.metaJSON)
										} else {
											*dest[5].(*[]byte) = []byte("{}")
										}
										return nil
									}
									return nil
								}}
							},
						}, nil
					},
					queryFn: func(qCtx context.Context, sql string, args ...any) (pgx.Rows, error) {
						gotSQL = sql
						if tc.mockSelectErr != nil {
							return nil, tc.mockSelectErr
						}
						rows := &mockVanadisPgxRows{
							scanErr:   tc.mockScanErr,
							rowsErr:   tc.mockRowsErr,
							totalRows: tc.totalRows,
						}
						rows.scanRowFn = func(idx int, dest ...any) error {
							if tc.mockScanErr != nil {
								return tc.mockScanErr
							}
							if len(dest) == 4 {
								*dest[0].(*int) = 100 + idx
								*dest[1].(*string) = "feat/open"
								*dest[2].(*string) = "1555405874565091380"
								*dest[3].(*string) = "headsha"
								return nil
							}
							if len(dest) == 8 {
								*dest[0].(*int64) = int64(200 + idx)
								*dest[1].(*string) = "azylman/aerial"
								*dest[2].(*int) = 200 + idx
								*dest[3].(*string) = "main"
								*dest[4].(*string) = "headsha"
								*dest[5].(*string) = "mergesha"
								*dest[6].(*string) = "1555405874565091380"
								if tc.metaJSON != "" {
									*dest[7].(*[]byte) = []byte(tc.metaJSON)
								}
								return nil
							}
							if len(dest) == 3 {
								*dest[0].(*string) = "arcane103"
								*dest[1].(*string) = "wrap it up"
								*dest[2].(*string) = "done"
								return nil
							}
							return nil
						}
						return rows, nil
					},
				}
			}

			var vURL string
			var vClient *http.Client
			if tc.vanadisStatus != 0 || tc.vanadisBody != "" {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					status := tc.vanadisStatus
					if status == 0 {
						status = http.StatusOK
					}
					w.WriteHeader(status)
					_, _ = w.Write([]byte(tc.vanadisBody))
				}))
				defer ts.Close()
				vURL = ts.URL
				vClient = ts.Client()
			}

			reg := NewPostgresPRRegistry(pool, WithVanadisClient(vURL, vClient))
			if tc.nilPool {
				reg = NewPostgresPRRegistry((*pgxpool.Pool)(nil), WithVanadisClient(vURL, vClient))
			}

			switch tc.op {
			case "UpdatePRMerged":
				gotTarget, err := reg.UpdatePRMerged(ctx, tc.repo, tc.prNumber, tc.sha)
				if (err != nil) != tc.wantErr {
					t.Fatalf("UpdatePRMerged err = %v, wantErr = %v", err, tc.wantErr)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
			case "UpdatePRClosedUnmerged":
				gotTarget, err := reg.UpdatePRClosedUnmerged(ctx, tc.repo, tc.prNumber)
				if (err != nil) != tc.wantErr {
					t.Fatalf("UpdatePRClosedUnmerged err = %v, wantErr = %v", err, tc.wantErr)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
			case "UpdatePRSync":
				gotTarget, err := reg.UpdatePRSync(ctx, tc.repo, tc.prNumber, tc.sha)
				if (err != nil) != tc.wantErr {
					t.Fatalf("UpdatePRSync err = %v, wantErr = %v", err, tc.wantErr)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
			case "BackfillPushMergeSHA":
				gotTarget, err := reg.BackfillPushMergeSHA(ctx, tc.repo, tc.prNumber, tc.sha)
				if (err != nil) != tc.wantErr {
					t.Fatalf("BackfillPushMergeSHA err = %v, wantErr = %v", err, tc.wantErr)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
			case "IncrementCIFailCount":
				gotCount, err := reg.IncrementCIFailCount(ctx, tc.repo, tc.prNumber)
				if (err != nil) != tc.wantErr {
					t.Fatalf("IncrementCIFailCount err = %v, wantErr = %v", err, tc.wantErr)
				}
				if gotCount != tc.wantPRNum {
					t.Errorf("gotCount = %d, want %d", gotCount, tc.wantPRNum)
				}
			case "RecordPRCheckStatus":
				updated, err := reg.RecordPRCheckStatus(ctx, tc.repo, tc.prNumber, tc.sha, tc.jobName, tc.targetID)
				if (err != nil) != tc.wantErr {
					t.Fatalf("RecordPRCheckStatus err = %v, wantErr = %v", err, tc.wantErr)
				}
				if updated != tc.wantUpdated {
					t.Errorf("updated = %v, want %v", updated, tc.wantUpdated)
				}
			case "AtomicTransitionCIFailed":
				gotTarget, updated, err := reg.AtomicTransitionCIFailed(ctx, tc.repo, tc.prNumber)
				if (err != nil) != tc.wantErr {
					t.Fatalf("AtomicTransitionCIFailed err = %v, wantErr = %v", err, tc.wantErr)
				}
				if updated != tc.wantUpdated {
					t.Errorf("updated = %v, want %v", updated, tc.wantUpdated)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
			case "AtomicTransitionConflict":
				gotTarget, updated, err := reg.AtomicTransitionConflict(ctx, tc.repo, tc.prNumber)
				if (err != nil) != tc.wantErr {
					t.Fatalf("AtomicTransitionConflict err = %v, wantErr = %v", err, tc.wantErr)
				}
				if updated != tc.wantUpdated {
					t.Errorf("updated = %v, want %v", updated, tc.wantUpdated)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
			case "AtomicTransitionDeployFailed":
				gotTarget, updated, err := reg.AtomicTransitionDeployFailed(ctx, tc.repo, tc.prNumber)
				if (err != nil) != tc.wantErr {
					t.Fatalf("AtomicTransitionDeployFailed err = %v, wantErr = %v", err, tc.wantErr)
				}
				if updated != tc.wantUpdated {
					t.Errorf("updated = %v, want %v", updated, tc.wantUpdated)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
			case "AtomicTransitionCIFailedByHeadSHA":
				gotTarget, gotPR, updated, err := reg.AtomicTransitionCIFailedByHeadSHA(ctx, tc.repo, tc.sha)
				if (err != nil) != tc.wantErr {
					t.Fatalf("AtomicTransitionCIFailedByHeadSHA err = %v, wantErr = %v", err, tc.wantErr)
				}
				if updated != tc.wantUpdated {
					t.Errorf("updated = %v, want %v", updated, tc.wantUpdated)
				}
				if gotPR != tc.wantPRNum {
					t.Errorf("gotPR = %d, want %d", gotPR, tc.wantPRNum)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
			case "ResolvePRBySHA":
				gotPR, gotBranch, gotTarget, err := reg.ResolvePRBySHA(ctx, tc.repo, tc.sha)
				if (err != nil) != tc.wantErr {
					t.Fatalf("ResolvePRBySHA err = %v, wantErr = %v", err, tc.wantErr)
				}
				if gotPR != tc.wantPRNum {
					t.Errorf("gotPR = %d, want %d", gotPR, tc.wantPRNum)
				}
				if gotBranch != tc.branchReturn {
					t.Errorf("gotBranch = %q, want %q", gotBranch, tc.branchReturn)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
			case "ResolvePRByNumber":
				gotTarget, gotBranch, _, _, err := reg.ResolvePRByNumber(ctx, tc.repo, tc.prNumber)
				if (err != nil) != tc.wantErr {
					t.Fatalf("ResolvePRByNumber err = %v, wantErr = %v", err, tc.wantErr)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
				if gotBranch != tc.branchReturn {
					t.Errorf("gotBranch = %q, want %q", gotBranch, tc.branchReturn)
				}
			case "TransitionDeploying":
				gotTarget, err := reg.TransitionDeploying(ctx, tc.repo, tc.prNumber, tc.jobs)
				if (err != nil) != tc.wantErr {
					t.Fatalf("TransitionDeploying err = %v, wantErr = %v", err, tc.wantErr)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
			case "ListOpenPRs":
				prs, err := reg.ListOpenPRs(ctx, tc.repo)
				if (err != nil) != tc.wantErr {
					t.Fatalf("ListOpenPRs err = %v, wantErr = %v", err, tc.wantErr)
				}
				if len(prs) != tc.wantCount {
					t.Errorf("len(prs) = %d, want %d", len(prs), tc.wantCount)
				}
			case "ListDeployingPRs":
				prs, err := reg.ListDeployingPRs(ctx)
				if (err != nil) != tc.wantErr {
					t.Fatalf("ListDeployingPRs err = %v, wantErr = %v", err, tc.wantErr)
				}
				if len(prs) != tc.wantCount {
					t.Errorf("len(prs) = %d, want %d", len(prs), tc.wantCount)
				}
			case "AtomicTransitionDeployedByJob":
				gotTarget, gotPR, _, _, updated, err := reg.AtomicTransitionDeployedByJob(ctx, tc.jobName)
				if (err != nil) != tc.wantErr {
					t.Fatalf("AtomicTransitionDeployedByJob err = %v, wantErr = %v", err, tc.wantErr)
				}
				if updated != tc.wantUpdated {
					t.Errorf("updated = %v, want %v", updated, tc.wantUpdated)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
				if gotPR != tc.wantPRNum {
					t.Errorf("gotPR = %d, want %d", gotPR, tc.wantPRNum)
				}
			case "AtomicTransitionDeployFailedByJob":
				gotTarget, gotPR, _, _, updated, err := reg.AtomicTransitionDeployFailedByJob(ctx, tc.jobName)
				if (err != nil) != tc.wantErr {
					t.Fatalf("AtomicTransitionDeployFailedByJob err = %v, wantErr = %v", err, tc.wantErr)
				}
				if updated != tc.wantUpdated {
					t.Errorf("updated = %v, want %v", updated, tc.wantUpdated)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
				if gotPR != tc.wantPRNum {
					t.Errorf("gotPR = %d, want %d", gotPR, tc.wantPRNum)
				}
			case "RegisterPR":
				req := PRRegisterRequest{
					Repo:           tc.repo,
					PRNumber:       tc.prNumber,
					Branch:         tc.branchReturn,
					HeadSHA:        tc.sha,
					TargetID:       tc.targetID,
					Title:          "feat: title",
					VanadisChunkID: tc.jobName,
					Metadata:       tc.metaJSON,
				}
				gotTarget, err := reg.RegisterPR(ctx, req)
				if (err != nil) != tc.wantErr {
					t.Fatalf("RegisterPR err = %v, wantErr = %v", err, tc.wantErr)
				}
				if tc.name == "RegisterPR chunk not found returns ErrVanadisChunkNotFound" {
					if !errors.Is(err, ErrVanadisChunkNotFound) {
						t.Errorf("err = %v, want ErrVanadisChunkNotFound", err)
					}
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
			case "EnsurePRRegistered":
				gotTarget, err := reg.EnsurePRRegistered(ctx, tc.repo, tc.prNumber, "feat/branch", "head123", "feat: title", tc.targetID, tc.jobName)
				if (err != nil) != tc.wantErr {
					t.Fatalf("EnsurePRRegistered err = %v, wantErr = %v", err, tc.wantErr)
				}
				if gotTarget != tc.wantTarget {
					t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
				}
			case "RecordExpectedJobs":
				gotHas, err := reg.RecordExpectedJobs(ctx, tc.repo, tc.prNumber, tc.jobs)
				if (err != nil) != tc.wantErr {
					t.Fatalf("RecordExpectedJobs err = %v, wantErr = %v", err, tc.wantErr)
				}
				if gotHas != tc.wantUpdated {
					t.Errorf("gotHas = %v, want %v", gotHas, tc.wantUpdated)
				}
			case "HasExpectedDeployJobs":
				gotHas, err := reg.HasExpectedDeployJobs(ctx, tc.repo, tc.prNumber)
				if (err != nil) != tc.wantErr {
					t.Fatalf("HasExpectedDeployJobs err = %v, wantErr = %v", err, tc.wantErr)
				}
				if gotHas != tc.wantUpdated {
					t.Errorf("gotHas = %v, want %v", gotHas, tc.wantUpdated)
				}
			}
			if tc.wantSQLSubstr != "" && !strings.Contains(gotSQL, tc.wantSQLSubstr) && !strings.Contains(gotExecSQL, tc.wantSQLSubstr) {
				t.Errorf("gotSQL = %q, gotExecSQL = %q, want substring %q", gotSQL, gotExecSQL, tc.wantSQLSubstr)
			}
		})
	}
}

func TestPostgresPRRegistry_VanadisClientOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		reg        *PostgresPRRegistry
		url        string
		client     *http.Client
		wantURL    string
		wantClient bool
	}{
		{
			name:       "nil registry returns defaults",
			reg:        nil,
			wantURL:    "http://vanadis.aerial",
			wantClient: true,
		},
		{
			name:       "uninitialized fields return defaults",
			reg:        &PostgresPRRegistry{},
			wantURL:    "http://vanadis.aerial",
			wantClient: true,
		},
		{
			name:       "custom URL and client",
			reg:        NewPostgresPRRegistry(nil, WithVanadisClient("http://custom.vanadis:8080/", &http.Client{Timeout: 5 * time.Second})),
			wantURL:    "http://custom.vanadis:8080",
			wantClient: true,
		},
		{
			name:       "WithVanadisClient nil registry guard",
			reg:        nil,
			url:        "http://custom.vanadis",
			wantURL:    "http://vanadis.aerial",
			wantClient: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.url != "" {
				WithVanadisClient(tc.url, tc.client)(tc.reg)
			}
			c, u := tc.reg.getVanadisClient()
			if u != tc.wantURL {
				t.Errorf("got URL %q, want %q", u, tc.wantURL)
			}
			if (c != nil) != tc.wantClient {
				t.Errorf("got client %v, wantClient %v", c, tc.wantClient)
			}
		})
	}
}

func TestPostgresPRRegistry_VanadisMethods_Unit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name               string
		method             string
		useNilRegistry     bool
		emptyGuard         bool
		httpStatus         int
		httpBody           string
		wantErr            bool
		wantBool           bool
		wantEffort         string
		wantMatched        bool
		wantAdvanced       bool
		wantBuildCompleted bool
		wantNextChunkIdx   int
	}{
		// Typed nil registry / uninitialized
		{
			name:           "AdvanceVanadisChunkByPR typed nil registry returns empty",
			method:         "AdvanceVanadisChunkByPR",
			useNilRegistry: true,
		},
		{
			name:           "ShouldSuppressVanadisDeployWake typed nil registry returns false",
			method:         "ShouldSuppressVanadisDeployWake",
			useNilRegistry: true,
			wantBool:       false,
		},
		{
			name:           "IsFinalVanadisChunk typed nil registry returns false",
			method:         "IsFinalVanadisChunk",
			useNilRegistry: true,
			wantBool:       false,
		},
		{
			name:           "ResolveVanadisChunkForPR typed nil registry returns false",
			method:         "ResolveVanadisChunkForPR",
			useNilRegistry: true,
			wantBool:       false,
		},
		{
			name:           "FailVanadisChunkByPR typed nil registry returns false",
			method:         "FailVanadisChunkByPR",
			useNilRegistry: true,
			wantBool:       false,
		},

		// Empty repo / prNumber <= 0 guard
		{
			name:       "ShouldSuppressVanadisDeployWake empty guard",
			method:     "ShouldSuppressVanadisDeployWake",
			emptyGuard: true,
		},
		{
			name:       "IsFinalVanadisChunk empty guard",
			method:     "IsFinalVanadisChunk",
			emptyGuard: true,
		},
		{
			name:       "ResolveVanadisChunkForPR empty guard",
			method:     "ResolveVanadisChunkForPR",
			emptyGuard: true,
		},
		{
			name:       "FailVanadisChunkByPR empty guard",
			method:     "FailVanadisChunkByPR",
			emptyGuard: true,
		},
		{
			name:       "AdvanceVanadisChunkByPR empty guard",
			method:     "AdvanceVanadisChunkByPR",
			emptyGuard: true,
		},

		// ShouldSuppressVanadisDeployWake
		{
			name:       "ShouldSuppressVanadisDeployWake returns true on match with remaining chunks",
			method:     "ShouldSuppressVanadisDeployWake",
			httpStatus: http.StatusOK,
			httpBody:   `{"matched":true,"chunk_id":"c1","build_id":"b1","has_remaining_chunks":true}`,
			wantBool:   true,
		},
		{
			name:       "ShouldSuppressVanadisDeployWake returns true when no remaining chunks",
			method:     "ShouldSuppressVanadisDeployWake",
			httpStatus: http.StatusOK,
			httpBody:   `{"matched":true,"chunk_id":"c1","build_id":"b1","has_remaining_chunks":false}`,
			wantBool:   true,
		},
		{
			name:       "ShouldSuppressVanadisDeployWake returns false when not matched",
			method:     "ShouldSuppressVanadisDeployWake",
			httpStatus: http.StatusOK,
			httpBody:   `{"matched":false}`,
			wantBool:   false,
		},
		{
			name:       "ShouldSuppressVanadisDeployWake returns error on 500",
			method:     "ShouldSuppressVanadisDeployWake",
			httpStatus: http.StatusInternalServerError,
			httpBody:   `{"error":"fail"}`,
			wantErr:    true,
		},
		{
			name:       "ShouldSuppressVanadisDeployWake returns error on invalid json",
			method:     "ShouldSuppressVanadisDeployWake",
			httpStatus: http.StatusOK,
			httpBody:   `{invalid-json`,
			wantErr:    true,
		},

		// IsFinalVanadisChunk
		{
			name:       "IsFinalVanadisChunk returns false on match with remaining chunks",
			method:     "IsFinalVanadisChunk",
			httpStatus: http.StatusOK,
			httpBody:   `{"matched":true,"chunk_id":"c1","build_id":"b1","has_remaining_chunks":true}`,
			wantBool:   false,
		},
		{
			name:       "IsFinalVanadisChunk returns true when no remaining chunks",
			method:     "IsFinalVanadisChunk",
			httpStatus: http.StatusOK,
			httpBody:   `{"matched":true,"chunk_id":"c1","build_id":"b1","has_remaining_chunks":false}`,
			wantBool:   true,
		},
		{
			name:       "IsFinalVanadisChunk returns false when not matched",
			method:     "IsFinalVanadisChunk",
			httpStatus: http.StatusOK,
			httpBody:   `{"matched":false}`,
			wantBool:   false,
		},
		{
			name:       "IsFinalVanadisChunk returns error on 500",
			method:     "IsFinalVanadisChunk",
			httpStatus: http.StatusInternalServerError,
			httpBody:   `{"error":"fail"}`,
			wantErr:    true,
		},
		{
			name:       "IsFinalVanadisChunk returns error on invalid json",
			method:     "IsFinalVanadisChunk",
			httpStatus: http.StatusOK,
			httpBody:   `{invalid-json`,
			wantErr:    true,
		},

		// ResolveVanadisChunkForPR
		{
			name:       "ResolveVanadisChunkForPR returns true and low effort on match",
			method:     "ResolveVanadisChunkForPR",
			httpStatus: http.StatusOK,
			httpBody:   `{"matched":true,"chunk_id":"c1","build_id":"b1","effort":"low"}`,
			wantBool:   true,
			wantEffort: "low",
		},
		{
			name:       "ResolveVanadisChunkForPR defaults to medium effort when empty",
			method:     "ResolveVanadisChunkForPR",
			httpStatus: http.StatusOK,
			httpBody:   `{"matched":true,"chunk_id":"c1","build_id":"b1","effort":""}`,
			wantBool:   true,
			wantEffort: "medium",
		},
		{
			name:       "ResolveVanadisChunkForPR returns false when not matched",
			method:     "ResolveVanadisChunkForPR",
			httpStatus: http.StatusOK,
			httpBody:   `{"matched":false}`,
			wantBool:   false,
		},
		{
			name:       "ResolveVanadisChunkForPR returns error on 500",
			method:     "ResolveVanadisChunkForPR",
			httpStatus: http.StatusInternalServerError,
			httpBody:   `{"error":"fail"}`,
			wantErr:    true,
		},
		{
			name:       "ResolveVanadisChunkForPR returns error on invalid json",
			method:     "ResolveVanadisChunkForPR",
			httpStatus: http.StatusOK,
			httpBody:   `{invalid-json`,
			wantErr:    true,
		},

		// FailVanadisChunkByPR
		{
			name:       "FailVanadisChunkByPR returns false on failed: false",
			method:     "FailVanadisChunkByPR",
			httpStatus: http.StatusOK,
			httpBody:   `{"failed":false}`,
			wantBool:   false,
		},
		{
			name:       "FailVanadisChunkByPR returns true on success",
			method:     "FailVanadisChunkByPR",
			httpStatus: http.StatusOK,
			httpBody:   `{"build_id":"b1","chunk_id":"c1","failed":true}`,
			wantBool:   true,
		},
		{
			name:       "FailVanadisChunkByPR returns error on 500",
			method:     "FailVanadisChunkByPR",
			httpStatus: http.StatusInternalServerError,
			httpBody:   `{"error":"fail"}`,
			wantErr:    true,
		},

		// AdvanceVanadisChunkByPR
		{
			name:        "AdvanceVanadisChunkByPR returns Matched=false when not found",
			method:      "AdvanceVanadisChunkByPR",
			httpStatus:  http.StatusOK,
			httpBody:    `{"matched":false}`,
			wantMatched: false,
		},
		{
			name:         "AdvanceVanadisChunkByPR returns unadvanced on terminal/already merged chunk",
			method:       "AdvanceVanadisChunkByPR",
			httpStatus:   http.StatusOK,
			httpBody:     `{"matched":true,"advanced":false,"build_id":"b1","thread_id":"t1","build_status":"running"}`,
			wantMatched:  true,
			wantAdvanced: false,
		},
		{
			name:             "AdvanceVanadisChunkByPR mid-run advancement success",
			method:           "AdvanceVanadisChunkByPR",
			httpStatus:       http.StatusOK,
			httpBody:         `{"matched":true,"advanced":true,"build_id":"b1","thread_id":"t1","next_chunk_index":1,"build_completed":false}`,
			wantMatched:      true,
			wantAdvanced:     true,
			wantNextChunkIdx: 1,
		},
		{
			name:               "AdvanceVanadisChunkByPR build completed advancement success",
			method:             "AdvanceVanadisChunkByPR",
			httpStatus:         http.StatusOK,
			httpBody:           `{"matched":true,"advanced":true,"build_id":"b1","thread_id":"t1","next_chunk_index":-1,"build_completed":true}`,
			wantMatched:        true,
			wantAdvanced:       true,
			wantBuildCompleted: true,
			wantNextChunkIdx:   -1,
		},
		{
			name:       "AdvanceVanadisChunkByPR server 500 returns error",
			method:     "AdvanceVanadisChunkByPR",
			httpStatus: http.StatusInternalServerError,
			httpBody:   `{"error":"fail"}`,
			wantErr:    true,
		},
		{
			name:       "AdvanceVanadisChunkByPR invalid json returns error",
			method:     "AdvanceVanadisChunkByPR",
			httpStatus: http.StatusOK,
			httpBody:   `{invalid-json`,
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var vURL string
			var vClient *http.Client
			if tc.httpStatus != 0 || tc.httpBody != "" {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					status := tc.httpStatus
					if status == 0 {
						status = http.StatusOK
					}
					w.WriteHeader(status)
					_, _ = w.Write([]byte(tc.httpBody))
				}))
				defer ts.Close()
				vURL = ts.URL
				vClient = ts.Client()
			}

			reg := NewPostgresPRRegistry(nil, WithVanadisClient(vURL, vClient))
			if tc.useNilRegistry {
				reg = nil
			}
			repo := "aerial"
			prNum := 10
			if tc.emptyGuard {
				repo = ""
				prNum = 0
			}
			switch tc.method {
			case "ShouldSuppressVanadisDeployWake":
				sup, err := reg.ShouldSuppressVanadisDeployWake(ctx, repo, prNum)
				if (err != nil) != tc.wantErr {
					t.Fatalf("ShouldSuppressVanadisDeployWake() err = %v, wantErr = %v", err, tc.wantErr)
				}
				if !tc.wantErr && sup != tc.wantBool {
					t.Errorf("got %v, want %v", sup, tc.wantBool)
				}
			case "IsFinalVanadisChunk":
				finalChunk, err := reg.IsFinalVanadisChunk(ctx, repo, prNum)
				if (err != nil) != tc.wantErr {
					t.Fatalf("IsFinalVanadisChunk() err = %v, wantErr = %v", err, tc.wantErr)
				}
				if !tc.wantErr && finalChunk != tc.wantBool {
					t.Errorf("got %v, want %v", finalChunk, tc.wantBool)
				}
			case "ResolveVanadisChunkForPR":
				matched, _, effort, err := reg.ResolveVanadisChunkForPR(ctx, repo, prNum)
				if (err != nil) != tc.wantErr {
					t.Fatalf("ResolveVanadisChunkForPR() err = %v, wantErr = %v", err, tc.wantErr)
				}
				if !tc.wantErr {
					if matched != tc.wantBool {
						t.Errorf("got matched %v, want %v", matched, tc.wantBool)
					}
					if tc.wantEffort != "" && effort != tc.wantEffort {
						t.Errorf("got effort %q, want %q", effort, tc.wantEffort)
					}
				}
			case "FailVanadisChunkByPR":
				failed, err := reg.FailVanadisChunkByPR(ctx, repo, prNum)
				if (err != nil) != tc.wantErr {
					t.Fatalf("FailVanadisChunkByPR() err = %v, wantErr = %v", err, tc.wantErr)
				}
				if !tc.wantErr && failed != tc.wantBool {
					t.Errorf("got %v, want %v", failed, tc.wantBool)
				}
			case "AdvanceVanadisChunkByPR":
				res, err := reg.AdvanceVanadisChunkByPR(ctx, repo, prNum)
				if (err != nil) != tc.wantErr {
					t.Fatalf("AdvanceVanadisChunkByPR() err = %v, wantErr = %v", err, tc.wantErr)
				}
				if !tc.wantErr {
					if res.Matched != tc.wantMatched {
						t.Errorf("Matched = %v, want %v", res.Matched, tc.wantMatched)
					}
					if res.Advanced != tc.wantAdvanced {
						t.Errorf("Advanced = %v, want %v", res.Advanced, tc.wantAdvanced)
					}
					if tc.wantAdvanced {
						if res.NextChunkIndex != tc.wantNextChunkIdx {
							t.Errorf("NextChunkIndex = %d, want %d", res.NextChunkIndex, tc.wantNextChunkIdx)
						}
						if res.BuildCompleted != tc.wantBuildCompleted {
							t.Errorf("BuildCompleted = %v, want %v", res.BuildCompleted, tc.wantBuildCompleted)
						}
					}
				}
			}
		})
	}
}
