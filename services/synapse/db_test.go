package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type mockSynapsePgxRow struct {
	scanFn func(dest ...any) error
}

func (r mockSynapsePgxRow) Scan(dest ...any) error {
	if r.scanFn != nil {
		return r.scanFn(dest...)
	}
	return nil
}

type mockSynapsePgxRows struct {
	pgx.Rows

	intItems  []int
	idx       int
	scanErr   error
	rowsErr   error
	totalRows int
	scanRowFn func(idx int, dest ...any) error
}

func (r *mockSynapsePgxRows) Close()     {}
func (r *mockSynapsePgxRows) Err() error { return r.rowsErr }
func (r *mockSynapsePgxRows) Next() bool {
	limit := max(r.totalRows, len(r.intItems))
	if r.idx < limit {
		r.idx++
		return true
	}
	return false
}

func (r *mockSynapsePgxRows) Scan(dest ...any) error {
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
	return nil
}

type mockSynapsePgxTx struct {
	pgx.Tx

	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
	execFn     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	queryFn    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	commitErr  error
	rbErr      error
}

func (t *mockSynapsePgxTx) Commit(ctx context.Context) error { return t.commitErr }
func (t *mockSynapsePgxTx) Rollback(ctx context.Context) error {
	if t.rbErr != nil {
		return t.rbErr
	}
	return pgx.ErrTxClosed
}

func (t *mockSynapsePgxTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if t.queryRowFn != nil {
		return t.queryRowFn(ctx, sql, args...)
	}
	return mockSynapsePgxRow{}
}

func (t *mockSynapsePgxTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if t.execFn != nil {
		return t.execFn(ctx, sql, args...)
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (t *mockSynapsePgxTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if t.queryFn != nil {
		return t.queryFn(ctx, sql, args...)
	}
	return &mockSynapsePgxRows{}, nil
}

type mockSynapsePgxPool struct {
	beginFn    func(ctx context.Context) (pgx.Tx, error)
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
	execFn     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	queryFn    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func (p *mockSynapsePgxPool) Begin(ctx context.Context) (pgx.Tx, error) {
	if p.beginFn != nil {
		return p.beginFn(ctx)
	}
	return &mockSynapsePgxTx{}, nil
}

func (p *mockSynapsePgxPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if p.queryRowFn != nil {
		return p.queryRowFn(ctx, sql, args...)
	}
	return mockSynapsePgxRow{}
}

func (p *mockSynapsePgxPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if p.execFn != nil {
		return p.execFn(ctx, sql, args...)
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (p *mockSynapsePgxPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if p.queryFn != nil {
		return p.queryFn(ctx, sql, args...)
	}
	return &mockSynapsePgxRows{}, nil
}

type postgresPRRegistryUnitCase struct {
	name           string
	op             string
	nilPool        bool
	repo           string
	prNumber       int
	sha            string
	jobName        string
	jobs           []string
	kustRequests   map[string]kustomizationRequest
	targetID       string
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
	wantErr        bool
	wantUpdated    bool
	wantTarget     string
	wantPRNum      int
	wantCount      int
	wantSQLSubstr  string
}

func postgresPRRegistryTransitionCases() []postgresPRRegistryUnitCase {
	return []postgresPRRegistryUnitCase{
		// Single-row transitions: UpdatePRMerged
		{name: "UpdatePRMerged success", op: "UpdatePRMerged", repo: "azylman/aerial", prNumber: 42, sha: "merge123", targetIDReturn: "100000000000000001", wantTarget: "100000000000000001", wantSQLSubstr: "CASE WHEN status IN ('synced', 'deploying', 'deployed', 'deploy_failed') THEN status ELSE 'merged' END"},
		{name: "UpdatePRMerged not found", op: "UpdatePRMerged", repo: "azylman/aerial", prNumber: 42, sha: "merge123", mockSelectErr: pgx.ErrNoRows, wantTarget: ""},
		{name: "UpdatePRMerged query error", op: "UpdatePRMerged", repo: "azylman/aerial", prNumber: 42, sha: "merge123", mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "UpdatePRMerged nil pool guard", op: "UpdatePRMerged", nilPool: true, repo: "azylman/aerial", prNumber: 42, sha: "merge123"},
		{name: "UpdatePRMerged empty repo guard", op: "UpdatePRMerged", repo: "", prNumber: 42},
		{name: "UpdatePRMerged prNumber zero guard", op: "UpdatePRMerged", repo: "azylman/aerial", prNumber: 0},

		// UpdatePRClosedUnmerged
		{name: "UpdatePRClosedUnmerged success", op: "UpdatePRClosedUnmerged", repo: "azylman/aerial", prNumber: 42, targetIDReturn: "100000000000000001", wantTarget: "100000000000000001"},
		{name: "UpdatePRClosedUnmerged not found", op: "UpdatePRClosedUnmerged", repo: "azylman/aerial", prNumber: 42, mockSelectErr: pgx.ErrNoRows, wantTarget: ""},
		{name: "UpdatePRClosedUnmerged db error", op: "UpdatePRClosedUnmerged", repo: "azylman/aerial", prNumber: 42, mockSelectErr: errors.New("db error"), wantErr: true},

		// UpdatePRSync
		{name: "UpdatePRSync success", op: "UpdatePRSync", repo: "azylman/aerial", prNumber: 42, sha: "head123", targetIDReturn: "100000000000000001", wantTarget: "100000000000000001", wantSQLSubstr: "COALESCE((metadata->>'ci_fail_count')::int, 0) > 3 THEN ((COALESCE(metadata, '{}'::jsonb) - 'banner') - 'ci_checks') - 'ci_fail_count'"},
		{name: "UpdatePRSync not found", op: "UpdatePRSync", repo: "azylman/aerial", prNumber: 42, sha: "head123", mockSelectErr: pgx.ErrNoRows, wantTarget: ""},
		{name: "UpdatePRSync db error", op: "UpdatePRSync", repo: "azylman/aerial", prNumber: 42, sha: "head123", mockSelectErr: errors.New("db error"), wantErr: true},

		// BackfillPushMergeSHA
		{name: "BackfillPushMergeSHA success", op: "BackfillPushMergeSHA", repo: "azylman/aerial", prNumber: 42, sha: "merge123", targetIDReturn: "100000000000000001", wantTarget: "100000000000000001", wantSQLSubstr: "CASE WHEN status IN ('synced', 'deploying', 'deployed', 'deploy_failed') THEN status ELSE 'merged' END"},
		{name: "BackfillPushMergeSHA not found", op: "BackfillPushMergeSHA", repo: "azylman/aerial", prNumber: 42, sha: "merge123", mockSelectErr: pgx.ErrNoRows, wantTarget: ""},
		{name: "BackfillPushMergeSHA db error", op: "BackfillPushMergeSHA", repo: "azylman/aerial", prNumber: 42, sha: "merge123", mockSelectErr: errors.New("db error"), wantErr: true},

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
		{name: "AtomicTransitionCIFailed success", op: "AtomicTransitionCIFailed", repo: "azylman/aerial", prNumber: 42, targetIDReturn: "100000000000000001", wantTarget: "100000000000000001", wantUpdated: true, wantSQLSubstr: "COALESCE(merge_sha, '') = '' AND status NOT IN ('ci_failed', 'merged', 'synced', 'closed', 'deploying', 'deployed', 'deploy_failed')"},
		{name: "AtomicTransitionCIFailed not found", op: "AtomicTransitionCIFailed", repo: "azylman/aerial", prNumber: 42, mockSelectErr: pgx.ErrNoRows, wantTarget: "", wantUpdated: false},
		{name: "AtomicTransitionCIFailed db error", op: "AtomicTransitionCIFailed", repo: "azylman/aerial", prNumber: 42, mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "AtomicTransitionCIFailed empty repo guard", op: "AtomicTransitionCIFailed", repo: "", prNumber: 42},
		{name: "AtomicTransitionCIFailed prNumber zero guard", op: "AtomicTransitionCIFailed", repo: "azylman/aerial", prNumber: 0},

		// AtomicTransitionConflict
		{name: "AtomicTransitionConflict success", op: "AtomicTransitionConflict", repo: "azylman/aerial", prNumber: 42, targetIDReturn: "100000000000000001", wantTarget: "100000000000000001", wantUpdated: true, wantSQLSubstr: "COALESCE(merge_sha, '') = '' AND status NOT IN ('conflict', 'merged', 'synced', 'closed', 'deploying', 'deployed', 'deploy_failed')"},
		{name: "AtomicTransitionConflict not found", op: "AtomicTransitionConflict", repo: "azylman/aerial", prNumber: 42, mockSelectErr: pgx.ErrNoRows, wantTarget: "", wantUpdated: false},
		{name: "AtomicTransitionConflict db error", op: "AtomicTransitionConflict", repo: "azylman/aerial", prNumber: 42, mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "AtomicTransitionConflict empty repo guard", op: "AtomicTransitionConflict", repo: "", prNumber: 42},
		{name: "AtomicTransitionConflict prNumber zero guard", op: "AtomicTransitionConflict", repo: "azylman/aerial", prNumber: 0},

		// AtomicTransitionDeployFailed
		{name: "AtomicTransitionDeployFailed success", op: "AtomicTransitionDeployFailed", repo: "azylman/aerial", prNumber: 42, targetIDReturn: "100000000000000001", wantTarget: "100000000000000001", wantUpdated: true},
		{name: "AtomicTransitionDeployFailed not found", op: "AtomicTransitionDeployFailed", repo: "azylman/aerial", prNumber: 42, mockSelectErr: pgx.ErrNoRows, wantTarget: "", wantUpdated: false},
		{name: "AtomicTransitionDeployFailed db error", op: "AtomicTransitionDeployFailed", repo: "azylman/aerial", prNumber: 42, mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "AtomicTransitionDeployFailed empty repo guard", op: "AtomicTransitionDeployFailed", repo: "", prNumber: 42},
		{name: "AtomicTransitionDeployFailed prNumber zero guard", op: "AtomicTransitionDeployFailed", repo: "azylman/aerial", prNumber: 0},

		// AtomicTransitionCIFailedByHeadSHA
		{name: "AtomicTransitionCIFailedByHeadSHA success", op: "AtomicTransitionCIFailedByHeadSHA", repo: "azylman/aerial", sha: "sha123", targetIDReturn: "100000000000000001", wantTarget: "100000000000000001", wantPRNum: 42, wantUpdated: true, wantSQLSubstr: "COALESCE(merge_sha, '') = '' AND status NOT IN ('ci_failed', 'merged', 'synced', 'closed', 'deploying', 'deployed', 'deploy_failed')"},
		{name: "AtomicTransitionCIFailedByHeadSHA not found", op: "AtomicTransitionCIFailedByHeadSHA", repo: "azylman/aerial", sha: "sha123", mockSelectErr: pgx.ErrNoRows, wantUpdated: false},
		{name: "AtomicTransitionCIFailedByHeadSHA db error", op: "AtomicTransitionCIFailedByHeadSHA", repo: "azylman/aerial", sha: "sha123", mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "AtomicTransitionCIFailedByHeadSHA empty sha guard", op: "AtomicTransitionCIFailedByHeadSHA", repo: "azylman/aerial", sha: ""},
		{name: "AtomicTransitionCIFailedByHeadSHA zero sha guard", op: "AtomicTransitionCIFailedByHeadSHA", repo: "azylman/aerial", sha: "0000000000000000000000000000000000000000"},

		// ResolvePRBySHA
		{name: "ResolvePRBySHA success", op: "ResolvePRBySHA", repo: "azylman/aerial", sha: "sha123", wantPRNum: 42, branchReturn: "feat/sha", targetIDReturn: "100000000000000001", wantTarget: "100000000000000001"},
		{name: "ResolvePRBySHA not found", op: "ResolvePRBySHA", repo: "azylman/aerial", sha: "sha123", mockSelectErr: pgx.ErrNoRows, wantPRNum: 0},
		{name: "ResolvePRBySHA db error", op: "ResolvePRBySHA", repo: "azylman/aerial", sha: "sha123", mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "ResolvePRBySHA zero sha guard", op: "ResolvePRBySHA", repo: "azylman/aerial", sha: "0000000000000000000000000000000000000000", wantPRNum: 0},

		// ResolvePRByNumber
		{name: "ResolvePRByNumber success", op: "ResolvePRByNumber", repo: "azylman/aerial", prNumber: 42, targetIDReturn: "100000000000000001", branchReturn: "feat/by-num", mergeSHAReturn: "merge123", wantTarget: "100000000000000001"},
		{name: "ResolvePRByNumber not found", op: "ResolvePRByNumber", repo: "azylman/aerial", prNumber: 42, mockSelectErr: pgx.ErrNoRows, wantTarget: ""},
		{name: "ResolvePRByNumber db error", op: "ResolvePRByNumber", repo: "azylman/aerial", prNumber: 42, mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "ResolvePRByNumber zero pr guard", op: "ResolvePRByNumber", repo: "azylman/aerial", prNumber: 0, wantTarget: ""},

		// TransitionDeploying
		{name: "TransitionDeploying empty jobs guard", op: "TransitionDeploying", repo: "azylman/aerial", prNumber: 42, jobs: nil, wantTarget: ""},
		{name: "TransitionDeploying empty repo guard", op: "TransitionDeploying", repo: "", prNumber: 42, jobs: []string{"brain"}},
		{name: "TransitionDeploying zero prNumber guard", op: "TransitionDeploying", repo: "azylman/aerial", prNumber: 0, jobs: []string{"brain"}},
		{name: "TransitionDeploying success", op: "TransitionDeploying", repo: "azylman/aerial", prNumber: 42, jobs: []string{"brain", "synapse"}, targetIDReturn: "100000000000000001", wantTarget: "100000000000000001"},
		{name: "TransitionDeploying db error", op: "TransitionDeploying", repo: "azylman/aerial", prNumber: 42, jobs: []string{"brain"}, mockSelectErr: errors.New("db error"), wantErr: true},

		// ListDeployingPRs
		{name: "ListDeployingPRs scan error", op: "ListDeployingPRs", totalRows: 1, mockScanErr: errors.New("scan err"), wantErr: true},
		{name: "ListDeployingPRs success", op: "ListDeployingPRs", totalRows: 2, metaJSON: `{"jobs":["brain"]}`, wantCount: 2},
		{name: "ListDeployingPRs invalid metadata JSON warning still succeeds", op: "ListDeployingPRs", totalRows: 1, metaJSON: `{invalid json`, wantCount: 1},
		{name: "ListDeployingPRs query error", op: "ListDeployingPRs", mockSelectErr: errors.New("query err"), wantErr: true},
	}
}

func postgresPRRegistryJobKustCases() []postgresPRRegistryUnitCase {
	return []postgresPRRegistryUnitCase{
		// AtomicTransitionDeployedByJob
		{name: "AtomicTransitionDeployedByJob empty job guard", op: "AtomicTransitionDeployedByJob", jobName: "", wantUpdated: false},
		{name: "AtomicTransitionDeployedByJob begin tx error", op: "AtomicTransitionDeployedByJob", jobName: "brain", mockBeginErr: errors.New("begin err"), wantErr: true},
		{name: "AtomicTransitionDeployedByJob select ErrNoRows", op: "AtomicTransitionDeployedByJob", jobName: "brain", mockSelectErr: pgx.ErrNoRows, wantUpdated: false},
		{name: "AtomicTransitionDeployedByJob select error", op: "AtomicTransitionDeployedByJob", jobName: "brain", mockSelectErr: errors.New("select err"), wantErr: true},
		{name: "AtomicTransitionDeployedByJob remaining jobs update success", op: "AtomicTransitionDeployedByJob", jobName: "cadvisor", repo: "azylman/aerial", targetIDReturn: "100000000000000001", metaJSON: `{"jobs":["cadvisor","node-exporter"]}`, wantUpdated: false, wantTarget: "100000000000000001", wantPRNum: 42},
		{name: "AtomicTransitionDeployedByJob remaining jobs update exec error", op: "AtomicTransitionDeployedByJob", jobName: "cadvisor", metaJSON: `{"jobs":["cadvisor","node-exporter"]}`, mockExecErr: errors.New("exec err"), wantErr: true},
		{name: "AtomicTransitionDeployedByJob remaining jobs update commit error", op: "AtomicTransitionDeployedByJob", jobName: "cadvisor", metaJSON: `{"jobs":["cadvisor","node-exporter"]}`, mockCommitErr: errors.New("commit err"), wantErr: true},
		{name: "AtomicTransitionDeployedByJob final job deployed RowsAffected 1", op: "AtomicTransitionDeployedByJob", jobName: "node-exporter", repo: "azylman/aerial", metaJSON: `{"jobs":["node-exporter"],"started_jobs":["node-exporter"]}`, targetIDReturn: "100000000000000001", wantPRNum: 42, mergeSHAReturn: "merge123", rowsAffected: 1, wantUpdated: true, wantTarget: "100000000000000001", wantSQLSubstr: "metadata->'jobs' ? $1 AND metadata->'started_jobs' ? $1"},
		{name: "AtomicTransitionDeployedByJob final job deployed RowsAffected 0", op: "AtomicTransitionDeployedByJob", jobName: "node-exporter", metaJSON: `{"jobs":["node-exporter"]}`, rowsAffected: 0, wantUpdated: false},
		{name: "AtomicTransitionDeployedByJob final job exec error", op: "AtomicTransitionDeployedByJob", jobName: "node-exporter", metaJSON: `{"jobs":["node-exporter"]}`, mockExecErr: errors.New("exec err"), wantErr: true},
		{name: "AtomicTransitionDeployedByJob final job commit error", op: "AtomicTransitionDeployedByJob", jobName: "node-exporter", metaJSON: `{"jobs":["node-exporter"]}`, rowsAffected: 1, mockCommitErr: errors.New("commit err"), wantErr: true},

		// RecordPRJobState
		{name: "RecordPRJobState success", op: "RecordPRJobState", jobName: "cadvisor", targetID: "verifying_health", targetIDReturn: "azylman/aerial", wantPRNum: 42, wantUpdated: true, wantSQLSubstr: "metadata->'job_states'"},
		{name: "RecordPRJobState unchanged or not found", op: "RecordPRJobState", jobName: "cadvisor", targetID: "verifying_health", mockSelectErr: pgx.ErrNoRows, wantUpdated: false},
		{name: "RecordPRJobState db error", op: "RecordPRJobState", jobName: "cadvisor", targetID: "verifying_health", mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "RecordPRJobState nil pool guard", op: "RecordPRJobState", nilPool: true, jobName: "cadvisor", targetID: "verifying_health", wantUpdated: false},
		{name: "RecordPRJobState empty jobName guard", op: "RecordPRJobState", jobName: "", targetID: "verifying_health", wantUpdated: false},
		{name: "RecordPRJobState empty jobState guard", op: "RecordPRJobState", jobName: "cadvisor", targetID: "", wantUpdated: false},

		// AtomicTransitionDeployFailedByJob
		{name: "AtomicTransitionDeployFailedByJob empty job guard", op: "AtomicTransitionDeployFailedByJob", jobName: "", wantUpdated: false},
		{name: "AtomicTransitionDeployFailedByJob select ErrNoRows", op: "AtomicTransitionDeployFailedByJob", jobName: "brain", mockSelectErr: pgx.ErrNoRows, wantUpdated: false, wantSQLSubstr: "deployed_jobs"},
		{name: "AtomicTransitionDeployFailedByJob select error", op: "AtomicTransitionDeployFailedByJob", jobName: "brain", mockSelectErr: errors.New("select err"), wantErr: true},
		{name: "AtomicTransitionDeployFailedByJob exec error", op: "AtomicTransitionDeployFailedByJob", jobName: "brain", mockExecErr: errors.New("exec err"), wantErr: true},
		{name: "AtomicTransitionDeployFailedByJob RowsAffected 0", op: "AtomicTransitionDeployFailedByJob", jobName: "brain", repo: "azylman/aerial", targetIDReturn: "100000000000000001", wantPRNum: 42, mergeSHAReturn: "merge123", rowsAffected: 0, wantUpdated: false, wantTarget: "100000000000000001"},
		{name: "AtomicTransitionDeployFailedByJob success", op: "AtomicTransitionDeployFailedByJob", jobName: "brain", repo: "azylman/aerial", targetIDReturn: "100000000000000001", wantPRNum: 42, mergeSHAReturn: "merge123", rowsAffected: 1, wantUpdated: true, wantTarget: "100000000000000001"},

		// EnsurePRRegistered
		{name: "EnsurePRRegistered empty repo guard", op: "EnsurePRRegistered", repo: "", prNumber: 692, wantTarget: ""},
		{name: "EnsurePRRegistered invalid targetID skips upsert", op: "EnsurePRRegistered", repo: "azylman/mirrormere", prNumber: 692, targetID: "invalid-target", wantTarget: ""},
		{name: "EnsurePRRegistered upsert exec error", op: "EnsurePRRegistered", repo: "azylman/mirrormere", prNumber: 692, targetID: "100000000000000002", mockExecErr: errors.New("upsert err"), wantErr: true},
		{name: "EnsurePRRegistered success", op: "EnsurePRRegistered", repo: "azylman/mirrormere", prNumber: 692, targetID: "100000000000000002", wantTarget: "100000000000000002"},

		// RegisterPR
		{name: "RegisterPR nil pool guard", op: "RegisterPR", nilPool: true, repo: "azylman/aerial", prNumber: 42, targetID: "100000000000000001", wantErr: true},
		{name: "RegisterPR empty repo guard", op: "RegisterPR", repo: "", prNumber: 42, targetID: "100000000000000001", wantErr: true},
		{name: "RegisterPR zero pr guard", op: "RegisterPR", repo: "azylman/aerial", prNumber: 0, targetID: "100000000000000001", wantErr: true},
		{name: "RegisterPR empty branch guard", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, sha: "head123", targetID: "100000000000000001", branchReturn: "", wantErr: true},
		{name: "RegisterPR empty head_sha guard", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, sha: "", targetID: "100000000000000001", branchReturn: "feat/foo", wantErr: true},
		{name: "RegisterPR invalid targetID without chunk returns error", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, branchReturn: "feat/foo", sha: "head123", targetID: "invalid-target", wantErr: true},
		{name: "RegisterPR empty targetID without chunk returns error", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, branchReturn: "feat/foo", sha: "head123", targetID: "", wantErr: true},
		{name: "RegisterPR upsert exec error", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, branchReturn: "feat/foo", sha: "head123", targetID: "100000000000000001", mockExecErr: errors.New("upsert err"), wantErr: true},
		{name: "RegisterPR success direct targetID", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, branchReturn: "feat/foo", sha: "head123", targetID: "100000000000000001", wantTarget: "100000000000000001", wantSQLSubstr: "INSERT INTO pr_registry"},
		{name: "RegisterPR success discord prefixed targetID", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, branchReturn: "feat/foo", sha: "head123", targetID: "discord-100000000000000001", wantTarget: "100000000000000001", wantSQLSubstr: "INSERT INTO pr_registry"},

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

		// AppendPRJobs
		{name: "AppendPRJobs empty repo guard", op: "AppendPRJobs", repo: "", prNumber: 42, jobs: []string{"coven"}},
		{name: "AppendPRJobs zero pr guard", op: "AppendPRJobs", repo: "azylman/aerial", prNumber: 0, jobs: []string{"coven"}},
		{name: "AppendPRJobs nil pool guard", op: "AppendPRJobs", nilPool: true, repo: "azylman/aerial", prNumber: 42, jobs: []string{"coven"}},
		{name: "AppendPRJobs empty jobs guard", op: "AppendPRJobs", repo: "azylman/aerial", prNumber: 42, jobs: []string{}},
		{name: "AppendPRJobs success", op: "AppendPRJobs", repo: "azylman/aerial", prNumber: 42, jobs: []string{"coven"}, wantSQLSubstr: "UPDATE pr_registry"},
		// RecordKustomizationRequests
		{name: "RecordKustomizationRequests empty repo guard", op: "RecordKustomizationRequests", repo: "", prNumber: 42, kustRequests: map[string]kustomizationRequest{"aerial-core": {Namespace: "flux-system", RequestedAt: "t1"}}, mockExecErr: errors.New("unexpected exec")},
		{name: "RecordKustomizationRequests zero pr guard", op: "RecordKustomizationRequests", repo: "azylman/aerial", prNumber: 0, kustRequests: map[string]kustomizationRequest{"aerial-core": {Namespace: "flux-system", RequestedAt: "t1"}}, mockExecErr: errors.New("unexpected exec")},
		{name: "RecordKustomizationRequests nil pool guard", op: "RecordKustomizationRequests", nilPool: true, repo: "azylman/aerial", prNumber: 42, kustRequests: map[string]kustomizationRequest{"aerial-core": {Namespace: "flux-system", RequestedAt: "t1"}}},
		{name: "RecordKustomizationRequests empty requests guard", op: "RecordKustomizationRequests", repo: "azylman/aerial", prNumber: 42, mockExecErr: errors.New("unexpected exec")},
		{name: "RecordKustomizationRequests replaces requests and clears deployed", op: "RecordKustomizationRequests", repo: "azylman/aerial", prNumber: 42, kustRequests: map[string]kustomizationRequest{"aerial-core": {Namespace: "flux-system", RequestedAt: "t1"}}, wantSQLSubstr: "COALESCE(metadata, '{}'::jsonb) - 'deployed_kustomizations', '{kustomization_requests}', $3::jsonb"},
		{name: "RecordKustomizationRequests exec error", op: "RecordKustomizationRequests", repo: "azylman/aerial", prNumber: 42, kustRequests: map[string]kustomizationRequest{"aerial-core": {Namespace: "flux-system", RequestedAt: "t1"}}, mockExecErr: errors.New("exec error"), wantErr: true},
		// MarkKustomizationsDeployed
		{name: "MarkKustomizationsDeployed empty repo guard", op: "MarkKustomizationsDeployed", repo: "", prNumber: 42, jobs: []string{"aerial-core"}, mockSelectErr: errors.New("unexpected query")},
		{name: "MarkKustomizationsDeployed zero pr guard", op: "MarkKustomizationsDeployed", repo: "azylman/aerial", prNumber: 0, jobs: []string{"aerial-core"}, mockSelectErr: errors.New("unexpected query")},
		{name: "MarkKustomizationsDeployed nil pool guard", op: "MarkKustomizationsDeployed", nilPool: true, repo: "azylman/aerial", prNumber: 42, jobs: []string{"aerial-core"}},
		{name: "MarkKustomizationsDeployed empty names guard", op: "MarkKustomizationsDeployed", repo: "azylman/aerial", prNumber: 42, jobs: []string{" "}, mockSelectErr: errors.New("unexpected query")},
		{name: "MarkKustomizationsDeployed returns the scanned transition", op: "MarkKustomizationsDeployed", repo: "azylman/aerial", prNumber: 42, jobs: []string{"marida-core"}, wantUpdated: true},
		{name: "MarkKustomizationsDeployed already deployed returns false", op: "MarkKustomizationsDeployed", repo: "azylman/aerial", prNumber: 42, jobs: []string{"marida-core"}, mockSelectErr: pgx.ErrNoRows},
		{name: "MarkKustomizationsDeployed db error", op: "MarkKustomizationsDeployed", repo: "azylman/aerial", prNumber: 42, jobs: []string{"marida-core"}, mockSelectErr: errors.New("db error"), wantErr: true},

		// HasKustomizationRequests
		{name: "HasKustomizationRequests zero pr guard", op: "HasKustomizationRequests", repo: "azylman/aerial", prNumber: 0, mockSelectErr: errors.New("unexpected query")},
		{name: "HasKustomizationRequests tracked PR", op: "HasKustomizationRequests", repo: "azylman/aerial", prNumber: 42, wantUpdated: true},
		{name: "HasKustomizationRequests unknown PR", op: "HasKustomizationRequests", repo: "azylman/aerial", prNumber: 42, mockSelectErr: pgx.ErrNoRows},
		{name: "HasKustomizationRequests db error", op: "HasKustomizationRequests", repo: "azylman/aerial", prNumber: 42, mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "AppendPRJobs exec error", op: "AppendPRJobs", repo: "azylman/aerial", prNumber: 42, jobs: []string{"coven"}, mockExecErr: errors.New("exec error"), wantErr: true},

		// UpdatePRStatus
		{name: "UpdatePRStatus empty repo guard", op: "UpdatePRStatus", repo: "", prNumber: 42, targetID: "deploying"},
		{name: "UpdatePRStatus zero pr guard", op: "UpdatePRStatus", repo: "azylman/aerial", prNumber: 0, targetID: "deploying"},
		{name: "UpdatePRStatus nil pool guard", op: "UpdatePRStatus", nilPool: true, repo: "azylman/aerial", prNumber: 42, targetID: "deploying"},
		{name: "UpdatePRStatus empty status guard", op: "UpdatePRStatus", repo: "azylman/aerial", prNumber: 42, targetID: ""},
		{name: "UpdatePRStatus success without meta", op: "UpdatePRStatus", repo: "azylman/aerial", prNumber: 42, targetID: "deploying", wantSQLSubstr: "UPDATE pr_registry"},
		{name: "UpdatePRStatus success with meta", op: "UpdatePRStatus", repo: "azylman/aerial", prNumber: 42, targetID: "deploying", metaJSON: `{"banner":"reverting"}`, wantSQLSubstr: "UPDATE pr_registry"},
		{name: "UpdatePRStatus exec error", op: "UpdatePRStatus", repo: "azylman/aerial", prNumber: 42, targetID: "deploying", mockExecErr: errors.New("exec error"), wantErr: true},
	}
}

func TestPostgresPRRegistry_Unit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := append(postgresPRRegistryTransitionCases(), postgresPRRegistryJobKustCases()...)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runPostgresPRRegistryUnitCase(ctx, t, tc)
		})
	}
}

func scanMockPRRegistryDestTypes(dest []any, tc postgresPRRegistryUnitCase) {
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
}

func scanMockPRRegistryDestPos(dest []any, tc postgresPRRegistryUnitCase) {
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
}

func scanMockPRRegistryRow(dest []any, tc postgresPRRegistryUnitCase) error {
	if tc.mockSelectErr != nil {
		return tc.mockSelectErr
	}
	if tc.mockScanErr != nil {
		return tc.mockScanErr
	}
	scanMockPRRegistryDestTypes(dest, tc)
	scanMockPRRegistryDestPos(dest, tc)
	return nil
}

func scanMockPRRegistryTxRow(dest []any, tc postgresPRRegistryUnitCase) error {
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
	}
	return nil
}

func scanMockPRRegistryRows(idx int, dest []any, tc postgresPRRegistryUnitCase) error {
	if tc.mockScanErr != nil {
		return tc.mockScanErr
	}
	if len(dest) == 8 {
		*dest[0].(*int64) = int64(200 + idx)
		*dest[1].(*string) = "azylman/aerial"
		*dest[2].(*int) = 200 + idx
		*dest[3].(*string) = "main"
		*dest[4].(*string) = "headsha"
		*dest[5].(*string) = "mergesha"
		*dest[6].(*string) = "100000000000000001"
		if tc.metaJSON != "" {
			*dest[7].(*[]byte) = []byte(tc.metaJSON)
		}
		return nil
	}
	if len(dest) == 3 {
		*dest[0].(*string) = "arcane103"
		*dest[1].(*string) = "wrap it up"
		*dest[2].(*string) = "done"
	}
	return nil
}

func buildMockPRRegistryPool(tc postgresPRRegistryUnitCase, gotSQL, gotExecSQL *string) *mockSynapsePgxPool {
	if tc.nilPool {
		return nil
	}
	return &mockSynapsePgxPool{
		queryRowFn: func(qCtx context.Context, sql string, args ...any) pgx.Row {
			*gotSQL = sql
			return mockSynapsePgxRow{scanFn: func(dest ...any) error {
				return scanMockPRRegistryRow(dest, tc)
			}}
		},
		execFn: func(qCtx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			*gotExecSQL = sql
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
			return &mockSynapsePgxTx{
				commitErr: tc.mockCommitErr,
				execFn: func(txCtx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
					*gotExecSQL = sql
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
					*gotSQL = sql
					return mockSynapsePgxRow{scanFn: func(dest ...any) error {
						return scanMockPRRegistryTxRow(dest, tc)
					}}
				},
			}, nil
		},
		queryFn: func(qCtx context.Context, sql string, args ...any) (pgx.Rows, error) {
			*gotSQL = sql
			if tc.mockSelectErr != nil {
				return nil, tc.mockSelectErr
			}
			rows := &mockSynapsePgxRows{
				scanErr:   tc.mockScanErr,
				rowsErr:   tc.mockRowsErr,
				totalRows: tc.totalRows,
			}
			rows.scanRowFn = func(idx int, dest ...any) error {
				return scanMockPRRegistryRows(idx, dest, tc)
			}
			return rows, nil
		},
	}
}

func executePRRegistryStatusUpdateOps(ctx context.Context, t *testing.T, reg *PostgresPRRegistry, tc postgresPRRegistryUnitCase) bool {
	t.Helper()
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
	case "UpdatePRStatus":
		var meta map[string]any
		if tc.metaJSON != "" {
			_ = json.Unmarshal([]byte(tc.metaJSON), &meta)
		}
		err := reg.UpdatePRStatus(ctx, tc.repo, tc.prNumber, tc.targetID, meta)
		if (err != nil) != tc.wantErr {
			t.Fatalf("UpdatePRStatus err = %v, wantErr = %v", err, tc.wantErr)
		}
	default:
		return false
	}
	return true
}

func executePRRegistryRegistrationOps(ctx context.Context, t *testing.T, reg *PostgresPRRegistry, tc postgresPRRegistryUnitCase) bool {
	t.Helper()
	switch tc.op {
	case "RegisterPR":
		req := PRRegisterRequest{
			Repo:     tc.repo,
			PRNumber: tc.prNumber,
			Branch:   tc.branchReturn,
			HeadSHA:  tc.sha,
			TargetID: tc.targetID,
			Title:    "feat: title",
			Metadata: tc.metaJSON,
		}
		gotTarget, err := reg.RegisterPR(ctx, req)
		if (err != nil) != tc.wantErr {
			t.Fatalf("RegisterPR err = %v, wantErr = %v", err, tc.wantErr)
		}
		if gotTarget != tc.wantTarget {
			t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
		}
	case "EnsurePRRegistered":
		gotTarget, err := reg.EnsurePRRegistered(ctx, tc.repo, tc.prNumber, "feat/branch", "head123", "feat: title", tc.targetID)
		if (err != nil) != tc.wantErr {
			t.Fatalf("EnsurePRRegistered err = %v, wantErr = %v", err, tc.wantErr)
		}
		if gotTarget != tc.wantTarget {
			t.Errorf("gotTarget = %q, want %q", gotTarget, tc.wantTarget)
		}
	default:
		return false
	}
	return true
}

func executePRRegistryCheckOps(ctx context.Context, t *testing.T, reg *PostgresPRRegistry, tc postgresPRRegistryUnitCase) bool {
	t.Helper()
	switch tc.op {
	case "RecordPRCheckStatus":
		updated, err := reg.RecordPRCheckStatus(ctx, tc.repo, tc.prNumber, tc.sha, tc.jobName, tc.targetID)
		if (err != nil) != tc.wantErr {
			t.Fatalf("RecordPRCheckStatus err = %v, wantErr = %v", err, tc.wantErr)
		}
		if updated != tc.wantUpdated {
			t.Errorf("updated = %v, want %v", updated, tc.wantUpdated)
		}
		return true
	default:
		return false
	}
}

func executePRRegistryAtomicTransitionOps(ctx context.Context, t *testing.T, reg *PostgresPRRegistry, tc postgresPRRegistryUnitCase) bool {
	t.Helper()
	switch tc.op {
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
	default:
		return false
	}
	return true
}

func executePRRegistryResolutionOps(ctx context.Context, t *testing.T, reg *PostgresPRRegistry, tc postgresPRRegistryUnitCase) bool {
	t.Helper()
	switch tc.op {
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
	case "ListDeployingPRs":
		prs, err := reg.ListDeployingPRs(ctx)
		if (err != nil) != tc.wantErr {
			t.Fatalf("ListDeployingPRs err = %v, wantErr = %v", err, tc.wantErr)
		}
		if len(prs) != tc.wantCount {
			t.Errorf("len(prs) = %d, want %d", len(prs), tc.wantCount)
		}
	default:
		return false
	}
	return true
}

func executePRRegistryJobTransitionOps(ctx context.Context, t *testing.T, reg *PostgresPRRegistry, tc postgresPRRegistryUnitCase) bool {
	t.Helper()
	switch tc.op {
	case "AtomicTransitionDeployedByJob":
		gotTarget, gotPR, _, gotRepo, updated, err := reg.AtomicTransitionDeployedByJob(ctx, tc.jobName)
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
		if tc.repo != "" && gotRepo != tc.repo {
			t.Errorf("gotRepo = %q, want %q", gotRepo, tc.repo)
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
	default:
		return false
	}
	return true
}

func executePRRegistryJobMetadataOps(ctx context.Context, t *testing.T, reg *PostgresPRRegistry, tc postgresPRRegistryUnitCase) bool {
	t.Helper()
	switch tc.op {
	case "RecordPRJobState":
		gotRepo, gotPR, updated, err := reg.RecordPRJobState(ctx, tc.jobName, tc.targetID)
		if (err != nil) != tc.wantErr {
			t.Fatalf("RecordPRJobState err = %v, wantErr = %v", err, tc.wantErr)
		}
		if updated != tc.wantUpdated {
			t.Errorf("updated = %v, want %v", updated, tc.wantUpdated)
		}
		if gotPR != tc.wantPRNum {
			t.Errorf("gotPR = %d, want %d", gotPR, tc.wantPRNum)
		}
		if tc.targetIDReturn != "" && gotRepo != tc.targetIDReturn {
			t.Errorf("gotRepo = %q, want %q", gotRepo, tc.targetIDReturn)
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
	case "AppendPRJobs":
		err := reg.AppendPRJobs(ctx, tc.repo, tc.prNumber, tc.jobs)
		if (err != nil) != tc.wantErr {
			t.Fatalf("AppendPRJobs err = %v, wantErr = %v", err, tc.wantErr)
		}
	default:
		return false
	}
	return true
}

func executePRRegistryKustOps(ctx context.Context, t *testing.T, reg *PostgresPRRegistry, tc postgresPRRegistryUnitCase) {
	t.Helper()
	switch tc.op {
	case "RecordKustomizationRequests":
		err := reg.RecordKustomizationRequests(ctx, tc.repo, tc.prNumber, tc.kustRequests)
		if (err != nil) != tc.wantErr {
			t.Fatalf("RecordKustomizationRequests err = %v, wantErr = %v", err, tc.wantErr)
		}
	case "MarkKustomizationsDeployed":
		got, err := reg.MarkKustomizationsDeployed(ctx, tc.repo, tc.prNumber, tc.jobs)
		if (err != nil) != tc.wantErr {
			t.Fatalf("MarkKustomizationsDeployed err = %v, wantErr = %v", err, tc.wantErr)
		}
		if got != tc.wantUpdated {
			t.Errorf("transitioned = %v, want %v", got, tc.wantUpdated)
		}
	case "HasKustomizationRequests":
		got, err := reg.HasKustomizationRequests(ctx, tc.repo, tc.prNumber)
		if (err != nil) != tc.wantErr {
			t.Fatalf("HasKustomizationRequests err = %v, wantErr = %v", err, tc.wantErr)
		}
		if got != tc.wantUpdated {
			t.Errorf("tracked = %v, want %v", got, tc.wantUpdated)
		}
	}
}

func runPostgresPRRegistryUnitCase(ctx context.Context, t *testing.T, tc postgresPRRegistryUnitCase) {
	t.Helper()
	var gotSQL, gotExecSQL string
	pool := buildMockPRRegistryPool(tc, &gotSQL, &gotExecSQL)

	reg := NewPostgresPRRegistry(pool)
	if tc.nilPool {
		reg = NewPostgresPRRegistry((*pgxpool.Pool)(nil))
	}

	if !executePRRegistryStatusUpdateOps(ctx, t, reg, tc) &&
		!executePRRegistryRegistrationOps(ctx, t, reg, tc) &&
		!executePRRegistryCheckOps(ctx, t, reg, tc) &&
		!executePRRegistryAtomicTransitionOps(ctx, t, reg, tc) &&
		!executePRRegistryResolutionOps(ctx, t, reg, tc) &&
		!executePRRegistryJobTransitionOps(ctx, t, reg, tc) &&
		!executePRRegistryJobMetadataOps(ctx, t, reg, tc) {
		executePRRegistryKustOps(ctx, t, reg, tc)
	}

	if tc.wantSQLSubstr != "" && !strings.Contains(gotSQL, tc.wantSQLSubstr) && !strings.Contains(gotExecSQL, tc.wantSQLSubstr) {
		t.Errorf("gotSQL = %q, gotExecSQL = %q, want substring %q", gotSQL, gotExecSQL, tc.wantSQLSubstr)
	}
}

func TestExtractJobsFromMetadata_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		metadata map[string]any
		expected []string
	}{
		{name: "nil metadata", metadata: nil, expected: nil},
		{name: "jobs array string", metadata: map[string]any{"jobs": []any{"brain", "scheduler-mcp"}}, expected: []string{"brain", "scheduler-mcp"}},
		{name: "deduplicates clean job names", metadata: map[string]any{"jobs": []any{"brain", "brain", "hangar"}}, expected: []string{"brain", "hangar"}},
		{name: "empty jobs array", metadata: map[string]any{"jobs": []any{}}, expected: nil},
		{name: "non-slice jobs", metadata: map[string]any{"jobs": 123}, expected: nil},
		{name: "typed string slice", metadata: map[string]any{"jobs": []string{"job-a", "job-b"}}, expected: []string{"job-a", "job-b"}},
		{name: "whitespace and empty jobs filtered", metadata: map[string]any{"jobs": []any{"", "  ", "brain"}}, expected: []string{"brain"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := extractJobsFromMetadata(tt.metadata)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("extractJobsFromMetadata() = %v, want %v", got, tt.expected)
			}
		})
	}
}
