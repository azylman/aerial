package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/azylman/aerial/services/synapse/internal/model"
)

const (
	prMerged   = "merged"
	prConflict = "conflict"
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

func (*mockSynapsePgxRows) Close()       {}
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
		return scanValues(dest, r.intItems[r.idx-1])
	}
	return nil
}

func scanValues(dest []any, vals ...any) error {
	if len(vals) > len(dest) {
		return fmt.Errorf("scan: %d values for %d destinations", len(vals), len(dest))
	}
	for i, v := range vals {
		if v == nil {
			continue
		}
		ptr, val := reflect.ValueOf(dest[i]), reflect.ValueOf(v)
		if ptr.Kind() != reflect.Pointer || ptr.IsNil() || ptr.Elem().Type() != val.Type() {
			return fmt.Errorf("scan column %d: destination %T cannot hold %T", i, dest[i], v)
		}
		ptr.Elem().Set(val)
	}
	return nil
}

type mockSynapsePgxPool struct {
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
	execFn     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	queryFn    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
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
	kustRequests   map[string]model.KustomizationRequest
	targetID       string
	mockSelectErr  error
	mockScanErr    error
	mockExecErr    error
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
		{name: "UpdatePRSync success", op: "UpdatePRSync", repo: "azylman/aerial", prNumber: 42, sha: "head123", targetIDReturn: "100000000000000001", wantTarget: "100000000000000001"},
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
		// RecordPRJobState
		{name: "RecordPRJobState success", op: "RecordPRJobState", jobName: "cadvisor", targetID: "verifying_health", targetIDReturn: "azylman/aerial", wantPRNum: 42, wantUpdated: true, wantSQLSubstr: "metadata->'job_states'"},
		{name: "RecordPRJobState unchanged or not found", op: "RecordPRJobState", jobName: "cadvisor", targetID: "verifying_health", mockSelectErr: pgx.ErrNoRows, wantUpdated: false},
		{name: "RecordPRJobState db error", op: "RecordPRJobState", jobName: "cadvisor", targetID: "verifying_health", mockSelectErr: errors.New("db error"), wantErr: true},
		{name: "RecordPRJobState nil pool guard", op: "RecordPRJobState", nilPool: true, jobName: "cadvisor", targetID: "verifying_health", wantUpdated: false},
		{name: "RecordPRJobState empty jobName guard", op: "RecordPRJobState", jobName: "", targetID: "verifying_health", wantUpdated: false},
		{name: "RecordPRJobState empty jobState guard", op: "RecordPRJobState", jobName: "cadvisor", targetID: "", wantUpdated: false},

		// EnsurePRRegistered
		{name: "EnsurePRRegistered empty repo guard", op: "EnsurePRRegistered", repo: "", prNumber: 692, wantTarget: ""},
		{name: "EnsurePRRegistered invalid targetID skips upsert", op: "EnsurePRRegistered", repo: "azylman/aerial", prNumber: 692, targetID: "invalid_id", wantTarget: ""},
		{name: "EnsurePRRegistered success", op: "EnsurePRRegistered", repo: "azylman/aerial", prNumber: 692, targetID: "100000000000000001", wantTarget: "100000000000000001", wantSQLSubstr: "INSERT INTO pr_registry"},
		{name: "EnsurePRRegistered exec error", op: "EnsurePRRegistered", repo: "azylman/aerial", prNumber: 692, targetID: "100000000000000001", mockExecErr: errors.New("exec error"), wantErr: true},

		// RecordExpectedJobs
		{name: "RecordExpectedJobs empty jobs delegates to HasExpected", op: "RecordExpectedJobs", repo: "azylman/aerial", prNumber: 42, jobs: nil, wantUpdated: true},
		{name: "RecordExpectedJobs success with non-empty jobs", op: "RecordExpectedJobs", repo: "azylman/aerial", prNumber: 42, jobs: []string{"brain"}, wantUpdated: true, wantSQLSubstr: "UPDATE pr_registry"},
		{name: "RecordExpectedJobs not found", op: "RecordExpectedJobs", repo: "azylman/aerial", prNumber: 42, jobs: []string{"brain"}, mockSelectErr: pgx.ErrNoRows, wantUpdated: false},
		{name: "RecordExpectedJobs query error", op: "RecordExpectedJobs", repo: "azylman/aerial", prNumber: 42, jobs: []string{"brain"}, mockSelectErr: errors.New("db error"), wantErr: true},

		// HasExpectedDeployJobs
		{name: "HasExpectedDeployJobs nil pool guard", op: "HasExpectedDeployJobs", nilPool: true, repo: "azylman/aerial", prNumber: 42},
		{name: "HasExpectedDeployJobs empty repo guard", op: "HasExpectedDeployJobs", repo: "", prNumber: 42},
		{name: "HasExpectedDeployJobs zero prNumber guard", op: "HasExpectedDeployJobs", repo: "azylman/aerial", prNumber: 0},
		{name: "HasExpectedDeployJobs success true", op: "HasExpectedDeployJobs", repo: "azylman/aerial", prNumber: 42, wantUpdated: true},
		{name: "HasExpectedDeployJobs not found returns false", op: "HasExpectedDeployJobs", repo: "azylman/aerial", prNumber: 42, mockSelectErr: pgx.ErrNoRows, wantUpdated: false},
		{name: "HasExpectedDeployJobs db error", op: "HasExpectedDeployJobs", repo: "azylman/aerial", prNumber: 42, mockSelectErr: errors.New("db error"), wantErr: true},

		// RegisterPR
		{name: "RegisterPR nil pool guard", op: "RegisterPR", nilPool: true, repo: "azylman/aerial", prNumber: 42, targetID: "100000000000000001", wantErr: true},
		{name: "RegisterPR empty repo guard", op: "RegisterPR", repo: "", prNumber: 42, targetID: "100000000000000001", wantErr: true},
		{name: "RegisterPR zero prNumber guard", op: "RegisterPR", repo: "azylman/aerial", prNumber: 0, targetID: "100000000000000001", wantErr: true},
		{name: "RegisterPR empty branch guard", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, branchReturn: "", sha: "head123", targetID: "100000000000000001", wantErr: true},
		{name: "RegisterPR empty headSHA guard", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, branchReturn: "feat/pr", sha: "", targetID: "100000000000000001", wantErr: true},
		{name: "RegisterPR empty targetID guard", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, branchReturn: "feat/pr", sha: "head123", targetID: "", wantErr: true},
		{name: "RegisterPR invalid targetID snowflake guard", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, branchReturn: "feat/pr", sha: "head123", targetID: "invalid-id", wantErr: true},
		{name: "RegisterPR success", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, branchReturn: "feat/pr", sha: "head123", targetID: "100000000000000001", metaJSON: `{"custom":"val"}`, wantTarget: "100000000000000001", wantSQLSubstr: "INSERT INTO pr_registry"},
		{name: "RegisterPR exec error", op: "RegisterPR", repo: "azylman/aerial", prNumber: 42, branchReturn: "feat/pr", sha: "head123", targetID: "100000000000000001", mockExecErr: errors.New("exec error"), wantErr: true},

		// AppendPRJobs
		{name: "AppendPRJobs empty repo guard", op: "AppendPRJobs", repo: "", prNumber: 42, jobs: []string{"coven"}, mockExecErr: errors.New("unexpected exec")},
		{name: "AppendPRJobs zero pr guard", op: "AppendPRJobs", repo: "azylman/aerial", prNumber: 0, jobs: []string{"coven"}, mockExecErr: errors.New("unexpected exec")},
		{name: "AppendPRJobs nil pool guard", op: "AppendPRJobs", nilPool: true, repo: "azylman/aerial", prNumber: 42, jobs: []string{"coven"}},
		{name: "AppendPRJobs empty jobs guard", op: "AppendPRJobs", repo: "azylman/aerial", prNumber: 42, jobs: nil, mockExecErr: errors.New("unexpected exec")},
		{name: "AppendPRJobs success", op: "AppendPRJobs", repo: "azylman/aerial", prNumber: 42, jobs: []string{"coven"}, wantSQLSubstr: "UPDATE pr_registry"},
		{name: "AppendPRJobs exec error", op: "AppendPRJobs", repo: "azylman/aerial", prNumber: 42, jobs: []string{"coven"}, mockExecErr: errors.New("exec error"), wantErr: true},

		// UpdatePRStatus
		{name: "UpdatePRStatus empty repo guard", op: "UpdatePRStatus", repo: "", prNumber: 42, targetID: "deploying"},
		{name: "UpdatePRStatus zero pr guard", op: "UpdatePRStatus", repo: "azylman/aerial", prNumber: 0, targetID: "deploying"},
		{name: "UpdatePRStatus nil pool guard", op: "UpdatePRStatus", nilPool: true, repo: "azylman/aerial", prNumber: 42, targetID: "deploying"},
		{name: "UpdatePRStatus empty status guard", op: "UpdatePRStatus", repo: "azylman/aerial", prNumber: 42, targetID: ""},
		{name: "UpdatePRStatus success without meta", op: "UpdatePRStatus", repo: "azylman/aerial", prNumber: 42, targetID: "deploying", wantSQLSubstr: "UPDATE pr_registry"},
		{name: "UpdatePRStatus success with meta", op: "UpdatePRStatus", repo: "azylman/aerial", prNumber: 42, targetID: "deploying", metaJSON: `{"banner":"reverting"}`, wantSQLSubstr: "UPDATE pr_registry"},
		{name: "UpdatePRStatus exec error", op: "UpdatePRStatus", repo: "azylman/aerial", prNumber: 42, targetID: "deploying", mockExecErr: errors.New("exec error"), wantErr: true},

		// AcknowledgeDeployFailedPublished
		{name: "AcknowledgeDeployFailedPublished empty repo guard", op: "AcknowledgeDeployFailedPublished", repo: "", prNumber: 42},
		{name: "AcknowledgeDeployFailedPublished zero pr guard", op: "AcknowledgeDeployFailedPublished", repo: "azylman/aerial", prNumber: 0},
		{name: "AcknowledgeDeployFailedPublished nil pool guard", op: "AcknowledgeDeployFailedPublished", nilPool: true, repo: "azylman/aerial", prNumber: 42},
		{name: "AcknowledgeDeployFailedPublished success", op: "AcknowledgeDeployFailedPublished", repo: "azylman/aerial", prNumber: 42, wantSQLSubstr: "status = 'deploy_failed'"},
		{name: "AcknowledgeDeployFailedPublished exec error", op: "AcknowledgeDeployFailedPublished", repo: "azylman/aerial", prNumber: 42, mockExecErr: errors.New("exec error"), wantErr: true},
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

func scanMockPRRegistryDestPos(dest []any, tc postgresPRRegistryUnitCase) error {
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
		return scanValues(dest, int64(100), tc.targetIDReturn, tc.wantPRNum, tc.mergeSHAReturn, tc.repo)
	}
	return nil
}

func scanMockPRRegistryRow(dest []any, tc postgresPRRegistryUnitCase) error {
	if tc.mockSelectErr != nil {
		return tc.mockSelectErr
	}
	if tc.mockScanErr != nil {
		return tc.mockScanErr
	}
	scanMockPRRegistryDestTypes(dest, tc)
	return scanMockPRRegistryDestPos(dest, tc)
}

func scanMockPRRegistryRows(idx int, dest []any, tc postgresPRRegistryUnitCase) error {
	if tc.mockScanErr != nil {
		return tc.mockScanErr
	}
	if len(dest) == 8 {
		var meta any
		if tc.metaJSON != "" {
			meta = []byte(tc.metaJSON)
		}
		return scanValues(dest, int64(200+idx), "azylman/aerial", 200+idx, "main", "headsha", "mergesha", "100000000000000001", meta)
	}
	if len(dest) == 3 {
		return scanValues(dest, "arcane103", "wrap it up", "done")
	}
	return nil
}

func buildMockPRRegistryPool(tc postgresPRRegistryUnitCase, gotSQL, gotExecSQL *string) PGXQueryExecer {
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
	case "AcknowledgeDeployFailedPublished":
		err := reg.AcknowledgeDeployFailedPublished(ctx, tc.repo, tc.prNumber)
		if (err != nil) != tc.wantErr {
			t.Fatalf("AcknowledgeDeployFailedPublished err = %v, wantErr = %v", err, tc.wantErr)
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
		req := model.PRRegisterRequest{
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
		!executePRRegistryJobMetadataOps(ctx, t, reg, tc) {
		t.Fatalf("unhandled operation: %s", tc.op)
	}

	if tc.wantSQLSubstr != "" && !strings.Contains(gotSQL, tc.wantSQLSubstr) && !strings.Contains(gotExecSQL, tc.wantSQLSubstr) {
		t.Errorf("gotSQL = %q, gotExecSQL = %q, want substring %q", gotSQL, gotExecSQL, tc.wantSQLSubstr)
	}
}

func newMockRowPool(dbErr error, raw []byte) *mockSynapsePgxPool {
	return &mockSynapsePgxPool{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return mockSynapsePgxRow{scanFn: func(dest ...any) error {
				if dbErr != nil {
					return dbErr
				}
				if len(dest) > 0 {
					if ptr, ok := dest[0].(*[]byte); ok {
						*ptr = raw
					}
				}
				return nil
			}}
		},
	}
}

func newMockStatusPool(dbErr error, status string) *mockSynapsePgxPool {
	return &mockSynapsePgxPool{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return mockSynapsePgxRow{scanFn: func(dest ...any) error {
				if dbErr != nil {
					return dbErr
				}
				if len(dest) > 0 {
					if ptr, ok := dest[0].(*string); ok {
						*ptr = status
					}
				}
				return nil
			}}
		},
	}
}

func TestPostgresPRRegistry_LoadRawPRMetadata(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		repo     string
		prNumber int
		nilPool  bool
		rawJSON  string
		dbErr    error
		wantErr  bool
		wantNil  bool
		wantVal  string
	}{
		{name: "success", repo: "azylman/aerial", prNumber: 42, rawJSON: `{"custom":"value"}`, wantVal: "value"},
		{name: "not found", repo: "azylman/aerial", prNumber: 42, dbErr: pgx.ErrNoRows, wantNil: true},
		{name: "db error", repo: "azylman/aerial", prNumber: 42, dbErr: errors.New("db down"), wantErr: true},
		{name: "invalid json", repo: "azylman/aerial", prNumber: 42, rawJSON: `{bad json`, wantErr: true},
		{name: "empty repo", repo: "", prNumber: 42, wantNil: true},
		{name: "zero pr", repo: "azylman/aerial", prNumber: 0, wantNil: true},
		{name: "nil pool", repo: "azylman/aerial", prNumber: 42, nilPool: true, wantNil: true},
		{name: "nil receiver", repo: "azylman/aerial", prNumber: 42, wantNil: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool := newMockRowPool(tc.dbErr, []byte(tc.rawJSON))
			reg := NewPostgresPRRegistry(pool)
			if tc.nilPool {
				reg = NewPostgresPRRegistry(nil)
			}
			if tc.name == "nil receiver" {
				reg = nil
			}
			got, err := reg.LoadRawPRMetadata(context.Background(), tc.repo, tc.prNumber)
			if (err != nil) != tc.wantErr {
				t.Fatalf("LoadRawPRMetadata() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantNil && len(got) != 0 {
				t.Fatalf("LoadRawPRMetadata() got = %v, want empty", got)
			}
			if tc.wantVal != "" {
				if len(got) == 0 || got["custom"] != tc.wantVal {
					t.Fatalf("LoadRawPRMetadata() got = %v, want custom=%q", got, tc.wantVal)
				}
			}
		})
	}
}

func TestPostgresPRRegistry_IsPRDeployed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		repo     string
		prNumber int
		nilPool  bool
		status   string
		dbErr    error
		wantErr  bool
		want     bool
	}{
		{name: "deployed exact", repo: "azylman/aerial", prNumber: 42, status: "deployed", want: true},
		{name: "deployed uppercase and padded", repo: "azylman/aerial", prNumber: 42, status: "  DEPLOYED ", want: true},
		{name: "deploying returns false", repo: "azylman/aerial", prNumber: 42, status: "deploying", want: false},
		{name: "open returns false", repo: "azylman/aerial", prNumber: 42, status: "open", want: false},
		{name: "not found returns false", repo: "azylman/aerial", prNumber: 42, dbErr: pgx.ErrNoRows, want: false},
		{name: "db error", repo: "azylman/aerial", prNumber: 42, dbErr: errors.New("db error"), wantErr: true},
		{name: "empty repo", repo: "", prNumber: 42, want: false},
		{name: "zero pr", repo: "azylman/aerial", prNumber: 0, want: false},
		{name: "nil pool", repo: "azylman/aerial", prNumber: 42, nilPool: true, want: false},
		{name: "nil receiver", repo: "azylman/aerial", prNumber: 42, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool := newMockStatusPool(tc.dbErr, tc.status)
			reg := NewPostgresPRRegistry(pool)
			if tc.nilPool {
				reg = NewPostgresPRRegistry(nil)
			}
			if tc.name == "nil receiver" {
				reg = nil
			}
			got, err := reg.IsPRDeployed(context.Background(), tc.repo, tc.prNumber)
			if (err != nil) != tc.wantErr {
				t.Fatalf("IsPRDeployed() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("IsPRDeployed() got = %v, want = %v", got, tc.want)
			}
		})
	}
}

func metadataFixture(t *testing.T) model.BuildMetadata {
	t.Helper()
	raw, err := os.ReadFile("../../testdata/build-metadata-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	m, err := model.ParseBuildMetadata(raw)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestRecordPRBuildMetadata(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, repo, kind     string
		nilPool, nilReceiver bool
		dbErr                error
		wantErr, want        bool
	}{
		{name: "updated", repo: "owner/repo", want: true},
		{name: "final", repo: "owner/repo", kind: "final", want: true},
		{name: "transport result rejected", repo: "owner/repo", kind: "result", wantErr: true},
		{name: "progress snapshot stored", repo: "owner/repo", kind: "progress", want: true},
		{name: "invalid snapshot rejected", repo: "owner/repo", kind: "unknown", wantErr: true},
		{name: "wrong repo", repo: "owner/other", wantErr: true},
		{name: "nil pool", repo: "owner/repo", nilPool: true},
		{name: "nil receiver", repo: "owner/repo", nilReceiver: true},
		{name: "unchanged", repo: "owner/repo", dbErr: pgx.ErrNoRows},
		{name: "database error", repo: "owner/repo", dbErr: errors.New("offline"), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := metadataFixture(t)
			if tc.kind != "" {
				m.Kind = tc.kind
			}
			if m.Kind == "result" {
				m.Service = "engine"
				m.ImageName = "engine"
				m.State = "success"
				m.SourceAttempt = 1
			}
			r := NewPostgresPRRegistry(&mockSynapsePgxPool{queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return mockSynapsePgxRow{scanFn: func(dest ...any) error {
					if tc.dbErr != nil {
						return tc.dbErr
					}
					return scanValues(dest, true)
				}}
			}})
			if tc.nilPool {
				r.pool = nil
			}
			if tc.nilReceiver {
				r = nil
			}
			got, err := r.RecordPRBuildMetadata(context.Background(), tc.repo, 1, m)
			if got != tc.want || (err != nil) != tc.wantErr {
				t.Fatalf("got=%v err=%v", got, err)
			}
		})
	}
}

func TestLoadPRBuildMetadata(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, raw, repo string
		nilPool         bool
		dbErr           error
		wantErr, want   bool
	}{
		{name: "valid", repo: "owner/repo", want: true},
		{name: "wrong repo", repo: "owner/other", wantErr: true},
		{name: "invalid JSON", repo: "owner/repo", raw: "{}", wantErr: true},
		{name: "nil pool", repo: "owner/repo", nilPool: true},
		{name: "absent", repo: "owner/repo", dbErr: pgx.ErrNoRows},
		{name: "database error", repo: "owner/repo", dbErr: errors.New("offline"), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw, err := os.ReadFile("../../testdata/build-metadata-v1.json")
			if err != nil {
				t.Fatal(err)
			}
			if tc.raw != "" {
				raw = []byte(tc.raw)
			}
			r := NewPostgresPRRegistry(&mockSynapsePgxPool{queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return mockSynapsePgxRow{scanFn: func(dest ...any) error {
					if tc.dbErr != nil {
						return tc.dbErr
					}
					return scanValues(dest, raw)
				}}
			}})
			if tc.nilPool {
				r.pool = nil
			}
			m, got, err := r.LoadPRBuildMetadata(context.Background(), tc.repo, 1)
			if got != tc.want || (err != nil) != tc.wantErr {
				t.Fatalf("got=%v err=%v", got, err)
			}
			if got && m.RunID != 1 {
				t.Fatal("wrong run identity")
			}
		})
	}
}

func TestLoadPRBuildLifecycle(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, repo string
		pr         int
		nilPool    bool
		dbErr      error
		want       model.PRBuildLifecycle
		wantErr    bool
	}{
		{"merged with started deploy", "owner/repo", 1, false, nil, model.PRBuildLifecycle{Status: prMerged, HeadSHA: "head", MergeSHA: "merge", Started: true}, false},
		{"absent PR", "owner/repo", 1, false, pgx.ErrNoRows, model.PRBuildLifecycle{}, false},
		{"database error", "owner/repo", 1, false, errors.New("offline"), model.PRBuildLifecycle{}, true},
		{"nil pool", "owner/repo", 1, true, nil, model.PRBuildLifecycle{}, false},
		{"invalid PR", "owner/repo", 0, false, nil, model.PRBuildLifecycle{}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := NewPostgresPRRegistry(&mockSynapsePgxPool{queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return mockSynapsePgxRow{scanFn: func(dest ...any) error {
					if tc.dbErr != nil {
						return tc.dbErr
					}
					return scanValues(dest, prMerged, "head", "merge", true)
				}}
			}})
			if tc.nilPool {
				r.pool = nil
			}
			got, err := r.LoadPRBuildLifecycle(context.Background(), tc.repo, tc.pr)
			if got != tc.want || (err != nil) != tc.wantErr {
				t.Fatalf("got=%+v err=%v", got, err)
			}
		})
	}
}
