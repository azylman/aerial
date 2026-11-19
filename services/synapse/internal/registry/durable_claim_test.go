package registry

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/azylman/aerial/services/synapse/internal/model"
)

// newDurableClaimPool builds a mock pool that captures executed SQL and scans
// hand-derived literals for the durable claim/state primitives.
func newDurableClaimPool(gotSQL, gotExecSQL *string, queryErr, execErr error, scanVals ...any) *mockSynapsePgxPool {
	return &mockSynapsePgxPool{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			*gotSQL = sql
			return mockSynapsePgxRow{scanFn: func(dest ...any) error {
				if queryErr != nil {
					return queryErr
				}
				return scanValues(dest, scanVals...)
			}}
		},
		execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			*gotExecSQL = sql
			if execErr != nil {
				return pgconn.CommandTag{}, execErr
			}
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
	}
}

// durableCase is the single homogeneous row shape for every durable
// claim/state primitive table. Rows set only the fields their method reads;
// method adapters translate concrete results into want for comparison.
type durableCase struct {
	name     string
	repo     string
	prNumber int
	sha      string
	checkKey string
	roborev  bool
	was      bool
	hadPrior bool
	nilPool  bool
	queryErr error
	execErr  error
	scan     []any
	want     []any
	wantErr  bool
	// wantNoDB asserts the method returns without touching the pool, so a
	// removed input guard fails instead of passing on mock zero values.
	wantNoDB bool
}

// runDurableCases executes one homogeneous table for the single method named
// by op. The call adapter invokes that method and normalizes its results.
func runDurableCases(t *testing.T, op string, tests []durableCase, call func(context.Context, *PostgresPRRegistry, durableCase) ([]any, error)) {
	t.Helper()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var gotSQL, gotExecSQL string
			reg := NewPostgresPRRegistry(newDurableClaimPool(&gotSQL, &gotExecSQL, tc.queryErr, tc.execErr, tc.scan...))
			if tc.nilPool {
				reg = NewPostgresPRRegistry(nil)
			}
			got, err := call(context.Background(), reg, tc)
			if (err != nil) != tc.wantErr {
				t.Fatalf("%s() err = %v, wantErr = %v", op, err, tc.wantErr)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("%s() = %v, want %v", op, got, tc.want)
			}
			if tc.wantNoDB && (gotSQL != "" || gotExecSQL != "") {
				t.Errorf("%s() touched the database on a guard row: sql = %q exec = %q", op, gotSQL, gotExecSQL)
			}
		})
	}
}

// durablePRGuards covers the repo/pr/pool guards shared by every primitive,
// expecting the method's zero value on guard paths.
func durablePRGuards(want []any) []durableCase {
	return []durableCase{
		{name: "empty repo guard", prNumber: 42, want: want, wantNoDB: true},
		{name: "zero pr guard", repo: "azylman/aerial", want: want, wantNoDB: true},
		{name: "nil pool guard", repo: "azylman/aerial", prNumber: 42, nilPool: true, want: want, wantNoDB: true},
	}
}

// durableSHAGuards covers the repo/pr/sha/pool guards shared by every
// sha-keyed primitive, expecting the method's zero value on guard paths.
func durableSHAGuards(want []any) []durableCase {
	return []durableCase{
		{name: "empty repo guard", prNumber: 42, sha: "abc123", want: want, wantNoDB: true},
		{name: "zero pr guard", repo: "azylman/aerial", sha: "abc123", want: want, wantNoDB: true},
		{name: "empty sha guard", repo: "azylman/aerial", prNumber: 42, want: want, wantNoDB: true},
		{name: "zero sha guard", repo: "azylman/aerial", prNumber: 42, sha: "0000000", want: want, wantNoDB: true},
		{name: "nil pool guard", repo: "azylman/aerial", prNumber: 42, sha: "abc123", nilPool: true, want: want, wantNoDB: true},
	}
}

func TestPostgresPRRegistry_ClaimDeploySuccess(t *testing.T) {
	t.Parallel()
	tests := []durableCase{
		{name: "no matching row claims nothing", repo: "azylman/aerial", prNumber: 42, queryErr: pgx.ErrNoRows, want: []any{"", false}},
		{name: "database error", repo: "azylman/aerial", prNumber: 42, queryErr: errors.New("db down"), want: []any{"", false}, wantErr: true},
		{name: "empty repo guard", prNumber: 42, want: []any{"", false}, wantNoDB: true},
		{name: "zero pr guard", repo: "azylman/aerial", want: []any{"", false}, wantNoDB: true},
		{name: "nil pool guard", repo: "azylman/aerial", prNumber: 42, nilPool: true, want: []any{"", false}, wantNoDB: true},
	}
	runDurableCases(t, "ClaimDeploySuccess", tests, adaptClaimDeploySuccess)
}

func TestPostgresPRRegistry_AcknowledgeDeploySuccessPublished(t *testing.T) {
	t.Parallel()
	tests := append([]durableCase{
		{name: "database error", repo: "azylman/aerial", prNumber: 42, execErr: errors.New("db down"), wantErr: true},
	}, durablePRGuards(nil)...)
	runDurableCases(t, "AcknowledgeDeploySuccessPublished", tests, adaptAcknowledgeDeploySuccessPublished)
}

func TestPostgresPRRegistry_RecordFailedCheckRun(t *testing.T) {
	t.Parallel()
	tests := []durableCase{
		{name: "database error", repo: "azylman/aerial", prNumber: 42, sha: "abc123", checkKey: "unit-tests", queryErr: errors.New("db down"), want: []any{false, false, false}, wantErr: true},
		{name: "no matching row records nothing", repo: "azylman/aerial", prNumber: 42, sha: "abc123", checkKey: "unit-tests", queryErr: pgx.ErrNoRows, want: []any{false, false, false}},
		{name: "empty check key guard", repo: "azylman/aerial", prNumber: 42, sha: "abc123", want: []any{false, false, false}, wantNoDB: true},
		{name: "empty repo guard", prNumber: 42, sha: "abc123", checkKey: "unit-tests", want: []any{false, false, false}, wantNoDB: true},
		{name: "zero pr guard", repo: "azylman/aerial", sha: "abc123", checkKey: "unit-tests", want: []any{false, false, false}, wantNoDB: true},
		{name: "empty sha guard", repo: "azylman/aerial", prNumber: 42, checkKey: "unit-tests", want: []any{false, false, false}, wantNoDB: true},
		{name: "zero sha guard", repo: "azylman/aerial", prNumber: 42, sha: "0000000", checkKey: "unit-tests", want: []any{false, false, false}, wantNoDB: true},
		{name: "nil pool guard", repo: "azylman/aerial", prNumber: 42, sha: "abc123", checkKey: "unit-tests", nilPool: true, want: []any{false, false, false}, wantNoDB: true},
	}
	runDurableCases(t, "RecordFailedCheckRun", tests, adaptRecordFailedCheckRun)
}

func TestPostgresPRRegistry_RollbackFailedCheckRun(t *testing.T) {
	t.Parallel()
	tests := append([]durableCase{
		{name: "database error", repo: "azylman/aerial", prNumber: 42, sha: "abc123", checkKey: "unit-tests", execErr: errors.New("db down"), wantErr: true},
		{name: "empty check key guard", repo: "azylman/aerial", prNumber: 42, sha: "abc123", wantNoDB: true},
	}, durableSHAGuards(nil)...)
	runDurableCases(t, "RollbackFailedCheckRun", tests, adaptRollbackFailedCheckRun)
}

func TestPostgresPRRegistry_RollbackFailedCheckRunExec(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		checkKey  string
		roborev   bool
		was       bool
		hadPrior  bool
		execErr   error
		nilPool   bool
		wantCalls int
		wantErr   bool
	}{
		{name: "was active performs no exec", checkKey: "unit-tests", was: true, wantCalls: 0},
		{name: "rollback forwards hadPrior true", checkKey: "unit-tests", hadPrior: true, wantCalls: 1},
		{name: "rollback forwards hadPrior false", checkKey: "unit-tests", wantCalls: 1},
		{name: "roborev rollback still execs", checkKey: "roborev", roborev: true, wantCalls: 1},
		{name: "exec error surfaces", checkKey: "unit-tests", execErr: errors.New("db down"), wantCalls: 1, wantErr: true},
		{name: "nil pool skips exec", checkKey: "unit-tests", nilPool: true, wantCalls: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var execArgs [][]any
			pool := &mockSynapsePgxPool{execFn: func(_ context.Context, _ string, args ...any) (pgconn.CommandTag, error) {
				execArgs = append(execArgs, args)
				if tc.execErr != nil {
					return pgconn.CommandTag{}, tc.execErr
				}
				return pgconn.NewCommandTag("UPDATE 1"), nil
			}}
			reg := NewPostgresPRRegistry(pool)
			if tc.nilPool {
				reg = NewPostgresPRRegistry(nil)
			}
			err := reg.RollbackFailedCheckRun(context.Background(), "azylman/aerial", 42, "abc123", tc.checkKey, tc.roborev, tc.was, tc.hadPrior)
			if (err != nil) != tc.wantErr {
				t.Fatalf("RollbackFailedCheckRun() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if len(execArgs) != tc.wantCalls {
				t.Fatalf("RollbackFailedCheckRun() exec calls = %d, want %d", len(execArgs), tc.wantCalls)
			}
			if tc.wantCalls > 0 && !tc.wantErr {
				got, ok := execArgs[0][5].(bool)
				if !ok || got != tc.hadPrior {
					t.Errorf("RollbackFailedCheckRun() hadPrior arg = %v, want %v", execArgs[0][5], tc.hadPrior)
				}
			}
		})
	}
}

func TestPostgresPRRegistry_RecoverNonGateCheckSuccess(t *testing.T) {
	t.Parallel()
	tests := append([]durableCase{
		{name: "no matching row recovers nothing", repo: "azylman/aerial", prNumber: 42, sha: "abc123", checkKey: "unit-tests", queryErr: pgx.ErrNoRows, want: []any{false}},
		{name: "database error", repo: "azylman/aerial", prNumber: 42, sha: "abc123", checkKey: "unit-tests", queryErr: errors.New("db down"), want: []any{false}, wantErr: true},
		{name: "empty check key guard", repo: "azylman/aerial", prNumber: 42, sha: "abc123", want: []any{false}, wantNoDB: true},
	}, durableSHAGuards([]any{false})...)
	runDurableCases(t, "RecoverNonGateCheckSuccess", tests, adaptRecoverNonGateCheckSuccess)
}

func TestPostgresPRRegistry_RecordConflictPublished(t *testing.T) {
	t.Parallel()
	tests := append([]durableCase{
		{name: "database error", repo: "azylman/aerial", prNumber: 42, sha: "abc123", queryErr: errors.New("db down"), want: []any{false}, wantErr: true},
		{name: "already published is not new", repo: "azylman/aerial", prNumber: 42, sha: "abc123", queryErr: pgx.ErrNoRows, want: []any{false}},
	}, durableSHAGuards([]any{false})...)
	runDurableCases(t, "RecordConflictPublished", tests, adaptRecordConflictPublished)
}

func TestPostgresPRRegistry_WasConflictPublished(t *testing.T) {
	t.Parallel()
	tests := []durableCase{
		{name: "no row does not match", repo: "azylman/aerial", prNumber: 42, sha: "abc123", queryErr: pgx.ErrNoRows, want: []any{false}},
		{name: "database error", repo: "azylman/aerial", prNumber: 42, sha: "abc123", queryErr: errors.New("db down"), want: []any{false}, wantErr: true},
		{name: "empty repo guard", prNumber: 42, sha: "abc123", want: []any{false}, wantNoDB: true},
		{name: "zero pr guard", repo: "azylman/aerial", sha: "abc123", want: []any{false}, wantNoDB: true},
		{name: "empty sha guard", repo: "azylman/aerial", prNumber: 42, want: []any{false}, wantNoDB: true},
		{name: "nil pool guard", repo: "azylman/aerial", prNumber: 42, sha: "abc123", nilPool: true, want: []any{false}, wantNoDB: true},
	}
	runDurableCases(t, "WasConflictPublished", tests, adaptWasConflictPublished)
}

func TestPostgresPRRegistry_ClearConflictPublished(t *testing.T) {
	t.Parallel()
	tests := append([]durableCase{
		{name: "database error", repo: "azylman/aerial", prNumber: 42, execErr: errors.New("db down"), wantErr: true},
	}, durablePRGuards(nil)...)
	runDurableCases(t, "ClearConflictPublished", tests, adaptClearConflictPublished)
}

func TestPostgresPRRegistry_MarkCollaboratorAlerted(t *testing.T) {
	t.Parallel()
	tests := append([]durableCase{
		{name: "collab alert already alerted is not new", repo: "azylman/aerial", prNumber: 42, sha: "abc123", queryErr: pgx.ErrNoRows, want: []any{false}},
		{name: "collab alert database error", repo: "azylman/aerial", prNumber: 42, sha: "abc123", queryErr: errors.New("db down"), want: []any{false}, wantErr: true},
	}, durableSHAGuards([]any{false})...)
	runDurableCases(t, "MarkCollaboratorAlerted", tests, adaptMarkCollaboratorAlerted)
}

func TestPostgresPRRegistry_ClearCollaboratorAlerted(t *testing.T) {
	t.Parallel()
	tests := append([]durableCase{
		{name: "database error", repo: "azylman/aerial", prNumber: 42, execErr: errors.New("db down"), wantErr: true},
	}, durablePRGuards(nil)...)
	runDurableCases(t, "ClearCollaboratorAlerted", tests, adaptClearCollaboratorAlerted)
}

func TestPostgresPRRegistry_MarkCollaboratorConflict(t *testing.T) {
	t.Parallel()
	tests := append([]durableCase{
		{name: "database error", repo: "azylman/aerial", prNumber: 42, sha: "abc123", queryErr: errors.New("db down"), want: []any{false}, wantErr: true},
		{name: "already alerted is not new", repo: "azylman/aerial", prNumber: 42, sha: "abc123", queryErr: pgx.ErrNoRows, want: []any{false}},
	}, durableSHAGuards([]any{false})...)
	runDurableCases(t, "MarkCollaboratorConflict", tests, adaptMarkCollaboratorConflict)
}

func TestPostgresPRRegistry_ClearCollaboratorConflict(t *testing.T) {
	t.Parallel()
	tests := append([]durableCase{
		{name: "database error", repo: "azylman/aerial", prNumber: 42, execErr: errors.New("db down"), wantErr: true},
	}, durablePRGuards(nil)...)
	runDurableCases(t, "ClearCollaboratorConflict", tests, adaptClearCollaboratorConflict)
}

func TestPostgresPRRegistry_NoteMainPush(t *testing.T) {
	t.Parallel()
	tests := []durableCase{
		{name: "already seen push is not new", repo: "azylman/aerial", sha: "def456", queryErr: pgx.ErrNoRows, want: []any{false}},
		{name: "database error", repo: "azylman/aerial", sha: "def456", queryErr: errors.New("db down"), want: []any{false}, wantErr: true},
		{name: "empty repo guard", want: []any{false}, wantNoDB: true},
		{name: "empty sha guard", repo: "azylman/aerial", want: []any{false}, wantNoDB: true},
		{name: "zero sha guard", repo: "azylman/aerial", sha: "0000000", want: []any{false}, wantNoDB: true},
		{name: "nil pool guard", repo: "azylman/aerial", sha: "def456", nilPool: true, want: []any{false}, wantNoDB: true},
	}
	runDurableCases(t, "NoteMainPush", tests, adaptNoteMainPush)
}

func TestPostgresPRRegistry_LoadMainPushSHA(t *testing.T) {
	t.Parallel()
	tests := []durableCase{
		{name: "database error", repo: "azylman/aerial", queryErr: errors.New("db down"), want: []any{""}, wantErr: true},
		{name: "no push recorded returns empty", repo: "azylman/aerial", queryErr: pgx.ErrNoRows, want: []any{""}},
		{name: "empty repo guard", want: []any{""}, wantNoDB: true},
		{name: "nil pool guard", repo: "azylman/aerial", nilPool: true, want: []any{""}, wantNoDB: true},
	}
	runDurableCases(t, "LoadMainPushSHA", tests, adaptLoadMainPushSHA)
}

func TestPostgresPRRegistry_MarkUpdatedForMain(t *testing.T) {
	t.Parallel()
	tests := append([]durableCase{
		{name: "database error", repo: "azylman/aerial", prNumber: 42, sha: "def456", execErr: errors.New("db down"), wantErr: true},
	}, durableSHAGuards(nil)...)
	runDurableCases(t, "MarkUpdatedForMain", tests, adaptMarkUpdatedForMain)
}

func TestPostgresPRRegistry_WasUpdatedForMain(t *testing.T) {
	t.Parallel()
	tests := []durableCase{
		{name: "flag present matches empty sha", repo: "azylman/aerial", prNumber: 42, scan: []any{"def456"}, want: []any{true}},
		{name: "updated for older sha", repo: "azylman/aerial", prNumber: 42, sha: "abc123", scan: []any{"def456"}, want: []any{false}},
		{name: "no row", repo: "azylman/aerial", prNumber: 42, sha: "def456", queryErr: pgx.ErrNoRows, want: []any{false}},
		{name: "database error", repo: "azylman/aerial", prNumber: 42, sha: "def456", queryErr: errors.New("db down"), want: []any{false}, wantErr: true},
		{name: "empty repo guard", prNumber: 42, sha: "def456", want: []any{false}, wantNoDB: true},
		{name: "zero pr guard", repo: "azylman/aerial", sha: "def456", want: []any{false}, wantNoDB: true},
		{name: "nil pool guard", repo: "azylman/aerial", prNumber: 42, sha: "def456", nilPool: true, want: []any{false}, wantNoDB: true},
	}
	runDurableCases(t, "WasUpdatedForMain", tests, adaptWasUpdatedForMain)
}

func TestPostgresPRRegistry_ClearUpdatedForMain(t *testing.T) {
	t.Parallel()
	tests := append([]durableCase{
		{name: "database error", repo: "azylman/aerial", prNumber: 42, execErr: errors.New("db down"), wantErr: true},
	}, durablePRGuards(nil)...)
	runDurableCases(t, "ClearUpdatedForMain", tests, adaptClearUpdatedForMain)
}

func adaptClaimDeploySuccess(ctx context.Context, reg *PostgresPRRegistry, tc durableCase) ([]any, error) {
	target, claimed, err := reg.ClaimDeploySuccess(ctx, tc.repo, tc.prNumber)
	return []any{target, claimed}, err
}

func adaptAcknowledgeDeploySuccessPublished(ctx context.Context, reg *PostgresPRRegistry, tc durableCase) ([]any, error) {
	return nil, reg.AcknowledgeDeploySuccessPublished(ctx, tc.repo, tc.prNumber)
}

func adaptRecordFailedCheckRun(ctx context.Context, reg *PostgresPRRegistry, tc durableCase) ([]any, error) {
	emit, was, had, err := reg.RecordFailedCheckRun(ctx, tc.repo, tc.prNumber, tc.sha, tc.checkKey, tc.roborev)
	return []any{emit, was, had}, err
}

func adaptRollbackFailedCheckRun(ctx context.Context, reg *PostgresPRRegistry, tc durableCase) ([]any, error) {
	return nil, reg.RollbackFailedCheckRun(ctx, tc.repo, tc.prNumber, tc.sha, tc.checkKey, tc.roborev, tc.was, tc.hadPrior)
}

func adaptRecoverNonGateCheckSuccess(ctx context.Context, reg *PostgresPRRegistry, tc durableCase) ([]any, error) {
	recovered, err := reg.RecoverNonGateCheckSuccess(ctx, tc.repo, tc.prNumber, tc.sha, tc.checkKey)
	return []any{recovered}, err
}

func adaptRecordConflictPublished(ctx context.Context, reg *PostgresPRRegistry, tc durableCase) ([]any, error) {
	isNew, err := reg.RecordConflictPublished(ctx, tc.repo, tc.prNumber, tc.sha)
	return []any{isNew}, err
}

func adaptWasConflictPublished(ctx context.Context, reg *PostgresPRRegistry, tc durableCase) ([]any, error) {
	published, err := reg.WasConflictPublished(ctx, tc.repo, tc.prNumber, tc.sha)
	return []any{published}, err
}

func adaptClearConflictPublished(ctx context.Context, reg *PostgresPRRegistry, tc durableCase) ([]any, error) {
	return nil, reg.ClearConflictPublished(ctx, tc.repo, tc.prNumber)
}

func adaptMarkCollaboratorAlerted(ctx context.Context, reg *PostgresPRRegistry, tc durableCase) ([]any, error) {
	isNew, err := reg.MarkCollaboratorAlerted(ctx, tc.repo, tc.prNumber, tc.sha)
	return []any{isNew}, err
}

func adaptClearCollaboratorAlerted(ctx context.Context, reg *PostgresPRRegistry, tc durableCase) ([]any, error) {
	return nil, reg.ClearCollaboratorAlerted(ctx, tc.repo, tc.prNumber)
}

func adaptMarkCollaboratorConflict(ctx context.Context, reg *PostgresPRRegistry, tc durableCase) ([]any, error) {
	isNew, err := reg.MarkCollaboratorConflict(ctx, tc.repo, tc.prNumber, tc.sha)
	return []any{isNew}, err
}

func adaptClearCollaboratorConflict(ctx context.Context, reg *PostgresPRRegistry, tc durableCase) ([]any, error) {
	return nil, reg.ClearCollaboratorConflict(ctx, tc.repo, tc.prNumber)
}

func adaptNoteMainPush(ctx context.Context, reg *PostgresPRRegistry, tc durableCase) ([]any, error) {
	isNew, err := reg.NoteMainPush(ctx, tc.repo, tc.sha)
	return []any{isNew}, err
}

func adaptLoadMainPushSHA(ctx context.Context, reg *PostgresPRRegistry, tc durableCase) ([]any, error) {
	sha, err := reg.LoadMainPushSHA(ctx, tc.repo)
	return []any{sha}, err
}

func adaptMarkUpdatedForMain(ctx context.Context, reg *PostgresPRRegistry, tc durableCase) ([]any, error) {
	return nil, reg.MarkUpdatedForMain(ctx, tc.repo, tc.prNumber, tc.sha)
}

func adaptWasUpdatedForMain(ctx context.Context, reg *PostgresPRRegistry, tc durableCase) ([]any, error) {
	updated, err := reg.WasUpdatedForMain(ctx, tc.repo, tc.prNumber, tc.sha)
	return []any{updated}, err
}

func adaptClearUpdatedForMain(ctx context.Context, reg *PostgresPRRegistry, tc durableCase) ([]any, error) {
	return nil, reg.ClearUpdatedForMain(ctx, tc.repo, tc.prNumber)
}

// queryRegistryString reads one scalar string from the fixture row.
func queryRegistryString(t *testing.T, conn *pgx.Conn, query string) string {
	t.Helper()
	var value string
	if err := conn.QueryRow(context.Background(), query).Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

// TestPostgresPRRegistry_DurableClaimRoundTrip exercises the deploy claim and
// ack against real SQL semantics: exactly-one first claim with at-most-once
// recovery republish, the terminal merged to deployed transition, and ack
// clearing the claim flags. It requires a disposable PostgreSQL fixture via
// -registry-test-dsn and skips without one, mirroring
// TestPostgresPRRegistry_Recovery.
func TestPostgresPRRegistry_DurableClaimRoundTrip(t *testing.T) {
	t.Parallel()
	if *registryTestDSN == "" {
		t.Skip("requires disposable PostgreSQL fixture via -registry-test-dsn")
	}
	ctx := context.Background()
	conn := newRecoveryFixture(t, "deployed", `{"deploy_success_pending":true}`)
	reg := NewPostgresPRRegistry(conn)
	prs, listErr := reg.ListDeployingPRs(ctx)
	if listErr != nil || len(prs) != 1 {
		t.Fatalf("pre-claim list = %d, err = %v", len(prs), listErr)
	}
	_, firstClaim, firstErr := reg.ClaimDeploySuccess(ctx, "example/project", 42)
	if firstErr != nil || !firstClaim {
		t.Fatalf("first claim = %v, err = %v", firstClaim, firstErr)
	}
	_, secondClaim, secondErr := reg.ClaimDeploySuccess(ctx, "example/project", 42)
	if secondErr != nil || secondClaim {
		t.Fatalf("recovery reclaim = %v, err = %v", secondClaim, secondErr)
	}
	pending := queryRegistryString(t, conn, `SELECT COALESCE(metadata->>'deploy_success_pending','') FROM pr_registry`)
	claimed := queryRegistryString(t, conn, `SELECT COALESCE(metadata->>'deploy_success_claimed','') FROM pr_registry`)
	if pending != "true" || claimed != "true" {
		t.Fatalf("claim flags = %q, %q", pending, claimed)
	}
	if _, execErr := conn.Exec(ctx, `UPDATE pr_registry SET metadata = metadata - 'deploy_success_pending'`); execErr != nil {
		t.Fatal(execErr)
	}
	prs, listErr = reg.ListDeployingPRs(ctx)
	if listErr != nil || len(prs) != 1 {
		t.Fatalf("claimed-only list = %d, err = %v", len(prs), listErr)
	}
	if _, second, err := reg.ClaimDeploySuccess(ctx, "example/project", 42); err != nil || second {
		t.Fatalf("claimed-only reclaim = %v, err = %v", second, err)
	}
	if pending := queryRegistryString(t, conn, `SELECT COALESCE(metadata->>'deploy_success_pending','') FROM pr_registry`); pending != "true" {
		t.Fatalf("re-claim did not re-arm pending: %q", pending)
	}
	if ackErr := reg.AcknowledgeDeploySuccessPublished(ctx, "example/project", 42); ackErr != nil {
		t.Fatal(ackErr)
	}
	if flags := queryRegistryString(t, conn, `SELECT COALESCE(metadata->>'deploy_success_pending','') || COALESCE(metadata->>'deploy_success_claimed','') FROM pr_registry`); flags != "" {
		t.Fatalf("claim flags persist after ack: %q", flags)
	}
	prs, listErr = reg.ListDeployingPRs(ctx)
	if listErr != nil || len(prs) != 0 {
		t.Fatalf("post-ack list = %d, err = %v", len(prs), listErr)
	}
}

// TestPostgresPRRegistry_DurableClaimMergesRow verifies the claim performs
// the terminal merged to deployed transition itself for unowned/no-deploy
// paths. It requires a disposable PostgreSQL fixture via -registry-test-dsn
// and skips without one.
func TestPostgresPRRegistry_DurableClaimMergesRow(t *testing.T) {
	t.Parallel()
	if *registryTestDSN == "" {
		t.Skip("requires disposable PostgreSQL fixture via -registry-test-dsn")
	}
	ctx := context.Background()
	conn := newRecoveryFixture(t, "merged", `{}`)
	reg := NewPostgresPRRegistry(conn)
	if _, first, err := reg.ClaimDeploySuccess(ctx, "example/project", 42); err != nil || !first {
		t.Fatalf("merged claim = %v, err = %v", first, err)
	}
	status := queryRegistryString(t, conn, `SELECT status FROM pr_registry`)
	if status != "deployed" {
		t.Fatalf("status = %s, want deployed", status)
	}
	if pending := queryRegistryString(t, conn, `SELECT COALESCE(metadata->>'deploy_success_pending','') FROM pr_registry`); pending != "true" {
		t.Fatalf("pending = %q, want true", pending)
	}
}

// TestPostgresPRRegistry_DurableCheckRunsRoundTrip exercises check_runs
// record, recover, generation reset, and rollback semantics against real SQL.
// It requires a disposable PostgreSQL fixture via -registry-test-dsn and
// skips without one.
func TestPostgresPRRegistry_DurableCheckRunsRoundTrip(t *testing.T) {
	t.Parallel()
	if *registryTestDSN == "" {
		t.Skip("requires disposable PostgreSQL fixture via -registry-test-dsn")
	}
	ctx := context.Background()
	conn := newRecoveryFixture(t, "open", `{}`)
	reg := NewPostgresPRRegistry(conn)
	record := func(sha, key string, roborev bool) (bool, bool, bool) {
		t.Helper()
		emit, was, had, err := reg.RecordFailedCheckRun(ctx, "example/project", 42, sha, key, roborev)
		if err != nil {
			t.Fatal(err)
		}
		return emit, was, had
	}
	if emit, was, had := record("abc123", "unit-tests", false); emit || was || had {
		t.Fatalf("first record = (%v,%v,%v)", emit, was, had)
	}
	if emit, was, had := record("abc123", "unit-tests", false); emit || !was || !had {
		t.Fatalf("repeat record = (%v,%v,%v)", emit, was, had)
	}
	if emit, was, had := record("abc123", "other", false); !emit || was || had {
		t.Fatalf("second check record = (%v,%v,%v)", emit, was, had)
	}
	clearCheck := func(sha, key string) bool {
		t.Helper()
		recovered, err := reg.RecoverNonGateCheckSuccess(ctx, "example/project", 42, sha, key)
		if err != nil {
			t.Fatal(err)
		}
		return recovered
	}
	if clearCheck("abc123", "other") {
		t.Fatal("partial recover returned true")
	}
	if !clearCheck("abc123", "unit-tests") {
		t.Fatal("full recover returned false")
	}
}

// TestPostgresPRRegistry_DurableCheckRunsGenerationRoundTrip verifies a new
// generation starts clean and a mismatched-SHA rollback leaves the active
// generation intact, against real SQL. It requires a disposable PostgreSQL
// fixture via -registry-test-dsn and skips without one.
func TestPostgresPRRegistry_DurableCheckRunsGenerationRoundTrip(t *testing.T) {
	t.Parallel()
	if *registryTestDSN == "" {
		t.Skip("requires disposable PostgreSQL fixture via -registry-test-dsn")
	}
	ctx := context.Background()
	conn := newRecoveryFixture(t, "open", `{}`)
	reg := NewPostgresPRRegistry(conn)
	record := func(sha, key string, roborev bool) (bool, bool, bool) {
		t.Helper()
		emit, was, had, err := reg.RecordFailedCheckRun(ctx, "example/project", 42, sha, key, roborev)
		if err != nil {
			t.Fatal(err)
		}
		return emit, was, had
	}
	if emit, was, had := record("def456", "unit-tests", false); emit || was || had {
		t.Fatalf("new generation record = (%v,%v,%v)", emit, was, had)
	}
	if rollbackErr := reg.RollbackFailedCheckRun(ctx, "example/project", 42, "ghi789", "unit-tests", false, false, false); rollbackErr != nil {
		t.Fatal(rollbackErr)
	}
	if emit, was, had := record("def456", "unit-tests", false); emit || !was || !had {
		t.Fatalf("post-mismatched-rollback record = (%v,%v,%v)", emit, was, had)
	}
}

// TestPostgresPRRegistry_DurableRollbackRoundTrip verifies rollback undoes a
// record while a wasActive rollback keeps the failure, against real SQL. It
// requires a disposable PostgreSQL fixture via -registry-test-dsn and skips
// without one.
func TestPostgresPRRegistry_DurableRollbackRoundTrip(t *testing.T) {
	t.Parallel()
	if *registryTestDSN == "" {
		t.Skip("requires disposable PostgreSQL fixture via -registry-test-dsn")
	}
	ctx := context.Background()
	firstConn := newRecoveryFixture(t, "open", `{}`)
	reg := NewPostgresPRRegistry(firstConn)
	if _, _, _, err := reg.RecordFailedCheckRun(ctx, "example/project", 42, "abc123", "unit-tests", false); err != nil {
		t.Fatal(err)
	}
	if err := reg.RollbackFailedCheckRun(ctx, "example/project", 42, "abc123", "unit-tests", false, false, false); err != nil {
		t.Fatal(err)
	}
	if emit, was, had, err := reg.RecordFailedCheckRun(ctx, "example/project", 42, "abc123", "unit-tests", false); err != nil || emit || was || had {
		t.Fatalf("post-rollback record = (%v,%v,%v), err = %v", emit, was, had, err)
	}
	secondConn := newRecoveryFixture(t, "open", `{}`)
	reg = NewPostgresPRRegistry(secondConn)
	if _, _, _, err := reg.RecordFailedCheckRun(ctx, "example/project", 42, "abc123", "unit-tests", false); err != nil {
		t.Fatal(err)
	}
	if err := reg.RollbackFailedCheckRun(ctx, "example/project", 42, "abc123", "unit-tests", false, true, true); err != nil {
		t.Fatal(err)
	}
	if emit, was, had, err := reg.RecordFailedCheckRun(ctx, "example/project", 42, "abc123", "unit-tests", false); err != nil || emit || !was || !had {
		t.Fatalf("post-noop-rollback record = (%v,%v,%v), err = %v", emit, was, had, err)
	}
	thirdConn := newRecoveryFixture(t, "open", `{}`)
	reg = NewPostgresPRRegistry(thirdConn)
	if _, _, _, err := reg.RecordFailedCheckRun(ctx, "example/project", 42, "abc123", "roborev", true); err != nil {
		t.Fatal(err)
	}
	if err := reg.RollbackFailedCheckRun(ctx, "example/project", 42, "abc123", "roborev", true, false, false); err != nil {
		t.Fatal(err)
	}
	if emit, was, had, err := reg.RecordFailedCheckRun(ctx, "example/project", 42, "abc123", "roborev", true); err != nil || emit || was || had {
		t.Fatalf("post-roborev-rollback record = (%v,%v,%v), err = %v", emit, was, had, err)
	}
}

// TestPostgresPRRegistry_DurableDedupRoundTrip exercises conflict publish
// dedup and clearing against real SQL. It requires a disposable PostgreSQL
// fixture via -registry-test-dsn and skips without one.
func TestPostgresPRRegistry_DurableDedupRoundTrip(t *testing.T) {
	t.Parallel()
	if *registryTestDSN == "" {
		t.Skip("requires disposable PostgreSQL fixture via -registry-test-dsn")
	}
	ctx := context.Background()
	conn := newRecoveryFixture(t, "open", `{}`)
	reg := NewPostgresPRRegistry(conn)
	if isNew, err := reg.RecordConflictPublished(ctx, "example/project", 42, "abc123"); err != nil || !isNew {
		t.Fatalf("first conflict publish = %v, err = %v", isNew, err)
	}
	if isNew, err := reg.RecordConflictPublished(ctx, "example/project", 42, "abc123"); err != nil || isNew {
		t.Fatalf("duplicate conflict publish = %v, err = %v", isNew, err)
	}
	if was, err := reg.WasConflictPublished(ctx, "example/project", 42, "abc123"); err != nil || !was {
		t.Fatalf("was conflict published = %v, err = %v", was, err)
	}
	if err := reg.ClearConflictPublished(ctx, "example/project", 42); err != nil {
		t.Fatal(err)
	}
	if was, err := reg.WasConflictPublished(ctx, "example/project", 42, "abc123"); err != nil || was {
		t.Fatalf("was conflict published after clear = %v, err = %v", was, err)
	}
}

// TestPostgresPRRegistry_DurableCollaboratorDedupRoundTrip exercises
// collaborator alert and conflict dedup plus clearing against real SQL. It
// requires a disposable PostgreSQL fixture via -registry-test-dsn and skips
// without one.
func TestPostgresPRRegistry_DurableCollaboratorDedupRoundTrip(t *testing.T) {
	t.Parallel()
	if *registryTestDSN == "" {
		t.Skip("requires disposable PostgreSQL fixture via -registry-test-dsn")
	}
	ctx := context.Background()
	conn := newRecoveryFixture(t, "open", `{}`)
	reg := NewPostgresPRRegistry(conn)
	if isNew, err := reg.MarkCollaboratorAlerted(ctx, "example/project", 42, "abc123"); err != nil || !isNew {
		t.Fatalf("first collaborator alert = %v, err = %v", isNew, err)
	}
	if isNew, err := reg.MarkCollaboratorAlerted(ctx, "example/project", 42, "abc123"); err != nil || isNew {
		t.Fatalf("duplicate collaborator alert = %v, err = %v", isNew, err)
	}
	if err := reg.ClearCollaboratorAlerted(ctx, "example/project", 42); err != nil {
		t.Fatal(err)
	}
	if sha := queryRegistryString(t, conn, `SELECT COALESCE(metadata->>'collaborator_alerted_sha','') FROM pr_registry`); sha != "" {
		t.Fatalf("collaborator alerted sha persists after clear: %q", sha)
	}
	if isNew, err := reg.MarkCollaboratorConflict(ctx, "example/project", 42, "abc123"); err != nil || !isNew {
		t.Fatalf("first collaborator conflict = %v, err = %v", isNew, err)
	}
	if isNew, err := reg.MarkCollaboratorConflict(ctx, "example/project", 42, "abc123"); err != nil || isNew {
		t.Fatalf("duplicate collaborator conflict = %v, err = %v", isNew, err)
	}
	if err := reg.ClearCollaboratorConflict(ctx, "example/project", 42); err != nil {
		t.Fatal(err)
	}
	if sha := queryRegistryString(t, conn, `SELECT COALESCE(metadata->>'collaborator_conflict_sha','') FROM pr_registry`); sha != "" {
		t.Fatalf("collaborator conflict sha persists after clear: %q", sha)
	}
}

// TestPostgresPRRegistry_DurableMainPushRoundTrip exercises main-push dedup
// and the updated-for-main flag against real SQL. It requires a disposable
// PostgreSQL fixture via -registry-test-dsn and skips without one.
func TestPostgresPRRegistry_DurableMainPushRoundTrip(t *testing.T) {
	t.Parallel()
	if *registryTestDSN == "" {
		t.Skip("requires disposable PostgreSQL fixture via -registry-test-dsn")
	}
	ctx := context.Background()
	conn := newRecoveryFixture(t, "open", `{}`)
	reg := NewPostgresPRRegistry(conn)
	notePush := func(sha string) bool {
		t.Helper()
		isNew, err := reg.NoteMainPush(ctx, "example/project", sha)
		if err != nil {
			t.Fatal(err)
		}
		return isNew
	}
	if !notePush("def456") || notePush("def456") {
		t.Fatal("main push dedup failed")
	}
	if sha, err := reg.LoadMainPushSHA(ctx, "example/project"); err != nil || sha != "def456" {
		t.Fatalf("latest main sha = %q, err = %v", sha, err)
	}
	if err := reg.MarkUpdatedForMain(ctx, "example/project", 42, "def456"); err != nil {
		t.Fatal(err)
	}
	if was, err := reg.WasUpdatedForMain(ctx, "example/project", 42, "def456"); err != nil || !was {
		t.Fatalf("was updated for main = %v, err = %v", was, err)
	}
	if !notePush("abc999") {
		t.Fatal("new main push did not win")
	}
	if was, err := reg.WasUpdatedForMain(ctx, "example/project", 42, "def456"); err != nil || was {
		t.Fatalf("was updated for stale main = %v, err = %v", was, err)
	}
	if sha, err := reg.LoadMainPushSHA(ctx, "example/project"); err != nil || sha != "abc999" {
		t.Fatalf("latest main sha after push = %q, err = %v", sha, err)
	}
	if err := reg.MarkUpdatedForMain(ctx, "example/project", 42, "zzz999"); err != nil {
		t.Fatal(err)
	}
	if err := reg.ClearUpdatedForMain(ctx, "example/project", 42); err != nil {
		t.Fatal(err)
	}
	if was, err := reg.WasUpdatedForMain(ctx, "example/project", 42, "zzz999"); err != nil || was {
		t.Fatalf("was updated after clear = %v, err = %v", was, err)
	}
}

// TestPostgresPRRegistry_DurableMainPushRepoScope verifies main-push
// seen-state lives at repo granularity: a push with no PR rows is still
// recorded, and redelivery after a new PR row registers is a no-op that
// leaves per-PR flags untouched. It requires a disposable PostgreSQL fixture
// via -registry-test-dsn and skips without one.
func TestPostgresPRRegistry_DurableMainPushRepoScope(t *testing.T) {
	t.Parallel()
	if *registryTestDSN == "" {
		t.Skip("requires disposable PostgreSQL fixture via -registry-test-dsn")
	}
	ctx := context.Background()
	emptyConn := newRecoveryFixture(t, "open", `{}`)
	emptyReg := NewPostgresPRRegistry(emptyConn)
	if _, err := emptyConn.Exec(ctx, `DELETE FROM pr_registry`); err != nil {
		t.Fatal(err)
	}
	if isNew, err := emptyReg.NoteMainPush(ctx, "example/project", "def456"); err != nil || !isNew {
		t.Fatalf("zero-row push = %v, err = %v", isNew, err)
	}
	if sha, err := emptyReg.LoadMainPushSHA(ctx, "example/project"); err != nil || sha != "def456" {
		t.Fatalf("latest main sha with no PR rows = %q, err = %v", sha, err)
	}
	conn := newRecoveryFixture(t, "open", `{}`)
	reg := NewPostgresPRRegistry(conn)
	if isNew, err := reg.NoteMainPush(ctx, "example/project", "def456"); err != nil || !isNew {
		t.Fatalf("first push = %v, err = %v", isNew, err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO pr_registry (repo, pr_number, status, metadata) VALUES ('example/project', 43, 'open', '{}')`); err != nil {
		t.Fatal(err)
	}
	if isNew, err := reg.NoteMainPush(ctx, "example/project", "def456"); err != nil || isNew {
		t.Fatalf("redelivered push after new row = %v, err = %v", isNew, err)
	}
	if sha, err := reg.LoadMainPushSHA(ctx, "example/project"); err != nil || sha != "def456" {
		t.Fatalf("latest main sha after redelivery = %q, err = %v", sha, err)
	}
}

// TestPostgresPRRegistry_DurableGenerationReset verifies a new gate
// generation and a readiness retry clear the deploy claim flags against real
// SQL. It requires a disposable PostgreSQL fixture via -registry-test-dsn
// and skips without one.
func TestPostgresPRRegistry_DurableGenerationReset(t *testing.T) {
	t.Parallel()
	if *registryTestDSN == "" {
		t.Skip("requires disposable PostgreSQL fixture via -registry-test-dsn")
	}
	ctx := context.Background()
	seedClaimed := func() *pgx.Conn {
		t.Helper()
		return newRecoveryFixture(t, "deployed", `{"deploy_success_pending":true,"deploy_success_claimed":true,"kustomization_requests":{"one":{}}}`)
	}
	assertFlagsCleared := func(conn *pgx.Conn, op string) {
		t.Helper()
		var flags string
		if err := conn.QueryRow(ctx, `SELECT COALESCE(metadata->>'deploy_success_pending','') || COALESCE(metadata->>'deploy_success_claimed','') FROM pr_registry`).Scan(&flags); err != nil {
			t.Fatal(err)
		}
		if flags != "" {
			t.Fatalf("claim flags persist after %s: %q", op, flags)
		}
	}
	conn := seedClaimed()
	reg := NewPostgresPRRegistry(conn)
	if err := reg.RecordKustomizationRequests(ctx, "example/project", 42, map[string]model.KustomizationRequest{"two": {Namespace: "flux-system", RequestedAt: "t2"}}); err != nil {
		t.Fatal(err)
	}
	assertFlagsCleared(conn, "new generation")
	conn = seedClaimed()
	if _, err := conn.Exec(ctx, `UPDATE pr_registry SET merge_sha='abc'`); err != nil {
		t.Fatal(err)
	}
	reg = NewPostgresPRRegistry(conn)
	if _, err := reg.RetryPRReadiness(ctx, "example/project", "abc", 42); err != nil {
		t.Fatal(err)
	}
	assertFlagsCleared(conn, "readiness retry")
}
