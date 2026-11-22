package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/pkg/pgutil"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// TestMain runs the package against one throwaway Postgres 18 server loaded
// from the production schema; every test clones its own database.
func TestMain(m *testing.M) {
	os.Exit(pgtest.Main(m))
}

// testOwner is the configured GitHub owner every test store uses.
const testOwner = "example-org"

// failFunctionSQL defines the trigger function the sabotage helpers attach.
const failFunctionSQL = `CREATE OR REPLACE FUNCTION pgtest_fail() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE EXCEPTION 'sabotaged % on %', TG_OP, TG_TABLE_NAME;
END $$;`

// failStatement returns DDL that makes every event (INSERT, UPDATE, DELETE)
// on table fail immediately inside the test's private database.
func failStatement(table, event string) string {
	return failFunctionSQL + fmt.Sprintf(` CREATE TRIGGER pgtest_fail_%[1]s_%[2]s BEFORE %[2]s ON %[1]s FOR EACH ROW EXECUTE FUNCTION pgtest_fail();`, table, event)
}

// failCommit returns DDL that lets event on table succeed but makes the
// enclosing transaction's COMMIT fail (a deferred constraint trigger).
func failCommit(table, event string) string {
	return failFunctionSQL + fmt.Sprintf(` CREATE CONSTRAINT TRIGGER pgtest_commit_%[1]s_%[2]s AFTER %[2]s ON %[1]s DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pgtest_fail();`, table, event)
}

// failChunkRowsSQL swaps vanadis_chunks for a view that raises while
// streaming any row whose banner is "boom": statements still prepare and
// earlier rows still arrive, so the failure surfaces from rows.Err().
const failChunkRowsSQL = `CREATE FUNCTION pgtest_boom(b text) RETURNS text LANGUAGE plpgsql VOLATILE AS $$
BEGIN
	IF b = 'boom' THEN RAISE EXCEPTION 'sabotaged row'; END IF;
	RETURN b;
END $$;
ALTER TABLE vanadis_chunks RENAME TO vanadis_chunks_base;
CREATE VIEW vanadis_chunks AS SELECT id, build_id, chunk_index, mode, repo, title, instructions, target_files, target_symbols, effort, acceptance_criteria, status, pr_number, stall_nudges, depends_on, ci_fail_count, ci_checks, rollout_state, pgtest_boom(banner) AS banner, merge_awakeable_id, deploy_awakeable_id, started_at, completed_at, created_at, updated_at FROM vanadis_chunks_base;`

// vanishChunkOnBuildReadSQL swaps vanadis_builds for a view whose id column
// deletes chunk c1 as a side effect. The statement that locks the parent
// still sees c1 (its snapshot predates the delete), but every later statement
// in the transaction finds c1 gone: the "chunk removed after the parent lock"
// race, reproduced deterministically.
const vanishChunkOnBuildReadSQL = `CREATE FUNCTION pgtest_vanish(v text) RETURNS text LANGUAGE plpgsql VOLATILE AS $$
BEGIN
	DELETE FROM vanadis_chunks WHERE id = 'c1';
	RETURN v;
END $$;
ALTER TABLE vanadis_builds RENAME TO vanadis_builds_base;
CREATE VIEW vanadis_builds AS SELECT pgtest_vanish(id) AS id, agent, thread_id, tier, title, objective, shared_context, status, status_message_id, quiet, created_at, updated_at FROM vanadis_builds_base;`

// breakChunkScanSQL retypes a chunk array column so rows still stream but
// scanning them into []string fails.
const breakChunkScanSQL = `ALTER TABLE vanadis_chunks ALTER COLUMN target_files TYPE text USING array_to_string(target_files, ',');`

// openStore opens a private cloned database through the production pool
// constructor and returns the Store plus the pool for seeding and reads.
func openStore(t *testing.T) (*Store, *pgxpool.Pool) {
	t.Helper()
	pool, err := pgutil.NewPool(context.Background(), pgtest.NewDSN(t), pgutil.PoolOptions{MaxConns: 10, Ping: true})
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return New(pool, testOwner), pool
}

// closedStore returns a Store whose pool is already closed, so Begin, Query,
// QueryRow and Exec all fail.
func closedStore(t *testing.T) *Store {
	t.Helper()
	st, pool := openStore(t)
	pool.Close()
	return st
}

// mustExec runs setup or sabotage SQL in the test's private database.
func mustExec(t *testing.T, pool *pgxpool.Pool, sql string) {
	t.Helper()
	if sql == "" {
		return
	}
	if _, err := pool.Exec(context.Background(), sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

// seedBuild inserts b; an empty status means running and an empty objective
// gets a placeholder so the NOT NULL column holds.
func seedBuild(t *testing.T, pool *pgxpool.Pool, b model.VanadisBuild) {
	t.Helper()
	if b.Status == "" {
		b.Status = "running"
	}
	if b.Objective == "" {
		b.Objective = "Objective"
	}
	if b.Agent == "" {
		b.Agent = "aerial"
	}
	if b.Tier == 0 {
		b.Tier = 2
	}
	_, err := pool.Exec(context.Background(), `INSERT INTO vanadis_builds (id, agent, thread_id, tier, title, objective, shared_context, status, status_message_id, quiet) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		b.ID, b.Agent, b.ThreadID, b.Tier, b.Title, b.Objective, b.SharedContext, b.Status, b.StatusMessageID, b.Quiet)
	if err != nil {
		t.Fatalf("seed build %s: %v", b.ID, err)
	}
}

// seedChunk inserts c. Empty mode means pr, a pr chunk without repo gets
// "aerial", an empty status means pending and a zero PR number is NULL. The
// SHA fields fold into ci_checks the way production stores them.
func seedChunk(t *testing.T, pool *pgxpool.Pool, c model.VanadisChunk) {
	t.Helper()
	if c.Mode == "" {
		c.Mode = "pr"
	}
	if c.Mode == "pr" && c.Repo == "" {
		c.Repo = "aerial"
	}
	if c.Status == "" {
		c.Status = "pending"
	}
	if c.Effort == "" {
		c.Effort = "medium"
	}
	checks := map[string]string{}
	maps.Copy(checks, c.CIChecks)
	for k, v := range map[string]string{"_head_sha": c.HeadSHA, "_merge_sha": c.MergeSHA, "_remediated_sha": c.RemediatedSHA} {
		if v != "" {
			checks[k] = v
		}
	}
	rawChecks, err := json.Marshal(checks)
	if err != nil {
		t.Fatalf("marshal checks: %v", err)
	}
	rawRollout := []byte("{}")
	if c.RolloutState != nil {
		if rawRollout, err = json.Marshal(c.RolloutState); err != nil {
			t.Fatalf("marshal rollout: %v", err)
		}
	}
	_, err = pool.Exec(context.Background(), `INSERT INTO vanadis_chunks (id, build_id, chunk_index, mode, repo, title, instructions, target_files, target_symbols, effort, acceptance_criteria, status, pr_number, stall_nudges, depends_on, ci_fail_count, ci_checks, rollout_state, banner, merge_awakeable_id, deploy_awakeable_id, started_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NULLIF($13, 0), $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)`,
		c.ID, c.BuildID, c.ChunkIndex, c.Mode, c.Repo, c.Title, c.Instructions, nonNil(c.TargetFiles), nonNil(c.TargetSymbols), c.Effort, nonNil(c.AcceptanceCriteria), c.Status,
		c.PRNumber, c.StallNudges, nonNil(c.DependsOn), c.CIFailCount, rawChecks, rawRollout, c.Banner, c.MergeAwakeableID, c.DeployAwakeableID, nullTime(c.StartedAt), nullTime(c.CompletedAt))
	if err != nil {
		t.Fatalf("seed chunk %s: %v", c.ID, err)
	}
}

// seed inserts builds then chunks.
func seed(t *testing.T, pool *pgxpool.Pool, builds []model.VanadisBuild, chunks []model.VanadisChunk) {
	t.Helper()
	for _, b := range builds {
		seedBuild(t, pool, b)
	}
	for _, c := range chunks {
		seedChunk(t, pool, c)
	}
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func nullTime(ts time.Time) *time.Time {
	if ts.IsZero() {
		return nil
	}
	return &ts
}

// loadChunk reads one stored chunk through the production scanner.
func loadChunk(t *testing.T, pool *pgxpool.Pool, id string) model.VanadisChunk {
	t.Helper()
	c, err := scanChunkRow(pool.QueryRow(context.Background(), `SELECT id, build_id, chunk_index, mode, repo, title, instructions, target_files, target_symbols, effort, acceptance_criteria, status, COALESCE(pr_number, 0), stall_nudges, depends_on, ci_fail_count, ci_checks, rollout_state, banner, merge_awakeable_id, deploy_awakeable_id, started_at, completed_at, created_at, updated_at FROM vanadis_chunks WHERE id = $1`, id))
	if err != nil {
		t.Fatalf("load chunk %s: %v", id, err)
	}
	return c
}

// loadBuild reads one stored build through the production scanner.
func loadBuild(t *testing.T, pool *pgxpool.Pool, id string) model.VanadisBuild {
	t.Helper()
	b, err := ScanBuild(pool.QueryRow(context.Background(), `SELECT `+BuildColumns+` FROM vanadis_builds WHERE id = $1`, id))
	if err != nil {
		t.Fatalf("load build %s: %v", id, err)
	}
	return b
}

// assertStoreErr checks err against wantErrIs or wantErr and reports whether
// an error was expected (so the caller stops asserting results).
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
