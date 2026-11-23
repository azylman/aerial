package build

// Postgres-backed store fixture for the build package: every test that needs
// a store opens a private cloned database (pgtest.NewDSN) through the
// production pool constructor and seeds real rows, so BuildObject logic runs
// against persisted state and the production SQL.

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/pkg/pgutil"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// testOwner is the configured GitHub owner every test store uses.
const testOwner = "example-owner"

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

// openStore opens a private cloned database through the production pool
// constructor and returns the Store plus the pool for seeding and sabotage.
func openStore(t *testing.T) (*store.Store, *pgxpool.Pool) {
	t.Helper()
	pool, err := pgutil.NewPool(context.Background(), pgtest.NewDSN(t), pgutil.PoolOptions{MaxConns: 10, Ping: true})
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return store.New(pool, testOwner), pool
}

// openStoreOnly opens a private cloned database and returns just the Store.
func openStoreOnly(t *testing.T) *store.Store {
	t.Helper()
	pool, err := pgutil.NewPool(context.Background(), pgtest.NewDSN(t), pgutil.PoolOptions{MaxConns: 10, Ping: true})
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return store.New(pool, testOwner)
}

// seededStore opens a private database and seeds builds then chunks.
func seededStore(t *testing.T, builds []model.VanadisBuild, chunks []model.VanadisChunk) (*store.Store, *pgxpool.Pool) {
	t.Helper()
	st, pool := openStore(t)
	for _, b := range builds {
		seedBuild(t, pool, b)
	}
	for _, c := range chunks {
		seedChunk(t, pool, c)
	}
	return st, pool
}

// seededStoreOnly opens a private database, seeds builds then chunks, and returns just the Store.
func seededStoreOnly(t *testing.T, builds []model.VanadisBuild, chunks []model.VanadisChunk) *store.Store {
	t.Helper()
	st, pool := openStore(t)
	for _, b := range builds {
		seedBuild(t, pool, b)
	}
	for _, c := range chunks {
		seedChunk(t, pool, c)
	}
	return st
}

// closedStore returns a Store whose pool is already closed, so Begin, Query,
// QueryRow and Exec all fail with a real driver error.
func closedStore(t *testing.T) *store.Store {
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

// seedBuild inserts b; empty status means running, empty objective and agent
// get placeholders and a zero tier becomes 2 so NOT NULL columns hold.
func seedBuild(t *testing.T, pool *pgxpool.Pool, b model.VanadisBuild) {
	t.Helper()
	if b.Status == "" {
		b.Status = statusRunning
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
// "aerial", empty status means pending and a zero PR number is NULL. The SHA
// fields fold into ci_checks the way production stores them.
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
	_, err = pool.Exec(context.Background(), `INSERT INTO vanadis_chunks (id, build_id, chunk_index, mode, repo, title, instructions, target_files, target_symbols, effort, acceptance_criteria, status, pr_number, stall_nudges, depends_on, ci_fail_count, ci_checks, rollout_state, banner, started_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NULLIF($13, 0), $14, $15, $16, $17, $18, $19, $20, $21)`,
		c.ID, c.BuildID, c.ChunkIndex, c.Mode, c.Repo, c.Title, c.Instructions, nonNil(c.TargetFiles), nonNil(c.TargetSymbols), c.Effort, nonNil(c.AcceptanceCriteria), c.Status,
		c.PRNumber, c.StallNudges, nonNil(c.DependsOn), c.CIFailCount, rawChecks, rawRollout, c.Banner, nullTime(c.StartedAt), nullTime(c.CompletedAt))
	if err != nil {
		t.Fatalf("seed chunk %s: %v", c.ID, err)
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

// loadBuild reads one stored build and its chunks through the production
// GetBuild query.
func loadBuild(t *testing.T, st *store.Store, id string) (model.VanadisBuild, []model.VanadisChunk) {
	t.Helper()
	b, chunks, err := st.GetBuild(context.Background(), id)
	if err != nil {
		t.Fatalf("load build %s: %v", id, err)
	}
	return *b, chunks
}

// loadBuildBuild reads one stored build without returning chunks.
func loadBuildBuild(t *testing.T, st *store.Store, id string) model.VanadisBuild {
	t.Helper()
	b, chunks, err := st.GetBuild(context.Background(), id)
	if err != nil {
		t.Fatalf("load build %s: %v", id, err)
	}
	_ = chunks
	return *b
}

// chunkStatuses maps chunk id to stored status for build id.
func chunkStatuses(t *testing.T, st *store.Store, id string) map[string]string {
	t.Helper()
	_, chunks := loadBuild(t, st, id)
	out := make(map[string]string, len(chunks))
	for _, c := range chunks {
		out[c.ID] = c.Status
	}
	return out
}
