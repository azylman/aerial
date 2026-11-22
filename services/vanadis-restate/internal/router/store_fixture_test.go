package router

// Postgres-backed store fixture for the router package: every route table row
// declares the builds and chunks it needs, opens a private cloned database
// (pgtest.NewDSN) through the production pool constructor and seeds real rows,
// so routing runs against the production lookup SQL.

import (
	"context"
	"encoding/json"
	"maps"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/pkg/pgutil"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// TestMain runs the package against one throwaway Postgres 18 server loaded
// from the production schema; every store-backed row clones its own database.
func TestMain(m *testing.M) {
	os.Exit(pgtest.Main(m))
}

// testOwner is the configured GitHub owner every test store uses.
const testOwner = "example-org"

// dbFixture declares the persisted state one table row routes against.
type dbFixture struct {
	builds   []model.VanadisBuild
	chunks   []model.VanadisChunk
	sabotage string // DDL applied in the row's private database after seeding
	closed   bool   // close the pool so every query fails with a driver error
	typedNil bool   // a typed nil *store.Store instead of a database
}

// openFixture returns the store a row runs against: a nil fixture is a nil
// store.VanadisStore, otherwise a seeded private database.
func openFixture(t *testing.T, f *dbFixture) store.VanadisStore {
	t.Helper()
	if f == nil {
		return nil
	}
	if f.typedNil {
		return (*store.Store)(nil)
	}
	st, pool := openStore(t)
	for _, b := range f.builds {
		seedBuild(t, pool, b)
	}
	for _, c := range f.chunks {
		seedChunk(t, pool, c)
	}
	mustExec(t, pool, f.sabotage)
	if f.closed {
		pool.Close()
	}
	return st
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
	_, err = pool.Exec(context.Background(), `INSERT INTO vanadis_chunks (id, build_id, chunk_index, mode, repo, title, instructions, target_files, target_symbols, effort, acceptance_criteria, status, pr_number, stall_nudges, depends_on, ci_fail_count, ci_checks, rollout_state, banner, merge_awakeable_id, deploy_awakeable_id, started_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NULLIF($13, 0), $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)`,
		c.ID, c.BuildID, c.ChunkIndex, c.Mode, c.Repo, c.Title, c.Instructions, nonNil(c.TargetFiles), nonNil(c.TargetSymbols), c.Effort, nonNil(c.AcceptanceCriteria), c.Status,
		c.PRNumber, c.StallNudges, nonNil(c.DependsOn), c.CIFailCount, rawChecks, rawRollout, c.Banner, c.MergeAwakeableID, c.DeployAwakeableID, nullTime(c.StartedAt), nullTime(c.CompletedAt))
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
