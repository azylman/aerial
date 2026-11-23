package store

import (
	"context"
	"database/sql"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/azylman/aerial/pkg/hermetictest"
	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/services/coven/internal/config"
)

// TestMain starts one throwaway Postgres for the package (pgtest.Main) inside
// the hermetic transport guard and goleak check that hermetictest.Main applies
// to every other service package.
func TestMain(m *testing.M) {
	hermetictest.Install()
	for _, kv := range os.Environ() {
		if key, _, ok := strings.Cut(kv, "="); ok && strings.HasPrefix(key, "OTEL_") {
			_ = os.Unsetenv(key)
		}
	}
	goleak.VerifyTestMain(pgtestRunner{m: m})
}

// pgtestRunner runs the tests under pgtest.Main, then closes idle HTTP
// connections so goleak only sees goroutines the tests really leaked.
type pgtestRunner struct{ m *testing.M }

func (r pgtestRunner) Run() int {
	code := pgtest.Main(r.m)
	http.DefaultClient.CloseIdleConnections()
	if cl, ok := http.DefaultTransport.(interface{ CloseIdleConnections() }); ok {
		cl.CloseIdleConnections()
	}
	return code
}

// Every DB-backed test in this package runs the production SQL against a real
// PostgreSQL 18 + pgvector server started once per test binary by pgtest.Main
// (see TestMain above). Each test gets its own database cloned
// from the production schema, so tests may call t.Parallel().

// newTestDB opens a private cloned database through the production
// constructor (openDB, as RunApp does via OpenDB).
func newTestDB(t testing.TB) *sql.DB {
	t.Helper()
	db, err := openDB(context.Background(), pgtest.NewDSN(t))
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// newTestStore returns a production SQLStore over a private cloned database.
func newTestStore(t testing.TB) *SQLStore {
	t.Helper()
	return NewSQLStore(newTestDB(t))
}

// execSQL runs setup, seed or sabotage SQL against the test's own database.
// Multi-statement strings are allowed (no arguments uses the simple protocol).
func execSQL(t testing.TB, db DBTX, query string, args ...any) {
	t.Helper()
	if query == "" {
		return
	}
	if _, err := db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// testEmbedding returns a 384-dim vector with only dimension 0 set.
func testEmbedding(firstVal float32) []float32 {
	return testEmbeddingAt(0, firstVal)
}

// testEmbeddingAt returns a 384-dim vector with only dimension idx set
// (a one-hot vector when val is non-zero: cosine similarity between two
// different indexes is exactly 0, between equal indexes exactly 1).
func testEmbeddingAt(idx int, val float32) []float32 {
	emb := make([]float32, ExpectedEmbeddingDim)
	if idx >= 0 && idx < ExpectedEmbeddingDim {
		emb[idx] = val
	}
	return emb
}

// angleEmbedding returns a unit vector in the plane of dimensions 0 and 1 at
// deg degrees from dimension 0. The cosine similarity of two angle embeddings
// is cos(deg1 - deg2), so nearest-neighbour order is hand-derivable:
// 0° vs 60° = 0.5, 0° vs 25.84° ≈ 0.9, 0° vs 90° = 0.
func angleEmbedding(deg float64) []float32 {
	rad := deg * math.Pi / 180
	emb := make([]float32, ExpectedEmbeddingDim)
	emb[0] = float32(math.Cos(rad))
	emb[1] = float32(math.Sin(rad))
	return emb
}

// testTurn is one messages row seeded for extraction queries.
type testTurn struct {
	agent    string
	thread   string
	rowID    int64
	content  string
	response string
	status   string
	admin    bool
	convID   string
}

// adminTurn builds a completed admin turn in the default stack.
func adminTurn(thread string, rowID int64, content, response string) testTurn {
	return testTurn{agent: config.DefaultStack, thread: thread, rowID: rowID, content: content, response: response, status: "COMPLETED", admin: true}
}

// seedTurns inserts messages rows with explicit row_ids. The message id is
// derived from the row id ("m<rowID>"); created_at is a fixed instant plus
// rowID seconds so ordering is deterministic.
func seedTurns(t testing.TB, db DBTX, turns ...testTurn) {
	t.Helper()
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, tt := range turns {
		agent := tt.agent
		if agent == "" {
			agent = config.DefaultStack
		}
		status := tt.status
		if status == "" {
			status = "COMPLETED"
		}
		var resp any
		if tt.response != "" {
			resp = tt.response
		}
		at := base.Add(time.Duration(tt.rowID) * time.Second)
		execSQL(t, db, `INSERT INTO messages (id, row_id, agent, thread_id, content, response_text, status, is_admin, conversation_id, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)`,
			"m"+strconv.FormatInt(tt.rowID, 10), tt.rowID, agent, tt.thread, tt.content, resp, status, tt.admin, tt.convID, at)
	}
}

// seedWatermark stores an extraction watermark row for (agent, thread).
func seedWatermark(t testing.TB, db DBTX, agent, thread string, rowID int64, extractedAt *time.Time) {
	t.Helper()
	execSQL(t, db, `INSERT INTO extraction_watermarks (agent, thread_id, last_extracted_rowid, fact_extracted_at) VALUES ($1, $2, $3, $4)`, agent, thread, rowID, extractedAt)
}

// seedSession stores a sessions row carrying a summary.
func seedSession(t testing.TB, db DBTX, agent, sessionID, summary string) {
	t.Helper()
	execSQL(t, db, `INSERT INTO sessions (agent, session_id, summary) VALUES ($1, $2, $3)`, agent, sessionID, summary)
}

// Sabotage DDL: each string breaks exactly one kind of statement in the
// test's own cloned database so a specific Store method fails for real.
const (
	sabotageFailFn = `CREATE FUNCTION sabotage_fail() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'sabotaged'; END$$;`

	// sabotageInsertFact fails InsertFact; reads, updates and deletes still work.
	sabotageInsertFact = sabotageFailFn + `CREATE TRIGGER sabotage BEFORE INSERT ON facts FOR EACH ROW EXECUTE FUNCTION sabotage_fail();`
	// sabotageUpdateFact fails UpdateFact (it sets fact_text); ReinforceFact still works.
	sabotageUpdateFact = sabotageFailFn + `CREATE TRIGGER sabotage BEFORE UPDATE OF fact_text ON facts FOR EACH ROW EXECUTE FUNCTION sabotage_fail();`
	// sabotageReinforceFact fails ReinforceFact (fact_text unchanged); UpdateFact with new text still works.
	sabotageReinforceFact = sabotageFailFn + `CREATE TRIGGER sabotage BEFORE UPDATE ON facts FOR EACH ROW WHEN (OLD.fact_text = NEW.fact_text) EXECUTE FUNCTION sabotage_fail();`
	// sabotageDeleteFact fails DeleteFact.
	sabotageDeleteFact = sabotageFailFn + `CREATE TRIGGER sabotage BEFORE DELETE ON facts FOR EACH ROW EXECUTE FUNCTION sabotage_fail();`
	// sabotageFacts fails every facts query.
	sabotageFacts = `DROP TABLE facts`
	// sabotageFactSearch fails only SearchCandidateFacts (the FTS column).
	sabotageFactSearch = `ALTER TABLE facts DROP COLUMN fts_tokens`

	// sabotageWatermarks fails Get and Update of extraction watermarks.
	sabotageWatermarks = `DROP TABLE extraction_watermarks`
	// sabotageWatermarkWrite fails only UpdateConversationFactWatermark.
	sabotageWatermarkWrite = sabotageFailFn + `CREATE TRIGGER sabotage BEFORE INSERT OR UPDATE ON extraction_watermarks FOR EACH ROW EXECUTE FUNCTION sabotage_fail();`

	// sabotageTurnReads fails the turn queries (unextracted and prior turns)
	// but keeps GetMaxMessageRowID and the session join working.
	sabotageTurnReads = `ALTER TABLE messages DROP COLUMN response_text`
	// sabotageMessages fails every messages query.
	sabotageMessages = `DROP TABLE messages`
	// sabotageSessions fails the session summary lookup.
	sabotageSessions = `DROP TABLE sessions`

	// sabotageProcedural fails every procedural_candidates query.
	sabotageProcedural = `DROP TABLE procedural_candidates`
	// sabotageProceduralInsert fails only InsertProceduralCandidate.
	sabotageProceduralInsert = sabotageFailFn + `CREATE TRIGGER sabotage BEFORE INSERT ON procedural_candidates FOR EACH ROW EXECUTE FUNCTION sabotage_fail();`
	// sabotageProceduralUpdate fails only the Mark* status updates.
	sabotageProceduralUpdate = sabotageFailFn + `CREATE TRIGGER sabotage BEFORE UPDATE ON procedural_candidates FOR EACH ROW EXECUTE FUNCTION sabotage_fail();`
)
