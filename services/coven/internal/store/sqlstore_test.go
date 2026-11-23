package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pgvector/pgvector-go"

	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/services/coven/internal/config"
)

// sqlstoreFact is one facts row seeded through the production InsertFact.
// deg places the embedding with angleEmbedding; count and ageDays then set
// reinforce_count and last_reinforced_at (ageDays before a shared reference
// instant, so equal ages tie exactly).
type sqlstoreFact struct {
	text    string
	deg     float64
	count   int
	ageDays int
}

// sqlstoreScored is an expected (fact text, score) result.
type sqlstoreScored struct {
	text  string
	score float64
}

// sqlstoreWatermark is one extraction_watermarks row.
type sqlstoreWatermark struct {
	agent     string
	thread    string
	rowID     int64
	extracted bool
}

const sqlstoreScoreTolerance = 1e-4

// sqlstoreSabotagePaginateSelect fails only the GetFactsPaginated row select
// (COUNT(*) does not read reinforce_count).
const sqlstoreSabotagePaginateSelect = `ALTER TABLE facts DROP COLUMN reinforce_count`

// seedSQLStoreFacts inserts facts through InsertFact and returns their ids.
func seedSQLStoreFacts(t *testing.T, store *SQLStore, facts ...sqlstoreFact) []int64 {
	t.Helper()
	ref := time.Now().UTC().Truncate(time.Second)
	ids := make([]int64, 0, len(facts))
	for _, f := range facts {
		id, err := store.InsertFact(context.Background(), f.text, angleEmbedding(f.deg))
		if err != nil {
			t.Fatalf("seed fact %q: %v", f.text, err)
		}
		execSQL(t, store.DB(), `UPDATE facts SET reinforce_count = $1, last_reinforced_at = $2 WHERE id = $3`, max(f.count, 1), ref.AddDate(0, 0, -f.ageDays), id)
		ids = append(ids, id)
	}
	return ids
}

// sqlstoreTestStore returns a real store with sabotage applied after facts are
// seeded, or a nil-db store when nilDB is set.
func sqlstoreTestStore(t *testing.T, nilDB bool, sabotage string, facts ...sqlstoreFact) *SQLStore {
	t.Helper()
	if nilDB {
		return &SQLStore{}
	}
	store := newTestStore(t)
	seedSQLStoreFacts(t, store, facts...)
	execSQL(t, store.DB(), sabotage)
	return store
}

func assertSQLStoreErr(t *testing.T, err error, wantErr bool, wantIs error) {
	t.Helper()
	if (err != nil) != (wantErr || wantIs != nil) {
		t.Fatalf("err = %v, wantErr = %v (want %v)", err, wantErr, wantIs)
	}
	if wantIs != nil && !errors.Is(err, wantIs) {
		t.Fatalf("err = %v, want errors.Is %v", err, wantIs)
	}
}

func assertScoredTexts(t *testing.T, gotTexts []string, gotScores []float64, want []sqlstoreScored) {
	t.Helper()
	if len(gotTexts) != len(want) {
		t.Fatalf("results = %q (scores %v), want %v", gotTexts, gotScores, want)
	}
	for i, w := range want {
		if gotTexts[i] != w.text || math.Abs(gotScores[i]-w.score) > sqlstoreScoreTolerance {
			t.Errorf("result[%d] = (%q, %.6f), want (%q, %.6f)", i, gotTexts[i], gotScores[i], w.text, w.score)
		}
	}
}

func TestSQLStore_ConstructorsAndSetters(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		nilStore  bool
		setStack  string
		wantStack string
	}{
		{name: "config.DefaultStack", wantStack: config.DefaultStack},
		{name: "CustomStack", setStack: "agent-alpha", wantStack: "agent-alpha"},
		{name: "StackWithWhitespace", setStack: "  agent-beta  ", wantStack: "agent-beta"},
		{name: "WhitespaceStackResetsToDefault", setStack: "   ", wantStack: config.DefaultStack},
		{name: "NilStoreFallback", nilStore: true, setStack: "agent-alpha", wantStack: config.DefaultStack},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			store := NewSQLStore(db)
			wantDB := DBTX(db)
			if tc.nilStore {
				store = nil
				wantDB = nil
			}
			store.SetStack(tc.setStack)
			ic := NewIndexerClient("http://transcripts", "test", nil)
			store.SetIndexerClient(ic)
			if got := store.effectiveStack(); got != tc.wantStack {
				t.Errorf("effectiveStack() = %q, want %q", got, tc.wantStack)
			}
			if got := store.DB(); got != wantDB {
				t.Errorf("DB() = %v, want %v", got, wantDB)
			}
			if !tc.nilStore && store.indexerClient != ic {
				t.Errorf("indexerClient = %v, want %v", store.indexerClient, ic)
			}
		})
	}
}

func TestSQLStore_WithPinnedConn(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	s := NewSQLStore(db)

	// nil receiver
	var nilStore *SQLStore
	if got := nilStore.WithPinnedConn(nil); got != nil {
		t.Errorf("nilStore.WithPinnedConn(nil) = %v, want nil", got)
	}

	// with nil conn
	if got := s.WithPinnedConn(nil); got != s {
		t.Errorf("s.WithPinnedConn(nil) = %v, want %v", got, s)
	}

	// with real pinned conn
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	withConn := s.WithPinnedConn(conn)
	if withConn == nil || withConn == s {
		t.Fatalf("expected non-nil cloned store")
	}
	sqlStore, ok := withConn.(*SQLStore)
	if !ok {
		t.Fatalf("expected *SQLStore, got %T", withConn)
	}
	if sqlStore.DB() != conn {
		t.Errorf("DB() = %v, want %v", sqlStore.DB(), conn)
	}
}

// sqlstoreHandle builds the DBTX fixture for TestSQLStore_Conn.
func sqlstoreHandle(t *testing.T, kind string) (DBTX, *sql.Conn) {
	t.Helper()
	db := newTestDB(t)
	switch kind {
	case "db":
		return db, nil
	case "conn":
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatalf("pin conn: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn, conn
	case "tx":
		tx, err := db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		t.Cleanup(func() { _ = tx.Rollback() })
		return tx, nil
	}
	return nil, nil
}

func TestSQLStore_Conn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		nilStore bool
		handle   string
		wantSame bool
		wantErr  bool
		wantIs   error
	}{
		{name: "PoolPinsUsableConn", handle: "db"},
		{name: "PinnedConnReturnsItself", handle: "conn", wantSame: true},
		{name: "TxIsNotPinnable", handle: "tx", wantIs: ErrNoPinnedConn},
		{name: "NilDB", handle: "", wantErr: true},
		{name: "NilStore", nilStore: true, handle: "db", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			handle, pinned := sqlstoreHandle(t, tc.handle)
			store := &SQLStore{db: handle}
			if tc.nilStore {
				store = nil
			}
			conn, err := store.Conn(ctx)
			assertSQLStoreErr(t, err, tc.wantErr, tc.wantIs)
			if err != nil {
				if conn != nil {
					t.Errorf("Conn() = %v on error, want nil", conn)
				}
				return
			}
			if tc.wantSame != (conn == pinned) {
				t.Errorf("Conn() same as pinned = %v, want %v", conn == pinned, tc.wantSame)
			}
			var one int
			if err := conn.QueryRowContext(ctx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
				t.Errorf("pinned conn SELECT 1 = (%d, %v), want (1, nil)", one, err)
			}
			if !tc.wantSame {
				_ = conn.Close()
			}
		})
	}
}

func TestSQLStore_InsertFact(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		nilDB       bool
		stack       string
		factText    string
		emb         []float32
		sourceAgent []string
		sabotage    string
		wantErr     bool
		wantText    string
		wantAgent   string
	}{
		{name: "NilDBError", nilDB: true, factText: "valid", emb: angleEmbedding(0), wantErr: true},
		{name: "EmptyFactText", factText: "   ", emb: angleEmbedding(0), wantErr: true},
		{name: "InvalidEmbeddingDim", factText: "valid", emb: make([]float32, 10), wantErr: true},
		{name: "InsertError", factText: "valid fact", emb: angleEmbedding(0), sabotage: sabotageInsertFact, wantErr: true},
		{name: "config.DefaultStackTrimmed", factText: "  Alex drinks matcha  ", emb: angleEmbedding(0), wantText: "Alex drinks matcha", wantAgent: "aerial"},
		{name: "StoreStack", stack: "agent-beta", factText: "Alex owns a kayak", emb: angleEmbedding(30), wantText: "Alex owns a kayak", wantAgent: "agent-beta"},
		{name: "ExplicitSourceAgent", stack: "agent-beta", factText: "Alex likes sushi", emb: angleEmbedding(60), sourceAgent: []string{"  agent-x  "}, wantText: "Alex likes sushi", wantAgent: "agent-x"},
		{name: "BlankSourceAgentFallsBack", factText: "Alex likes tea", emb: angleEmbedding(60), sourceAgent: []string{"  "}, wantText: "Alex likes tea", wantAgent: "aerial"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := sqlstoreTestStore(t, tc.nilDB, tc.sabotage)
			if tc.stack != "" {
				store.SetStack(tc.stack)
			}
			id, err := store.InsertFact(ctx, tc.factText, tc.emb, tc.sourceAgent...)
			assertSQLStoreErr(t, err, tc.wantErr, nil)
			if tc.wantErr {
				return
			}
			var text, agent string
			var count int
			var sameTimes bool
			if err := store.DB().QueryRowContext(ctx, `SELECT fact_text, reinforce_count, source_agent, created_at = last_reinforced_at FROM facts WHERE id = $1`, id).Scan(&text, &count, &agent, &sameTimes); err != nil {
				t.Fatalf("read back fact %d: %v", id, err)
			}
			if text != tc.wantText || count != 1 || agent != tc.wantAgent || !sameTimes {
				t.Errorf("row = (%q, %d, %q, sameTimes=%v), want (%q, 1, %q, true)", text, count, agent, sameTimes, tc.wantText, tc.wantAgent)
			}
		})
	}
}

// sqlstoreMissingFactID never exists in a test database.
const sqlstoreMissingFactID int64 = 987654321

func TestSQLStore_UpdateFact(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		nilDB     bool
		id        int64 // 0 = the seeded fact
		factText  string
		emb       []float32
		sabotage  string
		wantErr   bool
		wantIs    error
		wantText  string
		wantCount int
	}{
		{name: "NilDBError", nilDB: true, factText: "valid", emb: testEmbeddingAt(5, 1), wantErr: true},
		{name: "EmptyFactText", factText: " ", emb: testEmbeddingAt(5, 1), wantErr: true, wantText: "Alex drinks coffee", wantCount: 1},
		{name: "InvalidEmbeddingDim", factText: "valid", emb: make([]float32, 10), wantErr: true, wantText: "Alex drinks coffee", wantCount: 1},
		{name: "ExecError", factText: "Alex drinks matcha", emb: testEmbeddingAt(5, 1), sabotage: sabotageUpdateFact, wantErr: true, wantText: "Alex drinks coffee", wantCount: 1},
		{name: "NotFound", id: sqlstoreMissingFactID, factText: "Alex drinks matcha", emb: testEmbeddingAt(5, 1), wantIs: ErrFactNotFound, wantText: "Alex drinks coffee", wantCount: 1},
		{name: "SuccessRewritesAndReinforces", factText: "  Alex drinks matcha ", emb: testEmbeddingAt(5, 1), wantText: "Alex drinks matcha", wantCount: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			if tc.nilDB {
				assertSQLStoreErr(t, (&SQLStore{}).UpdateFact(ctx, 1, tc.factText, tc.emb), true, nil)
				return
			}
			store := newTestStore(t)
			seeded, err := store.InsertFact(ctx, "Alex drinks coffee", testEmbeddingAt(0, 1))
			if err != nil {
				t.Fatalf("seed: %v", err)
			}
			execSQL(t, store.DB(), tc.sabotage)
			id := tc.id
			if id == 0 {
				id = seeded
			}
			assertSQLStoreErr(t, store.UpdateFact(ctx, id, tc.factText, tc.emb), tc.wantErr, tc.wantIs)
			var text string
			var count int
			var simToNew float64
			if err := store.DB().QueryRowContext(ctx, `SELECT fact_text, reinforce_count, 1 - (embedding <=> $2) FROM facts WHERE id = $1`, seeded, pgvector.NewVector(testEmbeddingAt(5, 1))).Scan(&text, &count, &simToNew); err != nil {
				t.Fatalf("read back: %v", err)
			}
			wantSim := 0.0
			if tc.wantCount == 2 {
				wantSim = 1
			}
			if text != tc.wantText || count != tc.wantCount || math.Abs(simToNew-wantSim) > sqlstoreScoreTolerance {
				t.Errorf("row = (%q, %d, sim %.3f), want (%q, %d, sim %.0f)", text, count, simToNew, tc.wantText, tc.wantCount, wantSim)
			}
		})
	}
}

func TestSQLStore_ReinforceFact(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		nilDB          bool
		id             int64 // 0 = the seeded fact
		sabotage       string
		wantErr        bool
		wantIs         error
		wantCount      int
		wantReinforced bool
	}{
		{name: "NilDBError", nilDB: true, wantErr: true},
		{name: "ExecError", sabotage: sabotageReinforceFact, wantErr: true, wantCount: 1},
		{name: "NotFound", id: sqlstoreMissingFactID, wantIs: ErrFactNotFound, wantCount: 1},
		{name: "SuccessBumpsCountAndTime", wantCount: 2, wantReinforced: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			if tc.nilDB {
				assertSQLStoreErr(t, (&SQLStore{}).ReinforceFact(ctx, 1), true, nil)
				return
			}
			store := newTestStore(t)
			seeded := seedSQLStoreFacts(t, store, sqlstoreFact{text: "Alex drinks coffee", ageDays: 400})[0]
			execSQL(t, store.DB(), tc.sabotage)
			id := tc.id
			if id == 0 {
				id = seeded
			}
			assertSQLStoreErr(t, store.ReinforceFact(ctx, id), tc.wantErr, tc.wantIs)
			var count int
			var fresh bool
			if err := store.DB().QueryRowContext(ctx, `SELECT reinforce_count, last_reinforced_at > now() - interval '1 day' FROM facts WHERE id = $1`, seeded).Scan(&count, &fresh); err != nil {
				t.Fatalf("read back: %v", err)
			}
			if count != tc.wantCount || fresh != tc.wantReinforced {
				t.Errorf("row = (count %d, reinforced today %v), want (%d, %v)", count, fresh, tc.wantCount, tc.wantReinforced)
			}
		})
	}
}

func TestSQLStore_DeleteFact(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		nilDB    bool
		id       int64 // 0 = the seeded fact
		sabotage string
		wantErr  bool
		wantIs   error
		wantLeft int
	}{
		{name: "NilDBError", nilDB: true, wantErr: true},
		{name: "ExecError", sabotage: sabotageDeleteFact, wantErr: true, wantLeft: 1},
		{name: "NotFound", id: sqlstoreMissingFactID, wantIs: ErrFactNotFound, wantLeft: 1},
		{name: "SuccessRemovesRow", wantLeft: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			if tc.nilDB {
				assertSQLStoreErr(t, (&SQLStore{}).DeleteFact(ctx, 1), true, nil)
				return
			}
			store := newTestStore(t)
			seeded := seedSQLStoreFacts(t, store, sqlstoreFact{text: "Alex drinks coffee"})[0]
			execSQL(t, store.DB(), tc.sabotage)
			id := tc.id
			if id == 0 {
				id = seeded
			}
			assertSQLStoreErr(t, store.DeleteFact(ctx, id), tc.wantErr, tc.wantIs)
			var left int
			if err := store.DB().QueryRowContext(ctx, `SELECT count(*) FROM facts`).Scan(&left); err != nil {
				t.Fatalf("count: %v", err)
			}
			if left != tc.wantLeft {
				t.Errorf("facts left = %d, want %d", left, tc.wantLeft)
			}
		})
	}
}

// sqlstoreDupFacts sit at 25.84° (cos = 0.9 to 0°) and 60° (cos = 0.5).
var sqlstoreDupFacts = []sqlstoreFact{
	{text: "Alex drinks matcha daily", deg: 25.84, count: 3},
	{text: "Alex owns a kayak", deg: 60},
}

func TestSQLStore_FindDuplicateFact(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		nilDB     bool
		facts     []sqlstoreFact
		emb       []float32
		minSim    float64
		sabotage  string
		wantErr   bool
		wantText  string // "" = no duplicate returned
		wantCount int
		wantSim   float64
	}{
		{name: "NilDBError", nilDB: true, emb: angleEmbedding(0), wantErr: true},
		{name: "ShortEmbeddingReturnsNil", facts: sqlstoreDupFacts, emb: make([]float32, 10)},
		{name: "EmptyTableReturnsNil", emb: angleEmbedding(0), minSim: 0.1},
		{name: "QueryError", facts: sqlstoreDupFacts, emb: angleEmbedding(0), sabotage: sabotageFacts, wantErr: true},
		{name: "NearestAboveMin", facts: sqlstoreDupFacts, emb: angleEmbedding(0), minSim: 0.85, wantText: "Alex drinks matcha daily", wantCount: 3, wantSim: 0.9},
		{name: "DefaultMinSimIs085", facts: sqlstoreDupFacts, emb: angleEmbedding(0), wantText: "Alex drinks matcha daily", wantCount: 3, wantSim: 0.9},
		{name: "NearestBelowMinReportsSim", facts: sqlstoreDupFacts, emb: angleEmbedding(0), minSim: 0.95, wantSim: 0.9},
		{name: "OtherNeighbour", facts: sqlstoreDupFacts, emb: angleEmbedding(60), minSim: 0.99, wantText: "Alex owns a kayak", wantCount: 1, wantSim: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := sqlstoreTestStore(t, tc.nilDB, tc.sabotage, tc.facts...)
			dup, sim, err := store.FindDuplicateFact(context.Background(), tc.emb, tc.minSim)
			assertSQLStoreErr(t, err, tc.wantErr, nil)
			if math.Abs(sim-tc.wantSim) > sqlstoreScoreTolerance {
				t.Errorf("sim = %.6f, want %.6f", sim, tc.wantSim)
			}
			gotText := ""
			if dup != nil {
				gotText = dup.FactText
				if dup.ReinforceCount != tc.wantCount || dup.CreatedAt.IsZero() || dup.LastReinforcedAt.IsZero() {
					t.Errorf("dup = %+v, want count %d and non-zero timestamps", dup, tc.wantCount)
				}
			}
			if gotText != tc.wantText {
				t.Errorf("dup text = %q, want %q", gotText, tc.wantText)
			}
		})
	}
}

// sqlstoreRankingFacts against query angleEmbedding(0) + text "kayak" (vector
// rank = list order; FTS rank: the double-kayak fact first). Expected scores
// = 60*(0.7/(60+vrank) + 0.3/(60+trank)) * (1+0.15*ln(1+count)) * (1+days/14)^-0.35:
//
//	matcha (vector-only, vr1, count 1):       0.6885246*1.1039721            = 0.760112
//	double kayak (vr2 + tr1, count 1):        0.9725013*1.1039721            = 1.073614
//	sushi (vector-only, vr3, count 10):       0.6666667*1.3596842            = 0.906456
//	sails kayak (vr4 + tr2, count 1, 14 d):   0.9465726*1.1039721*0.7845840  = 0.819882
//	tea (vector-only, vr5 at 90°, count 1):   0.6461538*1.1039721            = 0.713336
var sqlstoreRankingFacts = []sqlstoreFact{
	{text: "Alex drinks matcha", deg: 0},
	{text: "Alex owns a kayak and a spare kayak", deg: 10},
	{text: "Alex likes sushi", deg: 20, count: 10},
	{text: "Alex sails a kayak", deg: 30, ageDays: 14},
	{text: "Alex brews tea", deg: 90},
}

func TestSQLStore_SearchCandidateFacts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		nilDB    bool
		facts    []sqlstoreFact
		emb      []float32
		query    string
		limit    int
		minScore float64
		sabotage string
		wantErr  bool
		want     []sqlstoreScored
	}{
		{name: "NilDBError", nilDB: true, emb: angleEmbedding(0), query: "q", wantErr: true},
		{name: "ShortEmbeddingReturnsNil", facts: sqlstoreRankingFacts, emb: make([]float32, 10), query: "kayak"},
		{name: "EmptyQueryReturnsNil", facts: sqlstoreRankingFacts, emb: angleEmbedding(0), query: "   "},
		{name: "QueryError", facts: sqlstoreRankingFacts, emb: angleEmbedding(0), query: "kayak", sabotage: sabotageFactSearch, wantErr: true},
		{name: "HybridRankingWithBoostAndDecayDefaults", facts: sqlstoreRankingFacts, emb: angleEmbedding(0), query: "kayak", limit: -1, minScore: -1, want: []sqlstoreScored{
			{"Alex owns a kayak and a spare kayak", 1.073614},
			{"Alex likes sushi", 0.906456},
			{"Alex sails a kayak", 0.819882},
			{"Alex drinks matcha", 0.760112},
			{"Alex brews tea", 0.713336},
		}},
		{name: "MinScoreCutoff", facts: sqlstoreRankingFacts, emb: angleEmbedding(0), query: "kayak", minScore: 0.75, want: []sqlstoreScored{
			{"Alex owns a kayak and a spare kayak", 1.073614},
			{"Alex likes sushi", 0.906456},
			{"Alex sails a kayak", 0.819882},
			{"Alex drinks matcha", 0.760112},
		}},
		// limit 2: vector hits {matcha, double kayak}, text hits {double kayak,
		// sails kayak}; sails kayak is text-only: 60*0.3/62*1.1039721*0.784584 = 0.251465.
		{name: "CandidateLimitTruncates", facts: sqlstoreRankingFacts, emb: angleEmbedding(0), query: "kayak", limit: 2, want: []sqlstoreScored{
			{"Alex owns a kayak and a spare kayak", 1.073614},
			{"Alex drinks matcha", 0.760112},
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := sqlstoreTestStore(t, tc.nilDB, tc.sabotage, tc.facts...)
			results, err := store.SearchCandidateFacts(context.Background(), tc.emb, tc.query, tc.limit, tc.minScore)
			assertSQLStoreErr(t, err, tc.wantErr, nil)
			texts := make([]string, 0, len(results))
			scores := make([]float64, 0, len(results))
			for _, r := range results {
				texts = append(texts, r.Fact.FactText)
				scores = append(scores, r.Score)
				if len(r.Embedding) != ExpectedEmbeddingDim || r.Fact.CreatedAt.IsZero() {
					t.Errorf("result %q embedding dim %d created %v, want %d and non-zero", r.Fact.FactText, len(r.Embedding), r.Fact.CreatedAt, ExpectedEmbeddingDim)
				}
			}
			assertScoredTexts(t, texts, scores, tc.want)
		})
	}
}

// sqlstorePageFacts ordered by reinforce_count desc, last_reinforced_at desc,
// id desc: kernel (3, today), vim (3, 1 day ago), then the count-1 ties of
// equal time by id desc: progress, linuxXuser, pasta, runs.
var sqlstorePageFacts = []sqlstoreFact{
	{text: "Linux kernel 100%", count: 3},
	{text: "linux_user prefers vim", count: 3, ageDays: 1},
	{text: "Alex runs Linux"},
	{text: "Cooking pasta"},
	{text: "linuxXuser"},
	{text: "progress 100x"},
}

func TestSQLStore_GetFactsPaginated(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		nilDB      bool
		filter     FactsFilter
		sabotage   string
		wantErr    bool
		wantTotal  int
		wantLimit  int
		wantOffset int
		wantTexts  []string
	}{
		{name: "NilDBError", nilDB: true, wantErr: true},
		{name: "CountQueryError", filter: FactsFilter{Limit: 10}, sabotage: sabotageFacts, wantErr: true},
		{name: "SelectQueryError", filter: FactsFilter{Limit: 5}, sabotage: sqlstoreSabotagePaginateSelect, wantErr: true},
		{name: "AllOrdered", wantTotal: 6, wantTexts: []string{"Linux kernel 100%", "linux_user prefers vim", "progress 100x", "linuxXuser", "Cooking pasta", "Alex runs Linux"}},
		{name: "CaseInsensitiveFilterNegativeOffset", filter: FactsFilter{Query: " LINUX ", Limit: 2, Offset: -1}, wantTotal: 4, wantLimit: 2, wantTexts: []string{"Linux kernel 100%", "linux_user prefers vim"}},
		{name: "UnderscoreEscaped", filter: FactsFilter{Query: "linux_user"}, wantTotal: 1, wantTexts: []string{"linux_user prefers vim"}},
		{name: "PercentEscaped", filter: FactsFilter{Query: "100%"}, wantTotal: 1, wantTexts: []string{"Linux kernel 100%"}},
		{name: "LimitOffset", filter: FactsFilter{Limit: 2, Offset: 2}, wantTotal: 6, wantLimit: 2, wantOffset: 2, wantTexts: []string{"progress 100x", "linuxXuser"}},
		{name: "OffsetOnly", filter: FactsFilter{Offset: 4}, wantTotal: 6, wantOffset: 4, wantTexts: []string{"Cooking pasta", "Alex runs Linux"}},
		{name: "NoMatchesEmptySlice", filter: FactsFilter{Query: "kayak"}, wantTexts: []string{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := sqlstoreTestStore(t, tc.nilDB, tc.sabotage, sqlstorePageFacts...)
			res, err := store.GetFactsPaginated(context.Background(), tc.filter)
			assertSQLStoreErr(t, err, tc.wantErr, nil)
			if tc.wantErr {
				return
			}
			texts := make([]string, 0, len(res.Facts))
			for _, f := range res.Facts {
				texts = append(texts, f.FactText)
			}
			if res.Total != tc.wantTotal || res.Limit != tc.wantLimit || res.Offset != tc.wantOffset || res.Status != "ok" || strings.Join(texts, "|") != strings.Join(tc.wantTexts, "|") || res.Facts == nil {
				t.Errorf("result = total %d limit %d offset %d status %q facts %q, want total %d limit %d offset %d status ok facts %q", res.Total, res.Limit, res.Offset, res.Status, texts, tc.wantTotal, tc.wantLimit, tc.wantOffset, tc.wantTexts)
			}
		})
	}
}

func TestIsExtractableAdminTurn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		msg  Message
		want bool
	}{
		{name: "CompletedAdminNormalText", msg: Message{Status: "COMPLETED", ResponseText: "Here is your answer.", IsAdmin: true}, want: true},
		{name: "NotCompleted", msg: Message{Status: "PENDING", ResponseText: "Here is your answer.", IsAdmin: true}},
		{name: "NotAdmin", msg: Message{Status: "COMPLETED", ResponseText: "Here is your answer."}},
		{name: "BracketPrefix", msg: Message{Status: "COMPLETED", ResponseText: "  [SILENT_TURN]", IsAdmin: true}},
		{name: "EmptyResponseText", msg: Message{Status: "COMPLETED", ResponseText: "   ", IsAdmin: true}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsExtractableAdminTurn(tc.msg); got != tc.want {
				t.Fatalf("IsExtractableAdminTurn() = %v, want %v", got, tc.want)
			}
		})
	}
}

// sqlstoreAlphaTurns exercise every GetUnextractedAdminTurns filter for
// agent-alpha/th-1 with a watermark at row 10: only row 12 qualifies.
var sqlstoreAlphaTurns = []testTurn{
	{agent: "agent-alpha", thread: "th-1", rowID: 5, content: "old", response: "below watermark", status: "COMPLETED", admin: true},
	{agent: "agent-alpha", thread: "th-1", rowID: 12, content: "hello", response: "world", status: "COMPLETED", admin: true},
	{agent: "agent-alpha", thread: "th-1", rowID: 13, content: "guest", response: "not admin", status: "COMPLETED"},
	{agent: "agent-alpha", thread: "th-1", rowID: 14, content: "pending", response: "not completed", status: "PENDING", admin: true},
	{agent: "agent-alpha", thread: "th-1", rowID: 15, content: "silent", response: "[SILENT_TURN]", status: "COMPLETED", admin: true},
	{agent: "agent-alpha", thread: "th-1", rowID: 16, content: "no response", status: "COMPLETED", admin: true},
	{agent: "agent-alpha", thread: "th-9", rowID: 17, content: "other thread", response: "x", status: "COMPLETED", admin: true},
	adminTurn("th-1", 18, "other agent", "x"),
	adminTurn("th-2", 25, "turn 2", "answer 2"),
	adminTurn("th-2", 11, "turn 1", "answer 1"),
}

var sqlstoreAlphaWatermarks = []sqlstoreWatermark{{agent: "agent-alpha", thread: "th-1", rowID: 10}}

func seedSQLStoreWatermarks(t *testing.T, db DBTX, marks []sqlstoreWatermark) {
	t.Helper()
	for _, m := range marks {
		seedWatermark(t, db, m.agent, m.thread, m.rowID, nil)
	}
}

func TestSQLStore_GetUnextractedAdminTurns(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		nilDB      bool
		turns      []testTurn
		watermarks []sqlstoreWatermark
		threadID   string
		limit      int
		agent      []string
		sabotage   string
		wantErr    bool
		want       []string // id|row|thread|content|response|created
	}{
		{name: "NilDBError", nilDB: true, threadID: "th-1", wantErr: true},
		{name: "EmptyThreadID", turns: sqlstoreAlphaTurns, threadID: "   "},
		{name: "WatermarkError", turns: sqlstoreAlphaTurns, threadID: "th-1", sabotage: sabotageWatermarks, wantErr: true},
		{name: "TurnQueryError", turns: sqlstoreAlphaTurns, threadID: "th-1", sabotage: sabotageTurnReads, wantErr: true},
		{name: "AgentScopedAboveWatermarkExtractableOnly", turns: sqlstoreAlphaTurns, watermarks: sqlstoreAlphaWatermarks, threadID: " th-1 ", limit: 50, agent: []string{" agent-alpha "}, want: []string{"m12|12|th-1|hello|world|2026-01-02T03:04:17Z"}},
		{name: "DefaultAgentSortedAscending", turns: sqlstoreAlphaTurns, watermarks: sqlstoreAlphaWatermarks, threadID: "th-2", want: []string{"m11|11|th-2|turn 1|answer 1|2026-01-02T03:04:16Z", "m25|25|th-2|turn 2|answer 2|2026-01-02T03:04:30Z"}},
		{name: "LimitBoundsBatch", turns: sqlstoreAlphaTurns, threadID: "th-2", limit: 1, want: []string{"m11|11|th-2|turn 1|answer 1|2026-01-02T03:04:16Z"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := sqlstoreTestStore(t, tc.nilDB, "")
			if !tc.nilDB {
				seedTurns(t, store.DB(), tc.turns...)
				seedSQLStoreWatermarks(t, store.DB(), tc.watermarks)
				execSQL(t, store.DB(), tc.sabotage)
			}
			turns, err := store.GetUnextractedAdminTurns(context.Background(), tc.threadID, tc.limit, tc.agent...)
			assertSQLStoreErr(t, err, tc.wantErr, nil)
			got := make([]string, 0, len(turns))
			for _, m := range turns {
				got = append(got, strings.Join([]string{m.ID, strconv.FormatInt(m.RowID, 10), m.ThreadID, m.Content, m.ResponseText, m.CreatedAt.Format(time.RFC3339)}, "|"))
				if m.Status != "COMPLETED" || !m.IsAdmin || !m.UpdatedAt.Equal(m.CreatedAt) {
					t.Errorf("turn %s status %q admin %v updated %v, want COMPLETED admin updated=created", m.ID, m.Status, m.IsAdmin, m.UpdatedAt)
				}
			}
			if strings.Join(got, "\n") != strings.Join(tc.want, "\n") {
				t.Errorf("turns = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSQLStore_GetMaxMessageRowID(t *testing.T) {
	t.Parallel()
	maxTurns := []testTurn{
		{agent: "agent-alpha", thread: "th-1", rowID: 42, content: "a", status: "COMPLETED"},
		{agent: "agent-alpha", thread: "th-1", rowID: 50, content: "b", status: "PENDING"},
		{agent: "agent-alpha", thread: "th-1", rowID: 41, content: "c", status: "COMPLETED", admin: true},
		adminTurn("th-1", 7, "d", "e"),
	}
	tests := []struct {
		name     string
		nilDB    bool
		threadID string
		agent    []string
		sabotage string
		wantErr  bool
		wantID   int64
	}{
		{name: "NilDBError", nilDB: true, threadID: "th-1", wantErr: true},
		{name: "EmptyThreadID", threadID: " "},
		{name: "QueryError", threadID: "th-1", sabotage: sabotageMessages, wantErr: true},
		{name: "AgentScopedCompletedOnly", threadID: "th-1", agent: []string{"agent-alpha"}, wantID: 42},
		{name: "DefaultAgent", threadID: " th-1 ", wantID: 7},
		{name: "UnknownThreadZero", threadID: "th-404", agent: []string{"agent-alpha"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := sqlstoreTestStore(t, tc.nilDB, "")
			if !tc.nilDB {
				seedTurns(t, store.DB(), maxTurns...)
				execSQL(t, store.DB(), tc.sabotage)
			}
			id, err := store.GetMaxMessageRowID(context.Background(), tc.threadID, tc.agent...)
			assertSQLStoreErr(t, err, tc.wantErr, nil)
			if id != tc.wantID {
				t.Errorf("GetMaxMessageRowID() = %d, want %d", id, tc.wantID)
			}
		})
	}
}

func TestSQLStore_UpdateConversationFactWatermark(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		nilDB    bool
		seed     []sqlstoreWatermark
		threadID string
		maxRowID int64
		agent    []string
		sabotage string
		wantErr  bool
		want     []sqlstoreWatermark // all rows ordered by agent, thread
	}{
		{name: "NilDBError", nilDB: true, threadID: "th-1", wantErr: true},
		{name: "EmptyThreadIDNoop", threadID: "  ", maxRowID: 9, want: []sqlstoreWatermark{}},
		{name: "ExecError", seed: []sqlstoreWatermark{{agent: "aerial", thread: "th-1", rowID: 5}}, threadID: "th-1", maxRowID: 99, sabotage: sabotageWatermarkWrite, wantErr: true, want: []sqlstoreWatermark{{agent: "aerial", thread: "th-1", rowID: 5}}},
		{name: "InsertsNewRow", threadID: " th-1 ", maxRowID: 99, want: []sqlstoreWatermark{{agent: "aerial", thread: "th-1", rowID: 99, extracted: true}}},
		{name: "AdvancesLowerRow", seed: []sqlstoreWatermark{{agent: "aerial", thread: "th-1", rowID: 50}}, threadID: "th-1", maxRowID: 99, want: []sqlstoreWatermark{{agent: "aerial", thread: "th-1", rowID: 99, extracted: true}}},
		{name: "GreatestKeepsHigherRow", seed: []sqlstoreWatermark{{agent: "aerial", thread: "th-1", rowID: 120}}, threadID: "th-1", maxRowID: 99, want: []sqlstoreWatermark{{agent: "aerial", thread: "th-1", rowID: 120, extracted: true}}},
		{name: "PerAgentScoping", seed: []sqlstoreWatermark{{agent: "agent-beta", thread: "th-1", rowID: 500}, {agent: "aerial", thread: "th-1", rowID: 3}}, threadID: "th-1", maxRowID: 99, agent: []string{" agent-beta "}, want: []sqlstoreWatermark{{agent: "aerial", thread: "th-1", rowID: 3}, {agent: "agent-beta", thread: "th-1", rowID: 500, extracted: true}}},
		{name: "DefaultAgentLeavesOtherAgent", seed: []sqlstoreWatermark{{agent: "agent-beta", thread: "th-1", rowID: 500}}, threadID: "th-1", maxRowID: 99, want: []sqlstoreWatermark{{agent: "aerial", thread: "th-1", rowID: 99, extracted: true}, {agent: "agent-beta", thread: "th-1", rowID: 500}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := sqlstoreTestStore(t, tc.nilDB, "")
			if !tc.nilDB {
				seedSQLStoreWatermarks(t, store.DB(), tc.seed)
				execSQL(t, store.DB(), tc.sabotage)
			}
			assertSQLStoreErr(t, store.UpdateConversationFactWatermark(ctx, tc.threadID, tc.maxRowID, tc.agent...), tc.wantErr, nil)
			if tc.nilDB {
				return
			}
			rows, err := store.DB().QueryContext(ctx, `SELECT agent, thread_id, last_extracted_rowid, fact_extracted_at IS NOT NULL FROM extraction_watermarks ORDER BY agent, thread_id`)
			if err != nil {
				t.Fatalf("read watermarks: %v", err)
			}
			defer rows.Close()
			got := []sqlstoreWatermark{}
			for rows.Next() {
				var w sqlstoreWatermark
				if err := rows.Scan(&w.agent, &w.thread, &w.rowID, &w.extracted); err != nil {
					t.Fatalf("scan: %v", err)
				}
				got = append(got, w)
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("read watermarks: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("watermarks = %+v, want %+v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("watermark[%d] = %+v, want %+v", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// sqlstoreNearestFacts sit at 0°, 25.84°, 60° and 90°; cosine similarity
// to angle q is cos(deg - q).
var sqlstoreNearestFacts = []sqlstoreFact{
	{text: "a", deg: 0},
	{text: "b", deg: 25.84},
	{text: "c", deg: 60},
	{text: "d", deg: 90},
}

func TestSQLStore_FindNearestScoredFacts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		nilDB    bool
		emb      []float32
		limit    int
		minSim   float64
		sabotage string
		wantErr  bool
		want     []sqlstoreScored
	}{
		{name: "NilDBError", nilDB: true, emb: angleEmbedding(0), wantErr: true},
		{name: "InvalidEmbeddingDim", emb: []float32{1, 2}},
		{name: "QueryError", emb: angleEmbedding(0), sabotage: sabotageFacts, wantErr: true},
		{name: "DefaultsMinSim060", emb: angleEmbedding(0), limit: -1, minSim: -1, want: []sqlstoreScored{{"a", 1}, {"b", 0.9}}},
		{name: "LowerMinSim", emb: angleEmbedding(0), limit: 5, minSim: 0.4, want: []sqlstoreScored{{"a", 1}, {"b", 0.9}, {"c", 0.5}}},
		{name: "LimitOne", emb: angleEmbedding(0), limit: 1, minSim: 0.1, want: []sqlstoreScored{{"a", 1}}},
		{name: "OtherQueryAngle", emb: angleEmbedding(90), minSim: 0.4, want: []sqlstoreScored{{"d", 1}, {"c", 0.866025}, {"b", 0.435860}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := sqlstoreTestStore(t, tc.nilDB, tc.sabotage, sqlstoreNearestFacts...)
			scored, err := store.FindNearestScoredFacts(context.Background(), tc.emb, tc.limit, tc.minSim)
			assertSQLStoreErr(t, err, tc.wantErr, nil)
			texts := make([]string, 0, len(scored))
			scores := make([]float64, 0, len(scored))
			for _, s := range scored {
				texts = append(texts, s.Fact.FactText)
				scores = append(scores, s.Score)
			}
			assertScoredTexts(t, texts, scores, tc.want)
		})
	}
}

func TestFactTime_Scan(t *testing.T) {
	t.Parallel()
	pdt := time.FixedZone("PDT", -7*3600)

	tests := []struct {
		name      string
		input     any
		wantTime  time.Time
		wantValid bool
		wantErr   bool
	}{
		{name: "NullColumn", input: nil},
		{name: "TimestamptzNormalizedToUTC", input: time.Date(2026, 10, 21, 2, 43, 1, 0, pdt), wantTime: time.Date(2026, 10, 21, 9, 43, 1, 0, time.UTC), wantValid: true},
		{name: "NonTimeValueRejected", input: "2026-10-21T09:43:01Z", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ft := FactTime{Time: time.Unix(1, 0), Valid: true}
			if err := ft.Scan(tc.input); (err != nil) != tc.wantErr {
				t.Fatalf("FactTime.Scan() err = %v, wantErr %v", err, tc.wantErr)
			}
			if ft.Valid != tc.wantValid || !ft.Time.Equal(tc.wantTime) || (tc.wantValid && ft.Time.Location() != time.UTC) {
				t.Errorf("FactTime.Scan() = %+v, want {Time:%v Valid:%v}", ft, tc.wantTime, tc.wantValid)
			}
		})
	}
}

func TestEscapeSQLLikeAndHelpers(t *testing.T) {
	ctx, cancel := withTimeout(nil, 1*time.Second)
	defer cancel()
	if ctx == nil {
		t.Fatal("expected non-nil context from withTimeout(nil)")
	}

	if got := EscapeSQLLike("a%b_c\\d"); got != "a\\%b\\_c\\\\d" {
		t.Errorf("EscapeSQLLike() = %q", got)
	}
}

func TestOpenDB(t *testing.T) {
	t.Parallel()
	validDSN := pgtest.NewDSN(t)

	tests := []struct {
		name    string
		cfg     *config.Config
		wantErr bool
	}{
		{"NilConfig", nil, true},
		{"UnsupportedScheme", &config.Config{DatabaseURL: "mysql://mock_user@127.0.0.1:1/mock_db"}, true},
		{"ValidPostgres", &config.Config{DatabaseURL: validDSN}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, err := OpenDB(t.Context(), tc.cfg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("OpenDB() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if db != nil {
				_ = db.Close()
			}
		})
	}
}

func TestNewOllamaClient(t *testing.T) {
	tests := []struct {
		name      string
		cfg       *config.OllamaConfig
		wantURL   string
		wantModel string
		wantCtx   int
	}{
		{
			name:      "NilConfigEmptyURL",
			cfg:       nil,
			wantURL:   "",
			wantModel: config.DefaultOllamaModel,
			wantCtx:   config.DefaultOllamaNumCtx,
		},
		{
			name:      "EmptyFieldsEmptyURL",
			cfg:       &config.OllamaConfig{BaseURL: "  ", Model: "", NumCtx: 0},
			wantURL:   "",
			wantModel: config.DefaultOllamaModel,
			wantCtx:   config.DefaultOllamaNumCtx,
		},
		{
			name:      "CustomFieldsTrimSlash",
			cfg:       &config.OllamaConfig{BaseURL: "http://ollama:11434/", Model: "bge", NumCtx: 256},
			wantURL:   "http://ollama:11434",
			wantModel: "bge",
			wantCtx:   256,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := NewOllamaClient(tc.cfg)
			if c.baseURL != tc.wantURL || c.model != tc.wantModel || c.numCtx != tc.wantCtx {
				t.Errorf("NewOllamaClient() = %+v, want url=%q model=%q ctx=%d", c, tc.wantURL, tc.wantModel, tc.wantCtx)
			}
		})
	}
}

func TestOllamaClient_GenerateEmbedding(t *testing.T) {
	var mu sync.Mutex
	var gotPrompt string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Prompt string `json:"prompt"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		gotPrompt = req.Prompt
		mu.Unlock()
		_, _ = w.Write([]byte(`{"embedding": [0.1, 0.2, 0.3]}`))
	}))
	defer server.Close()

	tests := []struct {
		name       string
		baseURL    string
		input      string
		isQuery    bool
		prefix     string
		wantPrompt string
		wantVecLen int
		wantErr    string
	}{
		{name: "SuccessDocument", baseURL: server.URL, input: "test fact text", prefix: "search: ", wantPrompt: "test fact text", wantVecLen: 3},
		{name: "SuccessQueryWithPrefix", baseURL: server.URL, input: "search query", isQuery: true, prefix: "search: ", wantPrompt: "search: search query", wantVecLen: 3},
		{name: "UnconfiguredBaseURLError", baseURL: "", input: "test text", wantErr: "ollama base URL is not configured"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mu.Lock()
			gotPrompt = ""
			mu.Unlock()
			client := NewOllamaClient(&config.OllamaConfig{BaseURL: tc.baseURL, QueryPrefix: tc.prefix})
			vec, err := client.GenerateEmbedding(context.Background(), tc.input, tc.isQuery, 0)
			gotErr := ""
			if err != nil {
				gotErr = err.Error()
			}
			if gotErr != tc.wantErr {
				t.Fatalf("GenerateEmbedding() err = %q, want %q", gotErr, tc.wantErr)
			}
			mu.Lock()
			prompt := gotPrompt
			mu.Unlock()
			if len(vec) != tc.wantVecLen || prompt != tc.wantPrompt {
				t.Errorf("GenerateEmbedding() len = %d prompt = %q, want len %d prompt %q", len(vec), prompt, tc.wantVecLen, tc.wantPrompt)
			}
		})
	}
}
