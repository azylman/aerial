package sqltest

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

var errBoom = errors.New("boom")

// deref returns the values the destination pointers hold.
func deref(dest []any) []any {
	out := make([]any, len(dest))
	for i, d := range dest {
		out[i] = reflect.ValueOf(d).Elem().Interface()
	}
	return out
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestScriptQueryRow(t *testing.T) {
	t.Parallel()
	chunk := model.VanadisChunk{ID: "c1", BuildID: "b1", ChunkIndex: 2, Mode: "pr", Title: "T", Status: "running", PRNumber: 7, StallNudges: 1}
	build := &Row{Build: &model.VanadisBuild{ID: "b9"}}
	tests := []struct {
		name           string
		script         Script
		query          string
		args           []any
		want           pgx.Row
		wantCandidates []string
	}{
		{name: "ci fail route", script: Script{Routes: []Route{{"RETURNING ci_fail_count", ValuesRow(4)}}}, query: "UPDATE x RETURNING ci_fail_count", want: ValuesRow(4)},
		{name: "ci fail row", script: Script{Row: ValuesRow(5)}, query: "RETURNING ci_fail_count", want: ValuesRow(5)},
		{name: "ci fail skipped", script: Script{ExecTag: "UPDATE 0"}, query: "RETURNING ci_fail_count", want: ErrRow(pgx.ErrNoRows)},
		{name: "ci fail default", query: "RETURNING ci_fail_count", want: ValuesRow(1)},
		{name: "route and candidates", script: Script{Routes: []Route{{"SELECT mode", ValuesRow("pr")}}}, query: "SELECT mode", args: []any{1, []string{"a", "b"}, []string{"c"}}, want: ValuesRow("pr"), wantCandidates: []string{"a", "b"}},
		{name: "chunk summary", script: Script{Chunks: []model.VanadisChunk{chunk}}, query: "SELECT COALESCE(pr_number, 0), stall_nudges FROM vanadis_chunks", want: ValuesRow("c1", "b1", 2, "pr", "T", "running", 7, 1)},
		{name: "chunk summary yields to values row", script: Script{Chunks: []model.VanadisChunk{chunk}, Row: ValuesRow("x")}, query: "COALESCE(pr_number, 0), stall_nudges FROM vanadis_chunks", want: ValuesRow("x")},
		{name: "build id lookup", query: "SELECT id FROM vanadis_builds WHERE id = (SELECT build_id FROM vanadis_chunks)", want: ValuesRow("b1")},
		{name: "count row", script: Script{CountRow: ValuesRow(3), BuildRow: build}, query: "SELECT COUNT(*) FROM vanadis_builds", want: ValuesRow(3)},
		{name: "build row", script: Script{BuildRow: build, Row: ValuesRow("r")}, query: "SELECT id FROM vanadis_builds WHERE id = $1", want: build},
		{name: "fallback row", script: Script{Row: ValuesRow("r")}, query: "SELECT 1", want: ValuesRow("r")},
		{name: "no row", query: "SELECT 1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, candidates := tc.script.DB()
			got := db.QueryRow(context.Background(), tc.query, tc.args...)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("QueryRow = %#v, want %#v", got, tc.want)
			}
			if !slices.Equal(*candidates, tc.wantCandidates) {
				t.Errorf("candidates = %v, want %v", *candidates, tc.wantCandidates)
			}
		})
	}
}

func TestScriptDBExec(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, query string
		script      Script
		wantTag     string
		wantErr     string
	}{
		{name: "default tag", query: "UPDATE x", wantTag: "UPDATE 1"},
		{name: "scripted tag", query: "UPDATE x", script: Script{ExecTag: "INSERT 0 0"}, wantTag: "INSERT 0 0"},
		{name: "matching failure", query: "INSERT INTO vanadis_chunks", script: Script{ExecErr: errBoom, FailExec: "vanadis_chunks"}, wantErr: "boom"},
		{name: "other statement", query: "INSERT INTO vanadis_builds", script: Script{ExecErr: errBoom, FailExec: "vanadis_chunks"}, wantTag: "UPDATE 1"},
		{name: "fail every statement", query: "UPDATE x", script: Script{ExecErr: errBoom}, wantErr: "boom"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, _ := tc.script.DB()
			tag, err := db.Exec(context.Background(), tc.query)
			if errText(err) != tc.wantErr || tag.String() != tc.wantTag {
				t.Errorf("Exec = %q, %v; want %q, %q", tag, err, tc.wantTag, tc.wantErr)
			}
		})
	}
}

func TestScriptDBQuery(t *testing.T) {
	t.Parallel()
	chunks := []model.VanadisChunk{{ID: "c1"}}
	builds := []model.VanadisBuild{{ID: "b1"}}
	tests := []struct {
		name     string
		script   Script
		wantRows pgx.Rows
		wantErr  string
	}{
		{name: "query error", script: Script{QueryErr: errBoom}, wantErr: "boom"},
		{name: "scripted rows", script: Script{Chunks: chunks, Builds: builds, ScanErr: errBoom, RowsErr: pgx.ErrTxClosed}, wantRows: &Rows{Items: chunks, Builds: builds, ScanErr: errBoom, RowsErr: pgx.ErrTxClosed}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, _ := tc.script.DB()
			rows, err := db.Query(context.Background(), "SELECT")
			if errText(err) != tc.wantErr {
				t.Fatalf("Query err = %v, want %q", err, tc.wantErr)
			}
			if err != nil {
				return
			}
			defer rows.Close()
			if !reflect.DeepEqual(rows, tc.wantRows) {
				t.Errorf("Query = %#v, want %#v", rows, tc.wantRows)
			}
		})
	}
}

func TestScriptDBBegin(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                      string
		script                    Script
		wantErr                   string
		wantTag                   string
		wantRow                   pgx.Row
		wantCommit, wantRollback  string
		wantQueryErr, wantRowsErr string
	}{
		{name: "begin error", script: Script{BeginErr: errBoom}, wantErr: "boom"},
		{name: "transaction shares the script", script: Script{ExecTag: "DELETE 2", Row: ValuesRow("r"), CommitErr: errBoom, RollbackErr: pgx.ErrTxClosed, RowsErr: errBoom}, wantTag: "DELETE 2", wantRow: ValuesRow("r"), wantCommit: "boom", wantRollback: "tx is closed", wantRowsErr: "boom"},
		{name: "transaction query error", script: Script{QueryErr: errBoom}, wantTag: "UPDATE 1", wantQueryErr: "boom"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db, _ := tc.script.DB()
			tx, err := db.Begin(ctx)
			if errText(err) != tc.wantErr {
				t.Fatalf("Begin err = %v, want %q", err, tc.wantErr)
			}
			if err != nil {
				return
			}
			tag, _ := tx.Exec(ctx, "UPDATE x")
			row := tx.QueryRow(ctx, "SELECT 1")
			rows, qerr := tx.Query(ctx, "SELECT")
			if tag.String() != tc.wantTag || !reflect.DeepEqual(row, tc.wantRow) || errText(qerr) != tc.wantQueryErr {
				t.Errorf("tx Exec/QueryRow/Query = %q, %#v, %v", tag, row, qerr)
			}
			if rows != nil {
				rows.Close()
				if errText(rows.Err()) != tc.wantRowsErr {
					t.Errorf("rows.Err = %v, want %q", rows.Err(), tc.wantRowsErr)
				}
			}
			if c, r := tx.Commit(ctx), tx.Rollback(ctx); errText(c) != tc.wantCommit || errText(r) != tc.wantRollback {
				t.Errorf("Commit/Rollback = %v/%v, want %q/%q", c, r, tc.wantCommit, tc.wantRollback)
			}
		})
	}
}

// executor is the statement surface DB and Tx share.
type executor interface {
	Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, query string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) pgx.Row
}

func TestZeroFakesSucceed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		fake    executor
		wantTag string
	}{
		{name: "db", fake: DB{}, wantTag: "UPDATE 1"},
		{name: "tx", fake: &Tx{}, wantTag: "INSERT 0 1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			tag, err := tc.fake.Exec(ctx, "UPDATE")
			rows, qerr := tc.fake.Query(ctx, "SELECT")
			if qerr != nil {
				t.Fatalf("Query err = %v", qerr)
			}
			defer rows.Close()
			row := tc.fake.QueryRow(ctx, "SELECT")
			if tag.String() != tc.wantTag || err != nil || !reflect.DeepEqual(rows, &Rows{}) || !reflect.DeepEqual(row, &Row{}) {
				t.Errorf("zero fake = %q %v / %#v / %#v", tag, err, rows, row)
			}
		})
	}
}

func TestZeroDBBegin(t *testing.T) {
	t.Parallel()
	tx, err := DB{}.Begin(context.Background())
	if err != nil || !reflect.DeepEqual(tx, &Tx{}) {
		t.Fatalf("Begin = %#v, %v; want zero Tx", tx, err)
	}
}

func TestScanValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		dest    []any
		vals    []any
		want    []any
		wantErr string
	}{
		{name: "assigns in order", dest: []any{new(string), new(int), new(bool)}, vals: []any{"a", 2}, want: []any{"a", 2, false}},
		{name: "nil keeps destination", dest: []any{new(string), new(int)}, vals: []any{nil, 3}, want: []any{"", 3}},
		{name: "too many values", dest: []any{new(string)}, vals: []any{"a", "b"}, want: []any{""}, wantErr: "scan: 2 values for 1 destinations"},
		{name: "type mismatch", dest: []any{new(string)}, vals: []any{1}, want: []any{""}, wantErr: "scan column 0: destination *string cannot hold int"},
		{name: "nil pointer", dest: []any{(*string)(nil)}, vals: []any{"a"}, want: []any{(*string)(nil)}, wantErr: "scan column 0: destination *string cannot hold string"},
		{name: "non-pointer", dest: []any{"x"}, vals: []any{"a"}, want: []any{"x"}, wantErr: "scan column 0: destination string cannot hold string"},
		{name: "values row", dest: []any{new([]string)}, vals: []any{[]string{"x"}}, want: []any{[]string{"x"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValuesRow(tc.vals...).Scan(tc.dest...)
			if errText(err) != tc.wantErr {
				t.Fatalf("Scan err = %v, want %q", err, tc.wantErr)
			}
			got := make([]any, len(tc.dest))
			for i, d := range tc.dest {
				if v := reflect.ValueOf(d); v.Kind() == reflect.Pointer && !v.IsNil() {
					got[i] = v.Elem().Interface()
				} else {
					got[i] = d
				}
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("scanned %#v, want %#v", got, tc.want)
			}
		})
	}
}

func buildDest12() []any {
	return []any{new(string), new(string), new(string), new(int), new(string), new(string), new(string), new(string), new(string), new(bool), new(time.Time), new(time.Time)}
}

func TestRowScan(t *testing.T) {
	t.Parallel()
	b := &model.VanadisBuild{ID: "b1", ThreadID: "th", Tier: 2, Title: "T", Objective: "O", Status: "running", StatusMessageID: "m"}
	tests := []struct {
		name    string
		row     *Row
		dest    []any
		want    []any
		wantErr string
	}{
		{name: "error", row: ErrRow(errBoom), dest: []any{new(string)}, want: []any{""}, wantErr: "boom"},
		{name: "empty row", row: &Row{}, dest: []any{new(string)}, want: []any{""}},
		{name: "status only", row: &Row{Build: b}, dest: []any{new(string)}, want: []any{"running"}},
		{name: "summary", row: &Row{Build: b}, dest: []any{new(string), new(string), new(int), new(string), new(string), new(string), new(string)}, want: []any{"b1", "th", 2, "T", "O", "running", "m"}},
		{name: "wrong width", row: &Row{Build: b}, dest: []any{new(string), new(string)}, want: []any{"", ""}, wantErr: "scan build: 2 destinations, want 12"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.row.Scan(tc.dest...)
			if errText(err) != tc.wantErr || !reflect.DeepEqual(deref(tc.dest), tc.want) {
				t.Errorf("Scan = %#v, %v; want %#v, %q", deref(tc.dest), err, tc.want, tc.wantErr)
			}
		})
	}
}

func TestFillBuildDest(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	tests := []struct {
		name  string
		build model.VanadisBuild
		want  []any
	}{
		{name: "default agent", build: model.VanadisBuild{ID: "b1", Status: "failed"}, want: []any{"b1", "aerial", "", 0, "", "", "", "failed", "", false, time.Time{}, time.Time{}}},
		{name: "all columns", build: model.VanadisBuild{ID: "b2", Agent: "beta", ThreadID: "th", Tier: 3, Title: "T", Objective: "O", SharedContext: "S", Status: "running", StatusMessageID: "m", Quiet: true, CreatedAt: at, UpdatedAt: at}, want: []any{"b2", "beta", "th", 3, "T", "O", "S", "running", "m", true, at, at}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dest := buildDest12()
			if err := FillBuildDest(tc.build, dest); err != nil || !reflect.DeepEqual(deref(dest), tc.want) {
				t.Errorf("FillBuildDest = %#v, %v; want %#v", deref(dest), err, tc.want)
			}
		})
	}
}

func TestRowBuilders(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		row    pgx.Row
		width  int
		wantAt map[int]any
	}{
		{name: "build row", row: BuildRow("b7", "failed"), width: 12, wantAt: map[int]any{0: "b7", 1: "aerial", 3: 3, 7: "failed", 9: false}},
		{name: "fail row defaults", row: FailRow("running", "failed"), width: 28, wantAt: map[int]any{0: "c1", 3: "action", 11: "running", 12: 0, 17: 0, 24: "failed"}},
		{name: "fail row generation", row: FailRow("failed", "running", 2, 9), width: 28, wantAt: map[int]any{11: "failed", 12: 9, 17: 2, 24: "running"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			vals, ok := tc.row.(Values)
			if !ok {
				t.Fatalf("row %T is not Values", tc.row)
			}
			if len(vals) != tc.width {
				t.Fatalf("width = %d, want %d", len(vals), tc.width)
			}
			for i, want := range tc.wantAt {
				if !reflect.DeepEqual(vals[i], want) {
					t.Errorf("column %d = %#v, want %#v", i, vals[i], want)
				}
			}
		})
	}
}

func TestAttachRoutes(t *testing.T) {
	t.Parallel()
	want := []Route{{"SELECT mode", Values{"action"}}, {"c.status = 'cancelled'", Values{true}}, {"SELECT EXISTS", Values{false}}}
	if got := AttachRoutes("action", false, true); !reflect.DeepEqual(got, want) {
		t.Fatalf("AttachRoutes = %#v, want %#v", got, want)
	}
}

func TestRecoveryChecks(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		chunk model.VanadisChunk
		want  string
	}{
		{name: "no shas", want: `{"lint":"passed"}`},
		{name: "head only", chunk: model.VanadisChunk{HeadSHA: "h"}, want: `{"_head_sha":"h","_merge_sha":""}`},
		{name: "checks keep only set shas", chunk: model.VanadisChunk{CIChecks: map[string]string{"test": "failed"}, MergeSHA: "m"}, want: `{"_merge_sha":"m","test":"failed"}`},
		{name: "checks with both shas", chunk: model.VanadisChunk{CIChecks: map[string]string{"test": "passed"}, HeadSHA: "h", MergeSHA: "m"}, want: `{"_head_sha":"h","_merge_sha":"m","test":"passed"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := string(RecoveryChecks(tc.chunk)); got != tc.want {
				t.Errorf("RecoveryChecks = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestRowsNext(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		rows Rows
		want []bool
	}{
		{name: "empty", want: []bool{false}},
		{name: "builds", rows: Rows{Builds: []model.VanadisBuild{{ID: "b1"}, {ID: "b2"}}}, want: []bool{true, true, false}},
		{name: "items win over builds", rows: Rows{Items: []model.VanadisChunk{{ID: "c1"}}, Builds: []model.VanadisBuild{{ID: "b1"}, {ID: "b2"}}}, want: []bool{true, false}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := make([]bool, len(tc.want))
			for i := range got {
				got[i] = tc.rows.Next()
			}
			tc.rows.Close()
			if !slices.Equal(got, tc.want) || tc.rows.Err() != nil {
				t.Errorf("Next = %v, want %v (err %v)", got, tc.want, tc.rows.Err())
			}
		})
	}
}

func TestRowsScan(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	chunk := model.VanadisChunk{ID: "c1", BuildID: "b1", ChunkIndex: 1, Mode: "pr", Repo: "aerial", Title: "T", Status: "running", PRNumber: 5, StartedAt: at, UpdatedAt: at, StallNudges: 2, CIFailCount: 1}
	rolled := chunk
	rolled.RolloutState = &pubsub.PRRolloutState{ExpectedJobs: []string{"web"}}
	items := []model.VanadisChunk{chunk}
	tests := []struct {
		name    string
		rows    Rows
		next    int
		width   int
		wantAt  map[int]any
		wantErr string
	}{
		{name: "scan error", rows: Rows{Items: items, ScanErr: errBoom}, next: 1, width: 2, wantErr: "boom"},
		{name: "ids", rows: Rows{Items: items}, next: 1, width: 3, wantAt: map[int]any{0: "b1", 1: "c1"}},
		{name: "ids before next", rows: Rows{Items: items}, width: 2, wantAt: map[int]any{0: "", 1: ""}},
		{name: "summary", rows: Rows{Items: items}, next: 1, width: 10, wantAt: map[int]any{0: "c1", 1: 1, 3: "aerial", 6: 5, 7: at}},
		{name: "summary before next", rows: Rows{Items: items}, width: 10, wantAt: map[int]any{0: ""}},
		{name: "build from builds", rows: Rows{Builds: []model.VanadisBuild{{ID: "b9", Status: "failed"}}}, next: 1, width: 12, wantAt: map[int]any{0: "b9", 1: "aerial", 7: "failed"}},
		{name: "build from chunk", rows: Rows{Items: items}, next: 1, width: 12, wantAt: map[int]any{0: "b1", 4: "T"}},
		{name: "build before next", width: 12, wantAt: map[int]any{0: ""}},
		{name: "chunk", rows: Rows{Items: items}, next: 1, width: 18, wantAt: map[int]any{0: "c1", 13: 2, 14: sql.NullTime{Time: at, Valid: true}, 15: sql.NullTime{}}},
		{name: "chunk before next", rows: Rows{Items: items}, width: 18, wantAt: map[int]any{0: ""}},
		{name: "full chunk", rows: Rows{Items: items}, next: 1, width: 25, wantAt: map[int]any{0: "c1", 15: 1, 16: []byte(`{"lint":"passed"}`), 17: []byte(`{"expected_jobs":["eri"]}`)}},
		{name: "full chunk with rollout", rows: Rows{Items: []model.VanadisChunk{rolled}}, next: 1, width: 26, wantAt: map[int]any{0: "c1", 17: []byte(`{"expected_jobs":["web"]}`)}},
		{name: "full chunk default", width: 25, wantAt: map[int]any{0: "c1", 1: "b1", 11: "pending", 16: []byte("{}")}},
		{name: "unknown width", rows: Rows{Items: items}, next: 1, width: 4, wantAt: map[int]any{0: ""}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dest := scanDest(tc.width)
			for range tc.next {
				tc.rows.Next()
			}
			if err := tc.rows.Scan(dest...); errText(err) != tc.wantErr {
				t.Fatalf("Scan err = %v, want %q", err, tc.wantErr)
			}
			for i, want := range tc.wantAt {
				if got := reflect.ValueOf(dest[i]).Elem().Interface(); !reflect.DeepEqual(got, want) {
					t.Errorf("column %d = %#v, want %#v", i, got, want)
				}
			}
		})
	}
}

// scanDest returns typed destinations matching the store's scan shapes.
func scanDest(width int) []any {
	switch width {
	case 10:
		return []any{new(string), new(int), new(string), new(string), new(string), new(string), new(int), new(time.Time), new(time.Time), new(time.Time)}
	case 12:
		return buildDest12()
	case 18:
		return []any{new(string), new(string), new(int), new(string), new(string), new(string), new(string), new([]string), new([]string), new(string), new([]string), new(string), new(int), new(int), new(sql.NullTime), new(sql.NullTime), new(time.Time), new(time.Time)}
	case 25, 26:
		dest := make([]any, 0, width)
		dest = append(dest, new(string), new(string), new(int), new(string), new(string), new(string), new(string), new([]string), new([]string), new(string), new([]string), new(string), new(int), new(int), new([]string), new(int), new([]byte), new([]byte), new(string), new(string), new(string), new(sql.NullTime), new(sql.NullTime), new(time.Time), new(time.Time))
		return append(dest, make([]any, width-25)...)
	}
	dest := make([]any, width)
	for i := range dest {
		dest[i] = new(string)
	}
	return dest
}

const retryResetSQL = "UPDATE vanadis_chunks SET status = 'pending', stall_nudges = stall_nudges + 1, repo = '', pr_number = NULL, started_at = NULL, completed_at = NULL, ci_fail_count = 0, ci_checks = '{}'::jsonb, rollout_state = '{}'::jsonb, banner = '', merge_awakeable_id = '', deploy_awakeable_id = '', instructions = COALESCE($2, instructions)"

const (
	lockParentSQL = "SELECT id FROM vanadis_builds WHERE id = $1 FOR UPDATE"
	lockChunksSQL = "SELECT id FROM vanadis_chunks ORDER BY chunk_index ASC FOR UPDATE"
)

func TestRetryFixtureDB(t *testing.T) {
	t.Parallel()
	replacement := "new instructions"
	start := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", Repo: "aerial", PRNumber: 4, Instructions: "old"}
	reset := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "pending", StallNudges: 1, Instructions: "new instructions"}
	tests := []struct {
		name          string
		parentSQL     string
		chunksSQL     string
		execs         []string
		args          []any
		commit        bool
		failBuild     bool
		lost          bool
		wantLockErr   string
		wantQueryErr  string
		wantExecErr   string
		wantFinishErr string
		wantChunk     model.VanadisChunk
		wantStatus    string
	}{
		{name: "parent not locked", parentSQL: "SELECT id FROM vanadis_builds", chunksSQL: lockChunksSQL, wantLockErr: "missing parent lock", wantQueryErr: "missing ordered chunk locks", wantFinishErr: "transaction never locked parent", wantChunk: start, wantStatus: "failed"},
		{name: "chunks not ordered", parentSQL: lockParentSQL, chunksSQL: "SELECT id FROM vanadis_chunks FOR UPDATE", wantQueryErr: "missing ordered chunk locks", commit: true, wantChunk: start, wantStatus: "failed"},
		{name: "missing reset", parentSQL: lockParentSQL, chunksSQL: lockChunksSQL, execs: []string{"UPDATE vanadis_chunks SET status = 'pending'"}, wantExecErr: "missing reset stall_nudges = stall_nudges + 1", wantChunk: start, wantStatus: "failed"},
		{name: "unexpected mutation", parentSQL: lockParentSQL, chunksSQL: lockChunksSQL, execs: []string{"DELETE FROM vanadis_chunks"}, wantExecErr: "unexpected mutation", wantChunk: start, wantStatus: "failed"},
		{name: "build write fails", parentSQL: lockParentSQL, chunksSQL: lockChunksSQL, execs: []string{"UPDATE vanadis_builds SET status = 'running'"}, failBuild: true, wantExecErr: "build write failed", wantChunk: start, wantStatus: "failed"},
		{name: "rollback discards", parentSQL: lockParentSQL, chunksSQL: lockChunksSQL, execs: []string{retryResetSQL, "UPDATE vanadis_builds SET status = 'running'"}, args: []any{"c1", &replacement}, wantChunk: start, wantStatus: "failed"},
		{name: "commit applies", parentSQL: lockParentSQL, chunksSQL: lockChunksSQL, execs: []string{retryResetSQL, "UPDATE vanadis_builds SET status = 'running'"}, args: []any{"c1", &replacement}, commit: true, wantChunk: reset, wantStatus: "running"},
		{name: "lost commit still applies", parentSQL: lockParentSQL, chunksSQL: lockChunksSQL, execs: []string{retryResetSQL}, args: []any{"c1", (*string)(nil)}, commit: true, lost: true, wantFinishErr: "commit response lost", wantChunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "pending", StallNudges: 1, Instructions: "old"}, wantStatus: "failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			current, status, lost := start, "failed", tc.lost
			db := RetryFixtureDB(&mu, &current, &status, model.VanadisChunk{ID: "c0", Status: "completed"}, tc.failBuild, &lost)
			got := runRetryTx(t, db, tc.parentSQL, tc.chunksSQL, tc.execs, tc.args, tc.commit)
			want := retryTxErrs{tc.wantLockErr, tc.wantQueryErr, tc.wantExecErr, tc.wantFinishErr, "tx is closed"}
			if got != want {
				t.Errorf("errors = %+v, want %+v", got, want)
			}
			if !mu.TryLock() {
				t.Error("fixture mutex still held after finish")
			}
			if !reflect.DeepEqual(current, tc.wantChunk) || status != tc.wantStatus || lost {
				t.Errorf("state = %+v %q lost=%v; want %+v %q", current, status, lost, tc.wantChunk, tc.wantStatus)
			}
		})
	}
}

// retryTxErrs is the error text of each step runRetryTx takes.
type retryTxErrs struct{ lock, query, exec, finish, again string }

// runRetryTx locks the parent, locks the chunks, runs execs (stopping at the
// first error), finishes the transaction and finishes it a second time.
func runRetryTx(t *testing.T, db DB, parentSQL, chunksSQL string, execs []string, args []any, commit bool) retryTxErrs {
	t.Helper()
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got retryTxErrs
	got.lock = errText(tx.QueryRow(ctx, parentSQL).Scan(buildDest12()...))
	rows, qerr := tx.Query(ctx, chunksSQL)
	got.query = errText(qerr)
	if rows != nil {
		if !rows.Next() {
			t.Error("locked rows are empty")
		}
		rows.Close()
	}
	for _, q := range execs {
		if _, eerr := tx.Exec(ctx, q, args...); eerr != nil {
			got.exec = eerr.Error()
			break
		}
	}
	finish := tx.Rollback
	if commit {
		finish = tx.Commit
	}
	got.finish = errText(finish(ctx))
	got.again = errText(tx.Rollback(ctx))
	return got
}
