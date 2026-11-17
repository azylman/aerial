package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/azylman/aerial/pkg/pubsub"
)

func TestPostgresVanadisStore_RecoverFailedPR(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, parent, status, mode, head string
		pr                               int
		sibling                          string
		skip                             bool
		unverified                       bool
		mergeSHA                         string
	}{
		{"single failure", "failed", "failed", "pr", "new", 12, "completed", false, false, ""},
		{"running parent", "running", "failed", "pr", "new", 12, "completed", false, false, ""},
		{"same head", "failed", "failed", "pr", "OLD", 12, "completed", true, false, ""},
		{"action", "failed", "failed", "action", "new", 12, "completed", true, false, ""},
		{"unattached", "failed", "failed", "pr", "new", 0, "completed", true, false, ""},
		{"cancelled parent", "cancelled", "failed", "pr", "new", 12, "completed", true, false, ""},
		{"completed parent", "completed", "failed", "pr", "new", 12, "completed", true, false, ""},
		{"cancelled chunk", "failed", "cancelled", "pr", "new", 12, "completed", true, false, ""},
		{"completed chunk", "failed", "completed", "pr", "new", 12, "completed", true, false, ""},
		{"unverified new head", "failed", "failed", "pr", "new", 12, "completed", true, true, ""},
		{"failed after merge", "failed", "failed", "pr", "new", 12, "completed", true, false, "merge"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, _ := dbScript{row: valuesRow("b1"), buildRow: buildRow("b1", tc.parent), chunks: []VanadisChunk{{ID: "c1", BuildID: "b1", Status: tc.status, Mode: tc.mode, PRNumber: tc.pr, HeadSHA: "old", MergeSHA: tc.mergeSHA}, {ID: "c2", Status: tc.sibling}}}.open()
			err := store.recoverFailedPR(context.WithValue(context.Background(), prHeadVerifiedContextKey{}, !tc.unverified), "c1", tc.head, 0)
			if errors.Is(err, errPRTransitionSkipped) != tc.skip || (err != nil && !tc.skip) {
				t.Fatalf("error=%v skip=%v", err, tc.skip)
			}
		})
	}
}

func TestPostgresVanadisStore_RecoverFailedPRFailures(t *testing.T) {
	t.Parallel()
	injected := errors.New("injected")
	tests := []struct {
		name   string
		db     dbScript
		at     int64
		seen   []string
		headAt time.Time
		want   error
	}{
		{name: "begin", db: dbScript{beginErr: injected}, want: injected},
		{name: "lookup", db: dbScript{row: errRow(injected)}, want: injected},
		{name: "parent", db: dbScript{buildRow: errRow(injected)}, want: injected},
		{name: "chunks", db: dbScript{queryErr: injected}, want: injected},
		{name: "chunk write", db: dbScript{failExec: "UPDATE vanadis_chunks", execErr: injected}, want: injected},
		{name: "parent write", db: dbScript{failExec: "UPDATE vanadis_builds", execErr: injected}, want: injected},
		{name: "commit", db: dbScript{commitErr: injected}, want: injected},
		{name: "verified old processing timestamp", seen: []string{"NEW"}, at: 10, headAt: time.Unix(0, 20)},
		{name: "seen head newer timestamp", seen: []string{"NEW"}, at: 30, headAt: time.Unix(0, 20)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := tc.db
			if db.row == nil {
				db.row = valuesRow("b1")
			}
			if db.buildRow == nil {
				db.buildRow = buildRow("b1", "failed")
			}
			db.chunks = []VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", PRNumber: 12, Status: "failed", HeadSHA: "old", HeadAt: tc.headAt, SeenHeads: tc.seen}}
			store, _ := db.open()
			ctx := context.WithValue(context.Background(), prHeadVerifiedContextKey{}, true)
			if err := store.recoverFailedPR(ctx, "c1", "new", tc.at); !errors.Is(err, tc.want) {
				t.Fatalf("err=%v want=%v", err, tc.want)
			}
		})
	}
}

// The database boundary keeps transaction writes private until commit.
func TestPostgresVanadisStore_PRRecoveryTransactions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		failParent bool
		wantParent string
		wantFailed int
	}{
		{"parent write rolls back last recovery", true, "failed", 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			parent := "failed"
			chunks := []VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", PRNumber: 12, Status: "failed", HeadSHA: "old"}, {ID: "c2", BuildID: "b1", Mode: "pr", PRNumber: 13, Status: "failed", HeadSHA: "old"}}
			store := recoveryFixtureStore(&mu, &parent, &chunks, tc.failParent)
			ctx := context.WithValue(context.Background(), prHeadVerifiedContextKey{}, true)
			if err := store.recoverFailedPR(ctx, "c1", "new", 0); err != nil {
				t.Fatal(err)
			}
			if err := store.recoverFailedPR(ctx, "c2", "new", 0); err == nil {
				t.Fatal("expected parent write failure")
			}
			failed := 0
			for _, c := range chunks {
				if c.Status == "failed" {
					failed++
				}
			}
			if parent != tc.wantParent || failed != tc.wantFailed {
				t.Fatalf("parent=%s failed=%d", parent, failed)
			}
		})
	}
}

func TestPostgresVanadisStore_PRRecoveryHeadUpdates(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, nextStatus string
		skip             bool
	}{
		{"open while sibling failed", "pr_opened", false},
		{"ci failure while sibling failed", "ci_failed", false},
		{"conflict while sibling failed", "conflict", false},
		{"deployment failure remains post-merge", "deploy_failed", true},
		{"merged remains terminal", "merged", true},
		{"cancelled remains terminal", "cancelled", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			parent := "failed"
			chunks := []VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", PRNumber: 12, Status: "failed", HeadSHA: "h1"}, {ID: "c2", BuildID: "b1", Mode: "pr", PRNumber: 13, Status: "failed", HeadSHA: "h1"}}
			store := recoveryFixtureStore(&mu, &parent, &chunks, false)
			if err := store.recoverFailedPR(context.WithValue(context.Background(), prHeadVerifiedContextKey{}, true), "c1", "h2", 0); err != nil {
				t.Fatal(err)
			}
			chunks[0].Status = tc.nextStatus
			err := store.recoverFailedPR(context.WithValue(context.Background(), prHeadVerifiedContextKey{}, true), "c1", "h3", 0)
			if errors.Is(err, errPRTransitionSkipped) != tc.skip || (err != nil && !tc.skip) {
				t.Fatalf("head update: %v", err)
			}
			wantHead := "h3"
			if tc.skip {
				wantHead = "h2"
			}
			if parent != "failed" || chunks[0].HeadSHA != wantHead {
				t.Fatalf("parent=%s head=%s", parent, chunks[0].HeadSHA)
			}
			if err := store.recoverFailedPR(context.WithValue(context.Background(), prHeadVerifiedContextKey{}, true), "c2", "h2", 0); err != nil {
				t.Fatal(err)
			}
			wantParent := "running"
			if tc.nextStatus == "cancelled" {
				wantParent = "failed"
			}
			if parent != wantParent || chunks[0].HeadSHA != wantHead {
				t.Fatalf("after sibling recovery parent=%s head=%s", parent, chunks[0].HeadSHA)
			}
		})
	}
}

type recoveryReadFailureStore struct{ *PostgresVanadisStore }

func (recoveryReadFailureStore) GetBuild(context.Context, string) (*VanadisBuild, []VanadisChunk, error) {
	return nil, nil, errors.New("read unavailable")
}

func TestBuildObject_PRRecoveryAfterRejectedCheck(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, event string }{{"check update", "check_updated"}, {"check failure", "check_failed"}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			parent := "failed"
			chunks := []VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", PRNumber: 12, Status: "failed", HeadSHA: "h1"}}
			store := recoveryReadFailureStore{recoveryFixtureStore(&mu, &parent, &chunks, false)}
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`{"state":"open","merged":false,"head":{"sha":"h2"}}`))
			}))
			t.Cleanup(api.Close)
			executor := NewExecutor("", nil, nil, "", api.Client())
			executor.gitHubAPIURL = api.URL
			bo := NewBuildObject("", nil).WithAuthority(store, executor)
			ctx := mockRunContext{Context: context.Background()}
			ev := pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "h2", CheckName: "lint", CheckState: "passed"}
			if err := bo.handlePREventType(ctx, "b1", "c1", tc.event, ev); !errors.Is(err, errPRTransitionSkipped) {
				t.Fatalf("check error=%v", err)
			}
			if err := bo.handlePREventType(ctx, "b1", "c1", "synchronize", ev); err != nil {
				t.Fatalf("recovery error=%v", err)
			}
			if parent != "running" || chunks[0].Status != "pr_opened" || chunks[0].HeadSHA != "h2" {
				t.Fatalf("recovered parent=%s chunk=%+v", parent, chunks[0])
			}
		})
	}
}

type recoveryLifecycleStore struct {
	*PostgresVanadisStore

	parent *string
	chunks *[]VanadisChunk
}

func (s recoveryLifecycleStore) GetBuild(context.Context, string) (*VanadisBuild, []VanadisChunk, error) {
	return &VanadisBuild{ID: "b1", Status: *s.parent}, *s.chunks, nil
}

func (s recoveryLifecycleStore) FailChunk(context.Context, string, string, string) (*VanadisBuild, *VanadisChunk, bool, error) {
	*s.parent = "failed"
	(*s.chunks)[0].Status = "failed"
	return &VanadisBuild{ID: "b1", Status: "failed"}, &(*s.chunks)[0], true, nil
}

func TestBuildObject_PRRecoveryAfterClosure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, body, wantParent, wantChunk, wantHead string
		wantError, wantSkipped                      bool
	}{
		{"still closed", `{"state":"closed","merged":false,"head":{"sha":"h2"}}`, "failed", "failed", "h1", true, true},
		{"inconsistent merged open", `{"state":"open","merged":true,"head":{"sha":"h2"}}`, "failed", "failed", "h1", true, true},
		{"missing state", `{"merged":false,"head":{"sha":"h2"}}`, "failed", "failed", "h1", true, false},
		{"missing merged flag", `{"state":"open","head":{"sha":"h2"}}`, "failed", "failed", "h1", true, false},
		{"reopened on newer head", `{"state":"open","merged":false,"head":{"sha":"h2"}}`, "running", "pr_opened", "h2", false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			parent := "running"
			chunks := []VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", PRNumber: 12, Status: "pr_opened", HeadSHA: "h1"}}
			store := recoveryLifecycleStore{recoveryFixtureStore(&mu, &parent, &chunks, false), &parent, &chunks}
			bo := NewBuildObject("", nil).WithAuthority(store, nil)
			ctx := mockRunContext{Context: context.Background()}
			if err := bo.handlePREventType(ctx, "b1", "c1", "closed_unmerged", pubsub.PRLifecycleEvent{HeadSHA: "h2"}); err != nil {
				t.Fatal(err)
			}
			if parent != "failed" || chunks[0].Status != "failed" || chunks[0].HeadSHA != "h1" {
				t.Fatalf("closure parent=%s chunk=%+v", parent, chunks[0])
			}
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(tc.body)) }))
			t.Cleanup(api.Close)
			executor := NewExecutor("", nil, nil, "", api.Client())
			executor.gitHubAPIURL = api.URL
			bo.WithAuthority(store, executor)
			err := bo.handlePREventType(ctx, "b1", "c1", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "h2"})
			if (err != nil) != tc.wantError || errors.Is(err, errPRTransitionSkipped) != tc.wantSkipped {
				t.Fatalf("sync error=%v", err)
			}
			if parent != tc.wantParent || chunks[0].Status != tc.wantChunk || chunks[0].HeadSHA != tc.wantHead {
				t.Fatalf("sync parent=%s chunk=%+v", parent, chunks[0])
			}
		})
	}
}

func TestBuildObject_PRRecoveryWidget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, event, parent string
		wantWidget          bool
	}{
		{"failed synchronize", "synchronize", "failed", true},
		{"failed opened", "opened", "failed", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, _ := dbScript{buildRow: buildRow("b1", tc.parent), routes: []sqlRoute{{match: "c.repo = ANY($1) AND c.pr_number = $2", row: valuesRow("c1", "b1", 0, "medium", true)}}, chunks: []VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", Status: "pr_opened", PRNumber: 12, HeadSHA: "old"}, {ID: "c2", BuildID: "b1", Mode: "pr", Status: "failed"}}}.open()
			var observed *StatusWidgetRequest
			eri := &mockEriClient{widgetFn: func(_ context.Context, req StatusWidgetRequest) (string, error) {
				observed = &req
				return "widget", nil
			}}
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`{"state":"open","merged":false,"head":{"sha":"new"}}`))
			}))
			t.Cleanup(api.Close)
			executor := NewExecutor("", eri, nil, "", nil)
			executor.gitHubAPIURL = api.URL
			executor.httpClient = api.Client()
			bo := NewBuildObject("", nil).WithAuthority(store, executor)
			err := bo.OnPREvent(makeObjectMockContext(t, "b1"), pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, Event: tc.event, HeadSHA: "new"})
			if err != nil {
				t.Fatal(err)
			}
			if observed == nil || observed.Status != "failed" || len(observed.Chunks) != 2 || observed.Chunks[0].Status != "open" {
				t.Fatalf("widget=%+v", observed)
			}
		})
	}
}

func recoveryFixtureStore(mu *sync.Mutex, parent *string, chunks *[]VanadisChunk, failParent bool) *PostgresVanadisStore {
	return newMockStore(mockDB{execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
		return pgconn.NewCommandTag("UPDATE 0"), nil
	}, beginFn: func(context.Context) (pgx.Tx, error) {
		var local []VanadisChunk
		var state string
		locked, closed := false, false
		tx := &mockTx{
			queryRowFn: func(_ context.Context, q string, _ ...any) pgx.Row {
				if strings.Contains(q, "SELECT build_id") {
					return valuesRow("b1")
				}
				mu.Lock()
				locked = true
				state = *parent
				local = append([]VanadisChunk(nil), (*chunks)...)
				return buildRow("b1", state)
			}, queryFn: func(context.Context, string, ...any) (pgx.Rows, error) { return &mockRows{items: local}, nil },
			execFn: func(_ context.Context, q string, args ...any) (pgconn.CommandTag, error) {
				if !locked {
					return pgconn.CommandTag{}, errors.New("write without lock")
				}
				if strings.Contains(q, "UPDATE vanadis_chunks") {
					applyRecoveryFixtureHead(local, args)
				} else {
					if failParent {
						return pgconn.CommandTag{}, errors.New("parent write failed")
					}
					state = "running"
				}
				return pgconn.NewCommandTag("UPDATE 1"), nil
			},
		}
		return &retryTransaction{mockTx: tx, finish: func(commit bool) error {
			if closed {
				return pgx.ErrTxClosed
			}
			closed = true
			if locked {
				defer mu.Unlock()
			}
			if commit {
				*chunks = local
				*parent = state
			}
			return nil
		}}, nil
	}})
}

func recoveryFixtureChecks(c VanadisChunk) []byte {
	if c.HeadSHA == "" {
		return []byte(`{"lint":"passed"}`)
	}
	checks := map[string]string{"_head_sha": c.HeadSHA, "_merge_sha": c.MergeSHA, "_seen_heads": strings.Join(c.SeenHeads, ",")}
	if !c.HeadAt.IsZero() {
		checks["_head_at"] = strconv.FormatInt(c.HeadAt.UnixNano(), 10)
	}
	raw, _ := json.Marshal(checks)
	return raw
}

func applyRecoveryFixtureHead(chunks []VanadisChunk, args []any) {
	for i := range chunks {
		if chunks[i].ID == args[0] {
			chunks[i].Status = "pr_opened"
			chunks[i].HeadSHA, _ = args[1].(string)
		}
	}
}
