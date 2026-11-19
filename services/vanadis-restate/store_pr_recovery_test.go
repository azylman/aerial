package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// The database boundary keeps transaction writes private until commit.
type recoveryReadFailureStore struct{ *store.Store }

func (recoveryReadFailureStore) GetBuild(context.Context, string) (*model.VanadisBuild, []model.VanadisChunk, error) {
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
			chunks := []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", PRNumber: 12, Status: "failed", HeadSHA: "h1"}}
			st := recoveryReadFailureStore{recoveryFixtureStore(&mu, &parent, &chunks, false)}
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`{"state":"open","merged":false,"head":{"sha":"h2"}}`))
			}))
			t.Cleanup(api.Close)
			exec := executor.NewExecutor("", nil, nil, "", api.Client())
			exec.GitHubAPIURL = api.URL
			bo := NewBuildObject("", nil).WithAuthority(st, exec)
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
	*store.Store

	parent *string
	chunks *[]model.VanadisChunk
}

func (s recoveryLifecycleStore) GetBuild(context.Context, string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	return &model.VanadisBuild{ID: "b1", Status: *s.parent}, *s.chunks, nil
}

func (s recoveryLifecycleStore) FailChunk(context.Context, string, string, string) (*model.VanadisBuild, *model.VanadisChunk, bool, error) {
	*s.parent = "failed"
	(*s.chunks)[0].Status = "failed"
	return &model.VanadisBuild{ID: "b1", Status: "failed"}, &(*s.chunks)[0], true, nil
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
			chunks := []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", PRNumber: 12, Status: "pr_opened", HeadSHA: "h1"}}
			st := recoveryLifecycleStore{recoveryFixtureStore(&mu, &parent, &chunks, false), &parent, &chunks}
			bo := NewBuildObject("", nil).WithAuthority(st, nil)
			ctx := mockRunContext{Context: context.Background()}
			if err := bo.handlePREventType(ctx, "b1", "c1", "closed_unmerged", pubsub.PRLifecycleEvent{HeadSHA: "h2"}); err != nil {
				t.Fatal(err)
			}
			if parent != "failed" || chunks[0].Status != "failed" || chunks[0].HeadSHA != "h1" {
				t.Fatalf("closure parent=%s chunk=%+v", parent, chunks[0])
			}
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(tc.body)) }))
			t.Cleanup(api.Close)
			exec := executor.NewExecutor("", nil, nil, "", api.Client())
			exec.GitHubAPIURL = api.URL
			bo.WithAuthority(st, exec)
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
			st, _ := dbScript{buildRow: buildRow("b1", tc.parent), routes: []sqlRoute{{match: "c.repo = ANY($1) AND c.pr_number = $2", row: valuesRow("c1", "b1", 0, "medium", true)}}, chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", Status: "pr_opened", PRNumber: 12, HeadSHA: "old"}, {ID: "c2", BuildID: "b1", Mode: "pr", Status: "failed"}}}.open()
			var observed *executor.StatusWidgetRequest
			eri := &mockEriClient{widgetFn: func(_ context.Context, req executor.StatusWidgetRequest) (string, error) {
				observed = &req
				return "widget", nil
			}}
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`{"state":"open","merged":false,"head":{"sha":"new"}}`))
			}))
			t.Cleanup(api.Close)
			exec := executor.NewExecutor("", eri, nil, "", nil)
			exec.GitHubAPIURL = api.URL
			exec.HTTPClient = api.Client()
			bo := NewBuildObject("", nil).WithAuthority(st, exec)
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

func recoveryFixtureStore(mu *sync.Mutex, parent *string, chunks *[]model.VanadisChunk, failParent bool) *store.Store {
	return newMockStore(mockDB{execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
		return pgconn.NewCommandTag("UPDATE 0"), nil
	}, beginFn: func(context.Context) (pgx.Tx, error) {
		var local []model.VanadisChunk
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
				local = append([]model.VanadisChunk(nil), (*chunks)...)
				return buildRow("b1", state)
			}, queryFn: func(context.Context, string, ...any) (pgx.Rows, error) { return &mockRows{items: local}, nil },
			execFn: func(_ context.Context, q string, args ...any) (pgconn.CommandTag, error) {
				if !locked {
					return pgconn.CommandTag{}, errors.New("write without lock")
				}
				if strings.Contains(q, "UPDATE vanadis_chunks") {
					applyRecoveryFixtureHead(local, args, strings.Contains(q, "SET ci_checks = COALESCE"))
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

func recoveryFixtureChecks(c model.VanadisChunk) []byte {
	if c.HeadSHA == "" && c.MergeSHA == "" {
		return []byte(`{"lint":"passed"}`)
	}
	checks := map[string]string{"_head_sha": c.HeadSHA, "_merge_sha": c.MergeSHA}
	raw, _ := json.Marshal(checks)
	return raw
}

func applyRecoveryFixtureHead(chunks []model.VanadisChunk, args []any, baseline bool) {
	for i := range chunks {
		if chunks[i].ID == args[0] {
			if !baseline {
				chunks[i].Status = "pr_opened"
			}
			chunks[i].HeadSHA, _ = args[1].(string)
		}
	}
}
