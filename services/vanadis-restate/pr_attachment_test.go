package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/pkg/pubsub"
)

func TestFirstAttachmentRecovery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, parent, sibling string
		wantConflict          bool
	}{
		{"failed parent", "failed", "completed", false},
		{"running parent", "running", "pending", false},
		{"failed sibling", "failed", "failed", true},
		{"cancelled sibling", "failed", "cancelled", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := VanadisBuild{ID: "b1", Status: tc.parent}
			store, _ := dbScript{buildRow: &mockRow{b: &b}, chunks: []VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", Repo: "aerial", Status: "failed"}, {ID: "other", Status: tc.sibling}}, routes: []sqlRoute{{match: "UPDATE vanadis_chunks", row: valuesRow("b1", "c1", "")}, {match: "SELECT mode", row: valuesRow("pr")}, {match: "SELECT build_id", row: valuesRow("b1")}}, row: valuesRow("b1", "c1", "")}.open()
			_, _, _, err := store.AttachChunkPR(context.Background(), attachRequest("c1", "aerial"))
			if tc.wantConflict {
				assertStoreErr(t, err, ErrBuildConflict, false)
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func attachmentScript(chunk, build, thread, status, repo string) dbScript {
	return dbScript{routes: []sqlRoute{{match: "SELECT mode", row: valuesRow("pr")}, {match: "SELECT build_id", row: valuesRow(build)}}, buildRow: &mockRow{b: &VanadisBuild{ID: build, ThreadID: thread, Status: status}}, chunks: []VanadisChunk{{ID: chunk, BuildID: build, Mode: "pr", Repo: repo, Status: "running"}}}
}

// attachmentMemory models the database boundary with transactional snapshots.
type attachmentMemory struct {
	mu      sync.Mutex
	b       VanadisBuild
	chunks  []VanadisChunk
	failure string
}
type attachmentTx struct {
	*mockTx

	owner  *attachmentMemory
	b      VanadisBuild
	chunks []VanadisChunk
}

func (m *attachmentMemory) store() *PostgresVanadisStore {
	return newMockStore(mockDB{
		queryRowFn: func(_ context.Context, q string, _ ...any) pgx.Row {
			if strings.Contains(q, "SELECT mode") {
				return valuesRow("pr")
			}
			if strings.Contains(q, "FROM vanadis_builds") {
				return &mockRow{b: &m.b}
			}
			return valuesRow(m.b.ID)
		},
		queryFn: func(context.Context, string, ...any) (pgx.Rows, error) { return &mockRows{items: m.chunks}, nil },
		execFn: func(_ context.Context, q string, args ...any) (pgconn.CommandTag, error) {
			c := &m.chunks[0]
			if c.Status == "failed" || c.Status == "cancelled" || c.Status == "completed" {
				return pgconn.NewCommandTag("UPDATE 0"), nil
			}
			c.Status, _ = args[0].(string)
			return pgconn.NewCommandTag("UPDATE 1"), nil
		},
		beginFn: func(context.Context) (pgx.Tx, error) {
			m.mu.Lock()
			tx := &attachmentTx{owner: m, b: m.b, chunks: append([]VanadisChunk(nil), m.chunks...)}
			tx.mockTx = &mockTx{
				queryRowFn: func(_ context.Context, q string, _ ...any) pgx.Row {
					if strings.Contains(q, "FROM vanadis_builds") {
						return &mockRow{b: &tx.b}
					}
					return valuesRow(tx.b.ID)
				},
				queryFn: func(context.Context, string, ...any) (pgx.Rows, error) { return &mockRows{items: tx.chunks}, nil },
				execFn: func(_ context.Context, q string, args ...any) (pgconn.CommandTag, error) {
					if m.failure != "" && strings.Contains(q, m.failure) {
						return pgconn.CommandTag{}, errors.New("write failure")
					}
					if strings.Contains(q, "UPDATE vanadis_chunks") {
						c := &tx.chunks[0]
						c.PRNumber, _ = args[0].(int)
						recoverWorker, _ := args[2].(bool)
						if recoverWorker {
							c.Status = "pr_opened"
							c.Banner = ""
							c.CompletedAt = time.Time{}
						} else if c.Status != "failed" && c.Status != "completed" && c.Status != "merged" && c.Status != "deploying" {
							c.Status = "pr_opened"
						}
					} else {
						tx.b.Status = "running"
					}
					return pgconn.NewCommandTag("UPDATE 1"), nil
				},
			}
			return tx, nil
		},
	})
}
func (tx *attachmentTx) Rollback(context.Context) error { tx.owner.mu.Unlock(); return nil }

func (tx *attachmentTx) Commit(context.Context) error {
	if tx.owner.failure == "commit" {
		return errors.New("commit failure")
	}
	tx.owner.b = tx.b
	tx.owner.chunks = tx.chunks
	return nil
}

func TestAttachmentAtomicState(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, parent, status, sibling, failure string
		previous, cap, index                   int
		wantStatus, wantParent                 string
		wantErr                                bool
	}{
		{name: "first", parent: "failed", status: "failed", wantStatus: "pr_opened", wantParent: "running"},
		{name: "middle", parent: "failed", status: "failed", index: 1, wantStatus: "pr_opened", wantParent: "running"},
		{name: "final", parent: "failed", status: "failed", index: 2, wantStatus: "pr_opened", wantParent: "running"},
		{name: "duplicate after failure", parent: "failed", status: "failed", previous: 101, wantStatus: "failed", wantParent: "failed"},
		{name: "cancelled chunk", parent: "running", status: "cancelled", wantStatus: "cancelled", wantParent: "running", wantErr: true},
		{name: "completed chunk different PR", parent: "running", status: "completed", previous: 100, wantStatus: "completed", wantParent: "running", wantErr: true},
		{name: "duplicate at cap", parent: "failed", status: "failed", previous: 101, cap: 3, wantStatus: "failed", wantParent: "failed"},

		{name: "cap preserved", parent: "failed", status: "failed", cap: 3, wantStatus: "failed", wantParent: "failed"},
		{name: "cancelled", parent: "cancelled", status: "failed", wantStatus: "failed", wantParent: "cancelled", wantErr: true},
		{name: "completed", parent: "completed", status: "failed", wantStatus: "failed", wantParent: "completed", wantErr: true},
		{name: "failed sibling", parent: "failed", status: "failed", sibling: "failed", wantStatus: "failed", wantParent: "failed", wantErr: true},
		{name: "cancelled sibling", parent: "failed", status: "failed", sibling: "cancelled", wantStatus: "failed", wantParent: "failed", wantErr: true},
		{name: "chunk rollback", parent: "failed", status: "failed", failure: "UPDATE vanadis_chunks", wantStatus: "failed", wantParent: "failed", wantErr: true},
		{name: "parent rollback", parent: "failed", status: "failed", failure: "UPDATE vanadis_builds", wantStatus: "failed", wantParent: "failed", wantErr: true},
		{name: "commit rollback", parent: "failed", status: "failed", failure: "commit", wantStatus: "failed", wantParent: "failed", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := &attachmentMemory{b: VanadisBuild{ID: "b1", Status: tc.parent}, chunks: []VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", Repo: "aerial", Status: tc.status, ChunkIndex: tc.index, PRNumber: tc.previous, CIFailCount: tc.cap, StallNudges: 4, CIChecks: map[string]string{"unit": "pending"}, DependsOn: []string{"done"}, Banner: "worker failed", CompletedAt: time.Unix(1, 0)}, {ID: "sibling", Status: tc.sibling}}, failure: tc.failure}
			before := m.chunks[0]
			_, _, _, err := m.store().AttachChunkPR(context.Background(), attachRequest("c1", "aerial"))
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v", err)
			}
			c := m.chunks[0]
			if c.Status != tc.wantStatus || m.b.Status != tc.wantParent || c.CIFailCount != tc.cap || c.StallNudges != 4 {
				t.Fatalf("state = %+v / %+v", m.b, c)
			}
			preserved := c
			preserved.PRNumber, preserved.Status, preserved.Banner, preserved.CompletedAt = before.PRNumber, before.Status, before.Banner, before.CompletedAt
			assert.Equal(t, before, preserved, "attachment changed preserved evidence")
			if tc.wantErr && !reflect.DeepEqual(c, before) {
				t.Fatal("rejected attachment mutated chunk")
			}
			if tc.wantStatus == "pr_opened" {
				if c.Banner != "" || !c.CompletedAt.IsZero() {
					t.Fatal("failure evidence retained")
				}
				if err := m.store().UpdatePRLifecycleStatus(context.Background(), "c1", "ci_failed"); err != nil {
					t.Fatal(err)
				}
				if m.chunks[0].Status != "ci_failed" {
					t.Fatal("CI failure was not recorded")
				}
			}
		})
	}
}

func TestRecoveredAttachmentRemediationWake(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		openedBefore bool
	}{{"opened event before attachment", true}, {"opened event after attachment", false}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := &attachmentMemory{b: VanadisBuild{ID: "b1", ThreadID: "thread-1", Status: "failed"}, chunks: []VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", Repo: "aerial", Status: "failed"}}}
			store := m.store()
			payloads := make(chan map[string]string, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var payload map[string]string
				if r.URL.Path != "/prompt" || r.Method != http.MethodPost {
					t.Errorf("request = %s %s", r.Method, r.URL.Path)
				}
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Error(err)
				}
				payloads <- payload
				w.Write([]byte(`{}`))
			}))
			defer server.Close()
			bo := NewBuildObject("", nil).WithAuthority(store, NewExecutor(t.TempDir(), NewHTTPEriClient(server.URL, server.Client()), nil, "", nil))
			ctx := makeObjectMockContext(t, "b1")
			_, err := restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
				return recoveredAttachmentEvents(rc, bo, store, tc.openedBefore)
			})
			if err != nil {
				t.Fatal(err)
			}
			if m.chunks[0].Status != "ci_failed" || m.b.Status != "running" {
				t.Fatalf("state = %+v / %+v", m.b, m.chunks)
			}
			select {
			case payload := <-payloads:
				if payload["channel_id"] != "thread-1" || !strings.Contains(payload["prompt"], "unit test failed") || !strings.Contains(payload["prompt"], "101") {
					t.Fatalf("payload = %+v", payload)
				}
			default:
				t.Fatal("no remediation wake")
			}
		})
	}
}

func TestConcurrentFirstAttachment(t *testing.T) {
	t.Parallel()
	m := &attachmentMemory{b: VanadisBuild{ID: "b1", Status: "failed"}, chunks: []VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", Repo: "aerial", Status: "failed"}}}
	store := m.store()
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Go(func() {
			_, _, _, err := store.AttachChunkPR(context.Background(), attachRequest("c1", "aerial"))
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if m.b.Status != "running" || m.chunks[0].Status != "pr_opened" || m.chunks[0].PRNumber != 101 {
		t.Fatalf("state = %+v / %+v", m.b, m.chunks)
	}
}

func recoveredAttachmentEvents(rc restate.RunContext, bo *BuildObject, store *PostgresVanadisStore, openedBefore bool) (bool, error) {
	if openedBefore {
		if err := bo.handlePREventType(rc, "b1", "c1", "opened", pubsub.PRLifecycleEvent{}); err != nil {
			return false, err
		}
	}
	if _, _, _, err := store.AttachChunkPR(rc, attachRequest("c1", "aerial")); err != nil {
		return false, err
	}
	if !openedBefore {
		if err := bo.handlePREventType(rc, "b1", "c1", "opened", pubsub.PRLifecycleEvent{}); err != nil {
			return false, err
		}
	}
	return true, bo.handlePREventType(rc, "b1", "c1", "ci_failed", pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, CheckName: "unit", Details: "unit test failed"})
}
