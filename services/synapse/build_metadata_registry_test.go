package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5"
)

func metadataFixture(t *testing.T) BuildMetadata {
	t.Helper()
	raw, err := os.ReadFile("testdata/build-metadata-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	m, err := ParseBuildMetadata(raw)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestRecordPRBuildMetadata(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, repo, kind     string
		nilPool, nilReceiver bool
		dbErr                error
		wantErr, want        bool
	}{
		{name: "updated", repo: "owner/repo", want: true},
		{name: "final", repo: "owner/repo", kind: "final", want: true},
		{name: "transport result rejected", repo: "owner/repo", kind: "result", wantErr: true},
		{name: "progress snapshot stored", repo: "owner/repo", kind: "progress", want: true},
		{name: "invalid snapshot rejected", repo: "owner/repo", kind: "unknown", wantErr: true},
		{name: "wrong repo", repo: "owner/other", wantErr: true},
		{name: "nil pool", repo: "owner/repo", nilPool: true},
		{name: "nil receiver", repo: "owner/repo", nilReceiver: true},
		{name: "unchanged", repo: "owner/repo", dbErr: pgx.ErrNoRows},
		{name: "database error", repo: "owner/repo", dbErr: errors.New("offline"), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := metadataFixture(t)
			if tc.kind != "" {
				m.Kind = tc.kind
			}
			if m.Kind == "result" {
				m.Service = "engine"
				m.ImageName = "engine"
				m.State = "success"
				m.SourceAttempt = 1
			}
			r := NewPostgresPRRegistry(&mockSynapsePgxPool{queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return mockSynapsePgxRow{scanFn: func(dest ...any) error {
					if tc.dbErr != nil {
						return tc.dbErr
					}
					return scanValues(dest, true)
				}}
			}})
			if tc.nilPool {
				r.pool = nil
			}
			if tc.nilReceiver {
				r = nil
			}
			got, err := r.RecordPRBuildMetadata(context.Background(), tc.repo, 1, m)
			if got != tc.want || (err != nil) != tc.wantErr {
				t.Fatalf("got=%v err=%v", got, err)
			}
		})
	}
}

func TestLoadPRBuildMetadata(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, raw, repo string
		nilPool         bool
		dbErr           error
		wantErr, want   bool
	}{
		{name: "valid", repo: "owner/repo", want: true},
		{name: "wrong repo", repo: "owner/other", wantErr: true},
		{name: "invalid JSON", repo: "owner/repo", raw: "{}", wantErr: true},
		{name: "nil pool", repo: "owner/repo", nilPool: true},
		{name: "absent", repo: "owner/repo", dbErr: pgx.ErrNoRows},
		{name: "database error", repo: "owner/repo", dbErr: errors.New("offline"), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw, err := os.ReadFile("testdata/build-metadata-v1.json")
			if err != nil {
				t.Fatal(err)
			}
			if tc.raw != "" {
				raw = []byte(tc.raw)
			}
			r := NewPostgresPRRegistry(&mockSynapsePgxPool{queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return mockSynapsePgxRow{scanFn: func(dest ...any) error {
					if tc.dbErr != nil {
						return tc.dbErr
					}
					return scanValues(dest, raw)
				}}
			}})
			if tc.nilPool {
				r.pool = nil
			}
			m, got, err := r.LoadPRBuildMetadata(context.Background(), tc.repo, 1)
			if got != tc.want || (err != nil) != tc.wantErr {
				t.Fatalf("got=%v err=%v", got, err)
			}
			if got && m.RunID != 1 {
				t.Fatal("wrong run identity")
			}
		})
	}
}

func TestLoadPRBuildLifecycle(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, repo string
		pr         int
		nilPool    bool
		dbErr      error
		want       PRBuildLifecycle
		wantErr    bool
	}{
		{"merged with started deploy", "owner/repo", 1, false, nil, PRBuildLifecycle{Status: prMerged, HeadSHA: "head", MergeSHA: "merge", Started: true}, false},
		{"absent PR", "owner/repo", 1, false, pgx.ErrNoRows, PRBuildLifecycle{}, false},
		{"database error", "owner/repo", 1, false, errors.New("offline"), PRBuildLifecycle{}, true},
		{"nil pool", "owner/repo", 1, true, nil, PRBuildLifecycle{}, false},
		{"invalid PR", "owner/repo", 0, false, nil, PRBuildLifecycle{}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := NewPostgresPRRegistry(&mockSynapsePgxPool{queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return mockSynapsePgxRow{scanFn: func(dest ...any) error {
					if tc.dbErr != nil {
						return tc.dbErr
					}
					return scanValues(dest, prMerged, "head", "merge", true)
				}}
			}})
			if tc.nilPool {
				r.pool = nil
			}
			got, err := r.LoadPRBuildLifecycle(context.Background(), tc.repo, tc.pr)
			if got != tc.want || (err != nil) != tc.wantErr {
				t.Fatalf("got=%+v err=%v", got, err)
			}
		})
	}
}

// Every connection owns a temporary registry; no persistent tables are touched.
func TestRecordPRBuildMetadataFencing(t *testing.T) {
	t.Parallel()
	if *registryTestDSN == "" {
		t.Skip("requires disposable PostgreSQL fixture via -registry-test-dsn")
	}
	tests := []struct {
		name                                     string
		runID, number                            int64
		attempt                                  int
		kind, source, execution, workflow, event string
		selected                                 []BuildSelection
		storedKind, status                       string
		want                                     bool
	}{
		{name: "unchanged", runID: 1, number: 2, attempt: 2},
		{name: "stale head", runID: 2, number: 3, attempt: 1, source: "cccccccccccccccccccccccccccccccccccccccc"},
		{name: "older run", runID: 2, number: 1, attempt: 9},
		{name: "older attempt", runID: 1, number: 2, attempt: 1},
		{name: "newer run", runID: 2, number: 3, attempt: 1, want: true},
		{name: "newer attempt", runID: 1, number: 2, attempt: 3, want: true},
		{name: "finalization", runID: 1, number: 2, attempt: 2, kind: "final", want: true},
		{name: "phase regression", runID: 1, number: 2, attempt: 2, storedKind: "final"},
		{name: "different run identity", runID: 9, number: 2, attempt: 2, kind: "final"},
		{name: "different execution", runID: 1, number: 2, attempt: 2, execution: "cccccccccccccccccccccccccccccccccccccccc"},
		{name: "different workflow", runID: 1, number: 2, attempt: 2, workflow: ".github/workflows/other.yml"},
		{name: "different selection", runID: 1, number: 2, attempt: 2, selected: []BuildSelection{{Service: "engine", ImageName: "engine"}}},
		{name: "different run number", runID: 1, number: 1, attempt: 2, kind: "final"},
		{name: "new attempt changed execution", runID: 1, number: 2, attempt: 3, execution: "cccccccccccccccccccccccccccccccccccccccc"},
		{name: "new attempt changed selection", runID: 1, number: 2, attempt: 3, selected: []BuildSelection{{Service: "engine", ImageName: "engine"}}},
		{name: "new attempt changed workflow", runID: 1, number: 2, attempt: 3, workflow: ".github/workflows/other.yml"},
		{name: "different event", runID: 1, number: 2, attempt: 2, event: "pull_request"},
		{name: "closed", runID: 2, number: 3, attempt: 1, status: "closed"},
		{name: "merged source", runID: 2, number: 3, attempt: 1, status: "merged", want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			seed := metadataFixture(t)
			seed.Repository = "example/project"
			seed.RunNumber = 2
			seed.RunAttempt = 2
			seed.Kind = firstNonEmpty(tc.storedKind, "manifest")
			raw, err := json.Marshal(map[string]any{"build_metadata": seed, "unrelated": "preserved"})
			if err != nil {
				t.Fatal(err)
			}
			conn := newRecoveryFixture(t, firstNonEmpty(tc.status, "open"), string(raw))
			if _, err = conn.Exec(ctx, `UPDATE pr_registry SET head_sha=$1,merge_sha=$1`, seed.SourceSHA); err != nil {
				t.Fatal(err)
			}
			candidate := seed
			candidate.RunID = tc.runID
			candidate.RunNumber = tc.number
			candidate.RunAttempt = tc.attempt
			candidate.Kind = firstNonEmpty(tc.kind, "manifest")
			candidate.SourceSHA = firstNonEmpty(tc.source, seed.SourceSHA)
			candidate.ExecutionSHA = firstNonEmpty(tc.execution, seed.ExecutionSHA)
			candidate.WorkflowPath = firstNonEmpty(tc.workflow, seed.WorkflowPath)
			candidate.Event = firstNonEmpty(tc.event, seed.Event)
			if tc.selected != nil {
				candidate.Selected = tc.selected
			}
			got, err := NewPostgresPRRegistry(conn).RecordPRBuildMetadata(ctx, "example/project", 42, candidate)
			if err != nil || got != tc.want {
				t.Fatalf("updated=%v want=%v err=%v", got, tc.want, err)
			}
			var stored []byte
			var unrelated, status string
			if err = conn.QueryRow(ctx, `SELECT metadata->'build_metadata',metadata->>'unrelated',status FROM pr_registry WHERE pr_number=42`).Scan(&stored, &unrelated, &status); err != nil {
				t.Fatal(err)
			}
			var snapshot BuildMetadata
			if err = json.Unmarshal(stored, &snapshot); err != nil {
				t.Fatal(err)
			}
			want := seed
			if tc.want {
				want = candidate
			}
			if !reflect.DeepEqual(snapshot, want) || unrelated != "preserved" || status != firstNonEmpty(tc.status, "open") {
				t.Fatalf("persisted snapshot=%+v want=%+v unrelated=%s status=%s", snapshot, want, unrelated, status)
			}
		})
	}
}
