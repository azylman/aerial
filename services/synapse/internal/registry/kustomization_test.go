package registry

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/services/synapse/internal/model"
)

var registryTestDSN = flag.String("registry-test-dsn", "", "disposable PostgreSQL fixture for registry SQL integration tests")

func TestPostgresPRRegistry_KustomizationUnit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []postgresPRRegistryUnitCase{
		// RecordDeployedJobs
		{name: "RecordDeployedJobs empty repo guard", op: "RecordDeployedJobs", repo: "", prNumber: 42, jobs: []string{"eri"}},
		{name: "RecordDeployedJobs zero pr guard", op: "RecordDeployedJobs", repo: "azylman/aerial", prNumber: 0, jobs: []string{"eri"}},
		{name: "RecordDeployedJobs nil pool guard", op: "RecordDeployedJobs", nilPool: true, repo: "azylman/aerial", prNumber: 42, jobs: []string{"eri"}},
		{name: "RecordDeployedJobs empty jobs guard", op: "RecordDeployedJobs", repo: "azylman/aerial", prNumber: 42, jobs: []string{" "}},
		{name: "RecordDeployedJobs success", op: "RecordDeployedJobs", repo: "azylman/aerial", prNumber: 42, jobs: []string{"eri"}, wantUpdated: true, wantSQLSubstr: "'{deployed_jobs}'"},
		{name: "RecordDeployedJobs already deployed returns false", op: "RecordDeployedJobs", repo: "azylman/aerial", prNumber: 42, jobs: []string{"eri"}, mockSelectErr: pgx.ErrNoRows},
		{name: "RecordDeployedJobs db error", op: "RecordDeployedJobs", repo: "azylman/aerial", prNumber: 42, jobs: []string{"eri"}, mockSelectErr: errors.New("db error"), wantErr: true},

		// RecordKustomizationRequests
		{name: "RecordKustomizationRequests empty repo guard", op: "RecordKustomizationRequests", repo: "", prNumber: 42, kustRequests: map[string]model.KustomizationRequest{"aerial-core": {Namespace: "flux-system", RequestedAt: "t1"}}, mockExecErr: errors.New("unexpected exec")},
		{name: "RecordKustomizationRequests zero pr guard", op: "RecordKustomizationRequests", repo: "azylman/aerial", prNumber: 0, kustRequests: map[string]model.KustomizationRequest{"aerial-core": {Namespace: "flux-system", RequestedAt: "t1"}}, mockExecErr: errors.New("unexpected exec")},
		{name: "RecordKustomizationRequests nil pool guard", op: "RecordKustomizationRequests", nilPool: true, repo: "azylman/aerial", prNumber: 42, kustRequests: map[string]model.KustomizationRequest{"aerial-core": {Namespace: "flux-system", RequestedAt: "t1"}}},
		{name: "RecordKustomizationRequests empty requests guard", op: "RecordKustomizationRequests", repo: "azylman/aerial", prNumber: 42, kustRequests: nil},
		{name: "RecordKustomizationRequests replaces requests and clears deployed", op: "RecordKustomizationRequests", repo: "azylman/aerial", prNumber: 42, kustRequests: map[string]model.KustomizationRequest{"aerial-core": {Namespace: "flux-system", RequestedAt: "t1"}}, wantSQLSubstr: "COALESCE(metadata, '{}'::jsonb) - 'deployed_kustomizations', '{kustomization_requests}', $3::jsonb"},
		{name: "RecordKustomizationRequests exec error", op: "RecordKustomizationRequests", repo: "azylman/aerial", prNumber: 42, kustRequests: map[string]model.KustomizationRequest{"aerial-core": {Namespace: "flux-system", RequestedAt: "t1"}}, mockExecErr: errors.New("exec error"), wantErr: true},

		// MarkKustomizationsDeployed
		{name: "MarkKustomizationsDeployed empty repo guard", op: "MarkKustomizationsDeployed", repo: "", prNumber: 42, jobs: []string{"aerial-core"}, mockSelectErr: errors.New("unexpected query")},
		{name: "MarkKustomizationsDeployed zero pr guard", op: "MarkKustomizationsDeployed", repo: "azylman/aerial", prNumber: 0, jobs: []string{"aerial-core"}, mockSelectErr: errors.New("unexpected query")},
		{name: "MarkKustomizationsDeployed nil pool guard", op: "MarkKustomizationsDeployed", nilPool: true, repo: "azylman/aerial", prNumber: 42, jobs: []string{"aerial-core"}},
		{name: "MarkKustomizationsDeployed empty names guard", op: "MarkKustomizationsDeployed", repo: "azylman/aerial", prNumber: 42, jobs: []string{" "}, mockSelectErr: errors.New("unexpected query")},
		{name: "MarkKustomizationsDeployed returns the scanned transition", op: "MarkKustomizationsDeployed", repo: "azylman/aerial", prNumber: 42, jobs: []string{"marida-core"}, wantUpdated: true},
		{name: "MarkKustomizationsDeployed already deployed returns false", op: "MarkKustomizationsDeployed", repo: "azylman/aerial", prNumber: 42, jobs: []string{"marida-core"}, mockSelectErr: pgx.ErrNoRows},
		{name: "MarkKustomizationsDeployed db error", op: "MarkKustomizationsDeployed", repo: "azylman/aerial", prNumber: 42, jobs: []string{"marida-core"}, mockSelectErr: errors.New("db error"), wantErr: true},

		// HasKustomizationRequests
		{name: "HasKustomizationRequests empty repo guard", op: "HasKustomizationRequests", repo: "", prNumber: 42},
		{name: "HasKustomizationRequests zero pr guard", op: "HasKustomizationRequests", repo: "azylman/aerial", prNumber: 0, mockSelectErr: errors.New("unexpected query")},
		{name: "HasKustomizationRequests nil pool guard", op: "HasKustomizationRequests", nilPool: true, repo: "azylman/aerial", prNumber: 42},
		{name: "HasKustomizationRequests tracked PR", op: "HasKustomizationRequests", repo: "azylman/aerial", prNumber: 42, wantUpdated: true},
		{name: "HasKustomizationRequests unknown PR", op: "HasKustomizationRequests", repo: "azylman/aerial", prNumber: 42, mockSelectErr: pgx.ErrNoRows},
		{name: "HasKustomizationRequests db error", op: "HasKustomizationRequests", repo: "azylman/aerial", prNumber: 42, mockSelectErr: errors.New("db error"), wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var gotSQL, gotExecSQL string
			pool := buildMockPRRegistryPool(tc, &gotSQL, &gotExecSQL)
			reg := NewPostgresPRRegistry(pool)

			executeKustomizationUnitOp(ctx, t, reg, tc)

			if tc.wantSQLSubstr != "" && !strings.Contains(gotSQL, tc.wantSQLSubstr) && !strings.Contains(gotExecSQL, tc.wantSQLSubstr) {
				t.Errorf("gotSQL = %q, gotExecSQL = %q, want substring %q", gotSQL, gotExecSQL, tc.wantSQLSubstr)
			}
		})
	}
}

func executeKustomizationUnitOp(ctx context.Context, t *testing.T, reg *PostgresPRRegistry, tc postgresPRRegistryUnitCase) {
	t.Helper()
	switch tc.op {
	case "RecordDeployedJobs":
		got, err := reg.RecordDeployedJobs(ctx, tc.repo, tc.prNumber, tc.jobs)
		if (err != nil) != tc.wantErr {
			t.Fatalf("RecordDeployedJobs err = %v, wantErr = %v", err, tc.wantErr)
		}
		if got != tc.wantUpdated {
			t.Errorf("updated = %v, want %v", got, tc.wantUpdated)
		}
	case "RecordKustomizationRequests":
		err := reg.RecordKustomizationRequests(ctx, tc.repo, tc.prNumber, tc.kustRequests)
		if (err != nil) != tc.wantErr {
			t.Fatalf("RecordKustomizationRequests err = %v, wantErr = %v", err, tc.wantErr)
		}
	case "MarkKustomizationsDeployed":
		got, err := reg.MarkKustomizationsDeployed(ctx, tc.repo, tc.prNumber, tc.jobs)
		if (err != nil) != tc.wantErr {
			t.Fatalf("MarkKustomizationsDeployed err = %v, wantErr = %v", err, tc.wantErr)
		}
		if got != tc.wantUpdated {
			t.Errorf("transitioned = %v, want %v", got, tc.wantUpdated)
		}
	case "HasKustomizationRequests":
		got, err := reg.HasKustomizationRequests(ctx, tc.repo, tc.prNumber)
		if (err != nil) != tc.wantErr {
			t.Fatalf("HasKustomizationRequests err = %v, wantErr = %v", err, tc.wantErr)
		}
		if got != tc.wantUpdated {
			t.Errorf("tracked = %v, want %v", got, tc.wantUpdated)
		}
	}
}

func TestPostgresPRRegistry_RetryReadinessUnit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		repo      string
		merge     string
		prNumber  int
		nilPool   bool
		respJSON  string
		dbErr     error
		wantErr   bool
		wantCount int
	}{
		{name: "success", repo: "azylman/aerial", merge: "sha123", prNumber: 42, respJSON: `{"aerial-core":{"namespace":"flux-system","requested_at":"t1"}}`, wantCount: 1},
		{name: "not found", repo: "azylman/aerial", merge: "sha123", prNumber: 42, dbErr: pgx.ErrNoRows, wantCount: 0},
		{name: "db error", repo: "azylman/aerial", merge: "sha123", prNumber: 42, dbErr: errors.New("db error"), wantErr: true},
		{name: "invalid json", repo: "azylman/aerial", merge: "sha123", prNumber: 42, respJSON: `invalid json`, wantErr: true},
		{name: "empty repo guard", repo: "", merge: "sha123", prNumber: 42, wantCount: 0},
		{name: "empty merge guard", repo: "azylman/aerial", merge: "", prNumber: 42, wantCount: 0},
		{name: "zero prNumber guard", repo: "azylman/aerial", merge: "sha123", prNumber: 0, wantCount: 0},
		{name: "nil pool guard", repo: "azylman/aerial", merge: "sha123", prNumber: 42, nilPool: true, wantCount: 0},
		{name: "nil receiver guard", repo: "azylman/aerial", merge: "sha123", prNumber: 42, wantCount: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool := newMockRowPool(tc.dbErr, []byte(tc.respJSON))
			reg := NewPostgresPRRegistry(pool)
			if tc.nilPool {
				reg = NewPostgresPRRegistry(nil)
			}
			if tc.name == "nil receiver guard" {
				reg = nil
			}
			got, err := reg.RetryPRReadiness(context.Background(), tc.repo, tc.merge, tc.prNumber)
			if (err != nil) != tc.wantErr {
				t.Fatalf("RetryPRReadiness() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if len(got) != tc.wantCount {
				t.Fatalf("RetryPRReadiness() count = %d, want = %d", len(got), tc.wantCount)
			}
		})
	}
}

func TestPostgresPRRegistry_Recovery(t *testing.T) {
	t.Parallel()
	if *registryTestDSN == "" {
		t.Skip("requires disposable PostgreSQL fixture via -registry-test-dsn")
	}
	tests := []struct {
		name            string
		status          string
		metadata        string
		wantListed      bool
		steps           [][]string
		wantTransitions []bool
		wantStatuses    []string
		wantMarkers     []bool
	}{
		{name: "failed two-target recovery and duplicate success", status: "deploy_failed", metadata: `{"kustomization_requests":{"one":{},"two":{}},"banner":"failed","deploy_failed_published":true}`, wantListed: true, steps: [][]string{{"unknown"}, {"one"}, {"one"}, {"two"}, {"two"}}, wantTransitions: []bool{false, false, false, true, false}, wantStatuses: []string{"deploy_failed", "deploy_failed", "deploy_failed", "deployed", "deployed"}, wantMarkers: []bool{true, true, true, false, false}},
		{name: "deploying completion", status: "deploying", metadata: `{"kustomization_requests":{"one":{},"two":{}}}`, wantListed: true, steps: [][]string{{"one"}, {"two"}}, wantTransitions: []bool{false, true}, wantStatuses: []string{"deploying", "deployed"}, wantMarkers: []bool{false, false}},
		{name: "deployed with deploy_success_pending listed", status: "deployed", metadata: `{"deploy_success_pending":true,"kustomization_requests":{"one":{}}}`, wantListed: true, steps: [][]string{{"one"}}, wantTransitions: []bool{false}, wantStatuses: []string{"deployed"}, wantMarkers: []bool{false}},
		{name: "closed excluded", status: "closed", metadata: `{"kustomization_requests":{"one":{}}}`, steps: [][]string{{"one"}}, wantTransitions: []bool{false}, wantStatuses: []string{"closed"}, wantMarkers: []bool{false}},
		{name: "deployed excluded", status: "deployed", metadata: `{"kustomization_requests":{"one":{}}}`, steps: [][]string{{"one"}}, wantTransitions: []bool{false}, wantStatuses: []string{"deployed"}, wantMarkers: []bool{false}},
		{name: "untracked failure excluded", status: "deploy_failed", metadata: `{}`, steps: [][]string{{"one"}}, wantTransitions: []bool{false}, wantStatuses: []string{"deploy_failed"}, wantMarkers: []bool{false}},
		{name: "empty tracked failure excluded", status: "deploy_failed", metadata: `{"kustomization_requests":{}}`, steps: [][]string{{"one"}}, wantTransitions: []bool{false}, wantStatuses: []string{"deploy_failed"}, wantMarkers: []bool{false}},
		{name: "array tracking excluded", status: "deploy_failed", metadata: `{"kustomization_requests":[]}`, steps: [][]string{{"one"}}, wantTransitions: []bool{false}, wantStatuses: []string{"deploy_failed"}, wantMarkers: []bool{false}},
		{name: "null tracking excluded", status: "deploy_failed", metadata: `{"kustomization_requests":null}`, steps: [][]string{{"one"}}, wantTransitions: []bool{false}, wantStatuses: []string{"deploy_failed"}, wantMarkers: []bool{false}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			conn := newRecoveryFixture(t, tc.status, tc.metadata)
			reg := NewPostgresPRRegistry(conn)
			prs, err := reg.ListDeployingPRs(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if (len(prs) == 1) != tc.wantListed {
				t.Fatalf("discovery = %v, want listed %v", prs, tc.wantListed)
			}
			if tc.wantListed && prs[0].PRNumber != 42 {
				t.Fatalf("discovered PR = %d", prs[0].PRNumber)
			}
			for i, names := range tc.steps {
				got, err := reg.MarkKustomizationsDeployed(ctx, "example/project", 42, names)
				if err != nil {
					t.Fatal(err)
				}
				if got != tc.wantTransitions[i] {
					t.Fatalf("step %d transition = %v", i, got)
				}
				assertRecoveryState(t, conn, tc.wantStatuses[i], tc.wantMarkers[i])
			}
		})
	}
}

func TestPostgresPRRegistry_RetryReadiness(t *testing.T) {
	t.Parallel()
	if *registryTestDSN == "" {
		t.Skip("requires disposable PostgreSQL fixture via -registry-test-dsn")
	}
	tests := []struct {
		name, status, merge string
		pending             bool
		want                int
	}{
		{"deployed retries original gate", "deployed", "abc", false, 1},
		{"failed retries original gate", "deploy_failed", "abc", false, 1},
		{"pending retains partial progress", "deploying", "abc", true, 1},
		{"stale merge rejected", "deployed", "old", true, 0},
		{"closed rejected", "closed", "abc", true, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			conn := newRecoveryFixture(t, tc.status, `{"kustomization_requests":{"config":{"namespace":"flux-system","requested_at":"original"}},"deployed_kustomizations":["config"],"jobs":["image"],"ci_checks":{"test":"passed"}}`)
			if _, err := conn.Exec(ctx, `UPDATE pr_registry SET merge_sha='abc'`); err != nil {
				t.Fatal(err)
			}
			requests, err := NewPostgresPRRegistry(conn).RetryPRReadiness(ctx, "example/project", tc.merge, 42)
			if err != nil || len(requests) != tc.want {
				t.Fatalf("requests %v err %v", requests, err)
			}
			if tc.want > 0 && requests["config"].RequestedAt != "original" {
				t.Fatal("request identity replaced")
			}
			var status string
			var body []byte
			if err := conn.QueryRow(ctx, `SELECT status,metadata FROM pr_registry`).Scan(&status, &body); err != nil {
				t.Fatal(err)
			}
			var metadata map[string]any
			if err := json.Unmarshal(body, &metadata); err != nil {
				t.Fatal(err)
			}
			_, retained := metadata["deployed_kustomizations"]
			if retained != tc.pending || !reflect.DeepEqual(metadata["jobs"], []any{"image"}) {
				t.Fatalf("metadata %s", body)
			}
			if tc.want > 0 && status != "deploying" || tc.want == 0 && status != tc.status {
				t.Fatalf("status %s", status)
			}
		})
	}
}

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
		selected                                 []model.BuildSelection
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
		{name: "different selection", runID: 1, number: 2, attempt: 2, selected: []model.BuildSelection{{Service: "engine", ImageName: "engine"}}},
		{name: "different run number", runID: 1, number: 1, attempt: 2, kind: "final"},
		{name: "new attempt changed execution", runID: 1, number: 2, attempt: 3, execution: "cccccccccccccccccccccccccccccccccccccccc"},
		{name: "new attempt changed selection", runID: 1, number: 2, attempt: 3, selected: []model.BuildSelection{{Service: "engine", ImageName: "engine"}}},
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
			seed.Kind = model.FirstNonEmpty(tc.storedKind, "manifest")
			raw, err := json.Marshal(map[string]any{"build_metadata": seed, "unrelated": "preserved"})
			if err != nil {
				t.Fatal(err)
			}
			conn := newRecoveryFixture(t, model.FirstNonEmpty(tc.status, "open"), string(raw))
			if _, err = conn.Exec(ctx, `UPDATE pr_registry SET head_sha=$1,merge_sha=$1`, seed.SourceSHA); err != nil {
				t.Fatal(err)
			}
			candidate := seed
			candidate.RunID = tc.runID
			candidate.RunNumber = tc.number
			candidate.RunAttempt = tc.attempt
			candidate.Kind = model.FirstNonEmpty(tc.kind, "manifest")
			candidate.SourceSHA = model.FirstNonEmpty(tc.source, seed.SourceSHA)
			candidate.ExecutionSHA = model.FirstNonEmpty(tc.execution, seed.ExecutionSHA)
			candidate.WorkflowPath = model.FirstNonEmpty(tc.workflow, seed.WorkflowPath)
			candidate.Event = model.FirstNonEmpty(tc.event, seed.Event)
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
			var snapshot model.BuildMetadata
			if err = json.Unmarshal(stored, &snapshot); err != nil {
				t.Fatal(err)
			}
			want := seed
			if tc.want {
				want = candidate
			}
			if !reflect.DeepEqual(snapshot, want) || unrelated != "preserved" || status != model.FirstNonEmpty(tc.status, "open") {
				t.Fatalf("persisted snapshot=%+v want=%+v unrelated=%s status=%s", snapshot, want, unrelated, status)
			}
		})
	}
}

func newRecoveryFixture(t *testing.T, status, metadata string) *pgx.Conn {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, *registryTestDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })
	_, err = conn.Exec(ctx, `CREATE TEMP TABLE pr_registry (id bigserial PRIMARY KEY, repo text, pr_number int, branch text NOT NULL DEFAULT '', head_sha text NOT NULL DEFAULT '', merge_sha text, target_id text NOT NULL DEFAULT '', status text, metadata jsonb, updated_at timestamptz)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, `INSERT INTO pr_registry (repo,pr_number,status,metadata) VALUES ('example/project',42,$1,$2::jsonb)`, status, metadata)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

func assertRecoveryState(t *testing.T, conn *pgx.Conn, wantStatus string, wantMarkers bool) {
	t.Helper()
	ctx := context.Background()
	var status string
	var meta map[string]any
	if err := conn.QueryRow(ctx, `SELECT status,metadata FROM pr_registry WHERE pr_number=42`).Scan(&status, &meta); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus {
		t.Fatalf("status = %s, want %s", status, wantStatus)
	}
	_, banner := meta["banner"]
	_, published := meta["deploy_failed_published"]
	if banner != wantMarkers || published != wantMarkers {
		t.Fatalf("failure markers = %v", meta)
	}
	if wantMarkers {
		if meta["banner"] != "failed" || meta["deploy_failed_published"] != true {
			t.Fatalf("failure metadata changed: %v", meta)
		}
		_, updated, err := NewPostgresPRRegistry(conn).AtomicTransitionDeployFailed(ctx, "example/project", 42)
		if err != nil || updated {
			t.Fatalf("duplicate failure updated=%v err=%v", updated, err)
		}
	}
}
