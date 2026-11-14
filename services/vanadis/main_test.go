package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

func assertConfigDefaults(t *testing.T, cfg Config) {
	t.Helper()
	if cfg.Port != "4025" {
		t.Fatalf("unexpected port default: %s", cfg.Port)
	}
	if cfg.BrainURL != "http://127.0.0.1:8088" {
		t.Fatalf("unexpected BrainURL default: %s", cfg.BrainURL)
	}
	if cfg.SynapseURL != "http://synapse.aerial" {
		t.Fatalf("unexpected SynapseURL default: %s", cfg.SynapseURL)
	}
}

func assertFileConfig(t *testing.T, cfg Config) {
	t.Helper()
	if cfg.Port != "4099" || cfg.PostgresURL != "postgres://u:p@db:5432/vanadis" {
		t.Fatalf("unexpected yaml port or postgres_url: %+v", cfg)
	}
	if cfg.BrainURL != "http://eri.aerial:8088" || cfg.EriURL != "http://eri.aerial:8088" || cfg.SynapseURL != "http://router.aerial:4020" {
		t.Fatalf("unexpected yaml endpoints: %+v", cfg)
	}
}

func assertEnvConfig(t *testing.T, cfg Config) {
	t.Helper()
	if cfg.Port != "4030" || cfg.PostgresURL != "postgres://env:5432/db" {
		t.Fatalf("unexpected env port or postgres_url: %+v", cfg)
	}
	if cfg.BrainURL != "http://env-eri:8088" || cfg.EriURL != "http://env-eri:8088" || cfg.SynapseURL != "http://env-router:4020" {
		t.Fatalf("unexpected env endpoints: %+v", cfg)
	}
}

func TestLoadConfigAndEnv(t *testing.T) {
	origFallback, origLookup := vanadisFallbackConfigPath, lookupHostFn
	defer func() {
		vanadisFallbackConfigPath, lookupHostFn = origFallback, origLookup
	}()
	for _, k := range []string{
		"PORT", "DATABASE_URL", "POSTGRES_URL", "POSTGRES_HOST", "POSTGRES_PORT",
		"POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB", "ERI_URL", "SYNAPSE_URL", "CONFIG_PATH",
	} {
		t.Setenv(k, "")
	}
	tmpDir := t.TempDir()
	vanadisFallbackConfigPath = filepath.Join(tmpDir, "nonexistent.yaml")
	tests := []struct {
		name, lookupHost, wantHost, password string
	}{
		{"DNSFallback_AerialPostgres", "aerial-postgres", "@aerial-postgres:5432/", ""},
		{"DNSFallback_Postgres", "postgres", "@postgres:5432/", ""},
		{"DNSFallback_Localhost", "other", "@127.0.0.1:5432/", ""},
		{"Decomposed_PostgresPassword", "other", "@postgres:5432/", "custom_vanadis_pass"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.password != "" {
				t.Setenv("POSTGRES_PASSWORD", tc.password)
			}
			lookupHostFn = func(_ context.Context, host string) ([]string, error) {
				if host == tc.lookupHost {
					return []string{"10.0.0.2"}, nil
				}
				return nil, errors.New("not found")
			}
			cfg := LoadConfig("")
			if !strings.Contains(cfg.PostgresURL, tc.wantHost) {
				t.Fatalf("expected postgres URL to contain %s, got %s", tc.wantHost, cfg.PostgresURL)
			}
			if tc.password != "" && !strings.Contains(cfg.PostgresURL, ":"+tc.password+"@") {
				t.Fatalf("expected postgres URL to contain password %s, got %s", tc.password, cfg.PostgresURL)
			}
			if tc.name == "DNSFallback_AerialPostgres" {
				assertConfigDefaults(t, cfg)
			}
		})
	}
	yamlPath := filepath.Join(tmpDir, "vanadis.yaml")
	_ = os.WriteFile(yamlPath, []byte("port: \"4099\"\npostgres_url: \"postgres://u:p@db:5432/vanadis\"\neri_url: \"http://eri.aerial:8088/\"\nsynapse_url: \"http://router.aerial:4020/\"\n"), 0o600)
	vanadisFallbackConfigPath = yamlPath
	assertFileConfig(t, LoadConfig(filepath.Join(tmpDir, "missing.yaml")))

	badPath := filepath.Join(tmpDir, "bad.yaml")
	_ = os.WriteFile(badPath, []byte(":::invalid_yaml"), 0o600)
	t.Setenv("CONFIG_PATH", badPath)
	t.Setenv("PORT", "4030")
	t.Setenv("DATABASE_URL", "postgres://env:5432/db")
	t.Setenv("ERI_URL", "http://env-eri:8088/")
	t.Setenv("SYNAPSE_URL", "http://env-router:4020/")
	assertEnvConfig(t, LoadConfigFromEnv())

	t.Setenv("SYNAPSE_URL", "http://synapse-override:4020/")
	if envCfg := LoadConfigFromEnv(); envCfg.SynapseURL != "http://synapse-override:4020" {
		t.Fatalf("unexpected SYNAPSE_URL override: %+v", envCfg)
	}
}

func TestExecuteChunkArgsAndNormalizeRepo(t *testing.T) {
	t.Parallel()
	if args := (ExecuteChunkArgs{BuildID: "b1", ChunkIndex: 2}); args.Kind() != "vanadis_execute_chunk" || args.InsertOpts().Queue != VanadisQueueName || args.InsertOpts().MaxAttempts != 3 || !args.InsertOpts().UniqueOpts.ByArgs {
		t.Fatalf("unexpected ExecuteChunkArgs metadata: %+v", args)
	}
}

func testPostgresVanadisStoreCreateValidation(ctx context.Context, t *testing.T, store *PostgresVanadisStore, validBuild VanadisBuild) {
	t.Helper()
	cases := []struct {
		name   string
		build  VanadisBuild
		chunks []VanadisChunk
	}{
		{"empty build id", mkBuild("", "1", "", "o", ""), nil},
		{"empty thread", mkBuild("b", "", "", "o", ""), nil},
		{"empty objective", mkBuild("b", "1", "", "", ""), nil},
		{"empty chunk id", validBuild, []VanadisChunk{mkChunk("", "", 0, "t", "i", "", "")}},
		{"mismatched chunk build_id", validBuild, []VanadisChunk{mkChunk("c", "other", 0, "t", "i", "", "")}},
		{"empty chunk title", validBuild, []VanadisChunk{mkChunk("c", "", 0, "", "i", "", "")}},
		{"empty chunk instructions", validBuild, []VanadisChunk{mkChunk("c", "", 0, "t", "", "", "")}},
		{"invalid chunk effort", validBuild, []VanadisChunk{mkChunk("c", "", 0, "t", "i", "ultra", "")}},
		{"invalid chunk mode", validBuild, []VanadisChunk{{ID: "c", ChunkIndex: 0, Mode: "bad-mode", Repo: "aerial", Title: "t", Instructions: "i"}}},
		{"pr mode empty repo", validBuild, []VanadisChunk{{ID: "c", ChunkIndex: 0, Mode: "pr", Repo: "", Title: "t", Instructions: "i"}}},
		{"action mode with repo", validBuild, []VanadisChunk{{ID: "c", ChunkIndex: 0, Mode: "action", Repo: "aerial", Title: "t", Instructions: "i"}}},
		{"action mode with files", validBuild, []VanadisChunk{{ID: "c", ChunkIndex: 0, Mode: "action", Repo: "", Title: "t", Instructions: "i", TargetFiles: []string{"f.go"}}}},
		{"action mode with symbols", validBuild, []VanadisChunk{{ID: "c", ChunkIndex: 0, Mode: "action", Repo: "", Title: "t", Instructions: "i", TargetSymbols: []string{"s"}}}},
	}
	for _, tc := range cases {
		if err := store.CreateBuild(ctx, tc.build, tc.chunks); err == nil {
			t.Fatalf("expected error for case %s", tc.name)
		}
	}
}

func testPostgresVanadisStoreCreateErrors(ctx context.Context, t *testing.T, validBuild VanadisBuild, validChunks []VanadisChunk) {
	t.Helper()
	callCount := 0
	errExecTx := &mockTx{execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
		return pgconn.CommandTag{}, errors.New("err")
	}}
	begins := []func(context.Context) (pgx.Tx, error){
		func(context.Context) (pgx.Tx, error) { return nil, errors.New("err") },
		func(context.Context) (pgx.Tx, error) { return errExecTx, nil },
		func(context.Context) (pgx.Tx, error) {
			return &mockTx{execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
				callCount++
				if callCount == 2 {
					return pgconn.CommandTag{}, errors.New("err")
				}
				return pgconn.NewCommandTag("INSERT 0 1"), nil
			}}, nil
		},
		func(context.Context) (pgx.Tx, error) {
			return &mockTx{commitErr: errors.New("err"), rollbackErr: errors.New("rb")}, nil
		},
	}
	for _, begin := range begins {
		if err := newMockStore(mockDB{beginFn: begin}).CreateBuild(ctx, validBuild, validChunks); err == nil {
			t.Fatal("expected CreateBuild error")
		}
	}
}

func testPostgresVanadisStoreGetBuild(ctx context.Context, t *testing.T, store *PostgresVanadisStore, validChunks []VanadisChunk, explicitStart, explicitComplete time.Time) {
	t.Helper()
	if _, _, err := store.GetBuild(ctx, "   "); err == nil {
		t.Fatal("expected empty build id error")
	}
	if _, _, err := newMockStore(mockDB{queryRowFn: func(context.Context, string, ...any) pgx.Row { return &mockRow{err: pgx.ErrNoRows} }}).GetBuild(ctx, "b-404"); !errors.Is(err, ErrBuildNotFound) {
		t.Fatalf("expected ErrBuildNotFound, got %v", err)
	}
	if _, _, err := newMockStore(mockDB{queryRowFn: func(context.Context, string, ...any) pgx.Row { return &mockRow{err: errors.New("err")} }}).GetBuild(ctx, "b-err"); err == nil {
		t.Fatal("expected queryRow error")
	}
	popBuild := &VanadisBuild{ID: "b-1", ThreadID: "thread-1", Tier: 2, Title: "Build Title", Objective: "Objective", SharedContext: "Shared Context", Status: "running", StatusMessageID: "msg-1", CreatedAt: time.Now(), UpdatedAt: time.Now()}
	for _, r := range []*mockRows{nil, {items: validChunks, scanErr: errors.New("err")}, {items: validChunks, rowsErr: errors.New("err")}} {
		if _, _, err := newMockStore(mockDB{
			queryRowFn: func(context.Context, string, ...any) pgx.Row { return &mockRow{b: popBuild} },
			queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
				if r == nil {
					return nil, errors.New("err")
				}
				return r, nil
			},
		}).GetBuild(ctx, "b-1"); err == nil {
			t.Fatal("expected GetBuild error")
		}
	}
	cGet0 := mkChunk("c-1", "b-1", 0, "T1", "I1", "", "")
	cGet0.StartedAt = explicitStart
	cGet0.CompletedAt = explicitComplete
	okGetStore := newMockStore(mockDB{
		queryRowFn: func(context.Context, string, ...any) pgx.Row { return &mockRow{b: popBuild} },
		queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
			return &mockRows{items: []VanadisChunk{cGet0, mkChunk("c-2", "b-1", 1, "T2", "I2", "low", "")}}, nil
		},
	})
	gotB, gotC, err := okGetStore.GetBuild(ctx, "b-1")
	if err != nil || gotB.ID != "b-1" || gotB.Title != "Build Title" || len(gotC) != 2 || gotC[0].Effort != "medium" || gotC[1].Effort != "low" || !gotC[0].StartedAt.Equal(explicitStart) || !gotC[0].CompletedAt.Equal(explicitComplete) || !gotC[1].StartedAt.IsZero() || !gotC[1].CompletedAt.IsZero() {
		t.Fatalf("unexpected GetBuild result: b=%+v c=%+v err=%v", gotB, gotC, err)
	}
}

func testPostgresVanadisStoreUpdates(ctx context.Context, t *testing.T, store *PostgresVanadisStore) {
	t.Helper()
	for _, err := range []error{store.UpdateChunkStatus(ctx, "", "running"), store.UpdateChunkStatus(ctx, "c-1", ""), store.UpdateBuildStatus(ctx, "", "completed"), store.UpdateBuildStatus(ctx, "b-1", ""), store.UpdateBuildStatusMessageID(ctx, "", "msg-1")} {
		if err == nil {
			t.Fatal("expected validation error")
		}
	}
	for _, err := range []error{store.UpdateChunkStatus(ctx, "c-1", "running"), store.UpdateBuildStatus(ctx, "b-1", "completed"), store.UpdateBuildStatusMessageID(ctx, "b-1", "msg-1")} {
		if err != nil {
			t.Fatal(err)
		}
	}
	zeroStore := newMockStore(mockDB{execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
		return pgconn.NewCommandTag("UPDATE 0"), nil
	}})
	if !errors.Is(zeroStore.UpdateChunkStatus(ctx, "c-404", "running"), ErrChunkNotFound) || !errors.Is(zeroStore.UpdateBuildStatus(ctx, "b-404", "completed"), ErrBuildNotFound) || !errors.Is(zeroStore.UpdateBuildStatusMessageID(ctx, "b-404", "msg-1"), ErrBuildNotFound) {
		t.Fatal("expected not found errors")
	}
	errExecStore := newMockStore(mockDB{execFn: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
		return pgconn.CommandTag{}, errors.New("err")
	}})
	if errExecStore.UpdateChunkStatus(ctx, "c-1", "running") == nil || errExecStore.UpdateBuildStatus(ctx, "b-1", "completed") == nil || errExecStore.UpdateBuildStatusMessageID(ctx, "b-1", "msg-1") == nil {
		t.Fatal("expected exec errors")
	}
}

func testPostgresVanadisStoreListBuilds(ctx context.Context, t *testing.T) {
	t.Helper()
	failQueries := []struct {
		buildRows *mockRows
		buildErr  error
		chunkRows *mockRows
		chunkErr  error
	}{
		{nil, errors.New("err"), nil, nil},
		{&mockRows{builds: []VanadisBuild{mkBuild("b-1", "", "", "", "")}, scanErr: errors.New("err")}, nil, nil, nil},
		{&mockRows{builds: []VanadisBuild{mkBuild("b-1", "", "", "", "")}, rowsErr: errors.New("err")}, nil, nil, nil},
		{&mockRows{builds: []VanadisBuild{mkBuild("b-1", "", "", "", "running")}}, nil, nil, errors.New("err")},
		{&mockRows{builds: []VanadisBuild{mkBuild("b-1", "", "", "", "running")}}, nil, &mockRows{items: []VanadisChunk{mkChunk("c-1", "b-1", 0, "", "", "", "")}, scanErr: errors.New("err")}, nil},
		{&mockRows{builds: []VanadisBuild{mkBuild("b-1", "", "", "", "running")}}, nil, &mockRows{items: []VanadisChunk{mkChunk("c-1", "b-1", 0, "", "", "", "")}, rowsErr: errors.New("err")}, nil},
	}
	for _, tc := range failQueries {
		s := newMockStore(mockDB{queryFn: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
			if strings.Contains(sql, "vanadis_builds") {
				return tc.buildRows, tc.buildErr
			}
			return tc.chunkRows, tc.chunkErr
		}})
		if _, err := s.ListBuilds(ctx, 10); err == nil {
			t.Fatal("expected ListBuilds error")
		}
	}
	entries, err := newMockStore(mockDB{queryFn: func(context.Context, string, ...any) (pgx.Rows, error) { return &mockRows{}, nil }}).ListBuilds(ctx, 0)
	if err != nil || len(entries) != 0 {
		t.Fatalf("expected 0 entries on 0 builds, got %d, err=%v", len(entries), err)
	}
	now := time.Now().UTC()
	b1, b2 := mkBuild("b-1", "100", "", "", "running"), mkBuild("b-2", "101", "", "", "completed")
	b1.CreatedAt, b1.UpdatedAt, b2.CreatedAt, b2.UpdatedAt = now.Add(-time.Hour), now, now.Add(-2*time.Hour), now
	c1, c2, c3 := mkChunk("c-1", "b-1", 0, "chunk 1", "", "", "running"), mkChunk("c-2", "b-1", 1, "chunk 2", "", "", "pending"), mkChunk("c-3", "b-2", 0, "chunk 3", "", "", "merged")
	c1.CreatedAt, c1.UpdatedAt, c2.CreatedAt, c2.UpdatedAt, c3.CreatedAt, c3.UpdatedAt = now, now, now, now, now, now
	var capturedLimit any
	okStore := newMockStore(mockDB{
		queryFn: func(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
			if strings.Contains(sql, "vanadis_builds") {
				if len(args) > 0 {
					capturedLimit = args[0]
				}
				return &mockRows{builds: []VanadisBuild{b1, b2}}, nil
			}
			return &mockRows{items: []VanadisChunk{c1, c2, c3}}, nil
		},
	})
	gotEntries, err := okStore.ListBuilds(ctx, 25)
	if err != nil || capturedLimit != 25 || len(gotEntries) != 2 || gotEntries[0].ID != "b-1" || gotEntries[0].TotalChunks != 2 || gotEntries[0].QueueState != "running" || gotEntries[1].ID != "b-2" || gotEntries[1].QueueState != "completed" {
		t.Fatalf("unexpected ListBuilds result: entries=%+v limit=%v err=%v", gotEntries, capturedLimit, err)
	}
}

func TestPostgresVanadisStore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var nilStore *PostgresVanadisStore
	_, _, getErr := nilStore.GetBuild(ctx, "b1")
	_, listErr := nilStore.ListBuilds(ctx, 10)
	_, emptyDbListErr := (&PostgresVanadisStore{}).ListBuilds(ctx, 10)
	if nilStore.CreateBuild(ctx, VanadisBuild{}, nil) == nil || nilStore.UpdateChunkStatus(ctx, "c1", "running") == nil || nilStore.UpdateBuildStatus(ctx, "b1", "completed") == nil || getErr == nil || listErr == nil || emptyDbListErr == nil {
		t.Fatal("expected error on nil store operations")
	}
	validBuild := mkBuild("b-1", "12345", "", "Ship Vanadis", "")
	validBuild.SharedContext = "Shared ctx test"
	cValid := mkChunk("c-1", "", 0, "T1", "I1", "medium", "")
	cValid.TargetFiles, cValid.TargetSymbols, cValid.AcceptanceCriteria, cValid.PRNumber = []string{"main.go"}, []string{"Foo"}, []string{"Must pass"}, 42
	validChunks := []VanadisChunk{cValid}
	store := newMockStore(mockDB{})

	testPostgresVanadisStoreCreateValidation(ctx, t, store, validBuild)

	explicitStart := time.Date(2026, 10, 16, 18, 0, 0, 0, time.UTC)
	explicitComplete := time.Date(2026, 10, 16, 18, 15, 0, 0, time.UTC)
	cStart := mkChunk("c-2", "", 1, "T2", "I2", "", "")
	cStart.StartedAt = explicitStart
	cStart.CompletedAt = explicitComplete
	createChunks := []VanadisChunk{validChunks[0], cStart, mkChunk("c-3", "", 2, "T3", "I3", "", "running"), mkChunk("c-4", "", 3, "T4", "I4", "", "merged")}
	if err := store.CreateBuild(ctx, validBuild, createChunks); err != nil {
		t.Fatalf("CreateBuild failed: %v", err)
	}

	testPostgresVanadisStoreCreateErrors(ctx, t, validBuild, validChunks)
	testPostgresVanadisStoreGetBuild(ctx, t, store, validChunks, explicitStart, explicitComplete)
	testPostgresVanadisStoreUpdates(ctx, t, store)
	testPostgresVanadisStoreListBuilds(ctx, t)
}

func assertPromptContains(t *testing.T, prompt string, wants []string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt missing expected substring %q:\n%s", want, prompt)
		}
	}
}

func TestFormatChunkPrompt(t *testing.T) {
	t.Parallel()
	build := mkBuild("build-999", "1555000111", "Build Vanadis Queue Execution Pipeline", "Build Vanadis Queue", "")
	build.SharedContext = "Architecture: River queue backed by PostgreSQL."
	c1 := mkChunk("build-999-chunk-1", "", 0, "Task 1: Schema", "Create SQL tables", "", "merged")
	c1.TargetFiles, c1.PRNumber = []string{"brain/schema.sql"}, 101
	c2 := mkChunk("build-999-chunk-2", "", 1, "Task 2: Service", "Create vanadis service", "high", "running")
	c2.TargetFiles, c2.TargetSymbols, c2.AcceptanceCriteria = []string{"vanadis/main.go", "vanadis/main_test.go"}, []string{"VanadisServer", "FormatChunkPrompt"}, []string{"Coverage >= 95.0%", "  ", "Table driven tests pass"}
	c3 := mkChunk("build-999-chunk-3", "", 2, "Task 3: Webhook Advancement", "Advance chunks on merge", "", "pending")
	chunks := []VanadisChunk{c1, c2, c3}
	for _, tc := range []struct {
		b VanadisBuild
		c []VanadisChunk
		i int
	}{
		{build, nil, 0}, {build, chunks, -1}, {build, chunks, 3},
	} {
		if _, err := FormatChunkPrompt(tc.b, tc.c, tc.i); err == nil {
			t.Fatal("expected error")
		}
	}
	p0, err := FormatChunkPrompt(build, chunks, 0)
	if err != nil || !strings.Contains(p0, "(none — this is the first chunk)") || !strings.Contains(p0, "DO NOT implement these remaining chunks in this turn") || !strings.Contains(p0, "=== Shared Build Context ===\nArchitecture: River queue backed by PostgreSQL.") {
		t.Fatalf("unexpected prompt for chunk 0:\n%s (err=%v)", p0, err)
	}
	p1, err := FormatChunkPrompt(build, chunks, 1)
	if err != nil {
		t.Fatalf("unexpected error for chunk 1: %v", err)
	}
	wantsP1 := []string{
		"[VANADIS BUILD CHUNK EXECUTION]", "Build ID: build-999", "Build Title: Build Vanadis Queue Execution Pipeline",
		"=== Build Objective ===\nBuild Vanadis Queue", "=== Shared Build Context ===\nArchitecture: River queue backed by PostgreSQL.",
		"[Chunk 1/3] Task 1: Schema (aerial, PR #101)", "=== Active Chunk (Chunk 2 of 3) ===", "Repository: aerial",
		"Target Files: vanadis/main.go, vanadis/main_test.go", "Target Symbols: VanadisServer, FormatChunkPrompt",
		"- [Chunk 3/3] Task 3: Webhook Advancement (aerial)", "optimizing all design and implementation choices for the `Build Objective`",
		"./scripts/check-coverage.sh --service <svc> --check --gaps",
		"Run after init: `grep -n -E \"VanadisServer|FormatChunkPrompt\" \"$SCRATCH_DIR/vanadis/main.go\" \"$SCRATCH_DIR/vanadis/main_test.go\"`",
		"4. NEVER spawn or invoke subagents (`invoke_subagent`, `define_subagent`): Vanadis build chunks are already isolated leaf tasks—execute all file edits, tests, and verification directly in this turn.",
		"5. File any out-of-scope bugs, tech debt, or follow-up tasks discovered during execution via the docket MCP create tool (referencing the active Build ID and Chunk ID \"build-999-chunk-2\" in the task description) rather than expanding the active PR's scope.",
		"6. Submit via `/share/aerial/scripts/aerial-pr.sh submit \"$SCRATCH_DIR\" \"<commit_msg>\" --vanadis-chunk build-999-chunk-2`",
		"7. The turn may ONLY end after `aerial-pr.sh submit` has succeeded or the vanadis MCP tool fail_chunk has been called with chunk_id \"build-999-chunk-2\". Run long downloads, installs (e.g. Playwright/Chromium), builds, and tests in the foreground with a long timeout—never background them and end the turn \"waiting for X\". If a dependency genuinely cannot be installed, either call fail_chunk with the reason, or skip only that verification step, state it in `PR_DESCRIPTION.md`, and submit.",
		"8. Terminate turn immediately after submission.",
	}
	assertPromptContains(t, p1, wantsP1)
	actionChunk := mkChunk("build-999-chunk-act", "", 1, "Task Action", "Do action task", "", "running")
	actionChunk.Mode = "action"
	actionChunk.Repo = ""
	pAction, err := FormatChunkPrompt(build, []VanadisChunk{c1, actionChunk, c3}, 1)
	if err != nil {
		t.Fatalf("unexpected error for action chunk prompt: %v", err)
	}
	wantsAction := []string{
		"=== Action Chunk Execution Contract ===",
		"5. Ending the turn is treated as successful completion: NEVER end it while work is still pending in the background (downloads, installs, builds, deploys, or waits)—run long operations in the foreground with a long timeout and confirm the outcome before ending the turn.",
		"6. If an unrecoverable blocker prevents completion, call the vanadis MCP tool fail_chunk with chunk_id \"build-999-chunk-act\" and the failure reason, and end the turn.",
		"7. File any out-of-scope bugs, tech debt, or follow-up tasks discovered during execution via the docket MCP create tool (referencing the active Build ID and Chunk ID \"build-999-chunk-act\" in the task description) rather than expanding the active chunk's scope.",
	}
	assertPromptContains(t, pAction, wantsAction)
	if strings.Contains(p1, "review the full `git diff`") || strings.Contains(p1, "=== Acceptance Criteria ===") || strings.Contains(p0, "Run after init:") {
		t.Fatalf("unexpected headers found in prompt output")
	}
	if got := formatSymbolGrepCommand([]string{"  ", "./pkg/foo.go"}, []string{" ", "Bar"}); got != `grep -n -E "Bar" "$SCRATCH_DIR/pkg/foo.go"` {
		t.Fatalf("unexpected formatSymbolGrepCommand output: %s", got)
	}
	if got := formatSymbolGrepCommand([]string{"   "}, []string{"Bar"}); got != "" {
		t.Fatalf("expected empty command for blank files, got %q", got)
	}
	emptySharedBuild := build
	emptySharedBuild.SharedContext = "   "
	pEmpty, err := FormatChunkPrompt(emptySharedBuild, chunks, 1)
	if err != nil || strings.Contains(pEmpty, "=== Shared Build Context ===") {
		t.Fatalf("unexpected Shared Build Context header when empty: err=%v prompt=%s", err, pEmpty)
	}
	p2, err := FormatChunkPrompt(build, chunks, 2)
	if err != nil || !strings.Contains(p2, "Target Files: (none specified)") || !strings.Contains(p2, "(none — this is the final chunk)") {
		t.Fatalf("unexpected prompt for chunk 2:\n%s (err=%v)", p2, err)
	}
}

func assertDispatchedPrompt(t *testing.T, req BrainDispatchRequest, wantSession, wantTarget, wantChunk string) {
	t.Helper()
	if req.Source != "vanadis" || req.SessionID != wantSession || req.TargetID != wantTarget || req.VanadisChunkID != wantChunk || req.Effort != "medium" {
		t.Fatalf("unexpected prompt dispatch metadata: %+v", req)
	}
	if !strings.Contains(req.Prompt, "Chunk ID: "+wantChunk) {
		t.Fatalf("prompt missing chunk id: %s", req.Prompt)
	}
}

func assertBuildAndChunkFailed(t *testing.T, memStore *memoryVanadisStore, buildID string, chunkIdx int) {
	t.Helper()
	bState, cState, _ := memStore.GetBuild(context.Background(), buildID)
	if bState.Status != "failed" || cState[chunkIdx].Status != "failed" {
		t.Fatalf("expected build and chunk marked failed, got build=%s chunk=%s", bState.Status, cState[chunkIdx].Status)
	}
}

func TestExecuteChunkWorkerAndProcessExecuteChunk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	if (&ExecuteChunkWorker{}).Work(ctx, &river.Job[ExecuteChunkArgs]{}) == nil {
		t.Fatal("expected error when worker server is nil")
	}
	if NewVanadisServer(Config{}, nil, nil, nil).ProcessExecuteChunk(ctx, "b1", 0, false) == nil {
		t.Fatal("expected error when store is nil")
	}
	var gotPrompts []BrainDispatchRequest
	brainStatus := http.StatusAccepted
	brainSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/dispatch" || r.Method != http.MethodPost {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var req BrainDispatchRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		gotPrompts = append(gotPrompts, req)
		w.WriteHeader(brainStatus)
		_, _ = w.Write([]byte(`{"status":"queued"}`))
	}))
	defer brainSrv.Close()
	memStore := newMemoryVanadisStore()
	srv := NewVanadisServer(Config{BrainURL: brainSrv.URL, SynapseURL: brainSrv.URL}, memStore, &mockRiverInserter{}, brainSrv.Client())
	if err := srv.ProcessExecuteChunk(ctx, "nonexistent", 0, false); err != nil {
		t.Fatalf("expected nil on missing build, got %v", err)
	}
	memStore.getErr = errors.New("db outage")
	if err := srv.ProcessExecuteChunk(ctx, "b1", 0, false); err == nil {
		t.Fatal("expected error when GetBuild fails")
	}
	memStore.getErr = nil
	_ = memStore.CreateBuild(ctx, mkBuild("b-cancelled", "", "", "", "cancelled"), []VanadisChunk{mkChunk("c0", "", 0, "", "", "", "")})
	if err := srv.ProcessExecuteChunk(ctx, "b-cancelled", 0, false); err != nil {
		t.Fatalf("expected nil on cancelled build, got %v", err)
	}
	c0 := mkChunk("c-0", "b-run", 0, "C0", "I0", "", "merged")
	c0.PRNumber = 10
	cActDone := mkChunk("c-act-done", "b-run", 2, "C2", "I2", "", "completed")
	cActDone.Mode = "action"
	cActDone.Repo = ""
	_ = memStore.CreateBuild(ctx, mkBuild("b-run", "thread-777", "", "", "running"), []VanadisChunk{c0, mkChunk("c-1", "b-run", 1, "C1", "I1", "high", "pending"), cActDone})
	if srv.ProcessExecuteChunk(ctx, "b-run", 99, false) == nil || srv.ProcessExecuteChunk(ctx, "b-run", 0, false) != nil || srv.ProcessExecuteChunk(ctx, "b-run", 2, false) != nil {
		t.Fatal("unexpected chunk status handling")
	}
	memStore.updateChunkErr = errors.New("update chunk failed")
	if err := srv.ProcessExecuteChunk(ctx, "b-run", 1, false); err == nil {
		t.Fatal("expected error when UpdateChunkStatus fails")
	}
	memStore.updateChunkErr = nil
	worker := &ExecuteChunkWorker{server: srv}
	job := &river.Job[ExecuteChunkArgs]{JobRow: &rivertype.JobRow{Attempt: 1, MaxAttempts: 3}, Args: ExecuteChunkArgs{BuildID: "b-run", ChunkIndex: 1}}
	if err := worker.Work(ctx, job); err != nil {
		t.Fatalf("unexpected worker error: %v", err)
	}
	if len(gotPrompts) != 1 {
		t.Fatalf("expected 1 prompt, got %d", len(gotPrompts))
	}
	assertDispatchedPrompt(t, gotPrompts[0], "vanadis-c-1", "thread-777", "c-1")
	brainStatus = http.StatusBadGateway
	if err := srv.ProcessExecuteChunk(ctx, "b-run", 1, false); err == nil {
		t.Fatal("expected error on 502 from brain")
	}
	if bState, _, _ := memStore.GetBuild(ctx, "b-run"); bState.Status != "running" {
		t.Fatalf("expected build to remain running on non-final attempt, got %s", bState.Status)
	}
	finalJob := &river.Job[ExecuteChunkArgs]{JobRow: &rivertype.JobRow{Attempt: 3, MaxAttempts: 3}, Args: ExecuteChunkArgs{BuildID: "b-run", ChunkIndex: 1}}
	if err := worker.Work(ctx, finalJob); err == nil {
		t.Fatal("expected error on final attempt 502 from brain")
	}
	assertBuildAndChunkFailed(t, memStore, "b-run", 1)

	_ = memStore.UpdateBuildStatus(ctx, "b-run", "running")
	_ = memStore.UpdateChunkStatus(ctx, "c-1", "pending")
	noopClient := &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("simulated network error")
	})}
	_ = NewVanadisServer(Config{BrainURL: "http://[::1]:namedport"}, memStore, nil, noopClient).ProcessExecuteChunk(ctx, "b-run", 1, true)
	closedSrv := httptest.NewServer(http.NotFoundHandler())
	closedURL := closedSrv.URL
	closedSrv.Close()
	memStore.updateBuildErr = errors.New("build fail update err")
	_ = NewVanadisServer(Config{BrainURL: closedURL, SynapseURL: closedURL}, memStore, nil, noopClient).ProcessExecuteChunk(ctx, "b-run", 1, true)
	memStore.updateBuildErr = nil
	httpkit.WriteJSON(httptest.NewRecorder(), http.StatusOK, make(chan int))
}

func testHTTPEndpointsBasicAndFlush(t *testing.T, srv *VanadisServer, doReq func(string, string, io.Reader) *httptest.ResponseRecorder) {
	t.Helper()
	checkCode := func(method, path string, body io.Reader, want int) {
		if rec := doReq(method, path, body); rec.Code != want {
			t.Fatalf("%s %s got %d, want %d", method, path, rec.Code, want)
		}
	}
	for _, p := range []string{"/health", "/healthz", "/metrics"} {
		checkCode(http.MethodGet, p, nil, http.StatusOK)
	}
	origMeta, origCounters := httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn
	defer func() { httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn = origMeta, origCounters }()
	httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn = func(string) error { return errors.New("meta err") }, func(string) error { return nil }
	checkCode(http.MethodPost, "/debug/coverage/flush", nil, http.StatusOK)
	httpkit.WriteCountersDirFn = func(string) error { return errors.New("counters err") }
	checkCode(http.MethodGet, "/debug/coverage/flush", nil, http.StatusInternalServerError)
	srv.SetStore(nil)
	checkCode(http.MethodPost, "/builds", strings.NewReader(`{}`), http.StatusInternalServerError)
	checkCode(http.MethodGet, "/builds/b1", nil, http.StatusInternalServerError)
	checkCode(http.MethodGet, "/builds", nil, http.StatusInternalServerError)
}

func testHTTPEndpointsBuildCreation(t *testing.T, srv *VanadisServer, memStore *memoryVanadisStore, inserter *mockRiverInserter, doReq func(string, string, io.Reader) *httptest.ResponseRecorder) string {
	t.Helper()
	checkCode := func(method, path string, body io.Reader, want int) {
		if rec := doReq(method, path, body); rec.Code != want {
			t.Fatalf("%s %s got %d, want %d", method, path, rec.Code, want)
		}
	}
	srv.SetStore(memStore)
	checkCode(http.MethodPost, "/builds", errorReader{}, http.StatusBadRequest)
	invalidPayloads := []string{
		`{invalid`, `{"thread_id":""}`,
		`{"thread_id":"not-numeric","objective":"obj","chunks":[{"repo":"aerial","title":"t","instructions":"i"}]}`,
		`{"thread_id":"123","objective":""}`, `{"thread_id":"123","objective":"obj","chunks":[]}`,
		`{"thread_id":"123","objective":"obj","chunks":[{"repo":"aerial","title":"","instructions":"i"}]}`,
		`{"thread_id":"123","objective":"obj","chunks":[{"repo":"aerial","title":"t","instructions":""}]}`,
		`{"thread_id":"123","objective":"obj","chunks":[{"repo":"aerial","title":"t","instructions":"i","effort":"unsupported"}]}`,
		`{"thread_id":"123","objective":"obj","chunks":[{"repo":"aerial","id":"bad:id","title":"t","instructions":"i"}]}`,
		`{"thread_id":"123","objective":"obj","chunks":[{"mode":"pr","repo":"","title":"t","instructions":"i"}]}`,
		`{"thread_id":"123","objective":"obj","chunks":[{"mode":"action","repo":"aerial","title":"t","instructions":"i"}]}`,
		`{"thread_id":"123","objective":"obj","chunks":[{"mode":"invalid","repo":"aerial","title":"t","instructions":"i"}]}`,
		`{"thread_id":"123","objective":"obj","chunks":[{"mode":"action","repo":"","target_files":["a.go"],"title":"t","instructions":"i"}]}`,
		`{"thread_id":"123","objective":"obj","chunks":[{"mode":"action","repo":"","target_symbols":["A"],"title":"t","instructions":"i"}]}`,
	}
	for _, p := range invalidPayloads {
		checkCode(http.MethodPost, "/builds", strings.NewReader(p), http.StatusBadRequest)
	}
	if err := validateCreateBuildInputs("b1", "123", "obj", []VanadisChunk{{ID: "bad:id", Title: "t", Instructions: "i"}}); err == nil || !strings.Contains(err.Error(), "chunks[0].id must be 1..120 characters") {
		t.Fatalf("expected validateCreateBuildInputs chunk id error, got %v", err)
	}
	validPayload := `{"thread_id":"stateful_medium:discord-1555999","title":"Implement Feature X Build Title","objective":"Implement feature X","shared_context":"Targeting Nomad cluster","chunks":[{"repo":"aerial","title":"Part 1","instructions":"Do part 1","effort":"low"},{"repo":"aerial","id":"custom-chunk-2","title":"Part 2","instructions":"Do part 2","target_files":["a.go"],"target_symbols":["A"],"effort":"high","acceptance_criteria":["All green"]}]}`
	memStore.createErr = errors.New("create failed")
	checkCode(http.MethodPost, "/builds", strings.NewReader(validPayload), http.StatusInternalServerError)
	memStore.createErr = nil
	srv.SetRiverClient(nil)
	checkCode(http.MethodPost, "/builds", strings.NewReader(validPayload), http.StatusInternalServerError)
	inserter.err = errors.New("river insert failed")
	srv.SetRiverClient(inserter)
	checkCode(http.MethodPost, "/builds", strings.NewReader(validPayload), http.StatusInternalServerError)
	inserter.err = nil
	rec := doReq(http.MethodPost, "/builds", strings.NewReader(validPayload))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", rec.Code, rec.Body.String())
	}
	var createResp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &createResp)
	buildID, _ := createResp["build_id"].(string)
	if buildID == "" || createResp["status"] != "accepted" || len(inserter.inserted) != 1 {
		t.Fatalf("unexpected create response: %+v", createResp)
	}
	storedBuild, storedChunks, _ := memStore.GetBuild(context.Background(), buildID)
	if storedBuild.ThreadID != "1555999" || storedBuild.Title != "Implement Feature X Build Title" || storedBuild.SharedContext != "Targeting Nomad cluster" || len(storedChunks) != 2 || storedChunks[1].Effort != "medium" || len(storedChunks[1].TargetSymbols) != 1 || len(storedChunks[1].AcceptanceCriteria) != 1 {
		t.Fatalf("stored build mismatch: build=%+v chunks=%+v", storedBuild, storedChunks)
	}
	return buildID
}

func testHTTPEndpointsBuildCancel(t *testing.T, srv *VanadisServer, memStore *memoryVanadisStore, buildID string, doReq func(string, string, io.Reader) *httptest.ResponseRecorder) {
	t.Helper()
	checkCode := func(method, path string, body io.Reader, want int) {
		if rec := doReq(method, path, body); rec.Code != want {
			t.Fatalf("%s %s got %d, want %d", method, path, rec.Code, want)
		}
	}
	routes := []struct {
		m, p string
		w    int
	}{
		{http.MethodGet, "/builds/", http.StatusBadRequest},
		{http.MethodDelete, "/builds/" + buildID, http.StatusMethodNotAllowed},
		{http.MethodGet, "/builds/unknown-id", http.StatusNotFound},
		{http.MethodGet, "/builds/" + buildID, http.StatusOK},
		{http.MethodGet, "/builds/" + buildID + "/cancel", http.StatusMethodNotAllowed},
		{http.MethodPost, "/builds/%20/cancel", http.StatusBadRequest},
		{http.MethodPost, "/builds/unknown-id/cancel", http.StatusNotFound},
	}
	for _, tc := range routes {
		checkCode(tc.m, tc.p, nil, tc.w)
	}
	memStore.getErr = errors.New("get error")
	checkCode(http.MethodGet, "/builds/"+buildID, nil, http.StatusInternalServerError)
	memStore.getErr = nil
	memStore.updateBuildErr = errors.New("cancel db err")
	checkCode(http.MethodPost, "/builds/"+buildID+"/cancel", nil, http.StatusInternalServerError)
	memStore.updateBuildErr = nil
	var cancelPromptBuildID, cancelWidgetSyncBuildID, cancelWidgetSyncBanner string
	cancelBrainSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/dispatch/cancel":
			var req map[string]string
			_ = json.NewDecoder(r.Body).Decode(&req)
			cancelPromptBuildID = req["build_id"]
			httpkit.WriteJSON(w, http.StatusOK, map[string]any{"status": "ok", "cancelled_count": 1})
			return
		case "/internal/widget/sync":
			var req map[string]string
			_ = json.NewDecoder(r.Body).Decode(&req)
			cancelWidgetSyncBuildID = req["build_id"]
			cancelWidgetSyncBanner = req["banner"]
			httpkit.WriteJSON(w, http.StatusOK, map[string]any{"status": "ok"})
			return
		}
	}))
	defer cancelBrainSrv.Close()
	srv.cfg.BrainURL = cancelBrainSrv.URL
	srv.cfg.SynapseURL = cancelBrainSrv.URL
	srv.httpClient = cancelBrainSrv.Client()
	_ = memStore.UpdateChunkStatus(context.Background(), buildID+"-chunk-1", "merged")
	_ = memStore.UpdateChunkStatus(context.Background(), "custom-chunk-2", "running")
	memStore.updateChunkErr = errors.New("simulated chunk cancel update err")
	checkCode(http.MethodPost, "/builds/"+buildID+"/cancel", bytes.NewReader(nil), http.StatusOK)
	memStore.updateChunkErr = nil
	if cancelPromptBuildID != buildID || cancelWidgetSyncBuildID != buildID || cancelWidgetSyncBanner != "" {
		t.Fatalf("unexpected cancel notifications: promptBuildID=%q widgetSyncBuildID=%q banner=%q", cancelPromptBuildID, cancelWidgetSyncBuildID, cancelWidgetSyncBanner)
	}
	_ = memStore.UpdateBuildStatus(context.Background(), buildID, "completed")
	checkCode(http.MethodPost, "/builds/"+buildID+"/cancel", bytes.NewReader(nil), http.StatusConflict)
}

func testHTTPEndpointsListAndFilter(t *testing.T, srv *VanadisServer, doReq func(string, string, io.Reader) *httptest.ResponseRecorder) {
	t.Helper()
	wPost := httptest.NewRecorder()
	srv.handleListBuilds(wPost, httptest.NewRequest(http.MethodPost, "/builds", nil))
	if wPost.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on handleListBuilds POST, got %d", wPost.Code)
	}
	now := time.Now().UTC()
	bRunning, bCompleted, bQueued := mkBuild("b-run", "111", "", "", "running"), mkBuild("b-comp", "222", "", "", "completed"), mkBuild("b-q", "333", "", "", "running")
	bRunning.CreatedAt, bCompleted.CreatedAt, bQueued.CreatedAt = now.Add(-time.Minute), now.Add(-5*time.Minute), now.Add(-10*time.Minute)
	listStore := newMemoryVanadisStore()
	_ = listStore.CreateBuild(context.Background(), bRunning, []VanadisChunk{mkChunk("c-run-1", "b-run", 0, "C1", "", "", "running")})
	_ = listStore.CreateBuild(context.Background(), bCompleted, []VanadisChunk{mkChunk("c-comp-1", "b-comp", 0, "C2", "", "", "completed")})
	_ = listStore.CreateBuild(context.Background(), bQueued, []VanadisChunk{mkChunk("c-q-1", "b-q", 0, "C3", "", "", "pending")})
	srv.SetStore(listStore)
	listStore.listErr = errors.New("simulated list failure")
	if rec := doReq(http.MethodGet, "/builds", nil); rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on listErr, got %d", rec.Code)
	}
	listStore.listErr = nil
	recList := doReq(http.MethodGet, "/builds", nil)
	var resp struct {
		Total          int                 `json:"total"`
		RunningCount   int                 `json:"running_count"`
		QueuedCount    int                 `json:"queued_count"`
		CompletedCount int                 `json:"completed_count"`
		Builds         []VanadisBuildEntry `json:"builds"`
	}
	if recList.Code != http.StatusOK || json.Unmarshal(recList.Body.Bytes(), &resp) != nil || resp.Total != 3 || resp.RunningCount != 1 || resp.CompletedCount != 1 || resp.QueuedCount != 1 {
		t.Fatalf("unexpected list response: code=%d resp=%+v", recList.Code, resp)
	}
	if rec := doReq(http.MethodGet, "/runs", nil); rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on /runs, got %d", rec.Code)
	}
	fetchList := func(query string) (int, string) {
		rec := doReq(http.MethodGet, query, nil)
		var out struct {
			Total  int                 `json:"total"`
			Builds []VanadisBuildEntry `json:"builds"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
		id := ""
		if len(out.Builds) > 0 {
			id = out.Builds[0].ID
		}
		return out.Total, id
	}
	queryCases := []struct {
		query, wantID string
		wantTot       int
	}{
		{"/builds?queue_state=running", "b-run", 1},
		{"/builds?status=completed", "b-comp", 1},
		{"/builds?limit=1", "", 1},
	}
	for _, tc := range queryCases {
		tot, id := fetchList(tc.query)
		if tot != tc.wantTot || (tc.wantID != "" && id != tc.wantID) {
			t.Fatalf("query %s got total=%d id=%s", tc.query, tot, id)
		}
	}
	recDetail := doReq(http.MethodGet, "/builds/b-run", nil)
	var detailResp struct {
		Entry VanadisBuildEntry `json:"entry"`
	}
	if err := json.Unmarshal(recDetail.Body.Bytes(), &detailResp); err != nil || detailResp.Entry.ID != "b-run" || detailResp.Entry.QueueState != "running" || detailResp.Entry.TotalChunks != 1 {
		t.Fatalf("unexpected Entry: %+v", detailResp.Entry)
	}
}

func TestHTTPEndpoints(t *testing.T) {
	memStore := newMemoryVanadisStore()
	inserter := &mockRiverInserter{}
	srv := NewVanadisServer(Config{Port: "0", BrainURL: "http://127.0.0.1:8088"}, memStore, inserter, noopHTTPClient)
	mux := srv.Routes()
	doReq := func(method, path string, body io.Reader) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(method, path, body))
		return rec
	}

	testHTTPEndpointsBasicAndFlush(t, srv, doReq)
	buildID := testHTTPEndpointsBuildCreation(t, srv, memStore, inserter, doReq)
	testHTTPEndpointsBuildCancel(t, srv, memStore, buildID, doReq)
	testHTTPEndpointsListAndFilter(t, srv, doReq)
}

func TestInitPostgresAndRiverAndRunServer(t *testing.T) {
	origPoolFn, origRiverFn := newPgxPoolFn, newRiverClientFn
	defer func() { newPgxPoolFn, newRiverClientFn = origPoolFn, origRiverFn }()
	ctx := context.Background()
	srv := NewVanadisServer(Config{}, nil, nil, nil)
	if _, err := initPostgresAndRiver(ctx, srv, "postgres://user:pass@invalid%xx/db"); err == nil {
		t.Fatal("expected parse error")
	}
	newPgxPoolFn = func(context.Context, *pgxpool.Config) (*pgxpool.Pool, error) {
		return nil, errors.New("pool creation failed")
	}
	if _, err := initPostgresAndRiver(ctx, srv, "postgres://u:p@127.0.0.1:5432/db"); err == nil {
		t.Fatal("expected pool error")
	}
	cfg, _ := pgxpool.ParseConfig("postgres://u:p@127.0.0.1:5432/db")
	newPgxPoolFn = origPoolFn
	for _, clientErr := range []error{errors.New("client error"), nil} {
		startErr := clientErr
		if startErr == nil {
			startErr = errors.New("start failed")
		}
		newRiverClientFn = func(*pgxpool.Pool, *river.Workers) (riverLifecycleClient, error) {
			if clientErr != nil {
				return nil, clientErr
			}
			return &mockRiverLifecycle{startErr: startErr}, nil
		}
		if _, err := initPostgresAndRiver(ctx, srv, cfg.ConnString()); err == nil {
			t.Fatal("expected error from initPostgresAndRiver")
		}
	}
	newRiverClientFn = func(*pgxpool.Pool, *river.Workers) (riverLifecycleClient, error) {
		return &mockRiverLifecycle{stopErr: errors.New("stop warning")}, nil
	}
	cleanup, err := initPostgresAndRiver(ctx, srv, cfg.ConnString())
	if err != nil || cleanup == nil {
		t.Fatalf("expected success from initPostgresAndRiver, got err=%v", err)
	}
	cleanup(ctx)
	if runServer(ctx, Config{Port: "-1", PostgresURL: cfg.ConnString()}, nil) == nil || runServer(ctx, Config{Port: "0", PostgresURL: "postgres://user:pass@invalid%xx/db"}, nil) == nil {
		t.Fatal("expected error on invalid runServer configs")
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	ready, done := make(chan string, 1), make(chan error, 1)
	go func() {
		done <- runServer(runCtx, Config{Port: "0", PostgresURL: cfg.ConnString()}, func(addr string) { ready <- addr })
	}()
	select {
	case addr := <-ready:
		if addr == "" {
			t.Fatal("expected non-empty listen addr")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for server ready")
	}
	runCancel()
	if err := <-done; err != nil {
		t.Fatalf("unexpected runServer exit error: %v", err)
	}
	appCtx, appCancel := context.WithCancel(context.Background())
	appCancel()
	if err := RunApp(appCtx, Config{Port: "0"}); err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("unexpected RunApp error: %v", err)
	}
}

func TestSyncBuildWidget(t *testing.T) {
	ctx := context.Background()
	var nilSrv *VanadisServer
	nilSrv.syncBuildWidget(ctx, "b1", "")
	store := newMemoryVanadisStore()
	store.builds["b1"] = mkBuild("b1", "1555405874565091380", "", "", "")
	var postedReq map[string]string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/widget/sync" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&postedReq)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "delivered",
		})
	}))
	defer ts.Close()
	srv := &VanadisServer{
		cfg:        Config{SynapseURL: ts.URL},
		httpClient: ts.Client(),
		store:      store,
	}
	srv.syncBuildWidget(ctx, "b1", "test banner")
	if postedReq["build_id"] != "b1" || postedReq["banner"] != "test banner" {
		t.Errorf("Unexpected posted widget payload: %+v", postedReq)
	}

	// Empty build ID early return
	postedReq = nil
	srv.syncBuildWidget(ctx, "   ", "banner")
	if postedReq != nil {
		t.Errorf("Expected nil postedReq for empty buildID, got %+v", postedReq)
	}

	// Nil httpClient early return
	nilClientSrv := &VanadisServer{cfg: Config{SynapseURL: ts.URL}}
	nilClientSrv.syncBuildWidget(ctx, "b1", "")

	// 500 error response status
	tsErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer tsErr.Close()
	srvErr := &VanadisServer{
		cfg:        Config{SynapseURL: tsErr.URL},
		httpClient: tsErr.Client(),
	}
	srvErr.syncBuildWidget(ctx, "b1", "")

	// Bad URL request creation error (e.g. invalid control char in URL or closed server)
	tsClosed := httptest.NewServer(http.NotFoundHandler())
	closedURL := tsClosed.URL
	tsClosed.Close()
	srvClosed := &VanadisServer{
		cfg:        Config{SynapseURL: closedURL},
		httpClient: ts.Client(),
	}
	srvClosed.syncBuildWidget(ctx, "b1", "")
}

func TestClassifyAndSummarizeBuildEntry(t *testing.T) {
	t.Parallel()
	classifyTests := []struct {
		name        string
		buildStatus string
		chunks      []VanadisChunk
		expected    string
	}{
		{"completed status", "completed", nil, "completed"},
		{"merged status", "merged", nil, "completed"},
		{"failed status", "failed", nil, "completed"},
		{"cancelled status", "cancelled", nil, "completed"},
		{"canceled status uppercase", "CANCELED", nil, "completed"},
		{"queued status", "queued", nil, "queued"},
		{"pending status", "pending", nil, "queued"},
		{"running with empty chunks", "running", []VanadisChunk{}, "running"},
		{"running with all pending chunks", "running", []VanadisChunk{mkChunk("c1", "", 0, "", "", "", "pending"), mkChunk("c2", "", 1, "", "", "", "pending")}, "queued"},
		{"running with mixed chunks", "running", []VanadisChunk{mkChunk("c1", "", 0, "", "", "", "pending"), mkChunk("c2", "", 1, "", "", "", "running")}, "running"},
		{"running with completed chunk", "running", []VanadisChunk{mkChunk("c1", "", 0, "", "", "", "completed"), mkChunk("c2", "", 1, "", "", "", "pending")}, "running"},
	}
	for _, tc := range classifyTests {
		t.Run("classify_"+tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyBuildQueueState(tc.buildStatus, tc.chunks); got != tc.expected {
				t.Errorf("ClassifyBuildQueueState(%q, chunks) = %q, want %q", tc.buildStatus, got, tc.expected)
			}
		})
	}
	b := mkBuild("b-sum", "100", "Title", "Obj", "running")
	b.CreatedAt = time.Unix(1760000000, 0)
	c1 := mkChunk("c-sum-1", "b-sum", 0, "Chunk 1", "inst", "low", "running")
	c1.StartedAt = time.Unix(1760000100, 0)
	c2 := mkChunk("c-sum-2", "b-sum", 1, "Chunk 2", "inst", "high", "pending")
	c2.StartedAt = time.Unix(1760000200, 0)
	c2.CompletedAt = time.Unix(1760000300, 0)
	entry := SummarizeBuildEntry(b, []VanadisChunk{c1, c2})
	if entry.ID != "b-sum" || entry.TotalChunks != 2 || entry.QueueState != "running" || len(entry.Chunks) != 2 || !entry.Chunks[0].StartedAt.Equal(c1.StartedAt) || !entry.Chunks[1].CompletedAt.Equal(c2.CompletedAt) || entry.Chunks[0].Effort != "low" || entry.Chunks[1].Effort != "high" {
		t.Fatalf("unexpected SummarizeBuildEntry result: %+v", entry)
	}
}

type mockRiverJobRows struct {
	pgx.Rows

	rows [][]any
	idx  int
}

func (r *mockRiverJobRows) Close()     {}
func (r *mockRiverJobRows) Err() error { return nil }
func (r *mockRiverJobRows) Next() bool {
	if r.idx < len(r.rows) {
		r.idx++
		return true
	}
	return false
}

func (r *mockRiverJobRows) Scan(dest ...any) error {
	row := r.rows[r.idx-1]
	*dest[0].(*string) = row[0].(string)
	*dest[1].(*int64) = row[1].(int64)
	*dest[2].(*float64) = row[2].(float64)
	return nil
}

type vanadisMetricsCase struct {
	name           string
	server         *VanadisServer
	method         string
	url            string
	wantStatus     int
	wantSubstrings []string
}

func makeVanadisMetricsCases(srvWithQueue, srvMem, srvPGErr *VanadisServer) []vanadisMetricsCase {
	var nilServer *VanadisServer
	nilStoreServer := &VanadisServer{}

	return []vanadisMetricsCase{
		{
			name:       "valid postgres server with active builds and river queue metrics",
			server:     srvWithQueue,
			method:     http.MethodGet,
			url:        "/metrics",
			wantStatus: http.StatusOK,
			wantSubstrings: []string{
				"vanadis_builds_active",
				`vanadis_builds_active{queue_state="running"} 1`,
				`vanadis_builds_active{queue_state="queued"} 1`,
				"vanadis_chunks_active",
				`vanadis_chunks_active{status="pending"} 3`,
				`vanadis_chunks_active{status="running"} 1`,
				`vanadis_chunks_active{status="pr_opened"} 1`,
				"vanadis_queue_depth",
				`vanadis_queue_depth{state="available"} 3`,
				`vanadis_queue_depth{state="running"} 1`,
				`vanadis_queue_depth{state="retryable"} 0`,
				`vanadis_queue_depth{state="scheduled"} 0`,
				"vanadis_queue_oldest_age_seconds 15.5",
				"vanadis_builds_total",
				`vanadis_builds_total{status="created"}`,
				`vanadis_builds_total{status="completed"}`,
				`vanadis_builds_total{status="failed"}`,
				`vanadis_builds_total{status="cancelled"}`,
				"vanadis_chunks_total",
				`vanadis_chunks_total{status="dispatched"}`,
				`vanadis_chunks_total{status="merged"}`,
				`vanadis_chunks_total{status="completed"}`,
				`vanadis_chunks_total{status="failed"}`,
				`vanadis_chunks_total{status="cancelled"}`,
			},
		},
		{
			name:       "memory store server populates build metrics",
			server:     srvMem,
			method:     http.MethodGet,
			url:        "/metrics",
			wantStatus: http.StatusOK,
			wantSubstrings: []string{
				"vanadis_builds_active",
				"vanadis_chunks_active",
				"vanadis_queue_depth",
			},
		},
		{
			name:       "nil server serves metrics ok",
			server:     nilServer,
			method:     http.MethodGet,
			url:        "/metrics",
			wantStatus: http.StatusOK,
			wantSubstrings: []string{
				"vanadis_builds_total",
				"vanadis_chunks_total",
			},
		},
		{
			name:       "server with nil store serves metrics ok",
			server:     nilStoreServer,
			method:     http.MethodGet,
			url:        "/metrics",
			wantStatus: http.StatusOK,
			wantSubstrings: []string{
				"vanadis_builds_total",
				"vanadis_chunks_total",
			},
		},
		{
			name:       "server with db error in queries still serves metrics ok",
			server:     srvPGErr,
			method:     http.MethodGet,
			url:        "/metrics",
			wantStatus: http.StatusOK,
			wantSubstrings: []string{
				"vanadis_builds_total",
				"vanadis_chunks_total",
			},
		},
	}
}

func runVanadisMetricsCase(t *testing.T, tc vanadisMetricsCase) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(tc.method, tc.url, nil)
	tc.server.handleMetrics(rec, req)
	if rec.Code != tc.wantStatus {
		t.Fatalf("handleMetrics() status = %d, want %d", rec.Code, tc.wantStatus)
	}
	body := rec.Body.String()
	for _, sub := range tc.wantSubstrings {
		if !strings.Contains(body, sub) {
			t.Errorf("expected body to contain %q, body:\n%s", sub, body)
		}
	}
}

func TestVanadisMetrics(t *testing.T) {
	RecordBuildTotal("created")
	RecordBuildTotal("completed")
	RecordBuildTotal("failed")
	RecordBuildTotal("cancelled")

	RecordChunkTotal("dispatched")
	RecordChunkTotal("merged")
	RecordChunkTotal("completed")
	RecordChunkTotal("failed")
	RecordChunkTotal("cancelled")

	registerVanadisCollector(BuildsActive)

	b1 := mkBuild("b-run-1", "101", "Running Build", "obj", "running")
	b2 := mkBuild("b-queue-1", "102", "Queued Build", "obj", "running")
	b3 := mkBuild("b-comp-1", "103", "Completed Build", "obj", "completed")

	c1 := mkChunk("c-1", "b-run-1", 0, "Chunk 1", "inst", "medium", "running")
	c2 := mkChunk("c-2", "b-run-1", 1, "Chunk 2", "inst", "medium", "pending")
	c3 := mkChunk("c-3", "b-run-1", 2, "Chunk 3", "inst", "medium", "pr_opened")
	c4 := mkChunk("c-4", "b-queue-1", 0, "Chunk 4", "inst", "medium", "pending")
	c5 := mkChunk("c-5", "b-queue-1", 1, "Chunk 5", "inst", "medium", "pending")
	c6 := mkChunk("c-6", "b-comp-1", 0, "Chunk 6", "inst", "medium", "merged")

	pgStoreWithQueue := newMockStore(mockDB{
		queryFn: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
			if strings.Contains(sql, "FROM river_job") {
				return &mockRiverJobRows{
					rows: [][]any{
						{"available", int64(3), float64(15.5)},
						{"running", int64(1), float64(2.0)},
					},
				}, nil
			}
			if strings.Contains(sql, "vanadis_builds") {
				return &mockRows{builds: []VanadisBuild{b1, b2, b3}}, nil
			}
			return &mockRows{items: []VanadisChunk{c1, c2, c3, c4, c5, c6}}, nil
		},
	})
	srvWithQueue := NewVanadisServer(Config{}, pgStoreWithQueue, nil, nil)

	memStore := newMemoryVanadisStore()
	_ = memStore.CreateBuild(context.Background(), b1, []VanadisChunk{c1, c2, c3})
	srvMem := NewVanadisServer(Config{}, memStore, nil, nil)

	pgStoreErr := newMockStore(mockDB{
		queryFn: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
			return nil, errors.New("db outage")
		},
	})
	srvPGErr := NewVanadisServer(Config{}, pgStoreErr, nil, nil)

	tests := makeVanadisMetricsCases(srvWithQueue, srvMem, srvPGErr)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runVanadisMetricsCase(t, tc)
		})
	}
}

func TestVanadisEndpointCounterIncrements(t *testing.T) {
	ctx := context.Background()
	memStore := newMemoryVanadisStore()
	inserter := &mockRiverInserter{}
	noopClient := noopHTTPClient
	srv := NewVanadisServer(Config{}, memStore, inserter, noopClient)

	// 1. handleCreateBuild success -> created
	reqBody := `{"thread_id":"123456789012345678","tier":2,"objective":"Metric test build","chunks":[{"repo":"aerial","title":"C1","instructions":"I1"}]}`
	recCreate := httptest.NewRecorder()
	srv.handleCreateBuild(recCreate, httptest.NewRequest(http.MethodPost, "/builds", strings.NewReader(reqBody)))
	if recCreate.Code != http.StatusAccepted {
		t.Fatalf("handleCreateBuild status = %d, want %d", recCreate.Code, http.StatusAccepted)
	}

	// 2. handleCreateBuild enqueue failure -> failed
	failInserter := &mockRiverInserter{err: errors.New("enqueue fail")}
	srvFailInsert := NewVanadisServer(Config{}, memStore, failInserter, noopClient)
	recCreateFail := httptest.NewRecorder()
	srvFailInsert.handleCreateBuild(recCreateFail, httptest.NewRequest(http.MethodPost, "/builds", strings.NewReader(reqBody)))
	if recCreateFail.Code != http.StatusInternalServerError {
		t.Fatalf("handleCreateBuild failure status = %d, want %d", recCreateFail.Code, http.StatusInternalServerError)
	}

	// 3. handleBuildCancel -> cancelled
	bCancel := mkBuild("b-to-cancel", "123456789012345678", "Cancel Test", "obj", "running")
	cCancel := mkChunk("c-to-cancel", "b-to-cancel", 0, "Cancel Chunk", "inst", "medium", "pending")
	_ = memStore.CreateBuild(ctx, bCancel, []VanadisChunk{cCancel})
	recCancel := httptest.NewRecorder()
	srv.handleBuildCancel(recCancel, httptest.NewRequest(http.MethodPost, "/builds/b-to-cancel/cancel", nil), "b-to-cancel")
	if recCancel.Code != http.StatusOK {
		t.Fatalf("handleBuildCancel status = %d, want %d", recCancel.Code, http.StatusOK)
	}

	// 4. handleAdvanceChunkPR -> merged and completed
	advStore := newMockStore(mockDB{
		beginFn: func(ctx context.Context) (pgx.Tx, error) {
			return &mockTx{
				queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
					return &genericMockRow{scanFn: func(dest ...any) error {
						*dest[0].(*string) = "c1"
						*dest[1].(*string) = "b1"
						*dest[2].(*int) = 0
						*dest[3].(*string) = "pr"
						*dest[4].(*string) = "Chunk 1"
						*dest[5].(*string) = "pr_opened"
						*dest[6].(*string) = "123456789012345678"
						*dest[7].(*int) = 2
						*dest[8].(*string) = "Build 1"
						*dest[9].(*string) = "Objective 1"
						*dest[10].(*string) = "running"
						*dest[11].(*string) = "msg-1"
						return nil
					}}
				},
				queryFn: func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
					c := mkChunk("c1", "b1", 0, "Chunk 1", "inst", "medium", "pr_opened")
					return &mockRows{items: []VanadisChunk{c}}, nil
				},
			}, nil
		},
	})
	srvAdv := NewVanadisServer(Config{}, advStore, inserter, noopClient)
	recAdv := httptest.NewRecorder()
	srvAdv.handleAdvanceChunkPR(recAdv, httptest.NewRequest(http.MethodPost, "/chunks/advance-pr", strings.NewReader(`{"repo":"aerial","pr_number":42}`)))
	if recAdv.Code != http.StatusOK {
		t.Fatalf("handleAdvanceChunkPR status = %d, want %d", recAdv.Code, http.StatusOK)
	}

	// 5. handleFailChunkPR -> failed
	failStore := newMockStore(mockDB{
		queryRowFn: func(ctx context.Context, sql string, args ...any) pgx.Row {
			return &genericMockRow{scanFn: func(dest ...any) error {
				*dest[0].(*string) = "b-fail"
				*dest[1].(*string) = "c-fail"
				return nil
			}}
		},
	})
	srvFailPR := NewVanadisServer(Config{}, failStore, inserter, noopClient)
	recFailPR := httptest.NewRecorder()
	srvFailPR.handleFailChunkPR(recFailPR, httptest.NewRequest(http.MethodPost, "/chunks/fail-pr", strings.NewReader(`{"repo":"aerial","pr_number":43}`)))
	if recFailPR.Code != http.StatusOK {
		t.Fatalf("handleFailChunkPR status = %d, want %d", recFailPR.Code, http.StatusOK)
	}
}
