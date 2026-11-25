# Declarative Database Schema Migrations Runbook (Ariga Atlas)

All database schema modifications across `aerial` and `aerial-sidecars` use declarative Ariga Atlas migrations executed cluster-wide via a standalone Kubernetes `db-migrate` batch `Job` triggered automatically when migration images update.

## 1. Declarative Schema Definition (`schema.sql`)
- Modify the repository's canonical schema file (`services/eri/pkg/db/schema.sql` for `aerial`, `db/schema.sql` for `aerial-sidecars`).
- Never hand-write migration SQL files or embed imperative DDL (`ALTER TABLE`, `DROP TABLE`, `CREATE INDEX`) in Go/Python/TypeScript application code or startup bootstrap routines. Imperative DDL in application code is mechanically blocked by `checkNoImperativeDDL` via `bazel run //scripts/code-health` (and `scripts/verify.sh`).

## 2. Generate Versioned Migration Diff (`scripts/atlas-diff.sh`)
- From the scratch workspace root, run:
  ```bash
  ./scripts/atlas-diff.sh <migration_name>
  ```
  (e.g., `./scripts/atlas-diff.sh flatten_messages_schema`).
- The script automatically provisions an ephemeral, airgapped local throwaway PostgreSQL 19 cluster in `/tmp/atlas.XXXXXX` via `initdb`/`pg_ctl`, computes the declarative diff against `migrations/`, writes the new timestamped `.sql` migration file, updates `migrations/atlas.sum`, and deterministically cleans up the directory on exit.
- If a migration requires data backfilling prior to a `NOT NULL` or column drop constraint, edit the generated migration `.sql` file and run `atlas migrate hash --dir "file://services/eri/pkg/db/migrations"` to recompute `atlas.sum`.

## 3. Update Hermetic Test Fixtures & Verify Integrity
- DB tests load the real `schema.sql` through `pkg/pgtest` (one throwaway Postgres per test binary), so no test schema copy exists to keep in sync; do not add SQLite or in-memory fake schemas.
- Run local pre-flight verification and package tests:
  ```bash
  ./scripts/verify.sh --staged
  go test -race ./services/eri/pkg/db/...
  ```
- `./scripts/verify.sh --staged` automatically validates `checkNoImperativeDDL` (run through `scripts/code-health`) and verifies migration directory checksums via `atlas migrate validate`.
- Cluster-wide schema rollout executes automatically via the standalone `db-migrate` Kubernetes `Job` (`k8s/core-services.yaml`) whenever `TAG_AERIAL_MIGRATOR` or `TAG_AERIAL_SIDECAR_MIGRATOR` updates.
