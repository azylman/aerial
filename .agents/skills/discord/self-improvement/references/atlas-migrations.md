# Declarative Database Schema Migrations Runbook (Ariga Atlas)

All database schema modifications across `aerial` and `aerial-sidecars` use declarative Ariga Atlas migrations executed cluster-wide via Nomad batch jobs prior to service rollout.

## 1. Declarative Schema Definition (`schema.sql`)
- Modify the repository's canonical schema file (`services/eri/pkg/db/schema.sql` for `aerial`, `db/schema.sql` for `aerial-sidecars`).
- Never hand-write migration SQL files or embed imperative DDL (`ALTER TABLE`, `DROP TABLE`, `CREATE INDEX`) in Go/Python/TypeScript application code or startup bootstrap routines. Imperative DDL in application code is mechanically blocked by `scripts/verify.sh` (`check_no_imperative_ddl`).

## 2. Generate Versioned Migration Diff (`scripts/atlas-diff.sh`)
- From the scratch workspace root, run:
  ```bash
  ./scripts/atlas-diff.sh <migration_name>
  ```
  (e.g., `./scripts/atlas-diff.sh flatten_messages_schema`).
- The script automatically provisions an ephemeral, airgapped PostgreSQL + `pgvector` container, computes the declarative diff against `migrations/`, writes the new timestamped `.sql` migration file, updates `migrations/atlas.sum`, and deterministically cleans up the container on exit.
- If a migration requires data backfilling prior to a `NOT NULL` or column drop constraint, edit the generated migration `.sql` file and run `atlas migrate hash --dir "file://services/eri/pkg/db/migrations"` to recompute `atlas.sum`.

## 3. Update Hermetic Test Fixtures & Verify Integrity
- Update any corresponding in-memory SQLite / test schema fixtures (e.g., `services/eri/pkg/db/sqlite_test_fixture_test.go`, `services/haro/db_test.go`, or `FakeStore` models) to stay in sync with `schema.sql`.
- Run local pre-flight verification and package tests:
  ```bash
  ./scripts/verify.sh --staged
  go test -race ./services/eri/pkg/db/...
  ```
- `./scripts/verify.sh --staged` automatically validates `check_no_imperative_ddl` and verifies migration directory checksums via `atlas migrate validate`.
- Cluster-wide schema rollout executes automatically via the `db-migrate` Nomad batch job (`nomad/jobs/db-migrate.nomad`) when Hangar reconciles the merged PR.
