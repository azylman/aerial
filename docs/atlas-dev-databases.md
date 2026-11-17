# Disposable Atlas development databases

`./scripts/atlas-diff.sh <name> [target_dir]` generates and validates a migration;
it never applies migrations to application databases. An explicit `ATLAS_DEV_URL`
is caller-owned: it is neither created nor dropped by these scripts. Without a
runtime configuration, the existing Docker fallback uses `pgvector/pgvector:pg18`.
`DATABASE_URL` and `POSTGRES_URL` are not provisioning credentials.

## Runtime contract

Mount `/local/atlas-dev.yaml` read-only in migration workers and the cleanup job:

```yaml
host: postgres.example.internal
port: 5432
maintenance_database: postgres
user: atlas_runner
template: atlas_vector
name_prefix: atlas_dev
max_age_seconds: 86400
```

This is a deliberately restricted flat YAML mapping of scalar values (plain or
quoted strings and decimal integers); nested YAML, aliases and tags are rejected.
Bash, Python 3 (standard library) and PostgreSQL `psql` are required. Do not place
credentials in YAML. Supply `ATLAS_DEV_PASSWORD` through Infisical → External
Secrets Operator → a scoped Kubernetes Secret → password environment variable.
The helper exports `PGPASSWORD` for psql and Atlas; generated URLs contain no
password. Never enable shell tracing in credential-bearing jobs.

An administrator must prepare a PostgreSQL 18 template with pgvector already
installed. The dedicated migration role needs LOGIN and CREATEDB, permission to
clone that template, and access to the maintenance database. It must have no
superuser, CREATEROLE, replication, or application-role memberships. CREATEDB
does not grant permission to install pgvector. Template upgrades and credentials
are deployment responsibilities; these scripts do not modify application schemas.

Each invocation clones the template into a unique database with a generated
epoch and random suffix. Cleanup bookkeeping is recorded before creation, so
partial setup failures still attempt cleanup. A single EXIT handler performs a
best-effort non-force drop, including after INT/TERM; the original setup or Atlas
status survives cleanup failure. Warnings indicate that the scheduled sweep must
finish cleanup. There are no supervisors, leases or advisory locks.

## Authoritative expiry sweep

```sh
./scripts/atlas-dev-cleanup.sh --config /local/atlas-dev.yaml --dry-run
./scripts/atlas-dev-cleanup.sh --config /local/atlas-dev.yaml
```

Deploy an hourly CronJob with `concurrencyPolicy: Forbid`, initially suspended or
in dry-run until restricted-role canaries pass. The default expiry is 24 hours.
The sweep checks exact generated name format, configured owner, timestamp, OID,
template status and connections, rechecking before a non-force drop. Future names,
malformed names, other owners, templates, active sessions and uncertain or racing
candidates are skipped when detected. Repeated sweeps are safe; failed drops retry next tick.

**Expiry is not a lease.** An idle job older than the expiry can lose its database
and fail on reconnect. Rerun that migration in a fresh disposable database. Active
connection checks reduce disruption but do not guarantee preservation of idle
jobs. A creation or connection race may leave a database for the next sweep. Catalog
rechecking and DROP are separate operations: without locks, replacement of a
database under the same name between them cannot be fully excluded. Generated
random names make accidental reuse unlikely; this is not an atomic identity guard.

For recovery, inspect candidates with dry-run, confirm owner and age using the
maintenance database, and rerun the sweep after connections close. Never force
terminate application sessions. A successful Atlas command does not guarantee
immediate database removal: scheduled cleanup is authoritative. Verify both
migration generation and cleanup outcomes before enabling this rollout.
