package db

import (
	"context"
	"database/sql"
)

// initSchemaPostgres delegates schema migration and versioning strictly to the embedded Atlas migration runner.
// The declarative single source of truth for the PostgreSQL schema is defined in brain/pkg/db/schema.sql.
func initSchemaPostgres(ctx context.Context, database *sql.DB) error {
	return RunMigrations(ctx, database)
}
