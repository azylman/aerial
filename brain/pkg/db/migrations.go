package db

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"log"
	"sort"
	"strings"
	"time"
)

//go:embed migrations/*.sql migrations/atlas.sum
var migrationFS embed.FS

const (
	migrationLockID     = 849201948201
	baselineVersion     = "20261001221347"
	baselineDescription = "baseline"
	baselineStmtCount   = 33
	baselineHash        = "uwrTrII8CWvC/4E6FLZx5vGvYuO48AxqjCh+Wi/43kg="
	operatorVersion     = "Atlas Embedded Runner v1.0.0"
)

// RunMigrations executes all embedded Atlas versioned migrations against PostgreSQL.
// It acquires an advisory lock to prevent race conditions during concurrent worker boots,
// auto-stamps existing legacy databases at the baseline version, and applies any pending migrations.
func RunMigrations(ctx context.Context, database *sql.DB) error {
	if database == nil {
		return fmt.Errorf("db: database cannot be nil")
	}

	conn, err := database.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire db connection for migrations: %w", err)
	}
	defer closeWarn(conn, "migration connection")

	// 1. Acquire advisory lock
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1);", migrationLockID); err != nil {
		return fmt.Errorf("failed to acquire migration advisory lock: %w", err)
	}
	defer func() {
		if _, err := conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1);", migrationLockID); err != nil {
			log.Printf("[DB] Warning releasing migration advisory lock: %v", err)
		}
	}()

	// 2. Ensure atlas_schema_revisions schema and table exist
	if err := ensureRevisionsTable(ctx, conn); err != nil {
		return fmt.Errorf("failed to ensure revisions table: %w", err)
	}

	// 3. Zero-Touch Baseline Guard:
	// If the database already has the 'facts' table but zero revisions recorded,
	// stamp the baseline version as applied so we do not re-run DDL on active data.
	if err := checkAndStampBaseline(ctx, conn); err != nil {
		return fmt.Errorf("failed checking baseline guard: %w", err)
	}

	// 4. Discover and apply pending migrations in order
	if err := applyPendingMigrations(ctx, conn); err != nil {
		return fmt.Errorf("failed applying migrations: %w", err)
	}

	// 5. Sequence resynchronization
	resyncSequences(ctx, conn)

	return nil
}

func ensureRevisionsTable(ctx context.Context, conn *sql.Conn) error {
	const ddl = `
CREATE SCHEMA IF NOT EXISTS atlas_schema_revisions;

CREATE TABLE IF NOT EXISTS atlas_schema_revisions.atlas_schema_revisions (
  version character varying NOT NULL,
  description character varying NOT NULL,
  type bigint NOT NULL DEFAULT 2,
  applied bigint NOT NULL DEFAULT 0,
  total bigint NOT NULL DEFAULT 0,
  executed_at timestamptz NOT NULL,
  execution_time bigint NOT NULL,
  error text NULL,
  error_stmt text NULL,
  hash character varying NOT NULL,
  partial_hashes jsonb NULL,
  operator_version character varying NOT NULL,
  PRIMARY KEY (version)
);`
	_, err := conn.ExecContext(ctx, ddl)
	return err
}

func checkAndStampBaseline(ctx context.Context, conn *sql.Conn) error {
	var factsTableExists bool
	err := conn.QueryRowContext(ctx, "SELECT to_regclass('public.facts') IS NOT NULL;").Scan(&factsTableExists)
	if err != nil {
		return fmt.Errorf("to_regclass check failed: %w", err)
	}

	var appliedCount int
	err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM atlas_schema_revisions.atlas_schema_revisions;").Scan(&appliedCount)
	if err != nil {
		return fmt.Errorf("revisions count failed: %w", err)
	}

	if factsTableExists && appliedCount == 0 {
		log.Printf("[DB] Legacy database detected with existing facts table. Auto-stamping baseline migration %s.", baselineVersion)
		_, err = conn.ExecContext(ctx, `
			INSERT INTO atlas_schema_revisions.atlas_schema_revisions 
			(version, description, type, applied, total, executed_at, execution_time, hash, operator_version)
			VALUES ($1, $2, 2, $3, $3, $4, 0, $5, $6)
			ON CONFLICT (version) DO NOTHING;
		`, baselineVersion, baselineDescription, baselineStmtCount, time.Now(), baselineHash, operatorVersion)
		if err != nil {
			return fmt.Errorf("failed to stamp baseline revision: %w", err)
		}
	}
	return nil
}

type migrationEntry struct {
	filename    string
	version     string
	description string
}

func applyPendingMigrations(ctx context.Context, conn *sql.Conn) error {
	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return fmt.Errorf("failed to read migrations dir: %w", err)
	}

	var migrations []migrationEntry
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		name := entry.Name()
		// Format: <version>_<description>.sql
		base := strings.TrimSuffix(name, ".sql")
		parts := strings.SplitN(base, "_", 2)
		ver := parts[0]
		desc := ""
		if len(parts) > 1 {
			desc = parts[1]
		}
		migrations = append(migrations, migrationEntry{
			filename:    name,
			version:     ver,
			description: desc,
		})
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].version < migrations[j].version
	})

	// Parse hashes from atlas.sum
	hashes := parseAtlasSum()

	for _, m := range migrations {
		var exists bool
		err := conn.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM atlas_schema_revisions.atlas_schema_revisions WHERE version = $1);", m.version).Scan(&exists)
		if err != nil {
			return fmt.Errorf("failed checking revision %s: %w", m.version, err)
		}
		if exists {
			continue
		}

		if err := applyMigrationFile(ctx, conn, m, hashes[m.filename]); err != nil {
			return fmt.Errorf("failed migration %s (%s): %w", m.version, m.description, err)
		}
	}

	return nil
}

func applyMigrationFile(ctx context.Context, conn *sql.Conn, m migrationEntry, hash string) error {
	content, err := migrationFS.ReadFile("migrations/" + m.filename)
	if err != nil {
		return fmt.Errorf("read file error: %w", err)
	}

	stmts := SplitSQLStatements(string(content))
	start := time.Now()

	for _, stmt := range stmts {
		trimmed := strings.TrimSpace(stmt)
		if trimmed == "" {
			continue
		}

		// Separate transactional statements from non-transactional (e.g. CONCURRENTLY)
		if strings.Contains(strings.ToUpper(trimmed), "CONCURRENTLY") {
			if _, err := conn.ExecContext(ctx, trimmed); err != nil {
				return fmt.Errorf("exec non-tx statement failed: %w (stmt: %s)", err, trimmed)
			}
			continue
		}

		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin tx failed: %w", err)
		}
		if _, err := tx.ExecContext(ctx, trimmed); err != nil {
			rollbackWarn(tx, "migration statement")
			return fmt.Errorf("exec tx statement failed: %w (stmt: %s)", err, trimmed)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit tx failed: %w", err)
		}
	}

	execTime := time.Since(start).Nanoseconds()
	if hash == "" {
		hash = "unknown"
	}

	_, err = conn.ExecContext(ctx, `
		INSERT INTO atlas_schema_revisions.atlas_schema_revisions 
		(version, description, type, applied, total, executed_at, execution_time, hash, operator_version)
		VALUES ($1, $2, 2, $3, $3, $4, $5, $6, $7)
		ON CONFLICT (version) DO NOTHING;
	`, m.version, m.description, len(stmts), time.Now(), execTime, hash, operatorVersion)
	if err != nil {
		return fmt.Errorf("failed to record applied revision %s: %w", m.version, err)
	}

	log.Printf("[DB] Successfully applied migration %s (%s) [%d statements, %v]", m.version, m.description, len(stmts), time.Since(start))
	return nil
}

func parseAtlasSum() map[string]string {
	hashes := make(map[string]string)
	content, err := migrationFS.ReadFile("migrations/atlas.sum")
	if err != nil {
		return hashes
	}
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			filename := parts[0]
			hash := strings.TrimPrefix(parts[1], "h1:")
			hashes[filename] = hash
		}
	}
	return hashes
}

// SplitSQLStatements splits a SQL script into discrete statements, taking into account
// single-line comments, block comments, single quotes, double quotes, and PostgreSQL dollar quotes.
func SplitSQLStatements(sqlText string) []string {
	var statements []string
	var current strings.Builder

	inSingleQuote := false
	inDoubleQuote := false
	inDollarQuote := false
	dollarTag := ""
	inLineComment := false
	inBlockComment := false

	chars := []rune(sqlText)
	n := len(chars)

	for i := 0; i < n; i++ {
		c := chars[i]

		// Handle comments
		if inLineComment {
			current.WriteRune(c)
			if c == '\n' {
				inLineComment = false
			}
			continue
		}

		if inBlockComment {
			current.WriteRune(c)
			if c == '*' && i+1 < n && chars[i+1] == '/' {
				current.WriteRune('/')
				i++
				inBlockComment = false
			}
			continue
		}

		if !inSingleQuote && !inDoubleQuote && !inDollarQuote {
			if c == '-' && i+1 < n && chars[i+1] == '-' {
				current.WriteRune('-')
				current.WriteRune('-')
				i++
				inLineComment = true
				continue
			}
			if c == '/' && i+1 < n && chars[i+1] == '*' {
				current.WriteRune('/')
				current.WriteRune('*')
				i++
				inBlockComment = true
				continue
			}
		}

		// Handle strings and dollar-quotes
		if !inDoubleQuote && !inDollarQuote {
			if c == '\'' {
				current.WriteRune(c)
				if inSingleQuote {
					// Check for escaped quote ''
					if i+1 < n && chars[i+1] == '\'' {
						current.WriteRune('\'')
						i++
					} else {
						inSingleQuote = false
					}
				} else {
					inSingleQuote = true
				}
				continue
			}
		}

		if !inSingleQuote && !inDollarQuote {
			if c == '"' {
				current.WriteRune(c)
				if inDoubleQuote {
					inDoubleQuote = false
				} else {
					inDoubleQuote = true
				}
				continue
			}
		}

		if !inSingleQuote && !inDoubleQuote {
			if c == '$' {
				// Find matching tag end
				tagEnd := -1
				for j := i + 1; j < n && j < i+32; j++ {
					if chars[j] == '$' {
						tagEnd = j
						break
					}
					if (chars[j] < 'a' || chars[j] > 'z') && (chars[j] < 'A' || chars[j] > 'Z') && (chars[j] < '0' || chars[j] > '9') && chars[j] != '_' {
						break
					}
				}
				if tagEnd != -1 {
					tag := string(chars[i : tagEnd+1])
					if inDollarQuote {
						if tag == dollarTag {
							inDollarQuote = false
							dollarTag = ""
						}
					} else {
						inDollarQuote = true
						dollarTag = tag
					}
					current.WriteString(tag)
					i = tagEnd
					continue
				}
			}
		}

		// Semicolon delimiter check
		if c == ';' && !inSingleQuote && !inDoubleQuote && !inDollarQuote {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
			continue
		}

		current.WriteRune(c)
	}

	remaining := strings.TrimSpace(current.String())
	if remaining != "" {
		statements = append(statements, remaining)
	}

	return statements
}

func resyncSequences(ctx context.Context, conn *sql.Conn) {
	queries := []string{
		`SELECT setval(pg_get_serial_sequence('facts', 'id'), COALESCE((SELECT MAX(id) FROM facts), 1), (SELECT COUNT(*) > 0 FROM facts));`,
		`SELECT setval(pg_get_serial_sequence('messages', 'row_id'), COALESCE((SELECT MAX(row_id) FROM messages), 1), (SELECT COUNT(*) > 0 FROM messages));`,
	}
	for _, q := range queries {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			log.Printf("[DB] Notice resyncing sequence (%s): %v", q, err)
		}
	}
}
