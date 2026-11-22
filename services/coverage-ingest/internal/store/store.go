package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/services/coverage-ingest/internal/config"
)

// CovdataRunner produces a `go tool covdata func` report over merged coverage dirs.
type CovdataRunner interface {
	Func(ctx context.Context, dirs []string) ([]byte, error)
}

// ExecCovdata runs `go tool covdata func` using the Go binary at GoBin.
type ExecCovdata struct {
	GoBin string
}

// covdataArgs returns the argv (after the go binary) for a merged func report.
func covdataArgs(dirs []string) []string {
	return []string{"tool", "covdata", "func", "-i=" + strings.Join(dirs, ",")}
}

// Func implements CovdataRunner.
func (e ExecCovdata) Func(ctx context.Context, dirs []string) ([]byte, error) {
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, e.GoBin, covdataArgs(dirs)...)
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("covdata func: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

// CoverageStore persists per-service coverage rows.
type CoverageStore interface {
	UpsertService(ctx context.Context, service string, rows []config.CoverageRow) error
	// PruneServices deletes every row whose service is not in keep and returns the
	// deleted row count per pruned service.
	PruneServices(ctx context.Context, keep []string) (map[string]int64, error)
}

// TxBeginner is the subset of a pgx pool the store needs.
type TxBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// PostgresStore writes production_code_coverage.
type PostgresStore struct {
	DB TxBeginner
}

// upsertCoverageSQL matches the retired collector: a 0% sample never lowers a recorded
// percent nor clears an older last_executed_at.
const upsertCoverageSQL = `INSERT INTO production_code_coverage
    (service, file, function_name, coverage_percent, last_executed_at, first_seen_at, updated_at)
SELECT $1, t.file, t.function_name, t.pct,
       CASE WHEN t.pct > 0 THEN now() ELSE NULL END, now(), now()
FROM unnest($2::text[], $3::text[], $4::real[]) AS t(file, function_name, pct)
ON CONFLICT (service, file, function_name) DO UPDATE SET
    coverage_percent = CASE WHEN excluded.coverage_percent > 0
        THEN excluded.coverage_percent ELSE production_code_coverage.coverage_percent END,
    last_executed_at = CASE WHEN excluded.coverage_percent > 0
        THEN now() ELSE production_code_coverage.last_executed_at END,
    updated_at = now()`

// deleteAbsentSQL removes a service's rows for functions no longer present in its binaries.
const deleteAbsentSQL = `DELETE FROM production_code_coverage p
WHERE p.service = $1
  AND NOT EXISTS (
    SELECT 1 FROM unnest($2::text[], $3::text[]) AS k(file, function_name)
    WHERE k.file = p.file AND k.function_name = p.function_name)`

// UpsertService applies rows for service in one transaction and prunes functions absent
// from rows. An empty rows slice is a no-op so a broken report never wipes a service.
func (s PostgresStore) UpsertService(ctx context.Context, service string, rows []config.CoverageRow) (err error) {
	if len(rows) == 0 {
		return nil
	}
	files := make([]string, len(rows))
	funcs := make([]string, len(rows))
	pcts := make([]float32, len(rows))
	for i, r := range rows {
		files[i], funcs[i], pcts[i] = r.File, r.Function, float32(r.Percent)
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin %s: %w", service, err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback(ctx))
		}
	}()
	if _, err = tx.Exec(ctx, upsertCoverageSQL, service, files, funcs, pcts); err != nil {
		return fmt.Errorf("upsert %s: %w", service, err)
	}
	if _, err = tx.Exec(ctx, deleteAbsentSQL, service, files, funcs); err != nil {
		return fmt.Errorf("prune %s: %w", service, err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit %s: %w", service, err)
	}
	return nil
}

// pruneServicesSQL deletes rows of services outside the keep set. eri-restate is
// excluded in SQL as well as by PruneKeepSet.
const pruneServicesSQL = `WITH pruned AS (
    DELETE FROM production_code_coverage
    WHERE service <> ALL($1::text[]) AND service <> 'eri-restate'
    RETURNING service)
SELECT service, count(*) FROM pruned GROUP BY service ORDER BY service`

// PruneServices deletes rows for services outside keep in one transaction. An empty
// keep set is refused.
func (s PostgresStore) PruneServices(ctx context.Context, keep []string) (pruned map[string]int64, err error) {
	if len(keep) == 0 {
		return nil, errors.New("prune: refusing empty keep set")
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin prune: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback(ctx))
		}
	}()
	rows, err := tx.Query(ctx, pruneServicesSQL, keep)
	if err != nil {
		return nil, fmt.Errorf("prune: %w", err)
	}
	pruned = map[string]int64{}
	for rows.Next() {
		var service string
		var n int64
		if err = rows.Scan(&service, &n); err != nil {
			rows.Close()
			return nil, fmt.Errorf("prune scan: %w", err)
		}
		pruned[service] = n
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("prune rows: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit prune: %w", err)
	}
	return pruned, nil
}
