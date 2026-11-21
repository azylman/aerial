package pgutil

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PoolOptions configures NewPool.
type PoolOptions struct {
	MaxConns, MinConns int32
	// Ping verifies connectivity (5s timeout) and closes the pool on failure.
	Ping bool
}

// NewPool parses dsn, applies the connection bounds and opens a pgx pool.
func NewPool(ctx context.Context, dsn string, opts PoolOptions) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres url: %w", err)
	}
	cfg.MaxConns, cfg.MinConns = opts.MaxConns, opts.MinConns
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pgxpool: %w", err)
	}
	if opts.Ping {
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := pool.Ping(pingCtx); err != nil {
			pool.Close()
			return nil, fmt.Errorf("ping postgres: %w", err)
		}
	}
	return pool, nil
}
