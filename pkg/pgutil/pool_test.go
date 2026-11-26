package pgutil

import (
	"context"
	"strings"
	"testing"
)

func TestNewPool(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		dsn     string
		opts    PoolOptions
		wantErr string
	}{
		{"invalid url", "://invalid-url", PoolOptions{MaxConns: 10, MinConns: 2}, "parse postgres url"},
		{"lazy pool without ping", "postgres://u:p@127.0.0.1:1/db", PoolOptions{MaxConns: 10, MinConns: 0}, ""},
		{"ping failure", "postgres://u:p@127.0.0.1:1/db", PoolOptions{MaxConns: 10, Ping: true}, "ping postgres"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool, err := NewPool(context.Background(), tc.dsn, tc.opts)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := pool.Config().MaxConns; got != tc.opts.MaxConns {
				t.Fatalf("MaxConns = %d, want %d", got, tc.opts.MaxConns)
			}
			if pool.Config().ConnConfig.Tracer == nil {
				t.Fatalf("expected Tracer to be set on pool ConnConfig")
			}
			pool.Close()
		})
	}
}
