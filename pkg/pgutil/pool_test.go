package pgutil

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/pgtest"
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
			pool.Close()
		})
	}
}

func TestNewPoolTracedQuery(t *testing.T) {
	_, tp, sr := setupTestTracer(t)
	dsn := pgtest.NewDSN(t)

	pool, err := NewPool(context.Background(), dsn, PoolOptions{MaxConns: 5, Ping: true})
	if err != nil {
		t.Fatalf("NewPool failed: %v", err)
	}
	defer pool.Close()

	sr.Reset()
	parentCtx, parentSpan := tp.Tracer("test").Start(context.Background(), "parent")
	defer parentSpan.End()

	var one int
	if err := pool.QueryRow(parentCtx, "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("QueryRow failed: %v", err)
	}
	if one != 1 {
		t.Fatalf("got %d, want 1", one)
	}

	ended := sr.Ended()
	var found bool
	for _, s := range ended {
		if s.Name() == "postgresql SELECT" && s.SpanKind() == trace.SpanKindClient && s.Parent().SpanID() == parentSpan.SpanContext().SpanID() {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected child span 'postgresql SELECT' under parent %s, ended spans: %v", parentSpan.SpanContext().SpanID(), ended)
	}
}
