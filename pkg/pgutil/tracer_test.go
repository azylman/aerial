package pgutil

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/pgtest"
)

func TestSQLOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{name: "empty", query: "", want: ""},
		{name: "only whitespace", query: "   \t\n\r  ", want: ""},
		{name: "only line comment", query: "-- just a comment", want: ""},
		{name: "only line comment with newline", query: "-- just a comment\n", want: ""},
		{name: "only block comment", query: "/* just a comment */", want: ""},
		{name: "unclosed block comment", query: "/* unclosed comment", want: ""},
		{name: "leading whitespace select", query: "   SELECT * FROM users", want: "SELECT"},
		{name: "lowercase select", query: "select id from users", want: "SELECT"},
		{name: "mixed case insert", query: "  InSeRt into t values (1)", want: "INSERT"},
		{name: "line comment then update", query: "-- update query\nUPDATE users SET name = 'a'", want: "UPDATE"},
		{name: "block comment then delete", query: "/* delete query */ DELETE FROM users", want: "DELETE"},
		{name: "multiple mixed comments then with", query: "/* block 1 */\n-- line 1\n  /* block 2 */\nWITH cte AS (SELECT 1) SELECT * FROM cte", want: "WITH"},
		{name: "commit with semicolon", query: "COMMIT;", want: "COMMIT"},
		{name: "values statement", query: "VALUES (1, 'test')", want: "VALUES"},
		{name: "begin transaction", query: "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED", want: "BEGIN"},
		{name: "unknown verb", query: "UNKNOWN statement", want: ""},
		{name: "numbers", query: "12345", want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := SQLOperation(tc.query); got != tc.want {
				t.Fatalf("SQLOperation(%q) = %q, want %q", tc.query, got, tc.want)
			}
		})
	}
}

func setupTestTracer(t *testing.T) (pgx.QueryTracer, *sdktrace.TracerProvider, *tracetest.SpanRecorder) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prevTP := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
	})
	return NewQueryTracer(), tp, sr
}

func TestQueryTracerNoParent(t *testing.T) {
	tracer, _, sr := setupTestTracer(t)
	ctx := tracer.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{
		SQL:  "SELECT 1",
		Args: []any{42},
	})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: nil})

	if len(sr.Ended()) != 0 {
		t.Fatalf("expected 0 ended spans, got %d", len(sr.Ended()))
	}
}

func TestQueryTracerValidParent(t *testing.T) {
	tracer, tp, sr := setupTestTracer(t)
	parentCtx, parentSpan := tp.Tracer("test").Start(context.Background(), "parent-op")
	defer parentSpan.End()

	rawSQL := "SELECT password_hash, token FROM users WHERE id = $1 AND secret = $2"
	ctx := tracer.TraceQueryStart(parentCtx, nil, pgx.TraceQueryStartData{
		SQL:  rawSQL,
		Args: []any{1001, "super-secret-token"},
	})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: nil})

	ended := sr.Ended()
	if len(ended) != 1 {
		t.Fatalf("expected 1 ended span, got %d", len(ended))
	}
	span := ended[0]
	if span.Name() != "postgresql SELECT" {
		t.Fatalf("span.Name() = %q, want %q", span.Name(), "postgresql SELECT")
	}
	if span.SpanKind() != trace.SpanKindClient {
		t.Fatalf("span.SpanKind() = %v, want %v", span.SpanKind(), trace.SpanKindClient)
	}
	if span.Parent().SpanID() != parentSpan.SpanContext().SpanID() {
		t.Fatalf("span parent ID = %v, want %v", span.Parent().SpanID(), parentSpan.SpanContext().SpanID())
	}

	assertTracedSpanAttributes(t, span, rawSQL)
}

func assertTracedSpanAttributes(t *testing.T, span sdktrace.ReadOnlySpan, rawSQL string) {
	t.Helper()
	var foundSystem, foundOp bool
	for _, a := range span.Attributes() {
		key := string(a.Key)
		val := a.Value.AsString()
		if key == "db.system.name" && val == "postgresql" {
			foundSystem = true
		}
		if key == "db.operation.name" && val == "SELECT" {
			foundOp = true
		}
		if key == "db.statement" || key == "db.query.text" || val == rawSQL || val == "super-secret-token" {
			t.Fatalf("forbidden raw query or argument leaked on attribute %s: %s", key, val)
		}
	}
	if !foundSystem {
		t.Errorf("missing db.system.name attribute")
	}
	if !foundOp {
		t.Errorf("missing db.operation.name attribute")
	}
}

func TestQueryTracerErrorHandling(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantError bool
	}{
		{name: "nil error is OK", err: nil, wantError: false},
		{name: "pgx.ErrNoRows is not error", err: pgx.ErrNoRows, wantError: false},
		{name: "sql.ErrNoRows is not error", err: sql.ErrNoRows, wantError: false},
		{name: "other query error sets codes.Error", err: errors.New("connection reset by peer"), wantError: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tracer, tp, sr := setupTestTracer(t)
			parentCtx, parentSpan := tp.Tracer("test").Start(context.Background(), "parent")
			defer parentSpan.End()

			ctx := tracer.TraceQueryStart(parentCtx, nil, pgx.TraceQueryStartData{
				SQL: "INSERT INTO items VALUES (1)",
			})
			tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: tc.err})

			ended := sr.Ended()
			if len(ended) != 1 {
				t.Fatalf("expected 1 ended span, got %d", len(ended))
			}
			span := ended[0]
			isErr := span.Status().Code == codes.Error
			if isErr != tc.wantError {
				t.Fatalf("span status error = %v, want %v (status=%+v)", isErr, tc.wantError, span.Status())
			}
			if tc.wantError && span.Status().Description != "query failed" {
				t.Fatalf("span status description = %q, want %q", span.Status().Description, "query failed")
			}
		})
	}
}

func TestQueryTracerUnrecognizedOp(t *testing.T) {
	tracer, tp, sr := setupTestTracer(t)
	parentCtx, parentSpan := tp.Tracer("test").Start(context.Background(), "parent")
	defer parentSpan.End()

	ctx := tracer.TraceQueryStart(parentCtx, nil, pgx.TraceQueryStartData{
		SQL: "-- only comments\n",
	})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: nil})

	ended := sr.Ended()
	if len(ended) != 1 {
		t.Fatalf("expected 1 ended span, got %d", len(ended))
	}
	span := ended[0]
	if span.Name() != "postgresql" {
		t.Fatalf("span.Name() = %q, want %q", span.Name(), "postgresql")
	}
	for _, a := range span.Attributes() {
		if a.Key == "db.operation.name" {
			t.Fatalf("unexpected db.operation.name attribute on unknown operation")
		}
	}
}

func TestOpenTracedPgxDB(t *testing.T) {
	t.Run("OpenSQL emits traced child span", func(t *testing.T) {
		_, tp, sr := setupTestTracer(t)
		dsn := pgtest.NewDSN(t)

		db, err := OpenSQL(context.Background(), dsn, SQLOptions{})
		if err != nil {
			t.Fatalf("OpenSQL failed: %v", err)
		}
		defer func() { _ = db.Close() }()

		sr.Reset()
		parentCtx, parentSpan := tp.Tracer("test").Start(context.Background(), "parent")
		defer parentSpan.End()

		var one int
		if err := db.QueryRowContext(parentCtx, "SELECT 1").Scan(&one); err != nil {
			t.Fatalf("QueryRowContext failed: %v", err)
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
	})

	t.Run("invalid url falls back to lazy sql.Open", func(t *testing.T) {
		db, err := OpenTracedPgxDB("://invalid-url")
		if err != nil {
			t.Fatalf("expected lazy open fallback, got err: %v", err)
		}
		defer func() { _ = db.Close() }()
	})
}
