package pgutil

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"unicode"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/tracing"
)

type querySpanKey struct{}

// standardSQLVerbs is the allowlist of recognized SQL statement verbs for span naming.
var standardSQLVerbs = map[string]struct{}{
	"SELECT":     {},
	"INSERT":     {},
	"UPDATE":     {},
	"DELETE":     {},
	"WITH":       {},
	"VALUES":     {},
	"BEGIN":      {},
	"COMMIT":     {},
	"ROLLBACK":   {},
	"SAVEPOINT":  {},
	"RELEASE":    {},
	"LOCK":       {},
	"EXPLAIN":    {},
	"SHOW":       {},
	"SET":        {},
	"RESET":      {},
	"NOTIFY":     {},
	"LISTEN":     {},
	"UNLISTEN":   {},
	"TRUNCATE":   {},
	"CREATE":     {},
	"ALTER":      {},
	"DROP":       {},
	"COPY":       {},
	"CALL":       {},
	"DO":         {},
	"MERGE":      {},
	"FETCH":      {},
	"DECLARE":    {},
	"CLOSE":      {},
	"PREPARE":    {},
	"EXECUTE":    {},
	"DEALLOCATE": {},
	"VACUUM":     {},
	"ANALYZE":    {},
}

// SQLOperation extracts the first SQL verb from query after skipping leading
// whitespace and comments (-- line comments and /* */ block comments).
// It returns the uppercase verb if it belongs to standardSQLVerbs, or "" otherwise.
func SQLOperation(query string) string {
	s := query
	for {
		s = strings.TrimLeftFunc(s, unicode.IsSpace)
		if strings.HasPrefix(s, "--") {
			if idx := strings.IndexByte(s, '\n'); idx >= 0 {
				s = s[idx+1:]
				continue
			}
			return ""
		}
		if strings.HasPrefix(s, "/*") {
			if idx := strings.Index(s, "*/"); idx >= 0 {
				s = s[idx+2:]
				continue
			}
			return ""
		}
		break
	}

	// Extract first ASCII token ([A-Za-z]+)
	var end int
	for end < len(s) {
		b := s[end]
		if (b < 'A' || b > 'Z') && (b < 'a' || b > 'z') {
			break
		}
		end++
	}
	if end == 0 {
		return ""
	}
	verb := strings.ToUpper(s[:end])
	if _, ok := standardSQLVerbs[verb]; ok {
		return verb
	}
	return ""
}

// queryTracer implements pgx.QueryTracer with parent-gated OpenTelemetry spans.
type queryTracer struct{}

// NewQueryTracer returns a pgx.QueryTracer that records low-cardinality, privacy-safe
// spans for Postgres queries only when the incoming context has a valid parent span.
func NewQueryTracer() pgx.QueryTracer {
	return &queryTracer{}
}

func (*queryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return ctx
	}

	op := SQLOperation(data.SQL)
	spanName := "postgresql"
	if op != "" {
		spanName = "postgresql " + op
	}

	attrs := []attribute.KeyValue{attribute.String("db.system.name", "postgresql")}
	if op != "" {
		attrs = append(attrs, attribute.String("db.operation.name", op))
	}

	//nolint:spancheck // span is stored in context and ended in TraceQueryEnd
	ctx, span := tracing.Tracer().Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
	//nolint:spancheck // span is stored in context and ended in TraceQueryEnd
	return context.WithValue(ctx, querySpanKey{}, span)
}

func (*queryTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	span, ok := ctx.Value(querySpanKey{}).(trace.Span)
	if !ok || span == nil {
		return
	}

	if data.Err != nil && !errors.Is(data.Err, pgx.ErrNoRows) && !errors.Is(data.Err, sql.ErrNoRows) {
		span.SetStatus(codes.Error, "query failed")
	}
	span.End()
}

// OpenTracedPgxDB opens a database/sql handle backed by pgx/v5/stdlib configured
// with OpenTelemetry query tracing. If parsing dsn fails, it falls back to
// sql.Open("pgx", dsn) to preserve lazy connection string validation semantics.
func OpenTracedPgxDB(dsn string) (*sql.DB, error) {
	connCfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return sql.Open("pgx", dsn)
	}
	connCfg.Tracer = NewQueryTracer()
	return stdlib.OpenDB(*connCfg), nil
}
