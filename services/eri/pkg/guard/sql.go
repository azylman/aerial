package guard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	// MaxUnboundedPlanRows is the maximum planner row estimate allowed for SELECT queries without a LIMIT node.
	MaxUnboundedPlanRows = 25
	// MaxSelectLimitRows is the maximum planner row estimate allowed on a SELECT LIMIT node.
	MaxSelectLimitRows = 50
	// DefaultSQLGuardTimeout bounds live PostgreSQL catalog/EXPLAIN validation latency.
	DefaultSQLGuardTimeout = 2 * time.Second
)

var (
	psqlFormatFlags = map[string]bool{"-A": true, "--no-align": true, "--csv": true, "-H": true, "--html": true}
	schemaErrCodes  = map[string]bool{"42703": true, "42P01": true, "42704": true, "42883": true}
)

// SQLColumn describes a resolved output column from PostgreSQL RowDescription.
type SQLColumn struct {
	Name     string
	TableOID uint32
	AttrNum  int16
}

// SQLPlan represents a node in PostgreSQL's EXPLAIN (VERBOSE, FORMAT JSON) plan tree.
type SQLPlan struct {
	NodeType     string    `json:"Node Type"`
	RelationName string    `json:"Relation Name"`
	PlanRows     float64   `json:"Plan Rows"`
	Output       []string  `json:"Output"`
	Plans        []SQLPlan `json:"Plans"`
}

// SQLStatementInfo holds the parsed PostgreSQL catalog and planner metadata for a statement.
type SQLStatementInfo struct {
	Columns         []SQLColumn
	TableColCounts  map[uint32]int
	TableNamesByOID map[uint32]string
	RelColCounts    map[string]int
	Plan            *SQLPlan
}

// SQLDescriber validates and describes SQL statements using PostgreSQL's parser and planner.
type SQLDescriber interface {
	Describe(ctx context.Context, sql string) (*SQLStatementInfo, error)
	SchemaHint(ctx context.Context, sql string, prepareErr error) string
}

type sqlCatalogConn interface {
	PrepareAndExplain(ctx context.Context, sqlStr string) ([]SQLColumn, []byte, error)
	QueryCatalog(ctx context.Context, sqlStr string, args ...any) ([][]string, error)
	Close(ctx context.Context)
}

type livePgxConn struct {
	conn *pgx.Conn
}

func (c *livePgxConn) PrepareAndExplain(ctx context.Context, sqlStr string) ([]SQLColumn, []byte, error) {
	tx, err := c.conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, nil, nil //nolint:nilerr
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck

	sd, err := tx.Prepare(ctx, "", sqlStr)
	if err != nil {
		return nil, nil, err
	}
	cols := make([]SQLColumn, len(sd.Fields))
	for i, f := range sd.Fields {
		cols[i] = SQLColumn{Name: f.Name, TableOID: f.TableOID, AttrNum: int16(f.TableAttributeNumber)}
	}
	var explainRaw []byte
	if len(cols) > 0 {
		_ = tx.QueryRow(ctx, "EXPLAIN (VERBOSE, FORMAT JSON) "+sqlStr).Scan(&explainRaw) //nolint:errcheck
	}
	return cols, explainRaw, nil
}

func (c *livePgxConn) QueryCatalog(ctx context.Context, sqlStr string, args ...any) ([][]string, error) {
	rows, err := c.conn.Query(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]string
	n := len(rows.FieldDescriptions())
	for rows.Next() {
		vals := make([]string, n)
		ptrs := make([]any, n)
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if rows.Scan(ptrs...) == nil {
			out = append(out, vals)
		}
	}
	return out, nil
}

func (c *livePgxConn) Close(ctx context.Context) {
	_ = c.conn.Close(ctx) //nolint:errcheck
}

// DefaultSQLDescriber returns a PostgreSQL describer backed by DATABASE_URL, or nil if unconfigured.
func DefaultSQLDescriber() SQLDescriber {
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if dsn == "" || dsn == ":memory:" || strings.HasPrefix(dsn, "sqlite") {
		return nil
	}
	return &pgxDescriber{
		connect: func(ctx context.Context) (sqlCatalogConn, error) {
			c, err := pgx.Connect(ctx, dsn)
			if err != nil {
				return nil, err
			}
			return &livePgxConn{conn: c}, nil
		},
	}
}

type pgxDescriber struct {
	connect func(ctx context.Context) (sqlCatalogConn, error)
	conn    sqlCatalogConn
}

func (d *pgxDescriber) ensureConn(ctx context.Context) (sqlCatalogConn, error) {
	if d.conn != nil {
		return d.conn, nil
	}
	c, err := d.connect(ctx)
	if err != nil {
		return nil, err
	}
	d.conn = c
	return c, nil
}

func (d *pgxDescriber) closeConn(ctx context.Context) {
	if d.conn != nil {
		d.conn.Close(ctx)
		d.conn = nil
	}
}

func (d *pgxDescriber) Describe(ctx context.Context, sqlStr string) (*SQLStatementInfo, error) {
	conn, err := d.ensureConn(ctx)
	if err != nil {
		return nil, nil //nolint:nilerr
	}
	cols, explainRaw, err := conn.PrepareAndExplain(ctx, sqlStr)
	if err != nil {
		return nil, err
	}

	info := &SQLStatementInfo{
		Columns:         cols,
		TableColCounts:  make(map[uint32]int),
		TableNamesByOID: make(map[uint32]string),
		RelColCounts:    make(map[string]int),
	}
	for _, col := range cols {
		if col.TableOID != 0 {
			info.TableColCounts[col.TableOID] = 0
		}
	}

	d.populateOIDMetadata(ctx, conn, info)
	d.populateExplainPlan(ctx, conn, info, explainRaw)
	return info, nil
}

func (d *pgxDescriber) populateOIDMetadata(ctx context.Context, conn sqlCatalogConn, info *SQLStatementInfo) {
	for oid := range info.TableColCounts {
		rows, qErr := conn.QueryCatalog(ctx,
			"SELECT c.relname, count(a.attnum)::text FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid WHERE c.oid = $1 AND a.attnum > 0 AND NOT a.attisdropped GROUP BY c.relname",
			oid,
		)
		if qErr == nil && len(rows) > 0 && len(rows[0]) == 2 {
			if cnt, pErr := strconv.Atoi(rows[0][1]); pErr == nil {
				info.TableNamesByOID[oid] = rows[0][0]
				info.TableColCounts[oid] = cnt
			}
		}
	}
}

func (d *pgxDescriber) populateExplainPlan(ctx context.Context, conn sqlCatalogConn, info *SQLStatementInfo, explainRaw []byte) {
	if len(explainRaw) == 0 {
		return
	}
	var roots []struct {
		Plan SQLPlan `json:"Plan"`
	}
	if json.Unmarshal(explainRaw, &roots) != nil || len(roots) == 0 {
		return
	}
	info.Plan = &roots[0].Plan
	for _, rel := range collectPlanRelations(info.Plan) {
		rows, rErr := conn.QueryCatalog(ctx,
			"SELECT count(a.attnum)::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_attribute a ON a.attrelid = c.oid WHERE n.nspname = 'public' AND c.relname = $1 AND a.attnum > 0 AND NOT a.attisdropped",
			rel,
		)
		if rErr == nil && len(rows) > 0 && len(rows[0]) == 1 {
			if cnt, pErr := strconv.Atoi(rows[0][0]); pErr == nil && cnt > 0 {
				info.RelColCounts[rel] = cnt
			}
		}
	}
}

func (d *pgxDescriber) SchemaHint(ctx context.Context, sqlStr string, prepareErr error) string {
	var pgErr *pgconn.PgError
	if !errors.As(prepareErr, &pgErr) || !schemaErrCodes[pgErr.Code] {
		return ""
	}
	conn, err := d.ensureConn(ctx)
	if err != nil {
		return ""
	}

	identSet := extractIdentifierSet(sqlStr)
	rows, err := conn.QueryCatalog(ctx,
		"SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' ORDER BY table_name, ordinal_position",
	)
	if err != nil {
		return fmt.Sprintf("psql query validation failed (%s: %s).", pgErr.Code, pgErr.Message)
	}

	tableCols := make(map[string][]string)
	var matchedTables []string
	for _, r := range rows {
		if len(r) == 3 {
			tbl, col, dtype := r[0], r[1], r[2]
			if _, seen := tableCols[tbl]; !seen && identSet[strings.ToLower(tbl)] {
				matchedTables = append(matchedTables, tbl)
			}
			tableCols[tbl] = append(tableCols[tbl], fmt.Sprintf("%s (%s)", col, dtype))
		}
	}

	if len(matchedTables) > 0 {
		var parts []string
		for _, tbl := range matchedTables {
			parts = append(parts, fmt.Sprintf("%s: [%s]", tbl, strings.Join(tableCols[tbl], ", ")))
		}
		return fmt.Sprintf("psql query validation failed: %s. Valid schema for referenced table(s): %s", pgErr.Message, strings.Join(parts, "; "))
	}

	return fmt.Sprintf("psql query validation failed: %s. Verify table and column names against schema.sql before querying.", pgErr.Message)
}

func collectPlanRelations(plan *SQLPlan) []string {
	if plan == nil {
		return nil
	}
	seen := make(map[string]bool)
	var rels []string
	var walk func(p *SQLPlan)
	walk = func(p *SQLPlan) {
		if p.RelationName != "" && !seen[p.RelationName] {
			seen[p.RelationName] = true
			rels = append(rels, p.RelationName)
		}
		for i := range p.Plans {
			walk(&p.Plans[i])
		}
	}
	walk(plan)
	return rels
}

func validateStatementInfo(info *SQLStatementInfo) *Decision {
	if info == nil || len(info.Columns) == 0 {
		return nil
	}
	if info.Plan != nil && info.Plan.NodeType == "ModifyTable" {
		return nil
	}
	if isFullTableProjection(info) {
		var colNames []string
		for _, col := range info.Columns {
			colNames = append(colNames, col.Name)
		}
		return &Decision{
			Decision: DecisionDeny,
			Reason:   fmt.Sprintf("Wildcard SELECT * is prohibited in psql to prevent context bloat. Available columns: %s. Specify only the columns you need.", strings.Join(colNames, ", ")),
		}
	}
	return validatePlanLimit(info.Plan)
}

func isFullTableProjection(info *SQLStatementInfo) bool {
	selectedByOID := make(map[uint32]map[int16]struct{})
	hasTableOID := false
	for _, col := range info.Columns {
		if col.TableOID != 0 && col.AttrNum > 0 {
			hasTableOID = true
			if selectedByOID[col.TableOID] == nil {
				selectedByOID[col.TableOID] = make(map[int16]struct{})
			}
			selectedByOID[col.TableOID][col.AttrNum] = struct{}{}
		}
	}
	for oid, attrs := range selectedByOID {
		if total := info.TableColCounts[oid]; total > 0 && len(attrs) >= total {
			return true
		}
	}
	if !hasTableOID {
		for _, total := range info.RelColCounts {
			if total > 0 && len(info.Columns) == total {
				return true
			}
		}
	}
	return false
}

func validatePlanLimit(plan *SQLPlan) *Decision {
	if plan == nil {
		return nil
	}
	if plan.NodeType == "Limit" {
		if plan.PlanRows > MaxSelectLimitRows {
			return &Decision{
				Decision: DecisionDeny,
				Reason:   fmt.Sprintf("psql LIMIT exceeds maximum allowed rows (%d). Use LIMIT <= %d to prevent context bloat.", MaxSelectLimitRows, MaxSelectLimitRows),
			}
		}
		return nil
	}
	if plan.PlanRows > MaxUnboundedPlanRows {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   fmt.Sprintf("Unbounded multi-row psql SELECT detected (estimated rows: %.0f). Add a 'LIMIT <N>' clause (<= %d) to prevent context spillovers.", plan.PlanRows, MaxSelectLimitRows),
		}
	}
	return nil
}

type locatedToken struct {
	value       string
	startOffset int
	endOffset   int
}

type psqlInvocation struct {
	insertOffset  int
	hasFormatFlag bool
	queries       []string
}

func inspectPsqlPipeline(cmdLine string, describer SQLDescriber) *Decision {
	invocations := findPsqlInvocations(cmdLine)
	if len(invocations) == 0 {
		return &Decision{Decision: DecisionAllow}
	}

	if describer != nil {
		if dec := evaluatePsqlInvocations(invocations, describer); dec != nil {
			return dec
		}
	}

	rewritten := cmdLine
	for i := len(invocations) - 1; i >= 0; i-- {
		inv := invocations[i]
		if !inv.hasFormatFlag && inv.insertOffset > 0 && inv.insertOffset <= len(rewritten) {
			rewritten = rewritten[:inv.insertOffset] + " -A" + rewritten[inv.insertOffset:]
		}
	}
	if rewritten != cmdLine {
		return &Decision{
			Decision:  DecisionAllow,
			Overwrite: map[string]any{"CommandLine": rewritten},
		}
	}
	return &Decision{Decision: DecisionAllow}
}

func evaluatePsqlInvocations(invocations []psqlInvocation, describer SQLDescriber) *Decision {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultSQLGuardTimeout)
	defer cancel()
	if closer, ok := describer.(*pgxDescriber); ok {
		defer closer.closeConn(ctx)
	}

	for _, inv := range invocations {
		for _, rawQuery := range inv.queries {
			if dec := evaluatePsqlQuery(ctx, strings.TrimSpace(rawQuery), describer); dec != nil {
				return dec
			}
		}
	}
	return nil
}

func evaluatePsqlQuery(ctx context.Context, trimmed string, describer SQLDescriber) *Decision {
	if trimmed == "" || strings.HasPrefix(trimmed, "\\") {
		return nil
	}
	for _, stmt := range splitSQLStatements(trimmed) {
		if strings.HasPrefix(stmt, "\\") {
			continue
		}
		info, err := describer.Describe(ctx, stmt)
		if err != nil {
			if hint := describer.SchemaHint(ctx, stmt, err); hint != "" {
				return &Decision{Decision: DecisionDeny, Reason: hint}
			}
			continue
		}
		if dec := validateStatementInfo(info); dec != nil {
			return dec
		}
	}
	return nil
}

func findPsqlInvocations(cmdLine string) []psqlInvocation {
	var invocations []psqlInvocation
	for _, seg := range splitPipelineWithOffsets(cmdLine) {
		tokens := tokenizeWithOffsets(seg.text, seg.offset)
		idx := skipEnvTokens(tokens)
		if idx < len(tokens) && filepath.Base(tokens[idx].value) == "psql" {
			invocations = append(invocations, parsePsqlArgs(tokens[idx].endOffset, tokens[idx+1:]))
		}
	}
	return invocations
}

func skipEnvTokens(tokens []locatedToken) int {
	idx := 0
	for idx < len(tokens) && strings.Contains(tokens[idx].value, "=") && !strings.HasPrefix(tokens[idx].value, "-") {
		idx++
	}
	return idx
}

func parsePsqlArgs(insertOffset int, tokens []locatedToken) psqlInvocation {
	inv := psqlInvocation{insertOffset: insertOffset}
	for i := 0; i < len(tokens); i++ {
		t := tokens[i].value
		if psqlFormatFlags[t] || strings.HasPrefix(t, "-Pformat=") || strings.HasPrefix(t, "--pset=format=") {
			inv.hasFormatFlag = true
			continue
		}
		if (t == "-P" || t == "--pset") && i+1 < len(tokens) {
			i++
			if strings.HasPrefix(tokens[i].value, "format=") {
				inv.hasFormatFlag = true
			}
			continue
		}
		if (t == "-c" || t == "--command") && i+1 < len(tokens) {
			i++
			inv.queries = append(inv.queries, tokens[i].value)
			continue
		}
		if strings.HasPrefix(t, "--command=") {
			inv.queries = append(inv.queries, strings.TrimPrefix(t, "--command="))
			continue
		}
		if strings.HasPrefix(t, "-") && !strings.HasPrefix(t, "--") {
			i = parseShortPsqlFlags(&inv, t[1:], tokens, i)
		}
	}
	return inv
}

func parseShortPsqlFlags(inv *psqlInvocation, flags string, tokens []locatedToken, i int) int {
	for pos := 0; pos < len(flags); pos++ {
		ch := flags[pos]
		if ch == 'A' || ch == 'H' {
			inv.hasFormatFlag = true
			continue
		}
		if ch == 'c' {
			if pos+1 < len(flags) {
				inv.queries = append(inv.queries, flags[pos+1:])
			} else if i+1 < len(tokens) {
				i++
				inv.queries = append(inv.queries, tokens[i].value)
			}
			return i
		}
		if strings.ContainsRune("UdhpvfoLTFRP", rune(ch)) {
			if pos+1 >= len(flags) && i+1 < len(tokens) {
				i++
			}
			return i
		}
	}
	return i
}

type pipelineSegment struct {
	text   string
	offset int
}

func splitPipelineWithOffsets(cmd string) []pipelineSegment {
	var segs []pipelineSegment
	inSingle, inDouble, escaped := false, false, false
	start := 0

	flush := func(end int) {
		raw := cmd[start:end]
		if trimmed := strings.TrimSpace(raw); trimmed != "" {
			segs = append(segs, pipelineSegment{text: trimmed, offset: start + strings.Index(raw, trimmed)})
		}
	}

	for i := 0; i < len(cmd); i++ {
		ch := cmd[i]
		if escaped {
			escaped = false
			continue
		}
		if ch == '\\' && !inSingle {
			escaped = true
			continue
		}
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
		} else if ch == '"' && !inSingle {
			inDouble = !inDouble
		} else if !inSingle && !inDouble {
			if step := delimiterWidth(cmd, i); step > 0 {
				flush(i)
				i += step - 1
				start = i + 1
			}
		}
	}
	flush(len(cmd))
	return segs
}

func delimiterWidth(cmd string, i int) int {
	ch := cmd[i]
	if (ch == '&' || ch == '|') && i+1 < len(cmd) && cmd[i+1] == ch {
		return 2
	}
	if ch == ';' || ch == '|' || ch == '\n' {
		return 1
	}
	return 0
}

func tokenizeWithOffsets(sub string, baseOffset int) []locatedToken {
	var tokens []locatedToken
	var current strings.Builder
	tokStart := 0
	inToken, inSingle, inDouble, escaped := false, false, false, false

	for i := 0; i < len(sub); i++ {
		ch := sub[i]
		if !escaped && !inSingle && !inDouble && (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r') {
			if inToken {
				tokens = append(tokens, locatedToken{value: current.String(), startOffset: baseOffset + tokStart, endOffset: baseOffset + i})
				current.Reset()
				inToken = false
			}
			continue
		}
		if !inToken {
			tokStart = i
			inToken = true
		}
		switch {
		case escaped:
			current.WriteByte(ch)
			escaped = false
		case ch == '\\' && !inSingle:
			escaped = true
		case ch == '\'' && !inDouble:
			inSingle = !inSingle
		case ch == '"' && !inSingle:
			inDouble = !inDouble
		default:
			current.WriteByte(ch)
		}
	}
	if inToken {
		tokens = append(tokens, locatedToken{value: current.String(), startOffset: baseOffset + tokStart, endOffset: baseOffset + len(sub)})
	}
	return tokens
}

func splitSQLStatements(sqlStr string) []string {
	var stmts []string
	inSingle, inDouble := false, false
	start := 0

	for i := 0; i < len(sqlStr); i++ {
		ch := sqlStr[i]
		if inSingle {
			if ch == '\'' {
				if i+1 < len(sqlStr) && sqlStr[i+1] == '\'' {
					i++
				} else {
					inSingle = false
				}
			}
			continue
		}
		if inDouble {
			inDouble = (ch != '"')
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '-':
			if i+1 < len(sqlStr) && sqlStr[i+1] == '-' {
				for i < len(sqlStr) && sqlStr[i] != '\n' {
					i++
				}
			}
		case ';':
			if s := strings.TrimSpace(sqlStr[start:i]); s != "" {
				stmts = append(stmts, s)
			}
			start = i + 1
		}
	}
	if s := strings.TrimSpace(sqlStr[start:]); s != "" {
		stmts = append(stmts, s)
	}
	return stmts
}

func extractIdentifierSet(sqlStr string) map[string]bool {
	idents := make(map[string]bool)
	var cur strings.Builder
	inSingle := false
	for i := 0; i < len(sqlStr); i++ {
		ch := sqlStr[i]
		if ch == '\'' {
			inSingle = !inSingle
			continue
		}
		if !inSingle && ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_') {
			cur.WriteByte(ch)
		} else if cur.Len() > 0 {
			idents[strings.ToLower(cur.String())] = true
			cur.Reset()
		}
	}
	if cur.Len() > 0 {
		idents[strings.ToLower(cur.String())] = true
	}
	return idents
}
