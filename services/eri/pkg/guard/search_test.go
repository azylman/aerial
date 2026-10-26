package guard

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

type fakeSQLCatalogConn struct {
	cols        []SQLColumn
	explainJSON string
	prepareErr  error
	tableMeta   map[uint32][2]string
	relCounts   map[string]string
	publicCols  [][3]string
	catalogErr  error
}

func (f *fakeSQLCatalogConn) PrepareAndExplain(_ context.Context, _ string) ([]SQLColumn, []byte, error) {
	if f.prepareErr != nil {
		return nil, nil, f.prepareErr
	}
	return f.cols, []byte(f.explainJSON), nil
}

func (f *fakeSQLCatalogConn) QueryCatalog(_ context.Context, sqlStr string, args ...any) ([][]string, error) {
	if f.catalogErr != nil {
		return nil, f.catalogErr
	}
	if strings.Contains(sqlStr, "WHERE c.oid = $1") && len(args) == 1 {
		if oid, ok := args[0].(uint32); ok {
			if m, found := f.tableMeta[oid]; found {
				return [][]string{{m[0], m[1]}}, nil
			}
		}
		return nil, nil
	}
	if strings.Contains(sqlStr, "WHERE n.nspname = 'public' AND c.relname = $1") && len(args) == 1 {
		if rel, ok := args[0].(string); ok {
			if cnt, found := f.relCounts[rel]; found {
				return [][]string{{cnt}}, nil
			}
		}
		return nil, nil
	}
	if strings.Contains(sqlStr, "information_schema.columns") {
		var out [][]string
		for _, r := range f.publicCols {
			out = append(out, []string{r[0], r[1], r[2]})
		}
		return out, nil
	}
	return nil, nil
}

func (f *fakeSQLCatalogConn) Close(_ context.Context) {}

func TestCheckSearch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		tool             string
		cmd              string
		cwd              string
		wantDeny         bool
		wantOverwriteSub string
	}{
		// Non-search tools / empty / prefix cases
		{"write_to_file allowed", "write_to_file", "find /", "", false, ""},
		{"empty command line allowed", "run_command", "", "", false, ""},
		{"whitespace command line allowed", "run_command", "   ", "", false, ""},
		{"default_api prefix deny", "default_api:run_command", "find . -name '*.go'", "", true, ""},
		{"empty grep_search allowed", "grep_search", "", "", false, ""},
		{"empty find_by_name allowed", "find_by_name", "", "", false, ""},
		{"escaped quote allowed", "run_command", `echo \"escaped\ quote\" && find . -maxdepth 1`, "", false, ""},
		{"repeated delimiters allowed", "run_command", `echo 1 ; ; echo 2 | | echo 3`, "", false, ""},

		// Find commands
		{"find targeting root directory denied", "run_command", "find / -maxdepth 1 -name '*.go'", "", true, ""},
		{"find targeting /proc denied", "run_command", "find /proc -maxdepth 1", "", true, ""},
		{"find targeting /sys denied", "run_command", "find /sys -maxdepth 1", "", true, ""},
		{"find dot when cwd is root denied", "run_command", "find . -maxdepth 1", "/", true, ""},
		{"find without maxdepth denied", "run_command", "find . -name '*.go'", "", true, ""},
		{"find in share without maxdepth denied", "run_command", "find /share/aerial -type f", "", true, ""},
		{"find with -maxdepth allowed", "run_command", "find . -maxdepth 2 -name '*.go'", "", false, ""},
		{"find with --maxdepth allowed", "run_command", "find /share/aerial -maxdepth 3 -type f", "", false, ""},
		{"find with -mtime flag allowed", "run_command", "find . -maxdepth 1 -mtime 1 -type f", "", false, ""},
		{"find --help allowed", "run_command", "find --help", "", false, ""},
		{"find -help allowed", "run_command", "find -help", "", false, ""},
		{"find --version allowed", "run_command", "find --version", "", false, ""},
		{"find targeting .git denied", "run_command", "find .git -maxdepth 1", "", true, ""},
		{"find targeting node_modules denied", "run_command", "find node_modules -maxdepth 1", "", true, ""},

		// Grep commands
		{"grep -r targeting root denied", "run_command", "grep -r 'pattern' /", "", true, ""},
		{"grep -r targeting /proc denied", "run_command", "grep -r 'pattern' /proc", "", true, ""},
		{"rg targeting root denied", "run_command", "rg 'pattern' /", "", true, ""},
		{"grep targeting .git denied", "run_command", "grep -r 'pattern' .git", "", true, ""},
		{"grep targeting node_modules denied", "run_command", "grep -r 'pattern' node_modules", "", true, ""},
		{"recursive grep without exclude-dir denied", "run_command", "grep -r 'pattern' .", "", true, ""},
		{"recursive grep targeting /share without exclude-dir denied", "run_command", "grep -rn 'pattern' /share", "", true, ""},
		{"recursive grep with --exclude-dir but no match limit denied", "run_command", "grep -rn --exclude-dir=.git 'pattern' .", "", true, ""},
		{"recursive grep with --exclude-dir and -m allowed", "run_command", "grep -rn -m 20 --exclude-dir=.git 'pattern' .", "", false, ""},
		{"recursive grep with space-delimited --exclude-dir and -l allowed", "run_command", "grep -rn -l --exclude-dir node_modules 'pattern' .", "", false, ""},
		{"non-recursive grep with --color allowed", "run_command", "grep --color 'pattern' .", "", false, ""},
		{"non-recursive grep with --ignore-case allowed", "run_command", "grep --ignore-case 'pattern' file.txt", "", false, ""},
		{"unbounded ripgrep targeting workspace denied", "run_command", "rg 'pattern' .", "", true, ""},
		{"ripgrep with -m targeting workspace allowed", "run_command", "rg -n -m 20 -g '*.go' 'pattern' .", "", false, ""},
		{"ripgrep with attached -nm20 allowed", "run_command", "rg -nm20 'pattern' .", "", false, ""},
		{"ripgrep with --max-count allowed", "run_command", "rg --max-count 10 'pattern' .", "", false, ""},
		{"ripgrep with -l allowed", "run_command", "rg -l 'pattern' /share/aerial", "", false, ""},
		{"ripgrep with --files allowed", "run_command", "rg --files /share/aerial", "", false, ""},
		{"ripgrep with --help allowed", "run_command", "rg --help", "", false, ""},
		{"ripgrep with --help and root path denied", "run_command", "rg --help /", "", true, ""},
		{"grep -r -h targeting root denied", "run_command", "grep -r -h 'pattern' /", "", true, ""},
		{"grep -r -h without match limit denied", "run_command", "grep -r -h --exclude-dir=.git 'pattern' .", "", true, ""},
		{"ripgrep reading from pipe without path args allowed", "run_command", "echo hello | rg hello", "", false, ""},
		{"grep targeting system binary directory denied", "run_command", "grep 'pattern' /usr/bin/bash", "", true, ""},
		{"strings targeting binary directory denied", "run_command", "strings /usr/local/bin/hook-guard", "", true, ""},
		{"grep targeting .so file denied", "run_command", "grep 'pattern' libfoo.so", "", true, ""},
		{"grep targeting .tar.gz denied", "run_command", "grep 'pattern' archive.tar.gz", "", true, ""},
		{"grep targeting .zip denied", "run_command", "grep 'pattern' file.zip", "", true, ""},
		{"grep for .zip string in source file allowed", "run_command", "grep '.zip' main.go", "", false, ""},
		{"grep in file named bin.go allowed", "run_command", "grep 'pattern' bin.go", "", false, ""},
		{"grep with explicit -e flag allowed", "run_command", "grep -e 'foo' main.go", "", false, ""},
		{"bash safe command in search", "run_command", "bash -c 'echo safe'", "", false, ""},
		{"grep short flag with digit", "run_command", "grep -m 10 foo file.txt", "", false, ""},
		{"unclosed sql single quote", "run_command", `psql -A -c "SELECT 'unclosed"`, "", false, ""},

		// Pipelines & Quotes
		{"echo containing find in quotes allowed", "run_command", `echo "find / -name foo"`, "", false, ""},
		{"chained safe commands allowed", "run_command", `ls -la && find . -maxdepth 1 -name '*.go'`, "", false, ""},
		{"pipeline with unconstrained find denied", "run_command", `echo starting && find . -name '*.go'`, "", true, ""},
		{"pipe inside quotes allowed", "run_command", `grep "a | b" file.txt`, "", false, ""},
		{"semicolon inside quotes allowed", "run_command", `git commit -m "fix; update search guard"`, "", false, ""},
		{"or operator pipeline with bad find denied", "run_command", `test -d /foo || find / -maxdepth 1`, "", true, ""},
		{"docker exec find without maxdepth denied", "run_command", `docker exec cc72895f4852 find /app -name "*live-session*"`, "", true, ""},
		{"docker exec find with maxdepth allowed", "run_command", `docker exec -i -u root -w /app cc72895f4852 find /app -maxdepth 2 -name "*.js"`, "", false, ""},
		{"docker exec recursive grep without limit denied", "run_command", `docker exec cc72895f4852 grep -rn "setupCanvas" /app`, "", true, ""},
		{"docker exec recursive grep with limit allowed", "run_command", `docker exec -e FOO=bar cc72895f4852 grep -rn -m 20 --exclude-dir=.git "setupCanvas" /app`, "", false, ""},
		{"docker exec sh -c unconstrained find denied", "run_command", `docker exec cc72895f4852 sh -c "find /app -name '*.js'"`, "", true, ""},
		{"docker exec without inner command allowed", "run_command", `docker exec -it cc72895f4852`, "", false, ""},

		// MCP tool_index & openobserve shaping (cmd = ToolName, cwd = Arguments JSON)
		{"mcp non-index tool allowed", "call_mcp_tool", "list_devices", `{}`, false, ""},
		{"mcp tool_index empty args denied", "call_mcp_tool", "unifi_tool_index", ``, true, ""},
		{"mcp tool_index empty object denied", "call_mcp_tool", "protect_tool_index", `{}`, true, ""},
		{"mcp tool_index search filter allowed", "call_mcp_tool", "unifi_tool_index", `{"search":"firewall"}`, false, ""},
		{"mcp tool_index string-encoded category filter allowed", "default_api:call_mcp_tool", `"protect_tool_index"`, `"{\"category\":\"cameras\"}"`, false, ""},
		{"mcp tool_index include_schemas without name denied", "call_mcp_tool", "unifi_tool_index", `{"search":"firewall","include_schemas":true}`, true, ""},
		{"mcp tool_index include_schemas with exact name allowed", "call_mcp_tool", "unifi_tool_index", `{"name":"unifi_list_firewall_policies","include_schemas":true}`, false, ""},
		{"mcp openobserve StreamSchema denied", "call_mcp_tool", "StreamSchema", `{"org_id":"default","stream_name":"docker_logs"}`, true, ""},
		{"mcp openobserve SearchSQL without request_body allowed", "call_mcp_tool", "SearchSQL", ``, false, ""},
		{"mcp openobserve SearchSQL select star denied", "call_mcp_tool", "SearchSQL", `{"org_id":"default","request_body":{"query":{"sql":"SELECT * FROM docker_logs LIMIT 10"}}}`, true, ""},
		{"mcp openobserve SearchSQL table star denied", "call_mcp_tool", "SearchSQL", `"{\"org_id\":\"default\",\"request_body\":{\"query\":{\"sql\":\"SELECT t.* FROM docker_logs t LIMIT 10\"}}}"`, true, ""},
		{"mcp openobserve SearchSQL excessive SQL LIMIT denied", "call_mcp_tool", "SearchSQL", `{"org_id":"default","request_body":{"query":{"sql":"SELECT _timestamp, message FROM docker_logs LIMIT 100"}}}`, true, ""},
		{"mcp openobserve SearchSQL semicolon-terminated excessive LIMIT denied", "call_mcp_tool", "SearchSQL", `{"org_id":"default","request_body":{"query":{"sql":"SELECT _timestamp, message FROM docker_logs LIMIT 100;"}}}`, true, ""},
		{"mcp openobserve SearchSQL excessive query size denied", "call_mcp_tool", "SearchSQL", `{"org_id":"default","request_body":{"query":{"size":100,"sql":"SELECT _timestamp, message FROM docker_logs LIMIT 10"}}}`, true, ""},
		{"mcp openobserve SearchSQL explicit columns defaults query size", "call_mcp_tool", "SearchSQL", `{"org_id":"default","request_body":{"query":{"sql":"SELECT _timestamp, service, COUNT(*) FROM docker_logs WHERE message LIKE '%SELECT *%''s%' -- * comment\n GROUP BY _timestamp, service LIMIT 50"}}}`, false, `"size":50`},
		{"mcp openobserve SearchSQL csv output_format rewritten to json", "call_mcp_tool", "SearchSQL", `{"org_id":"default","request_body":{"agent_options":{"output_format":"csv"},"query":{"size":20,"sql":"SELECT _timestamp, message FROM docker_logs LIMIT 10"}}}`, false, `"output_format":"json"`},
		{"mcp openobserve SearchSQL bounded size and json output_format allowed", "call_mcp_tool", "SearchSQL", `{"org_id":"default","request_body":{"agent_options":{"output_format":"json"},"query":{"size":20,"sql":"SELECT _timestamp, message FROM docker_logs LIMIT 10"}}}`, false, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tool := tc.tool
			if tool == "" {
				tool = "run_command"
			}
			args := ToolArgs{CommandLine: tc.cmd, Cwd: tc.cwd}
			if normalizeToolName(tool) == "call_mcp_tool" {
				args = ToolArgs{ToolName: tc.cmd, Arguments: json.RawMessage(tc.cwd)}
			}
			d := CheckSearchWithSQL(tool, args, nil)
			if tc.wantDeny && d.Decision != DecisionDeny {
				t.Fatalf("cmd %q: expected deny, got %v", tc.cmd, d)
			}
			if !tc.wantDeny && d.Decision != DecisionAllow {
				t.Fatalf("cmd %q: expected allow, got deny: %v", tc.cmd, d)
			}
			if tc.wantOverwriteSub == "" {
				if d.Overwrite != nil {
					t.Fatalf("cmd %q: unexpected Overwrite: %v", tc.cmd, d.Overwrite)
				}
				return
			}
			rawOverwrite, err := json.Marshal(d.Overwrite["Arguments"])
			if err != nil || !strings.Contains(string(rawOverwrite), tc.wantOverwriteSub) {
				t.Fatalf("cmd %q: Overwrite[Arguments] = %s (err=%v), want substring %q", tc.cmd, rawOverwrite, err, tc.wantOverwriteSub)
			}
		})
	}
}

func TestPsqlGuard(t *testing.T) {
	twoCols := []SQLColumn{{Name: "id", TableOID: 100, AttrNum: 1}, {Name: "author_name", TableOID: 100, AttrNum: 2}}
	threeCols := []SQLColumn{{Name: "id", TableOID: 100, AttrNum: 1}, {Name: "author_name", TableOID: 100, AttrNum: 2}, {Name: "content", TableOID: 100, AttrNum: 3}}
	cteCols := []SQLColumn{{Name: "id", TableOID: 0, AttrNum: 0}, {Name: "author_name", TableOID: 0, AttrNum: 0}, {Name: "content", TableOID: 0, AttrNum: 0}}
	embedCols := []SQLColumn{{Name: "id", TableOID: 200, AttrNum: 1}, {Name: "embedding", TableOID: 200, AttrNum: 2}}

	meta3 := map[uint32][2]string{100: {"messages", "3"}, 200: {"facts", "7"}}
	rel3 := map[string]string{"messages": "3"}
	schemaRows := [][3]string{{"messages", "id", "text"}, {"messages", "author_name", "text"}, {"messages", "content", "text"}}

	limitPlan := `[{"Plan": {"Node Type": "Limit", "Plan Rows": 10, "Plans": [{"Node Type": "Seq Scan", "Relation Name": "messages", "Plan Rows": 100}]}}]`
	overLimitPlan := `[{"Plan": {"Node Type": "Limit", "Plan Rows": 100, "Plans": [{"Node Type": "Seq Scan", "Relation Name": "messages", "Plan Rows": 200}]}}]`
	unboundedPlan := `[{"Plan": {"Node Type": "Sort", "Plan Rows": 103, "Plans": [{"Node Type": "Seq Scan", "Relation Name": "messages", "Plan Rows": 103}]}}]`
	pkPlan := `[{"Plan": {"Node Type": "Index Scan", "Relation Name": "messages", "Plan Rows": 1}}]`
	dmlPlan := `[{"Plan": {"Node Type": "ModifyTable", "Relation Name": "messages", "Plan Rows": 1}}]`

	tests := []struct {
		name          string
		cmd           string
		conn          *fakeSQLCatalogConn
		connErr       error
		wantDeny      bool
		wantReasonSub string
		wantOverwrite string
	}{
		{
			name:          "scoped select injects -A",
			cmd:           `psql "$DATABASE_URL" -c "SELECT id, author_name FROM messages LIMIT 10;"`,
			conn:          &fakeSQLCatalogConn{cols: twoCols, explainJSON: limitPlan, tableMeta: meta3, relCounts: rel3},
			wantOverwrite: `psql -A "$DATABASE_URL" -c "SELECT id, author_name FROM messages LIMIT 10;"`,
		},
		{
			name:     "existing -Atc preserves command without overwrite",
			cmd:      `psql "$DATABASE_URL" -Atc "SELECT id, author_name FROM messages LIMIT 10;"`,
			conn:     &fakeSQLCatalogConn{cols: twoCols, explainJSON: limitPlan, tableMeta: meta3},
			wantDeny: false,
		},
		{
			name:     "existing --csv preserves command without overwrite",
			cmd:      `psql "$DATABASE_URL" --csv --command="SELECT id, author_name FROM messages WHERE id = '1'"`,
			conn:     &fakeSQLCatalogConn{cols: twoCols, explainJSON: pkPlan, tableMeta: meta3},
			wantDeny: false,
		},
		{
			name:     "existing -P format=unaligned and -Pformat=csv",
			cmd:      `psql -P format=unaligned -c "\d messages" && psql -Pformat=csv -U aerial -d aerial -c "SELECT '--not-comment'; SELECT id, author_name FROM messages WHERE id = '1'"`,
			conn:     &fakeSQLCatalogConn{cols: twoCols, explainJSON: pkPlan, tableMeta: meta3},
			wantDeny: false,
		},
		{
			name:          "leading env var and attached -c injects -A",
			cmd:           `PGPASSWORD=secret /usr/bin/psql -U Admin -d Aerial -c"SELECT id, embedding FROM facts WHERE id = 1"`,
			conn:          &fakeSQLCatalogConn{cols: embedCols, explainJSON: pkPlan, tableMeta: meta3},
			wantOverwrite: `PGPASSWORD=secret /usr/bin/psql -A -U Admin -d Aerial -c"SELECT id, embedding FROM facts WHERE id = 1"`,
		},
		{
			name:          "select star detected via TableOID column count",
			cmd:           `psql "$DATABASE_URL" -c "SELECT * FROM messages LIMIT 5"`,
			conn:          &fakeSQLCatalogConn{cols: threeCols, explainJSON: limitPlan, tableMeta: meta3, relCounts: rel3},
			wantDeny:      true,
			wantReasonSub: "Available columns: id, author_name, content",
		},
		{
			name:          "select star detected via CTE relation column count",
			cmd:           `psql "$DATABASE_URL" -c "WITH c AS (SELECT * FROM messages LIMIT 5) SELECT * FROM c"`,
			conn:          &fakeSQLCatalogConn{cols: cteCols, explainJSON: limitPlan, tableMeta: meta3, relCounts: rel3},
			wantDeny:      true,
			wantReasonSub: "Wildcard SELECT * is prohibited",
		},
		{
			name:          "unbounded multi-row select without LIMIT denied",
			cmd:           `psql "$DATABASE_URL" -c "SELECT id, author_name FROM messages ORDER BY id"`,
			conn:          &fakeSQLCatalogConn{cols: twoCols, explainJSON: unboundedPlan, tableMeta: meta3},
			wantDeny:      true,
			wantReasonSub: "Unbounded multi-row psql SELECT detected (estimated rows: 103)",
		},
		{
			name:          "select with excessive LIMIT denied",
			cmd:           `psql "$DATABASE_URL" -c "SELECT id, author_name FROM messages LIMIT 100"`,
			conn:          &fakeSQLCatalogConn{cols: twoCols, explainJSON: overLimitPlan, tableMeta: meta3},
			wantDeny:      true,
			wantReasonSub: "psql LIMIT exceeds maximum allowed rows",
		},
		{
			name:          "DML RETURNING all columns allowed and injects -A",
			cmd:           `psql "$DATABASE_URL" -c "UPDATE messages SET content = 'a''b' WHERE id = '1' RETURNING *"`,
			conn:          &fakeSQLCatalogConn{cols: threeCols, explainJSON: dmlPlan, tableMeta: meta3},
			wantOverwrite: `psql -A "$DATABASE_URL" -c "UPDATE messages SET content = 'a''b' WHERE id = '1' RETURNING *"`,
		},
		{
			name:          "undefined column 42703 returns schema hint for referenced table",
			cmd:           `psql "$DATABASE_URL" -c "SELECT author_username FROM \"messages\" -- comment\n WHERE id = '1'"`,
			conn:          &fakeSQLCatalogConn{prepareErr: &pgconn.PgError{Code: "42703", Message: `column "author_username" does not exist`}, publicCols: schemaRows},
			wantDeny:      true,
			wantReasonSub: "messages: [id (text), author_name (text), content (text)]",
		},
		{
			name:          "undefined table 42P01 returns generic schema hint",
			cmd:           `psql "$DATABASE_URL" -c "SELECT id FROM unknown_tbl"`,
			conn:          &fakeSQLCatalogConn{prepareErr: &pgconn.PgError{Code: "42P01", Message: `relation "unknown_tbl" does not exist`}, publicCols: schemaRows},
			wantDeny:      true,
			wantReasonSub: "Verify table and column names against schema.sql",
		},
		{
			name:          "undefined column with catalog query error still returns pgErr",
			cmd:           `psql "$DATABASE_URL" -c "SELECT bad_col FROM messages"`,
			conn:          &fakeSQLCatalogConn{prepareErr: &pgconn.PgError{Code: "42703", Message: `column "bad_col" does not exist`}, catalogErr: errors.New("catalog fail")},
			wantDeny:      true,
			wantReasonSub: `42703: column "bad_col" does not exist`,
		},
		{
			name:          "syntax error 42601 on psql variable fails open and injects -A",
			cmd:           `psql "$DATABASE_URL" -c "SELECT :var FROM messages"`,
			conn:          &fakeSQLCatalogConn{prepareErr: &pgconn.PgError{Code: "42601", Message: "syntax error"}},
			wantOverwrite: `psql -A "$DATABASE_URL" -c "SELECT :var FROM messages"`,
		},
		{
			name:          "connection failure fails open and injects -A",
			cmd:           `psql "$DATABASE_URL" -c "SELECT * FROM messages"`,
			connErr:       errors.New("dial error"),
			wantOverwrite: `psql -A "$DATABASE_URL" -c "SELECT * FROM messages"`,
		},
		{
			name:          "backslash command in multi-statement skipped and injects -A",
			cmd:           `psql "$DATABASE_URL" --command "\dt" -c "\d; SELECT id, author_name FROM messages LIMIT 5"`,
			conn:          &fakeSQLCatalogConn{cols: twoCols, explainJSON: limitPlan, tableMeta: meta3},
			wantOverwrite: `psql -A "$DATABASE_URL" --command "\dt" -c "\d; SELECT id, author_name FROM messages LIMIT 5"`,
		},
		{
			name:          "docker exec psql injects -A",
			cmd:           `docker exec -i -e PGPASSWORD=secret postgres psql -U aerial -d aerial -c "SELECT id, author_name FROM messages LIMIT 5"`,
			conn:          &fakeSQLCatalogConn{cols: twoCols, explainJSON: limitPlan, tableMeta: meta3},
			wantOverwrite: `docker exec -i -e PGPASSWORD=secret postgres psql -A -U aerial -d aerial -c "SELECT id, author_name FROM messages LIMIT 5"`,
		},
		{
			name:          "docker exec psql select star denied",
			cmd:           `docker exec postgres psql -U aerial -c "SELECT * FROM messages LIMIT 5"`,
			conn:          &fakeSQLCatalogConn{cols: threeCols, explainJSON: limitPlan, tableMeta: meta3, relCounts: rel3},
			wantDeny:      true,
			wantReasonSub: "Available columns: id, author_name, content",
		},
		{
			name:     "docker exec without inner command ignored by psql guard",
			cmd:      `docker exec -e FOO=bar postgres`,
			conn:     &fakeSQLCatalogConn{cols: twoCols, explainJSON: limitPlan, tableMeta: meta3},
			wantDeny: false,
		},
		{
			name:          "psql empty query allowed",
			cmd:           `psql "$DATABASE_URL" -c ""`,
			wantDeny:      false,
			wantOverwrite: `psql -A "$DATABASE_URL" -c ""`,
		},
		{
			name:          "psql backslash stmt skipped",
			cmd:           `psql "$DATABASE_URL" -c "SELECT id, author_name FROM messages LIMIT 5; \timing"`,
			conn:          &fakeSQLCatalogConn{cols: twoCols, explainJSON: limitPlan, tableMeta: meta3},
			wantOverwrite: `psql -A "$DATABASE_URL" -c "SELECT id, author_name FROM messages LIMIT 5; \timing"`,
		},
		{
			name:          "schema hint connect failure returns empty",
			cmd:           `psql "$DATABASE_URL" -c "SELECT bad_col FROM messages"`,
			connErr:       errors.New("dial fail"),
			conn:          &fakeSQLCatalogConn{prepareErr: &pgconn.PgError{Code: "42703", Message: `column "bad_col" does not exist`}},
			wantDeny:      false,
			wantOverwrite: `psql -A "$DATABASE_URL" -c "SELECT bad_col FROM messages"`,
		},
		{
			name:          "psql empty explain plan ignored",
			cmd:           `psql "$DATABASE_URL" -c "SELECT id FROM messages"`,
			conn:          &fakeSQLCatalogConn{cols: twoCols, explainJSON: ""},
			wantOverwrite: `psql -A "$DATABASE_URL" -c "SELECT id FROM messages"`,
		},
	}

	t.Setenv("DATABASE_URL", "postgres://127.0.0.1:1/aerial?connect_timeout=1")
	if d := CheckSearch("run_command", ToolArgs{CommandLine: `psql -c "SELECT id FROM messages LIMIT 1"`}); d.Decision != DecisionAllow {
		t.Fatalf("expected allow on unreachable default describer, got %+v", d)
	}
	t.Setenv("DATABASE_URL", ":memory:")
	if DefaultSQLDescriber() != nil {
		t.Fatalf("expected nil describer for :memory:")
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			desc := &pgxDescriber{
				connect: func(_ context.Context) (sqlCatalogConn, error) {
					if tc.connErr != nil {
						return nil, tc.connErr
					}
					return tc.conn, nil
				},
			}
			d := CheckSearchWithSQL("run_command", ToolArgs{CommandLine: tc.cmd}, desc)
			if tc.wantDeny && d.Decision != DecisionDeny {
				t.Fatalf("expected deny, got %+v", d)
			}
			if !tc.wantDeny && d.Decision != DecisionAllow {
				t.Fatalf("expected allow, got %+v", d)
			}
			if tc.wantReasonSub != "" && !strings.Contains(d.Reason, tc.wantReasonSub) {
				t.Fatalf("reason %q missing %q", d.Reason, tc.wantReasonSub)
			}
			gotOverwrite, _ := d.Overwrite["CommandLine"].(string)
			if gotOverwrite != tc.wantOverwrite {
				t.Fatalf("Overwrite[CommandLine] = %q, want %q", gotOverwrite, tc.wantOverwrite)
			}
		})
	}
}

func TestSearchParsers(t *testing.T) {
	t.Parallel()

	pipelineTests := []struct {
		name  string
		input string
		want  []string
	}{
		{"empty", "", nil},
		{"simple single", "echo 1", []string{"echo 1"}},
		{"semicolon", "echo 1; echo 2", []string{"echo 1", "echo 2"}},
		{"and operator", "echo 1 && echo 2", []string{"echo 1", "echo 2"}},
		{"or operator", "echo 1 || echo 2", []string{"echo 1", "echo 2"}},
		{"pipe operator", "echo 1 | grep 1", []string{"echo 1", "grep 1"}},
		{"quoted pipe", `echo "1 | 2"`, []string{`echo "1 | 2"`}},
		{"escaped quotes", `echo \"hello\"`, []string{`echo \"hello\"`}},
	}
	for _, tc := range pipelineTests {
		t.Run("pipeline_"+tc.name, func(t *testing.T) {
			t.Parallel()
			got := splitPipeline(tc.input)
			if len(got) != len(tc.want) {
				t.Fatalf("splitPipeline(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}

	tokenTests := []struct {
		name  string
		input string
		want  int
	}{
		{"empty", "", 0},
		{"simple tokens", "find . -maxdepth 1", 4},
		{"single quotes", "grep 'a b c' file", 3},
		{"double quotes", `grep "hello world" file`, 3},
		{"escaped space", `grep hello\ world file`, 3},
	}
	for _, tc := range tokenTests {
		t.Run("tokens_"+tc.name, func(t *testing.T) {
			t.Parallel()
			got := tokenizeQuoteAware(tc.input)
			if len(got) != tc.want {
				t.Fatalf("tokenizeQuoteAware(%q) length = %d, want %d", tc.input, len(got), tc.want)
			}
		})
	}

	argTests := []struct {
		name     string
		rawJSON  string
		wantFile string
		wantCmd  string
	}{
		{"object format", `{"TargetFile": "/share/aerial", "CommandLine": "find ."}`, "/share/aerial", "find ."},
		{"string format", `"{\"CommandLine\": \"grep foo\"}"`, "", "grep foo"},
	}
	for _, tc := range argTests {
		t.Run("args_"+tc.name, func(t *testing.T) {
			t.Parallel()
			args := ParseArgs([]byte(tc.rawJSON))
			if args.FilePath() != tc.wantFile || args.CommandLine != tc.wantCmd {
				t.Fatalf("ParseArgs(%q) = %+v, want file=%q cmd=%q", tc.rawJSON, args, tc.wantFile, tc.wantCmd)
			}
		})
	}
}
