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

type searchTestCase struct {
	name             string
	tool             string
	cmd              string
	cwd              string
	wantDeny         bool
	wantOverwriteSub string
	server           string
}

func checkSearchBaseCases() []searchTestCase {
	return []searchTestCase{
		// Non-search tools / empty / prefix cases
		{"write_to_file allowed", "write_to_file", "find /", "", false, "", ""},
		{"empty command line allowed", "run_command", "", "", false, "", ""},
		{"whitespace command line allowed", "run_command", "   ", "", false, "", ""},
		{"default_api prefix deny", "default_api:run_command", "find . -name '*.go'", "", true, "", ""},
		{"empty grep_search allowed", "grep_search", "", "", false, "", ""},
		{"empty find_by_name allowed", "find_by_name", "", "", false, "", ""},
		{"escaped quote allowed", "run_command", `echo \"escaped\ quote\" && find . -maxdepth 1`, "", false, "", ""},
		{"repeated delimiters allowed", "run_command", `echo 1 ; ; echo 2 | | echo 3`, "", false, "", ""},

		// Find commands
		{"find targeting root directory denied", "run_command", "find / -maxdepth 1 -name '*.go'", "", true, "", ""},
		{"find targeting /proc denied", "run_command", "find /proc -maxdepth 1", "", true, "", ""},
		{"find targeting /sys denied", "run_command", "find /sys -maxdepth 1", "", true, "", ""},
		{"find dot when cwd is root denied", "run_command", "find . -maxdepth 1", "/", true, "", ""},
		{"find without maxdepth denied", "run_command", "find . -name '*.go'", "", true, "", ""},
		{"find in share without maxdepth denied", "run_command", "find /share/aerial -type f", "", true, "", ""},
		{"find with -maxdepth allowed", "run_command", "find . -maxdepth 2 -name '*.go'", "", false, "", ""},
		{"find with --maxdepth allowed", "run_command", "find /share/aerial -maxdepth 3 -type f", "", false, "", ""},
		{"find with -mtime flag allowed", "run_command", "find . -maxdepth 1 -mtime 1 -type f", "", false, "", ""},
		{"find --help allowed", "run_command", "find --help", "", false, "", ""},
		{"find -help allowed", "run_command", "find -help", "", false, "", ""},
		{"find --version allowed", "run_command", "find --version", "", false, "", ""},
		{"find targeting .git denied", "run_command", "find .git -maxdepth 1", "", true, "", ""},
		{"find targeting node_modules denied", "run_command", "find node_modules -maxdepth 1", "", true, "", ""},
		{"find targeting /data/runtimes denied", "run_command", "find /data/runtimes -maxdepth 2 -name '*.jsonl'", "", true, "", ""},
		{"find targeting /data/runtimes subdir denied", "run_command", "find /data/runtimes/discord -maxdepth 2", "", true, "", ""},

		// Grep commands
		{"grep -r targeting root denied", "run_command", "grep -r 'pattern' /", "", true, "", ""},
		{"grep -rn targeting /data/runtimes denied", "run_command", "grep -rn -m 10 'RFC1918' /data/runtimes/", "", true, "", ""},
		{"rg targeting /data/runtimes/discord denied", "run_command", "rg -m 10 'pattern' /data/runtimes/discord", "", true, "", ""},
		{"grep -r targeting /proc denied", "run_command", "grep -r 'pattern' /proc", "", true, "", ""},
		{"rg targeting root denied", "run_command", "rg 'pattern' /", "", true, "", ""},
		{"grep targeting .git denied", "run_command", "grep -r 'pattern' .git", "", true, "", ""},
		{"grep targeting node_modules denied", "run_command", "grep -r 'pattern' node_modules", "", true, "", ""},
		{"recursive grep without exclude-dir denied", "run_command", "grep -r 'pattern' .", "", true, "", ""},
		{"recursive grep targeting /share without exclude-dir denied", "run_command", "grep -rn 'pattern' /share", "", true, "", ""},
		{"recursive grep with --exclude-dir on broad root denied", "run_command", "grep -rn --exclude-dir=.git 'pattern' .", "", true, "", ""},
		{"recursive grep with --exclude-dir and -m on broad root denied", "run_command", "grep -rn -m 20 --exclude-dir=.git 'pattern' .", "", true, "", ""},
		{"recursive grep with --exclude-dir and -m on scoped package allowed", "run_command", "grep -rn -m 20 --exclude-dir=.git 'pattern' services/eri", "", false, "", ""},
		{"recursive grep with space-delimited --exclude-dir and -l allowed", "run_command", "grep -rn -l --exclude-dir node_modules 'pattern' .", "", false, "", ""},
		{"non-recursive grep without -m allowed", "run_command", "grep --color 'pattern' file.txt", "", false, "", ""},
		{"non-recursive grep with -m and --color allowed", "run_command", "grep -m 10 --color 'pattern' file.txt", "", false, "", ""},
		{"non-recursive grep with -m and --ignore-case allowed", "run_command", "grep -m 10 --ignore-case 'pattern' file.txt", "", false, "", ""},
		{"unbounded ripgrep targeting workspace denied", "run_command", "rg 'pattern' .", "", true, "", ""},
		{"ripgrep with -m targeting broad root without -l/-c denied", "run_command", "rg -n -m 20 -g '*.go' 'pattern' .", "", true, "", ""},
		{"ripgrep with -m targeting multiple directories without -l/-c denied", "run_command", "rg -n -m 20 'pattern' /share/aerial/services/vanadis /share/aerial/services/synapse", "", true, "", ""},
		{"ripgrep with -l targeting multiple directories allowed", "run_command", "rg -l 'pattern' /share/aerial/services/vanadis /share/aerial/services/synapse", "", false, "", ""},
		{"ripgrep with -m targeting scoped package allowed", "run_command", "rg -n -m 20 -g '*.go' 'pattern' services/eri", "", false, "", ""},
		{"ripgrep without -m targeting scoped package allowed", "run_command", "rg -n 'pattern' services/eri", "", false, "", ""},
		{"ripgrep with -m 50 on scoped package allowed", "run_command", "rg -n -m 50 'pattern' services/eri", "", false, "", ""},
		{"ripgrep with --max-count=50 on scoped package allowed", "run_command", "rg -n --max-count=50 'pattern' services/eri", "", false, "", ""},
		{"ripgrep with excessive -C 15 on recursive search denied", "run_command", "rg -n -m 20 -C 15 'pattern' services/eri", "", true, "", ""},
		{"ripgrep with attached -C10 on recursive search denied", "run_command", "rg -n -m 20 -C10 'pattern' services/eri", "", true, "", ""},
		{"ripgrep with --context=10 on recursive search denied", "run_command", "rg -n -m 20 --context=10 'pattern' services/eri", "", true, "", ""},
		{"ripgrep with -C 2 on scoped package allowed", "run_command", "rg -n -m 20 -C 2 'pattern' services/eri", "", false, "", ""},
		{"unbounded git grep denied", "run_command", "git grep 'pattern'", "", true, "", ""},
		{"unbounded git -C dir --no-pager grep denied", "run_command", "git -C /share/aerial --no-pager grep -n 'pattern' services/", "", true, "", ""},
		{"git grep with -m but no path token denied", "run_command", "git grep -n -m 20 'pattern'", "", true, "", ""},
		{"bounded git grep with -m on scoped path allowed", "run_command", "git grep -n -m 20 'pattern' services/eri/", "", false, "", ""},
		{"bounded git -C dir grep with -l allowed", "run_command", "git -C /share/aerial -c color.ui=false grep -l 'pattern'", "", false, "", ""},
		{"git log -p without -n denied", "run_command", "git -C /share/aerial log -p -S 'swiftshader'", "", true, "", ""},
		{"git log --patch with excessive -n 10 denied", "run_command", "git log -n 10 --patch", "", true, "", ""},
		{"git log -p with -n 3 allowed", "run_command", "git log -n 3 -p -- services/eri/", "", false, "", ""},
		{"git log -p with attached -n2 allowed", "run_command", "git log -n2 -p", "", false, "", ""},
		{"git log -p with --max-count=4 allowed", "run_command", "git log --max-count=4 -p", "", false, "", ""},
		{"git log -p with -3 shorthand allowed", "run_command", "git log -3 -p", "", false, "", ""},
		{"git show commit without -s/--stat or path denied", "run_command", "git show 5c89fb1", "", true, "", ""},
		{"git show commit with -s allowed", "run_command", "git -C /share/mirrormere show -s 5c89fb1", "", false, "", ""},
		{"git show commit with --stat allowed", "run_command", "git show --stat 5c89fb1", "", false, "", ""},
		{"git show commit with --format allowed", "run_command", "git show --format=%B -s 5c89fb1", "", false, "", ""},
		{"git show rev:file blob allowed", "run_command", "git show HEAD:services/eri/main.go", "", false, "", ""},
		{"git show commit with -- path allowed", "run_command", "git show 5c89fb1 -- services/eri/main.go", "", false, "", ""},
		{"git status subcommand allowed", "run_command", "git -C /share/aerial status", "", false, "", ""},
		{"bare git command allowed", "run_command", "git --version", "", false, "", ""},
		{"ripgrep with attached -nm20 on scoped path allowed", "run_command", "rg -nm20 'pattern' services/eri", "", false, "", ""},
		{"ripgrep with --max-count on scoped path allowed", "run_command", "rg --max-count 10 'pattern' services/eri", "", false, "", ""},
		{"ripgrep with -l allowed", "run_command", "rg -l 'pattern' /share/aerial", "", false, "", ""},
		{"ripgrep with --files allowed", "run_command", "rg --files /share/aerial", "", false, "", ""},
		{"ripgrep with --help allowed", "run_command", "rg --help", "", false, "", ""},
		{"ripgrep with --help and root path denied", "run_command", "rg --help /", "", true, "", ""},
		{"grep -r -h targeting root denied", "run_command", "grep -r -h 'pattern' /", "", true, "", ""},
		{"grep -r -h without match limit denied", "run_command", "grep -r -h --exclude-dir=.git 'pattern' .", "", true, "", ""},
		{"ripgrep reading from pipe without path args allowed", "run_command", "echo hello | rg hello", "", false, "", ""},
		{"grep targeting system binary directory denied", "run_command", "grep 'pattern' /usr/bin/bash", "", true, "", ""},
		{"strings targeting binary directory denied", "run_command", "strings /usr/local/bin/hook-guard", "", true, "", ""},
		{"strings targeting safe file allowed", "run_command", "strings /tmp/sample.txt", "", false, "", ""},
		{"grep targeting .so file denied", "run_command", "grep 'pattern' libfoo.so", "", true, "", ""},
		{"grep targeting .tar.gz denied", "run_command", "grep 'pattern' archive.tar.gz", "", true, "", ""},
		{"grep targeting .zip denied", "run_command", "grep 'pattern' file.zip", "", true, "", ""},
		{"grep for .zip string in source file allowed", "run_command", "grep -m 10 '.zip' main.go", "", false, "", ""},
		{"grep in file named bin.go allowed", "run_command", "grep -m 10 'pattern' bin.go", "", false, "", ""},
		{"grep with explicit -e flag allowed", "run_command", "grep -m 10 -e 'foo' main.go", "", false, "", ""},
		{"bash safe command in search", "run_command", "bash -c 'echo safe'", "", false, "", ""},
		{"grep short flag with digit", "run_command", "grep -m 10 foo file.txt", "", false, "", ""},
		{"unclosed sql single quote", "run_command", `psql -A -c "SELECT 'unclosed"`, "", false, "", ""},
	}
}

func checkSearchExtendedCases() []searchTestCase {
	return []searchTestCase{
		// Pipelines & Quotes
		{"echo containing find in quotes allowed", "run_command", `echo "find / -name foo"`, "", false, "", ""},
		{"chained safe commands allowed", "run_command", `ls -la && find . -maxdepth 1 -name '*.go'`, "", false, "", ""},
		{"pipeline with unconstrained find denied", "run_command", `echo starting && find . -name '*.go'`, "", true, "", ""},
		{"pipe inside quotes allowed", "run_command", `grep -m 10 "a | b" file.txt`, "", false, "", ""},
		{"semicolon inside quotes allowed", "run_command", `git commit -m "fix; update search guard"`, "", false, "", ""},
		{"or operator pipeline with bad find denied", "run_command", `test -d /foo || find / -maxdepth 1`, "", true, "", ""},
		{"docker exec find without maxdepth denied", "run_command", `docker exec cc72895f4852 find /app -name "*live-session*"`, "", true, "", ""},
		{"docker exec find with maxdepth allowed", "run_command", `docker exec -i -u root -w /app cc72895f4852 find /app -maxdepth 2 -name "*.js"`, "", false, "", ""},
		{"docker exec recursive grep without limit denied", "run_command", `docker exec cc72895f4852 grep -rn "setupCanvas" /app`, "", true, "", ""},
		{"docker exec recursive grep with limit allowed", "run_command", `docker exec -e FOO=bar cc72895f4852 grep -rn -m 20 --exclude-dir=.git "setupCanvas" /app`, "", false, "", ""},
		{"docker exec sh -c unconstrained find denied", "run_command", `docker exec cc72895f4852 sh -c "find /app -name '*.js'"`, "", true, "", ""},
		{"docker exec without inner command allowed", "run_command", `docker exec -it cc72895f4852`, "", false, "", ""},

		// MCP tool shaping (cmd = ToolName, cwd = Arguments JSON, server = ServerName)
		{"mcp non-index tool allowed", "call_mcp_tool", "list_devices", `{}`, false, "", ""},
		{"mcp tool_index empty args denied", "call_mcp_tool", "unifi_tool_index", ``, true, "", ""},
		{"mcp tool_index empty object denied", "call_mcp_tool", "protect_tool_index", `{}`, true, "", ""},
		{"mcp tool_index search filter allowed", "call_mcp_tool", "unifi_tool_index", `{"search":"firewall"}`, false, "", ""},
		{"mcp tool_index string-encoded category filter allowed", "default_api:call_mcp_tool", `"protect_tool_index"`, `"{\"category\":\"cameras\"}"`, false, "", ""},
		{"mcp tool_index include_schemas without name denied", "call_mcp_tool", "unifi_tool_index", `{"search":"firewall","include_schemas":true}`, true, "", ""},
		{"mcp tool_index include_schemas with exact name allowed", "call_mcp_tool", "unifi_tool_index", `{"name":"unifi_list_firewall_policies","include_schemas":true}`, false, "", ""},
		{"mcp discord_search_messages default limit allowed", "call_mcp_tool", "discord_search_messages", `{"guildId":"123","content":"hello"}`, false, "", ""},
		{"mcp discord_search_messages limit 25 allowed", "call_mcp_tool", "discord_search_messages", `{"guildId":"123","limit":25}`, false, "", ""},
		{"mcp discord_search_messages limit 50 denied", "call_mcp_tool", "discord_search_messages", `{"guildId":"123","limit":50}`, true, "", ""},
		{"mcp github get_file_contents empty args allowed", "call_mcp_tool", "get_file_contents", ``, false, "", ""},
		{"mcp github get_file_contents local aerial default branch denied", "call_mcp_tool", "get_file_contents", `{"owner":"azylman","repo":"aerial","path":"services/eri/main.go"}`, true, "", ""},
		{"mcp github get_file_contents local aerial-config main ref denied", "call_mcp_tool", "get_file_contents", `{"owner":"azylman","repo":"aerial-config","path":"config.yaml","ref":"main"}`, true, "", ""},
		{"mcp github get_file_contents mirrormere default branch allowed and injects fields", "call_mcp_tool", "get_file_contents", `{"owner":"azylman","repo":"mirrormere","path":"web/index.html"}`, false, `"fields":["name","type","path","size"]`, ""},
		{"mcp github get_file_contents aerial feature branch allowed and injects fields", "call_mcp_tool", "get_file_contents", `{"owner":"azylman","repo":"aerial","path":"services/eri","ref":"feat/branch"}`, false, `"fields":["name","type","path","size"]`, ""},
		{"mcp github get_file_contents external repo with fields preserved", "call_mcp_tool", "get_file_contents", `{"owner":"golang","repo":"go","path":"README.md","fields":["name","type"]}`, false, "", ""},
		{"mcp victoriametrics series empty args denied", "call_mcp_tool", "series", `{}`, true, "", ""},
		{"mcp victoriametrics series regex __name__=~ denied", "call_mcp_tool", "series", `{"match":"{__name__=~\"aerial_.*\"}","limit":50}`, true, "", ""},
		{"mcp victoriametrics series label-only selector without __name__ denied", "call_mcp_tool", "series", `{"match":"{job=\"aerial-brain\"}","limit":50}`, true, "", ""},
		{"mcp victoriametrics series concrete metric without limit denied", "call_mcp_tool", "series", `{"match":"aerial_brain_turns_total"}`, true, "", ""},
		{"mcp victoriametrics series concrete metric with limit > 100 denied", "call_mcp_tool", "series", `{"match":"aerial_brain_turns_total","limit":250}`, true, "", ""},
		{"mcp victoriametrics series concrete metric with limit <= 100 allowed", "call_mcp_tool", "series", `{"match":"aerial_brain_turns_total{status=\"success\"}","limit":50}`, false, "", ""},
		{"mcp victoriametrics series exact __name__ selector with limit allowed", "call_mcp_tool", "series", `{"match":"{__name__=\"aerial_brain_turns_total\"}","limit":100}`, false, "", ""},

		// victorialogs LogsQL guard: bounded time range (<= 7d) on windowed tools, | limit N (N <= 100) on raw-row query
		{"victorialogs query bounded start/end with limit allowed", "call_mcp_tool", "query", `{"query":"kubernetes.container_name:eri error | limit 50","start":"2026-11-12T00:00:00Z","end":"2026-11-12T06:00:00Z"}`, false, "", "victorialogs"},
		{"victorialogs query start/end over 7 days denied", "call_mcp_tool", "query", `{"query":"kubernetes.container_name:eri | limit 50","start":"2026-11-01T00:00:00Z","end":"2026-11-12T00:00:00Z"}`, true, "", "victorialogs"},
		{"victorialogs query exactly 7 day range allowed", "call_mcp_tool", "query", `{"query":"* | limit 10","start":"2026-11-05T00:00:00Z","end":"2026-11-12T00:00:00Z"}`, false, "", "victorialogs"},
		{"victorialogs query inverted range denied", "call_mcp_tool", "query", `{"query":"* | limit 10","start":"2026-11-12T00:00:00Z","end":"2026-11-11T00:00:00Z"}`, true, "", "victorialogs"},
		{"victorialogs query unix second range allowed", "call_mcp_tool", "query", `{"query":"* | limit 10","start":"1794441600","end":"1794445200"}`, false, "", "victorialogs"},
		{"victorialogs query bare star without range denied", "call_mcp_tool", "query", `{"query":"* | limit 10"}`, true, "", "victorialogs"},
		{"victorialogs query ancient start without end denied", "call_mcp_tool", "query", `{"query":"* | limit 10","start":"2020-01-01T00:00:00Z"}`, true, "", "victorialogs"},
		{"victorialogs query relative _time filter bounds wide args", "call_mcp_tool", "query", `{"query":"_time:1h kubernetes.container_name:eri | limit 20","start":"2020-01-01T00:00:00Z"}`, false, "", "victorialogs"},
		{"victorialogs query compound _time duration allowed", "call_mcp_tool", "query", `{"query":"_time:6d23h error | limit 20"}`, false, "", "victorialogs"},
		{"victorialogs query _time over 7 days denied", "call_mcp_tool", "query", `{"query":"_time:30d error | limit 20"}`, true, "", "victorialogs"},
		{"victorialogs query _time older-than filter denied", "call_mcp_tool", "query", `{"query":"_time:>1h error | limit 20"}`, true, "", "victorialogs"},
		{"victorialogs query _time bracket range allowed", "call_mcp_tool", "query", `{"query":"_time:[2026-11-12T00:00:00Z, 2026-11-12T06:00:00Z) error | limit 20"}`, false, "", "victorialogs"},
		{"victorialogs query _time bracket range over 7 days denied", "call_mcp_tool", "query", `{"query":"_time:[2026-10-01, 2026-11-01] error | limit 20"}`, true, "", "victorialogs"},
		{"victorialogs query _time single day allowed", "call_mcp_tool", "query", `{"query":"_time:2026-11-12 error | limit 20"}`, false, "", "victorialogs"},
		{"victorialogs query _time whole month denied", "call_mcp_tool", "query", `{"query":"_time:2026-11 error | limit 20"}`, true, "", "victorialogs"},
		{"victorialogs query _time inside quoted phrase ignored", "call_mcp_tool", "query", `{"query":"\"_time:1h\" | limit 20"}`, true, "", "victorialogs"},
		{"victorialogs query raw rows without limit denied", "call_mcp_tool", "query", `{"query":"kubernetes.container_name:eri error","start":"2026-11-12T00:00:00Z","end":"2026-11-12T06:00:00Z"}`, true, "", "victorialogs"},
		{"victorialogs query limit over 100 denied", "call_mcp_tool", "query", `{"query":"_time:1h error | limit 500"}`, true, "", "victorialogs"},
		{"victorialogs query limit exactly 100 allowed", "call_mcp_tool", "query", `{"query":"_time:1h error | limit 100"}`, false, "", "victorialogs"},
		{"victorialogs query head alias allowed", "call_mcp_tool", "query", `{"query":"_time:1h error | sort by (_time) desc | head 25"}`, false, "", "victorialogs"},
		{"victorialogs query pipe inside quoted phrase is not a limit", "call_mcp_tool", "query", `{"query":"_time:1h \"a | limit 5\""}`, true, "", "victorialogs"},
		{"victorialogs query stats pipe exempt from limit", "call_mcp_tool", "query", `{"query":"_time:1h error | stats by (kubernetes.container_name) count() hits"}`, false, "", "victorialogs"},
		{"victorialogs query shorthand stats function exempt from limit", "call_mcp_tool", "query", `{"query":"_time:1h error | count() hits"}`, false, "", "victorialogs"},
		{"victorialogs query stats still needs bounded range", "call_mcp_tool", "query", `{"query":"error | stats count() hits"}`, true, "", "victorialogs"},
		{"victorialogs string-encoded args allowed", "call_mcp_tool", "query", `"{\"query\":\"_time:5m | limit 5\"}"`, false, "", "victorialogs"},
		{"victorialogs-mcp server alias enforced", "call_mcp_tool", "query", `{"query":"error"}`, true, "", "victorialogs-mcp"},
		{"victorialogs stats_query without _time denied", "call_mcp_tool", "stats_query", `{"query":"* | stats count() hits","time":"2026-11-12T00:00:00Z"}`, true, "", "victorialogs"},
		{"victorialogs stats_query with _time allowed without limit", "call_mcp_tool", "stats_query", `{"query":"_time:1d * | stats by (kubernetes.container_name) count() hits"}`, false, "", "victorialogs"},
		{"victorialogs stats_query_range over 7 days denied", "call_mcp_tool", "stats_query_range", `{"query":"* | stats count() hits","start":"2026-10-01T00:00:00Z","end":"2026-11-12T00:00:00Z","step":"1h"}`, true, "", "victorialogs"},
		{"victorialogs hits bounded range allowed", "call_mcp_tool", "hits", `{"query":"error","start":"2026-11-11T00:00:00Z","end":"2026-11-12T00:00:00Z","step":"1h"}`, false, "", "victorialogs"},
		{"victorialogs facets unbounded denied", "call_mcp_tool", "facets", `{"query":"*","start":"2026-01-01T00:00:00Z","end":"2026-11-12T00:00:00Z"}`, true, "", "victorialogs"},
		{"victorialogs ungoverned tool allowed", "call_mcp_tool", "field_names", `{"query":"*","start":"2026-01-01T00:00:00Z"}`, false, "", "victorialogs"},
		{"victoriametrics query arg name not treated as logsql", "call_mcp_tool", "query", `{"query":"up"}`, false, "", "victoriametrics"},
		{"claude victorialogs query unbounded denied", "mcp__victorialogs__query", "query", `{"query":"* | limit 10"}`, true, "", "victorialogs"},
		{"claude victorialogs query bounded allowed", "mcp__victorialogs__query", "query", `{"query":"_time:15m kubernetes.container_name:synapse | limit 10"}`, false, "", "victorialogs"},
		{"claude discord_search_messages limit 50 denied", "mcp__discord__discord_search_messages", "discord_search_messages", `{"guildId":"123","limit":50}`, true, "", "discord"},
	}
}

func checkSearchTestCases() []searchTestCase {
	return append(checkSearchBaseCases(), checkSearchExtendedCases()...)
}

func runCheckSearchCase(t *testing.T, tc searchTestCase) {
	t.Helper()
	tool := tc.tool
	if tool == "" {
		tool = "run_command"
	}
	args := ToolArgs{CommandLine: tc.cmd, Cwd: tc.cwd}
	if normalizeToolName(tool) == "call_mcp_tool" || isClaudeMCPToolName(tool) {
		args = ToolArgs{ServerName: tc.server, ToolName: tc.cmd, Arguments: json.RawMessage(tc.cwd)}
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
}

func TestCheckSearch(t *testing.T) {
	t.Parallel()

	tests := checkSearchTestCases()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runCheckSearchCase(t, tc)
		})
	}
}

type psqlTestCase struct {
	name          string
	cmd           string
	conn          *fakeSQLCatalogConn
	connErr       error
	wantDeny      bool
	wantReasonSub string
	wantOverwrite string
}

func psqlGuardBasicCases(twoCols, threeCols, cteCols, embedCols []SQLColumn, meta3 map[uint32][2]string, rel3 map[string]string, limitPlan, aggLimitPlan, overLimitPlan, unboundedPlan, pkPlan, dmlPlan string) []psqlTestCase {
	return []psqlTestCase{
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
			name:          "aggregate group by with matching column count allowed",
			cmd:           `psql "$DATABASE_URL" -c "SELECT author_name, count(1), max(id) FROM messages GROUP BY author_name LIMIT 10"`,
			conn:          &fakeSQLCatalogConn{cols: cteCols, explainJSON: aggLimitPlan, tableMeta: meta3, relCounts: rel3},
			wantDeny:      false,
			wantOverwrite: `psql -A "$DATABASE_URL" -c "SELECT author_name, count(1), max(id) FROM messages GROUP BY author_name LIMIT 10"`,
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
	}
}

func psqlGuardErrorCases(twoCols, threeCols []SQLColumn, meta3 map[uint32][2]string, rel3 map[string]string, schemaRows [][3]string, limitPlan string) []psqlTestCase {
	return []psqlTestCase{
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
}

func psqlGuardTestCases(twoCols, threeCols, cteCols, embedCols []SQLColumn, meta3 map[uint32][2]string, rel3 map[string]string, schemaRows [][3]string, limitPlan, aggLimitPlan, overLimitPlan, unboundedPlan, pkPlan, dmlPlan string) []psqlTestCase {
	cases := psqlGuardBasicCases(twoCols, threeCols, cteCols, embedCols, meta3, rel3, limitPlan, aggLimitPlan, overLimitPlan, unboundedPlan, pkPlan, dmlPlan)
	return append(cases, psqlGuardErrorCases(twoCols, threeCols, meta3, rel3, schemaRows, limitPlan)...)
}

func runPsqlGuardCase(t *testing.T, tc psqlTestCase) {
	t.Helper()
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
	aggLimitPlan := `[{"Plan": {"Node Type": "Limit", "Plan Rows": 10, "Plans": [{"Node Type": "Aggregate", "Plan Rows": 10, "Plans": [{"Node Type": "Seq Scan", "Relation Name": "messages", "Plan Rows": 100}]}]}}]`
	overLimitPlan := `[{"Plan": {"Node Type": "Limit", "Plan Rows": 100, "Plans": [{"Node Type": "Seq Scan", "Relation Name": "messages", "Plan Rows": 200}]}}]`
	unboundedPlan := `[{"Plan": {"Node Type": "Sort", "Plan Rows": 103, "Plans": [{"Node Type": "Seq Scan", "Relation Name": "messages", "Plan Rows": 103}]}}]`
	pkPlan := `[{"Plan": {"Node Type": "Index Scan", "Relation Name": "messages", "Plan Rows": 1}}]`
	dmlPlan := `[{"Plan": {"Node Type": "ModifyTable", "Relation Name": "messages", "Plan Rows": 1}}]`

	tests := psqlGuardTestCases(twoCols, threeCols, cteCols, embedCols, meta3, rel3, schemaRows, limitPlan, aggLimitPlan, overLimitPlan, unboundedPlan, pkPlan, dmlPlan)

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
			runPsqlGuardCase(t, tc)
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
