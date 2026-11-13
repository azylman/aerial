package guard

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	maxLogsQLWindow   = 7 * 24 * time.Hour
	maxLogsQLRawLimit = 100
)

// victoriaLogsWindowTools are the victorialogs-mcp tools that scan log rows over a time window.
// "query" returns raw rows and additionally requires a bounded `| limit N` pipe; the others
// return aggregates and only need a bounded window.
var victoriaLogsWindowTools = map[string]bool{
	"query":             true,
	"hits":              true,
	"facets":            true,
	"stats_query":       true,
	"stats_query_range": true,
}

// logsQLStatsFuncs are stats pipe functions LogsQL accepts without the leading `stats` keyword.
var logsQLStatsFuncs = map[string]bool{
	"avg": true, "count": true, "count_empty": true, "count_uniq": true, "count_uniq_hash": true,
	"histogram": true, "max": true, "median": true, "min": true, "quantile": true, "rate": true,
	"rate_sum": true, "row_any": true, "row_max": true, "row_min": true, "sum": true, "sum_len": true,
	"uniq_values": true, "values": true,
}

var (
	logsQLTimeFilterRe = regexp.MustCompile(`(?i)_time:\s*([\[(][^\])]*[\])]|[^\s|()]+)`)
	logsQLDurationRe   = regexp.MustCompile(`^(?:\d+(?:\.\d+)?(?:ms|y|w|d|h|m|s))+$`)
	logsQLDurationPart = regexp.MustCompile(`(\d+(?:\.\d+)?)(ms|y|w|d|h|m|s)`)
)

var logsQLDurationUnits = map[string]time.Duration{
	"ms": time.Millisecond,
	"s":  time.Second,
	"m":  time.Minute,
	"h":  time.Hour,
	"d":  24 * time.Hour,
	"w":  7 * 24 * time.Hour,
	"y":  365 * 24 * time.Hour,
}

// logsQLDayOrFinerLayouts are absolute timestamp layouts whose implied range is at most one day.
var logsQLDayOrFinerLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02T15:04:05",
	"2006-01-02T15:04",
	"2006-01-02T15",
	"2006-01-02",
}

// isClaudeMCPToolName reports whether toolName uses Claude's mcp__<server>__<tool> form.
func isClaudeMCPToolName(toolName string) bool {
	n := strings.ToLower(strings.TrimSpace(toolName))
	return strings.HasPrefix(n, "mcp__") && strings.Count(n, "__") >= 2
}

func isVictoriaLogsServer(args ToolArgs) bool {
	server := args.ServerName
	if server == "" {
		server = args.ServerNameLower
	}
	server = strings.ToLower(strings.Trim(strings.TrimSpace(server), `"`))
	return server == "victorialogs" || server == "victorialogs-mcp"
}

// stripLogsQLQuotes blanks out quoted phrases so pipes and filters inside string literals are ignored.
func stripLogsQLQuotes(q string) string {
	var b strings.Builder
	for i := 0; i < len(q); i++ {
		c := q[i]
		if c != '"' && c != '\'' && c != '`' {
			b.WriteByte(c)
			continue
		}
		for i++; i < len(q) && q[i] != c; i++ {
			if q[i] == '\\' && c != '`' {
				i++
			}
		}
		b.WriteByte(' ')
	}
	return b.String()
}

func parseLogsQLDuration(s string) (time.Duration, bool) {
	s = strings.ToLower(s)
	if !logsQLDurationRe.MatchString(s) {
		return 0, false
	}
	var total time.Duration
	for _, m := range logsQLDurationPart.FindAllStringSubmatch(s, -1) {
		n, err := strconv.ParseFloat(m[1], 64)
		if err != nil {
			return 0, false
		}
		total += time.Duration(n * float64(logsQLDurationUnits[m[2]]))
	}
	return total, total > 0
}

func parseLogsQLTimestamp(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		if n > 1e11 {
			return time.UnixMilli(n), true
		}
		return time.Unix(n, 0), true
	}
	return parseLogsQLDayOrFiner(s)
}

// parseLogsQLDayOrFiner parses absolute timestamps with at most day granularity (never bare years or months).
func parseLogsQLDayOrFiner(s string) (time.Time, bool) {
	for _, layout := range logsQLDayOrFinerLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// logsQLFilterBounded reports whether the query carries a `_time:` filter covering at most maxLogsQLWindow:
// a relative duration (`_time:1h`), a bracketed range (`_time:[a, b)`), or a single day-or-finer timestamp.
func logsQLFilterBounded(cleaned string) bool {
	for _, m := range logsQLTimeFilterRe.FindAllStringSubmatch(cleaned, -1) {
		v := strings.TrimSpace(m[1])
		if d, ok := parseLogsQLDuration(v); ok && d <= maxLogsQLWindow {
			return true
		}
		if strings.HasPrefix(v, "[") || strings.HasPrefix(v, "(") {
			parts := strings.SplitN(v[1:len(v)-1], ",", 2)
			if len(parts) != 2 {
				continue
			}
			start, okStart := parseLogsQLTimestamp(parts[0])
			end, okEnd := parseLogsQLTimestamp(parts[1])
			if okStart && okEnd && end.After(start) && end.Sub(start) <= maxLogsQLWindow {
				return true
			}
			continue
		}
		if _, ok := parseLogsQLDayOrFiner(v); ok {
			return true
		}
	}
	return false
}

// logsQLArgsBounded reports whether the start/end tool arguments span at most maxLogsQLWindow.
// A missing end defaults to now, matching victorialogs-mcp behaviour.
func logsQLArgsBounded(m map[string]any, now time.Time) bool {
	start, ok := parseLogsQLTimestamp(stringField(m, "start"))
	if !ok {
		return false
	}
	end := now
	if rawEnd := stringField(m, "end"); rawEnd != "" {
		if end, ok = parseLogsQLTimestamp(rawEnd); !ok {
			return false
		}
	}
	return end.After(start) && end.Sub(start) <= maxLogsQLWindow
}

// logsQLRowShape scans the pipes of a quote-stripped LogsQL query. It reports whether a stats-style
// aggregation pipe is present and the smallest row cap from `limit`/`head`/`first`/`last` pipes (0 if none).
func logsQLRowShape(cleaned string) (isStats bool, rowCap int) {
	segments := strings.Split(cleaned, "|")
	for _, seg := range segments[1:] {
		fields := strings.Fields(strings.ToLower(seg))
		if len(fields) == 0 {
			continue
		}
		head := fields[0]
		funcName, _, hasParen := strings.Cut(head, "(")
		switch {
		case head == "stats" || head == "top" || head == "by" || strings.HasPrefix(head, "by("):
			isStats = true
		case hasParen && logsQLStatsFuncs[funcName]:
			isStats = true
		case head == "limit" || head == "head" || head == "first" || head == "last":
			if len(fields) < 2 {
				continue
			}
			if n, err := strconv.Atoi(fields[1]); err == nil && n > 0 && (rowCap == 0 || n < rowCap) {
				rowCap = n
			}
		}
	}
	return isStats, rowCap
}

// inspectVictoriaLogsQuery enforces bounded LogsQL scans on victorialogs-mcp: every windowed tool needs a
// time range of at most 7 days, and raw-row `query` calls need `| limit N` with N <= 100 unless they aggregate.
// It returns nil for tools it does not govern.
func inspectVictoriaLogsQuery(tool string, raw json.RawMessage, now time.Time) *Decision {
	if !victoriaLogsWindowTools[tool] {
		return nil
	}
	m := parseRawJSONMap(raw)
	cleaned := stripLogsQLQuotes(stringField(m, "query"))
	if !logsQLFilterBounded(cleaned) && !logsQLArgsBounded(m, now) {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Unbounded victorialogs " + tool + " detected: LogsQL scans require an explicit time range of at most 7 days. Pass 'start' (and 'end') no more than 7 days apart, or add a '_time:' filter such as '_time:1h' or '_time:[2026-01-02T00:00:00Z, 2026-01-02T06:00:00Z)' to the query.",
		}
	}
	if tool != "query" {
		return nil
	}
	isStats, rowCap := logsQLRowShape(cleaned)
	if !isStats && (rowCap == 0 || rowCap > maxLogsQLRawLimit) {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "victorialogs query returns raw log rows and requires a '| limit N' pipe with N <= 100 (e.g. 'kubernetes.container_name:eri error | limit 50'). Use a '| stats' pipe (or the stats_query/hits tools) for counts and aggregations.",
		}
	}
	return nil
}
