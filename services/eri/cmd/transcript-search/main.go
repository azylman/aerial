package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/transcript"
)

type CLIConfig struct {
	Command  string
	Query    string
	Mode     string
	Tool     string
	Session  string
	Limit    int
	Step     int
	Context  int
	JSON     bool
	BaseURL  string
	DirectDB bool
}

func splitSubcommandAndArgs(args []string) (string, []string, error) {
	if len(args) == 0 {
		return "", nil, fmt.Errorf("no arguments provided")
	}
	first := args[0]
	switch first {
	case "stats", "search", "get":
		return first, args[1:], nil
	default:
		return "search", args, nil
	}
}

func isFlagWithValue(name string) bool {
	switch name {
	case "query", "q", "mode", "m", "tool", "t", "session", "s", "limit", "n", "url", "step", "i", "context", "C":
		return true
	default:
		return false
	}
}

func partitionCLIFlagsAndPositional(remaining []string) ([]string, []string) {
	var flags []string
	var positional []string
	for i := 0; i < len(remaining); i++ {
		arg := remaining[i]
		if !strings.HasPrefix(arg, "-") {
			positional = append(positional, arg)
			continue
		}
		flags = append(flags, arg)
		if strings.Contains(arg, "=") {
			continue
		}
		flagName := strings.TrimLeft(arg, "-")
		if isFlagWithValue(flagName) && i+1 < len(remaining) && !strings.HasPrefix(remaining[i+1], "-") {
			i++
			flags = append(flags, remaining[i])
		}
	}
	return flags, positional
}

func validateParsedCLIConfig(cfg *CLIConfig, positional []string) error {
	if cfg.Limit < 0 {
		return fmt.Errorf("limit must be non-negative")
	}
	if cfg.Context < 0 {
		return fmt.Errorf("context window size must be non-negative")
	}
	if cfg.Command == "get" {
		if strings.TrimSpace(cfg.Session) == "" && len(positional) > 0 {
			cfg.Session = positional[0]
		}
		if strings.TrimSpace(cfg.Session) == "" {
			return fmt.Errorf("session ID is required for get command (usage: transcript-search get --session <id> --step <N>)")
		}
		if cfg.Step < 0 {
			return fmt.Errorf("step index is required and must be non-negative for get command (usage: transcript-search get --session <id> --step <N>)")
		}
	}
	if cfg.Command == "search" && cfg.Query == "" && len(positional) > 0 {
		cfg.Query = strings.Join(positional, " ")
	}
	return nil
}

func parseCLIArgs(args []string) (CLIConfig, error) {
	cfg := CLIConfig{
		Mode:    "auto",
		Limit:   10,
		Step:    -1,
		Context: 2,
		BaseURL: "http://localhost:8080",
	}

	if envURL := os.Getenv("ERI_URL"); envURL != "" {
		cfg.BaseURL = strings.TrimRight(envURL, "/")
	}

	cmd, remaining, err := splitSubcommandAndArgs(args)
	if err != nil {
		return cfg, err
	}
	cfg.Command = cmd

	flags, positional := partitionCLIFlagsAndPositional(remaining)

	fs := flag.NewFlagSet("transcript-search", flag.ContinueOnError)
	fs.StringVar(&cfg.Query, "query", cfg.Query, "Search query string")
	fs.StringVar(&cfg.Query, "q", cfg.Query, "Search query string (shorthand)")
	fs.StringVar(&cfg.Mode, "mode", cfg.Mode, "Search mode: auto, steps, sessions")
	fs.StringVar(&cfg.Mode, "m", cfg.Mode, "Search mode (shorthand)")
	fs.StringVar(&cfg.Tool, "tool", "", "Filter steps by tool name (e.g. run_command, view_file)")
	fs.StringVar(&cfg.Tool, "t", "", "Filter steps by tool name (shorthand)")
	fs.StringVar(&cfg.Session, "session", "", "Filter steps by session ID")
	fs.StringVar(&cfg.Session, "s", "", "Filter steps by session ID (shorthand)")
	fs.IntVar(&cfg.Limit, "limit", cfg.Limit, "Maximum results to return")
	fs.IntVar(&cfg.Limit, "n", cfg.Limit, "Maximum results to return (shorthand)")
	fs.IntVar(&cfg.Step, "step", cfg.Step, "Target step index to inspect")
	fs.IntVar(&cfg.Step, "i", cfg.Step, "Target step index (shorthand)")
	fs.IntVar(&cfg.Context, "context", cfg.Context, "Surrounding context step window size")
	fs.IntVar(&cfg.Context, "C", cfg.Context, "Surrounding context step window size (shorthand)")
	fs.BoolVar(&cfg.JSON, "json", false, "Output results as raw JSON")
	fs.StringVar(&cfg.BaseURL, "url", cfg.BaseURL, "Aerial brain base HTTP URL")
	fs.BoolVar(&cfg.DirectDB, "direct-db", false, "Force direct PostgreSQL connection instead of HTTP daemon")

	if err := fs.Parse(flags); err != nil {
		return cfg, err
	}

	if err := validateParsedCLIConfig(&cfg, positional); err != nil {
		return cfg, err
	}

	return cfg, nil
}

func formatSearchSessions(sb *strings.Builder, sessions []db.SessionSummary) {
	sb.WriteString(fmt.Sprintf("### 📄 Session Summaries (%d matches)\n", len(sessions)))
	for _, s := range sessions {
		convID := s.SessionID
		shortID := convID
		if len(shortID) > 8 {
			shortID = shortID[:8] + "..."
		}
		link := fmt.Sprintf("[%s](conversation://%s)", shortID, convID)
		sb.WriteString(fmt.Sprintf("- Session %s", link))
		if s.ThreadID != "" {
			sb.WriteString(fmt.Sprintf(" (Thread: `%s`)", s.ThreadID))
		}
		sb.WriteString(":\n")
		if s.Summary != "" {
			sb.WriteString(fmt.Sprintf("  *Summary:* %s\n", transcript.CleanSessionSummary(s.Summary, transcript.DefaultSummaryMaxRunes)))
		}
		sb.WriteString("\n")
	}
}

func formatSearchSteps(sb *strings.Builder, steps []db.TranscriptStep) {
	sb.WriteString(fmt.Sprintf("### 💬 Transcript Execution Steps (%d matches)\n", len(steps)))
	for _, step := range steps {
		convID := step.SessionID
		shortID := convID
		if len(shortID) > 8 {
			shortID = shortID[:8] + "..."
		}
		link := fmt.Sprintf("[%s](conversation://%s)", shortID, convID)
		toolBadge := "STEP"
		if step.ToolName != "" {
			toolBadge = step.ToolName
		} else if step.StepType != "" {
			toolBadge = step.StepType
		}

		ts := step.CreatedAt.Format("2006-01-02 15:04:05")
		sb.WriteString(fmt.Sprintf("- **[%s]** Step #%d in %s (%s):\n", toolBadge, step.StepIndex, link, ts))
		if step.Content != "" {
			snippet := transcript.CleanSessionSummary(step.Content, 300)
			if strings.Contains(snippet, "\n") {
				sb.WriteString(fmt.Sprintf("  ```\n  %s\n  ```\n", strings.ReplaceAll(snippet, "\n", "\n  ")))
			} else {
				sb.WriteString(fmt.Sprintf("  *Content:* %s\n", snippet))
			}
		}
		sb.WriteString("\n")
	}
}

func formatSearchResults(res transcript.SearchResult) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("🔍 Search Results for: `%s` (mode: %s)\n\n", res.Query, res.Mode))

	hasMatches := false
	if len(res.Sessions) > 0 {
		hasMatches = true
		formatSearchSessions(&sb, res.Sessions)
	}

	if len(res.Steps) > 0 {
		hasMatches = true
		formatSearchSteps(&sb, res.Steps)
	}

	if !hasMatches {
		sb.WriteString("*(No matching records found)*\n")
	}

	return sb.String()
}

func formatStepWindow(res transcript.StepWindowResult) string {
	var sb strings.Builder
	convID := res.SessionID
	shortID := convID
	if len(shortID) > 8 {
		shortID = shortID[:8] + "..."
	}
	link := fmt.Sprintf("[%s](conversation://%s)", shortID, convID)
	sb.WriteString(fmt.Sprintf("🔍 Transcript Step Window for Session %s (step: %d, context: ±%d)\n\n", link, res.CenterStep, res.Context))

	if len(res.Steps) == 0 {
		sb.WriteString("*(No matching steps found in range)*\n")
		return sb.String()
	}

	for _, step := range res.Steps {
		marker := " "
		maxRunes := 600
		if step.StepIndex == res.CenterStep {
			marker = "▶"
			maxRunes = 2500
		}
		toolBadge := "STEP"
		if step.ToolName != "" {
			toolBadge = step.ToolName
		} else if step.StepType != "" {
			toolBadge = step.StepType
		}

		ts := step.CreatedAt.Format("2006-01-02 15:04:05")
		sb.WriteString(fmt.Sprintf("%s **[%s]** Step #%d (%s):\n", marker, toolBadge, step.StepIndex, ts))
		if step.Content != "" {
			content := transcript.CleanSessionSummary(step.Content, maxRunes)
			if strings.Contains(content, "\n") {
				sb.WriteString(fmt.Sprintf("  ```\n  %s\n  ```\n", strings.ReplaceAll(content, "\n", "\n  ")))
			} else {
				sb.WriteString(fmt.Sprintf("  *Content:* %s\n", content))
			}
		}
		sb.WriteString("\n")
	}

	return sb.String()
}

var (
	newRequestWithContext = http.NewRequestWithContext
	newHTTPClient         = func(timeout time.Duration) *http.Client {
		return &http.Client{Timeout: timeout}
	}
)

func fetchTranscriptAPI[T any](ctx context.Context, baseURL, endpointPath string, params url.Values) (T, error) {
	var out T
	targetURL := strings.TrimRight(baseURL, "/") + endpointPath
	if len(params) > 0 {
		targetURL += "?" + params.Encode()
	}

	req, err := newRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return out, err
	}

	// Use 5s timeout to leave headroom for direct-DB fallback within parent CLI context
	client := newHTTPClient(5 * time.Second)
	resp, err := client.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return out, err
	}

	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	if err := json.Unmarshal(body, &out); err != nil {
		return out, fmt.Errorf("failed unmarshaling response: %w", err)
	}

	return out, nil
}

func executeSearchHTTP(ctx context.Context, cfg CLIConfig) (transcript.SearchResult, error) {
	params := url.Values{}
	params.Set("q", cfg.Query)
	params.Set("mode", cfg.Mode)
	if cfg.Tool != "" {
		params.Set("tool", cfg.Tool)
	}
	if cfg.Session != "" {
		params.Set("session", cfg.Session)
	}
	params.Set("limit", strconv.Itoa(cfg.Limit))

	return fetchTranscriptAPI[transcript.SearchResult](ctx, cfg.BaseURL, "/api/transcripts/search", params)
}

func executeStatsHTTP(ctx context.Context, cfg CLIConfig) (map[string]any, error) {
	return fetchTranscriptAPI[map[string]any](ctx, cfg.BaseURL, "/api/transcripts/stats", nil)
}

var (
	openDBFn          = db.Open
	jsonMarshalIndent = json.MarshalIndent
)

var newDirectStoreFn = func(pgURL string) (db.Store, func(), error) {
	sqlDB, err := openDBFn(pgURL)
	if err != nil {
		return nil, nil, fmt.Errorf("postgres connect failed: %w", err)
	}
	return db.NewSQLStore(sqlDB), func() {
		if err := sqlDB.Close(); err != nil {
			return
		}
	}, nil
}

func executeSearchDirectDB(ctx context.Context, cfg CLIConfig) (transcript.SearchResult, error) {
	pgURL := os.Getenv("POSTGRES_URL")
	if pgURL == "" {
		pgURL = "postgres://aerial:aerial@aerial-postgres:5432/aerial?sslmode=disable"
	}
	store, closeFn, err := newDirectStoreFn(pgURL)
	if err != nil {
		return transcript.SearchResult{}, err
	}
	defer closeFn()

	return transcript.SearchTranscripts(ctx, store, nil, cfg.Query, cfg.Mode, cfg.Session, cfg.Tool, cfg.Limit)
}

func executeGetHTTP(ctx context.Context, cfg CLIConfig) (transcript.StepWindowResult, error) {
	params := url.Values{}
	params.Set("session", cfg.Session)
	params.Set("step", strconv.Itoa(cfg.Step))
	params.Set("context", strconv.Itoa(cfg.Context))

	return fetchTranscriptAPI[transcript.StepWindowResult](ctx, cfg.BaseURL, "/api/transcripts/steps", params)
}

func executeGetDirectDB(ctx context.Context, cfg CLIConfig) (transcript.StepWindowResult, error) {
	pgURL := os.Getenv("POSTGRES_URL")
	if pgURL == "" {
		pgURL = "postgres://aerial:aerial@aerial-postgres:5432/aerial?sslmode=disable"
	}
	store, closeFn, err := newDirectStoreFn(pgURL)
	if err != nil {
		return transcript.StepWindowResult{}, err
	}
	defer closeFn()

	return transcript.GetTranscriptStepWindow(ctx, store, cfg.Session, cfg.Step, cfg.Context)
}

func writeCLIOutput(out io.Writer, asJSON bool, payload any, formatted string) error {
	if asJSON {
		b, err := jsonMarshalIndent(payload, "", "  ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(out, string(b))
		return err
	}
	_, err := fmt.Fprint(out, formatted)
	return err
}

func runStatsCLI(ctx context.Context, cfg CLIConfig, out io.Writer) error {
	stats, err := executeStatsHTTP(ctx, cfg)
	if err != nil {
		return fmt.Errorf("failed fetching stats: %w", err)
	}
	formatted := fmt.Sprintf("📊 Aerial Transcript Index Stats:\n- Total Indexed Sessions: %v\n- Status: %v\n", stats["total_sessions"], stats["status"])
	return writeCLIOutput(out, cfg.JSON, stats, formatted)
}

func resolveGetResult(ctx context.Context, cfg CLIConfig) (transcript.StepWindowResult, error) {
	if cfg.DirectDB {
		dbRes, dbErr := executeGetDirectDB(ctx, cfg)
		if dbErr != nil {
			return transcript.StepWindowResult{}, fmt.Errorf("direct DB get failed: %w", dbErr)
		}
		return dbRes, nil
	}
	httpRes, httpErr := executeGetHTTP(ctx, cfg)
	if httpErr == nil {
		return httpRes, nil
	}
	dbRes, dbErr := executeGetDirectDB(ctx, cfg)
	if dbErr != nil {
		return transcript.StepWindowResult{}, fmt.Errorf("get failed via HTTP (%w) and direct DB (%w)", httpErr, dbErr)
	}
	return dbRes, nil
}

func runGetCLI(ctx context.Context, cfg CLIConfig, out io.Writer) error {
	res, err := resolveGetResult(ctx, cfg)
	if err != nil {
		return err
	}
	return writeCLIOutput(out, cfg.JSON, res, formatStepWindow(res))
}

func resolveSearchResult(ctx context.Context, cfg CLIConfig) (transcript.SearchResult, error) {
	if cfg.DirectDB {
		dbRes, dbErr := executeSearchDirectDB(ctx, cfg)
		if dbErr != nil {
			return transcript.SearchResult{}, fmt.Errorf("direct DB search failed: %w", dbErr)
		}
		return dbRes, nil
	}
	httpRes, httpErr := executeSearchHTTP(ctx, cfg)
	if httpErr == nil {
		return httpRes, nil
	}
	dbRes, dbErr := executeSearchDirectDB(ctx, cfg)
	if dbErr != nil {
		return transcript.SearchResult{}, fmt.Errorf("search failed via HTTP (%w) and direct DB (%w)", httpErr, dbErr)
	}
	return dbRes, nil
}

func runSearchCLI(ctx context.Context, cfg CLIConfig, out io.Writer) error {
	if cfg.Query == "" {
		return fmt.Errorf("search query cannot be empty (usage: transcript-search search <query>)")
	}

	res, err := resolveSearchResult(ctx, cfg)
	if err != nil {
		return err
	}
	return writeCLIOutput(out, cfg.JSON, res, formatSearchResults(res))
}

func runCLI(ctx context.Context, args []string, out io.Writer) error {
	cfg, err := parseCLIArgs(args)
	if err != nil {
		return err
	}

	switch cfg.Command {
	case "stats":
		return runStatsCLI(ctx, cfg, out)
	case "get":
		return runGetCLI(ctx, cfg, out)
	default:
		return runSearchCLI(ctx, cfg, out)
	}
}

func runMain(args []string, out, errOut io.Writer) int {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := runCLI(ctx, args, out); err != nil {
		if _, wErr := fmt.Fprintf(errOut, "Error: %v\n", err); wErr != nil {
			return 1
		}
		return 1
	}
	return 0
}

func main() {
	os.Exit(runMain(os.Args[1:], os.Stdout, os.Stderr))
}
