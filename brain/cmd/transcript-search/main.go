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

	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/transcript"
)

type CLIConfig struct {
	Command  string
	Query    string
	Mode     string
	Tool     string
	Session  string
	Limit    int
	JSON     bool
	BaseURL  string
	DirectDB bool
}

func parseCLIArgs(args []string) (CLIConfig, error) {
	cfg := CLIConfig{
		Mode:    "auto",
		Limit:   10,
		BaseURL: "http://localhost:8080",
	}

	if envURL := os.Getenv("AERIAL_BRAIN_URL"); envURL != "" {
		cfg.BaseURL = strings.TrimRight(envURL, "/")
	}

	if len(args) == 0 {
		return cfg, fmt.Errorf("no arguments provided")
	}

	// Subcommand detection
	first := args[0]
	var remaining []string

	if first == "stats" {
		cfg.Command = "stats"
		remaining = args[1:]
	} else if first == "search" {
		cfg.Command = "search"
		remaining = args[1:]
	} else {
		cfg.Command = "search"
		remaining = args
	}

	var flags []string
	var positional []string
	for i := 0; i < len(remaining); i++ {
		arg := remaining[i]
		if strings.HasPrefix(arg, "-") {
			flags = append(flags, arg)
			flagName := strings.TrimLeft(arg, "-")
			if !strings.Contains(arg, "=") {
				switch flagName {
				case "query", "q", "mode", "m", "tool", "t", "session", "s", "limit", "n", "url":
					if i+1 < len(remaining) && !strings.HasPrefix(remaining[i+1], "-") {
						i++
						flags = append(flags, remaining[i])
					}
				}
			}
		} else {
			positional = append(positional, arg)
		}
	}

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
	fs.BoolVar(&cfg.JSON, "json", false, "Output results as raw JSON")
	fs.StringVar(&cfg.BaseURL, "url", cfg.BaseURL, "Aerial brain base HTTP URL")
	fs.BoolVar(&cfg.DirectDB, "direct-db", false, "Force direct PostgreSQL connection instead of HTTP daemon")

	if err := fs.Parse(flags); err != nil {
		return cfg, err
	}

	if cfg.Command == "search" && cfg.Query == "" && len(positional) > 0 {
		cfg.Query = strings.Join(positional, " ")
	}

	return cfg, nil
}

func formatSearchResults(res transcript.SearchResult) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("🔍 Search Results for: `%s` (mode: %s)\n\n", res.Query, res.Mode))

	hasMatches := false

	if len(res.Sessions) > 0 {
		hasMatches = true
		sb.WriteString(fmt.Sprintf("### 📄 Session Summaries (%d matches)\n", len(res.Sessions)))
		for _, s := range res.Sessions {
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
				sb.WriteString(fmt.Sprintf("  *Summary:* %s\n", s.Summary))
			}
			sb.WriteString("\n")
		}
	}

	if len(res.Steps) > 0 {
		hasMatches = true
		sb.WriteString(fmt.Sprintf("### 💬 Transcript Execution Steps (%d matches)\n", len(res.Steps)))
		for _, step := range res.Steps {
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
				snippet := strings.TrimSpace(step.Content)
				if len(snippet) > 300 {
					snippet = snippet[:300] + "... [truncated]"
				}
				// Format inline code block for multiline snippet
				if strings.Contains(snippet, "\n") {
					sb.WriteString(fmt.Sprintf("  ```\n  %s\n  ```\n", strings.ReplaceAll(snippet, "\n", "\n  ")))
				} else {
					sb.WriteString(fmt.Sprintf("  *Content:* %s\n", snippet))
				}
			}
			sb.WriteString("\n")
		}
	}

	if !hasMatches {
		sb.WriteString("*(No matching records found)*\n")
	}

	return sb.String()
}

func executeSearchHTTP(ctx context.Context, cfg CLIConfig) (transcript.SearchResult, error) {
	endpoint := fmt.Sprintf("%s/api/transcripts/search", strings.TrimRight(cfg.BaseURL, "/"))
	u, err := url.Parse(endpoint)
	if err != nil {
		return transcript.SearchResult{}, err
	}

	q := u.Query()
	q.Set("q", cfg.Query)
	q.Set("mode", cfg.Mode)
	if cfg.Tool != "" {
		q.Set("tool", cfg.Tool)
	}
	if cfg.Session != "" {
		q.Set("session", cfg.Session)
	}
	q.Set("limit", strconv.Itoa(cfg.Limit))
	u.RawQuery = q.Encode()

	req, err := newRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return transcript.SearchResult{}, err
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return transcript.SearchResult{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return transcript.SearchResult{}, err
	}

	if resp.StatusCode != http.StatusOK {
		return transcript.SearchResult{}, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	var res transcript.SearchResult
	if err := json.Unmarshal(body, &res); err != nil {
		return transcript.SearchResult{}, fmt.Errorf("failed unmarshaling response: %w", err)
	}

	return res, nil
}

var newRequestWithContext = http.NewRequestWithContext

func executeStatsHTTP(ctx context.Context, cfg CLIConfig) (map[string]any, error) {
	endpoint := fmt.Sprintf("%s/api/transcripts/stats", strings.TrimRight(cfg.BaseURL, "/"))
	req, err := newRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	var stats map[string]any
	if err := json.Unmarshal(body, &stats); err != nil {
		return nil, err
	}
	return stats, nil
}

var (
	initDBFn          = db.InitDB
	jsonMarshalIndent = json.MarshalIndent
)

var newDirectStoreFn = func(pgURL string) (db.Store, func(), error) {
	sqlDB, err := initDBFn(pgURL)
	if err != nil {
		return nil, nil, fmt.Errorf("postgres connect failed: %w", err)
	}
	return db.NewSQLStore(sqlDB), func() { _ = sqlDB.Close() }, nil //nolint:errcheck
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

func runCLI(ctx context.Context, args []string, out io.Writer) error {
	cfg, err := parseCLIArgs(args)
	if err != nil {
		return err
	}

	if cfg.Command == "stats" {
		stats, err := executeStatsHTTP(ctx, cfg)
		if err != nil {
			return fmt.Errorf("failed fetching stats: %w", err)
		}
		if cfg.JSON {
			b, err := jsonMarshalIndent(stats, "", "  ")
			if err != nil {
				return err
			}
			if _, err := fmt.Fprintln(out, string(b)); err != nil {
				return err
			}
		} else {
			if _, err := fmt.Fprintf(out, "📊 Aerial Transcript Index Stats:\n- Total Indexed Sessions: %v\n- Status: %v\n", stats["total_sessions"], stats["status"]); err != nil {
				return err
			}
		}
		return nil
	}

	// Search execution
	if cfg.Query == "" {
		return fmt.Errorf("search query cannot be empty (usage: transcript-search search <query>)")
	}

	var res transcript.SearchResult
	if !cfg.DirectDB {
		httpRes, httpErr := executeSearchHTTP(ctx, cfg)
		if httpErr == nil {
			res = httpRes
		} else {
			// Fallback to direct DB if HTTP daemon is down or unreachable
			dbRes, dbErr := executeSearchDirectDB(ctx, cfg)
			if dbErr != nil {
				return fmt.Errorf("search failed via HTTP (%w) and direct DB (%w)", httpErr, dbErr)
			}
			res = dbRes
		}
	} else {
		dbRes, dbErr := executeSearchDirectDB(ctx, cfg)
		if dbErr != nil {
			return fmt.Errorf("direct DB search failed: %w", dbErr)
		}
		res = dbRes
	}

	if cfg.JSON {
		b, err := jsonMarshalIndent(res, "", "  ")
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintln(out, string(b)); err != nil {
			return err
		}
	} else {
		if _, err := fmt.Fprint(out, formatSearchResults(res)); err != nil {
			return err
		}
	}

	return nil
}

func runMain(args []string, out, errOut io.Writer) int {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := runCLI(ctx, args, out); err != nil {
		_, _ = fmt.Fprintf(errOut, "Error: %v\n", err) //nolint:errcheck
		return 1
	}
	return 0
}

func main() {
	os.Exit(runMain(os.Args[1:], os.Stdout, os.Stderr))
}
