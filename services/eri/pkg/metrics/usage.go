package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	UsageQuotaRemainingFraction = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "aerial_brain_usage_quota_remaining_fraction",
		Help: "Remaining quota fraction (0.0 - 1.0) reported by the engine usage poller per model group and window.",
	}, []string{"bucket_id", "group", "window", "disabled"})
	UsageQuotaResetTimestampSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "aerial_brain_usage_quota_reset_timestamp_seconds",
		Help: "Unix timestamp in seconds when the engine usage quota window resets.",
	}, []string{"bucket_id", "group", "window"})
	latestQuotaMu        sync.RWMutex
	latestQuotaGroups    []QuotaGroup
	latestQuotaUpdatedAt time.Time
)

func init() {
	Registry.MustRegister(UsageQuotaRemainingFraction, UsageQuotaResetTimestampSeconds)
}

type QuotaBucket struct {
	ID                string  `json:"id"`
	Window            string  `json:"window"`
	ResetTime         string  `json:"reset_time"`
	RemainingFraction float64 `json:"remaining_fraction"`
	Disabled          bool    `json:"disabled"`
}

type QuotaGroup struct {
	Name    string        `json:"name"`
	Buckets []QuotaBucket `json:"buckets"`
}

type usageCLIResponse struct {
	Command struct {
		Data struct {
			Groups []QuotaGroup `json:"groups"`
		} `json:"data"`
	} `json:"command"`
}
type UsageRunnerFunc func(ctx context.Context) ([]byte, error)

func LatestQuotaGroups() []QuotaGroup {
	latestQuotaMu.RLock()
	defer latestQuotaMu.RUnlock()
	out := make([]QuotaGroup, len(latestQuotaGroups))
	for i, g := range latestQuotaGroups {
		out[i] = QuotaGroup{Name: g.Name, Buckets: append([]QuotaBucket(nil), g.Buckets...)}
	}
	return out
}

func LatestQuotaUpdatedAt() time.Time {
	latestQuotaMu.RLock()
	defer latestQuotaMu.RUnlock()
	return latestQuotaUpdatedAt
}

func RecordUsageJSON(raw []byte) error {
	var resp usageCLIResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return err
	}
	if len(resp.Command.Data.Groups) == 0 {
		return errors.New("usage payload contains no quota groups")
	}
	recordQuotaGroups(resp.Command.Data.Groups)
	return nil
}

func recordQuotaGroups(groups []QuotaGroup) {
	latestQuotaMu.Lock()
	latestQuotaGroups = groups
	latestQuotaUpdatedAt = time.Now().UTC()
	latestQuotaMu.Unlock()
	UsageQuotaRemainingFraction.Reset()
	UsageQuotaResetTimestampSeconds.Reset()
	for _, g := range groups {
		for _, b := range g.Buckets {
			if strings.TrimSpace(b.ID) == "" {
				continue
			}
			UsageQuotaRemainingFraction.WithLabelValues(b.ID, g.Name, b.Window, strconv.FormatBool(b.Disabled)).Set(b.RemainingFraction)
			if ts, err := time.Parse(time.RFC3339, strings.TrimSpace(b.ResetTime)); err == nil {
				UsageQuotaResetTimestampSeconds.WithLabelValues(b.ID, g.Name, b.Window).Set(float64(ts.Unix()))
			}
		}
	}
}

// ClaudeUsageBaseURL is the Anthropic API origin serving the OAuth usage endpoint.
const ClaudeUsageBaseURL = "https://api.anthropic.com"

const claudeUsageMaxBody = 1 << 20

type claudeUsageWindow struct {
	Utilization *float64 `json:"utilization"`
	ResetsAt    string   `json:"resets_at"`
}

type claudeUsageResponse struct {
	FiveHour       *claudeUsageWindow `json:"five_hour"`
	SevenDay       *claudeUsageWindow `json:"seven_day"`
	SevenDayOpus   *claudeUsageWindow `json:"seven_day_opus"`
	SevenDaySonnet *claudeUsageWindow `json:"seven_day_sonnet"`
}

// ClaudeUsageToGroups maps an Anthropic OAuth usage payload onto the shared QuotaGroup contract.
func ClaudeUsageToGroups(raw []byte) ([]QuotaGroup, error) {
	var resp claudeUsageResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, err
	}
	windows := []struct {
		id, window string
		w          *claudeUsageWindow
	}{
		{"claude-five-hour", "5h", resp.FiveHour},
		{"claude-seven-day", "weekly", resp.SevenDay},
		{"claude-seven-day-opus", "weekly-opus", resp.SevenDayOpus},
		{"claude-seven-day-sonnet", "weekly-sonnet", resp.SevenDaySonnet},
	}
	var buckets []QuotaBucket
	for _, win := range windows {
		if win.w == nil || win.w.Utilization == nil {
			continue
		}
		reset := ""
		if ts, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(win.w.ResetsAt)); err == nil {
			reset = ts.UTC().Format(time.RFC3339)
		}
		buckets = append(buckets, QuotaBucket{
			ID:                win.id,
			Window:            win.window,
			ResetTime:         reset,
			RemainingFraction: min(max((100-*win.w.Utilization)/100, 0), 1),
		})
	}
	if len(buckets) == 0 {
		return nil, errors.New("claude usage payload contains no quota windows")
	}
	return []QuotaGroup{{Name: "Claude", Buckets: buckets}}, nil
}

// RecordClaudeUsageJSON parses an Anthropic OAuth usage payload and publishes it as quota telemetry.
func RecordClaudeUsageJSON(raw []byte) error {
	groups, err := ClaudeUsageToGroups(raw)
	if err != nil {
		return err
	}
	recordQuotaGroups(groups)
	return nil
}

// DefaultClaudeUsageRunner returns nil (polling disabled) under tests or without a token.
func DefaultClaudeUsageRunner(token string, isTest bool) UsageRunnerFunc {
	if isTest || strings.TrimSpace(token) == "" {
		return nil
	}
	return NewClaudeUsageRunner(http.DefaultClient, ClaudeUsageBaseURL, token)
}

// NewClaudeUsageRunner fetches the Anthropic OAuth usage endpoint. Errors never include the token.
func NewClaudeUsageRunner(client *http.Client, baseURL, token string) UsageRunnerFunc {
	endpoint := strings.TrimRight(baseURL, "/") + "/api/oauth/usage"
	auth := "Bearer " + strings.TrimSpace(token)
	return func(ctx context.Context) ([]byte, error) {
		runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(runCtx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", auth)
		req.Header.Set("Anthropic-Beta", "oauth-2025-04-20")
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return nil, fmt.Errorf("claude usage endpoint returned HTTP %d", resp.StatusCode)
		}
		return io.ReadAll(io.LimitReader(resp.Body, claudeUsageMaxBody))
	}
}

func DefaultUsageRunner(agyBin, homeDir string, baseEnv []string, isTest bool) UsageRunnerFunc {
	if isTest {
		return nil
	}
	return NewAgyUsageRunner(agyBin, homeDir, baseEnv)
}

func NewAgyUsageRunner(agyBin, homeDir string, baseEnv []string) UsageRunnerFunc {
	bin := strings.TrimSpace(agyBin)
	if bin == "" {
		bin = "agy"
	}
	var env []string
	for _, e := range baseEnv {
		if !strings.HasPrefix(e, "HOME=") {
			env = append(env, e)
		}
	}
	if strings.TrimSpace(homeDir) != "" {
		env = append(env, "HOME="+strings.TrimSpace(homeDir))
	}
	return func(ctx context.Context) ([]byte, error) {
		runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(runCtx, bin, "--print", "/usage", "--output-format", "json")
		cmd.Env = env
		return cmd.Output()
	}
}

func StartUsagePoller(ctx context.Context, interval time.Duration, run UsageRunnerFunc, record func([]byte) error) {
	if run == nil || record == nil || interval <= 0 {
		return
	}
	go func() {
		poll := func() {
			if out, err := run(ctx); err != nil {
				log.Printf("[UsagePoller] Warning: usage poll failed: %v", err)
			} else if err := record(out); err != nil {
				log.Printf("[UsagePoller] Warning: failed to parse usage output: %v", err)
			}
		}
		poll()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				poll()
			}
		}
	}()
}
