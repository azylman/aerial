package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"log"
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
		Help: "Remaining quota fraction (0.0 - 1.0) reported by agy /usage per model group and window.",
	}, []string{"bucket_id", "group", "window", "disabled"})
	UsageQuotaResetTimestampSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "aerial_brain_usage_quota_reset_timestamp_seconds",
		Help: "Unix timestamp in seconds when the agy /usage quota window resets.",
	}, []string{"bucket_id", "group", "window"})
	latestQuotaMu     sync.RWMutex
	latestQuotaGroups []QuotaGroup
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

func RecordUsageJSON(raw []byte) error {
	var resp usageCLIResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return err
	}
	if len(resp.Command.Data.Groups) == 0 {
		return errors.New("usage payload contains no quota groups")
	}
	latestQuotaMu.Lock()
	latestQuotaGroups = resp.Command.Data.Groups
	latestQuotaMu.Unlock()
	UsageQuotaRemainingFraction.Reset()
	UsageQuotaResetTimestampSeconds.Reset()
	for _, g := range resp.Command.Data.Groups {
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
	return nil
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

func StartUsagePoller(ctx context.Context, interval time.Duration, run UsageRunnerFunc) {
	if run == nil || interval <= 0 {
		return
	}
	go func() {
		poll := func() {
			if out, err := run(ctx); err != nil {
				log.Printf("[UsagePoller] Warning: agy /usage execution failed: %v", err)
			} else if err := RecordUsageJSON(out); err != nil {
				log.Printf("[UsagePoller] Warning: failed to parse agy /usage output: %v", err)
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
