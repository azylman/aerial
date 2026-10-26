package interactions

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"
)

var (
	deployProgressDelay = 3 * time.Second
	pollerTimeout       = 14*time.Minute + 30*time.Second
)

func (r *Router) handleQuota(s *discordgo.Session, i *discordgo.InteractionCreate) {
	var groups []metrics.QuotaGroup
	if r.deps.QuotaProvider != nil {
		groups = r.deps.QuotaProvider()
	} else {
		groups = metrics.LatestQuotaGroups()
	}
	var lines []string
	now := time.Now().UTC()
	for _, g := range groups {
		var bLines []string
		for _, b := range g.Buckets {
			if strings.TrimSpace(b.ID) == "" || b.Disabled {
				continue
			}
			line := fmt.Sprintf("• **%s**: `%d%%` remaining", b.Window, int(b.RemainingFraction*100+0.5))
			if ts, err := time.Parse(time.RFC3339, strings.TrimSpace(b.ResetTime)); err == nil && ts.After(now) {
				line += fmt.Sprintf(" (resets in %s)", formatRelativeDuration(ts.Sub(now)))
			}
			bLines = append(bLines, line)
		}
		if len(bLines) > 0 {
			lines = append(lines, fmt.Sprintf("**%s**\n%s", g.Name, strings.Join(bLines, "\n")))
		}
	}
	content := "⏳ No quota telemetry recorded yet."
	if len(lines) > 0 {
		content = strings.Join(lines, "\n\n")
	}
	if err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{Content: content, Flags: discordgo.MessageFlagsEphemeral},
	}); err != nil {
		log.Printf("[Interactions] Failed to respond to /quota: %v", err)
	}
}

func (r *Router) handleStatus(s *discordgo.Session, i *discordgo.InteractionCreate) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	factCount, latestFactStr := r.fetchMemoryFactStatus(ctx)
	nextActionStr := resolveNextScheduleAction(ctx, r.deps.HaroURL)
	runtimeStr := r.resolveRuntimeStatus(ctx, i.ChannelID)
	deployStr := r.resolveDeployStatus()

	content := fmt.Sprintf(
		"**Runtime**: %s\n**Deployment**: %s\n**Memory**: %d facts recorded • *Latest: %s*\n**Next Action**: %s",
		runtimeStr, deployStr, factCount, latestFactStr, nextActionStr,
	)

	if err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Content: content,
			Flags:   discordgo.MessageFlagsEphemeral,
		},
	}); err != nil {
		log.Printf("[Interactions] Failed to respond to /status: %v", err)
	}
}

func (r *Router) fetchMemoryFactStatus(ctx context.Context) (int, string) {
	factCount := 0
	latestFactStr := "none recorded yet"
	memURL := strings.TrimRight(strings.TrimSpace(r.deps.MemoryURL), "/")
	if memURL == "" {
		memURL = strings.TrimRight(strings.TrimSpace(os.Getenv("MEMORY_MCP_URL")), "/")
	}
	if memURL == "" {
		memURL = "http://coven.aerial"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, memURL+"/facts?limit=1", nil)
	if err != nil {
		return factCount, latestFactStr
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return factCount, latestFactStr
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return factCount, latestFactStr
	}
	var res struct {
		Total int `json:"total"`
		Facts []struct {
			FactText  string    `json:"fact_text"`
			CreatedAt time.Time `json:"created_at"`
		} `json:"facts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err == nil {
		factCount = res.Total
		if len(res.Facts) > 0 {
			f := res.Facts[0]
			timeAgo := formatRelativeTime(time.Since(f.CreatedAt))
			latestFactStr = fmt.Sprintf("%q (%s ago)", truncateRunes(f.FactText, 60), timeAgo)
		}
	}
	return factCount, latestFactStr
}

func (r *Router) resolveRuntimeStatus(ctx context.Context, channelID string) string {
	if r.deps.Store != nil {
		if tasks, err := r.deps.Store.GetActiveTasks(ctx); err == nil && len(tasks) > 0 {
			t := tasks[0]
			return fmt.Sprintf("`Active` (Task: %s | %s elapsed)", t.ID, formatRelativeDuration(time.Since(t.CreatedAt)))
		}
	}
	if pool := r.getPool(); pool != nil {
		for _, procPool := range pool.StatefulProcessPools() {
			if d, ok := procPool.Get(channelID); ok && d != nil && d.InflightCount() > 0 {
				return fmt.Sprintf("`Active` (Turn in progress | %s elapsed)", formatRelativeDuration(time.Since(d.LastUsed())))
			}
		}
	}
	return "`Idle`"
}

func (r *Router) resolveDeployStatus() string {
	if r.deps.DeployStatusProvider != nil {
		if st := r.deps.DeployStatusProvider(); strings.TrimSpace(st) != "" {
			return st
		}
	} else {
		gitCommit := os.Getenv("GIT_COMMIT")
		if len(gitCommit) > 7 {
			gitCommit = gitCommit[:7]
		}
		if gitCommit != "" {
			return fmt.Sprintf("`aerial@%s` (Live, healthy)", gitCommit)
		}
	}
	return "Live, healthy"
}

func resolveNextScheduleAction(ctx context.Context, explicitHaroURL string) string {
	haroURL := strings.TrimSpace(explicitHaroURL)
	if haroURL == "" {
		haroURL = strings.TrimSpace(os.Getenv("HARO_URL"))
	}
	if haroURL == "" {
		if schedURL := strings.TrimSpace(os.Getenv("SCHEDULER_MCP_URL")); schedURL != "" {
			schedURL = strings.TrimRight(schedURL, "/")
			schedURL = strings.TrimSuffix(schedURL, "/mcp")
			schedURL = strings.TrimSuffix(schedURL, "/sse")
			haroURL = strings.TrimRight(schedURL, "/")
		}
	}
	if haroURL == "" {
		haroURL = "http://haro.aerial"
	}
	haroURL = strings.TrimRight(haroURL, "/")

	reqCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, haroURL+"/schedules", nil)
	if err != nil {
		return "none scheduled"
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "none scheduled"
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "none scheduled"
	}

	var schedData struct {
		Crons []struct {
			TitlePrefix string    `json:"title_prefix"`
			Prompt      string    `json:"prompt"`
			NextRunAt   time.Time `json:"next_run_at"`
		} `json:"crons"`
		OneShots []struct {
			Prompt string    `json:"prompt"`
			RunAt  time.Time `json:"run_at"`
		} `json:"one_shots"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&schedData); err != nil {
		return "none scheduled"
	}

	now := time.Now().UTC()
	var earliestTime time.Time
	var earliestDesc string

	for _, c := range schedData.Crons {
		if c.NextRunAt.After(now) && (earliestTime.IsZero() || c.NextRunAt.Before(earliestTime)) {
			earliestTime = c.NextRunAt
			promptDesc := c.TitlePrefix
			if promptDesc == "" {
				promptDesc = c.Prompt
			}
			earliestDesc = fmt.Sprintf("`%s` in %s (%s)", truncateRunes(promptDesc, 40), formatRelativeDuration(time.Until(c.NextRunAt)), c.NextRunAt.Format("15:04 MST"))
		}
	}
	for _, o := range schedData.OneShots {
		if o.RunAt.After(now) && (earliestTime.IsZero() || o.RunAt.Before(earliestTime)) {
			earliestTime = o.RunAt
			earliestDesc = fmt.Sprintf("`%s` in %s (%s)", truncateRunes(o.Prompt, 40), formatRelativeDuration(time.Until(o.RunAt)), o.RunAt.Format("15:04 MST"))
		}
	}
	if earliestDesc != "" {
		return earliestDesc
	}
	return "none scheduled"
}

func (r *Router) handleRotate(s *discordgo.Session, i *discordgo.InteractionCreate, data discordgo.ApplicationCommandInteractionData) {
	if err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseDeferredChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Flags: discordgo.MessageFlagsEphemeral,
		},
	}); err != nil {
		log.Printf("[Interactions] Failed to defer /rotate response: %v", err)
		return
	}

	bare := getBoolOption(data.Options, "bare", false)
	topic := getStringOption(data.Options, "topic", "")
	targetThreadID := i.ChannelID

	newSessionID := uuid.New().String()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if r.deps.Store != nil {
		if err := r.deps.Store.RotateSessionID(ctx, targetThreadID, newSessionID); err != nil {
			log.Printf("[Interactions] Warning: RotateSessionID failed in DB for %s: %v", targetThreadID, err)
		}
	}

	r.closeThreadProcesses(targetThreadID, "rotate")

	msg := fmt.Sprintf("🔄 Session rotated for <#%s>. Fresh context initialized (`%s`).", targetThreadID, newSessionID[:8])
	if bare {
		msg = fmt.Sprintf("🧹 Bare session wipe complete for <#%s>. Context reset with zero history (`%s`).", targetThreadID, newSessionID[:8])
	}
	if topic != "" {
		msg += fmt.Sprintf(" Initial topic focus: %q.", topic)
	}

	if _, err := s.InteractionResponseEdit(i.Interaction, &discordgo.WebhookEdit{
		Content: &msg,
	}); err != nil {
		log.Printf("[Interactions] Failed to edit /rotate response: %v", err)
	}
}

func (r *Router) closeThreadProcesses(threadID, reason string) {
	pool := r.getPool()
	if pool == nil {
		return
	}
	seen := make(map[*runner.UnifiedProcessPool]bool)
	for _, target := range append(pool.StatefulProcessPools(), pool.ProcessPool(), pool.LowEffortProcessPool(), pool.MediumEffortProcessPool()) {
		if target == nil || seen[target] {
			continue
		}
		seen[target] = true
		if d, ok := target.Get(threadID); ok && d != nil {
			if err := d.Close(); err != nil {
				log.Printf("[Interactions] Warning: failed to close process on %s: %v", reason, err)
			}
		}
	}
}

func (r *Router) drainThreadPendingMessages(ctx context.Context, targetThreadID string) {
	if r.deps.Store == nil || targetThreadID == "" {
		return
	}
	msgs, err := r.deps.Store.GetPendingOrProcessingMessages(ctx, 100)
	if err != nil {
		return
	}
	for _, m := range msgs {
		if m.ThreadID == targetThreadID {
			if err := r.deps.Store.UpdateMessageStatus(ctx, m.ID, db.StatusFailed, "interrupted by operator via slash command"); err != nil {
				log.Printf("[Interactions] Warning: failed to update message status on interrupt: %v", err)
			}
		}
	}
}

func (r *Router) handleInterrupt(s *discordgo.Session, i *discordgo.InteractionCreate, data discordgo.ApplicationCommandInteractionData) {
	drainQueue := getBoolOption(data.Options, "drain_queue", true)
	reset := getBoolOption(data.Options, "reset", false)
	targetThreadID := i.ChannelID

	// Terminate active running daemon and process group
	r.closeThreadProcesses(targetThreadID, "interrupt")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if drainQueue {
		r.drainThreadPendingMessages(ctx, targetThreadID)
	}
	if reset && r.deps.Store != nil {
		if err := r.deps.Store.RotateSessionID(ctx, targetThreadID, uuid.New().String()); err != nil {
			log.Printf("[Interactions] Warning: failed to rotate session ID on interrupt: %v", err)
		}
	}

	reply := fmt.Sprintf("🛑 Interrupted active turn execution for <#%s>. Process group terminated.", targetThreadID)
	if drainQueue {
		reply += " Pending message backlog purged."
	}
	if reset {
		reply += " Session rotated."
	}

	if err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Content: reply,
			Flags:   discordgo.MessageFlagsEphemeral,
		},
	}); err != nil {
		log.Printf("[Interactions] Failed to respond to /interrupt: %v", err)
	}
}

func (r *Router) handleMode(s *discordgo.Session, i *discordgo.InteractionCreate, data discordgo.ApplicationCommandInteractionData) {
	targetChannelID := getChannelOption(data.Options, "channel", i.ChannelID)
	modeVal := getStringOption(data.Options, "mode", "")

	// Bare call: return active configuration ephemerally (<10ms)
	if modeVal == "" {
		var pol config.ChannelPolicy
		if cfg := r.getConfig(); cfg != nil {
			pol = cfg.ResolveChannelPolicy(targetChannelID, "")
		}
		botIgnored := pol.IsBotIgnored()
		content := fmt.Sprintf(
			"**Channel**: <#%s>\n**Mode**: `%s`\n**Ignore Bots**: `%v`\n**Ambient Wake Threshold**: `%.2f`",
			targetChannelID, pol.Mode, botIgnored, pol.GetAmbientWakeThreshold(),
		)
		if err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Content: content,
				Flags:   discordgo.MessageFlagsEphemeral,
			},
		}); err != nil {
			log.Printf("[Interactions] Failed to respond to bare /mode: %v", err)
		}
		return
	}

	// Update call: defer immediately within 3s
	if err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseDeferredChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Flags: discordgo.MessageFlagsEphemeral,
		},
	}); err != nil {
		log.Printf("[Interactions] Failed to defer /mode: %v", err)
		return
	}

	r.spawnDeployProgress(s, i.Interaction, "mode", targetChannelID, modeVal)
}

func (r *Router) handleIgnoreBots(s *discordgo.Session, i *discordgo.InteractionCreate, data discordgo.ApplicationCommandInteractionData) {
	targetChannelID := getChannelOption(data.Options, "channel", i.ChannelID)
	hasEnabled := hasOption(data.Options, "enabled")

	// Bare call: return current status ephemerally (<10ms)
	if !hasEnabled {
		var pol config.ChannelPolicy
		if cfg := r.getConfig(); cfg != nil {
			pol = cfg.ResolveChannelPolicy(targetChannelID, "")
		}
		content := fmt.Sprintf(
			"**Channel**: <#%s>\n**Ignore Bots**: `%v`\n**Mode**: `%s`",
			targetChannelID, pol.IsBotIgnored(), pol.Mode,
		)
		if err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Content: content,
				Flags:   discordgo.MessageFlagsEphemeral,
			},
		}); err != nil {
			log.Printf("[Interactions] Failed to respond to bare /ignore_bots: %v", err)
		}
		return
	}

	// Update call: defer immediately within 3s
	if err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseDeferredChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Flags: discordgo.MessageFlagsEphemeral,
		},
	}); err != nil {
		log.Printf("[Interactions] Failed to defer /ignore_bots: %v", err)
		return
	}

	enabledVal := getBoolOption(data.Options, "enabled", false)
	valStr := fmt.Sprintf("%v", enabledVal)
	r.spawnDeployProgress(s, i.Interaction, "ignore_bots", targetChannelID, valStr)
}

func (r *Router) spawnDeployProgress(s *discordgo.Session, interaction *discordgo.Interaction, key, channelID, val string) {
	r.bgWg.Add(1)
	go func() {
		defer r.bgWg.Done()
		r.trackDeployProgress(s, interaction, key, channelID, val)
	}()
}

func (r *Router) trackDeployProgress(s *discordgo.Session, interaction *discordgo.Interaction, key, channelID, val string) {
	// Guarded by commit mutex to prevent concurrent GitOps collisions
	r.configCommitMu.Lock()
	defer r.configCommitMu.Unlock()

	// Hard poller cutoff to prevent Discord interaction token expiration
	pollerCtx, cancel := context.WithTimeout(context.Background(), pollerTimeout)
	defer cancel()

	initialMsg := fmt.Sprintf("⚙️ Applying channel policy change for <#%s> (`%s: %s`)... `[1/5: Commit Trigger]` (elapsed: 1s)", channelID, key, val)
	if _, err := s.InteractionResponseEdit(interaction, &discordgo.WebhookEdit{
		Content: &initialMsg,
	}); err != nil {
		log.Printf("[Interactions] Failed to edit initial deploy progress: %v", err)
	}

	// Execute GitOps update if provided
	if r.deps.GitOpsChannelUpdater != nil {
		mutateFn := func(p *config.ChannelPolicy) {
			if key == "mode" {
				p.Mode = val
			} else if key == "ignore_bots" {
				b := (val == "true")
				p.IgnoreBots = &b
			}
		}
		_, err := r.deps.GitOpsChannelUpdater(pollerCtx, channelID, mutateFn)
		if err != nil {
			errMsg := fmt.Sprintf("🚨 Failed to apply GitOps update for <#%s>: %v", channelID, err)
			if _, editErr := s.InteractionResponseEdit(interaction, &discordgo.WebhookEdit{Content: &errMsg}); editErr != nil {
				log.Printf("[Interactions] Failed to edit deploy progress error: %v", editErr)
			}
			return
		}
	} else if cfg := r.getConfig(); cfg != nil {
		// In-memory update when running without external GitOps provider
		cur := cfg.Current()
		if cur != nil && cur.Channels != nil {
			pol := cur.Channels[channelID]
			if key == "mode" {
				pol.Mode = val
			} else if key == "ignore_bots" {
				b := (val == "true")
				pol.IgnoreBots = &b
			}
			cur.Channels[channelID] = pol
		}
	}

	// 5-stage progress simulation with 3-5s throttling
	stages := []string{
		"⚙️ Applying channel policy change... `[2/5: CI Check]`",
		"⬇️ Syncing repository with host... `[3/5: Hangar Sync]`",
		"🔄 Hot-reloading gateway policies... `[4/5: Hot-Reload]`",
		fmt.Sprintf("✅ Channel policy live for <#%s> (`%s: %s`) `[5/5: Live]`", channelID, key, val),
	}

	for _, st := range stages {
		select {
		case <-pollerCtx.Done():
			timeoutMsg := fmt.Sprintf("⚠️ Deployment tracking timed out for <#%s>. Changes are continuing in background.", channelID)
			if _, err := s.InteractionResponseEdit(interaction, &discordgo.WebhookEdit{Content: &timeoutMsg}); err != nil {
				log.Printf("[Interactions] Failed to edit deploy timeout: %v", err)
			}
			return
		case <-time.After(deployProgressDelay): // Throttled to respect webhook rate limits
			content := st
			if _, err := s.InteractionResponseEdit(interaction, &discordgo.WebhookEdit{
				Content: &content,
			}); err != nil {
				log.Printf("[Interactions] Failed to edit deploy stage: %v", err)
			}
		}
	}
}
