package interactions

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/delivery"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/queue"
)

// noScheduleAction is shown when no upcoming schedule can be resolved.
const noScheduleAction = "none scheduled"

var (
	deployProgressDelay = 3 * time.Second
	pollerTimeout       = 14*time.Minute + 30*time.Second
)

func (r *Router) handleQuota(s *discordgo.Session, i *discordgo.InteractionCreate) {
	var snapshot metrics.QuotaSnapshot
	if r.deps.QuotaProvider != nil {
		snapshot = r.deps.QuotaProvider()
	} else {
		snapshot = metrics.LatestQuotaSnapshot()
	}
	groups, updatedAt := snapshot.Groups, snapshot.UpdatedAt
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
	if len(lines) > 0 && !updatedAt.IsZero() {
		lines = append(lines, fmt.Sprintf("*Last refreshed: %s ago (<t:%d:t>)*", formatRelativeDuration(now.Sub(updatedAt)), updatedAt.Unix()))
	}
	content := "⏳ No quota telemetry recorded yet."
	if len(lines) > 0 {
		content = strings.Join(lines, "\n\n")
	}
	if err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{Content: delivery.RedactOutbound(content), Flags: discordgo.MessageFlagsEphemeral},
	}); err != nil {
		log.Printf("[Interactions] Failed to respond to /quota: %v", err)
	}
}

func (r *Router) handleStatus(ctx context.Context, s *discordgo.Session, i *discordgo.InteractionCreate) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	factCount, latestFactStr := r.fetchMemoryFactStatus(ctx)
	nextActionStr := r.resolveNextScheduleAction(ctx)
	runtimeStr := r.resolveRuntimeStatus(ctx, i.ChannelID)
	deployStr := r.resolveDeployStatus()

	content := fmt.Sprintf(
		"**Runtime**: %s\n**Deployment**: %s\n**Memory**: %d facts recorded • *Latest: %s*\n**Next Action**: %s",
		runtimeStr, deployStr, factCount, latestFactStr, nextActionStr,
	)
	content = delivery.RedactOutbound(content)

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
	if memURL == "" && r.getConfig() != nil && r.getConfig().Current() != nil {
		memURL = strings.TrimRight(strings.TrimSpace(r.getConfig().Current().CovenURL), "/")
	}
	if memURL == "" {
		return factCount, latestFactStr
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, memURL+"/facts?limit=1", http.NoBody)
	if err != nil {
		return factCount, latestFactStr
	}
	client := r.memoryClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
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
			if d, ok := procPool.Get(db.DiscordSessionID(channelID)); ok && d != nil && d.InflightCount() > 0 {
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

func (r *Router) resolveNextScheduleAction(ctx context.Context) string {
	haroURL := strings.TrimRight(strings.TrimSpace(r.deps.HaroURL), "/")
	if haroURL == "" && r.getConfig() != nil && r.getConfig().Current() != nil {
		haroURL = strings.TrimRight(strings.TrimSpace(r.getConfig().Current().HaroURL), "/")
	}
	return resolveNextScheduleAction(ctx, haroURL)
}

func resolveNextScheduleAction(ctx context.Context, explicitHaroURL string) string {
	haroURL := strings.TrimRight(strings.TrimSpace(explicitHaroURL), "/")
	if haroURL == "" {
		return noScheduleAction
	}

	reqCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, haroURL+"/schedules", http.NoBody)
	if err != nil {
		return noScheduleAction
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return noScheduleAction
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return noScheduleAction
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
		return noScheduleAction
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
	return noScheduleAction
}

func (r *Router) handleInterrupt(ctx context.Context, s *discordgo.Session, i *discordgo.InteractionCreate, _ discordgo.ApplicationCommandInteractionData) {
	targetThreadID := i.ChannelID

	// Defer first: Discord's 3s window is shorter than a Restate round-trip can take.
	if err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseDeferredChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Flags: discordgo.MessageFlagsEphemeral,
		},
	}); err != nil {
		log.Printf("[Interactions] Failed to defer /interrupt: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	reply := r.cancelThreadTurn(ctx, targetThreadID)
	if _, err := s.InteractionResponseEdit(i.Interaction, &discordgo.WebhookEdit{Content: &reply}); err != nil {
		log.Printf("[Interactions] Failed to edit /interrupt response: %v", err)
	}
}

func (r *Router) cancelThreadTurn(ctx context.Context, threadID string) string {
	if r.deps.TurnCancellerProvider == nil {
		return "interrupt unavailable"
	}
	canceller := r.deps.TurnCancellerProvider()
	if canceller == nil {
		return "interrupt unavailable"
	}
	resp, err := canceller.CancelThreadTurn(ctx, threadID, queue.CancelTurnRequest{Reason: "cancelled by operator via /interrupt"})
	if err != nil {
		return fmt.Sprintf("Interrupt failed: %v", err)
	}
	return formatInterruptReply(threadID, resp)
}

func formatInterruptReply(threadID string, resp queue.CancelTurnResponse) string {
	switch {
	case resp.CancelledActiveTurn && resp.CancelledCount > 0:
		return fmt.Sprintf("🛑 Interrupted the active turn in <#%s> and dropped %d queued messages", threadID, resp.CancelledCount)
	case resp.CancelledActiveTurn:
		return fmt.Sprintf("🛑 Interrupted the active turn in <#%s>", threadID)
	case resp.CancelledCount > 0:
		return fmt.Sprintf("No active turn in <#%s>; dropped %d queued messages.", threadID, resp.CancelledCount)
	default:
		return fmt.Sprintf("Nothing was running in <#%s>.", threadID)
	}
}

func (r *Router) handleMode(ctx context.Context, s *discordgo.Session, i *discordgo.InteractionCreate, data discordgo.ApplicationCommandInteractionData) {
	targetChannelID := getChannelOption(data.Options, "channel", i.ChannelID)
	modeVal := getStringOption(data.Options, optionMode, "")

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

	r.spawnDeployProgress(ctx, s, i.Interaction, optionMode, targetChannelID, modeVal)
}

func (r *Router) handleIgnoreBots(ctx context.Context, s *discordgo.Session, i *discordgo.InteractionCreate, data discordgo.ApplicationCommandInteractionData) {
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
	valStr := strconv.FormatBool(enabledVal)
	r.spawnDeployProgress(ctx, s, i.Interaction, optionIgnoreBots, targetChannelID, valStr)
}

func (r *Router) spawnDeployProgress(ctx context.Context, s *discordgo.Session, interaction *discordgo.Interaction, key, channelID, val string) {
	r.bgWg.Go(func() {
		r.trackDeployProgress(ctx, s, interaction, key, channelID, val)
	})
}

func (r *Router) trackDeployProgress(ctx context.Context, s *discordgo.Session, interaction *discordgo.Interaction, key, channelID, val string) {
	// Guarded by commit mutex to prevent concurrent GitOps collisions
	r.configCommitMu.Lock()
	defer r.configCommitMu.Unlock()

	// Hard poller cutoff to prevent Discord interaction token expiration
	pollerCtx, cancel := context.WithTimeout(ctx, pollerTimeout)
	defer cancel()

	initialMsg := fmt.Sprintf("⚙️ Applying channel policy change for <#%s> (`%s: %s`)... `[1/5: Commit Trigger]` (elapsed: 1s)", channelID, key, val)
	if _, err := s.InteractionResponseEdit(interaction, &discordgo.WebhookEdit{
		Content: &initialMsg,
	}); err != nil {
		log.Printf("[Interactions] Failed to edit initial deploy progress: %v", err)
	}

	if err := r.applyChannelPolicyUpdate(pollerCtx, channelID, key, val); err != nil {
		errMsg := delivery.RedactOutbound(fmt.Sprintf("🚨 Failed to apply GitOps update for <#%s>: %v", channelID, err))
		if _, editErr := s.InteractionResponseEdit(interaction, &discordgo.WebhookEdit{Content: &errMsg}); editErr != nil {
			log.Printf("[Interactions] Failed to edit deploy progress error: %v", editErr)
		}
		return
	}

	// 5-stage progress simulation with 3-5s throttling
	stages := []string{
		"⚙️ Applying channel policy change... `[2/5: CI Check]`",
		"⬇️ Syncing repository with host... `[3/5: Flux Sync]`",
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

func (r *Router) applyChannelPolicyUpdate(ctx context.Context, channelID, key, val string) error {
	if r.deps.GitOpsChannelUpdater != nil {
		mutateFn := func(p *config.ChannelPolicy) {
			switch key {
			case optionMode:
				p.Mode = val
			case optionIgnoreBots:
				b := (val == "true")
				p.IgnoreBots = &b
			}
		}
		_, err := r.deps.GitOpsChannelUpdater(ctx, channelID, mutateFn)
		return err
	}

	cfg := r.getConfig()
	if cfg == nil {
		return nil
	}
	cur := cfg.Current()
	if cur != nil && cur.Channels != nil {
		pol := cur.Channels[channelID]
		switch key {
		case optionMode:
			pol.Mode = val
		case optionIgnoreBots:
			b := (val == "true")
			pol.IgnoreBots = &b
		}
		cur.Channels[channelID] = pol
	}
	return nil
}
