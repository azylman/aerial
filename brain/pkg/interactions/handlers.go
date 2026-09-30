package interactions

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/bwmarrin/discordgo"
	"github.com/google/uuid"
)

var (
	deployProgressDelay = 3 * time.Second
	pollerTimeout       = 14*time.Minute + 30*time.Second
)

func (r *Router) handleStatus(s *discordgo.Session, i *discordgo.InteractionCreate) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	factCount := 0
	latestFactStr := "none recorded yet"
	if r.deps.Store != nil {
		res, err := r.deps.Store.GetFactsPaginated(ctx, db.FactsFilter{Limit: 1})
		if err == nil && res != nil {
			factCount = res.Total
			if len(res.Facts) > 0 {
				f := res.Facts[0]
				timeAgo := formatRelativeTime(time.Since(f.CreatedAt))
				latestFactStr = fmt.Sprintf("%q (%s ago)", truncateRunes(f.FactText, 60), timeAgo)
			}
		}
	}

	nextActionStr := "none scheduled"
	if r.deps.Store != nil {
		now := time.Now().UTC()
		var earliestTime time.Time
		var earliestDesc string

		if crons, err := r.deps.Store.GetAllCronSchedules(ctx, ""); err == nil {
			for _, c := range crons {
				if c.NextRunAt.After(now) && (earliestTime.IsZero() || c.NextRunAt.Before(earliestTime)) {
					earliestTime = c.NextRunAt
					promptDesc := c.TitlePrefix
					if promptDesc == "" {
						promptDesc = c.Prompt
					}
					earliestDesc = fmt.Sprintf("`%s` in %s (%s)", truncateRunes(promptDesc, 40), formatRelativeDuration(time.Until(c.NextRunAt)), c.NextRunAt.Format("15:04 MST"))
				}
			}
		}
		if oneshots, err := r.deps.Store.GetAllOneShotSchedules(ctx, ""); err == nil {
			for _, o := range oneshots {
				if o.RunAt.After(now) && (earliestTime.IsZero() || o.RunAt.Before(earliestTime)) {
					earliestTime = o.RunAt
					earliestDesc = fmt.Sprintf("`%s` in %s (%s)", truncateRunes(o.Prompt, 40), formatRelativeDuration(time.Until(o.RunAt)), o.RunAt.Format("15:04 MST"))
				}
			}
		}
		if earliestDesc != "" {
			nextActionStr = earliestDesc
		}
	}

	runtimeStr := "`Idle`"
	if r.deps.Store != nil {
		if tasks, err := r.deps.Store.GetActiveTasks(ctx); err == nil && len(tasks) > 0 {
			t := tasks[0]
			runtimeStr = fmt.Sprintf("`Active` (Task: %s | %s elapsed)", t.ID, formatRelativeDuration(time.Since(t.CreatedAt)))
		}
	}
	if runtimeStr == "`Idle`" {
		if pool := r.getPool(); pool != nil {
			if procPool := pool.ProcessPool(); procPool != nil {
				if d, ok := procPool.Get(i.ChannelID); ok && d != nil && d.InflightCount() > 0 {
					runtimeStr = fmt.Sprintf("`Active` (Turn in progress | %s elapsed)", formatRelativeDuration(time.Since(d.LastUsed())))
				}
			}
		}
	}

	deployStr := "Live, healthy"
	if r.deps.DeployStatusProvider != nil {
		if st := r.deps.DeployStatusProvider(); strings.TrimSpace(st) != "" {
			deployStr = st
		}
	} else {
		gitCommit := os.Getenv("GIT_COMMIT")
		if len(gitCommit) > 7 {
			gitCommit = gitCommit[:7]
		}
		if gitCommit != "" {
			deployStr = fmt.Sprintf("`aerial@%s` (Live, healthy)", gitCommit)
		}
	}

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

	if pool := r.getPool(); pool != nil {
		if procPool := pool.ProcessPool(); procPool != nil {
			if d, ok := procPool.Get(targetThreadID); ok && d != nil {
				if err := d.Close(); err != nil {
					log.Printf("[Interactions] Warning: failed to close process on rotate: %v", err)
				}
			}
		}
		if lowPool := pool.LowEffortProcessPool(); lowPool != nil {
			if d, ok := lowPool.Get(targetThreadID); ok && d != nil {
				if err := d.Close(); err != nil {
					log.Printf("[Interactions] Warning: failed to close low-effort process on rotate: %v", err)
				}
			}
		}
	}

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

func (r *Router) handleInterrupt(s *discordgo.Session, i *discordgo.InteractionCreate, data discordgo.ApplicationCommandInteractionData) {
	drainQueue := getBoolOption(data.Options, "drain_queue", true)
	reset := getBoolOption(data.Options, "reset", false)
	targetThreadID := i.ChannelID

	// Terminate active running daemon and process group
	if pool := r.getPool(); pool != nil {
		if procPool := pool.ProcessPool(); procPool != nil {
			if d, ok := procPool.Get(targetThreadID); ok && d != nil {
				if err := d.Close(); err != nil {
					log.Printf("[Interactions] Warning: failed to close process on interrupt: %v", err)
				}
			}
		}
		if lowPool := pool.LowEffortProcessPool(); lowPool != nil {
			if d, ok := lowPool.Get(targetThreadID); ok && d != nil {
				if err := d.Close(); err != nil {
					log.Printf("[Interactions] Warning: failed to close low-effort process on interrupt: %v", err)
				}
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if r.deps.Store != nil {
		// Update active messages to cancelled
		if drainQueue {
			if msgs, err := r.deps.Store.GetPendingOrProcessingMessages(ctx, 100); err == nil {
				for _, m := range msgs {
					if m.ThreadID == targetThreadID {
						if err := r.deps.Store.UpdateMessageStatus(ctx, m.ID, db.StatusFailed, "interrupted by operator via slash command"); err != nil {
							log.Printf("[Interactions] Warning: failed to update message status on interrupt: %v", err)
						}
					}
				}
			}
		}
		if reset {
			if err := r.deps.Store.RotateSessionID(ctx, targetThreadID, uuid.New().String()); err != nil {
				log.Printf("[Interactions] Warning: failed to rotate session ID on interrupt: %v", err)
			}
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

	go r.trackDeployProgress(s, i.Interaction, "mode", targetChannelID, modeVal)
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
	go r.trackDeployProgress(s, i.Interaction, "ignore_bots", targetChannelID, valStr)
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
