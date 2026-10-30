package interactions

import (
	"fmt"
	"log"
	"sync"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/bwmarrin/discordgo"
)

// Router routes incoming Discord application command interactions to their respective handlers.
type Router struct {
	deps           InteractionDeps
	configCommitMu sync.Mutex
	bgWg           sync.WaitGroup
	seenMu         sync.Mutex
	seenIDs        map[string]struct{}
}

// NewRouter creates a new interaction Router with injected dependencies.
func NewRouter(deps InteractionDeps) *Router {
	return &Router{
		deps:    deps,
		seenIDs: make(map[string]struct{}),
	}
}

func (r *Router) getConfig() *config.Config {
	if r.deps.ConfigProvider != nil {
		if cfg := r.deps.ConfigProvider(); cfg != nil {
			return cfg
		}
	}
	return r.deps.Config
}

func (r *Router) getPool() *queue.WorkerPool {
	if r.deps.PoolProvider != nil {
		if pool := r.deps.PoolProvider(); pool != nil {
			return pool
		}
	}
	return r.deps.Pool
}

func (r *Router) claimInteraction(id string) bool {
	r.seenMu.Lock()
	defer r.seenMu.Unlock()
	if r.seenIDs == nil || len(r.seenIDs) >= 2048 {
		r.seenIDs = make(map[string]struct{})
	}
	if _, seen := r.seenIDs[id]; seen {
		return false
	}
	r.seenIDs[id] = struct{}{}
	return true
}

func extractCallerUser(i *discordgo.InteractionCreate) *discordgo.User {
	if i == nil {
		return nil
	}
	if i.Member != nil && i.Member.User != nil {
		return i.Member.User
	}
	return i.User
}

func (r *Router) dispatchCommand(s *discordgo.Session, i *discordgo.InteractionCreate, data discordgo.ApplicationCommandInteractionData) {
	switch data.Name {
	case "status":
		r.handleStatus(s, i)
	case "quota":
		r.handleQuota(s, i)
	case "rotate":
		r.handleRotate(s, i, data)
	case "interrupt":
		r.handleInterrupt(s, i, data)
	case "mode":
		r.handleMode(s, i, data)
	case "ignore_bots":
		r.handleIgnoreBots(s, i, data)
	default:
		if err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Content: fmt.Sprintf("⚠️ Unrecognized command: `/%s`", data.Name),
				Flags:   discordgo.MessageFlagsEphemeral,
			},
		}); err != nil {
			log.Printf("[Interactions] Failed to respond unrecognized command: %v", err)
		}
	}
}

// Handle processes incoming interaction events, enforces admin permissions, and dispatches to handlers.
func (r *Router) Handle(s *discordgo.Session, i *discordgo.InteractionCreate) {
	if s == nil || s.Ratelimiter == nil || i == nil || i.Interaction == nil || i.ID == "" {
		return
	}

	if !r.claimInteraction(i.ID) {
		return
	}

	if i.Type != discordgo.InteractionApplicationCommand {
		return
	}

	user := extractCallerUser(i)
	if user == nil {
		log.Printf("[Interactions] Could not identify caller user for interaction %s", i.ID)
		return
	}

	isAdmin := false
	if cfg := r.getConfig(); cfg != nil {
		isAdmin = cfg.IsAdmin(user.ID, user.Username, user.GlobalName)
	}

	if !isAdmin {
		if err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseChannelMessageWithSource,
			Data: &discordgo.InteractionResponseData{
				Content: "⛔ Unauthorized: Slash commands require administrator privileges.",
				Flags:   discordgo.MessageFlagsEphemeral,
			},
		}); err != nil {
			log.Printf("[Interactions] Failed to respond unauthorized: %v", err)
		}
		return
	}

	r.dispatchCommand(s, i, i.ApplicationCommandData())
}
