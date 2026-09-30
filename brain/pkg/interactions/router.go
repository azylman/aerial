package interactions

import (
	"fmt"
	"log"
	"sync"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/queue"
	"github.com/bwmarrin/discordgo"
)

// Router routes incoming Discord application command interactions to their respective handlers.
type Router struct {
	deps           InteractionDeps
	configCommitMu sync.Mutex
}

// NewRouter creates a new interaction Router with injected dependencies.
func NewRouter(deps InteractionDeps) *Router {
	return &Router{
		deps: deps,
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

// Handle processes incoming interaction events, enforces admin permissions, and dispatches to handlers.
func (r *Router) Handle(s *discordgo.Session, i *discordgo.InteractionCreate) {
	if s == nil || s.Ratelimiter == nil || i == nil || i.Interaction == nil {
		return
	}
	if i.Type != discordgo.InteractionApplicationCommand {
		return
	}

	var user *discordgo.User
	if i.Member != nil && i.Member.User != nil {
		user = i.Member.User
	} else if i.User != nil {
		user = i.User
	}
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

	data := i.ApplicationCommandData()
	switch data.Name {
	case "status":
		r.handleStatus(s, i)
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
