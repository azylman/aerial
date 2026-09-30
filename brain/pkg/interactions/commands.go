package interactions

import (
	"fmt"
	"log"
	"sync"

	"github.com/bwmarrin/discordgo"
)

var registeredGuilds sync.Map

// CommandDefinitions returns the canonical slice of Aerial's Discord slash commands.
func CommandDefinitions() []*discordgo.ApplicationCommand {
	return []*discordgo.ApplicationCommand{
		{
			Name:        "status",
			Description: "Operator HUD snapshot of runtime state, deployment, facts, and upcoming schedules",
		},
		{
			Name:        "rotate",
			Description: "Rotate session lifecycle and start a fresh context window",
			Options: []*discordgo.ApplicationCommandOption{
				{
					Type:        discordgo.ApplicationCommandOptionBoolean,
					Name:        "bare",
					Description: "Clean wipe with zero history or summaries carried over (default false)",
					Required:    false,
				},
				{
					Type:        discordgo.ApplicationCommandOptionString,
					Name:        "topic",
					Description: "Seed the fresh session with an explicit objective or topic",
					Required:    false,
				},
			},
		},
		{
			Name:        "interrupt",
			Description: "Interrupt active turn execution, terminate process group, and clear queue",
			Options: []*discordgo.ApplicationCommandOption{
				{
					Type:        discordgo.ApplicationCommandOptionBoolean,
					Name:        "drain_queue",
					Description: "Purge pending unhandled messages for this thread in queue (default true)",
					Required:    false,
				},
				{
					Type:        discordgo.ApplicationCommandOptionBoolean,
					Name:        "reset",
					Description: "Rotate session immediately after aborting turn (default false)",
					Required:    false,
				},
			},
		},
		{
			Name:        "mode",
			Description: "Inspect or reconfigure channel wake mode",
			Options: []*discordgo.ApplicationCommandOption{
				{
					Type:        discordgo.ApplicationCommandOptionString,
					Name:        "mode",
					Description: "Operational mode for the channel",
					Required:    false,
					Choices: []*discordgo.ApplicationCommandOptionChoice{
						{Name: "thread", Value: "thread"},
						{Name: "classifier", Value: "classifier"},
						{Name: "mention", Value: "mention"},
						{Name: "ignore", Value: "ignore"},
					},
				},
				{
					Type:        discordgo.ApplicationCommandOptionChannel,
					Name:        "channel",
					Description: "Target channel (defaults to current channel)",
					Required:    false,
				},
			},
		},
		{
			Name:        "ignore_bots",
			Description: "Toggle peer bot message gating for a channel",
			Options: []*discordgo.ApplicationCommandOption{
				{
					Type:        discordgo.ApplicationCommandOptionBoolean,
					Name:        "enabled",
					Description: "Ignore peer bot messages in target channel",
					Required:    false,
				},
				{
					Type:        discordgo.ApplicationCommandOptionChannel,
					Name:        "channel",
					Description: "Target channel (defaults to current channel)",
					Required:    false,
				},
			},
		},
	}
}

// RegisterGuildCommands registers or bulk-overwrites slash commands for a specific guild.
func RegisterGuildCommands(s *discordgo.Session, appID, guildID string) ([]*discordgo.ApplicationCommand, error) {
	if s == nil || s.Ratelimiter == nil {
		return nil, fmt.Errorf("nil or uninitialized discord session")
	}
	if appID == "" {
		if s.State != nil && s.State.User != nil && s.State.User.ID != "" {
			appID = s.State.User.ID
		} else {
			return nil, fmt.Errorf("empty appID and session state user is empty")
		}
	}
	if guildID == "" {
		return nil, fmt.Errorf("empty guildID")
	}

	cmds := CommandDefinitions()
	created, err := s.ApplicationCommandBulkOverwrite(appID, guildID, cmds)
	if err != nil {
		log.Printf("[Interactions] Failed to bulk overwrite commands for guild %s: %v", guildID, err)
		return nil, err
	}
	log.Printf("[Interactions] Registered %d slash commands for guild %s", len(created), guildID)
	return created, nil
}

// RegisterGuildCommandsOnce registers slash commands for a guild only once per process lifecycle
// unless reset or registration failed.
func RegisterGuildCommandsOnce(s *discordgo.Session, appID, guildID string) ([]*discordgo.ApplicationCommand, error) {
	if _, loaded := registeredGuilds.LoadOrStore(guildID, true); loaded {
		return nil, nil
	}
	cmds, err := RegisterGuildCommands(s, appID, guildID)
	if err != nil {
		registeredGuilds.Delete(guildID)
		return nil, err
	}
	return cmds, nil
}

// ResetRegisteredGuilds clears cached guild registrations.
func ResetRegisteredGuilds() {
	registeredGuilds.Range(func(key, value any) bool {
		registeredGuilds.Delete(key)
		return true
	})
}
