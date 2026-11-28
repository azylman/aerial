package discord

import (
	"context"
	"log"
	"time"

	"github.com/bwmarrin/discordgo"
)

// HydrateOptions configures Discord REST session hydration and retry behavior.
type HydrateOptions struct {
	OnChannel     func(*discordgo.Channel)
	OnGuild       func(*discordgo.Session, string, string)
	MemberFetcher func(*discordgo.Session, string, string) (*discordgo.Member, error)
	HydrateFunc   func(context.Context, *discordgo.Session) error
	RetryBackoff  time.Duration
	LogPrefix     string
}

func withOptionalContext(ctx context.Context) []discordgo.RequestOption {
	if ctx == nil {
		return nil
	}
	return []discordgo.RequestOption{discordgo.WithContext(ctx)}
}

func hydrateGuildRoles(ctx context.Context, dg *discordgo.Session, guildID, prefix string) {
	roles, err := dg.GuildRoles(guildID, withOptionalContext(ctx)...)
	if err != nil {
		return
	}
	for _, role := range roles {
		if role == nil {
			continue
		}
		if err := dg.State.RoleAdd(guildID, role); err != nil {
			log.Printf("%s[WARN] Failed to add role %s to session state: %v", prefix, role.ID, err)
		}
	}
}

func hydrateGuildChannels(ctx context.Context, dg *discordgo.Session, guildID string, onChannel func(*discordgo.Channel), prefix string) {
	channels, err := dg.GuildChannels(guildID, withOptionalContext(ctx)...)
	if err != nil {
		return
	}
	for _, ch := range channels {
		if ch == nil {
			continue
		}
		ch.GuildID = guildID
		if onChannel != nil {
			onChannel(ch)
		}
		if err := dg.State.ChannelAdd(ch); err != nil {
			log.Printf("%s[WARN] Failed to add channel %s to session state: %v", prefix, ch.ID, err)
		}
	}
}

func hydrateGuildThreads(ctx context.Context, dg *discordgo.Session, guildID string, onChannel func(*discordgo.Channel), prefix string) {
	activeThreads, err := dg.GuildThreadsActive(guildID, withOptionalContext(ctx)...)
	if err != nil || activeThreads == nil {
		return
	}
	for _, th := range activeThreads.Threads {
		if th == nil {
			continue
		}
		th.GuildID = guildID
		if onChannel != nil {
			onChannel(th)
		}
		if err := dg.State.ChannelAdd(th); err != nil {
			log.Printf("%s[WARN] Failed to add thread %s to session state: %v", prefix, th.ID, err)
		}
	}
}

// SyncBotMemberRoles fetches and caches the bot's guild member roles into dg.State
// if they are not already present.
func SyncBotMemberRoles(ctx context.Context, dg *discordgo.Session, guildID, botID string, opts HydrateOptions) {
	if dg == nil || dg.State == nil || guildID == "" || botID == "" || (ctx != nil && ctx.Err() != nil) {
		return
	}
	if m, err := dg.State.Member(guildID, botID); err == nil && m != nil {
		return
	}
	var m *discordgo.Member
	var err error
	if opts.MemberFetcher != nil {
		m, err = opts.MemberFetcher(dg, guildID, botID)
	} else {
		m, err = dg.GuildMember(guildID, botID, withOptionalContext(ctx)...)
	}
	if err != nil || m == nil {
		log.Printf("%sWarning: failed to fetch bot member roles for guild %s: %v", opts.LogPrefix, guildID, err)
		return
	}
	if m.GuildID == "" {
		m.GuildID = guildID
	}
	if m.User != nil {
		if err := dg.State.MemberAdd(m); err != nil {
			log.Printf("%s[WARN] Failed to add bot member %s to session state: %v", opts.LogPrefix, botID, err)
		}
	}
	log.Printf("%sCached %d bot role(s) for guild %s", opts.LogPrefix, len(m.Roles), guildID)
}

// HydrateSessionREST populates dg.State with the bot user, guilds, roles,
// channels, active threads, and bot member roles via Discord REST.
func HydrateSessionREST(ctx context.Context, dg *discordgo.Session, opts HydrateOptions) error {
	if dg == nil || dg.Token == "" {
		return nil
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	u, err := dg.User("@me", withOptionalContext(ctx)...)
	if err != nil {
		return err
	}
	if dg.State == nil {
		dg.State = discordgo.NewState()
	}
	dg.State.Lock()
	dg.State.User = u
	dg.State.Unlock()

	userGuilds, err := dg.UserGuilds(100, "", "", false, withOptionalContext(ctx)...)
	if err != nil {
		return err
	}

	for _, ug := range userGuilds {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if ug == nil || ug.ID == "" {
			continue
		}
		if err := dg.State.GuildAdd(&discordgo.Guild{ID: ug.ID, Name: ug.Name}); err != nil {
			log.Printf("%s[WARN] Failed to add guild %s to session state: %v", opts.LogPrefix, ug.ID, err)
		}
		hydrateGuildRoles(ctx, dg, ug.ID, opts.LogPrefix)
		hydrateGuildChannels(ctx, dg, ug.ID, opts.OnChannel, opts.LogPrefix)
		hydrateGuildThreads(ctx, dg, ug.ID, opts.OnChannel, opts.LogPrefix)
		SyncBotMemberRoles(ctx, dg, ug.ID, u.ID, opts)
		if opts.OnGuild != nil {
			opts.OnGuild(dg, u.ID, ug.ID)
		}
	}
	return nil
}

// OpenSessionWithRetry asynchronously hydrates dg with exponential backoff (capped at 60s)
// until hydration succeeds or ctx is canceled. The returned stop func cancels the retry loop
// and blocks until the background goroutine exits.
func OpenSessionWithRetry(ctx context.Context, dg *discordgo.Session, opts HydrateOptions) (stop func()) {
	if ctx == nil || ctx.Err() != nil || dg == nil {
		return func() {}
	}
	hydrateFn := opts.HydrateFunc
	if hydrateFn == nil {
		hydrateFn = func(hCtx context.Context, s *discordgo.Session) error {
			return HydrateSessionREST(hCtx, s, opts)
		}
	}
	backoff := opts.RetryBackoff
	if backoff <= 0 {
		backoff = 2 * time.Second
	}
	const maxBackoff = 60 * time.Second

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			err := hydrateFn(runCtx, dg)
			if err == nil {
				log.Printf("%sDiscord REST session hydrated", opts.LogPrefix)
				return
			}
			if runCtx.Err() != nil {
				return
			}
			log.Printf("%sWarning: Discord session hydration failed: %v. Retrying in %v...", opts.LogPrefix, err, backoff)

			timer := time.NewTimer(backoff)
			select {
			case <-runCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			backoff = min(backoff*2, maxBackoff)
		}
	}()

	return func() {
		cancel()
		<-done
	}
}
