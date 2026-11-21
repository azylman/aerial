package bootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/bwmarrin/discordgo"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/delivery"
	"github.com/azylman/aerial/services/eri/pkg/env"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
)

// ReloadTrigger runs git-sync webhook reloads on a single background worker,
// coalescing bursts of requests. pending holds at most one queued reload: a
// Trigger that arrives while one is already pending is dropped, because the
// queued reload will read the newest files anyway. A burst of webhook calls
// therefore costs at most one extra reload after the one in flight.
type ReloadTrigger struct {
	pending chan struct{}
	done    chan struct{}
}

// NewReloadTrigger starts the reload worker. The worker calls reloadConfig for
// each pending request until ctx is cancelled, then closes Done. A request
// still pending at cancellation is discarded rather than reloaded mid-shutdown.
func NewReloadTrigger(ctx context.Context, reloadConfig func(string)) *ReloadTrigger {
	t := &ReloadTrigger{pending: make(chan struct{}, 1), done: make(chan struct{})}
	go func() {
		defer close(t.done)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.pending:
				if ctx.Err() != nil {
					return
				}
				reloadConfig("git-sync webhook")
			}
		}
	}()
	return t
}

// Trigger queues a reload without blocking; it is a no-op when one is already
// pending or the trigger is nil.
func (t *ReloadTrigger) Trigger() {
	if t == nil {
		return
	}
	select {
	case t.pending <- struct{}{}:
	default:
	}
}

// Done is closed once the reload worker has stopped.
func (t *ReloadTrigger) Done() <-chan struct{} {
	return t.done
}

// GitSyncReloadHandler serves the git-sync sidecar webhook. A full reload
// (config, rules, skills, MCP config, daemon pool MarkDirty) takes 1-3s, but
// git-sync's webhook has a 1s default timeout and retries every few seconds
// until it sees its configured success status. Reloading on the request path
// therefore made every call time out and every retry run another reload that
// recycled warm daemons. The handler only queues a coalesced reload on trigger
// and answers 200 immediately. A pod may run two git-sync sidecars (aerial
// rules/skills and aerial-config) that fire at the same moment; the trigger's
// single worker serializes their reloads, and the reload func retains the last
// known good config on failure, so there is no error to report back to
// git-sync. Callers mount it behind httpkit.LoopbackOnly.
func GitSyncReloadHandler(trigger *ReloadTrigger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		trigger.Trigger()
		httpkit.WriteJSON(w, http.StatusOK, map[string]string{"status": "queued"})
	})
}

// DirtyMarker is a pool whose warm daemons are recycled when the daemon
// configuration changes.
type DirtyMarker interface {
	MarkDirty()
}

// ReloadOption configures CreateReloadConfigFunc.
type ReloadOption func(*reloadConfigOptions)

type reloadConfigOptions struct {
	dgSession             *discordgo.Session
	reloadSupplier        func(candidate *config.Config) error
	syncAssets            func(cfg *config.Config) error
	dirtyPools            []DirtyMarker
	onReload              func(source string)
	lastDaemonFingerprint string
}

// WithDiscordSession posts an "Invalid Configuration File" system alert through
// s whenever a reload is rejected.
func WithDiscordSession(s *discordgo.Session) ReloadOption {
	return func(o *reloadConfigOptions) {
		o.dgSession = s
	}
}

// WithDirtyPools marks pools dirty after an accepted reload that changed the
// daemon configuration fingerprint.
func WithDirtyPools(pools ...DirtyMarker) ReloadOption {
	return func(o *reloadConfigOptions) {
		o.dirtyPools = append(o.dirtyPools, pools...)
	}
}

// WithProvisioner re-provisions runtime assets from the accepted config before
// pools are marked dirty. A nil provisioner is ignored.
func WithProvisioner(ctx context.Context, p *env.Provisioner) ReloadOption {
	return func(o *reloadConfigOptions) {
		if p == nil {
			return
		}
		o.syncAssets = func(cfg *config.Config) error { return p.Sync(ctx, cfg) }
	}
}

// WithOnReload calls fn after every reload attempt, accepted or rejected.
func WithOnReload(fn func(source string)) ReloadOption {
	return func(o *reloadConfigOptions) {
		o.onReload = fn
	}
}

func ensureConfigDefaults(cfg *config.Config) {
	if cfg == nil {
		return
	}
	cur := cfg.Current()
	changed := false
	if cur.GeminiHomeDir == "" {
		cur.GeminiHomeDir = DefaultGeminiHomeDir()
		changed = true
	}
	if cur.DataDir == "" {
		cur.DataDir = DefaultDataDir()
		changed = true
	}
	if changed {
		cfg.Update(cur)
	}
}

func computeDaemonReloadFingerprint(cfg *config.Config) string {
	if cfg == nil || cfg.Current() == nil {
		return ""
	}
	cur := cfg.Current()
	homeDir := cur.GeminiHomeDir
	if homeDir == "" {
		homeDir = DefaultGeminiHomeDir()
	}
	dataDir := cur.DataDir
	if dataDir == "" {
		dataDir = DefaultDataDir()
	}

	var b strings.Builder
	b.WriteString("homeDir:")
	b.WriteString(homeDir)
	b.WriteString("\ndataDir:")
	b.WriteString(dataDir)
	b.WriteString("\nengine:")
	b.WriteString(cur.GetEngine())
	b.WriteString("\nclaudeBin:")
	b.WriteString(cur.ClaudeBin)
	b.WriteString("\ncodexBin:")
	b.WriteString(cur.CodexBin)
	b.WriteString("\nmuseBin:")
	b.WriteString(cur.MuseBin)
	b.WriteString("\nagyBin:")
	b.WriteString(cur.AgyBin)
	b.WriteString("\nmodel:")
	b.WriteString(cur.Model)
	b.WriteString("\nlowEffortModel:")
	b.WriteString(cur.LowEffortModel)
	b.WriteString("\nmediumEffortModel:")
	b.WriteString(cur.MediumEffortModel)
	b.WriteString("\ntimezone:")
	b.WriteString(cur.Timezone)
	b.WriteString("\napiKey:")
	b.WriteString(cur.APIKey)
	b.WriteString("\nharnessApiKey:")
	b.WriteString(cur.HarnessAPIKey)
	b.WriteString("\nsystemPrompt:")
	b.WriteString(cur.SystemPrompt)
	b.WriteString("\nvoice.engine:")
	b.WriteString(cur.Voice.Engine)
	b.WriteString("\nvoice.model:")
	b.WriteString(cur.Voice.Model)
	b.WriteString("\nvoice.prewarmedTargets:")
	b.WriteString(strings.Join(cur.Voice.PrewarmedTargets, ","))
	b.WriteString("\nvoiceAllowedTools:")
	b.WriteString(strings.Join(cur.VoiceAllowedTools(), ","))
	b.WriteString("\nvoiceAmbientContext:")
	if amb := cur.VoiceAmbientContext(); amb != nil {
		if ambBytes, err := json.Marshal(amb); err == nil {
			b.Write(ambBytes)
		}
	} else {
		b.WriteString("nil")
	}
	b.WriteString("\nmcpServers:")
	if mcpServersJSON, err := json.Marshal(cur.McpServers); err == nil {
		b.Write(mcpServersJSON)
	}
	b.WriteString("\nmcps:")
	if mcpsJSON, err := json.Marshal(cur.MCPServers); err == nil {
		b.Write(mcpsJSON)
	}
	b.WriteString("\nmcpConfig:")
	b.WriteString(cur.MCPConfig)
	b.WriteString("\ncredentialSources:")
	if credJSON, err := json.Marshal(cur.CredentialSources); err == nil {
		b.Write(credJSON)
	}
	b.WriteByte('\n')

	h := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(h[:])
}

func markDirtyIfFingerprintChanged(source string, cfg *config.Config, options *reloadConfigOptions) {
	nextFP := computeDaemonReloadFingerprint(cfg)
	if nextFP == options.lastDaemonFingerprint {
		log.Printf("[%s] Daemon configuration and provisioned assets unchanged; preserving active daemon sessions", source)
		return
	}
	options.lastDaemonFingerprint = nextFP
	for _, p := range options.dirtyPools {
		if p != nil {
			p.MarkDirty()
		}
	}
}

func executeConfigReload(source string, cfg *config.Config, options *reloadConfigOptions) {
	candidate := config.NewFromData(cfg.Current())
	if err := options.reloadSupplier(candidate); err != nil {
		metrics.ConfigReloadsTotal.WithLabelValues(source, "failure").Inc()
		log.Printf("[%s] Warning: Failed to reload config: %v", source, err)
		if options.dgSession != nil {
			alertMsg := fmt.Sprintf("Failed to reload config:\n```\n%v\n```\nAerial has retained the Last Known Good Configuration (LKGC).", err)
			if alertErr := delivery.SendSystemAlert(options.dgSession, cfg.Current().SystemChannel, "Invalid Configuration File", alertMsg); alertErr != nil {
				log.Printf("[%s] Warning: Failed to send system alert for config reload failure: %v", source, alertErr)
			}
		}
		return
	}
	ensureConfigDefaults(candidate)
	cfg.Update(candidate.Current())
	metrics.ConfigReloadsTotal.WithLabelValues(source, "success").Inc()
	config.RegisterConfigTokens(cfg)

	if options.syncAssets != nil {
		if err := options.syncAssets(cfg); err != nil {
			log.Printf("[%s] Warning: env sync error on reload: %v", source, err)
		}
	}
	markDirtyIfFingerprintChanged(source, cfg, options)
}

func defaultReloadSupplier(candidate *config.Config) error {
	// Loading a reload candidate must not publish to ActiveConfig before validation.
	return config.ReloadWithSupplier(candidate, func() (*config.Config, error) {
		return config.LoadConfigFromLookup(config.WithSharedSecretsLookup(os.Getenv, config.SharedSecretsPath),
			config.ResolveConfigSearchPaths(os.Getenv, config.ConfigSearchPaths...)...)
	})
}

// CreateReloadConfigFunc returns the serialized config reload callback. Each
// call loads a candidate config without touching cfg; a rejected candidate
// keeps the Last Known Good Configuration and alerts the system channel. An
// accepted candidate is published to cfg, its tokens registered for redaction,
// runtime assets re-provisioned, and pools marked dirty only when the daemon
// fingerprint changed.
func CreateReloadConfigFunc(cfg *config.Config, opts ...ReloadOption) func(source string) {
	options := &reloadConfigOptions{reloadSupplier: defaultReloadSupplier}
	for _, opt := range opts {
		if opt != nil {
			opt(options)
		}
	}

	options.lastDaemonFingerprint = computeDaemonReloadFingerprint(cfg)

	var reloadMu sync.Mutex
	return func(source string) {
		reloadMu.Lock()
		defer reloadMu.Unlock()
		defer func() {
			if options.onReload != nil {
				options.onReload(source)
			}
		}()
		log.Printf("[%s] Changes detected. Reloading configuration, system rules, and skills...", source)
		executeConfigReload(source, cfg, options)
	}
}
