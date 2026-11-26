package ambient

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/mcp"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

// Resolver resolves ambient context using configured MCP servers.
type Resolver struct {
	cfg       *config.Config
	providers sync.Map
	sf        singleflight.Group
}

// NewResolver constructs a new ambient context Resolver.
func NewResolver(cfg *config.Config) *Resolver {
	return &Resolver{cfg: cfg}
}

func extractMCPServers(cur *config.ConfigData, includeDiscord bool) []runner.MCPServerConfig {
	serverMap := make(map[string]runner.MCPServerConfig)
	if cur != nil {
		type srvJSON struct {
			ServerURL string            `json:"serverUrl"`
			Headers   map[string]string `json:"headers,omitempty"`
		}
		parseMap := func(m map[string]json.RawMessage) {
			for name, raw := range m {
				var sj srvJSON
				if err := json.Unmarshal(raw, &sj); err == nil && strings.TrimSpace(sj.ServerURL) != "" {
					serverMap[name] = runner.MCPServerConfig{
						Name:      name,
						ServerURL: strings.TrimSpace(sj.ServerURL),
						Headers:   sj.Headers,
					}
				}
			}
		}
		parseMap(cur.McpServers.Common)
		if includeDiscord {
			parseMap(cur.McpServers.Discord)
		}
		parseMap(cur.McpServers.Voice)
	}
	result := make([]runner.MCPServerConfig, 0, len(serverMap))
	for _, s := range serverMap {
		result = append(result, s)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}

func ambientAllowedTools(tools []config.AmbientToolConfig) []string {
	if len(tools) == 0 {
		return nil
	}
	toolSet := make(map[string]struct{})
	for _, t := range tools {
		trimmed := strings.TrimSpace(t.Tool)
		if trimmed != "" {
			toolSet[trimmed] = struct{}{}
		}
	}
	if len(toolSet) == 0 {
		return nil
	}
	result := make([]string, 0, len(toolSet))
	for tool := range toolSet {
		result = append(result, tool)
	}
	sort.Strings(result)
	return result
}

func selectAmbientMCPServers(cur *config.ConfigData, tools []config.AmbientToolConfig, isVoice bool) []runner.MCPServerConfig {
	if cur == nil || len(tools) == 0 {
		return nil
	}

	namedServers := make(map[string]bool)
	hasUnnamedTool := false
	for _, t := range tools {
		sName := strings.TrimSpace(t.Server)
		if sName == "" {
			hasUnnamedTool = true
		} else {
			namedServers[sName] = true
		}
	}

	if !hasUnnamedTool {
		allServers := extractMCPServers(cur, true)
		result := make([]runner.MCPServerConfig, 0, len(namedServers))
		for _, s := range allServers {
			if namedServers[s.Name] {
				result = append(result, s)
			}
		}
		return result
	}

	// For unnamed tools: exclude discord servers only for voice ambient context.
	baseServers := extractMCPServers(cur, !isVoice)
	serverMap := make(map[string]runner.MCPServerConfig, len(baseServers)+len(namedServers))
	for _, s := range baseServers {
		serverMap[s.Name] = s
	}

	if len(namedServers) > 0 {
		allServers := extractMCPServers(cur, true)
		for _, s := range allServers {
			if namedServers[s.Name] {
				serverMap[s.Name] = s
			}
		}
	}

	result := make([]runner.MCPServerConfig, 0, len(serverMap))
	for _, s := range serverMap {
		result = append(result, s)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}

func primeProvider(ctx context.Context, ambCfg *config.AmbientContextConfig, disp *runner.DefaultDispatcher, prov *Provider, allowedTools []string) {
	if len(allowedTools) == 0 {
		if err := prov.Prime(ctx); err != nil {
			log.Printf("[Ambient] Ambient context prime warning: %v", err)
		}
		return
	}
	primeTimeout := 2500 * time.Millisecond
	if ambCfg.TimeoutMs > 0 {
		primeTimeout = time.Duration(ambCfg.TimeoutMs) * time.Millisecond
	}

	dispCtx, dispCancel := context.WithTimeout(ctx, primeTimeout)
	if err := disp.Prime(dispCtx); err != nil {
		log.Printf("[Ambient] MCP dispatcher prime warning: %v", err)
	}
	dispCancel()

	provCtx, provCancel := context.WithTimeout(ctx, primeTimeout+2500*time.Millisecond)
	defer provCancel()
	if err := prov.Prime(provCtx); err != nil {
		log.Printf("[Ambient] Ambient context prime warning: %v", err)
	}
}

func (r *Resolver) createProvider(ctx context.Context, ambCfg *config.AmbientContextConfig, key string, isVoice bool) (*Provider, error) {
	primeBase := context.WithoutCancel(ctx)
	ch := r.sf.DoChan(key, func() (any, error) {
		if val, ok := r.providers.Load(key); ok {
			if p, ok := val.(*Provider); ok {
				return p, nil
			}
		}
		var curData *config.ConfigData
		if r.cfg != nil {
			curData = r.cfg.Current()
		}
		servers := selectAmbientMCPServers(curData, ambCfg.Tools, isVoice)
		allowedTools := ambientAllowedTools(ambCfg.Tools)
		disp := mcp.NewDispatcher(servers, nil, allowedTools)
		p, pErr := NewProvider(*ambCfg, disp)
		if pErr != nil {
			return nil, fmt.Errorf("failed to create ambient provider: %w", pErr)
		}
		primeProvider(primeBase, ambCfg, disp, p, allowedTools)
		r.providers.Store(key, p)
		return p, nil
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		prov, ok := res.Val.(*Provider)
		if !ok || prov == nil {
			return nil, fmt.Errorf("unexpected provider type from singleflight: %T", res.Val)
		}
		return prov, nil
	}
}

func (r *Resolver) resolveWithVoice(ctx context.Context, ambCfg *config.AmbientContextConfig, isVoice bool) (string, error) {
	if ambCfg == nil {
		return "", nil
	}
	toolsJSON, err := json.Marshal(ambCfg.Tools)
	if err != nil {
		log.Printf("[Ambient] Warning: failed to marshal ambient tools for cache key: %v", err)
	}
	key := fmt.Sprintf("%t:%s:%s:%s:%s:%d", isVoice, ambCfg.TemplatePath, ambCfg.Template, string(toolsJSON), ambCfg.CacheTTL, ambCfg.TimeoutMs)
	if val, ok := r.providers.Load(key); ok {
		if p, ok := val.(*Provider); ok {
			return p.Retrieve(ctx)
		}
	}
	prov, err := r.createProvider(ctx, ambCfg, key, isVoice)
	if err != nil {
		return "", err
	}
	return prov.Retrieve(ctx)
}

// Resolve resolves ambient context for ambCfg, defaulting to channel context.
func (r *Resolver) Resolve(ctx context.Context, ambCfg *config.AmbientContextConfig) (string, error) {
	return r.ResolveChannel(ctx, ambCfg)
}

// ResolveVoice resolves ambient context specifically for voice, excluding Discord MCP servers for unnamed tools.
func (r *Resolver) ResolveVoice(ctx context.Context, ambCfg *config.AmbientContextConfig) (string, error) {
	return r.resolveWithVoice(ctx, ambCfg, true)
}

// ResolveChannel resolves ambient context for a text channel, including Discord MCP servers for unnamed tools.
func (r *Resolver) ResolveChannel(ctx context.Context, ambCfg *config.AmbientContextConfig) (string, error) {
	return r.resolveWithVoice(ctx, ambCfg, false)
}

// PrimeConfigured primes configured voice and channel ambient contexts.
func (r *Resolver) PrimeConfigured(ctx context.Context) {
	if r == nil || r.cfg == nil || ctx.Err() != nil {
		return
	}
	cur := r.cfg.Current()
	if vAmb := cur.VoiceAmbientContext(); vAmb != nil {
		if _, err := r.ResolveVoice(ctx, vAmb); err != nil {
			log.Printf("[Ambient] Voice ambient prime warning: %v", err)
		}
	}
	channelNames := make([]string, 0, len(cur.Channels))
	for name := range cur.Channels {
		channelNames = append(channelNames, name)
	}
	sort.Strings(channelNames)
	for _, name := range channelNames {
		if ctx.Err() != nil {
			return
		}
		policy := cur.Channels[name]
		if policy.AmbientContext != nil {
			if _, err := r.ResolveChannel(ctx, policy.AmbientContext); err != nil {
				log.Printf("[Ambient] Channel %s ambient prime warning: %v", name, err)
			}
		}
	}
}
