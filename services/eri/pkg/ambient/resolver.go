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

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/mcp"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

// Resolver resolves ambient context using configured MCP servers.
type Resolver struct {
	cfg       *config.Config
	providers sync.Map
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

// Resolve resolves ambient context for ambCfg using cached or new Providers.
func (r *Resolver) Resolve(ctx context.Context, ambCfg *config.AmbientContextConfig) (string, error) {
	if ambCfg == nil {
		return "", nil
	}
	toolsJSON, err := json.Marshal(ambCfg.Tools)
	if err != nil {
		log.Printf("[Ambient] Warning: failed to marshal ambient tools for cache key: %v", err)
	}
	key := fmt.Sprintf("%s:%s:%s:%s", ambCfg.TemplatePath, ambCfg.Template, string(toolsJSON), ambCfg.CacheTTL)
	val, ok := r.providers.Load(key)
	var prov *Provider
	if ok {
		if p, ok := val.(*Provider); ok {
			prov = p
		}
	}
	if prov == nil {
		var curData *config.ConfigData
		if r.cfg != nil {
			curData = r.cfg.Current()
		}
		servers := extractMCPServers(curData, true)
		disp := mcp.NewDispatcher(servers, nil)
		var pErr error
		prov, pErr = NewProvider(*ambCfg, disp)
		if pErr != nil {
			return "", fmt.Errorf("failed to create ambient provider: %w", pErr)
		}
		go func() {
			primeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			if err := prov.Prime(primeCtx); err != nil {
				log.Printf("[Ambient] Ambient context prime warning: %v", err)
			}
		}()
		r.providers.Store(key, prov)
	}
	return prov.Retrieve(ctx)
}
