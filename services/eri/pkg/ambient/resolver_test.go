package ambient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/services/eri/pkg/config"
)

func TestResolver_Resolve(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfg        *config.Config
		ambCfg     *config.AmbientContextConfig
		wantEmpty  bool
		wantErrSub string
	}{
		{
			name:      "nil ambient config returns empty without error",
			cfg:       nil,
			ambCfg:    nil,
			wantEmpty: true,
		},
		{
			name: "valid static template resolves rendered context",
			cfg: config.NewFromData(&config.ConfigData{
				McpServers: config.TargetMcpConfig{
					Common: map[string]json.RawMessage{
						"dummy": []byte(`{"serverUrl":"http://localhost:12345"}`),
					},
				},
			}),
			ambCfg: &config.AmbientContextConfig{
				Template: "Static ambient text",
				CacheTTL: "5s",
			},
			wantEmpty: false,
		},
		{
			name: "invalid template syntax returns error",
			cfg:  config.NewFromData(&config.ConfigData{}),
			ambCfg: &config.AmbientContextConfig{
				Template: "{{ .Unclosed",
			},
			wantErrSub: "failed to create ambient provider",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := NewResolver(tc.cfg)
			out, err := r.Resolve(context.Background(), tc.ambCfg)
			if tc.wantErrSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrSub)
				return
			}
			require.NoError(t, err)
			if tc.wantEmpty {
				assert.Empty(t, out)
			} else {
				assert.Contains(t, out, "Static ambient text")
			}
		})
	}
}

func TestResolver_CacheHitAndMCPServers(t *testing.T) {
	t.Parallel()

	cfg := config.NewFromData(&config.ConfigData{
		McpServers: config.TargetMcpConfig{
			Common: map[string]json.RawMessage{
				"srv-common": []byte(`{"serverUrl":"http://localhost:1111"}`),
			},
			Discord: map[string]json.RawMessage{
				"srv-discord": []byte(`{"serverUrl":"http://localhost:2222"}`),
			},
			Voice: map[string]json.RawMessage{
				"srv-voice": []byte(`{"serverUrl":"http://localhost:3333"}`),
			},
		},
	})
	r := NewResolver(cfg)
	ambCfg := &config.AmbientContextConfig{
		Template: "Hello cached ambient",
		CacheTTL: "10s",
	}

	// First call populates cache
	out1, err1 := r.Resolve(context.Background(), ambCfg)
	require.NoError(t, err1)
	assert.Contains(t, out1, "Hello cached ambient")

	// Second call hits cache (exercising prov = p load)
	out2, err2 := r.Resolve(context.Background(), ambCfg)
	require.NoError(t, err2)
	assert.Equal(t, out1, out2)

	// extractMCPServers with nil cur
	servers := extractMCPServers(nil, false)
	assert.Empty(t, servers)
}

func TestAmbientAllowedTools(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tools     []config.AmbientToolConfig
		wantTools []string
	}{
		{
			name:      "nil tools returns nil",
			tools:     nil,
			wantTools: nil,
		},
		{
			name:      "empty tools returns nil",
			tools:     []config.AmbientToolConfig{},
			wantTools: nil,
		},
		{
			name: "only whitespace tools returns nil",
			tools: []config.AmbientToolConfig{
				{Tool: "   "},
				{Tool: ""},
			},
			wantTools: nil,
		},
		{
			name: "single trimmed tool returns single item",
			tools: []config.AmbientToolConfig{
				{Tool: " ha_search "},
			},
			wantTools: []string{"ha_search"},
		},
		{
			name: "duplicates and whitespace deduplicated and sorted",
			tools: []config.AmbientToolConfig{
				{Tool: "weather_report"},
				{Tool: "ha_search"},
				{Tool: "  weather_report  "},
				{Tool: ""},
			},
			wantTools: []string{"ha_search", "weather_report"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ambientAllowedTools(tc.tools)
			assert.Equal(t, tc.wantTools, got)
		})
	}
}

func TestSelectAmbientMCPServers(t *testing.T) {
	t.Parallel()

	baseCfg := &config.ConfigData{
		McpServers: config.TargetMcpConfig{
			Common: map[string]json.RawMessage{
				"srv-common": []byte(`{"serverUrl":"http://localhost:1111"}`),
			},
			Discord: map[string]json.RawMessage{
				"srv-discord": []byte(`{"serverUrl":"http://localhost:2222"}`),
			},
			Voice: map[string]json.RawMessage{
				"srv-voice": []byte(`{"serverUrl":"http://localhost:3333"}`),
			},
		},
	}

	tests := []struct {
		name        string
		hasConfig   bool
		tools       []config.AmbientToolConfig
		wantServers []string
	}{
		{
			name:        "nil cur returns nil",
			hasConfig:   false,
			tools:       []config.AmbientToolConfig{{Tool: "foo"}},
			wantServers: nil,
		},
		{
			name:        "empty tools returns nil",
			hasConfig:   true,
			tools:       nil,
			wantServers: nil,
		},
		{
			name:      "unnamed tools excludes discord servers",
			hasConfig: true,
			tools: []config.AmbientToolConfig{
				{Tool: "some_tool"},
			},
			wantServers: []string{"srv-common", "srv-voice"},
		},
		{
			name:      "all-named tools returns only named servers",
			hasConfig: true,
			tools: []config.AmbientToolConfig{
				{Server: "srv-discord", Tool: "discord_tool"},
			},
			wantServers: []string{"srv-discord"},
		},
		{
			name:      "mixed named and unnamed tools includes common, voice, and named discord",
			hasConfig: true,
			tools: []config.AmbientToolConfig{
				{Tool: "unnamed_tool"},
				{Server: "srv-discord", Tool: "discord_tool"},
			},
			wantServers: []string{"srv-common", "srv-discord", "srv-voice"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var cur *config.ConfigData
			if tc.hasConfig {
				cur = baseCfg
			}
			servers := selectAmbientMCPServers(cur, tc.tools)
			var serverNames []string
			for _, s := range servers {
				serverNames = append(serverNames, s.Name)
			}
			assert.Equal(t, tc.wantServers, serverNames)
		})
	}
}

func TestResolver_Resolve_ScopedMCPServers(t *testing.T) {
	t.Parallel()

	var discordCalls atomic.Int32
	discordServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		discordCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(discordServer.Close)

	var voiceCalls atomic.Int32
	voiceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		voiceCalls.Add(1)
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		method, _ := req["method"].(string)
		id := req["id"]
		if id == nil {
			id = 1
		}
		switch method {
		case "initialize":
			w.Header().Set("Mcp-Session-Id", "sess-voice")
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      id,
				"result":  map[string]any{"protocolVersion": "2024-11-05"},
			}); err != nil {
				return
			}
		case "notifications/initialized":
			w.WriteHeader(http.StatusOK)
		case "tools/list":
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      id,
				"result": map[string]any{
					"tools": []map[string]any{
						{"name": "ha_state", "description": "state"},
					},
				},
			}); err != nil {
				return
			}
		case "tools/call":
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      id,
				"result": map[string]any{
					"content": []map[string]any{
						{"type": "text", "text": `{"status":"operational"}`},
					},
				},
			}); err != nil {
				return
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(voiceServer.Close)

	cfg := config.NewFromData(&config.ConfigData{
		McpServers: config.TargetMcpConfig{
			Discord: map[string]json.RawMessage{
				"srv-discord": []byte(fmt.Sprintf(`{"serverUrl":%q}`, discordServer.URL)),
			},
			Voice: map[string]json.RawMessage{
				"srv-voice": []byte(fmt.Sprintf(`{"serverUrl":%q}`, voiceServer.URL)),
			},
		},
	})

	r := NewResolver(cfg)
	ambCfg := &config.AmbientContextConfig{
		Template:  "Voice Status: {{.voice.status}}",
		CacheTTL:  "10s",
		TimeoutMs: 1500,
		Tools: []config.AmbientToolConfig{
			{Name: "voice", Tool: "ha_state"},
		},
	}

	out, err := r.Resolve(context.Background(), ambCfg)
	require.NoError(t, err)
	assert.Contains(t, out, "Voice Status: operational")
	assert.Equal(t, int32(0), discordCalls.Load(), "Discord-only MCP server must not be contacted")
	assert.Greater(t, voiceCalls.Load(), int32(0), "Voice MCP server should be contacted")
}

func TestResolver_PrimeConfigured(t *testing.T) {
	t.Parallel()

	// 1. Nil resolver and nil config are safe no-ops
	var nilResolver *Resolver
	nilResolver.PrimeConfigured(context.Background())

	rNilCfg := NewResolver(nil)
	rNilCfg.PrimeConfigured(context.Background())

	// 2. Pre-cancelled context is a safe no-op
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := config.NewFromData(&config.ConfigData{
		Voice: config.VoiceConfig{
			AmbientContext: &config.AmbientContextConfig{
				Template: "Voice Ambient Primed",
			},
		},
		Channels: map[string]config.ChannelPolicy{
			"beta": {
				AmbientContext: &config.AmbientContextConfig{
					Template: "Beta Ambient Primed",
				},
			},
			"alpha": {
				AmbientContext: &config.AmbientContextConfig{
					Template: "Alpha Ambient Primed",
				},
			},
			"gamma": {
				AmbientContext: nil,
			},
		},
	})
	r := NewResolver(cfg)
	r.PrimeConfigured(canceledCtx)

	// 3. Active context primes voice and sorted channel contexts
	r.PrimeConfigured(context.Background())

	vAmb := cfg.Current().VoiceAmbientContext()
	require.NotNil(t, vAmb)
	outVoice, err := r.Resolve(context.Background(), vAmb)
	require.NoError(t, err)
	assert.Contains(t, outVoice, "Voice Ambient Primed")

	alphaAmb := cfg.Current().Channels["alpha"].AmbientContext
	require.NotNil(t, alphaAmb)
	outAlpha, err := r.Resolve(context.Background(), alphaAmb)
	require.NoError(t, err)
	assert.Contains(t, outAlpha, "Alpha Ambient Primed")

	// 4. Config returning nil Data is safe no-op
	rNilData := NewResolver(config.NewFromData(nil))
	rNilData.PrimeConfigured(context.Background())

	// 5. Config with invalid ambient templates handles error branches cleanly
	cfgInvalid := config.NewFromData(&config.ConfigData{
		Voice: config.VoiceConfig{
			AmbientContext: &config.AmbientContextConfig{
				Template: "{{ .Unclosed",
			},
		},
		Channels: map[string]config.ChannelPolicy{
			"alpha": {
				AmbientContext: &config.AmbientContextConfig{
					Template: "{{ .Unclosed",
				},
			},
		},
	})
	rInvalid := NewResolver(cfgInvalid)
	rInvalid.PrimeConfigured(context.Background())
}

func TestResolver_PrimeProvider_Warnings(t *testing.T) {
	t.Parallel()

	failingServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(failingServer.Close)

	cfg := config.NewFromData(&config.ConfigData{
		McpServers: config.TargetMcpConfig{
			Common: map[string]json.RawMessage{
				"srv-failing": []byte(fmt.Sprintf(`{"serverUrl":%q}`, failingServer.URL)),
			},
		},
	})

	r := NewResolver(cfg)
	ambCfg := &config.AmbientContextConfig{
		Template: "Hello {{.failing.val}}",
		Tools: []config.AmbientToolConfig{
			{Name: "failing", Tool: "some_tool"},
		},
	}

	out, err := r.Resolve(context.Background(), ambCfg)
	require.NoError(t, err)
	assert.Contains(t, out, "Hello")

	// Empty allowed tools with Prime error
	ambCfgErr := &config.AmbientContextConfig{
		Template: "{{call .now}}",
	}
	rErr := NewResolver(cfg)
	_, err = rErr.Resolve(context.Background(), ambCfgErr)
	require.Error(t, err)

	// Pre-cancelled context in Resolve exercises primeProvider ctx.Err() check
	cancCtx, cancel := context.WithCancel(context.Background())
	cancel()
	ambCfgCanc := &config.AmbientContextConfig{
		Template: "Canceled: {{.tool.val}}",
		Tools: []config.AmbientToolConfig{
			{Name: "tool", Tool: "tool_canc"},
		},
	}
	rCanc := NewResolver(cfg)
	outCanc, errCanc := rCanc.Resolve(cancCtx, ambCfgCanc)
	require.NoError(t, errCanc)
	assert.Contains(t, outCanc, "Canceled:")
}
