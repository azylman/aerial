package ambient

import (
	"context"
	"encoding/json"
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
