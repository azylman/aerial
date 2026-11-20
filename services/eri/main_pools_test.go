package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/azylman/aerial/services/eri/pkg/config"
)

func TestExtractMCPServers(t *testing.T) {
	t.Parallel()

	// 1. Nil config
	nilVoice := extractMCPServers(nil, false)
	if len(nilVoice) != 0 {
		t.Fatalf("expected 0 servers for nil voice config, got: %+v", nilVoice)
	}
	nilAll := extractMCPServers(nil, true)
	if len(nilAll) != 0 {
		t.Fatalf("expected 0 servers for nil all config, got: %+v", nilAll)
	}

	// 2. Populated config with Common, Voice, Discord
	rawValid := json.RawMessage(`{"serverUrl":"http://alpha:8080/mcp","headers":{"Auth":"Bearer tok"}}`)
	rawDiscord := json.RawMessage(`{"serverUrl":"http://discord:8080/mcp"}`)
	rawVoice := json.RawMessage(`{"serverUrl":"http://voice:8080/mcp"}`)
	rawSched := json.RawMessage(`{"serverUrl":"http://haro.aerial/mcp"}`)
	data := &config.ConfigData{
		McpServers: config.TargetMcpConfig{
			Common: map[string]json.RawMessage{
				"scheduler": rawSched,
				"alpha":     rawValid,
			},
			Discord: map[string]json.RawMessage{
				"beta": rawDiscord,
			},
			Voice: map[string]json.RawMessage{
				"gamma": rawVoice,
			},
		},
	}

	voiceServers := extractMCPServers(data, false)
	if len(voiceServers) != 3 {
		t.Fatalf("expected 3 servers for voice (scheduler, alpha, gamma), got %d: %+v", len(voiceServers), voiceServers)
	}
	allServers := extractMCPServers(data, true)
	if len(allServers) != 4 {
		t.Fatalf("expected 4 servers for all (scheduler, alpha, beta, gamma), got %d: %+v", len(allServers), allServers)
	}
}

func TestNewAmbientContextResolver(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cfg        *config.Config
		ambCfg     *config.AmbientContextConfig
		repeatCall bool
		wantErr    bool
		wantSubstr string
	}{
		{
			name:   "NilAmbConfig",
			cfg:    nil,
			ambCfg: nil,
		},
		{
			name: "RenderValidTemplateAndCacheHit",
			cfg:  newTestConfig(func(d *config.ConfigData) {}),
			ambCfg: &config.AmbientContextConfig{
				Template: "Static ambient text",
				CacheTTL: "1m",
			},
			repeatCall: true,
			wantSubstr: "Static ambient text",
		},
		{
			name: "InvalidTemplateError",
			cfg:  newTestConfig(func(d *config.ConfigData) {}),
			ambCfg: &config.AmbientContextConfig{
				Template: "{{.unclosed",
				CacheTTL: "1m",
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resolver := newAmbientContextResolver(tc.cfg)
			res, err := resolver(context.Background(), tc.ambCfg)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantSubstr != "" && !strings.Contains(res, tc.wantSubstr) {
				t.Errorf("expected result to contain %q, got %q", tc.wantSubstr, res)
			}
			if tc.repeatCall {
				res2, err2 := resolver(context.Background(), tc.ambCfg)
				if err2 != nil {
					t.Fatalf("unexpected error on repeated call: %v", err2)
				}
				if res2 != res {
					t.Errorf("expected cached result %q, got %q", res, res2)
				}
			}
		})
	}
}

type serveBrainHTTPCase struct {
	name       string
	closeLn    bool
	asyncErr   error
	cancelCtx  bool
	wantErr    bool
	wantErrSub string
}

func assertServeBrainHTTPCase(t *testing.T, tc serveBrainHTTPCase) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to bind listener: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var asyncErrCh chan error
	if tc.asyncErr != nil {
		asyncErrCh = make(chan error, 1)
		asyncErrCh <- tc.asyncErr
	}

	if tc.closeLn {
		_ = ln.Close()
	}
	if tc.cancelCtx {
		cancel()
	}

	mux := http.NewServeMux()
	var errResult error
	if asyncErrCh != nil {
		errResult = serveBrainHTTP(ctx, ln, mux, "test-model", asyncErrCh)
	} else {
		errResult = serveBrainHTTP(ctx, ln, mux, "test-model")
	}

	if tc.wantErr {
		if errResult == nil {
			t.Fatalf("expected error, got nil")
		}
		if tc.wantErrSub != "" && !strings.Contains(errResult.Error(), tc.wantErrSub) {
			t.Errorf("expected error containing %q, got %v", tc.wantErrSub, errResult)
		}
		return
	}
	if errResult != nil {
		t.Fatalf("unexpected error: %v", errResult)
	}
}

func TestServeBrainHTTP(t *testing.T) {
	t.Parallel()

	tests := []serveBrainHTTPCase{
		{name: "GracefulShutdownOnContextDone", cancelCtx: true},
		{name: "AsyncLeaderError", asyncErr: errors.New("leader failed"), wantErr: true, wantErrSub: "leader failed"},
		{name: "ListenerClosedServeError", closeLn: true, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertServeBrainHTTPCase(t, tc)
		})
	}
}
