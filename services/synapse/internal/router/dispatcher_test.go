package router

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/bwmarrin/discordgo"
)

func TestDefaultOutboundDispatcher_ResolveEriURL(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Bots: map[string]BotConfig{
			"aerial":      {EriURL: "http://eri-aerial.local:8080", DefaultOwner: true},
			"agent-alpha": {EriURL: "http://eri-agent-alpha.local:8080"},
		},
	}
	disp := NewDefaultOutboundDispatcher("http://fallback.local:8080", nil)
	disp.SetConfigSource(fixedConfig(cfg))

	tests := []struct {
		name  string
		disp  *DefaultOutboundDispatcher
		agent string
		want  string
	}{
		{name: "empty agent returns empty", disp: disp, agent: "", want: ""},
		{name: "exact aerial returns aerial server", disp: disp, agent: "aerial", want: "http://eri-aerial.local:8080"},
		{name: "case-insensitive aerial returns aerial server", disp: disp, agent: "AERIAL", want: "http://eri-aerial.local:8080"},
		{name: "exact agent-alpha returns agent-alpha server", disp: disp, agent: "agent-alpha", want: "http://eri-agent-alpha.local:8080"},
		{name: "unknown agent returns empty", disp: disp, agent: "unknown", want: ""},
		{name: "nil dispatcher returns empty", disp: nil, agent: "aerial", want: ""},
		{name: "dispatcher without config returns empty", disp: &DefaultOutboundDispatcher{}, agent: "aerial", want: ""},
		{name: "dispatcher with empty bots returns empty", disp: &DefaultOutboundDispatcher{cfgSrc: fixedConfig(Config{})}, agent: "aerial", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.disp.resolveEriURL(tc.agent); got != tc.want {
				t.Errorf("resolveEriURL(%q) = %q, want %q", tc.agent, got, tc.want)
			}
		})
	}
}

func TestDefaultOutboundDispatcher_MultiBotRouting(t *testing.T) {
	t.Parallel()

	var aerialIngressHits, aerialInteractionHits atomic.Int32
	aerialServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/internal/discord/message":
			aerialIngressHits.Add(1)
		case "/internal/discord/interaction":
			aerialInteractionHits.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer aerialServer.Close()

	var alphaIngressHits atomic.Int32
	alphaServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/discord/message" {
			alphaIngressHits.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer alphaServer.Close()

	var fallbackIngressHits, fallbackInteractionHits atomic.Int32
	fallbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/internal/discord/message":
			fallbackIngressHits.Add(1)
		case "/internal/discord/interaction":
			fallbackInteractionHits.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer fallbackServer.Close()

	cfg := Config{
		Bots: map[string]BotConfig{
			"aerial":      {EriURL: aerialServer.URL, DefaultOwner: true},
			"agent-alpha": {EriURL: alphaServer.URL},
		},
	}
	disp := NewDefaultOutboundDispatcher(fallbackServer.URL, fallbackServer.Client())
	disp.SetConfigSource(fixedConfig(cfg))

	ctx := context.Background()

	// Dispatch message for "aerial" should hit only aerialServer
	if err := disp.DispatchDiscordMessage(ctx, DiscordMessageIngressRequest{
		Agent:     "aerial",
		BotUserID: "bot-1",
		RoleNames: map[string]string{"admin": "admin"},
		Message:   &discordgo.Message{ID: "m1", Content: "hi"},
	}); err != nil {
		t.Fatalf("DispatchDiscordMessage(aerial) err = %v", err)
	}
	if aerialIngressHits.Load() != 1 || fallbackIngressHits.Load() != 0 || alphaIngressHits.Load() != 0 {
		t.Errorf("aerial dispatch hit counts: aerial=%d, fallback=%d, alpha=%d; want (1, 0, 0)",
			aerialIngressHits.Load(), fallbackIngressHits.Load(), alphaIngressHits.Load())
	}

	// Dispatch message for "unknown" should fall back to fallbackServer
	if err := disp.DispatchDiscordMessage(ctx, DiscordMessageIngressRequest{
		Agent:     "unknown",
		BotUserID: "bot-1",
		RoleNames: map[string]string{"admin": "admin"},
		Message:   &discordgo.Message{ID: "m2", Content: "hi"},
	}); err != nil {
		t.Fatalf("DispatchDiscordMessage(unknown) err = %v", err)
	}
	if aerialIngressHits.Load() != 1 || fallbackIngressHits.Load() != 1 || alphaIngressHits.Load() != 0 {
		t.Errorf("unknown dispatch hit counts: aerial=%d, fallback=%d, alpha=%d; want (1, 1, 0)",
			aerialIngressHits.Load(), fallbackIngressHits.Load(), alphaIngressHits.Load())
	}

	// Dispatch interaction for "aerial" should hit only aerialServer
	if err := disp.DispatchDiscordInteractionForAgent(ctx, "aerial", &discordgo.Interaction{
		ID:   "i1",
		Type: discordgo.InteractionApplicationCommand,
	}); err != nil {
		t.Fatalf("DispatchDiscordInteractionForAgent(aerial) err = %v", err)
	}
	if aerialInteractionHits.Load() != 1 || fallbackInteractionHits.Load() != 0 {
		t.Errorf("aerial interaction hit counts: aerial=%d, fallback=%d; want (1, 0)",
			aerialInteractionHits.Load(), fallbackInteractionHits.Load())
	}

	// Dispatch interaction for "unknown" should fall back to fallbackServer
	if err := disp.DispatchDiscordInteractionForAgent(ctx, "unknown", &discordgo.Interaction{
		ID:   "i2",
		Type: discordgo.InteractionApplicationCommand,
	}); err != nil {
		t.Fatalf("DispatchDiscordInteractionForAgent(unknown) err = %v", err)
	}
	if aerialInteractionHits.Load() != 1 || fallbackInteractionHits.Load() != 1 {
		t.Errorf("unknown interaction hit counts: aerial=%d, fallback=%d; want (1, 1)",
			aerialInteractionHits.Load(), fallbackInteractionHits.Load())
	}
}
