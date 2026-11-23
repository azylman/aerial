package router

import (
	"cmp"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// TestDefaultOutboundDispatcher_KubeConfig pins the promoted kube accessors
// the Flux watcher reads: it silently does nothing if APIServer() is empty.
func TestDefaultOutboundDispatcher_KubeConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		apiServer      string
		retryDelay     time.Duration
		wantAPIServer  string
		wantRetryDelay time.Duration
	}{
		{name: "unconfigured keeps defaults", wantRetryDelay: 1500 * time.Millisecond},
		{name: "configured server and retry delay", apiServer: "https://kube.test:6443/", retryDelay: 20 * time.Millisecond, wantAPIServer: "https://kube.test:6443", wantRetryDelay: 20 * time.Millisecond},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d := NewDefaultOutboundDispatcher("http://eri.test", nil)
			if tc.apiServer != "" {
				d.SetKubeConfig(tc.apiServer, "tok")
			}
			if tc.retryDelay != 0 {
				d.kubeClient.SetRetryConfig(tc.retryDelay, 3)
			}
			if got := d.APIServer(); got != tc.wantAPIServer {
				t.Errorf("APIServer() = %q, want %q", got, tc.wantAPIServer)
			}
			if got := d.RetryDelay(); got != tc.wantRetryDelay {
				t.Errorf("RetryDelay() = %v, want %v", got, tc.wantRetryDelay)
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

type mockOutboundDispatcher struct {
	mu                           sync.Mutex
	promptCalls                  []PromptRequest
	dispatchCalls                []DispatchRequest
	directMessageCalls           []DirectMessageRequest
	discordMessageCalls          []DiscordMessageIngressRequest
	discordInteractionCalls      []*discordgo.Interaction
	discordInteractionAgentCalls []string
	discoverErr                  error
	sourceRepo                   string
	sourceRepoErr                error
	promptErr                    error
	dispatchErr                  error
	directMessageErr             error
	discordMessageErr            error
	discordMessageFailCount      int
	discordMessageAttempts       int
	// discordMessageCancel, when set, is called after each recorded
	// DispatchDiscordMessage, cancelling the caller's context mid-retry.
	discordMessageCancel  context.CancelFunc
	discordInteractionErr error
	kustomizationsByImage map[string][]fluxKustomizationRef
	workloads             []workloadRollout
	workloadsErr          error
	configMaps            []configMapChange
	configMapsErr         error
	repoKustomizations    map[string][]fluxKustomizationRef
	repoDiscoverErr       error
}

func (m *mockOutboundDispatcher) DiscoverImageKustomizations(ctx context.Context, imageNames []string) ([]fluxKustomizationRef, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.discoverErr != nil {
		return nil, m.discoverErr
	}
	var refs []fluxKustomizationRef
	for _, name := range imageNames {
		for _, ref := range m.kustomizationsByImage[name] {
			if !slices.Contains(refs, ref) {
				refs = append(refs, ref)
			}
		}
	}
	return refs, nil
}

func (m *mockOutboundDispatcher) ResolveFluxSourceRepo(ctx context.Context, obj FluxInvolvedObject) (string, error) {
	return m.sourceRepo, m.sourceRepoErr
}

func (m *mockOutboundDispatcher) DiscoverRepoKustomizations(ctx context.Context, repo string) ([]fluxKustomizationRef, error) {
	if m.repoDiscoverErr != nil {
		return nil, m.repoDiscoverErr
	}
	return m.repoKustomizations[strings.TrimSpace(repo)], nil
}

func (m *mockOutboundDispatcher) DispatchPrompt(ctx context.Context, req PromptRequest) error {
	m.mu.Lock()
	m.promptCalls = append(m.promptCalls, req)
	m.mu.Unlock()
	return m.promptErr
}

func (m *mockOutboundDispatcher) DispatchBackground(ctx context.Context, req DispatchRequest) error {
	m.mu.Lock()
	m.dispatchCalls = append(m.dispatchCalls, req)
	m.mu.Unlock()
	return m.dispatchErr
}

func (m *mockOutboundDispatcher) DispatchDirectMessage(ctx context.Context, req DirectMessageRequest) error {
	m.mu.Lock()
	m.directMessageCalls = append(m.directMessageCalls, req)
	m.mu.Unlock()
	return m.directMessageErr
}

func (m *mockOutboundDispatcher) PromptCalls() []PromptRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]PromptRequest(nil), m.promptCalls...)
}

func (m *mockOutboundDispatcher) DispatchCalls() []DispatchRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]DispatchRequest(nil), m.dispatchCalls...)
}

func (m *mockOutboundDispatcher) DirectMessageCalls() []DirectMessageRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]DirectMessageRequest(nil), m.directMessageCalls...)
}

func (m *mockOutboundDispatcher) DispatchDiscordMessage(ctx context.Context, req DiscordMessageIngressRequest) error {
	m.mu.Lock()
	m.discordMessageCalls = append(m.discordMessageCalls, req)
	m.discordMessageAttempts++
	if m.discordMessageFailCount > 0 {
		if m.discordMessageAttempts <= m.discordMessageFailCount {
			err := m.discordMessageErr
			if err == nil {
				err = errors.New("transient error")
			}
			m.mu.Unlock()
			return err
		}
		m.mu.Unlock()
		return nil
	}
	err := m.discordMessageErr
	cancel := m.discordMessageCancel
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return err
}

func (m *mockOutboundDispatcher) DispatchDiscordInteraction(ctx context.Context, interaction *discordgo.Interaction) error {
	m.mu.Lock()
	m.discordInteractionCalls = append(m.discordInteractionCalls, interaction)
	m.mu.Unlock()
	return m.discordInteractionErr
}

func (m *mockOutboundDispatcher) DispatchDiscordInteractionForAgent(ctx context.Context, agent string, interaction *discordgo.Interaction) error {
	m.mu.Lock()
	m.discordInteractionAgentCalls = append(m.discordInteractionAgentCalls, agent)
	m.discordInteractionCalls = append(m.discordInteractionCalls, interaction)
	m.mu.Unlock()
	return m.discordInteractionErr
}

func (m *mockOutboundDispatcher) DiscordInteractionAgentCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.discordInteractionAgentCalls...)
}

func (m *mockOutboundDispatcher) DiscordMessageCalls() []DiscordMessageIngressRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]DiscordMessageIngressRequest(nil), m.discordMessageCalls...)
}

func (m *mockOutboundDispatcher) DiscordInteractionCalls() []*discordgo.Interaction {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*discordgo.Interaction(nil), m.discordInteractionCalls...)
}

type kubeRoute struct {
	status int
	body   string
}

// kubeFixture is a declarative kube API mock that records every request in arrival order.
type kubeFixture struct {
	mu       sync.Mutex
	routes   map[string]kubeRoute
	requests []string
	queries  map[string]string
}

func newKubeFixture(t *testing.T, routes map[string]kubeRoute) (*kubeFixture, *httptest.Server) {
	t.Helper()
	f := &kubeFixture{routes: routes, queries: map[string]string{}}
	ts := httptest.NewServer(f)
	t.Cleanup(ts.Close)
	return f, ts
}

func (f *kubeFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.Method + " " + r.URL.Path
	f.mu.Lock()
	f.requests = append(f.requests, key)
	f.queries[key] = r.URL.RawQuery
	route, ok := f.routes[key]
	f.mu.Unlock()
	if !ok {
		route = kubeRoute{status: http.StatusNotFound, body: `{"kind":"Status","reason":"NotFound"}`}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(cmp.Or(route.status, http.StatusOK))
	_, _ = w.Write([]byte(route.body))
}

func (f *kubeFixture) Requests() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.requests...)
}

func (f *kubeFixture) Query(key string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.queries[key]
}
