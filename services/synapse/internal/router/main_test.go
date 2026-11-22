package router

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/azylman/aerial/pkg/k8sauthtest"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
)

// --- Shared Test Mocks & Helpers ---

type mockEventBus struct {
	pubsub.Bus

	mu                   sync.Mutex
	insertedJobs         []GitHubWebhookArgs
	insertedDeployJobs   []DeployWebhookArgs
	insertedDiscordJobs  []DiscordIngressArgs
	insertedInteractJobs []DiscordInteractionArgs
	publishedPREvents    []pubsub.PRLifecycleEvent
	// publishedTraceparents records the W3C traceparent the real bus injects into
	// each published message's NATS headers.
	publishedTraceparents []string
	// subjects and events record every publish subject and each event-trigger payload.
	subjects []string
	events   []pubsub.ExternalEvent
	err      error
}

func (m *mockEventBus) Publish(ctx context.Context, subject, msgID string, data any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.publishedTraceparents = append(m.publishedTraceparents, tracing.Traceparent(ctx))
	m.subjects = append(m.subjects, subject)
	m.recordPublishedEvents(data)
	m.recordPublishedJobs(data)
	return nil
}

func (m *mockEventBus) recordPublishedEvents(data any) {
	switch a := data.(type) {
	case pubsub.ExternalEvent:
		m.events = append(m.events, a)
	case pubsub.PRLifecycleEvent:
		m.publishedPREvents = append(m.publishedPREvents, a)
	case *pubsub.PRLifecycleEvent:
		if a != nil {
			m.publishedPREvents = append(m.publishedPREvents, *a)
		}
	}
}

func (m *mockEventBus) recordPublishedJobs(data any) {
	switch a := data.(type) {
	case GitHubWebhookArgs:
		m.insertedJobs = append(m.insertedJobs, a)
	case *GitHubWebhookArgs:
		if a != nil {
			m.insertedJobs = append(m.insertedJobs, *a)
		}
	case DeployWebhookArgs:
		m.insertedDeployJobs = append(m.insertedDeployJobs, a)
	case *DeployWebhookArgs:
		if a != nil {
			m.insertedDeployJobs = append(m.insertedDeployJobs, *a)
		}
	case DiscordIngressArgs:
		m.insertedDiscordJobs = append(m.insertedDiscordJobs, a)
	case *DiscordIngressArgs:
		if a != nil {
			m.insertedDiscordJobs = append(m.insertedDiscordJobs, *a)
		}
	case DiscordInteractionArgs:
		m.insertedInteractJobs = append(m.insertedInteractJobs, a)
	case *DiscordInteractionArgs:
		if a != nil {
			m.insertedInteractJobs = append(m.insertedInteractJobs, *a)
		}
	}
}

func (m *mockEventBus) publishCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.subjects)
}

type errReader struct{}

func (errReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("simulated read error")
}

type zeroCallTransport struct {
	calls atomic.Int32
}

func (z *zeroCallTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	z.calls.Add(1)
	return nil, errors.New("unexpected outbound HTTP call")
}

func newTestRouter(cfg Config, client *http.Client, disp OutboundDispatcher, bus ...pubsub.Bus) *RouterServer {
	if client == nil && cfg.GitHubAPIURL == "" && cfg.KubeAPIServer == "" {
		client = &http.Client{Transport: &zeroCallTransport{}}
	}
	srv := NewRouterServer(fixedConfig(cfg), client)
	if disp != nil {
		srv.SetDispatcher(disp)
	}
	if len(bus) > 0 && bus[0] != nil {
		srv.SetEventBus(bus[0])
	}
	return srv
}

func doHTTPRequest(handler http.Handler, method, path string, body io.Reader, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, body)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// --- 4. Deploy Event Processing ---

type fixedConfigSource struct{ cfg Config }

func (f *fixedConfigSource) Current() *Config { return &f.cfg }

func fixedConfig(cfg Config) ConfigSource { return &fixedConfigSource{cfg: cfg} }

func TestK8sAuthEnabled(t *testing.T) {
	t.Parallel()
	authCfg := k8sauthtest.Config(t, &k8sauthtest.RoundTripper{})
	tests := []struct {
		name string
		srv  *RouterServer
		want bool
	}{
		{"no config source", &RouterServer{}, false},
		{"config disables k8s auth", &RouterServer{cfgSrc: fixedConfig(Config{})}, false},
		{"config enables k8s auth", &RouterServer{cfgSrc: fixedConfig(Config{K8sAuthEnabled: true, K8sAuthConfig: authCfg})}, true},
		{"enabled authenticator without config", &RouterServer{authenticator: k8sauthtest.Authenticator(t, &k8sauthtest.RoundTripper{})}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.srv.k8sAuthEnabled(); got != tc.want {
				t.Errorf("k8sAuthEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestGetAuthenticator pins the lazy authenticator: built once from the
// config when k8s auth is enabled, then returned unchanged.
func TestGetAuthenticator(t *testing.T) {
	t.Parallel()
	authCfg := k8sauthtest.Config(t, &k8sauthtest.RoundTripper{})
	tests := []struct {
		name    string
		srv     *RouterServer
		wantNil bool
	}{
		{"no config source", &RouterServer{}, true},
		{"config disables k8s auth", &RouterServer{cfgSrc: fixedConfig(Config{})}, true},
		{"config enables k8s auth", &RouterServer{cfgSrc: fixedConfig(Config{K8sAuthEnabled: true, K8sAuthConfig: authCfg})}, false},
		{"preset authenticator", &RouterServer{authenticator: k8sauthtest.Authenticator(t, &k8sauthtest.RoundTripper{})}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			first := tc.srv.getAuthenticator()
			if (first == nil) != tc.wantNil {
				t.Fatalf("getAuthenticator() = %v, want nil %v", first, tc.wantNil)
			}
			if second := tc.srv.getAuthenticator(); second != first {
				t.Errorf("second getAuthenticator() = %p, want the cached %p", second, first)
			}
		})
	}
}
