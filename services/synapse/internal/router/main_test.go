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
	err                   error
}

func (m *mockEventBus) Publish(ctx context.Context, subject, msgID string, data any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.publishedTraceparents = append(m.publishedTraceparents, tracing.Traceparent(ctx))
	m.recordPublishedEvents(data)
	m.recordPublishedJobs(data)
	return nil
}

func (m *mockEventBus) recordPublishedEvents(data any) {
	switch a := data.(type) {
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

type loopbackOnlyTransport struct{ base http.RoundTripper }

func (l *loopbackOnlyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if h := req.URL.Hostname(); h != "127.0.0.1" && h != "::1" && h != "localhost" {
		return nil, errors.New("hermetic test guard: blocked non-loopback HTTP call to " + req.URL.Host)
	}
	return l.base.RoundTrip(req)
}

func init() {
	http.DefaultTransport = &loopbackOnlyTransport{base: http.DefaultTransport}
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

func TestRouterServer_AuthAccessors(t *testing.T) {
	t.Parallel()
	srv := &RouterServer{}
	if srv.k8sAuthEnabled() {
		t.Error("expected k8sAuthEnabled to be false for empty srv")
	}
	if srv.getAuthenticator() != nil {
		t.Error("expected getAuthenticator to be nil")
	}
	authCfg := newTestK8sAuthConfig(t, &staticAuthRoundTripper{})
	srvLazy := &RouterServer{cfgSrc: fixedConfig(Config{K8sAuthEnabled: true, K8sAuthConfig: authCfg})}
	if srvLazy.authenticator != nil {
		t.Error("expected authenticator to be uninitialized initially")
	}
	if !srvLazy.k8sAuthEnabled() {
		t.Error("expected k8sAuthEnabled to be true with K8sAuthEnabled in config")
	}
	if srvLazy.getAuthenticator() == nil {
		t.Error("expected getAuthenticator to lazily initialize authenticator")
	}
	if srvLazy.authenticator == nil {
		t.Error("expected authenticator to be set after getAuthenticator")
	}
}
