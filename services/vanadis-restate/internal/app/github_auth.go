package app

// GitHub REST authentication for vanadis-restate: every GitHub REST client
// (prstate, cilog, executor REST) shares one githubauth.Transport whose
// credential is picked per request by the github_credential setting in
// DefaultServiceConfigPath (vanadis_restate.github_credential: app|pat,
// default app). App secrets stay env; the choice is non-secret YAML read JIT
// so an aerial-config PR moves traffic without a restart.

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v3"

	"github.com/azylman/aerial/pkg/githubauth"
	"github.com/azylman/aerial/pkg/tracing"
)

// DefaultServiceConfigPath is the non-secret vanadis YAML holding the
// vanadis_restate section.
const DefaultServiceConfigPath = "/share/aerial-config/services/vanadis/vanadis.yaml"

// credentialReloadInterval bounds how often the credential file is re-read.
const credentialReloadInterval = 30 * time.Second

// githubAPIHost is the only host the transport authenticates.
const githubAPIHost = "api.github.com"

var (
	githubRequestsBySource = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "github_requests_by_source_total",
		Help: "GitHub REST requests sent, by credential source (app or pat).",
	}, []string{"source"})
	githubRateLimitRemaining = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "github_rate_limit_remaining",
		Help: "Last X-RateLimit-Remaining GitHub returned, by credential source.",
	}, []string{"source"})
	githubTokenMints = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "github_token_mint_total",
		Help: "GitHub App installation token mints, by result.",
	}, []string{"result"})
	githubCredentialConfigured = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "github_credential_configured",
		Help: "1 for the github_credential choice currently configured (app or pat), 0 otherwise.",
	}, []string{"credential"})
)

func init() {
	for _, c := range []prometheus.Collector{githubRequestsBySource, githubRateLimitRemaining, githubTokenMints, githubCredentialConfigured} {
		if err := prometheus.Register(c); err != nil {
			if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); !ok {
				panic(fmt.Sprintf("app: register metric: %v", err))
			}
		}
	}
}

// promGitHubMetrics backs githubauth.Metrics with the default registry.
type promGitHubMetrics struct{}

func (promGitHubMetrics) IncRequest(source string) {
	githubRequestsBySource.WithLabelValues(source).Inc()
}

func (promGitHubMetrics) SetRateLimitRemaining(source string, remaining float64) {
	githubRateLimitRemaining.WithLabelValues(source).Set(remaining)
}

func (promGitHubMetrics) IncTokenMint(result string) {
	githubTokenMints.WithLabelValues(result).Inc()
}

// serviceYAML is the subset of DefaultServiceConfigPath vanadis-restate reads.
type serviceYAML struct {
	VanadisRestate struct {
		GitHubCredential *string `yaml:"github_credential"`
		// GitHubOwner qualifies bare repo names; read once at startup.
		GitHubOwner string `yaml:"github_owner"`
	} `yaml:"vanadis_restate"`
}

// ReadGitHubOwner reads vanadis_restate.github_owner from the service config
// at path. A read or parse failure, or an unset or blank owner, is an error:
// there is no code default, so vanadis-restate refuses to start without it.
func ReadGitHubOwner(path string, readFile func(string) ([]byte, error)) (string, error) {
	data, err := readFile(path)
	if err != nil {
		return "", fmt.Errorf("read github_owner config %s: %w", path, err)
	}
	var cfg serviceYAML
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return "", fmt.Errorf("parse github_owner config %s: %w", path, err)
	}
	owner := strings.Trim(strings.TrimSpace(cfg.VanadisRestate.GitHubOwner), "/")
	if owner == "" {
		return "", fmt.Errorf("vanadis_restate.github_owner is not set in %s", path)
	}
	return owner, nil
}

// parseGitHubCredential resolves the github_credential choice from one read
// of the config file. A missing file or unset key selects app; a read or
// parse failure or an unknown value keeps last and reports the problem.
func parseGitHubCredential(data []byte, readErr error, last string) (string, string) {
	if readErr != nil {
		if errors.Is(readErr, os.ErrNotExist) {
			return githubauth.SourceApp, ""
		}
		return last, fmt.Sprintf("read config: %v", readErr)
	}
	var cfg serviceYAML
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return last, fmt.Sprintf("parse config: %v", err)
	}
	if cfg.VanadisRestate.GitHubCredential == nil {
		return githubauth.SourceApp, ""
	}
	switch v := strings.ToLower(strings.TrimSpace(*cfg.VanadisRestate.GitHubCredential)); v {
	case "":
		return githubauth.SourceApp, ""
	case githubauth.SourceApp, githubauth.SourcePAT:
		return v, ""
	default:
		return last, fmt.Sprintf("github_credential %q is not app or pat", v)
	}
}

// credentialSetting serves the github_credential choice, re-reading the
// config file at most every credentialReloadInterval and keeping the last
// good value on errors.
type credentialSetting struct {
	path     string
	readFile func(string) ([]byte, error)
	now      func() time.Time
	logger   *slog.Logger

	mu      sync.Mutex
	current string
	readAt  time.Time
	loaded  bool
}

func newCredentialSetting(path string, readFile func(string) ([]byte, error), now func() time.Time, logger *slog.Logger) *credentialSetting {
	return &credentialSetting{path: path, readFile: readFile, now: now, logger: logger, current: githubauth.SourceApp}
}

// Current returns the configured credential ("app" or "pat").
func (c *credentialSetting) Current() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if c.loaded && now.Sub(c.readAt) < credentialReloadInterval {
		return c.current
	}
	data, err := c.readFile(c.path)
	next, problem := parseGitHubCredential(data, err, c.current)
	if problem != "" {
		c.logger.Error("[vanadis-restate] github_credential config error; keeping last good value", "path", c.path, "error", problem, "credential", next)
	} else if next != c.current || !c.loaded {
		c.logger.Info("[vanadis-restate] github_credential configured", "credential", next)
	}
	c.current, c.readAt, c.loaded = next, now, true
	for _, cred := range []string{githubauth.SourceApp, githubauth.SourcePAT} {
		v := 0.0
		if cred == next {
			v = 1
		}
		githubCredentialConfigured.WithLabelValues(cred).Set(v)
	}
	return next
}

// gitHubAuthValues are the secret inputs read once at startup.
type gitHubAuthValues struct {
	AppID          string
	InstallationID string
	PrivateKeyPEM  string
	PAT            string
}

// newGitHubTokenSource builds the app source (falling back to PAT when it
// cannot mint, and plain PAT when the App env is unset) plus a static PAT
// source, selected per request by choose.
func newGitHubTokenSource(v gitHubAuthValues, choose func() string, mintClient *http.Client, logger *slog.Logger) githubauth.TokenSource {
	app := githubauth.NewTokenSourceFromValues(v.AppID, v.InstallationID, v.PrivateKeyPEM, v.PAT, githubauth.FromValuesOptions{
		Client:  mintClient,
		Logger:  logger,
		Metrics: promGitHubMetrics{},
	})
	return githubauth.NewSelectingTokenSource(app, githubauth.NewStaticTokenSource(v.PAT), choose)
}

// newGitHubHTTPClient returns a traced client whose transport injects the
// src credential into api.github.com requests over base (nil means
// http.DefaultTransport) and records per-source metrics. Requests to any other host pass through without credentials.
func newGitHubHTTPClient(src githubauth.TokenSource, base http.RoundTripper, timeout time.Duration) *http.Client {
	return tracing.ExternalClient(&http.Client{
		Timeout:   timeout,
		Transport: githubauth.NewTransport(base, src, githubAPIHost, githubauth.WithMetrics(promGitHubMetrics{})),
	})
}
