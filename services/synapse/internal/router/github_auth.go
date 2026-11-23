package router

// GitHub REST credential selection for synapse: RunServer reads the App
// private key once from env and builds a selector that asks the live config's
// github_credential (app|pat) on every Token call, so an aerial-config PR
// moves traffic without a restart. The PEM never enters the hot-reloadable
// Config; the App IDs (non-secret) come from the startup config snapshot.

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/githubauth"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
)

// gitHubAppEnv holds the GitHub App inputs read once at startup: the IDs
// from the startup config snapshot (service YAML, env fallback) and the
// GITHUB_APP_PRIVATE_KEY secret from env.
type gitHubAppEnv struct {
	AppID          string
	InstallationID string
	PrivateKeyPEM  string
}

// newGitHubTokenSource builds the App source (falling back to the startup
// GITHUB_PAT when it cannot mint, and plain PAT when the App env is unset)
// plus a PAT source, selected per call by cfgSrc's github_credential.
func newGitHubTokenSource(cfgSrc ConfigSource, env gitHubAppEnv) githubauth.TokenSource {
	cfg := cfgSrc.Current()
	appConfigured := strings.TrimSpace(env.AppID) != "" || strings.TrimSpace(env.InstallationID) != "" || strings.TrimSpace(env.PrivateKeyPEM) != ""
	slog.Info("[synapse] [github] credential sources ready", "app_env_present", appConfigured, "github_credential", cfg.GitHubCredential)
	app := githubauth.NewTokenSourceFromValues(env.AppID, env.InstallationID, env.PrivateKeyPEM, cfg.GitHubToken, githubauth.FromValuesOptions{
		APIURL:  cfg.GitHubAPIURL,
		Client:  tracing.ExternalClient(&http.Client{Timeout: 15 * time.Second}),
		Metrics: metrics.GitHubAuthMetrics{},
	})
	return githubauth.NewSelectingTokenSource(app, configPATSource{cfgSrc: cfgSrc}, func() string {
		c := cfgSrc.Current()
		if c == nil {
			return githubauth.SourceApp
		}
		metrics.SetGitHubCredentialConfigured(c.GitHubCredential)
		return c.GitHubCredential
	})
}

// RouterOption configures optional NewRouterServer dependencies.
type RouterOption func(*routerOptions)

type routerOptions struct {
	githubTokens githubauth.TokenSource
}

// WithGitHubTokenSource sets the credential source every GitHub REST call
// uses. Without it synapse attaches the live config's static GITHUB_PAT.
func WithGitHubTokenSource(src githubauth.TokenSource) RouterOption {
	return func(o *routerOptions) { o.githubTokens = src }
}

// configPATSource serves the live config snapshot's GitHubToken as a PAT.
type configPATSource struct{ cfgSrc ConfigSource }

func (c configPATSource) Token(context.Context) (string, string, error) {
	if c.cfgSrc == nil {
		return "", githubauth.SourcePAT, nil
	}
	cfg := c.cfgSrc.Current()
	if cfg == nil {
		return "", githubauth.SourcePAT, nil
	}
	return strings.TrimSpace(cfg.GitHubToken), githubauth.SourcePAT, nil
}

func (configPATSource) Invalidate(string) {}

// githubAPIHost returns the host the githubauth transport authenticates:
// the configured GitHub API URL's host, else api.github.com.
func githubAPIHost(cfgSrc ConfigSource) string {
	if cfgSrc != nil {
		if cfg := cfgSrc.Current(); cfg != nil {
			if u, err := url.Parse(strings.TrimSpace(cfg.GitHubAPIURL)); err == nil && u.Host != "" {
				return u.Host
			}
		}
	}
	return "api.github.com"
}
