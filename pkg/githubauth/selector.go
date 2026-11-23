package githubauth

import (
	"context"
	"strings"
)

type selectingSource struct {
	app    TokenSource
	pat    TokenSource
	choose func() string
}

// NewSelectingTokenSource returns a TokenSource that asks choose on every
// Token call which credential to use: SourcePAT ("pat") routes to pat, any
// other value (including "" and unknown values) routes to app. Reading the
// choice per call lets a config hot reload move traffic without a restart.
// A nil choose always selects app. The selector never switches to pat on a
// rate limit; the app source falls back to its own PAT only when it cannot
// mint.
func NewSelectingTokenSource(app, pat TokenSource, choose func() string) TokenSource {
	return selectingSource{app: app, pat: pat, choose: choose}
}

func (s selectingSource) pick() TokenSource {
	if s.choose != nil && strings.EqualFold(strings.TrimSpace(s.choose()), SourcePAT) {
		return s.pat
	}
	return s.app
}

func (s selectingSource) Token(ctx context.Context) (string, string, error) {
	return s.pick().Token(ctx)
}

// Invalidate forwards to both sources; each drops value only if it caches it.
func (s selectingSource) Invalidate(value string) {
	s.app.Invalidate(value)
	s.pat.Invalidate(value)
}
