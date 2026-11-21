package router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

// Synapse is a Postgres-free edge in front of the Restate PR lifecycle.
//
// Restate PRObject (key repo#pr) and RepoObject (key repo), in the prstate
// Restate service, are the only PR lifecycle authority:
// they hold the state, write every pr_registry row as a journaled
// projection, and run every PR- or repo-keyed effect (eri wakes, BuildRouter
// routes, aerial.pr.> facts, GitHub calls, Flux reconciles, image
// promotion). Synapse has no database connection and no registry.
//
// What synapse does: it verifies webhooks, normalizes GitHub, Flux,
// Kubernetes and CI observations (including the stateless "(#N)" squash
// commit parse and the GitHub commits-API PR lookup), renders the facts an
// event carries, and sends one-way events to Restate ingress (/send plus a
// deterministic Idempotency-Key derived from the inbound event identity).
// Events with a PR number go to PRObject; SHA-only events go to the
// RepoObject SHA handlers, which resolve the PR from their own durable
// index and forward. The only request-response calls are PR register
// (aerial-pr.sh needs the target_id back), readiness retry (409/202), and
// the read models the deploy watcher and build-metadata consumer need
// (deploy view, rollout index, build lifecycle, SHA lookup).
//
// Failure handling: a send failure returns an error so nothing is silently
// lost. JetStream jobs retry on the restate backoff, watcher ticks retry on
// the next tick, and HTTP handlers answer 503; Restate's ingress idempotency
// key and prstate's journaled effect keys dedupe the redelivery. A terminal
// 4xx rejection drops the job, since no retry can fix the payload. Every
// request is counted in synapse_restate_sends_total{handler,status}.
//
// Synapse still owns the duties that are not PR lifecycle: Discord gateway
// ingress and interactions, alert forwarding, PR-less system alerts, and
// Flux/Kubernetes watch observation as an event source.

// restateTimeout bounds every Restate ingress request so a wedged ingress
// can never stall a webhook, job or watcher tick indefinitely.
const restateTimeout = 2 * time.Second

// currentConfig returns the live config snapshot, tolerating a nil server.
func (s *RouterServer) currentConfig() Config {
	if s == nil || s.cfgSrc == nil {
		return Config{}
	}
	if cfg := s.cfgSrc.Current(); cfg != nil {
		return *cfg
	}
	return Config{}
}

// restateClient builds the Restate ingress client from the live config
// snapshot, so a hot reload of restate_url or the service names takes
// effect on the next request. restate_url is required at config load; an
// empty one here only happens on a server built without config.
func (s *RouterServer) restateClient() (*restateclient.Client, error) {
	cfg := s.currentConfig()
	url := strings.TrimSpace(cfg.RestateURL)
	if url == "" {
		return nil, errors.New("no restate_url configured")
	}
	return restateclient.NewClient(url, cfg.RestatePRService, cfg.RestateRepoService, &http.Client{Timeout: restateTimeout}), nil
}

// restateSendError is a failed Restate send. Nothing else performs the
// event's effects, so the job retries it with the restate backoff; a
// terminal failure drops the job instead.
type restateSendError struct {
	handler  string
	terminal bool
	err      error
}

func (e *restateSendError) Error() string {
	return fmt.Sprintf("restate %s send: %v", e.handler, e.err)
}

func (e *restateSendError) Unwrap() error { return e.err }

// restateFailure classifies one send failure, logging terminal rejections at
// error level because the job is dropped.
func restateFailure(ctx context.Context, handler string, err error) error {
	if restateclient.IsTerminal(err) {
		metrics.RecordRestateSend(handler, "terminal")
		slog.ErrorContext(ctx, "[synapse] [restate] send rejected terminally; dropping",
			"handler", handler, "error", err)
		return &restateSendError{handler: handler, terminal: true, err: err}
	}
	metrics.RecordRestateSend(handler, "error")
	slog.WarnContext(ctx, "[synapse] [restate] send failed; retrying",
		"handler", handler, "error", err)
	return &restateSendError{handler: handler, err: err}
}

// restateSendFailure reports whether err carries a Restate send failure, and
// whether it is terminal.
func restateSendFailure(err error) (failed, terminal bool) {
	var rerr *restateSendError
	if !errors.As(err, &rerr) {
		return false, false
	}
	return true, rerr.terminal
}

// restateSend runs one request through the configured client, counting and
// classifying the outcome.
func (s *RouterServer) restateSend(ctx context.Context, handler string, send func(*restateclient.Client) error) error {
	c, err := s.restateClient()
	if err != nil {
		return restateFailure(ctx, handler, err)
	}
	if err := send(c); err != nil {
		return restateFailure(ctx, handler, err)
	}
	metrics.RecordRestateSend(handler, "sent")
	return nil
}

// sendOwnedPR sends one PRObject event. The agent stays whatever the caller
// knows (empty when unknown); synapse never looks the owner up. PR-less
// events have no PRObject key and send nothing.
func (s *RouterServer) sendOwnedPR(ctx context.Context, handler string, ev restateclient.PREvent) error {
	if ev.PRNumber <= 0 || normalizeRepo(ev.Repo) == "" {
		return nil
	}
	return s.restateSend(ctx, handler, func(c *restateclient.Client) error {
		return c.SendPREvent(ctx, handler, ev)
	})
}

// sendOwnedRepo sends one RepoObject event.
func (s *RouterServer) sendOwnedRepo(ctx context.Context, handler string, ev restateclient.RepoEvent) error {
	if normalizeRepo(ev.Repo) == "" {
		return nil
	}
	return s.restateSend(ctx, handler, func(c *restateclient.Client) error {
		return c.SendRepoEvent(ctx, handler, ev)
	})
}

// sendOwnedPROrSHA sends ev to PRObject when it carries a PR number and
// otherwise wraps it for the RepoObject SHA handler keyed on sha. Events
// with neither a PR nor a usable SHA send nothing.
func (s *RouterServer) sendOwnedPROrSHA(ctx context.Context, shaHandler, prHandler, sha string, ev restateclient.PREvent) error {
	if ev.PRNumber > 0 {
		return s.sendOwnedPR(ctx, prHandler, ev)
	}
	sha = strings.TrimSpace(sha)
	if sha == "" || isZeroSHA(sha) || normalizeRepo(ev.Repo) == "" {
		return nil
	}
	return s.restateSend(ctx, shaHandler, func(c *restateclient.Client) error {
		return c.SendSHAEvent(ctx, shaHandler, restateclient.SHAEvent{Repo: ev.Repo, SHA: sha, Handler: prHandler, Event: ev})
	})
}

// facts wraps one rendered fact for a PREvent.
func facts(fact pubsub.PRLifecycleEvent) []pubsub.PRLifecycleEvent {
	return []pubsub.PRLifecycleEvent{fact}
}
