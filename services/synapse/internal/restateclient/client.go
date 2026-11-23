// Package restateclient is the synapse-side Restate ingress invoker.
//
// It carries the frozen chunk-01 payloads (PREvent for PRObject keyed
// repo#prNumber, RepoEvent for RepoObject keyed repo) as JSON over the
// Restate HTTP ingress API. Lifecycle events are one-way sends
// (POST .../<handler>/send) carrying a deterministic Idempotency-Key, so
// synapse never waits behind a handler holding the exclusive object lock
// while it runs effects. Request-response invocations remain only for the
// sync APIs: Register, RetryReadiness and the read-only queries
// (GetDeployView, GetBuildLifecycle, ListRollouts, ResolvePRBySHA).
//
// Unmanaged-PR guard: this package has no BuildRouter send path and
// performs no build-ID resolution. Every event, owned or unmanaged,
// addresses PRObject or RepoObject only; a build ID rides opaquely in
// the payload for the Restate side to decide.
package restateclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/githubapi"
	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/restateingress"
	"github.com/azylman/aerial/pkg/strutil"
	"github.com/azylman/aerial/pkg/tracing"
)

// Frozen PRObject handler surface from chunk-01. SendPREvent rejects any
// handler outside this set without emitting a request.
var prHandlers = map[string]bool{
	prwire.PRHandlerCheckFailed:            true,
	prwire.PRHandlerCheckPassed:            true,
	prwire.PRHandlerCheckRerunRequested:    true,
	prwire.PRHandlerCIRecovered:            true,
	prwire.PRHandlerConflict:               true,
	prwire.PRHandlerDeploySuccess:          true,
	prwire.PRHandlerDeployAttemptFailed:    true,
	prwire.PRHandlerCollaboratorAlert:      true,
	prwire.PRHandlerConflictResolved:       true,
	prwire.PRHandlerMerged:                 true,
	prwire.PRHandlerClosed:                 true,
	prwire.PRHandlerReopened:               true,
	prwire.PRHandlerOpened:                 true,
	prwire.PRHandlerSelfRegistered:         true,
	prwire.PRHandlerReadinessRetry:         true,
	prwire.PRHandlerRolloutProgress:        true,
	prwire.PRHandlerKustomizationRequested: true,
	prwire.PRHandlerDeployFailed:           true,
	prwire.PRHandlerBuildMetadata:          true,
	prwire.PRHandlerSynchronized:           true,
	prwire.PRHandlerCheckStatus:            true,
	prwire.PRHandlerConflictCheck:          true,
	prwire.PRHandlerDeployObserved:         true,
	prwire.PRHandlerReconcileLost:          true,
}

// Frozen RepoObject handler surface from chunk-01. SendRepoEvent rejects
// any handler outside this set without emitting a request.
var repoHandlers = map[string]bool{
	prwire.RepoHandlerMainPush:            true,
	prwire.RepoHandlerFanout:              true,
	prwire.RepoHandlerKustomizationStatus: true,
	prwire.RepoHandlerAutoUpdate:          true,
	prwire.RepoHandlerWorkflowRerun:       true,
	prwire.RepoHandlerGitPush:             true,
	prwire.RepoHandlerImagePromotion:      true,
	prwire.RepoHandlerUntrackedReconcile:  true,
}

// SHA-only RepoObject handlers: each wraps the PRObject event it would
// deliver once RepoObject resolves the SHA from its durable index, holding
// the event until the SHA is indexed. The wrapped PRObject handler must be
// one RepoObject accepts for that SHA kind.
var repoSHAHandlers = map[string]map[string]bool{
	prwire.RepoHandlerCheckRunBySHA:    {prwire.PRHandlerCheckFailed: true, prwire.PRHandlerCheckPassed: true, prwire.PRHandlerCheckRerunRequested: true, prwire.PRHandlerCollaboratorAlert: true},
	prwire.RepoHandlerWorkflowRunBySHA: {prwire.PRHandlerCheckFailed: true, prwire.PRHandlerCheckPassed: true, prwire.PRHandlerCheckRerunRequested: true, prwire.PRHandlerCollaboratorAlert: true, prwire.PRHandlerDeployFailed: true},
	prwire.RepoHandlerPushBySHA:        {prwire.PRHandlerMerged: true, prwire.PRHandlerRolloutProgress: true},
}

// ErrInvalidEvent marks a payload rejected before any request: unknown
// handlers and missing keys. No retry can fix it, so IsTerminal reports it.
var ErrInvalidEvent = errors.New("invalid restate event")

// IsTerminal reports whether err can never succeed on retry: an invalid
// payload or a 4xx ingress rejection other than 408 Request Timeout and 429
// Too Many Requests. Transport failures, 5xx, and an unconfigured client are
// retryable.
func IsTerminal(err error) bool {
	if errors.Is(err, ErrInvalidEvent) {
		return true
	}
	httpErr, ok := errors.AsType[*restateingress.HTTPError](err)
	if !ok {
		return false
	}
	switch httpErr.Status {
	case http.StatusRequestTimeout, http.StatusTooManyRequests:
		return false
	default:
		return httpErr.Status >= 400 && httpErr.Status < 500
	}
}

type contextKey int

const (
	eventIdentityCtx contextKey = iota
)

// WithEventIdentity binds the identity of the inbound event (GitHub
// delivery ID, deploy idempotency key, kustomization observation) into ctx.
// One-way sends derive their Idempotency-Key from it.
func WithEventIdentity(ctx context.Context, identity string) context.Context {
	return context.WithValue(ctx, eventIdentityCtx, strings.TrimSpace(identity))
}

// EventIdentityFromContext returns the identity bound by WithEventIdentity.
func EventIdentityFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if val, ok := ctx.Value(eventIdentityCtx).(string); ok {
		return strings.TrimSpace(val)
	}
	return ""
}

// SendIdempotencyKey derives the deterministic Idempotency-Key for one
// one-way send: handler, event identity, and a digest of the exact payload.
// A redelivery of the same event re-sends the same payload and dedups at
// the Restate ingress; two distinct payloads from one event never collide.
// Without an identity the key falls back to handler plus payload digest.
func SendIdempotencyKey(identity, handler string, payload []byte) string {
	sum := sha256.Sum256(payload)
	digest := hex.EncodeToString(sum[:8])
	identity = strings.TrimSpace(identity)
	if identity == "" {
		return handler + ":" + digest
	}
	return handler + ":" + identity + ":" + digest
}

// Client invokes Restate Virtual Object handlers over the ingress HTTP API.
type Client struct {
	baseURL     string
	owner       string
	prService   string
	repoService string
	httpClient  *http.Client
}

// NewClient constructs a Client for the Restate ingress baseURL (for
// example http://restate.aerial:8080) with explicit Virtual Object service
// names. Empty service names fall back to the frozen PRObject/RepoObject
// defaults. owner qualifies bare repo names before they key an object; with
// an empty owner a bare repo is rejected with ErrInvalidEvent. An empty
// baseURL leaves the client disabled: every send fails closed without
// emitting a request. A nil httpClient gets a default.
func NewClient(baseURL, owner, prService, repoService string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &Client{
		baseURL:     strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		owner:       strings.TrimSpace(owner),
		prService:   strutil.FirstNonEmpty(strings.TrimSpace(prService), prwire.PRObjectServiceName),
		repoService: strutil.FirstNonEmpty(strings.TrimSpace(repoService), prwire.RepoObjectServiceName),
		httpClient:  tracing.Client(httpClient),
	}
}

// qualifyRepo owner-qualifies repo with the configured GitHub owner. An
// empty repo, or one still bare because no owner is configured, cannot key
// a Restate object and is rejected before any request.
func (c *Client) qualifyRepo(repo string) (string, error) {
	qualified := githubapi.QualifyRepo(c.owner, repo)
	if qualified == "" {
		return "", fmt.Errorf("%w: repo is required", ErrInvalidEvent)
	}
	if !strings.Contains(qualified, "/") {
		return "", fmt.Errorf("%w: repo %q has no owner and github_owner is unset", ErrInvalidEvent, qualified)
	}
	return qualified, nil
}

// SendPREvent enqueues ev at one frozen PRObject handler for repo#prNumber
// as a one-way send: Restate acknowledges once the invocation is durably
// accepted, never after the handler runs.
// Unmanaged collaborator events and owned events alike address PRObject
// only; the client never resolves a build ID and never signals BuildRouter.
// Every agent's events address the same single PRObject service: ev.Agent is
// an ownership hint carried in the payload, never a routing input.
func (c *Client) SendPREvent(ctx context.Context, handler string, ev prwire.PREvent) error {
	if !prHandlers[handler] {
		return fmt.Errorf("%w: unknown pr handler %q", ErrInvalidEvent, handler)
	}
	if strings.TrimSpace(c.baseURL) == "" {
		return errors.New("restate is not configured")
	}
	repo, err := c.qualifyRepo(ev.Repo)
	if err != nil {
		return err
	}
	ev.Repo = repo
	if ev.PRNumber <= 0 {
		return fmt.Errorf("%w: pr_number is required", ErrInvalidEvent)
	}
	return c.send(ctx, c.prService, prwire.PRObjectKey(ev.Repo, ev.PRNumber), handler, ev)
}

// SendRepoEvent enqueues ev at one frozen RepoObject handler for repo as a
// one-way send.
func (c *Client) SendRepoEvent(ctx context.Context, handler string, ev prwire.RepoEvent) error {
	if !repoHandlers[handler] {
		return fmt.Errorf("%w: unknown repo handler %q", ErrInvalidEvent, handler)
	}
	if strings.TrimSpace(c.baseURL) == "" {
		return errors.New("restate is not configured")
	}
	repo, err := c.qualifyRepo(ev.Repo)
	if err != nil {
		return err
	}
	ev.Repo = repo
	return c.send(ctx, c.repoService, prwire.RepoObjectKey(ev.Repo), handler, ev)
}

// SendSHAEvent enqueues one SHA-only event at a RepoObject SHA handler as a
// one-way send. RepoObject resolves the SHA from its own durable index and
// forwards ev.Event to ev.Handler on PRObject; synapse never resolves it.
func (c *Client) SendSHAEvent(ctx context.Context, handler string, ev prwire.SHAEvent) error {
	allowed, ok := repoSHAHandlers[handler]
	if !ok {
		return fmt.Errorf("%w: unknown repo sha handler %q", ErrInvalidEvent, handler)
	}
	if !allowed[ev.Handler] {
		return fmt.Errorf("%w: pr handler %q is not routable by %s", ErrInvalidEvent, ev.Handler, handler)
	}
	if strings.TrimSpace(c.baseURL) == "" {
		return errors.New("restate is not configured")
	}
	repo, err := c.qualifyRepo(ev.Repo)
	if err != nil {
		return err
	}
	ev.Repo = repo
	ev.SHA = strings.TrimSpace(ev.SHA)
	if ev.SHA == "" || prwire.IsZeroSHA(ev.SHA) {
		return fmt.Errorf("%w: sha is required", ErrInvalidEvent)
	}
	ev.Event.Repo = ev.Repo
	return c.send(ctx, c.repoService, prwire.RepoObjectKey(ev.Repo), handler, ev)
}

// ResolvePRBySHA invokes the shared, read-only RepoObject.ResolvePRBySHA
// lookup. It records nothing.
func (c *Client) ResolvePRBySHA(ctx context.Context, repo, sha string) (prwire.ResolveSHAResponse, error) {
	if strings.TrimSpace(c.baseURL) == "" {
		return prwire.ResolveSHAResponse{}, errors.New("restate is not configured")
	}
	repo, err := c.qualifyRepo(repo)
	if err != nil {
		return prwire.ResolveSHAResponse{}, err
	}
	sha = strings.TrimSpace(sha)
	if sha == "" {
		return prwire.ResolveSHAResponse{}, fmt.Errorf("%w: sha is required", ErrInvalidEvent)
	}
	var out prwire.ResolveSHAResponse
	if err := c.callService(ctx, c.repoService, prwire.RepoObjectKey(repo), prwire.RepoHandlerResolvePRBySHA, struct {
		Repo string `json:"repo"`
		SHA  string `json:"sha"`
	}{Repo: repo, SHA: sha}, &out); err != nil {
		return prwire.ResolveSHAResponse{}, err
	}
	return out, nil
}

// GetDeployView invokes the shared, read-only PRObject.GetDeployView query.
func (c *Client) GetDeployView(ctx context.Context, repo string, prNumber int) (prwire.DeployView, error) {
	return queryPR[prwire.DeployView](ctx, c, repo, prNumber, prwire.PRHandlerGetDeployView)
}

// queryPR invokes one shared, read-only PRObject query that takes no input.
func queryPR[T any](ctx context.Context, c *Client, repo string, prNumber int, handler string) (T, error) {
	var out T
	if strings.TrimSpace(c.baseURL) == "" {
		return out, errors.New("restate is not configured")
	}
	repo, err := c.qualifyRepo(repo)
	if err != nil {
		return out, err
	}
	if prNumber <= 0 {
		return out, fmt.Errorf("%w: pr_number is required", ErrInvalidEvent)
	}
	if err := c.call(ctx, prwire.PRObjectKey(repo, prNumber), handler, struct{}{}, &out); err != nil {
		var zero T
		return zero, err
	}
	return out, nil
}

// RolloutIndexKey is the RepoObject key holding the cross-repo rollout
// index; it mirrors prstate.RolloutIndexKey.
const RolloutIndexKey = "_rollouts"

// ListRollouts invokes the shared, read-only RepoObject.ListRollouts query
// on the rollout index: every PR deploying with recorded Kustomization
// requests, across repos.
func (c *Client) ListRollouts(ctx context.Context) ([]prwire.RolloutRef, error) {
	if strings.TrimSpace(c.baseURL) == "" {
		return nil, errors.New("restate is not configured")
	}
	var out struct {
		Rollouts []prwire.RolloutRef `json:"rollouts"`
	}
	if err := c.callService(ctx, c.repoService, RolloutIndexKey, prwire.RepoHandlerListRollouts, struct{}{}, &out); err != nil {
		return nil, err
	}
	return out.Rollouts, nil
}

// Register invokes PRObject.Register (request-response, exclusive) and
// returns the resolved target ID. Owner mismatches (409) and invalid
// requests (400) come back as terminal *restateingress.HTTPError values.
func (c *Client) Register(ctx context.Context, req prwire.RegisterRequest) (prwire.RegisterResponse, error) {
	if strings.TrimSpace(c.baseURL) == "" {
		return prwire.RegisterResponse{}, errors.New("restate is not configured")
	}
	repo, err := c.qualifyRepo(req.Repo)
	if err != nil {
		return prwire.RegisterResponse{}, err
	}
	req.Repo = repo
	if req.PRNumber <= 0 {
		return prwire.RegisterResponse{}, fmt.Errorf("%w: pr_number is required", ErrInvalidEvent)
	}
	var out prwire.RegisterResponse
	if err := c.call(ctx, prwire.PRObjectKey(req.Repo, req.PRNumber), prwire.PRHandlerRegister, req, &out); err != nil {
		return prwire.RegisterResponse{}, err
	}
	return out, nil
}

// GetBuildLifecycle invokes the shared, read-only PRObject.GetBuildLifecycle
// query. A PR Restate has never seen yields the zero lifecycle.
func (c *Client) GetBuildLifecycle(ctx context.Context, repo string, prNumber int) (prwire.BuildLifecycle, error) {
	return queryPR[prwire.BuildLifecycle](ctx, c, repo, prNumber, prwire.PRHandlerGetBuildLifecycle)
}

// RetryReadiness invokes PRObject.RetryReadiness (request-response,
// exclusive) and returns the 409/202 readiness decision. A missing merge
// SHA comes back as a terminal 400 *restateingress.HTTPError, a fenced caller not
// authorized for the stored owner as a terminal 409.
func (c *Client) RetryReadiness(ctx context.Context, repo string, prNumber int, req prwire.RetryReadinessRequest) (prwire.RetryReadinessResponse, error) {
	if strings.TrimSpace(c.baseURL) == "" {
		return prwire.RetryReadinessResponse{}, errors.New("restate is not configured")
	}
	repo, err := c.qualifyRepo(repo)
	if err != nil {
		return prwire.RetryReadinessResponse{}, err
	}
	if prNumber <= 0 {
		return prwire.RetryReadinessResponse{}, fmt.Errorf("%w: pr_number is required", ErrInvalidEvent)
	}
	var out prwire.RetryReadinessResponse
	if err := c.call(ctx, prwire.PRObjectKey(repo, prNumber), prwire.PRHandlerRetryReadiness, req, &out); err != nil {
		return prwire.RetryReadinessResponse{}, err
	}
	return out, nil
}

// call is one request-response PRObject invocation without an
// Idempotency-Key: the result reflects current state and must not be cached.
func (c *Client) call(ctx context.Context, key, handler string, payload, out any) error {
	return c.callService(ctx, c.prService, key, handler, payload, out)
}

// callService is one request-response invocation on service without an
// Idempotency-Key.
func (c *Client) callService(ctx context.Context, service, key, handler string, payload, out any) error {
	body, err := c.post(ctx, c.ingressURL(service, key, handler), "", payload)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode restate response: %w", err)
	}
	return nil
}

func (c *Client) send(ctx context.Context, service, key, handler string, payload any) error {
	keyBytes, err := json.Marshal(digestShape(idempotencyPayload(payload)))
	if err != nil {
		return fmt.Errorf("marshal restate idempotency payload: %w", err)
	}
	idKey := SendIdempotencyKey(EventIdentityFromContext(ctx), handler, keyBytes)
	_, err = c.post(ctx, c.ingressURL(service, key, handler)+"/send", idKey, payload)
	return err
}

// idempotencyPayload strips wall-clock noise from a payload before it is
// digested into the Idempotency-Key: kustomization requested_at stamps and
// fact occurred_at stamps are minted per attempt, so a retried event must
// still dedup at the ingress (the first accepted request wins). Every other
// payload digests as sent.
func idempotencyPayload(payload any) any {
	ev, ok := payload.(prwire.PREvent)
	if !ok || (len(ev.KustomizationRequests) == 0 && len(ev.Facts) == 0) {
		return payload
	}
	if len(ev.KustomizationRequests) > 0 {
		stripped := make(map[string]prwire.KustomizationRequest, len(ev.KustomizationRequests))
		for name, req := range ev.KustomizationRequests {
			req.RequestedAt = ""
			stripped[name] = req
		}
		ev.KustomizationRequests = stripped
	}
	if len(ev.Facts) > 0 {
		facts := make([]pubsub.PRLifecycleEvent, len(ev.Facts))
		for i, f := range ev.Facts {
			f.OccurredAt = time.Time{}
			facts[i] = f
		}
		ev.Facts = facts
	}
	return ev
}

// digestPREvent is the PREvent field order synapse digested before it
// adopted prwire (Agent after BuildID without omitempty, AutoUpdate after
// BuildMetadata, Kustomization last). Only the Idempotency-Key digest uses
// it; request bodies marshal prwire.PREvent as is. Keeping the digest bytes
// stable means a JetStream redelivery that straddles a synapse rollout still
// dedups at the Restate ingress: a duplicate HandleKustomizationRequested
// would otherwise start a new gate generation and re-patch the Flux reconcile.
type digestPREvent struct {
	Event                  string                                 `json:"event"`
	Repo                   string                                 `json:"repo"`
	PRNumber               int                                    `json:"pr_number"`
	HeadSHA                string                                 `json:"head_sha"`
	MergeSHA               string                                 `json:"merge_sha"`
	CheckName              string                                 `json:"check_name"`
	JobName                string                                 `json:"job_name"`
	Branch                 string                                 `json:"branch"`
	Title                  string                                 `json:"title"`
	GateGeneration         uint64                                 `json:"gate_generation"`
	ReopenEpoch            uint64                                 `json:"reopen_epoch"`
	Unmanaged              bool                                   `json:"unmanaged"`
	BuildID                string                                 `json:"build_id"`
	Agent                  string                                 `json:"agent"`
	BeforeSHA              string                                 `json:"before_sha,omitempty"`
	TargetID               string                                 `json:"target_id,omitempty"`
	CheckState             string                                 `json:"check_state,omitempty"`
	Jobs                   []string                               `json:"jobs,omitempty"`
	DeployedJobs           []string                               `json:"deployed_jobs,omitempty"`
	JobState               string                                 `json:"job_state,omitempty"`
	KustomizationRequests  map[string]prwire.KustomizationRequest `json:"kustomization_requests,omitempty"`
	DeployedKustomizations []string                               `json:"deployed_kustomizations,omitempty"`
	Banner                 string                                 `json:"banner,omitempty"`
	BuildMetadata          *prwire.BuildMetadata                  `json:"build_metadata,omitempty"`
	AutoUpdate             *prwire.AutoUpdatePolicy               `json:"auto_update,omitempty"`
	EriEffects             []prwire.EriEffect                     `json:"eri_effects,omitempty"`
	Facts                  []pubsub.PRLifecycleEvent              `json:"facts,omitempty"`
	ExpectedJobs           []string                               `json:"expected_jobs,omitempty"`
	ObservedReconcileAt    string                                 `json:"observed_reconcile_at,omitempty"`
	Kustomization          string                                 `json:"kustomization,omitempty"`
}

// digestSHAEvent is prwire.SHAEvent with its wrapped event in digest order.
type digestSHAEvent struct {
	Repo    string        `json:"repo"`
	SHA     string        `json:"sha"`
	Handler string        `json:"handler"`
	Event   digestPREvent `json:"event"`
}

func toDigestPREvent(ev prwire.PREvent) digestPREvent {
	return digestPREvent{
		Event: ev.Event, Repo: ev.Repo, PRNumber: ev.PRNumber, HeadSHA: ev.HeadSHA, MergeSHA: ev.MergeSHA,
		CheckName: ev.CheckName, JobName: ev.JobName, Branch: ev.Branch, Title: ev.Title,
		GateGeneration: ev.GateGeneration, ReopenEpoch: ev.ReopenEpoch, Unmanaged: ev.Unmanaged, BuildID: ev.BuildID,
		Agent: ev.Agent, BeforeSHA: ev.BeforeSHA, TargetID: ev.TargetID, CheckState: ev.CheckState, Jobs: ev.Jobs,
		DeployedJobs: ev.DeployedJobs, JobState: ev.JobState, KustomizationRequests: ev.KustomizationRequests,
		DeployedKustomizations: ev.DeployedKustomizations, Banner: ev.Banner,
		BuildMetadata: ev.BuildMetadata, AutoUpdate: ev.AutoUpdate, EriEffects: ev.EriEffects, Facts: ev.Facts,
		ExpectedJobs: ev.ExpectedJobs, ObservedReconcileAt: ev.ObservedReconcileAt, Kustomization: ev.Kustomization,
	}
}

// digestShape maps a payload to the shape its Idempotency-Key digests:
// PREvent and SHAEvent in their pre-prwire field order, every other payload
// as is.
func digestShape(payload any) any {
	switch ev := payload.(type) {
	case prwire.PREvent:
		return toDigestPREvent(ev)
	case prwire.SHAEvent:
		return digestSHAEvent{Repo: ev.Repo, SHA: ev.SHA, Handler: ev.Handler, Event: toDigestPREvent(ev.Event)}
	default:
		return payload
	}
}

func (c *Client) ingressURL(service, key, handler string) string {
	return c.baseURL + "/" + url.PathEscape(service) + "/" + url.PathEscape(key) + "/" + handler
}

// post sends payload to the ingress endpoint. Non-2xx responses come back
// as *restateingress.HTTPError; every other failure is wrapped as a failed
// ingress request.
func (c *Client) post(ctx context.Context, endpoint, idKey string, payload any) ([]byte, error) {
	body, err := restateingress.Post(ctx, c.httpClient, endpoint, idKey, payload, 64<<10)
	if err == nil {
		return body, nil
	}
	if _, ok := errors.AsType[*restateingress.HTTPError](err); ok {
		return nil, err
	}
	return nil, fmt.Errorf("restate ingress request failed: %w", err)
}
