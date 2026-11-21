// Package restateclient is the synapse-side Restate ingress invoker.
//
// It carries the frozen chunk-01 payloads (PREvent for PRObject keyed
// repo#prNumber, RepoEvent for RepoObject keyed repo) as JSON over the
// Restate HTTP ingress API. Lifecycle events are one-way sends
// (POST .../<handler>/send) carrying a deterministic Idempotency-Key, so
// synapse never waits behind a handler holding the exclusive object lock
// while it runs effects. Request-response invocations remain only for the
// verdict calls the legacy path still consumes.
//
// Unmanaged-PR guard: this package has no BuildRouter send path and
// performs no build-ID resolution. Every event, owned or unmanaged,
// addresses PRObject or RepoObject only; a build ID rides opaquely in
// the payload for the Restate side to decide. pr_registry is never
// consulted here: it stays a read-only projection.
package restateclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/synapse/internal/github"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

// Frozen Restate service defaults from the chunk-01 handler contracts.
const (
	DefaultPRService   = "PRObject"
	DefaultRepoService = "RepoObject"
)

// PRObject handlers named by more than one routing table.
const (
	HandlerCheckFailed         = "HandleCheckFailed"
	HandlerCheckPassed         = "HandleCheckPassed"
	HandlerCheckRerunRequested = "HandleCheckRerunRequested"
	HandlerCollaboratorAlert   = "HandleCollaboratorAlert"
	HandlerMerged              = "HandleMerged"
	HandlerRolloutProgress     = "HandleRolloutProgress"
)

// Frozen PRObject handler surface from chunk-01. SendPREvent rejects any
// handler outside this set without emitting a request.
var prHandlers = map[string]bool{
	HandlerCheckFailed:             true,
	HandlerCheckPassed:             true,
	HandlerCheckRerunRequested:     true,
	"HandleCIRecovery":             true,
	"HandleCIRecovered":            true,
	"HandleConflict":               true,
	"HandleDeploySuccess":          true,
	"HandleDeployAttemptFailed":    true,
	HandlerCollaboratorAlert:       true,
	"HandleConflictDetected":       true,
	"HandleConflictPublished":      true,
	"HandleConflictResolved":       true,
	HandlerMerged:                  true,
	"HandleClosed":                 true,
	"HandleReopened":               true,
	"HandleOpened":                 true,
	"HandleReadinessRetry":         true,
	HandlerRolloutProgress:         true,
	"HandleDeployDispatched":       true,
	"HandleDeploySuccessPublished": true,
	"HandleKustomizationRequested": true,
	"HandleDeployFailed":           true,
	"HandleBuildMetadata":          true,
	"HandleSynchronized":           true,
	"HandleCheckStatus":            true,
	"HandleConflictCheck":          true,
}

// Frozen RepoObject handler surface from chunk-01. SendRepoEvent rejects
// any handler outside this set without emitting a request.
var repoHandlers = map[string]bool{
	"HandleMainPush":            true,
	"HandleFanout":              true,
	"HandleKustomizationStatus": true,
	"HandleAutoUpdate":          true,
	"HandleWorkflowRerun":       true,
}

// PREvent is the canonical PRObject payload. Field names and JSON tags
// mirror the frozen chunk-01 contract exactly; that package cannot be
// imported here because it lives under another module's internal tree,
// so TestPayloadJSONShape pins the key set against drift.
type PREvent struct {
	Event          string `json:"event"`
	Repo           string `json:"repo"`
	PRNumber       int    `json:"pr_number"`
	HeadSHA        string `json:"head_sha"`
	MergeSHA       string `json:"merge_sha"`
	CheckName      string `json:"check_name"`
	JobName        string `json:"job_name"`
	Branch         string `json:"branch"`
	Title          string `json:"title"`
	GateGeneration uint64 `json:"gate_generation"`
	ReopenEpoch    uint64 `json:"reopen_epoch"`
	Unmanaged      bool   `json:"unmanaged"`
	BuildID        string `json:"build_id"`
	Agent          string `json:"agent"`
	EffectsOwner   string `json:"effects_owner"`

	// Registry row payload: omitted when empty so events without registry
	// data keep the frozen wire key set. Tags mirror the prstate contract.
	BeforeSHA              string                                `json:"before_sha,omitempty"`
	TargetID               string                                `json:"target_id,omitempty"`
	CheckState             string                                `json:"check_state,omitempty"`
	Jobs                   []string                              `json:"jobs,omitempty"`
	DeployedJobs           []string                              `json:"deployed_jobs,omitempty"`
	JobState               string                                `json:"job_state,omitempty"`
	KustomizationRequests  map[string]model.KustomizationRequest `json:"kustomization_requests,omitempty"`
	DeployedKustomizations []string                              `json:"deployed_kustomizations,omitempty"`
	Banner                 string                                `json:"banner,omitempty"`
	Acknowledged           bool                                  `json:"acknowledged,omitempty"`
	BuildMetadata          *model.BuildMetadata                  `json:"build_metadata,omitempty"`
	// EriEffects are the rendered eri-bound effects for this event; tags
	// mirror the prstate EriEffect contract.
	EriEffects []EriEffect `json:"eri_effects,omitempty"`
	// Facts are the rendered aerial.pr.> lifecycle facts for this event;
	// PRObject publishes them when it owns the event's effects and records
	// them as intended otherwise.
	Facts []pubsub.PRLifecycleEvent `json:"facts,omitempty"`
}

// Eri effect kinds; values mirror the prstate Effect* constants.
const (
	EffectEriPrompt        = "eri_prompt"
	EffectEriBackground    = "eri_background"
	EffectEriDirectMessage = "eri_direct_message"
)

// GitHub effect kinds; values mirror the prstate Effect* constants. Synapse
// logs each legacy GitHub call as [synapse-effect] under these kinds.
const (
	EffectGitHubAutoUpdate    = "github_auto_update"
	EffectGitHubUpdateBranch  = "github_update_branch"
	EffectGitHubConflictCheck = "github_conflict_check"
	EffectGitHubRerun         = "github_rerun"
)

// EriEffect is one rendered eri-bound effect synapse attaches to a PREvent.
// PRObject fires it to the stored owner when it owns the event's effects
// and records it as intended otherwise. JSON tags mirror prstate exactly.
type EriEffect struct {
	Kind         string `json:"kind"`
	ChannelID    string `json:"channel_id,omitempty"`
	Content      string `json:"content"`
	Effort       string `json:"effort,omitempty"`
	SkipIfActive bool   `json:"skip_if_active,omitempty"`
}

// EffectMatchKey renders the generation-free effect key
// (repo#pr@sha:kind[:job=..][:check=..]) that prstate's EffectKey.MatchKey
// renders for the same inputs. Synapse never knows the gate generation, so
// the [synapse-effect] log and prstate's intended-effect record meet here.
func EffectMatchKey(repo string, prNumber int, headSHA, kind, jobName, checkName string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s#%d@%s:%s", model.NormalizeRepo(repo), prNumber, strings.TrimSpace(headSHA), strings.TrimSpace(kind))
	if clean := strings.TrimSpace(jobName); clean != "" {
		fmt.Fprintf(&sb, ":job=%s", clean)
	}
	if canon := github.CanonicalFailedCheckKey(checkName); canon != "" {
		fmt.Fprintf(&sb, ":check=%s", canon)
	}
	return sb.String()
}

// FactEffectPrefix prefixes every lifecycle-fact effect kind; the value
// mirrors prstate FactEffectPrefix.
const FactEffectPrefix = "pr_fact_"

// FactEffectKind is the effect kind for a lifecycle fact event
// (pr_fact_<event>), matching prstate FactEffectKind.
func FactEffectKind(event string) string {
	return FactEffectPrefix + strings.ToLower(strings.TrimSpace(event))
}

// FactMatchKey renders the generation-free key prstate records for one
// lifecycle fact: keyed on its head SHA, falling back to its merge SHA,
// plus its job and check names.
func FactMatchKey(fact pubsub.PRLifecycleEvent) string {
	sha := strings.TrimSpace(fact.HeadSHA)
	if sha == "" || isZeroSHA(sha) {
		sha = strings.TrimSpace(fact.MergeSHA)
	}
	if isZeroSHA(sha) {
		sha = ""
	}
	return EffectMatchKey(fact.Repo, fact.PRNumber, sha, FactEffectKind(fact.Event), fact.JobName, fact.CheckName)
}

func isZeroSHA(sha string) bool {
	return strings.Trim(sha, "0") == ""
}

// RegisterRequest is the PRObject.Register payload: the normalized register
// request plus the existing-owner fence and the headless target fallback.
// JSON tags mirror the prstate RegisterRequest contract exactly.
type RegisterRequest struct {
	Repo     string `json:"repo"`
	PRNumber int    `json:"pr_number"`
	Branch   string `json:"branch"`
	HeadSHA  string `json:"head_sha"`
	TargetID string `json:"target_id"`
	Title    string `json:"title,omitempty"`
	Agent    string `json:"agent,omitempty"`
	BotID    string `json:"bot_id,omitempty"`
	// Metadata carries the string-valued register metadata keys opaquely;
	// PRObject decides which keys it records.
	Metadata         map[string]string `json:"metadata,omitempty"`
	FallbackTargetID string            `json:"fallback_target_id,omitempty"`
	EnforceOwner     bool              `json:"enforce_owner,omitempty"`
	ExpectedOwner    string            `json:"expected_owner,omitempty"`
	// EffectsOwner is the request's effect-owner stamp: restate makes
	// PRObject write the registry row, legacy leaves it to synapse.
	EffectsOwner string `json:"effects_owner,omitempty"`
}

// RegisterResponse is the PRObject.Register result.
type RegisterResponse struct {
	TargetID string `json:"target_id"`
	Agent    string `json:"agent"`
	Status   string `json:"status"`
}

// BuildLifecycle is the PRObject.GetBuildLifecycle result: the registry
// lifecycle fence plus the revision-fenced build metadata snapshot.
type BuildLifecycle struct {
	Found            bool                 `json:"found"`
	Status           string               `json:"status"`
	HeadSHA          string               `json:"head_sha"`
	MergeSHA         string               `json:"merge_sha"`
	Started          bool                 `json:"started"`
	HasBuildMetadata bool                 `json:"has_build_metadata"`
	BuildMetadata    *model.BuildMetadata `json:"build_metadata,omitempty"`
	// Agent is the stored owner, empty when nothing claimed the PR yet.
	Agent string `json:"agent,omitempty"`
}

// SHA-only RepoObject handlers: each wraps the PRObject event it would
// deliver once RepoObject resolves the SHA from its durable index, holding
// the event until the SHA is indexed. The wrapped PRObject handler must be
// one RepoObject accepts for that SHA kind.
var repoSHAHandlers = map[string]map[string]bool{
	"HandleCheckRunBySHA":    {HandlerCheckFailed: true, HandlerCheckPassed: true, HandlerCheckRerunRequested: true, HandlerCollaboratorAlert: true},
	"HandleWorkflowRunBySHA": {HandlerCheckFailed: true, HandlerCheckPassed: true, HandlerCheckRerunRequested: true, HandlerCollaboratorAlert: true},
	"HandlePushBySHA":        {HandlerMerged: true, HandlerRolloutProgress: true},
}

// SHAEvent is the RepoObject SHA-only routing payload. JSON tags mirror the
// prstate SHAEvent contract exactly.
type SHAEvent struct {
	Repo    string  `json:"repo"`
	SHA     string  `json:"sha"`
	Handler string  `json:"handler"`
	Event   PREvent `json:"event"`
}

// ResolveSHAResponse is the RepoObject.ResolvePRBySHA result.
type ResolveSHAResponse struct {
	PRNumber int    `json:"pr_number"`
	Branch   string `json:"branch"`
	Found    bool   `json:"found"`
}

// RepoEvent is the canonical RepoObject payload: main-push indexing,
// dispatched-SHA tombstones, and fan-out sends only. JSON tags mirror
// the frozen chunk-01 contract exactly.
type RepoEvent struct {
	Repo         string `json:"repo"`
	MainSHA      string `json:"main_sha"`
	Generation   uint64 `json:"generation"`
	PRNumber     int    `json:"pr_number"`
	HeadSHA      string `json:"head_sha"`
	Agent        string `json:"agent"`
	EffectsOwner string `json:"effects_owner"`

	// GitHub effect payload, omitted when empty; tags mirror the prstate
	// RepoEvent contract. CheckName scopes an auto-update or rerun trigger,
	// RunID names the workflow run to rerun, and AutoUpdate carries the
	// policy synapse renders from its config.
	CheckName  string            `json:"check_name,omitempty"`
	RunID      int64             `json:"run_id,omitempty"`
	AutoUpdate *AutoUpdatePolicy `json:"auto_update,omitempty"`
}

// AutoUpdatePolicy is the stateless auto-update config RepoObject's cascade
// needs: allowlisted PR authors, whether this synapse is the default owner,
// and the repo's collaborator alert rules. JSON tags mirror prstate.
type AutoUpdatePolicy struct {
	Authors       []string            `json:"authors,omitempty"`
	DefaultOwner  bool                `json:"default_owner,omitempty"`
	Collaborators []CollaboratorAlert `json:"collaborators,omitempty"`
}

// CollaboratorAlert routes one collaborator's conflict DM.
type CollaboratorAlert struct {
	Author    string `json:"author"`
	ChannelID string `json:"channel_id"`
	Mention   string `json:"mention"`
}

// Effect ownership values carried in PREvent/RepoEvent.EffectsOwner. Each
// event is stamped exactly once when synapse first accepts it: legacy means
// synapse performed the side effects and prstate only records intent;
// restate means prstate executes them. A missing value means legacy.
const (
	EffectsOwnerLegacy  = "legacy"
	EffectsOwnerRestate = "restate"
)

// NormalizeEffectsOwner maps any stamp to legacy or restate. Only an exact
// (case-insensitive) restate stamp hands effects to Restate; anything else,
// including an empty or unknown stamp, stays legacy so a malformed value can
// never silently move effects off the legacy pipeline.
func NormalizeEffectsOwner(owner string) string {
	if strings.EqualFold(strings.TrimSpace(owner), EffectsOwnerRestate) {
		return EffectsOwnerRestate
	}
	return EffectsOwnerLegacy
}

// ErrInvalidEvent marks a payload rejected before any request: unknown
// handlers and missing keys. No retry can fix it, so IsTerminal reports it.
var ErrInvalidEvent = errors.New("invalid restate event")

// StatusError is a non-2xx Restate ingress response.
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("restate ingress returned status %d", e.Code)
	}
	return fmt.Sprintf("restate ingress returned status %d: %s", e.Code, e.Body)
}

// IsTerminal reports whether err can never succeed on retry: an invalid
// payload or a 4xx ingress rejection other than 408 Request Timeout and 429
// Too Many Requests. Transport failures, 5xx, and an unconfigured client are
// retryable.
func IsTerminal(err error) bool {
	if errors.Is(err, ErrInvalidEvent) {
		return true
	}
	var se *StatusError
	if !errors.As(err, &se) {
		return false
	}
	if se.Code == http.StatusRequestTimeout || se.Code == http.StatusTooManyRequests {
		return false
	}
	return se.Code >= 400 && se.Code < 500
}

// PRObjectKey returns the Restate key for a PR object: repo#prNumber.
// It mirrors the chunk-01 PRObjectKey normalization exactly.
func PRObjectKey(repo string, prNumber int) string {
	return fmt.Sprintf("%s#%d", model.NormalizeRepo(repo), prNumber)
}

// DeployDispatchDecision mirrors the Restate PRObject deploy verdict: the
// sole authority for whether synapse publishes deploy_success when
// restate_authoritative is on. JSON tags match the PRObject handler wire
// format exactly; a decode failure surfaces so the caller retries.
type DeployDispatchDecision struct {
	Disposition   string `json:"disposition"`
	ShouldPublish bool   `json:"should_publish"`
	Deduped       bool   `json:"deduped"`
	EffectKey     string `json:"effect_key"`
}

// ConflictDispatchDecision mirrors the Restate PRObject conflict verdict:
// the sole authority for whether synapse publishes a conflict republish
// when restate_authoritative is on. JSON tags match the PRObject handler
// wire format exactly; a decode failure surfaces so the caller retries.
type ConflictDispatchDecision struct {
	Disposition   string `json:"disposition"`
	ShouldPublish bool   `json:"should_publish"`
	Deduped       bool   `json:"deduped"`
}

// CIRecoveryDecision mirrors the Restate PRObject CI-recovery verdict: the
// sole authority for whether the PR is CI-failed for the event head SHA
// when restate_authoritative is on. JSON tags match the PRObject handler
// wire format exactly; a decode failure surfaces so the caller retries.
type CIRecoveryDecision struct {
	Disposition string `json:"disposition"`
	CIFailed    bool   `json:"cifailed"`
}

// RepoObjectKey returns the Restate key for a repo object: repo.
// It mirrors the chunk-01 RepoObjectKey normalization exactly.
func RepoObjectKey(repo string) string {
	return model.NormalizeRepo(repo)
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
	prService   string
	repoService string
	httpClient  *http.Client
}

// NewClient constructs a Client for the Restate ingress baseURL (for
// example http://restate.aerial:8080) with explicit Virtual Object service
// names. Empty service names fall back to the frozen PRObject/RepoObject
// defaults. An empty baseURL leaves the client disabled: every send fails
// closed without emitting a request. A nil httpClient gets a default.
func NewClient(baseURL, prService, repoService string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &Client{
		baseURL:     strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		prService:   model.FirstNonEmpty(strings.TrimSpace(prService), DefaultPRService),
		repoService: model.FirstNonEmpty(strings.TrimSpace(repoService), DefaultRepoService),
		httpClient:  tracing.Client(httpClient),
	}
}

// SendPREvent enqueues ev at one frozen PRObject handler for repo#prNumber
// as a one-way send: Restate acknowledges once the invocation is durably
// accepted, never after the handler runs.
// Unmanaged collaborator events and owned events alike address PRObject
// only; the client never resolves a build ID and never signals BuildRouter.
// Every agent's events address the same single PRObject service: ev.Agent is
// an ownership hint carried in the payload, never a routing input.
func (c *Client) SendPREvent(ctx context.Context, handler string, ev PREvent) error {
	if !prHandlers[handler] {
		return fmt.Errorf("%w: unknown pr handler %q", ErrInvalidEvent, handler)
	}
	if strings.TrimSpace(c.baseURL) == "" {
		return errors.New("restate is not configured")
	}
	ev.Repo = model.NormalizeRepo(ev.Repo)
	if ev.Repo == "" {
		return fmt.Errorf("%w: repo is required", ErrInvalidEvent)
	}
	if ev.PRNumber <= 0 {
		return fmt.Errorf("%w: pr_number is required", ErrInvalidEvent)
	}
	ev.EffectsOwner = NormalizeEffectsOwner(ev.EffectsOwner)
	return c.send(ctx, c.prService, PRObjectKey(ev.Repo, ev.PRNumber), handler, ev)
}

// SendRepoEvent enqueues ev at one frozen RepoObject handler for repo as a
// one-way send.
func (c *Client) SendRepoEvent(ctx context.Context, handler string, ev RepoEvent) error {
	if !repoHandlers[handler] {
		return fmt.Errorf("%w: unknown repo handler %q", ErrInvalidEvent, handler)
	}
	if strings.TrimSpace(c.baseURL) == "" {
		return errors.New("restate is not configured")
	}
	ev.Repo = model.NormalizeRepo(ev.Repo)
	if ev.Repo == "" {
		return fmt.Errorf("%w: repo is required", ErrInvalidEvent)
	}
	ev.EffectsOwner = NormalizeEffectsOwner(ev.EffectsOwner)
	return c.send(ctx, c.repoService, RepoObjectKey(ev.Repo), handler, ev)
}

// SendSHAEvent enqueues one SHA-only event at a RepoObject SHA handler as a
// one-way send. RepoObject resolves the SHA from its own durable index and
// forwards ev.Event to ev.Handler on PRObject; synapse never resolves it.
func (c *Client) SendSHAEvent(ctx context.Context, handler string, ev SHAEvent) error {
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
	ev.Repo = model.NormalizeRepo(ev.Repo)
	ev.SHA = strings.TrimSpace(ev.SHA)
	if ev.Repo == "" {
		return fmt.Errorf("%w: repo is required", ErrInvalidEvent)
	}
	if ev.SHA == "" || isZeroSHA(ev.SHA) {
		return fmt.Errorf("%w: sha is required", ErrInvalidEvent)
	}
	ev.Event.Repo = ev.Repo
	ev.Event.EffectsOwner = NormalizeEffectsOwner(ev.Event.EffectsOwner)
	return c.send(ctx, c.repoService, RepoObjectKey(ev.Repo), handler, ev)
}

// ResolvePRBySHA invokes the shared, read-only RepoObject.ResolvePRBySHA
// lookup. It records nothing; shadow comparisons use it read-only.
func (c *Client) ResolvePRBySHA(ctx context.Context, repo, sha string) (ResolveSHAResponse, error) {
	if strings.TrimSpace(c.baseURL) == "" {
		return ResolveSHAResponse{}, errors.New("restate is not configured")
	}
	repo = model.NormalizeRepo(repo)
	sha = strings.TrimSpace(sha)
	if repo == "" || sha == "" {
		return ResolveSHAResponse{}, fmt.Errorf("%w: repo and sha are required", ErrInvalidEvent)
	}
	var out ResolveSHAResponse
	if err := c.callService(ctx, c.repoService, RepoObjectKey(repo), "ResolvePRBySHA", struct {
		Repo string `json:"repo"`
		SHA  string `json:"sha"`
	}{Repo: repo, SHA: sha}, &out); err != nil {
		return ResolveSHAResponse{}, err
	}
	return out, nil
}

// InvokePREvent delivers ev to one frozen PRObject handler and decodes the
// JSON verdict into out. Unknown handlers, unconfigured clients, and invalid
// payloads fail closed without emitting a request; transport failures and
// undecodable bodies surface so the caller retries instead of losing the
// signal. Verdicts are request-response reads of current state, so they
// carry no Idempotency-Key: a cached verdict would go stale.
func (c *Client) InvokePREvent(ctx context.Context, handler string, ev PREvent, out any) error {
	if !prHandlers[handler] {
		return fmt.Errorf("%w: unknown pr handler %q", ErrInvalidEvent, handler)
	}
	if strings.TrimSpace(c.baseURL) == "" {
		return errors.New("restate is not configured")
	}
	ev.Repo = model.NormalizeRepo(ev.Repo)
	if ev.Repo == "" {
		return fmt.Errorf("%w: repo is required", ErrInvalidEvent)
	}
	if ev.PRNumber <= 0 {
		return fmt.Errorf("%w: pr_number is required", ErrInvalidEvent)
	}
	if out == nil {
		return fmt.Errorf("%w: output is required", ErrInvalidEvent)
	}
	ev.EffectsOwner = NormalizeEffectsOwner(ev.EffectsOwner)
	bodyBytes, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal restate payload: %w", err)
	}
	body, err := c.post(ctx, c.ingressURL(c.prService, PRObjectKey(ev.Repo, ev.PRNumber), handler), bodyBytes, "")
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode restate response: %w", err)
	}
	return nil
}

// Register invokes PRObject.Register (request-response, exclusive) and
// returns the resolved target ID. Owner mismatches (409) and invalid
// requests (400) come back as terminal StatusErrors.
func (c *Client) Register(ctx context.Context, req RegisterRequest) (RegisterResponse, error) {
	if strings.TrimSpace(c.baseURL) == "" {
		return RegisterResponse{}, errors.New("restate is not configured")
	}
	req.Repo = model.NormalizeRepo(req.Repo)
	if req.Repo == "" || req.PRNumber <= 0 {
		return RegisterResponse{}, fmt.Errorf("%w: repo and pr_number are required", ErrInvalidEvent)
	}
	var out RegisterResponse
	if err := c.call(ctx, PRObjectKey(req.Repo, req.PRNumber), "Register", req, &out); err != nil {
		return RegisterResponse{}, err
	}
	return out, nil
}

// GetBuildLifecycle invokes the shared, read-only PRObject.GetBuildLifecycle
// query. A PR Restate has never seen yields the zero lifecycle.
func (c *Client) GetBuildLifecycle(ctx context.Context, repo string, prNumber int) (BuildLifecycle, error) {
	if strings.TrimSpace(c.baseURL) == "" {
		return BuildLifecycle{}, errors.New("restate is not configured")
	}
	repo = model.NormalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return BuildLifecycle{}, fmt.Errorf("%w: repo and pr_number are required", ErrInvalidEvent)
	}
	var out BuildLifecycle
	if err := c.call(ctx, PRObjectKey(repo, prNumber), "GetBuildLifecycle", struct{}{}, &out); err != nil {
		return BuildLifecycle{}, err
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
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal restate payload: %w", err)
	}
	body, err := c.post(ctx, c.ingressURL(service, key, handler), bodyBytes, "")
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode restate response: %w", err)
	}
	return nil
}

func (c *Client) send(ctx context.Context, service, key, handler string, payload any) error {
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal restate payload: %w", err)
	}
	keyBytes, err := json.Marshal(idempotencyPayload(payload))
	if err != nil {
		return fmt.Errorf("marshal restate idempotency payload: %w", err)
	}
	idKey := SendIdempotencyKey(EventIdentityFromContext(ctx), handler, keyBytes)
	_, err = c.post(ctx, c.ingressURL(service, key, handler)+"/send", bodyBytes, idKey)
	return err
}

// idempotencyPayload strips wall-clock noise from a payload before it is
// digested into the Idempotency-Key: kustomization requested_at stamps and
// fact occurred_at stamps are minted per attempt, so a retried event must
// still dedup at the ingress (the first accepted request wins). Every other
// payload digests as sent.
func idempotencyPayload(payload any) any {
	ev, ok := payload.(PREvent)
	if !ok || (len(ev.KustomizationRequests) == 0 && len(ev.Facts) == 0) {
		return payload
	}
	if len(ev.KustomizationRequests) > 0 {
		stripped := make(map[string]model.KustomizationRequest, len(ev.KustomizationRequests))
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

func (c *Client) ingressURL(service, key, handler string) string {
	return c.baseURL + "/" + url.PathEscape(service) + "/" + url.PathEscape(key) + "/" + handler
}

func (c *Client) post(ctx context.Context, endpoint string, bodyBytes []byte, idKey string) ([]byte, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("create restate request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if idKey != "" {
		httpReq.Header.Set("Idempotency-Key", idKey)
	}
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("restate ingress request failed: %w", err)
	}
	defer resp.Body.Close()
	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if readErr != nil {
			return nil, fmt.Errorf("drain restate response: %w", readErr)
		}
		return respBody, nil
	}
	if readErr != nil {
		return nil, &StatusError{Code: resp.StatusCode}
	}
	return nil, &StatusError{Code: resp.StatusCode, Body: string(respBody)}
}
