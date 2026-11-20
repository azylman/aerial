// Package restateclient is the dark synapse-side Restate ingress invoker.
//
// It carries the frozen chunk-01 payloads (PREvent for PRObject keyed
// repo#prNumber, RepoEvent for RepoObject keyed repo) as JSON over the
// Restate HTTP ingress API. The client is constructed but called by no
// production path yet; Restate-side handler implementations and consumer
// wiring land in later chunks.
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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

// Frozen Restate service defaults from the chunk-01 handler contracts.
const (
	DefaultPRService   = "PRObject"
	DefaultRepoService = "RepoObject"
)

// Frozen PRObject handler surface from chunk-01. SendPREvent rejects any
// handler outside this set without emitting a request.
var prHandlers = map[string]bool{
	"HandleCheckFailed":            true,
	"HandleCheckPassed":            true,
	"HandleCheckRerunRequested":    true,
	"HandleCollaboratorAlert":      true,
	"HandleConflictDetected":       true,
	"HandleConflictResolved":       true,
	"HandleMerged":                 true,
	"HandleClosed":                 true,
	"HandleReopened":               true,
	"HandleReadinessRetry":         true,
	"HandleRolloutProgress":        true,
	"HandleDeployDispatched":       true,
	"HandleDeploySuccessPublished": true,
	"HandleKustomizationRequested": true,
}

// Frozen RepoObject handler surface from chunk-01. SendRepoEvent rejects
// any handler outside this set without emitting a request.
var repoHandlers = map[string]bool{
	"HandleMainPush":            true,
	"HandleFanout":              true,
	"HandleKustomizationStatus": true,
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
}

// RepoEvent is the canonical RepoObject payload: main-push indexing,
// dispatched-SHA tombstones, and fan-out sends only. JSON tags mirror
// the frozen chunk-01 contract exactly.
type RepoEvent struct {
	Repo       string `json:"repo"`
	MainSHA    string `json:"main_sha"`
	Generation uint64 `json:"generation"`
	PRNumber   int    `json:"pr_number"`
	HeadSHA    string `json:"head_sha"`
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

// RepoObjectKey returns the Restate key for a repo object: repo.
// It mirrors the chunk-01 RepoObjectKey normalization exactly.
func RepoObjectKey(repo string) string {
	return model.NormalizeRepo(repo)
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

// SendPREvent delivers ev to one frozen PRObject handler for repo#prNumber.
// Unmanaged collaborator events and owned events alike address PRObject
// only; the client never resolves a build ID and never signals BuildRouter.
func (c *Client) SendPREvent(ctx context.Context, handler string, ev PREvent) error {
	if !prHandlers[handler] {
		return fmt.Errorf("unknown pr handler %q", handler)
	}
	if strings.TrimSpace(c.baseURL) == "" {
		return errors.New("restate is not configured")
	}
	ev.Repo = model.NormalizeRepo(ev.Repo)
	if ev.Repo == "" {
		return errors.New("repo is required")
	}
	if ev.PRNumber <= 0 {
		return errors.New("pr_number is required")
	}
	return c.send(ctx, c.prService, PRObjectKey(ev.Repo, ev.PRNumber), handler, ev)
}

// SendRepoEvent delivers ev to one frozen RepoObject handler for repo.
func (c *Client) SendRepoEvent(ctx context.Context, handler string, ev RepoEvent) error {
	if !repoHandlers[handler] {
		return fmt.Errorf("unknown repo handler %q", handler)
	}
	if strings.TrimSpace(c.baseURL) == "" {
		return errors.New("restate is not configured")
	}
	ev.Repo = model.NormalizeRepo(ev.Repo)
	if ev.Repo == "" {
		return errors.New("repo is required")
	}
	return c.send(ctx, c.repoService, RepoObjectKey(ev.Repo), handler, ev)
}

// InvokePREvent delivers ev to one frozen PRObject handler and decodes the
// JSON verdict into out. Unknown handlers, unconfigured clients, and invalid
// payloads fail closed without emitting a request; transport failures and
// undecodable bodies surface so the caller retries instead of losing the
// signal.
func (c *Client) InvokePREvent(ctx context.Context, handler string, ev PREvent, out any) error {
	if !prHandlers[handler] {
		return fmt.Errorf("unknown pr handler %q", handler)
	}
	if strings.TrimSpace(c.baseURL) == "" {
		return errors.New("restate is not configured")
	}
	ev.Repo = model.NormalizeRepo(ev.Repo)
	if ev.Repo == "" {
		return errors.New("repo is required")
	}
	if ev.PRNumber <= 0 {
		return errors.New("pr_number is required")
	}
	if out == nil {
		return errors.New("output is required")
	}
	body, err := c.post(ctx, c.prService, PRObjectKey(ev.Repo, ev.PRNumber), handler, ev)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode restate response: %w", err)
	}
	return nil
}

func (c *Client) send(ctx context.Context, service, key, handler string, payload any) error {
	_, err := c.post(ctx, service, key, handler, payload)
	return err
}

func (c *Client) post(ctx context.Context, service, key, handler string, payload any) ([]byte, error) {
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal restate payload: %w", err)
	}
	endpoint := c.baseURL + "/" + service + "/" + url.PathEscape(key) + "/" + handler
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("create restate request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("restate ingress request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if readErr != nil {
			return nil, fmt.Errorf("drain restate response: %w", readErr)
		}
		return respBody, nil
	}
	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if readErr != nil {
		return nil, fmt.Errorf("restate ingress returned status %d", resp.StatusCode)
	}
	return nil, fmt.Errorf("restate ingress returned status %d: %s", resp.StatusCode, string(respBody))
}
