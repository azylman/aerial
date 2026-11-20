package executor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/cilog"
	"github.com/azylman/aerial/services/vanadis-restate/internal/ericlient"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/prompt"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
	"github.com/azylman/aerial/services/vanadis-restate/internal/widget"
)

const (
	DefaultShareRoot         = "/share"
	MaxCIRemediationAttempts = 5

	// Local vocabulary retained after the frozen-helper extraction; the
	// canonical pure repo/branch helpers live in internal/model.
	chunkModeAction  = "action"
	defaultRepoOwner = "azylman"
	effortMedium     = "medium"
)

// Type aliases preserving the executor public surface.
type (
	EriClient                = ericlient.EriClient
	HTTPEriClient            = ericlient.HTTPEriClient
	RestateEriClient         = ericlient.RestateEriClient
	StatusWidgetDeliverer    = ericlient.StatusWidgetDeliverer
	BrainDispatchRequest     = ericlient.BrainDispatchRequest
	StatusWidgetChunk        = ericlient.StatusWidgetChunk
	StatusWidgetRequest      = ericlient.StatusWidgetRequest
	SyncChunkPRWidgetRequest = ericlient.SyncChunkPRWidgetRequest
)

var (
	ErrDispatchActive = ericlient.ErrDispatchActive

	NewHTTPEriClient    = ericlient.NewHTTPEriClient
	NewRestateEriClient = ericlient.NewRestateEriClient

	FormatChunkActiveStep     = widget.FormatChunkActiveStep
	FindSymbolSpan            = prompt.FindSymbolSpan
	ResolveSymbolSpans        = prompt.ResolveSymbolSpans
	FormatCIFailurePrompt     = prompt.FormatCIFailurePrompt
	FormatNoPRResumePrompt    = prompt.FormatNoPRResumePrompt
	FormatConflictPrompt      = prompt.FormatConflictPrompt
	FormatDeployFailureBanner = prompt.FormatDeployFailureBanner
	FormatDeployFailurePrompt = prompt.FormatDeployFailurePrompt
	ExtractCIFailureDetails   = cilog.ExtractCIFailureDetails
)

// Executor coordinates chunk dispatch, widget sync, CI logs, prompts, and event publishing.
type Executor struct {
	shareRoot    string
	EriClient    EriClient
	bus          pubsub.Bus
	gitHubToken  string
	GitHubAPIURL string
	HTTPClient   *http.Client
	deployUIURL  string
}

// NewExecutor creates a new Executor instance.
func NewExecutor(shareRoot string, eriClient EriClient, bus pubsub.Bus, gitHubToken string, httpClient *http.Client, deployUIURL ...string) *Executor {
	if shareRoot == "" {
		shareRoot = DefaultShareRoot
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	var dURL string
	if len(deployUIURL) > 0 {
		dURL = deployUIURL[0]
	}
	return &Executor{
		shareRoot:    shareRoot,
		EriClient:    eriClient,
		bus:          bus,
		gitHubToken:  gitHubToken,
		GitHubAPIURL: "https://api.github.com",
		HTTPClient:   httpClient,
		deployUIURL:  dURL,
	}
}

func (e *Executor) ciClient() *cilog.Client {
	if e == nil {
		return nil
	}
	c := cilog.NewClient(e.gitHubToken, e.HTTPClient)
	c.GitHubAPIURL = e.GitHubAPIURL
	if e.HTTPClient == nil {
		c.HTTPClient = nil
	}
	return c
}

// FormatChunkPrompt renders a structured prompt for executing a specific chunk.
func (e *Executor) FormatChunkPrompt(build model.VanadisBuild, chunks []model.VanadisChunk, activeIdx int) (string, error) {
	root := DefaultShareRoot
	if e != nil && e.shareRoot != "" {
		root = e.shareRoot
	}
	return prompt.FormatChunkPromptWithShareRoot(build, chunks, activeIdx, root)
}

// ExecuteLaunchChunk reserves the chunk in PostgreSQL and dispatches it directly to Eri via POST /dispatch.
// It explicitly DOES NOT publish to aerial.jobs.vanadis.chunk.
func (e *Executor) ExecuteLaunchChunk(ctx context.Context, st store.VanadisStore, build model.VanadisBuild, chunks []model.VanadisChunk, activeIdx int, notice string) error {
	if e == nil || e.EriClient == nil {
		return errors.New("executor or eri client is nil")
	}
	if activeIdx < 0 || activeIdx >= len(chunks) {
		return fmt.Errorf("chunk index %d out of bounds", activeIdx)
	}
	chunk := chunks[activeIdx]

	var reserveErr error
	if st != nil {
		reserveErr = st.ReserveChunkForLaunch(ctx, chunk.ID)
	}
	if reserveErr != nil && chunk.Mode == chunkModeAction && chunk.StallNudges > 0 {
		return fmt.Errorf("reserve action generation: %w", reserveErr)
	}
	if reserveErr != nil && !errors.Is(reserveErr, store.ErrChunkNotFound) {
		slog.WarnContext(ctx, "[vanadis-restate] failed reserving chunk for launch", "chunk_id", chunk.ID, "error", reserveErr)
	}

	chunkPrompt, err := e.FormatChunkPrompt(build, chunks, activeIdx)
	if err != nil {
		return fmt.Errorf("format chunk prompt: %w", err)
	}

	agent := strings.TrimSpace(build.Agent)
	if agent == "" {
		agent = "aerial"
	}
	req := BrainDispatchRequest{
		ID:             model.ActionDispatchID(chunk),
		Agent:          agent,
		Source:         "vanadis-restate",
		SessionID:      "vanadis-" + strings.TrimSpace(chunk.ID),
		TargetID:       strings.TrimSpace(build.ThreadID),
		VanadisChunkID: strings.TrimSpace(chunk.ID),
		VanadisBuildID: strings.TrimSpace(build.ID),
		Prompt:         notice + chunkPrompt,
		Effort:         store.CapChunkEffort(chunk.Effort),
		SkipIfActive:   true,
	}

	return e.EriClient.DispatchAccepted(ctx, req)
}

func IsGateAggregatorCheck(checkName string) bool {
	lower := strings.ToLower(strings.TrimSpace(checkName))
	return lower == "ci gate" || strings.HasSuffix(lower, " gate")
}

func CIRemediationDispatchID(chunkID, headSHA string, failCount int) string {
	cleanChunk := strings.TrimSpace(chunkID)
	cleanSHA := strings.ToLower(strings.TrimSpace(headSHA))
	if cleanSHA != "" {
		return fmt.Sprintf("vanadis-%s-cifail-%s-%d", cleanChunk, cleanSHA, failCount)
	}
	return fmt.Sprintf("vanadis-%s-cifail-%d", cleanChunk, failCount)
}

func ConflictRemediationDispatchID(chunkID, headSHA string) string {
	cleanChunk := strings.TrimSpace(chunkID)
	cleanSHA := strings.ToLower(strings.TrimSpace(headSHA))
	if cleanSHA != "" {
		shortSHA := cleanSHA
		if len(shortSHA) > 7 {
			shortSHA = shortSHA[:7]
		}
		return fmt.Sprintf("vanadis-conflict-%s-%s", cleanChunk, shortSHA)
	}
	return "vanadis-conflict-" + cleanChunk
}

// DispatchChunkRemediation delegates to Eri's POST /dispatch endpoint for PR failure remediation.
func (e *Executor) DispatchChunkRemediation(ctx context.Context, req BrainDispatchRequest) error {
	if e == nil || e.EriClient == nil {
		return errors.New("executor or eri client is nil")
	}
	return e.EriClient.Dispatch(ctx, req)
}

// CancelChunkDispatch calls Eri's POST /dispatch/cancel for a chunk.
func (e *Executor) CancelChunkDispatch(ctx context.Context, chunkID string) error {
	if e == nil || e.EriClient == nil {
		return nil
	}
	return e.EriClient.CancelDispatch(ctx, chunkID)
}

// DispatchWakePrompt dispatches a prompt to a Discord thread.
func (e *Executor) DispatchWakePrompt(ctx context.Context, threadID, wakePrompt string) error {
	if e == nil || e.EriClient == nil {
		return nil
	}
	return e.EriClient.Prompt(ctx, threadID, wakePrompt)
}

// DispatchSilentWakePrompt wakes a thread via Eri /dispatch so the turn runs
// as a background dispatch whose final text is not delivered to Discord.
// The occurrence discriminator (e.g. the source event's OccurredAt) keeps
// distinct failure occurrences on separate dispatch IDs while redeliveries
// of the same occurrence reuse the ID and stay already_exists-as-success.
func (e *Executor) DispatchSilentWakePrompt(ctx context.Context, threadID, chunkID, wakePrompt, occurrence string) error {
	if e == nil || e.EriClient == nil {
		return nil
	}
	cleanChunk := strings.TrimSpace(chunkID)
	sum := sha256.Sum256([]byte(strings.TrimSpace(threadID) + "\x00" + cleanChunk + "\x00" + strings.TrimSpace(wakePrompt) + "\x00" + strings.TrimSpace(occurrence)))
	return e.EriClient.Dispatch(ctx, BrainDispatchRequest{
		ID:             "vanadis-deploy-fail-" + cleanChunk + "-" + hex.EncodeToString(sum[:8]),
		Source:         "vanadis-restate",
		SessionID:      "vanadis-" + cleanChunk,
		TargetID:       strings.TrimSpace(threadID),
		VanadisChunkID: cleanChunk,
		Prompt:         wakePrompt,
		Effort:         effortMedium,
		SkipIfActive:   true,
	})
}

// DispatchDirectMessage posts a message to a Discord channel.
func (e *Executor) DispatchDirectMessage(ctx context.Context, channelID, content string) error {
	if e == nil || e.EriClient == nil {
		return nil
	}
	return e.EriClient.Message(ctx, channelID, content)
}

// SyncBuildWidget formats the Discord tracker widget and dispatches it directly to Eri via POST /discord/widget.
// It explicitly DOES NOT publish to aerial.coalesced.widget_sync.>.
func (e *Executor) SyncBuildWidget(ctx context.Context, st store.VanadisStore, build model.VanadisBuild, chunks []model.VanadisChunk, notice string) (string, error) {
	if e == nil || e.EriClient == nil {
		return "", nil
	}
	widgetReq := widget.BuildSyncWidgetRequest(build, chunks, e.deployUIURL)
	if notice != "" {
		widgetReq.Banner = notice
	}
	msgID, err := e.EriClient.Widget(ctx, widgetReq)
	if err != nil {
		return "", fmt.Errorf("sync widget to eri: %w", err)
	}
	if msgID != "" && msgID != build.StatusMessageID && st != nil {
		if uErr := st.UpdateBuildStatusMessageID(ctx, build.ID, msgID); uErr != nil {
			slog.WarnContext(ctx, "[vanadis-restate] update status message id failed", "error", uErr)
		}
	}
	return msgID, nil
}

// PublishExternalEvent publishes external events strictly for haro / audit consumers.
// It publishes to aerial.events.<source>.<eventType>.
func (e *Executor) PublishExternalEvent(ctx context.Context, source, eventType string, targets []string, contextHeader string) error {
	if e == nil || e.bus == nil {
		return nil
	}
	evID := fmt.Sprintf("%s-%s-%s", source, eventType, strings.Join(targets, "-"))
	payload := pubsub.ExternalEvent{
		EventID:       evID,
		Source:        source,
		EventType:     eventType,
		Targets:       targets,
		ContextHeader: contextHeader,
		OccurredAt:    time.Now().UTC(),
	}
	subj := fmt.Sprintf("aerial.events.%s.%s", source, eventType)
	return e.bus.Publish(ctx, subj, evID, payload)
}

// ResolveCIFailureDetailsChecked extracts failing logs or RoboRev findings for a PR check,
// returning a retryable error when a GitHub API call fails with a transport error or retryable HTTP status.
func (e *Executor) ResolveCIFailureDetailsChecked(ctx context.Context, repo string, prNumber int, checkName, headSHA, checkURL, details string) (string, error) {
	return e.ciClient().ResolveCIFailureDetailsChecked(ctx, repo, prNumber, checkName, headSHA, checkURL, details)
}

// ResolveCIFailureDetails extracts failing logs or RoboRev findings for a PR check.
func (e *Executor) ResolveCIFailureDetails(ctx context.Context, repo string, prNumber int, checkName, headSHA, checkURL, details string) string {
	res, err := e.ResolveCIFailureDetailsChecked(ctx, repo, prNumber, checkName, headSHA, checkURL, details)
	if err != nil {
		return details
	}
	return res
}

func (e *Executor) newGitHubRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	apiURL := store.FirstNonEmpty(e.GitHubAPIURL, "https://api.github.com")
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(apiURL, "/")+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	if token := strings.TrimSpace(e.gitHubToken); token != "" {
		req.Header.Set("Authorization", "token "+token)
	}
	return req, nil
}
