package executor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/githubapi"
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
	chunkModeAction = "action"
	effortMedium    = "medium"
)

// Type aliases preserving the executor public surface.
type (
	EriClient                = ericlient.EriClient
	HTTPEriClient            = ericlient.HTTPEriClient
	RestateEriClient         = ericlient.RestateEriClient
	StatusWidgetDeliverer    = ericlient.StatusWidgetDeliverer
	StatusWidgetChunk        = ericlient.StatusWidgetChunk
	StatusWidgetRequest      = ericlient.StatusWidgetRequest
	SyncChunkPRWidgetRequest = ericlient.SyncChunkPRWidgetRequest
)

var (
	NewHTTPEriClient    = ericlient.NewHTTPEriClient
	NewRestateEriClient = ericlient.NewRestateEriClient

	FormatChunkActiveStep     = widget.FormatChunkActiveStep
	FindSymbolSpan            = prompt.FindSymbolSpan
	ResolveSymbolSpans        = prompt.ResolveSymbolSpans
	FormatCIFailurePrompt     = prompt.FormatCIFailurePrompt
	FormatNoPRResumePrompt    = prompt.FormatNoPRResumePrompt
	FormatActionResumePrompt  = prompt.FormatActionResumePrompt
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
	gitHubOwner  string

	buildLocks      sync.Map
	buildMessageIDs sync.Map
	activeSteps     sync.Map
}

// NewExecutor creates a new Executor instance. gitHubOwner qualifies bare
// repo names in GitHub API paths and PR lookups.
func NewExecutor(shareRoot string, eriClient EriClient, bus pubsub.Bus, gitHubToken, gitHubOwner string, httpClient *http.Client, deployUIURL ...string) *Executor {
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
		GitHubAPIURL: githubapi.DefaultAPIURL,
		HTTPClient:   httpClient,
		deployUIURL:  dURL,
		gitHubOwner:  gitHubOwner,
	}
}

// GitHubOwner returns the configured GitHub owner; "" for a nil Executor.
func (e *Executor) GitHubOwner() string {
	if e == nil {
		return ""
	}
	return e.gitHubOwner
}

func (e *Executor) ciClient() *cilog.Client {
	if e == nil {
		return nil
	}
	c := cilog.NewClient(e.gitHubToken, e.gitHubOwner, e.HTTPClient)
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

// ExecuteLaunchChunk dispatches the chunk directly to Eri and reserves it in PostgreSQL once Eri accepts.
// It explicitly DOES NOT publish to aerial.jobs.vanadis.chunk.
func (e *Executor) ExecuteLaunchChunk(ctx context.Context, st store.VanadisStore, build model.VanadisBuild, chunks []model.VanadisChunk, activeIdx int, notice string) error {
	if e == nil || e.EriClient == nil {
		return errors.New("executor or eri client is nil")
	}
	if activeIdx < 0 || activeIdx >= len(chunks) {
		return fmt.Errorf("chunk index %d out of bounds", activeIdx)
	}
	chunk := chunks[activeIdx]

	chunkPrompt, err := e.FormatChunkPrompt(build, chunks, activeIdx)
	if err != nil {
		return fmt.Errorf("format chunk prompt: %w", err)
	}

	agent := strings.TrimSpace(build.Agent)
	if agent == "" {
		agent = "aerial"
	}
	req := eriwire.BrainDispatchRequest{
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

	// Reserve only after eri accepts: a deferred (skipped_active) or failed
	// launch leaves the chunk in its pre-launch status.
	if err := e.EriClient.DispatchAccepted(ctx, req); err != nil {
		return err
	}
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
	return nil
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

// ParseCIRemediationDispatchID inverts CIRemediationDispatchID for chunkID. It
// reports ok=false for any other chunk or dispatch shape; the SHA-less form
// yields an empty headSHA.
func ParseCIRemediationDispatchID(chunkID, dispatchID string) (string, int, bool) {
	cleanChunk := strings.TrimSpace(chunkID)
	if cleanChunk == "" {
		return "", 0, false
	}
	rest, ok := strings.CutPrefix(strings.TrimSpace(dispatchID), "vanadis-"+cleanChunk+"-cifail-")
	if !ok {
		return "", 0, false
	}
	headSHA, countText, hasSHA := strings.CutLast(rest, "-")
	if !hasSHA {
		headSHA, countText = "", rest
	} else if headSHA == "" || strings.Contains(headSHA, "-") {
		return "", 0, false
	}
	attempt, err := strconv.Atoi(countText)
	if err != nil || attempt <= 0 || strconv.Itoa(attempt) != countText {
		return "", 0, false
	}
	return headSHA, attempt, true
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
func (e *Executor) DispatchChunkRemediation(ctx context.Context, req eriwire.BrainDispatchRequest) error {
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
	return e.EriClient.Dispatch(ctx, eriwire.BrainDispatchRequest{
		ID:             "vanadis-deploy-fail-" + cleanChunk + "-" + hex.EncodeToString(sum[:8]),
		Agent:          ericlient.AgentFromContext(ctx),
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

func (e *Executor) recordMessageIDAdvance(buildID, prevID, latestID string) {
	if e == nil || buildID == "" || latestID == "" || prevID == latestID {
		return
	}
	val, _ := e.buildMessageIDs.LoadOrStore(buildID, &sync.Map{})
	if m, ok := val.(*sync.Map); ok {
		m.Store(prevID, latestID)
	}
}

func (e *Executor) resolveLatestMessageID(buildID, msgID string) string {
	if e == nil || buildID == "" {
		return msgID
	}
	val, ok := e.buildMessageIDs.Load(buildID)
	if !ok || val == nil {
		return msgID
	}
	m, ok := val.(*sync.Map)
	if !ok {
		return msgID
	}
	curr := msgID
	visited := make(map[string]bool)
	for {
		nextVal, exists := m.Load(curr)
		if !exists {
			break
		}
		next, ok := nextVal.(string)
		if !ok || next == "" || next == curr || visited[next] {
			break
		}
		visited[curr] = true
		curr = next
	}
	return curr
}

// SetActiveSteps updates the in-memory active steps map for a build.
func (e *Executor) SetActiveSteps(buildID string, steps map[string]string) {
	if e == nil || strings.TrimSpace(buildID) == "" {
		return
	}
	allEmpty := true
	clean := make(map[string]string, len(steps))
	for k, v := range steps {
		kClean := strings.TrimSpace(k)
		vClean := strings.TrimSpace(v)
		if kClean != "" && vClean != "" {
			clean[kClean] = vClean
			allEmpty = false
		}
	}
	if allEmpty {
		e.activeSteps.Delete(buildID)
		return
	}
	e.activeSteps.Store(buildID, clean)
}

// ClearActiveSteps removes any stored active steps for a build.
func (e *Executor) ClearActiveSteps(buildID string) {
	if e == nil || strings.TrimSpace(buildID) == "" {
		return
	}
	e.activeSteps.Delete(buildID)
}

// OverlayActiveSteps overlays running chunks with live in-memory active steps if not already populated.
func (e *Executor) OverlayActiveSteps(buildID string, chunks []model.VanadisChunk) {
	if e == nil || strings.TrimSpace(buildID) == "" || len(chunks) == 0 {
		return
	}
	val, ok := e.activeSteps.Load(buildID)
	if !ok || val == nil {
		return
	}
	steps, ok := val.(map[string]string)
	if !ok || len(steps) == 0 {
		return
	}
	for i := range chunks {
		if chunks[i].ActiveStep == "" {
			if s, exists := steps[chunks[i].ID]; exists && s != "" {
				chunks[i].ActiveStep = s
			}
		}
	}
}

// SyncBuildWidget formats the Discord tracker widget and dispatches it directly to Eri via POST /discord/widget.
// It explicitly DOES NOT publish to aerial.coalesced.widget_sync.>. A newly
// posted message is stored only by compare-and-swap against
// build.StatusMessageID; when another sync already stored a different ID the
// new message is deleted and "" is returned.
func (e *Executor) SyncBuildWidget(ctx context.Context, st store.VanadisStore, build model.VanadisBuild, chunks []model.VanadisChunk, notice string) (string, error) {
	if e == nil || e.EriClient == nil {
		return "", nil
	}
	if build.ID != "" {
		lockVal, _ := e.buildLocks.LoadOrStore(build.ID, &sync.Mutex{})
		if mu, ok := lockVal.(*sync.Mutex); ok && mu != nil {
			mu.Lock()
			defer mu.Unlock()
		}

		if latest := e.resolveLatestMessageID(build.ID, build.StatusMessageID); latest != "" && latest != build.StatusMessageID {
			build.StatusMessageID = latest
		}
	}
	e.OverlayActiveSteps(build.ID, chunks)
	widgetReq := widget.BuildSyncWidgetRequest(build, chunks, e.deployUIURL)
	if notice != "" {
		widgetReq.Banner = notice
	}
	msgID, err := e.EriClient.Widget(ctx, widgetReq)
	if err != nil {
		return "", fmt.Errorf("sync widget to eri: %w", err)
	}
	if msgID == "" || msgID == build.StatusMessageID || st == nil {
		return msgID, nil
	}
	// Concurrent syncs of one build can each re-post the card from the same
	// stale StatusMessageID; only the first to swap keeps its message.
	swapped, cErr := st.CompareAndSwapBuildStatusMessageID(ctx, build.ID, build.StatusMessageID, msgID)
	if cErr != nil {
		slog.WarnContext(ctx, "[vanadis-restate] update status message id failed", "build_id", build.ID, "error", cErr)
		return msgID, nil
	}
	if swapped {
		e.recordMessageIDAdvance(build.ID, build.StatusMessageID, msgID)
		return msgID, nil
	}
	slog.InfoContext(ctx, "[vanadis-restate] status widget lost the message id race; deleting duplicate card", "build_id", build.ID, "message_id", msgID)
	if dErr := e.EriClient.DeleteWidget(ctx, widgetReq.Agent, widgetReq.ChannelID, msgID); dErr != nil {
		slog.WarnContext(ctx, "[vanadis-restate] delete duplicate status widget failed", "build_id", build.ID, "message_id", msgID, "error", dErr)
	}
	return "", nil
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
