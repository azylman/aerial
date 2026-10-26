package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

// UniqueSkippedTotal records the total number of River jobs dropped due to unique constraint debouncing.
var UniqueSkippedTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "webhooks_router_unique_skipped_total",
		Help: "Total number of River jobs skipped as duplicates due to unique constraints.",
	},
	[]string{"kind"},
)

func init() {
	prometheus.MustRegister(UniqueSkippedTotal)
}

// RecordUniqueSkipped increments the UniqueSkippedTotal counter for the specified kind.
func RecordUniqueSkipped(kind string) {
	if kind == "" {
		return
	}
	UniqueSkippedTotal.WithLabelValues(kind).Inc()
}

// RiverInserter defines the interface for inserting jobs into River.
type RiverInserter interface {
	Insert(ctx context.Context, args river.JobArgs, opts *river.InsertOpts) (*rivertype.JobInsertResult, error)
}

type riverJobArgsWithOpts interface {
	river.JobArgs
	InsertOpts() river.InsertOpts
}

func defaultRiverInsertOpts() river.InsertOpts {
	return river.InsertOpts{
		MaxAttempts: 5,
		UniqueOpts:  river.UniqueOpts{ByArgs: true, ByPeriod: 15 * time.Minute},
	}
}

func (s *RouterServer) insertRiverJob(ctx context.Context, args riverJobArgsWithOpts) error {
	opts := args.InsertOpts()
	res, err := s.riverClient.Insert(ctx, args, &opts)
	if err != nil {
		return err
	}
	if res != nil && res.UniqueSkippedAsDuplicate {
		RecordUniqueSkipped(args.Kind())
	}
	return nil
}

// GitHubWebhookArgs contains payload data for durable background processing of GitHub webhooks.
type GitHubWebhookArgs struct {
	Event    string `json:"event"`
	Delivery string `json:"delivery" river:"unique"`
	Body     []byte `json:"body"`
}

func (GitHubWebhookArgs) Kind() string                 { return "github_webhook" }
func (GitHubWebhookArgs) InsertOpts() river.InsertOpts { return defaultRiverInsertOpts() }

// GitHubWebhookWorker processes GitHub webhook events asynchronously from River.
type GitHubWebhookWorker struct {
	river.WorkerDefaults[GitHubWebhookArgs]
	server *RouterServer
}

func (w *GitHubWebhookWorker) Work(ctx context.Context, job *river.Job[GitHubWebhookArgs]) error {
	if w.server == nil {
		return errors.New("router server not configured on worker")
	}
	log.Printf("[webhooks-router] [river] processing github job id=%d event=%s delivery=%s", job.ID, job.Args.Event, job.Args.Delivery)
	if _, err := w.server.ProcessGitHubEvent(ctx, job.Args.Event, job.Args.Delivery, job.Args.Body); err != nil {
		log.Printf("[webhooks-router] [river] error processing github job id=%d event=%s delivery=%s: %v", job.ID, job.Args.Event, job.Args.Delivery, err)
		return err
	}
	return nil
}

// HangarWebhookArgs contains payload data for durable background processing of Hangar webhooks.
type HangarWebhookArgs struct {
	IdempotencyKey string            `json:"idempotency_key,omitempty" river:"unique"`
	Event          HangarDeployEvent `json:"event"`
}

func (HangarWebhookArgs) Kind() string                 { return "hangar_webhook" }
func (HangarWebhookArgs) InsertOpts() river.InsertOpts { return defaultRiverInsertOpts() }

// HangarWebhookWorker processes Hangar deployment and sync webhook events from River.
type HangarWebhookWorker struct {
	river.WorkerDefaults[HangarWebhookArgs]
	server *RouterServer
}

func (w *HangarWebhookWorker) Work(ctx context.Context, job *river.Job[HangarWebhookArgs]) error {
	if w.server == nil {
		return errors.New("router server not configured on worker")
	}
	log.Printf("[webhooks-router] [river] processing hangar job id=%d event=%s job_name=%s", job.ID, job.Args.Event.Event, job.Args.Event.JobName)
	if _, err := w.server.ProcessHangarEvent(ctx, job.Args.Event); err != nil {
		log.Printf("[webhooks-router] [river] error processing hangar job id=%d: %v", job.ID, err)
		return err
	}
	return nil
}

// NomadEventArgs contains payload data for durable background processing of Nomad stream events.
type NomadEventArgs struct {
	IdempotencyKey string          `json:"idempotency_key,omitempty" river:"unique"`
	Topic          string          `json:"topic"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Index          uint64          `json:"index,omitempty"`
}

func (NomadEventArgs) Kind() string                 { return "nomad_event" }
func (NomadEventArgs) InsertOpts() river.InsertOpts { return defaultRiverInsertOpts() }

// NomadEventWorker processes Nomad deployment and allocation events from River.
type NomadEventWorker struct {
	river.WorkerDefaults[NomadEventArgs]
	server *RouterServer
}

func (w *NomadEventWorker) Work(ctx context.Context, job *river.Job[NomadEventArgs]) error {
	if w.server == nil {
		return errors.New("router server not configured on worker")
	}
	log.Printf("[webhooks-router] [river] processing nomad job id=%d topic=%s type=%s", job.ID, job.Args.Topic, job.Args.Type)
	if err := w.server.ProcessNomadEvent(ctx, job.Args.Topic, job.Args.Type, job.Args.Payload); err != nil {
		log.Printf("[webhooks-router] [river] error processing nomad job id=%d: %v", job.ID, err)
		return err
	}
	return nil
}

// NomadCrashAlertArgs contains payload data for durable background processing of unmanaged Nomad crash alerts.
type NomadCrashAlertArgs struct {
	JobID      string `json:"job_id" river:"unique"`
	AllocID    string `json:"alloc_id"`
	Status     string `json:"status"`
	StatusDesc string `json:"status_desc"`
}

func (NomadCrashAlertArgs) Kind() string                 { return "nomad_crash_alert" }
func (NomadCrashAlertArgs) InsertOpts() river.InsertOpts { return defaultRiverInsertOpts() }

// NomadCrashAlertWorker processes unmanaged Nomad job crash alerts from River.
type NomadCrashAlertWorker struct {
	river.WorkerDefaults[NomadCrashAlertArgs]
	server *RouterServer
}

func (w *NomadCrashAlertWorker) Work(ctx context.Context, job *river.Job[NomadCrashAlertArgs]) error {
	if w.server == nil {
		return errors.New("router server not configured on worker")
	}
	var jobID int64
	if job != nil && job.JobRow != nil {
		jobID = job.ID
	}
	log.Printf("[webhooks-router] [river] processing nomad crash alert job id=%d job_id=%s alloc_id=%s", jobID, job.Args.JobID, job.Args.AllocID)
	if err := w.server.ProcessCrashAlert(ctx, job.Args); err != nil {
		log.Printf("[webhooks-router] [river] error processing nomad crash alert job id=%d: %v", jobID, err)
		return err
	}
	return nil
}

// WidgetSyncArgs contains payload data for durable background processing of Discord PR widget updates.
type WidgetSyncArgs struct {
	Repo     string `json:"repo"`
	PRNumber int    `json:"pr_number"`
}

func (WidgetSyncArgs) Kind() string { return "widget_sync" }
func (WidgetSyncArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{MaxAttempts: 5}
}

// WidgetSyncWorker processes Discord PR status widget updates from River.
type WidgetSyncWorker struct {
	river.WorkerDefaults[WidgetSyncArgs]
	server *RouterServer
}

func (w *WidgetSyncWorker) Work(ctx context.Context, job *river.Job[WidgetSyncArgs]) error {
	if w.server == nil {
		return errors.New("router server not configured on worker")
	}
	log.Printf("[webhooks-router] [river] processing widget sync job id=%d repo=%s pr=%d", job.ID, job.Args.Repo, job.Args.PRNumber)
	if err := w.server.ProcessWidgetSync(ctx, job.Args); err != nil {
		log.Printf("[webhooks-router] [river] error processing widget sync job id=%d: %v", job.ID, err)
		return err
	}
	return nil
}

// StatusWidgetChunk represents a chunk in a Vanadis build or a PR task.
// Note: eri's BuildStatusWidgetComponents (services/eri/pkg/discord/widget.go) renders
// chunk status badges, repo tags, titles, PR links, and runtime durations only; it does
// not display per-job deploying_jobs lists.
type StatusWidgetChunk struct {
	Index     int       `json:"index"`
	Repo      string    `json:"repo"`
	Title     string    `json:"title"`
	Status    string    `json:"status"`
	PRNumber  int       `json:"pr_number,omitempty"`
	StartedAt time.Time `json:"started_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

// StatusWidgetRequest represents a request to deliver or update a Discord status tracker widget.
type StatusWidgetRequest struct {
	ChannelID string              `json:"channel_id"`
	MessageID string              `json:"message_id,omitempty"`
	ID        string              `json:"id"`
	Tier      int                 `json:"tier,omitempty"`
	Title     string              `json:"title"`
	Status    string              `json:"status"`
	Banner    string              `json:"banner,omitempty"`
	Chunks    []StatusWidgetChunk `json:"chunks"`
}

type GitPushEventRequest struct {
	Repo     string `json:"repo"`
	Ref      string `json:"ref"`
	Commit   string `json:"commit"`
	TargetID string `json:"target_id,omitempty"`
	PRNumber int    `json:"pr_number,omitempty"`
}

type ImageReadyEventRequest struct {
	Image     string `json:"image"`
	Digest    string `json:"digest,omitempty"`
	Repo      string `json:"repo,omitempty"`
	CommitSHA string `json:"commit_sha,omitempty"`
	PRNumber  int    `json:"pr_number,omitempty"`
	TargetID  string `json:"target_id,omitempty"`
}

type PromptRequest struct {
	ChannelID string `json:"channel_id"`
	Prompt    string `json:"prompt"`
	Effort    string `json:"effort,omitempty"`
	Silent    bool   `json:"silent,omitempty"`
}

type DirectMessageRequest struct {
	ChannelID string `json:"channel_id"`
	Content   string `json:"content"`
}

type ImageReadyEventResponse struct {
	Status      string   `json:"status"`
	Image       string   `json:"image,omitempty"`
	MatchedJobs []string `json:"matched_jobs,omitempty"`
	Message     string   `json:"message,omitempty"`
}

type OutboundDispatcher interface {
	DispatchGitPush(ctx context.Context, req GitPushEventRequest) error
	DispatchImageReady(ctx context.Context, req ImageReadyEventRequest) ([]string, error)
	DispatchPrompt(ctx context.Context, req PromptRequest) error
	DispatchDirectMessage(ctx context.Context, req DirectMessageRequest) error
	DispatchWidget(ctx context.Context, payload StatusWidgetRequest) (string, error)
}

type DefaultOutboundDispatcher struct {
	hangarURL     string
	brainURL      string
	httpClient    *http.Client
	gitPushClient *http.Client
	retryDelay    time.Duration
	maxAttempts   int
}

func NewDefaultOutboundDispatcher(hangarURL, brainURL string, client *http.Client) *DefaultOutboundDispatcher {
	gitPushClient := client
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
		gitPushClient = &http.Client{Timeout: 65 * time.Second}
	}
	return &DefaultOutboundDispatcher{
		hangarURL:     strings.TrimRight(hangarURL, "/"),
		brainURL:      strings.TrimRight(brainURL, "/"),
		httpClient:    client,
		gitPushClient: gitPushClient,
		retryDelay:    1500 * time.Millisecond,
		maxAttempts:   6,
	}
}

func (d *DefaultOutboundDispatcher) postHangar(ctx context.Context, endpointPath, opName, targetDesc string, payload any) ([]byte, bool, error) {
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, false, fmt.Errorf("marshal %s request: %w", opName, err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, d.hangarURL+endpointPath, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, false, fmt.Errorf("create %s request: %w", opName, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpClient := d.httpClient
	if endpointPath == "/events/git_push" && d.gitPushClient != nil {
		httpClient = d.gitPushClient
	}
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return nil, false, fmt.Errorf("%s request failed: %w", opName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if readErr == nil && isHangarUnmanagedError(respBody) {
			log.Printf("[webhooks-router] [dispatcher] hangar confirmed %s (skipping): %s", targetDesc, string(respBody))
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("hangar %s returned 404: %s", opName, string(respBody))
	}
	respBody, readErr := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if readErr != nil {
			return nil, false, fmt.Errorf("hangar %s returned status %d (read error: %w)", opName, resp.StatusCode, readErr)
		}
		return nil, false, fmt.Errorf("hangar %s returned status %d: %s", opName, resp.StatusCode, string(respBody))
	}
	return respBody, false, nil
}

func (d *DefaultOutboundDispatcher) DispatchGitPush(ctx context.Context, req GitPushEventRequest) error {
	_, _, err := d.postHangar(ctx, "/events/git_push", "git push", "repo "+req.Repo+" is not managed by hangar", req)
	return err
}

func (d *DefaultOutboundDispatcher) DispatchImageReady(ctx context.Context, req ImageReadyEventRequest) ([]string, error) {
	respBody, skipped, err := d.postHangar(ctx, "/events/image_ready", "image ready", "image "+req.Image+" has no matching nomad jobs", req)
	if err != nil || skipped {
		return nil, err
	}
	var res ImageReadyEventResponse
	if err := json.Unmarshal(respBody, &res); err != nil {
		log.Printf("[webhooks-router] [dispatcher] warning decoding image ready response for %s: %v", req.Image, err)
		return nil, nil
	}
	return res.MatchedJobs, nil
}

func (d *DefaultOutboundDispatcher) postBrainWithRetry(ctx context.Context, endpointPath, reqLabel, statusLabel, channelID string, maxAttempts int, defaultDelay time.Duration, expBackoff bool, payload any) error {
	if !IsValidDiscordChannel(channelID) {
		return fmt.Errorf("invalid discord snowflake channel id: %q", channelID)
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal %s request: %w", reqLabel, err)
	}
	delay := d.retryDelay
	if delay <= 0 {
		delay = defaultDelay
	}
	if maxAttempts <= 0 {
		maxAttempts = 6
	}
	endpoint := d.brainURL + endpointPath
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
		if err != nil {
			return fmt.Errorf("create %s request: %w", reqLabel, err)
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := d.httpClient.Do(httpReq)
		if err != nil {
			lastErr = fmt.Errorf("%s request failed (attempt %d/%d): %w", reqLabel, attempt, maxAttempts, err)
		} else {
			respBody, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				respBody = []byte(fmt.Sprintf("error reading body: %v", readErr))
			}
			resp.Body.Close()

			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			if resp.StatusCode < 500 {
				return fmt.Errorf("%s returned status %d: %s", statusLabel, resp.StatusCode, string(respBody))
			}
			lastErr = fmt.Errorf("%s returned status %d (attempt %d/%d): %s", statusLabel, resp.StatusCode, attempt, maxAttempts, string(respBody))
		}
		if attempt < maxAttempts {
			select {
			case <-time.After(delay):
				if expBackoff {
					if delay = time.Duration(float64(delay) * 1.5); delay > 5*time.Second {
						delay = 5 * time.Second
					}
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return lastErr
}

func (d *DefaultOutboundDispatcher) DispatchPrompt(ctx context.Context, req PromptRequest) error {
	return d.postBrainWithRetry(ctx, "/prompt", "prompt", "brain prompt", req.ChannelID, d.maxAttempts, 1500*time.Millisecond, true, req)
}

func (d *DefaultOutboundDispatcher) DispatchDirectMessage(ctx context.Context, req DirectMessageRequest) error {
	return d.postBrainWithRetry(ctx, "/discord/message", "direct message", "brain internal message", req.ChannelID, 3, 500*time.Millisecond, false, req)
}

func (d *DefaultOutboundDispatcher) DispatchWidget(ctx context.Context, payload StatusWidgetRequest) (string, error) {
	if !IsValidDiscordSnowflake(payload.ChannelID) {
		return "", fmt.Errorf("invalid discord snowflake channel id: %q", payload.ChannelID)
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal widget request: %w", err)
	}
	endpoint := d.brainURL + "/discord/widget"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", fmt.Errorf("create widget request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("dispatch widget request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read widget response body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("brain widget returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var res struct {
		Status    string `json:"status"`
		MessageID string `json:"message_id"`
	}
	if err := json.Unmarshal(respBody, &res); err != nil {
		return "", fmt.Errorf("decode widget response: %w", err)
	}
	return res.MessageID, nil
}

type hangarJSONResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

func isHangarUnmanagedError(body []byte) bool {
	var hResp hangarJSONResponse
	if err := json.Unmarshal(body, &hResp); err != nil {
		return false
	}
	if hResp.Status == "not_found" {
		return true
	}
	msg := strings.ToLower(hResp.Message)
	return strings.Contains(msg, "not managed by hangar") || strings.Contains(msg, "no nomad jobs found")
}
