package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
)

type GitHubPRRef struct {
	Ref string `json:"ref"`
	SHA string `json:"sha"`
}

type GitHubPullRequest struct {
	NodeID         string      `json:"node_id"`
	Number         int         `json:"number"`
	State          string      `json:"state"`
	Title          string      `json:"title"`
	Body           string      `json:"body"`
	Head           GitHubPRRef `json:"head"`
	Base           GitHubPRRef `json:"base"`
	Draft          bool        `json:"draft"`
	Merged         bool        `json:"merged"`
	MergeCommitSHA string      `json:"merge_commit_sha"`
	Mergeable      *bool       `json:"mergeable,omitempty"`
	MergeableState string      `json:"mergeable_state,omitempty"`
	User           struct {
		Login string `json:"login"`
	} `json:"user"`
}

func (pr GitHubPullRequest) GetBody() string {
	return pr.Body
}

func (pr GitHubPullRequest) GetNumber() int {
	return pr.Number
}

func isPRMergeConflict(pr GitHubPullRequest) bool {
	if strings.EqualFold(strings.TrimSpace(pr.MergeableState), "dirty") {
		return true
	}
	if pr.Mergeable != nil && !*pr.Mergeable {
		return true
	}
	return false
}

func isPRMergeabilityUnknown(pr GitHubPullRequest) bool {
	if pr.Mergeable != nil {
		return false
	}
	trimmedState := strings.TrimSpace(pr.MergeableState)
	return trimmedState == "" || strings.EqualFold(trimmedState, "unknown")
}

const aerialPRMetadataPrefix = "<!-- aerial-pr:"

// AerialPRMetadata captures structured metadata trailers in PR descriptions.
type AerialPRMetadata struct {
	TargetID       string `json:"target_id,omitempty"`
	VanadisChunkID string `json:"vanadis_chunk_id,omitempty"`
	BotID          string `json:"bot_id,omitempty"`
}

func (m AerialPRMetadata) OK() bool {
	return m.TargetID != "" || m.VanadisChunkID != "" || m.BotID != ""
}

// ParseAerialPRMetadata extracts the Aerial PR metadata JSON payload from a markdown body.
func ParseAerialPRMetadata(body string) AerialPRMetadata {
	_, after0, ok0 := strings.CutLast(body, aerialPRMetadataPrefix)
	if !ok0 {
		return AerialPRMetadata{}
	}
	rest := after0
	before, _, ok := strings.Cut(rest, "-->")
	if !ok {
		return AerialPRMetadata{}
	}
	var trailer AerialPRMetadata
	if err := json.Unmarshal([]byte(strings.TrimSpace(before)), &trailer); err != nil {
		return AerialPRMetadata{}
	}
	targetID := NormalizeDiscordTargetID(trailer.TargetID)
	if !IsValidDiscordSnowflake(targetID) {
		targetID = ""
	}
	return AerialPRMetadata{
		TargetID:       targetID,
		VanadisChunkID: strings.TrimSpace(trailer.VanadisChunkID),
		BotID:          strings.TrimSpace(trailer.BotID),
	}
}

func (s *RouterServer) isPROwnedLocally(ctx context.Context, repo string, prNum int) bool {
	if s == nil || s.registry == nil || prNum <= 0 {
		return false
	}
	targetID, branch, _, _, err := s.registry.ResolvePRByNumber(ctx, repo, prNum)
	if err != nil {
		return false
	}
	return strings.TrimSpace(targetID) != "" || strings.TrimSpace(branch) != ""
}

func (s *RouterServer) isEventOwnedLocally(ctx context.Context, repo string, prNum int, sha string) bool {
	if s == nil || s.registry == nil {
		return false
	}
	if prNum > 0 && s.isPROwnedLocally(ctx, repo, prNum) {
		return true
	}
	if sha == "" || isZeroSHA(sha) {
		return false
	}
	resolvedPRNum, branch, targetID, err := s.registry.ResolvePRBySHA(ctx, repo, sha)
	if err != nil {
		return false
	}
	if resolvedPRNum > 0 && s.isPROwnedLocally(ctx, repo, resolvedPRNum) {
		return true
	}
	return strings.TrimSpace(targetID) != "" || strings.TrimSpace(branch) != ""
}

func (s *RouterServer) forwardRawWebhook(ctx context.Context, targetBaseURL, reqPath string, headers http.Header, body []byte) {
	if s == nil || s.httpClient == nil || targetBaseURL == "" {
		return
	}
	targetURL := strings.TrimRight(targetBaseURL, "/") + reqPath
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		log.Printf("[synapse] [forward] error creating request to %s: %v", targetURL, err)
		return
	}
	for k, vv := range headers {
		if strings.EqualFold(k, "Host") || strings.EqualFold(k, "Content-Length") || strings.EqualFold(k, "Connection") {
			continue
		}
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		log.Printf("[synapse] [forward] error forwarding webhook to %s: %v", targetURL, err)
		return
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		log.Printf("[synapse] [forward] warning draining response: %v", err)
	}
}

func (s *RouterServer) forwardRawWebhookToPeers(ctx context.Context, reqPath string, headers http.Header, body []byte) {
	if s == nil {
		return
	}
	cfg := s.cfgSrc.Current()
	if len(cfg.WebhookForwardURLs) == 0 {
		return
	}
	for botID, baseURL := range cfg.WebhookForwardURLs {
		if strings.EqualFold(strings.TrimSpace(botID), strings.TrimSpace(cfg.BotID)) {
			continue
		}
		s.forwardRawWebhook(ctx, baseURL, reqPath, headers, body)
	}
}

func (s *RouterServer) forwardRawWebhookToBot(ctx context.Context, botID, reqPath string, headers http.Header, body []byte) {
	if s == nil {
		return
	}
	cfg := s.cfgSrc.Current()
	if len(cfg.WebhookForwardURLs) == 0 {
		return
	}
	if strings.EqualFold(strings.TrimSpace(botID), strings.TrimSpace(cfg.BotID)) {
		return
	}
	for peerID, baseURL := range cfg.WebhookForwardURLs {
		if strings.EqualFold(strings.TrimSpace(peerID), strings.TrimSpace(botID)) {
			s.forwardRawWebhook(ctx, baseURL, reqPath, headers, body)
			return
		}
	}
}

func (s *RouterServer) teeGitHubPREvent(ctx context.Context, reqPath string, fwdHeaders http.Header, body []byte) {
	var p struct {
		Number      int `json:"number"`
		PullRequest struct {
			Body string `json:"body"`
		} `json:"pull_request"`
		Repository struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
	}
	if err := json.Unmarshal(body, &p); err != nil {
		return
	}
	meta := ParseAerialPRMetadata(p.PullRequest.Body)
	if meta.BotID != "" {
		if !strings.EqualFold(meta.BotID, s.cfgSrc.Current().BotID) {
			s.forwardRawWebhookToBot(ctx, meta.BotID, reqPath, fwdHeaders, body)
		}
	} else if !s.isPROwnedLocally(ctx, p.Repository.FullName, p.Number) {
		s.forwardRawWebhookToPeers(ctx, reqPath, fwdHeaders, body)
	}
}

func (s *RouterServer) teeGitHubPushEvent(ctx context.Context, reqPath string, fwdHeaders http.Header, body []byte) {
	var p struct {
		Ref        string `json:"ref"`
		After      string `json:"after"`
		HeadCommit *struct {
			ID string `json:"id"`
		} `json:"head_commit"`
		Repository struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
	}
	if err := json.Unmarshal(body, &p); err != nil {
		return
	}
	sha := p.After
	if sha == "" && p.HeadCommit != nil {
		sha = p.HeadCommit.ID
	}
	if strings.HasPrefix(p.Ref, "refs/heads/main") || !s.isEventOwnedLocally(ctx, p.Repository.FullName, 0, sha) {
		s.forwardRawWebhookToPeers(ctx, reqPath, fwdHeaders, body)
	}
}

func (s *RouterServer) teeGitHubSHAEvent(ctx context.Context, reqPath string, fwdHeaders http.Header, repo string, prs []GitHubCheckRunPR, headSHA string, body []byte) {
	prNum := 0
	if len(prs) > 0 {
		prNum = prs[0].Number
	}
	if !s.isEventOwnedLocally(ctx, repo, prNum, headSHA) {
		s.forwardRawWebhookToPeers(ctx, reqPath, fwdHeaders, body)
	}
}

func (s *RouterServer) teeGitHubCheckRunEvent(ctx context.Context, reqPath string, fwdHeaders http.Header, body []byte) {
	var p struct {
		CheckRun struct {
			HeadSHA      string             `json:"head_sha"`
			PullRequests []GitHubCheckRunPR `json:"pull_requests"`
		} `json:"check_run"`
		Repository struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
	}
	if err := json.Unmarshal(body, &p); err == nil {
		s.teeGitHubSHAEvent(ctx, reqPath, fwdHeaders, p.Repository.FullName, p.CheckRun.PullRequests, p.CheckRun.HeadSHA, body)
	}
}

func (s *RouterServer) teeGitHubWorkflowRunEvent(ctx context.Context, reqPath string, fwdHeaders http.Header, body []byte) {
	var p struct {
		WorkflowRun struct {
			HeadSHA      string             `json:"head_sha"`
			PullRequests []GitHubCheckRunPR `json:"pull_requests"`
		} `json:"workflow_run"`
		Repository struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
	}
	if err := json.Unmarshal(body, &p); err == nil {
		s.teeGitHubSHAEvent(ctx, reqPath, fwdHeaders, p.Repository.FullName, p.WorkflowRun.PullRequests, p.WorkflowRun.HeadSHA, body)
	}
}

func (s *RouterServer) teeGitHubWebhook(ctx context.Context, reqPath string, r *http.Request, event string, body []byte) {
	if s == nil {
		return
	}
	if len(s.cfgSrc.Current().WebhookForwardURLs) == 0 {
		return
	}
	fwdHeaders := make(http.Header)
	ct := r.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/json"
	}
	fwdHeaders.Set("Content-Type", ct)
	fwdHeaders.Set("X-Github-Event", event)
	if del := r.Header.Get("X-Github-Delivery"); del != "" {
		fwdHeaders.Set("X-Github-Delivery", del)
	}

	switch event {
	case "pull_request":
		s.teeGitHubPREvent(ctx, reqPath, fwdHeaders, body)
	case "push":
		s.teeGitHubPushEvent(ctx, reqPath, fwdHeaders, body)
	case "check_run":
		s.teeGitHubCheckRunEvent(ctx, reqPath, fwdHeaders, body)
	case "workflow_run":
		s.teeGitHubWorkflowRunEvent(ctx, reqPath, fwdHeaders, body)
	}
}
