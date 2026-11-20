package router

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/azylman/aerial/services/synapse/internal/github"
	"github.com/azylman/aerial/services/synapse/internal/model"
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
	reason := github.ExtractAutoMergeBlockReason(pr.MergeableState, pr.Mergeable)
	return github.IsActionableAutoMergeBlock(reason)
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
	TargetID string `json:"target_id,omitempty"`
	BotID    string `json:"bot_id,omitempty"`
	Agent    string `json:"agent,omitempty"`
}

func (m AerialPRMetadata) OK() bool {
	return m.TargetID != "" || m.BotID != "" || m.Agent != ""
}

// AgentName returns the normalized agent associated with the PR, preferring Agent over BotID.
func (m AerialPRMetadata) AgentName() string {
	if strings.TrimSpace(m.Agent) != "" {
		return strings.ToLower(strings.TrimSpace(m.Agent))
	}
	return strings.ToLower(strings.TrimSpace(m.BotID))
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
	targetID := model.NormalizeDiscordTargetID(trailer.TargetID)
	if !model.IsValidDiscordSnowflake(targetID) && !model.IsHeadlessSessionKey(targetID) {
		targetID = ""
	}
	return AerialPRMetadata{
		TargetID: targetID,
		BotID:    strings.TrimSpace(trailer.BotID),
		Agent:    strings.TrimSpace(trailer.Agent),
	}
}

// isForeignBotPR reports whether meta specifies a non-empty BotID/Agent that differs
// from the local synapse instance's configured BotID.
func isForeignBotPR(meta AerialPRMetadata, cfgBotID string) bool {
	metaBotID := strings.TrimSpace(meta.AgentName())
	cfgBotID = strings.TrimSpace(cfgBotID)
	return metaBotID != "" && cfgBotID != "" && !strings.EqualFold(metaBotID, cfgBotID)
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
