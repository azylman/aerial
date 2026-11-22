package prstate

// Stateless renders the GitHub effects need when no synapse is in the loop
// (the RepoObject auto-update cascade): the collaborator conflict DM, the
// conflict and closed-unmerged lifecycle facts, and the Aerial PR-metadata
// trailer check. Each mirrors its synapse counterpart byte for byte so a
// restate-owned effect renders what the legacy pipeline sends.

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/pkg/pubsub"
)

// Lifecycle fact events and statuses the GitHub effects publish.
const (
	FactEventConflict       = "conflict"
	FactEventClosedUnmerged = "closed_unmerged"
)

const aerialPRMetadataPrefix = "<!-- aerial-pr:"

// HasAerialPRMetadata reports whether a PR body carries an Aerial metadata
// trailer naming a target, bot, or agent (synapse
// ParseAerialPRMetadata(body).OK()). Such PRs are bot-owned, never
// collaborator PRs.
func HasAerialPRMetadata(body string) bool {
	_, after, ok := strings.Cut(lastCutPrefix(body), aerialPRMetadataPrefix)
	if !ok {
		return false
	}
	before, _, ok := strings.Cut(after, "-->")
	if !ok {
		return false
	}
	var trailer struct {
		TargetID string `json:"target_id"`
		BotID    string `json:"bot_id"`
		Agent    string `json:"agent"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(before)), &trailer); err != nil {
		return false
	}
	if strings.TrimSpace(trailer.BotID) != "" || strings.TrimSpace(trailer.Agent) != "" {
		return true
	}
	target := normalizeDiscordTargetID(trailer.TargetID)
	return IsValidDiscordSnowflake(target) || isHeadlessSessionKey(target)
}

// lastCutPrefix returns body from its last metadata prefix on, so the last
// trailer wins exactly like strings.CutLast in synapse.
func lastCutPrefix(body string) string {
	if idx := strings.LastIndex(body, aerialPRMetadataPrefix); idx >= 0 {
		return body[idx:]
	}
	return ""
}

func normalizeDiscordTargetID(id string) string {
	clean := strings.TrimSpace(id)
	if idx := strings.LastIndex(clean, ":"); idx >= 0 {
		clean = strings.TrimSpace(clean[idx+1:])
	}
	return strings.TrimSpace(strings.TrimPrefix(clean, "discord-"))
}

func isHeadlessSessionKey(id string) bool {
	clean := strings.TrimSpace(id)
	if idx := strings.LastIndex(clean, ":"); idx >= 0 {
		clean = strings.TrimSpace(clean[idx+1:])
	}
	return strings.HasPrefix(clean, "dispatch-") || strings.HasPrefix(clean, "heartbeat-") ||
		strings.HasPrefix(clean, "worker-") || strings.Contains(clean, "-worker-")
}

// FormatCollaboratorPRTarget renders the PR link plus title, branch, and
// short commit, matching synapse FormatCollaboratorPRTarget.
func FormatCollaboratorPRTarget(repo string, prNumber int, title, branch, headSHA string) string {
	repo = strings.TrimSpace(repo)
	short := strings.TrimSpace(headSHA)
	if len(short) > 7 {
		short = short[:7]
	}
	link := fmt.Sprintf("PR #%d", prNumber)
	if repo != "" {
		link = fmt.Sprintf("[%s#%d](https://github.com/%s/pull/%d)", repo, prNumber, repo, prNumber)
	}
	var meta []string
	if clean := strings.TrimSpace(title); clean != "" {
		meta = append(meta, fmt.Sprintf("%q", clean))
	}
	if clean := strings.TrimPrefix(strings.TrimSpace(branch), "refs/heads/"); clean != "" {
		meta = append(meta, "branch: "+clean)
	}
	if short != "" {
		meta = append(meta, "commit: "+short)
	}
	if len(meta) == 0 {
		return link
	}
	return fmt.Sprintf("%s (%s)", link, strings.Join(meta, ", "))
}

// CollaboratorConflictMessage renders the collaborator merge-conflict DM,
// matching synapse BuildCollaboratorConflictMessage.
func CollaboratorConflictMessage(mention, repo string, prNumber int, title, branch, headSHA string) string {
	msg := fmt.Sprintf("Merge conflict with `main` detected on %s — automatic branch update could not resolve conflicts. Please rebase or merge `origin/main` and push.",
		FormatCollaboratorPRTarget(repo, prNumber, title, branch, headSHA))
	if clean := strings.TrimSpace(mention); clean != "" {
		return clean + " " + msg
	}
	return msg
}

// collaboratorConflictDM renders the collaborator conflict DM for one open
// PR when the policy has a rule for its author and the PR carries no Aerial
// metadata; ok is false otherwise.
func collaboratorConflictDM(policy *AutoUpdatePolicy, repo string, pr GitHubPR) (EriEffect, bool) {
	if policy == nil || pr.Merged || HasAerialPRMetadata(pr.Body) {
		return EriEffect{}, false
	}
	if state := strings.TrimSpace(pr.State); state != "" && !strings.EqualFold(state, "open") {
		return EriEffect{}, false
	}
	rule, ok := collaborator(policy, pr.User.Login)
	if !ok {
		return EriEffect{}, false
	}
	return EriEffect{
		Kind:      prwire.EffectEriDirectMessage,
		ChannelID: rule.ChannelID,
		Content:   CollaboratorConflictMessage(rule.Mention, repo, pr.Number, pr.Title, pr.Head.Ref, pr.Head.SHA),
	}, true
}

// isDependabotBranch reports a dependabot branch; its facts publish quiet.
func isDependabotBranch(branch string) bool {
	return strings.HasPrefix(strings.TrimSpace(branch), "dependabot/")
}

// prFact renders one lifecycle fact for st, matching synapse conflictFact
// and the stale closed_unmerged publish. RolloutState is left nil here;
// runFacts renders it from committed PRState before a restate-owned
// publish (BuildPRRolloutState).
func prFact(st *PRState, event, branch, headSHA string, at time.Time) pubsub.PRLifecycleEvent {
	if strings.TrimSpace(branch) == "" {
		branch = st.Branch
	}
	return pubsub.PRLifecycleEvent{
		Event:      event,
		Repo:       st.Repo,
		PRNumber:   st.PRNumber,
		Branch:     branch,
		HeadSHA:    strings.TrimSpace(headSHA),
		TargetID:   st.TargetID,
		Status:     event,
		Quiet:      isDependabotBranch(branch),
		OccurredAt: at,
	}
}
