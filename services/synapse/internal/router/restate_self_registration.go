package router

import (
	"context"
	"strings"

	"github.com/azylman/aerial/services/synapse/internal/model"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

// Self-registration: synapse tells PRObject about PRs nobody registered
// through aerial-pr.sh (default-owner dependabot PRs and PRs whose body
// carries bot metadata) with a HandleSelfRegistered event. PRObject fills
// the target and title only when none is stored and seeds the owner only on
// a PR it has not seen, so the stored target always wins.

// prSelfRegistration is one self-registration: the resolved target (always
// a valid snowflake), the title (may be empty) and the owner seed
// (lowercased; empty when unknown).
type prSelfRegistration struct {
	TargetID string
	Title    string
	Agent    string
}

// newSelfRegistration normalizes one registration; ok is false when the
// target is not a valid snowflake.
func newSelfRegistration(targetID, title, agent string) (prSelfRegistration, bool) {
	targetID = model.NormalizeDiscordTargetID(targetID)
	if !isValidDiscordSnowflake(targetID) {
		return prSelfRegistration{}, false
	}
	return prSelfRegistration{
		TargetID: targetID,
		Title:    strings.TrimSpace(title),
		Agent:    strings.ToLower(strings.TrimSpace(agent)),
	}, true
}

// systemChannelTarget is the configured system channel as a target, or ""
// when it is not a valid snowflake.
func (s *RouterServer) systemChannelTarget() string {
	if sys := model.NormalizeDiscordTargetID(s.currentConfig().SystemChannelID); isValidDiscordSnowflake(sys) {
		return sys
	}
	return ""
}

// dependabotSelfRegistration: a default-owner dependabot PR on opened,
// reopened or synchronize registers under the system channel with its title
// and no owner.
func (s *RouterServer) dependabotSelfRegistration(res *ResolvedGitHubEvent, p GitHubPullRequestPayload) (prSelfRegistration, bool) {
	if !s.currentConfig().DefaultOwner || !isDependabotPR(p) || res.PRNumber <= 0 {
		return prSelfRegistration{}, false
	}
	switch res.Action {
	case ghActionOpened, ghActionReopened, ghActionSynchronize:
	default:
		return prSelfRegistration{}, false
	}
	return newSelfRegistration(s.systemChannelTarget(), p.PullRequest.Title, "")
}

// metadataSelfRegistration: a PR
// with bot metadata (or any PR on a default-owner synapse) whose body names
// a target registers on every action except an unmerged close, under the
// body snowflake or, for a headless session key, the system channel, with
// its title and the PR-body owner.
func (s *RouterServer) metadataSelfRegistration(res *ResolvedGitHubEvent, pr GitHubPullRequest) (prSelfRegistration, bool) {
	meta := ParseAerialPRMetadata(pr.GetBody())
	if res.PRNumber <= 0 || s.isForeignBotEvent(meta) || (meta.BotID == "" && !s.currentConfig().DefaultOwner) ||
		(res.Action == ghActionClosed && !pr.Merged) {
		return prSelfRegistration{}, false
	}
	targetID := meta.TargetID
	if IsHeadlessSessionKey(targetID) {
		targetID = s.systemChannelTarget()
	}
	return newSelfRegistration(targetID, pr.Title, meta.OwningAgent())
}

// dependabotCIHealRegistration: a CI failure on a default-owner dependabot
// branch registers under the system channel with
// no title and no owner.
func (s *RouterServer) dependabotCIHealRegistration(res *ResolvedGitHubEvent) (prSelfRegistration, bool) {
	if !s.currentConfig().DefaultOwner || !isDependabotBranch(res.Branch) || res.PRNumber <= 0 {
		return prSelfRegistration{}, false
	}
	return newSelfRegistration(s.systemChannelTarget(), "", "")
}

// pullRequestSelfRegistration picks the registration for a pull_request
// event: the dependabot rule runs before the metadata rule.
func (s *RouterServer) pullRequestSelfRegistration(res *ResolvedGitHubEvent, p GitHubPullRequestPayload) (prSelfRegistration, bool) {
	if reg, ok := s.dependabotSelfRegistration(res, p); ok {
		return reg, true
	}
	return s.metadataSelfRegistration(res, p.PullRequest)
}

// selfRegistrationEvent renders the HandleSelfRegistered payload.
func selfRegistrationEvent(res *ResolvedGitHubEvent, reg prSelfRegistration) restateclient.PREvent {
	return restateclient.PREvent{
		Repo: res.Repo, PRNumber: res.PRNumber, HeadSHA: res.HeadSHA, Branch: res.Branch,
		Title: reg.Title, TargetID: reg.TargetID, Agent: reg.Agent,
	}
}

// routeSelfRegistration sends one registration to PRObject. The agent is
// the owner seed for a PR PRObject has not seen; it never overwrites a
// stored owner.
func (s *RouterServer) routeSelfRegistration(ctx context.Context, res *ResolvedGitHubEvent, reg prSelfRegistration) error {
	return s.sendOwnedPR(ctx, restateclient.HandlerSelfRegistered, selfRegistrationEvent(res, reg))
}
