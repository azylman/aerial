package router

import (
	"context"
	"strings"

	"github.com/azylman/aerial/services/synapse/internal/model"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

// Self-registration parity: legacy self-registers PRs it was never told
// about with registry.EnsurePRRegistered (fill-only target, title, head and
// an insert-only owner seed). Every such write is journaled into PRObject
// first as HandleSelfRegistered, and the restate-owned path sends the same
// event from the same rule, so PRObject holds the target and title the row
// holds and the PR is tracked in both modes.

// prSelfRegistration is one EnsurePRRegistered write: the resolved target
// (always a valid snowflake), the title (may be empty) and the owner seeded
// on insert (lowercased like ensureOwnerMetadata; empty when unknown).
type prSelfRegistration struct {
	TargetID string
	Title    string
	Agent    string
}

// headlessTargetFunc resolves the target of a headless-metadata or
// default-owner dependabot PR.
type headlessTargetFunc func(ctx context.Context, repo string, prNumber int) string

// newSelfRegistration normalizes one registration; ok is false when the
// target is not a valid snowflake, where EnsurePRRegistered writes nothing.
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

// ownedHeadlessTarget is the restate-owned headless resolver: the system
// channel only, since that path never reads pr_registry. Legacy's
// resolveHeadlessPRTargetID first returns a stored row target, but that
// changes nothing: EnsurePRRegistered fills target_id only when the row has
// none and PRObject.HandleSelfRegistered fills TargetID only when none is
// stored, so on both sides the stored target wins and an unregistered PR
// gets the system channel.
func (s *RouterServer) ownedHeadlessTarget(context.Context, string, int) string {
	return s.systemChannelTarget()
}

// dependabotSelfRegistration is handleDependabotPROpen's rule: a
// default-owner dependabot PR on opened, reopened or synchronize registers
// under the headless target with its title and no owner.
func (s *RouterServer) dependabotSelfRegistration(ctx context.Context, res *ResolvedGitHubEvent, p GitHubPullRequestPayload, headless headlessTargetFunc) (prSelfRegistration, bool) {
	if !s.currentConfig().DefaultOwner || !isDependabotPR(p) || res.PRNumber <= 0 {
		return prSelfRegistration{}, false
	}
	switch res.Action {
	case ghActionOpened, ghActionReopened, ghActionSynchronize:
	default:
		return prSelfRegistration{}, false
	}
	return newSelfRegistration(headless(ctx, res.Repo, res.PRNumber), p.PullRequest.Title, "")
}

// metadataSelfRegistration is handlePullRequestRegistryAction's rule: a PR
// with bot metadata (or any PR on a default-owner synapse) whose body names
// a target registers on every action except an unmerged close, under the
// body snowflake or, for a headless session key, the headless target, with
// its title and the PR-body owner.
func (s *RouterServer) metadataSelfRegistration(ctx context.Context, res *ResolvedGitHubEvent, pr GitHubPullRequest, headless headlessTargetFunc) (prSelfRegistration, bool) {
	meta := ParseAerialPRMetadata(pr.GetBody())
	if res.PRNumber <= 0 || s.isForeignBotEvent(meta) || (meta.BotID == "" && !s.currentConfig().DefaultOwner) ||
		(res.Action == ghActionClosed && !pr.Merged) {
		return prSelfRegistration{}, false
	}
	targetID := meta.TargetID
	if IsHeadlessSessionKey(targetID) {
		targetID = headless(ctx, res.Repo, res.PRNumber)
	}
	return newSelfRegistration(targetID, pr.Title, meta.OwningAgent())
}

// dependabotCIHealRegistration is healDependabotPR's rule: a CI failure on a
// default-owner dependabot branch registers under the headless target with
// no title and no owner.
func (s *RouterServer) dependabotCIHealRegistration(ctx context.Context, res *ResolvedGitHubEvent, headless headlessTargetFunc) (prSelfRegistration, bool) {
	if !s.currentConfig().DefaultOwner || !isDependabotBranch(res.Branch) || res.PRNumber <= 0 {
		return prSelfRegistration{}, false
	}
	return newSelfRegistration(headless(ctx, res.Repo, res.PRNumber), "", "")
}

// pullRequestSelfRegistration is the first registration legacy writes for a
// pull_request event: the dependabot rule runs before the metadata rule,
// and EnsurePRRegistered is fill-only, so a second write changes nothing.
func (s *RouterServer) pullRequestSelfRegistration(ctx context.Context, res *ResolvedGitHubEvent, p GitHubPullRequestPayload, headless headlessTargetFunc) (prSelfRegistration, bool) {
	if reg, ok := s.dependabotSelfRegistration(ctx, res, p, headless); ok {
		return reg, true
	}
	return s.metadataSelfRegistration(ctx, res, p.PullRequest, headless)
}

// selfRegistrationEvent renders the HandleSelfRegistered payload.
func selfRegistrationEvent(res *ResolvedGitHubEvent, reg prSelfRegistration) restateclient.PREvent {
	return restateclient.PREvent{
		Repo: res.Repo, PRNumber: res.PRNumber, HeadSHA: res.HeadSHA, Branch: res.Branch,
		Title: reg.Title, TargetID: reg.TargetID, Agent: reg.Agent,
	}
}

// routeSelfRegistration journals one registration into PRObject before the
// legacy EnsurePRRegistered write, with routePREvent's journal-or-mirror
// semantics (an authoritative journal failure returns, so the caller skips
// the write and the redelivery retries both). It skips routePREvent's owner
// lookup: the agent is the insert's owner seed, and a looked-up or default
// owner would seed one the row never gets.
func (s *RouterServer) routeSelfRegistration(ctx context.Context, res *ResolvedGitHubEvent, reg prSelfRegistration) error {
	ev := selfRegistrationEvent(res, reg)
	if restateOwnsEffects(ctx) {
		return s.sendOwnedPR(ctx, restateclient.HandlerSelfRegistered, ev)
	}
	if !s.restateAuthoritative() && !s.restateShadow().Enabled() {
		return nil
	}
	ev.EffectsOwner = effectsOwnerFromContext(ctx)
	return s.routeEvent(ctx, restateclient.HandlerSelfRegistered,
		func(r *shadowRestateReporter, ctx context.Context) error {
			return r.client.SendPREvent(ctx, restateclient.HandlerSelfRegistered, ev)
		},
		func(ctx context.Context) {
			s.shadowPREvent(ctx, restateclient.HandlerSelfRegistered, ev)
		})
}
