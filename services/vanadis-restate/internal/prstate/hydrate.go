package prstate

// Hydrate seeds PRObject state from one pr_registry row snapshot. It is a
// pure fill-if-empty transition: a field PRObject already holds is never
// overwritten, so replaying a Hydrate (or hydrating after live events have
// landed) is idempotent and can never roll committed state back to a stale
// row. Ownership comes from metadata.agent, falling back to metadata.bot_id,
// then to the agent column only when it holds a non-default value: the
// retired per-stack synapses recorded the owning stack only in that column,
// while today's single synapse never writes it, so its schema default
// ('aerial') carries no information and is ignored. Hydrate is an
// authoritative owner source, so it replaces an owner that only an event
// hint filled, but never an owner Register claimed.

import "strings"

// HydrateRequest is the PRObject.Hydrate input: one full pr_registry row.
type HydrateRequest struct {
	Row RegistryRow `json:"row"`
}

// HydrateResponse is the PRObject.Hydrate result.
type HydrateResponse struct {
	Changed bool   `json:"changed"`
	Agent   string `json:"agent"`
	Status  string `json:"status"`
}

// RowOwner returns the owning agent a registry row records:
// metadata.agent, else metadata.bot_id, else a non-default agent column,
// else "" (an unknown owner resolves to DefaultAgent and stays hintable).
func RowOwner(row RegistryRow) string {
	if agent := strings.TrimSpace(row.Metadata.Agent); agent != "" {
		return agent
	}
	if bot := strings.TrimSpace(row.Metadata.BotID); bot != "" {
		return bot
	}
	if column := strings.TrimSpace(row.Agent); column != DefaultAgent {
		return column
	}
	return ""
}

// hydrateTerminal maps a registry status to the terminal state it implies:
// closed rows are closed, every post-merge status (including deploy_failed)
// is merged, and pre-merge statuses imply none.
func hydrateTerminal(status string) TerminalState {
	switch {
	case status == RegistryStatusClosed:
		return TerminalClosed
	case isPostMergeStatus(status) || status == RegistryStatusDeployFailed:
		return TerminalMerged
	default:
		return TerminalNone
	}
}

func fillString(dst *string, v string) bool {
	v = strings.TrimSpace(v)
	if strings.TrimSpace(*dst) != "" || v == "" {
		return false
	}
	*dst = v
	return true
}

func fillSHA(dst *string, v string) bool {
	if IsZeroSHA(strings.TrimSpace(v)) {
		return false
	}
	return fillString(dst, v)
}

func fillSet(dst *[]string, v []string) bool {
	clean := cleanStrings(v)
	if len(*dst) != 0 || len(clean) == 0 {
		return false
	}
	*dst = clean
	return true
}

func fillMap[V any](dst *map[string]V, v map[string]V) bool {
	if len(*dst) != 0 || len(v) == 0 {
		return false
	}
	*dst = cloneNonEmpty(v)
	return true
}

func fillBool(dst *bool, v bool) bool {
	if *dst || !v {
		return false
	}
	*dst = true
	return true
}

// ApplyHydrate fills every empty field from row and returns true when
// anything changed. Rows for another repo#pr never apply.
func (s *PRState) ApplyHydrate(row RegistryRow) bool {
	if s == nil || NormalizeRepo(row.Repo) != s.Repo || row.PRNumber != s.PRNumber {
		return false
	}
	m := row.Metadata
	changed := false
	for _, ok := range []bool{
		fillString(&s.Branch, row.Branch),
		fillSHA(&s.HeadSHA, row.HeadSHA),
		fillSHA(&s.MergeSHA, row.MergeSHA),
		fillString(&s.TargetID, row.TargetID),
		fillString(&s.Title, row.Title),
		fillString(&s.Status, row.Status),
		fillString(&s.BotID, m.BotID),
		fillString(&s.Banner, m.Banner),
		fillString(&s.VanadisChunkID, m.VanadisChunkID),
		fillString(&s.WidgetState, m.WidgetState),
		fillSet(&s.Jobs, m.Jobs),
		fillSet(&s.StartedJobs, m.StartedJobs),
		fillSet(&s.DeployedJobs, m.DeployedJobs),
		fillSet(&s.DeployedKustomizations, m.DeployedKustomizations),
		fillMap(&s.JobStates, m.JobStates),
		fillMap(&s.CIChecks, m.CIChecks),
		fillMap(&s.KustomizationRequests, m.KustomizationRequests),
		fillBool(&s.DeployFailedPublished, m.DeployFailedPublished),
		fillBool(&s.DeploySuccessPending, m.DeploySuccessPending),
		s.hydrateOwner(RowOwner(row)),
		s.hydrateTerminal(),
	} {
		changed = changed || ok
	}
	if s.CIFailCount == 0 && m.CIFailCount > 0 {
		s.CIFailCount = m.CIFailCount
		changed = true
	}
	if s.BuildMetadata == nil && m.BuildMetadata != nil {
		clone := *m.BuildMetadata
		s.BuildMetadata = &clone
		changed = true
	}
	return changed
}

// hydrateOwner claims the row owner unless Register already claimed one,
// in which case it only fills an empty slot.
func (s *PRState) hydrateOwner(owner string) bool {
	if s.Registered {
		return s.ApplyAgentHint(owner)
	}
	return s.ClaimOwner(owner)
}

// hydrateTerminal fills an empty terminal state from the (possibly just
// hydrated) registry status so later same-epoch events absorb.
func (s *PRState) hydrateTerminal() bool {
	if s.Terminal != TerminalNone {
		return false
	}
	t := hydrateTerminal(s.Status)
	if t == TerminalNone {
		return false
	}
	s.Terminal = t
	return true
}

// hydrateDeploying reports whether a hydrated row is still a deploy
// candidate RepoObject must fan kustomization status to: deploying rows and
// deploy_failed rows with outstanding reconcile requests (the registry's
// pending-rollout set).
func (s *PRState) hydrateDeploying() bool {
	return s.Status == RegistryStatusDeploying || (s.Status == RegistryStatusDeployFailed && len(s.KustomizationRequests) > 0)
}
