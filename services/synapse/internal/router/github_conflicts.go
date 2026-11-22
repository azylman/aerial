package router

import (
	"slices"
	"strings"

	"github.com/azylman/aerial/pkg/prwire"
)

// autoUpdatePolicy renders the stateless auto-update config RepoObject's
// cascade needs for repo: the allowlisted authors, the default-owner flag,
// and the repo's collaborator alert rules sorted by author.
func (s *RouterServer) autoUpdatePolicy(repo string) *prwire.AutoUpdatePolicy {
	cfg := s.currentConfig()
	policy := &prwire.AutoUpdatePolicy{Authors: slices.Clone(cfg.AutoUpdatePRAuthors), DefaultOwner: cfg.DefaultOwner}
	for _, rule := range cfg.CollaboratorCIAlerts {
		if !strings.EqualFold(rule.Repo, repo) {
			continue
		}
		authors := make([]string, 0, len(rule.Authors))
		for author := range rule.Authors {
			authors = append(authors, author)
		}
		slices.Sort(authors)
		for _, author := range authors {
			if mention := strings.TrimSpace(rule.Authors[author]); mention != "" {
				policy.Collaborators = append(policy.Collaborators, prwire.CollaboratorAlert{Author: author, ChannelID: rule.ChannelID, Mention: mention})
			}
		}
	}
	return policy
}

func isDependabotBranch(branch string) bool {
	return strings.HasPrefix(strings.TrimSpace(branch), "dependabot/")
}

func isDependabotPR(p GitHubPullRequestPayload) bool {
	return isDependabotBranch(p.PullRequest.Head.Ref) &&
		(strings.EqualFold(strings.TrimSpace(p.PullRequest.User.Login), "dependabot[bot]") ||
			strings.EqualFold(strings.TrimSpace(p.Sender.Login), "dependabot[bot]"))
}
