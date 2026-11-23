package githubapi

import "strings"

// QualifyRepo returns repo as "owner/name". Spaces and surrounding slashes
// are trimmed; a repo that already names an owner, or an empty owner,
// leaves repo unchanged; an empty repo stays empty.
func QualifyRepo(owner, repo string) string {
	repo = strings.Trim(strings.TrimSpace(repo), "/")
	if repo == "" || strings.Contains(repo, "/") {
		return repo
	}
	owner = strings.Trim(strings.TrimSpace(owner), "/")
	if owner == "" {
		return repo
	}
	return owner + "/" + repo
}
