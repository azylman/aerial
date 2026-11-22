package prwire

import (
	"strconv"
	"strings"
)

// PRObjectKey is the PRObject Virtual Object key: repo#prNumber. repo must
// already be owner-qualified ("owner/name"); the key is byte-identical to
// the one prstate addresses.
func PRObjectKey(repo string, prNumber int) string {
	return strings.TrimSpace(repo) + "#" + strconv.Itoa(prNumber)
}

// RepoObjectKey is the RepoObject Virtual Object key: the owner-qualified
// repo.
func RepoObjectKey(repo string) string {
	return strings.TrimSpace(repo)
}
