package main

import (
	"github.com/azylman/aerial/services/vanadis/internal/store"
)

// repoMatchCandidates returns the repo spellings a PR lookup should match.
var repoMatchCandidates = store.RepoMatchCandidates
