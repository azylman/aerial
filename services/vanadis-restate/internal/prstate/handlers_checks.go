package prstate

// Check-run helpers shared by the PRObject check handlers (service.go) and
// the RepoObject SHA routing.

import (
	"strings"
)

// usableHeadSHA returns the trimmed head SHA, or "" when it cannot anchor a
// failure record or an effect key.
func usableHeadSHA(headSHA string) string {
	clean := strings.TrimSpace(headSHA)
	if clean == "" || IsZeroSHA(clean) {
		return ""
	}
	return clean
}
