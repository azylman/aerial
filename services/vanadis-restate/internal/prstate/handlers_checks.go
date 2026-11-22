package prstate

// Check-run helpers shared by the PRObject check handlers (service.go) and
// the RepoObject SHA routing.

import (
	"strings"

	"github.com/azylman/aerial/pkg/prwire"
)

// usableHeadSHA returns the trimmed head SHA, or "" when it cannot anchor a
// failure record or an effect key.
func usableHeadSHA(headSHA string) string {
	clean := strings.TrimSpace(headSHA)
	if clean == "" || prwire.IsZeroSHA(clean) {
		return ""
	}
	return clean
}
