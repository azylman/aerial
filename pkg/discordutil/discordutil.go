// Package discordutil holds small Discord identifier helpers shared across services.
package discordutil

// IsSnowflake reports whether id is a non-empty string of ASCII digits.
// It does not trim; callers that accept padded input trim first.
func IsSnowflake(id string) bool {
	if id == "" {
		return false
	}
	for i := range len(id) {
		if id[i] < '0' || id[i] > '9' {
			return false
		}
	}
	return true
}
