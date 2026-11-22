package pubsub

import "strings"

// SanitizeSubjectToken makes an untrusted value safe as one NATS subject
// token: it trims surrounding space and maps every rune outside
// [A-Za-z0-9_-] to '_'. A result that is empty or only underscores becomes
// "unknown".
func SanitizeSubjectToken(token string) string {
	clean := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, strings.TrimSpace(token))
	if strings.Trim(clean, "_") == "" {
		return "unknown"
	}
	return clean
}
