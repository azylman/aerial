package transcriptwire

import "strings"

// SanitizeText strips NUL bytes and replaces invalid UTF-8 with U+FFFD so
// Postgres text columns accept the value; one dirty step must not reject its
// whole insert batch.
func SanitizeText(s string) string {
	return strings.ToValidUTF8(strings.ReplaceAll(s, "\x00", ""), "�")
}
