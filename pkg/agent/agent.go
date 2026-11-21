// Package agent holds the shared agent-name helpers used to route Restate
// services per agent stack.
package agent

import "strings"

// Default is the agent name served by the unsuffixed Restate services.
const Default = "aerial"

// Clean trims and lowercases an agent name. Validation and storage paths use it.
func Clean(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

// Normalize cleans name for Restate routing: empty stays empty, and a name
// containing any rune outside [a-z0-9_-] falls back to Default.
func Normalize(name string) string {
	clean := Clean(name)
	for _, c := range clean {
		if !isSafeChar(c) {
			return Default
		}
	}
	return clean
}

// ScopedServiceName returns base for the default agent (or an empty name) and
// base-<agent> for any other agent.
func ScopedServiceName(base, name string) string {
	n := Normalize(name)
	if n == "" || n == Default {
		return base
	}
	return base + "-" + n
}

func isSafeChar(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_'
}
