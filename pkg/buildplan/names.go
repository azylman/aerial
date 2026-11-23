package buildplan

// BuildServiceName derives the Restate Build Virtual Object name for an agent.
// With vanadis-restate consolidated into a single agent-aware instance,
// the agent parameter is intentionally ignored for roadmap compatibility
// and this unconditionally returns the canonical "Build" Virtual Object name.
func BuildServiceName(_ string) string {
	return "Build"
}

// BuildRouterServiceName derives the Restate BuildRouter service name for an agent.
// With vanadis-restate consolidated into a single agent-aware instance,
// the agent parameter is intentionally ignored for roadmap compatibility
// and this unconditionally returns the canonical "BuildRouter" service name.
func BuildRouterServiceName(_ string) string {
	return "BuildRouter"
}
