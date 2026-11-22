package buildplan

import "github.com/azylman/aerial/pkg/agent"

// BuildServiceName derives the Restate Build Virtual Object name for an agent.
// Empty, default and unsafe agent names resolve to the unsuffixed Build name.
func BuildServiceName(name string) string {
	return agent.ScopedServiceName("Build", name)
}

// BuildRouterServiceName derives the Restate BuildRouter service name for an agent.
// Empty, default and unsafe agent names resolve to the unsuffixed BuildRouter name.
func BuildRouterServiceName(name string) string {
	return agent.ScopedServiceName("BuildRouter", name)
}
