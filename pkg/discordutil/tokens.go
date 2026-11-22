package discordutil

import (
	"strings"

	"github.com/azylman/aerial/pkg/agent"
)

// agentTokenEnvPrefix prefixes every per-agent Discord bot token variable.
const agentTokenEnvPrefix = "DISCORD_TOKEN_"

// TokenEnvKey returns the environment variable holding agent's Discord bot
// token: DISCORD_TOKEN for "" or agent.Default, else DISCORD_TOKEN_<AGENT>
// with the agent upper-cased and '-' mapped to '_'.
func TokenEnvKey(agentName string) string {
	if agentName == "" || agentName == agent.Default {
		return "DISCORD_TOKEN"
	}
	return agentTokenEnvPrefix + strings.ToUpper(strings.ReplaceAll(agentName, "-", "_"))
}

// AgentTokensFromEnv maps agent -> trimmed token for every non-empty
// DISCORD_TOKEN_<X> entry in environ (os.Environ's KEY=VALUE form). The agent
// is X lower-cased with '_' mapped to '-', the inverse of TokenEnvKey. Plain
// DISCORD_TOKEN / DISCORD_BOT_TOKEN fallbacks are left to callers.
func AgentTokensFromEnv(environ []string) map[string]string {
	tokens := make(map[string]string)
	for _, env := range environ {
		key, rawVal, ok := strings.Cut(env, "=")
		if !ok {
			continue
		}
		val := strings.TrimSpace(rawVal)
		rawAgent, ok := strings.CutPrefix(key, agentTokenEnvPrefix)
		if !ok || rawAgent == "" || val == "" {
			continue
		}
		tokens[strings.ToLower(strings.ReplaceAll(rawAgent, "_", "-"))] = val
	}
	return tokens
}
