package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/transcript"
	"github.com/bwmarrin/discordgo"
)

const discordErrCodeThreadAlreadyCreated = 160004

var mentionAndEmojiRegex = regexp.MustCompile(`(?i)<(@!?|@&|#|a?:[a-z0-9_]+:)[0-9]+>`)

// DeriveThreadTitle extracts a clean, readable thread title from a message content string.
func DeriveThreadTitle(content string) string {
	cleaned := strings.TrimSpace(mentionAndEmojiRegex.ReplaceAllString(content, ""))
	var firstLine string
	for line := range strings.SplitSeq(cleaned, "\n") {
		if trimmed := strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(line), "#> \t")); trimmed != "" {
			firstLine = trimmed
			break
		}
	}
	if firstLine == "" {
		firstLine = "Aerial Discussion"
	}
	runes := []rune(firstLine)
	if len(runes) > 60 {
		cut := runes[:57]
		for len(cut) > 0 {
			switch cut[len(cut)-1] {
			case '\u200D', '\u200B', '\uFEFF':
				cut = cut[:len(cut)-1]
				continue
			}
			break
		}
		return string(cut) + "..."
	}
	return string(runes)
}

// IsThreadAlreadyExistsError returns true if the error indicates a Discord thread already exists for a message (code 160004).
func IsThreadAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	var restErr *discordgo.RESTError
	if !errors.As(err, &restErr) {
		if restErrVal, ok := errors.AsType[discordgo.RESTError](err); ok {
			restErr = &restErrVal
		}
	}
	if restErr != nil {
		if restErr.Message != nil && restErr.Message.Code == discordErrCodeThreadAlreadyCreated {
			return true
		}
		bodyStr := strings.ToLower(string(restErr.ResponseBody))
		return strings.Contains(bodyStr, "160004") || strings.Contains(bodyStr, "already been created")
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "160004") || strings.Contains(errStr, "already been created")
}

// ResolveGuildID resolves the Discord Guild ID using strict precedence: message -> cached -> channel.
func ResolveGuildID(msgGuildID, cachedGuildID, channelGuildID string) string {
	if msgGuildID != "" {
		return msgGuildID
	}
	if cachedGuildID != "" {
		return cachedGuildID
	}
	return channelGuildID
}

// buildSyncLLM adapts a runner.LLMFunc into a transcript.LLMClientFunc using the preferred low-effort model.
func buildSyncLLM(rawLLM runner.LLMFunc, lowEffortModel, defaultModel string) transcript.LLMClientFunc {
	if rawLLM == nil {
		return nil
	}
	model := strings.TrimSpace(lowEffortModel)
	if model == "" {
		model = strings.TrimSpace(defaultModel)
	}
	return func(ctx context.Context, prompt string) (string, error) {
		return rawLLM(ctx, model, prompt)
	}
}

// DefaultGeminiHomeDir returns the production default base directory for .gemini files.
func DefaultGeminiHomeDir() string {
	return defaultGeminiHomeDirWithLookup(os.Getenv("HOME"), testing.Testing(), os.UserHomeDir)
}

func defaultGeminiHomeDirWithLookup(home string, isTesting bool, userHomeDirFn func() (string, error)) string {
	if strings.TrimSpace(home) != "" {
		return strings.TrimSpace(home)
	}
	if isTesting {
		return filepath.Join(os.TempDir(), "aerial-test-gemini")
	}
	if userHomeDirFn != nil {
		if h, err := userHomeDirFn(); err == nil && strings.TrimSpace(h) != "" {
			return strings.TrimSpace(h)
		}
	}
	return "/root"
}

// DefaultDataDir returns the production default base directory for persistent data.
func DefaultDataDir() string {
	return defaultDataDir(testing.Testing())
}

func defaultDataDir(isTesting bool) string {
	if isTesting {
		return filepath.Join(os.TempDir(), "aerial-test-data")
	}
	return "/data"
}

func buildDiscordPools(basePoolCfg func(home, model string, prewarmed []string) runner.PoolConfig, discordHome, highModel, medModel, lowModel string, spawner runner.DaemonSpawner) (map[queue.PoolKey]runner.AgentPool, []runner.AgentPool) {
	pools := make(map[queue.PoolKey]runner.AgentPool, 5)
	ordered := make([]runner.AgentPool, 0, len(queue.AllValidPoolKeys()))
	for _, k := range queue.AllValidPoolKeys() {
		model := highModel
		switch k.Effort {
		case "medium":
			model = medModel
		case "low":
			model = lowModel
		}
		var p runner.AgentPool
		switch {
		case !k.Ephemeral:
			p = runner.NewUnifiedProcessPool(basePoolCfg(discordHome, model, nil), spawner)
		case k.Effort == "medium":
			p = newWorkerInterchangeablePool(basePoolCfg(discordHome, model, []string{"discord:medium-worker-0", "discord:medium-worker-1"}), spawner, "discord:medium-worker")
		default:
			p = newWorkerInterchangeablePool(basePoolCfg(discordHome, model, []string{"discord:worker-0", "discord:worker-1"}), spawner, "discord:worker")
		}
		pools[k] = p
		ordered = append(ordered, p)
	}
	return pools, ordered
}
