package server

import (
	"errors"
	"regexp"
	"strings"

	"github.com/bwmarrin/discordgo"

	"github.com/azylman/aerial/pkg/discordutil"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/queue"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

const discordErrCodeThreadAlreadyCreated = 160004

var (
	mentionAndEmojiRegex = regexp.MustCompile(`(?i)<(@!?|@&|#|a?:[a-z0-9_]+:)\d+>`)
	reDispatchSource     = regexp.MustCompile(`^[a-z0-9_-]{1,64}$`)
)

func validateDispatchRequest(req *DispatchRequest) error {
	source := strings.ToLower(strings.TrimSpace(req.Source))
	if !reDispatchSource.MatchString(source) {
		return errors.New("Invalid payload: 'source' must match ^[a-z0-9_-]{1,64}$")
	}
	if strings.TrimSpace(req.Prompt) == "" {
		return errors.New("Invalid payload: 'prompt' field is required and cannot be empty")
	}
	sessionID := strings.TrimSpace(req.SessionID)
	if sessionID != "" {
		if discordutil.IsSnowflake(sessionID) || strings.HasPrefix(strings.ToLower(sessionID), "discord-") || !queue.IsValidThreadKey(sessionID) {
			return errors.New("Invalid payload: 'session_id' cannot be a numeric snowflake, start with 'discord-', or contain invalid characters")
		}
	}
	effort := strings.ToLower(strings.TrimSpace(req.Effort))
	if effort != "" && effort != config.EffortLow && effort != config.EffortMedium && effort != config.EffortHigh {
		return errors.New("Invalid payload: 'effort' must be 'low', 'medium', or 'high'")
	}
	req.TargetID = runner.NormalizeEnvTargetID(req.TargetID)
	req.VanadisChunkID = strings.TrimSpace(req.VanadisChunkID)
	return nil
}

func validatePromptEffort(effort string) error {
	effort = strings.ToLower(strings.TrimSpace(effort))
	if effort != "" && effort != config.EffortLow && effort != config.EffortMedium && effort != config.EffortHigh {
		return errors.New("Invalid payload: 'effort' must be 'low', 'medium', or 'high'")
	}
	return nil
}

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
