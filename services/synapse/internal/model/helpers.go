package model

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	MaxPromptRunes         = 1500
	MaxRoboRevCommentRunes = 4000
)

var (
	snowflakeRegex   = regexp.MustCompile(`^\d{17,20}$`)
	channelNameRegex = regexp.MustCompile(`^#?[a-z0-9_-]{1,100}$`)
	re4xx            = regexp.MustCompile(`\bstatus 4\d{2}\b`)
)

func FirstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func RandomNonce() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("nonce-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// NormalizeRepo ensures repository names are normalized with the owner prefix.
func NormalizeRepo(repo string) string {
	repo = strings.TrimSpace(repo)
	if repo == "" {
		return ""
	}
	if !strings.Contains(repo, "/") {
		return "azylman/" + repo
	}
	return repo
}

// CleanRepoName strips the owner prefix (e.g. "azylman/") for concise Discord notifications.
func CleanRepoName(repo string) string {
	return strings.TrimPrefix(strings.TrimSpace(repo), "azylman/")
}

// FormatDirectMessagePrefix formats the bold PR and repo prefix for direct Discord messages.
func FormatDirectMessagePrefix(prNumber int, repo string) string {
	repoName := CleanRepoName(repo)
	switch {
	case prNumber > 0 && repoName != "":
		return fmt.Sprintf("**(PR: #%d, repo: %s)** ", prNumber, repoName)
	case repoName != "":
		return fmt.Sprintf("**(repo: %s)** ", repoName)
	case prNumber > 0:
		return fmt.Sprintf("**(PR: #%d)** ", prNumber)
	default:
		return ""
	}
}

func DedupeTrimmedStrings(items []string) []string {
	seen := make(map[string]struct{}, len(items))
	var out []string
	for _, item := range items {
		if clean := strings.TrimSpace(item); clean != "" {
			if _, exists := seen[clean]; !exists {
				seen[clean] = struct{}{}
				out = append(out, clean)
			}
		}
	}
	return out
}

// NormalizeDiscordTargetID strips Model 2 session prefixes ("discord-" and optional
// colon-delimited pool scopes like "stateful_medium:discord-") from a target identifier.
func NormalizeDiscordTargetID(id string) string {
	clean := strings.TrimSpace(id)
	if idx := strings.LastIndex(clean, ":"); idx >= 0 {
		clean = strings.TrimSpace(clean[idx+1:])
	}
	return strings.TrimSpace(strings.TrimPrefix(clean, "discord-"))
}

// IsHeadlessSessionKey returns true if id is a Model 2 headless dispatch, heartbeat,
// or pooled worker session key.
func IsHeadlessSessionKey(id string) bool {
	clean := strings.TrimSpace(id)
	if idx := strings.LastIndex(clean, ":"); idx >= 0 {
		clean = strings.TrimSpace(clean[idx+1:])
	}
	return strings.HasPrefix(clean, "dispatch-") ||
		strings.HasPrefix(clean, "heartbeat-") ||
		strings.HasPrefix(clean, "worker-") ||
		strings.Contains(clean, "-worker-")
}

func IsValidDiscordSnowflake(id string) bool {
	return snowflakeRegex.MatchString(strings.TrimSpace(id))
}

func IsValidDiscordChannel(id string) bool {
	clean := strings.TrimSpace(id)
	return IsValidDiscordSnowflake(clean) || channelNameRegex.MatchString(clean)
}

// IsZeroSHA reports whether sha is empty or consists entirely of '0' characters.
func IsZeroSHA(sha string) bool {
	return strings.Trim(sha, "0") == ""
}

// Is4xxClientError returns true if the error indicates a 4xx HTTP client error or invalid Discord snowflake.
func Is4xxClientError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return re4xx.MatchString(msg) || strings.Contains(msg, "invalid discord snowflake")
}

func ExtractImageWorkloadName(imageRef string) string {
	ref := strings.TrimSpace(imageRef)
	if lastSlash := strings.LastIndex(ref, "/"); lastSlash != -1 {
		ref = ref[lastSlash+1:]
	}
	if colonIdx := strings.Index(ref, ":"); colonIdx != -1 {
		ref = ref[:colonIdx]
	}
	return strings.TrimSpace(ref)
}

func ResolveImageTag(repo, imageRef string) (tagKey, imageName, workloadName string) {
	workloadName = ExtractImageWorkloadName(imageRef)
	if workloadName == "" || strings.EqualFold(workloadName, "none") {
		return "", "", ""
	}
	switch repo[strings.LastIndex(repo, "/")+1:] {
	case "aerial":
		imageName = "aerial-" + strings.TrimPrefix(workloadName, "aerial-")
	case "aerial-sidecars":
		imageName = "aerial-sidecar-" + strings.TrimPrefix(workloadName, "aerial-sidecar-")
	case "mirrormere":
		if strings.HasPrefix(workloadName, "mirrormere") {
			imageName = workloadName
		} else {
			imageName = "mirrormere-" + workloadName
		}
	default:
		imageName = workloadName
	}
	tagKey = "TAG_" + strings.ToUpper(strings.ReplaceAll(imageName, "-", "_"))
	return tagKey, imageName, workloadName
}

func FormatErrorDetailsBlock(header, details, extraLine, footer string) string {
	lines := []string{header}
	if clean := strings.TrimSpace(details); clean != "" {
		lines = append(lines, fmt.Sprintf("Error details:\n```\n%s\n```", TruncatePromptDetails(clean)))
	}
	if cleanExtra := strings.TrimSpace(extraLine); cleanExtra != "" {
		lines = append(lines, cleanExtra)
	}
	if footer != "" {
		lines = append(lines, footer)
	}
	return strings.Join(lines, "\n")
}

func TruncatePromptDetails(s string) string {
	return TruncatePromptRunes(strings.ReplaceAll(s, "```", "'''"), MaxPromptRunes)
}

func TruncatePromptRunes(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "... (truncated)"
}

// ExtractStringSliceFromMetadata extracts a slice of strings stored under key in metadata JSONB.
func ExtractStringSliceFromMetadata(meta map[string]any, key string) []string {
	if meta == nil {
		return nil
	}
	var raw []string
	if rawVal, ok := meta[key]; ok {
		switch v := rawVal.(type) {
		case []any:
			for _, item := range v {
				if s, ok := item.(string); ok {
					raw = append(raw, s)
				}
			}
		case []string:
			raw = append(raw, v...)
		}
	}
	return DedupeTrimmedStrings(raw)
}

// ExtractJobsFromMetadata extracts candidate job names stored in the PR registry metadata JSONB.
func ExtractJobsFromMetadata(meta map[string]any) []string {
	return ExtractStringSliceFromMetadata(meta, "jobs")
}
