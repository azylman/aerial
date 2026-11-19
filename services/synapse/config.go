package main

import (
	"github.com/azylman/aerial/services/synapse/internal/config"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

type (
	Config                  = config.Config
	ConfigSource            = config.ConfigSource
	CollaboratorCIAlertRule = config.CollaboratorCIAlertRule
	LegacyBuildWorkflow     = config.LegacyBuildWorkflow
	RouterYAMLConfig        = config.RouterYAMLConfig
)

const (
	maxPromptRunes         = model.MaxPromptRunes
	maxRoboRevCommentRunes = model.MaxRoboRevCommentRunes
)

func LoadConfigFromEnv() Config {
	return config.LoadConfigFromEnv()
}

func loadConfigFromEnvStrict() (*Config, error) {
	return config.LoadConfigFromEnvStrict()
}

func synapseRestartRequired(old, next *Config) bool {
	return config.RestartRequired(old, next)
}

func (s *RouterServer) hasCollaboratorCIAlertsForRepo(repo string) bool {
	if s == nil || s.cfgSrc == nil {
		return false
	}
	return config.HasCollaboratorCIAlertsForRepo(s.cfgSrc.Current(), repo)
}

func (s *RouterServer) lookupCollaboratorCIAlert(repo, author string) (channelID, mention string, ok bool) {
	if s == nil || s.cfgSrc == nil {
		return "", "", false
	}
	return config.LookupCollaboratorCIAlert(s.cfgSrc.Current(), repo, author)
}

func firstNonEmpty(vals ...string) string {
	return model.FirstNonEmpty(vals...)
}

func randomNonce() string {
	return model.RandomNonce()
}

func normalizeRepo(repo string) string {
	return model.NormalizeRepo(repo)
}

func formatDirectMessagePrefix(prNumber int, repo string) string {
	return model.FormatDirectMessagePrefix(prNumber, repo)
}

func dedupeTrimmedStrings(items []string) []string {
	return model.DedupeTrimmedStrings(items)
}

func NormalizeDiscordTargetID(id string) string {
	return model.NormalizeDiscordTargetID(id)
}

func IsHeadlessSessionKey(id string) bool {
	return model.IsHeadlessSessionKey(id)
}

func IsValidDiscordSnowflake(id string) bool {
	return model.IsValidDiscordSnowflake(id)
}

func IsValidDiscordChannel(id string) bool {
	return model.IsValidDiscordChannel(id)
}

func is4xxClientError(err error) bool {
	return model.Is4xxClientError(err)
}

func formatErrorDetailsBlock(header, details, extraLine, footer string) string {
	return model.FormatErrorDetailsBlock(header, details, extraLine, footer)
}

func truncatePromptRunes(s string, maxRunes int) string {
	return model.TruncatePromptRunes(s, maxRunes)
}
