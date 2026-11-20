package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/bwmarrin/discordgo"
)

func TestDeriveThreadTitle_TableDriven(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{"empty string", "", "Aerial Discussion"},
		{"whitespace only", "   \n\n\t  ", "Aerial Discussion"},
		{"pure user mention", "<@1542035925603713086>", "Aerial Discussion"},
		{"pure nickname mention", "<@!1542035925603713086>", "Aerial Discussion"},
		{"pure role mention", "<@&9999999999>", "Aerial Discussion"},
		{"pure channel mention", "<#8888888888>", "Aerial Discussion"},
		{"pure custom emoji", "<:pepe:1234567>", "Aerial Discussion"},
		{"pure animated emoji", "<a:party_parrot:7654321>", "Aerial Discussion"},
		{"mentions with leading markdown header and body", "<@12345> ### Deploying to production cluster", "Deploying to production cluster"},
		{"first line header only followed by actual body", "###\nDatabase latency spiked to 250ms", "Database latency spiked to 250ms"},
		{"blockquote formatting", "> Critical alert: CPU throttled", "Critical alert: CPU throttled"},
		{"exact 60 runes", strings.Repeat("a", 60), strings.Repeat("a", 60)},
		{"61 runes truncated with ellipsis", strings.Repeat("b", 61), strings.Repeat("b", 57) + "..."},
		{"multibyte japanese runes truncated safely", strings.Repeat("こんにちは世界", 10), string([]rune(strings.Repeat("こんにちは世界", 10))[:57]) + "..."},
		{"trailing ZWJ trimmed before ellipsis", strings.Repeat("x", 56) + "\u200D" + strings.Repeat("y", 10), strings.Repeat("x", 56) + "..."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DeriveThreadTitle(tt.content); got != tt.want {
				t.Errorf("DeriveThreadTitle() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIsThreadAlreadyExistsError_TableDriven(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"discordgo RESTError with code 160004", &discordgo.RESTError{Message: &discordgo.APIErrorMessage{Code: 160004}}, true},
		{"discordgo RESTError with code in response body", &discordgo.RESTError{ResponseBody: []byte(`{"code": 160004, "message": "Thread already exists"}`)}, true},
		{"discordgo RESTError with already been created body string", &discordgo.RESTError{ResponseBody: []byte(`{"message": "A thread has already been created for this message"}`)}, true},
		{"discordgo RESTError with different code", &discordgo.RESTError{Message: &discordgo.APIErrorMessage{Code: 50001}, ResponseBody: []byte(`{"code": 50001, "message": "Missing Access"}`)}, false},
		{"generic error string with 160004", errors.New("HTTP 400 Bad Request: 160004 error"), true},
		{"generic error string with already been created", errors.New("thread has already been created"), true},
		{"unrelated generic error", errors.New("connection refused"), false},
		{"wrapped RESTError", fmt.Errorf("outer wrapper: %w", discordgo.RESTError{Message: &discordgo.APIErrorMessage{Code: 160004}}), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsThreadAlreadyExistsError(tt.err); got != tt.want {
				t.Errorf("IsThreadAlreadyExistsError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResolveGuildID_TableDriven(t *testing.T) {
	tests := []struct {
		name      string
		msgID     string
		cachedID  string
		channelID string
		want      string
	}{
		{"msgGuildID priority", "g-msg", "g-cached", "g-chan", "g-msg"},
		{"cachedGuildID priority when msg empty", "", "g-cached", "g-chan", "g-cached"},
		{"channelGuildID fallback", "", "", "g-chan", "g-chan"},
		{"all empty", "", "", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ResolveGuildID(tt.msgID, tt.cachedID, tt.channelID); got != tt.want {
				t.Errorf("ResolveGuildID() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestValidateDispatchRequest(t *testing.T) {
	tests := []struct {
		name    string
		req     *DispatchRequest
		wantErr bool
	}{
		{
			name:    "invalid source",
			req:     &DispatchRequest{Source: "INVALID!SOURCE", Prompt: "test"},
			wantErr: true,
		},
		{
			name:    "empty prompt",
			req:     &DispatchRequest{Source: "heartbeat", Prompt: ""},
			wantErr: true,
		},
		{
			name:    "valid request",
			req:     &DispatchRequest{Source: "heartbeat", Prompt: "test prompt"},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateDispatchRequest(tc.req)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateDispatchRequest() error = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestValidatePromptEffort(t *testing.T) {
	tests := []struct {
		name    string
		effort  string
		wantErr bool
	}{
		{name: "empty effort", effort: "", wantErr: false},
		{name: "low effort", effort: "low", wantErr: false},
		{name: "medium effort", effort: "medium", wantErr: false},
		{name: "high effort", effort: "high", wantErr: false},
		{name: "invalid effort", effort: "extreme", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validatePromptEffort(tc.effort)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validatePromptEffort(%q) error = %v, wantErr = %v", tc.effort, err, tc.wantErr)
			}
		})
	}
}
