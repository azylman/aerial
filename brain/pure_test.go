package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/bwmarrin/discordgo"
)

func TestOrdinal_TableDriven(t *testing.T) {
	tests := []struct {
		input int
		want  string
	}{
		{-11, "-11th"},
		{-1, "-1st"},
		{0, "0th"},
		{1, "1st"},
		{2, "2nd"},
		{3, "3rd"},
		{4, "4th"},
		{10, "10th"},
		{11, "11th"},
		{12, "12th"},
		{13, "13th"},
		{14, "14th"},
		{20, "20th"},
		{21, "21st"},
		{22, "22nd"},
		{23, "23rd"},
		{24, "24th"},
		{31, "31st"},
		{100, "100th"},
		{101, "101st"},
		{102, "102nd"},
		{103, "103rd"},
		{104, "104th"},
		{111, "111th"},
		{112, "112th"},
		{113, "113th"},
		{114, "114th"},
		{121, "121st"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("n=%d", tt.input), func(t *testing.T) {
			if got := Ordinal(tt.input); got != tt.want {
				t.Errorf("Ordinal(%d) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestFormatCronDescription_TableDriven(t *testing.T) {
	tests := []struct {
		expr string
		want string
	}{
		{"", ""},
		{"   ", ""},
		{"@yearly", "Every year on Jan 1st at 00:00"},
		{"@annually", "Every year on Jan 1st at 00:00"},
		{"@monthly", "1st of every month at 00:00"},
		{"@weekly", "Every week on Sunday at 00:00"},
		{"@daily", "Every day at 00:00"},
		{"@midnight", "Every day at 00:00"},
		{"@hourly", "Every hour"},
		{"@DAILY", "Every day at 00:00"},
		{"* *", "* *"},
		{"1 2 3 4 5 6", "1 2 3 4 5 6"},
		{"* * * * *", "Every minute"},
		{"*/15 * * * *", "Every 15 minutes"},
		{"0 */2 * * *", "Every 2 hours"},
		{"xx yy * * *", "xx yy * * *"},
		{"0 9 * * *", "Every day at 09:00"},
		{"0 9 * * 1-5", "Weekdays (Mon–Fri) at 09:00"},
		{"0 9 * * MON-FRI", "Weekdays (Mon–Fri) at 09:00"},
		{"0 9 * * 0,6", "Weekends (Sat–Sun) at 09:00"},
		{"0 9 * * SAT,SUN", "Weekends (Sat–Sun) at 09:00"},
		{"0 9 * * 0", "Every Sunday at 09:00"},
		{"0 9 * * 5", "Every Friday at 09:00"},
		{"0 9 * * 1,3,5", "Mon, Wed, Fri at 09:00"},
		{"0 9 * * Mon,Wed", "Mon, Wed at 09:00"},
		{"0 12 1 * *", "1st of every month at 12:00"},
		{"0 12 15 * *", "15th of every month at 12:00"},
		{"0 0 1 1 *", "Every year on Jan 1st at 00:00"},
		{"0 12 25 12 *", "Every year on Dec 25th at 12:00"},
		{"0 9 1-5 * *", "At 09:00 (cron: 0 9 1-5 * *)"},
		{"0 0 4 7 *", "Every year on Jul 4th at 00:00"},
		{"0 9 * 5 *", "At 09:00 (cron: 0 9 * 5 *)"},
		{"0/15 * * * *", "0/15 * * * *"},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			if got := FormatCronDescription(tt.expr); got != tt.want {
				t.Errorf("FormatCronDescription(%q) = %q, want %q", tt.expr, got, tt.want)
			}
		})
	}
}

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

func TestIsMessageableChannel_TableDriven(t *testing.T) {
	tests := []struct {
		chType discordgo.ChannelType
		want   bool
	}{
		{discordgo.ChannelTypeGuildText, true},
		{discordgo.ChannelTypeGuildNews, true},
		{discordgo.ChannelTypeGuildNewsThread, true},
		{discordgo.ChannelTypeGuildPublicThread, true},
		{discordgo.ChannelTypeGuildPrivateThread, true},
		{discordgo.ChannelTypeGuildVoice, false},
		{discordgo.ChannelTypeGuildCategory, false},
		{discordgo.ChannelTypeGuildStore, false},
		{discordgo.ChannelTypeGuildStageVoice, false},
		{discordgo.ChannelTypeGuildForum, false},
		{discordgo.ChannelTypeDM, false},
		{discordgo.ChannelTypeGroupDM, false},
		{discordgo.ChannelType(9999), false},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("type=%d", tt.chType), func(t *testing.T) {
			if got := IsMessageableChannel(tt.chType); got != tt.want {
				t.Errorf("IsMessageableChannel(%d) = %v, want %v", tt.chType, got, tt.want)
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
