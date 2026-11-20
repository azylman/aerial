package widget

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/services/vanadis-restate/internal/ericlient"
)

func TestDeliverStatusWidgetFunction(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	notFoundErr := &discordgo.RESTError{Response: &http.Response{StatusCode: http.StatusNotFound}}
	foreignAuthorErr := &discordgo.RESTError{Response: &http.Response{StatusCode: http.StatusForbidden}, Message: &discordgo.APIErrorMessage{Code: 50005, Message: "Cannot edit a message authored by another user"}}
	missingPermsErr := &discordgo.RESTError{Response: &http.Response{StatusCode: http.StatusForbidden}, Message: &discordgo.APIErrorMessage{Code: 50013, Message: "Missing Permissions"}}
	missingAccessErr := &discordgo.RESTError{Response: &http.Response{StatusCode: http.StatusForbidden}, Message: &discordgo.APIErrorMessage{Code: 50001, Message: "Missing Access"}}

	tests := []struct {
		name           string
		expectSent     bool
		expectDeleteID string
		nilSender      bool
		req            ericlient.StatusWidgetRequest
		nilSendMsg     bool
		sendErr        error
		sendMsg        *discordgo.Message
		editErr        error
		editMsg        *discordgo.Message
		deleteErr      error
		msgs           []*discordgo.Message
		expectID       string
		expectErrSub   string
	}{
		{name: "nil sender returns error", nilSender: true, req: ericlient.StatusWidgetRequest{ChannelID: "chan-1"}, expectErrSub: "discord widget sender is nil"},
		{name: "empty channel ID returns error", req: ericlient.StatusWidgetRequest{ChannelID: ""}, expectErrSub: "channel_id is required"},
		{name: "new message success", expectSent: true, req: ericlient.StatusWidgetRequest{ChannelID: "chan-1", Title: "Build 1", Status: "running"}, sendMsg: &discordgo.Message{ID: "msg-new-123"}, expectID: "msg-new-123"},
		{name: "new message send failure", expectSent: true, req: ericlient.StatusWidgetRequest{ChannelID: "chan-1", Title: "Build 1"}, sendErr: errors.New("network failure"), expectErrSub: "failed to send status widget message"},
		{name: "new message send nil message returns error", expectSent: true, req: ericlient.StatusWidgetRequest{ChannelID: "chan-1", Title: "Build 1"}, nilSendMsg: true, expectErrSub: "received nil message"},
		{name: "edit existing message success", req: ericlient.StatusWidgetRequest{ChannelID: "chan-1", MessageID: "msg-orig-1", Title: "Build 1"}, editMsg: &discordgo.Message{ID: "msg-orig-1"}, expectID: "msg-orig-1"},
		{name: "edit existing message 404 recovers by creating new", expectSent: true, req: ericlient.StatusWidgetRequest{ChannelID: "chan-1", MessageID: "msg-deleted-1", Title: "Build 1"}, editErr: notFoundErr, sendMsg: &discordgo.Message{ID: "msg-recreated-2"}, expectID: "msg-recreated-2"},
		{name: "edit existing message 404 recreate error fails", expectSent: true, req: ericlient.StatusWidgetRequest{ChannelID: "chan-1", MessageID: "msg-deleted-1", Title: "Build 1"}, editErr: notFoundErr, sendErr: errors.New("recreate error"), expectErrSub: "failed to send new status widget message after 404 edit"},
		{name: "edit existing message 404 recreate nil message fails", expectSent: true, req: ericlient.StatusWidgetRequest{ChannelID: "chan-1", MessageID: "msg-deleted-1", Title: "Build 1"}, editErr: notFoundErr, nilSendMsg: true, expectErrSub: "received nil message"},
		{name: "edit existing non-404 error fails", req: ericlient.StatusWidgetRequest{ChannelID: "chan-1", MessageID: "msg-err-1", Title: "Build 1"}, editErr: errors.New("edit permission denied"), expectErrSub: "failed to edit status widget message"},
		{name: "edit foreign-authored message re-posts and deletes old", req: ericlient.StatusWidgetRequest{ChannelID: "chan-1", MessageID: "msg-foreign-1", Title: "Build 1"}, editErr: foreignAuthorErr, sendMsg: &discordgo.Message{ID: "msg-reposted-1"}, expectID: "msg-reposted-1", expectSent: true, expectDeleteID: "msg-foreign-1"},
		{name: "edit foreign-authored message delete missing permissions still returns new ID", req: ericlient.StatusWidgetRequest{ChannelID: "chan-1", MessageID: "msg-foreign-2", Title: "Build 1"}, editErr: foreignAuthorErr, sendMsg: &discordgo.Message{ID: "msg-reposted-2"}, deleteErr: missingPermsErr, expectID: "msg-reposted-2", expectSent: true, expectDeleteID: "msg-foreign-2"},
		{name: "edit foreign-authored message delete 404 still returns new ID", req: ericlient.StatusWidgetRequest{ChannelID: "chan-1", MessageID: "msg-foreign-3", Title: "Build 1"}, editErr: foreignAuthorErr, sendMsg: &discordgo.Message{ID: "msg-reposted-3"}, deleteErr: notFoundErr, expectID: "msg-reposted-3", expectSent: true, expectDeleteID: "msg-foreign-3"},
		{name: "edit foreign-authored message delete other error still returns new ID", req: ericlient.StatusWidgetRequest{ChannelID: "chan-1", MessageID: "msg-foreign-4", Title: "Build 1"}, editErr: foreignAuthorErr, sendMsg: &discordgo.Message{ID: "msg-reposted-4"}, deleteErr: errors.New("delete timeout"), expectID: "msg-reposted-4", expectSent: true, expectDeleteID: "msg-foreign-4"},
		{name: "edit foreign-authored message re-post error fails", req: ericlient.StatusWidgetRequest{ChannelID: "chan-1", MessageID: "msg-foreign-5", Title: "Build 1"}, editErr: foreignAuthorErr, sendErr: errors.New("repost error"), expectSent: true, expectErrSub: "failed to send new status widget message after foreign-author edit"},
		{name: "edit foreign-authored message re-post nil message fails", req: ericlient.StatusWidgetRequest{ChannelID: "chan-1", MessageID: "msg-foreign-6", Title: "Build 1"}, editErr: foreignAuthorErr, nilSendMsg: true, expectSent: true, expectErrSub: "after foreign-author edit: received nil message"},
		{name: "edit missing access 403 fails without re-posting", req: ericlient.StatusWidgetRequest{ChannelID: "chan-1", MessageID: "msg-noaccess-1", Title: "Build 1"}, editErr: missingAccessErr, expectErrSub: "failed to edit status widget message msg-noaccess-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var sender DiscordWidgetSender
			mock := &mockWidgetSender{
				sendMsg:    tc.sendMsg,
				nilSendMsg: tc.nilSendMsg,
				sendErr:    tc.sendErr,
				editMsg:    tc.editMsg,
				editErr:    tc.editErr,
				deleteErr:  tc.deleteErr,
				msgs:       tc.msgs,
			}
			if !tc.nilSender {
				sender = mock
			}

			id, err := DeliverStatusWidget(sender, tc.req, now)
			if tc.expectErrSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectErrSub)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectID, id)
			}
			assert.Equal(t, tc.expectSent, mock.lastSendData != nil)
			assert.Equal(t, tc.expectDeleteID, mock.lastDeleteID)
		})
	}
}

func TestIsNotFoundError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"standard error", errors.New("boom"), false},
		{"404 RESTError", &discordgo.RESTError{Response: &http.Response{StatusCode: http.StatusNotFound}}, true},
		{"code 10008 RESTError", &discordgo.RESTError{Message: &discordgo.APIErrorMessage{Code: 10008}}, true},
		{"other RESTError", &discordgo.RESTError{Response: &http.Response{StatusCode: http.StatusInternalServerError}}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, isNotFoundError(tc.err))
		})
	}
}

func TestIsForeignAuthorError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"standard error", errors.New("boom"), false},
		{"code 50005 RESTError", &discordgo.RESTError{Response: &http.Response{StatusCode: http.StatusForbidden}, Message: &discordgo.APIErrorMessage{Code: 50005}}, true},
		{"code 50001 RESTError", &discordgo.RESTError{Response: &http.Response{StatusCode: http.StatusForbidden}, Message: &discordgo.APIErrorMessage{Code: 50001}}, false},
		{"403 RESTError without body", &discordgo.RESTError{Response: &http.Response{StatusCode: http.StatusForbidden}}, false},
		{"404 RESTError", &discordgo.RESTError{Response: &http.Response{StatusCode: http.StatusNotFound}}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, isForeignAuthorError(tc.err))
		})
	}
}
