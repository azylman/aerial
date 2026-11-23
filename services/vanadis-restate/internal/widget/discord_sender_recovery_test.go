package widget

import (
	"errors"
	"net/http"
	"testing"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/assert"
)

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
