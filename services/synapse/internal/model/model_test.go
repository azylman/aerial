package model

import (
	"testing"
)

func TestPRRegisterRequest_OwningAgent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		req  PRRegisterRequest
		want string
	}{
		{
			name: "explicit agent takes precedence",
			req:  PRRegisterRequest{Agent: "agent-alpha", BotID: "aerial"},
			want: "agent-alpha",
		},
		{
			name: "agent empty falls back to bot_id",
			req:  PRRegisterRequest{Agent: "", BotID: "agent-beta"},
			want: "agent-beta",
		},
		{
			name: "both empty returns empty",
			req:  PRRegisterRequest{},
			want: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.req.OwningAgent(); got != tc.want {
				t.Errorf("PRRegisterRequest.OwningAgent() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestAerialPRMetadata_Methods(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		meta      AerialPRMetadata
		wantOK    bool
		wantOwner string
	}{
		{
			name:      "all empty",
			meta:      AerialPRMetadata{},
			wantOK:    false,
			wantOwner: "",
		},
		{
			name:      "target id only",
			meta:      AerialPRMetadata{TargetID: "123456789012345678"},
			wantOK:    true,
			wantOwner: "",
		},
		{
			name:      "bot id only",
			meta:      AerialPRMetadata{BotID: "aerial"},
			wantOK:    true,
			wantOwner: "aerial",
		},
		{
			name:      "agent takes precedence over bot id",
			meta:      AerialPRMetadata{Agent: "agent-alpha", BotID: "aerial"},
			wantOK:    true,
			wantOwner: "agent-alpha",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.meta.OK(); got != tc.wantOK {
				t.Errorf("AerialPRMetadata.OK() = %v, want %v", got, tc.wantOK)
			}
			if got := tc.meta.OwningAgent(); got != tc.wantOwner {
				t.Errorf("AerialPRMetadata.OwningAgent() = %q, want %q", got, tc.wantOwner)
			}
		})
	}
}
