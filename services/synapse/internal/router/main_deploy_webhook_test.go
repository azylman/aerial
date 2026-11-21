package router

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// --- 5. Outbound Dispatcher ---

func TestDispatchDeploymentSuccessPrompt(t *testing.T) {
	t.Parallel()
	validSnowflake := "100000000000000001"
	tests := []struct {
		name           string
		targetID       string
		wantPrompts    int
		wantPromptSubs []string
	}{
		{
			name:        "valid target prompts the system channel",
			targetID:    validSnowflake,
			wantPrompts: 1,
			wantPromptSubs: []string{
				"Continuous Delivery deployment completed for job brain on azylman/aerial (PR #0, commit: sha123, ref: alloc-1).",
				"Directive: Confirm the deployment and suggest logical next steps, follow-up verification, or related tasks concisely.",
			},
		},
		{name: "invalid snowflake is no-op", targetID: "invalid-snowflake"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDisp := &mockOutboundDispatcher{}
			srv := newTestRouter(Config{}, nil, mockDisp)
			evt := DeployEvent{TargetID: tc.targetID, JobName: "brain", Repo: "azylman/aerial", CommitSHA: "sha123", DeploymentID: "alloc-1"}
			if err := srv.dispatchDeploymentSuccessPrompt(context.Background(), evt); err != nil {
				t.Fatalf("dispatchDeploymentSuccessPrompt() = %v", err)
			}
			pCalls := mockDisp.PromptCalls()
			if len(pCalls) != tc.wantPrompts {
				t.Fatalf("got %d prompts, want %d", len(pCalls), tc.wantPrompts)
			}
			for _, sub := range tc.wantPromptSubs {
				if !strings.Contains(pCalls[0].Prompt, sub) {
					t.Errorf("prompt %q missing %q", pCalls[0].Prompt, sub)
				}
			}
			if got := len(mockDisp.DirectMessageCalls()); got != 0 {
				t.Errorf("got %d DMs, want 0", got)
			}
		})
	}
}

func TestDispatchAlertWithPrompt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	validSnowflake := "123456789012345678"

	tests := []struct {
		name          string
		nilDispatcher bool
		targetID      string
		dmMsg         string
		prompt        string
		opt           *alertPromptOpts
		dispatchErr   error
		wantErr       bool
		wantDMs       int
		wantPrompts   int
		wantBg        int
	}{
		{
			name:          "nil dispatcher returns nil",
			nilDispatcher: true,
			targetID:      validSnowflake,
			prompt:        "test prompt",
		},
		{
			name:        "invalid target without silent is no-op",
			targetID:    "invalid-snowflake",
			prompt:      "test prompt",
			wantPrompts: 0,
		},
		{
			name:        "valid target with dm and prompt dispatches both",
			targetID:    validSnowflake,
			dmMsg:       "direct message alert",
			prompt:      "prompt directive",
			wantDMs:     1,
			wantPrompts: 1,
		},
		{
			name:        "prompt dispatch error returns error",
			targetID:    validSnowflake,
			prompt:      "prompt directive",
			dispatchErr: errors.New("prompt dispatch failed"),
			wantErr:     true,
			wantPrompts: 1,
		},
		{
			name:     "silent background dispatch",
			targetID: validSnowflake,
			prompt:   "background prompt",
			opt: &alertPromptOpts{
				silent:       true,
				skipIfActive: true,
				effort:       "high",
				dispatchID:   "disp-1",
			},
			wantBg: 1,
		},
		{
			name:     "silent background dispatch error returns error",
			targetID: validSnowflake,
			prompt:   "background prompt",
			opt: &alertPromptOpts{
				silent: true,
			},
			dispatchErr: errors.New("background dispatch failed"),
			wantErr:     true,
			wantBg:      1,
		},
		{
			name:        "dmMsg dispatch error logs warning and continues to prompt",
			targetID:    validSnowflake,
			dmMsg:       "dm alert",
			prompt:      "prompt directive",
			dispatchErr: errors.New("dm error"),
			wantErr:     true, // prompt also fails when dispatcher returns error
			wantDMs:     1,
			wantPrompts: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := NewRouterServer(fixedConfig(Config{}), nil)
			var mockDisp *mockOutboundDispatcher
			if !tc.nilDispatcher {
				mockDisp = &mockOutboundDispatcher{
					promptErr:        tc.dispatchErr,
					dispatchErr:      tc.dispatchErr,
					directMessageErr: tc.dispatchErr,
				}
				srv.SetDispatcher(mockDisp)
			}

			var aOpts []alertPromptOpts
			if tc.opt != nil {
				aOpts = append(aOpts, *tc.opt)
			}
			err := srv.dispatchAlertWithPrompt(ctx, tc.targetID, "test_alert", tc.dmMsg, tc.prompt, aOpts...)
			if (err != nil) != tc.wantErr {
				t.Fatalf("dispatchAlertWithPrompt() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if mockDisp != nil {
				if len(mockDisp.DirectMessageCalls()) != tc.wantDMs {
					t.Errorf("got %d DMs, want %d", len(mockDisp.DirectMessageCalls()), tc.wantDMs)
				}
				if len(mockDisp.PromptCalls()) != tc.wantPrompts {
					t.Errorf("got %d prompts, want %d", len(mockDisp.PromptCalls()), tc.wantPrompts)
				}
				if len(mockDisp.DispatchCalls()) != tc.wantBg {
					t.Errorf("got %d bg calls, want %d", len(mockDisp.DispatchCalls()), tc.wantBg)
				}
			}
		})
	}
}
