package main

import (
	"strings"
	"testing"
)

func TestValidateInitiateCallArgs(t *testing.T) {
	t.Parallel()
	longString := func(n int) string {
		return strings.Repeat("a", n)
	}

	tests := []struct {
		name          string
		toRaw         string
		fromNumber    string
		message       string
		brief         CallBrief
		wantErr       bool
		wantTo        string
		wantLanguage  string
		wantDisclose  string
		wantMaxDur    int
		wantApprovals string
		wantTask      string
	}{
		{
			name:       "valid standard call with defaults applied",
			toRaw:      "(510) 555-0199",
			fromNumber: "+15105550100",
			message:    "Hello, I am calling to check table availability.",
			brief: CallBrief{
				Task: "Check dinner availability for 4 people at 7pm tonight.",
			},
			wantErr:       false,
			wantTo:        "+15105550199",
			wantLanguage:  "en-US",
			wantDisclose:  "volunteer",
			wantMaxDur:    300,
			wantApprovals: defaultApprovalFirewall,
			wantTask:      "Check dinner availability for 4 people at 7pm tonight.",
		},
		{
			name:          "message copied to brief task when task empty",
			toRaw:         "+15105550199",
			fromNumber:    "+15105550100",
			message:       "Calling for reservation",
			brief:         CallBrief{},
			wantErr:       false,
			wantTo:        "+15105550199",
			wantLanguage:  "en-US",
			wantDisclose:  "volunteer",
			wantMaxDur:    300,
			wantApprovals: defaultApprovalFirewall,
			wantTask:      "Calling for reservation",
		},
		{
			name:       "custom brief fields preserved and max duration capped at 900",
			toRaw:      "+15105550199",
			fromNumber: "+15105550100",
			message:    "Calling for reservation",
			brief: CallBrief{
				Task:               "Make a reservation",
				Language:           "es-US",
				Identity:           CallIdentity{Introduction: "Hola", Disclose: "when-asked"},
				Approvals:          "Authorized to commit up to $50 deposit.",
				MaxDurationSeconds: 1200,
			},
			wantErr:       false,
			wantTo:        "+15105550199",
			wantLanguage:  "es-US",
			wantDisclose:  "when-asked",
			wantMaxDur:    900,
			wantApprovals: "Authorized to commit up to $50 deposit.",
		},
		{
			name:       "empty message and task rejected",
			toRaw:      "+15105550199",
			fromNumber: "+15105550100",
			message:    "   ",
			brief:      CallBrief{Task: "   "},
			wantErr:    true,
		},
		{
			name:       "self-call rejected",
			toRaw:      "(510) 555-0100",
			fromNumber: "+1-510-555-0100",
			message:    "Loopback",
			brief:      CallBrief{Task: "Self-test"},
			wantErr:    true,
		},
		{
			name:       "invalid destination phone number",
			toRaw:      "not-a-number",
			fromNumber: "+15105550100",
			message:    "Hello",
			brief:      CallBrief{Task: "Test"},
			wantErr:    true,
		},
		{
			name:       "task exceeds 2000 chars",
			toRaw:      "+15105550199",
			fromNumber: "+15105550100",
			message:    "Hello",
			brief:      CallBrief{Task: longString(2001)},
			wantErr:    true,
		},
		{
			name:       "context exceeds 4000 chars",
			toRaw:      "+15105550199",
			fromNumber: "+15105550100",
			message:    "Hello",
			brief:      CallBrief{Task: "Task", Context: longString(4001)},
			wantErr:    true,
		},
		{
			name:       "approvals exceeds 2000 chars",
			toRaw:      "+15105550199",
			fromNumber: "+15105550100",
			message:    "Hello",
			brief:      CallBrief{Task: "Task", Approvals: longString(2001)},
			wantErr:    true,
		},
		{
			name:       "voicemail_message exceeds 1000 chars",
			toRaw:      "+15105550199",
			fromNumber: "+15105550100",
			message:    "Hello",
			brief:      CallBrief{Task: "Task", VoicemailMessage: longString(1001)},
			wantErr:    true,
		},
		{
			name:       "success_criteria exceeds 1000 chars",
			toRaw:      "+15105550199",
			fromNumber: "+15105550100",
			message:    "Hello",
			brief:      CallBrief{Task: "Task", SuccessCriteria: longString(1001)},
			wantErr:    true,
		},
		{
			name:       "disclosures exceeds 20 items",
			toRaw:      "+15105550199",
			fromNumber: "+15105550100",
			message:    "Hello",
			brief: CallBrief{
				Task:        "Task",
				Disclosures: make([]string, 21),
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotTo, gotBrief, err := ValidateInitiateCallArgs(tc.toRaw, tc.fromNumber, tc.message, tc.brief)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateInitiateCallArgs() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if gotTo != tc.wantTo {
				t.Errorf("gotTo = %q, want %q", gotTo, tc.wantTo)
			}
			if gotBrief.Language != tc.wantLanguage {
				t.Errorf("gotBrief.Language = %q, want %q", gotBrief.Language, tc.wantLanguage)
			}
			if gotBrief.Identity.Disclose != tc.wantDisclose {
				t.Errorf("gotBrief.Identity.Disclose = %q, want %q", gotBrief.Identity.Disclose, tc.wantDisclose)
			}
			if gotBrief.MaxDurationSeconds != tc.wantMaxDur {
				t.Errorf("gotBrief.MaxDurationSeconds = %d, want %d", gotBrief.MaxDurationSeconds, tc.wantMaxDur)
			}
			if tc.wantApprovals != "" && gotBrief.Approvals != tc.wantApprovals {
				t.Errorf("gotBrief.Approvals = %q, want %q", gotBrief.Approvals, tc.wantApprovals)
			}
			if tc.wantTask != "" && gotBrief.Task != tc.wantTask {
				t.Errorf("gotBrief.Task = %q, want %q", gotBrief.Task, tc.wantTask)
			}
		})
	}
}

func TestBuildSystemPrompt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name              string
		brief             CallBrief
		mode              CallMode
		ownerInstructions []string
		wantContains      []string
	}{
		{
			name: "conversation mode with hold music and dtmf instructions",
			brief: CallBrief{
				Task:            "Reserve a table",
				Context:         "Anniversary dinner",
				Language:        "en-US",
				Identity:        CallIdentity{Introduction: "Hello, this is Prospera", Disclose: "volunteer"},
				Disclosures:     []string{"Phone number: 510-555-0100", "Party size: 4"},
				Approvals:       "Zero spend authorized.",
				SuccessCriteria: "Confirmation number received",
			},
			mode:              ModeConversation,
			ownerInstructions: []string{"Mention we prefer outdoor seating"},
			wantContains: []string{
				"Reserve a table",
				"Anniversary dinner",
				"SPOKEN LANGUAGE: en-US",
				"IDENTITY INTRODUCTION: Hello, this is Prospera",
				"AI IDENTITY DISCLOSURE POLICY: volunteer",
				"PERMITTED DISCLOSURES:",
				"- Phone number: 510-555-0100",
				"HOLD MUSIC & SILENCE RULE",
				"DTMF & PHONE TREES",
				"Zero spend authorized.",
				"Confirmation number received",
				"Mention we prefer outdoor seating",
			},
		},
		{
			name: "callback mode receptionist guardrail",
			brief: CallBrief{
				Task: "Answering callback regarding dentist appointment",
			},
			mode:              ModeCallback,
			ownerInstructions: nil,
			wantContains: []string{
				"CALLBACK RECEPTIONIST MODE",
				"dentist appointment",
			},
		},
		{
			name: "notify mode instruction",
			brief: CallBrief{
				Task: "Notify client that technician has arrived",
			},
			mode:              ModeNotify,
			ownerInstructions: nil,
			wantContains: []string{
				"NOTIFY MODE",
				"technician has arrived",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			prompt := BuildSystemPrompt(tc.brief, tc.mode, tc.ownerInstructions)
			for _, substr := range tc.wantContains {
				if !strings.Contains(prompt, substr) {
					t.Errorf("BuildSystemPrompt() missing expected substring %q in prompt:\n%s", substr, prompt)
				}
			}
		})
	}
}

func TestResolveVoicemailMessage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		brief          CallBrief
		openingMessage string
		want           string
	}{
		{
			name: "explicit voicemail message in brief",
			brief: CallBrief{
				VoicemailMessage: "Hi, please call the account owner back at 510-555-0100.",
				Task:             "Book table",
			},
			openingMessage: "Hello, checking availability",
			want:           "Hi, please call the account owner back at 510-555-0100.",
		},
		{
			name: "opening message fallback when voicemail message is empty",
			brief: CallBrief{
				Task: "Book table",
			},
			openingMessage: "Hello, calling to check hours.",
			want:           "Hello, calling to check hours.",
		},
		{
			name: "task fallback when opening message is empty",
			brief: CallBrief{
				Task: "Inquire about plumbing service availability",
			},
			openingMessage: "",
			want:           "Hello, this is an automated message regarding: Inquire about plumbing service availability. Please return this call at your earliest convenience.",
		},
		{
			name:           "generic fallback when both brief and opening are empty",
			brief:          CallBrief{},
			openingMessage: "",
			want:           "Hello, please return this call at your convenience. Thank you.",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ResolveVoicemailMessage(tc.brief, tc.openingMessage)
			if got != tc.want {
				t.Fatalf("ResolveVoicemailMessage() = %q, want %q", got, tc.want)
			}
		})
	}
}
