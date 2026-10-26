package main

import (
	"fmt"
	"strings"
)

// CallMode represents the operating mode of a voice call session.
type CallMode string

const (
	ModeConversation CallMode = "conversation"
	ModeNotify       CallMode = "notify"
	ModeCallback     CallMode = "callback"
)

// CallIdentity specifies assistant self-identification rules for a call.
type CallIdentity struct {
	Introduction string `json:"introduction,omitempty"`
	Disclose     string `json:"disclose,omitempty"`
}

// CallBrief contains the intent, context, boundaries, and approval rules for a phone call.
type CallBrief struct {
	Task               string       `json:"task"`
	Context            string       `json:"context,omitempty"`
	Language           string       `json:"language,omitempty"`
	Identity           CallIdentity `json:"identity,omitempty"`
	Disclosures        []string     `json:"disclosures,omitempty"`
	Approvals          string       `json:"approvals,omitempty"`
	VoicemailMessage   string       `json:"voicemail_message,omitempty"`
	SuccessCriteria    string       `json:"success_criteria,omitempty"`
	MaxDurationSeconds int          `json:"max_duration_seconds,omitempty"`
}

const defaultApprovalFirewall = "ZERO SPEND AUTHORIZED. You are not authorized to agree to any financial charges, fees, deposits, or non-refundable commitments. If the other party requires payment or financial authorization, politely inform them you will have the account owner follow up directly."

// ValidateInitiateCallArgs validates and normalizes arguments for initiating a call.
func ValidateInitiateCallArgs(toRaw, fromNumber, message string, brief CallBrief) (string, CallBrief, error) {
	normTo, err := NormalizeE164(toRaw)
	if err != nil {
		return "", brief, fmt.Errorf("invalid destination phone number: %w", err)
	}

	if fromNumber != "" {
		normFrom, err := NormalizeE164(fromNumber)
		if err == nil && normTo == normFrom {
			return "", brief, fmt.Errorf("destination number cannot match source from_number")
		}
	}

	trimmedMessage := strings.TrimSpace(message)
	if strings.TrimSpace(brief.Task) == "" && trimmedMessage != "" {
		brief.Task = trimmedMessage
	}

	if strings.TrimSpace(brief.Task) == "" {
		return "", brief, fmt.Errorf("opening message or task is required")
	}

	if len(brief.Task) > 2000 {
		return "", brief, fmt.Errorf("task exceeds 2000 characters (length: %d)", len(brief.Task))
	}
	if len(brief.Context) > 4000 {
		return "", brief, fmt.Errorf("context exceeds 4000 characters (length: %d)", len(brief.Context))
	}
	if len(brief.Approvals) > 2000 {
		return "", brief, fmt.Errorf("approvals exceeds 2000 characters (length: %d)", len(brief.Approvals))
	}
	if len(brief.VoicemailMessage) > 1000 {
		return "", brief, fmt.Errorf("voicemail_message exceeds 1000 characters (length: %d)", len(brief.VoicemailMessage))
	}
	if len(brief.SuccessCriteria) > 1000 {
		return "", brief, fmt.Errorf("success_criteria exceeds 1000 characters (length: %d)", len(brief.SuccessCriteria))
	}
	if len(brief.Disclosures) > 20 {
		return "", brief, fmt.Errorf("disclosures exceeds 20 items (count: %d)", len(brief.Disclosures))
	}

	if brief.Language == "" {
		brief.Language = "en-US"
	}
	if brief.Identity.Disclose == "" {
		brief.Identity.Disclose = "volunteer"
	}
	if brief.MaxDurationSeconds <= 0 {
		brief.MaxDurationSeconds = 300
	} else if brief.MaxDurationSeconds > 900 {
		brief.MaxDurationSeconds = 900
	}

	if strings.TrimSpace(brief.Approvals) == "" {
		brief.Approvals = defaultApprovalFirewall
	}

	return normTo, brief, nil
}

// BuildSystemPrompt constructs the system prompt for the Gemini Live session.
func BuildSystemPrompt(brief CallBrief, mode CallMode, ownerInstructions []string) string {
	var sb strings.Builder

	sb.WriteString("You are Prospera, an autonomous AI voice phone assistant calling on behalf of the owner.\n")
	sb.WriteString("Speak naturally, concisely, and professionally. Keep spoken turns brief (1-2 sentences) suited for real-time telephone conversation.\n\n")

	if brief.Language != "" {
		sb.WriteString(fmt.Sprintf("SPOKEN LANGUAGE: %s\n\n", brief.Language))
	}

	if brief.Identity.Introduction != "" {
		sb.WriteString(fmt.Sprintf("IDENTITY INTRODUCTION: %s\n", brief.Identity.Introduction))
	}
	if brief.Identity.Disclose != "" {
		sb.WriteString(fmt.Sprintf("AI IDENTITY DISCLOSURE POLICY: %s\n\n", brief.Identity.Disclose))
	}

	sb.WriteString(fmt.Sprintf("TASK OBJECTIVE:\n%s\n\n", brief.Task))

	if brief.Context != "" {
		sb.WriteString(fmt.Sprintf("BACKGROUND CONTEXT:\n%s\n\n", brief.Context))
	}

	if len(brief.Disclosures) > 0 {
		sb.WriteString("PERMITTED DISCLOSURES:\n")
		for _, d := range brief.Disclosures {
			sb.WriteString(fmt.Sprintf("- %s\n", d))
		}
		sb.WriteString("\n")
	}

	sb.WriteString("APPROVAL FIREWALL:\n")
	sb.WriteString(brief.Approvals)
	sb.WriteString("\n\n")

	sb.WriteString("HOLD MUSIC & SILENCE RULE: When placed on hold or listening to hold music or automated silence, DO NOT speak or interrupt. Wait silently until a live human speaks.\n\n")

	sb.WriteString("DTMF & PHONE TREES: When navigating interactive voice response (IVR) phone menus, call the 'send_dtmf' tool with the exact key sequence needed.\n\n")

	if brief.SuccessCriteria != "" {
		sb.WriteString(fmt.Sprintf("SUCCESS CRITERIA (CONFIRM BEFORE ENDING):\n%s\n\n", brief.SuccessCriteria))
	}

	switch mode {
	case ModeCallback:
		sb.WriteString("CALLBACK RECEPTIONIST MODE: You are answering an inbound callback. Greet the caller, identify that you are returning or receiving their call regarding the recent inquiry, ask how you can help or take a message, and note their name and phone number. Do not promise specific commitments.\n\n")
	case ModeNotify:
		sb.WriteString("NOTIFY MODE: Deliver the intended notification message clearly, confirm receipt if possible, and conclude the call.\n\n")
	}

	if len(ownerInstructions) > 0 {
		sb.WriteString("LIVE OWNER GUIDANCE:\n")
		for _, instr := range ownerInstructions {
			sb.WriteString(fmt.Sprintf("- %s\n", instr))
		}
		sb.WriteString("\n")
	}

	return sb.String()
}

// ResolveVoicemailMessage determines the message to play or leave when voicemail is reached.
func ResolveVoicemailMessage(brief CallBrief, openingMessage string) string {
	if trimmed := strings.TrimSpace(brief.VoicemailMessage); trimmed != "" {
		return trimmed
	}
	if trimmed := strings.TrimSpace(openingMessage); trimmed != "" {
		return trimmed
	}
	if trimmed := strings.TrimSpace(brief.Task); trimmed != "" {
		return fmt.Sprintf("Hello, this is an automated message regarding: %s. Please return this call at your earliest convenience.", trimmed)
	}
	return "Hello, please return this call at your convenience. Thank you."
}
