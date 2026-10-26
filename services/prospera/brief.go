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
	Mode               string       `json:"mode,omitempty"`
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

const defaultApprovalFirewall = "ZERO SPEND AUTHORIZED. You are not authorized to agree to any financial charges, fees, deposits, or non-refundable commitments. If the other party requires payment or financial authorization, politely inform them you will have the person you are calling for follow up directly."

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

	if trimmedMode := strings.TrimSpace(brief.Mode); trimmedMode != "" {
		switch CallMode(trimmedMode) {
		case ModeConversation, ModeNotify:
			brief.Mode = trimmedMode
		default:
			return "", brief, fmt.Errorf("invalid mode %q: must be conversation or notify", brief.Mode)
		}
	} else {
		brief.Mode = string(ModeConversation)
	}

	trimmedMessage := strings.TrimSpace(message)
	if strings.TrimSpace(brief.Task) == "" && trimmedMessage != "" {
		brief.Task = trimmedMessage
	}

	if strings.TrimSpace(brief.Task) == "" {
		return "", brief, fmt.Errorf("opening message or task is required")
	}

	if err := validateBriefLimits(brief); err != nil {
		return "", brief, err
	}

	normalizeBriefDefaults(&brief)
	return normTo, brief, nil
}

func validateBriefLimits(b CallBrief) error {
	limits := []struct {
		name string
		val  int
		max  int
	}{
		{"task", len(b.Task), 2000},
		{"context", len(b.Context), 4000},
		{"approvals", len(b.Approvals), 2000},
		{"voicemail_message", len(b.VoicemailMessage), 1000},
		{"success_criteria", len(b.SuccessCriteria), 1000},
		{"disclosures", len(b.Disclosures), 20},
	}
	for _, l := range limits {
		if l.val > l.max {
			unit := "characters"
			metric := "length"
			if l.name == "disclosures" {
				unit = "items"
				metric = "count"
			}
			return fmt.Errorf("%s exceeds %d %s (%s: %d)", l.name, l.max, unit, metric, l.val)
		}
	}
	return nil
}

func normalizeBriefDefaults(b *CallBrief) {
	if b.Language == "" {
		b.Language = "en-US"
	}
	if b.Identity.Disclose == "" {
		b.Identity.Disclose = "when-asked"
	}
	if b.MaxDurationSeconds <= 0 {
		b.MaxDurationSeconds = 300
	} else if b.MaxDurationSeconds > 900 {
		b.MaxDurationSeconds = 900
	}
	if strings.TrimSpace(b.Approvals) == "" {
		b.Approvals = defaultApprovalFirewall
	}
}

// BuildSystemPrompt constructs the system prompt for the Gemini Live session.
func BuildSystemPrompt(brief CallBrief, mode CallMode, ownerInstructions []string) string {
	var sb strings.Builder

	sb.WriteString("You are Aerial, an assistant making or handling a phone call.\n")
	sb.WriteString("Speak naturally, concisely, and professionally. Keep spoken turns brief (1-2 sentences) suited for real-time telephone conversation.\n\n")

	if brief.Language != "" {
		sb.WriteString(fmt.Sprintf("SPOKEN LANGUAGE: %s\n\n", brief.Language))
	}

	if brief.Identity.Introduction != "" {
		sb.WriteString(fmt.Sprintf("IDENTITY INTRODUCTION: %s\n", brief.Identity.Introduction))
	}
	switch brief.Identity.Disclose {
	case "", "when-asked":
		if brief.Identity.Introduction == "" {
			sb.WriteString("AI IDENTITY DISCLOSURE POLICY: when-asked (Progressive disclosure: focus directly on the task without introducing yourself upfront. If asked who is calling or for your name, state that your name is Aerial and phrase your role naturally for the situation—for example, '<Name>'s assistant' when the person's name has been given or is in the permitted disclosures/context, 'the homeowner's assistant' for a contractor, or simply that you are an assistant helping schedule or check on this—never literally say 'the owner's assistant' where it sounds unnatural like at a restaurant or dentist. Only disclose that you are an AI assistant if explicitly asked whether you are an AI or automated system.)\n\n")
		} else {
			sb.WriteString("AI IDENTITY DISCLOSURE POLICY: when-asked (Progressive disclosure: if asked who is calling or for your name, state that your name is Aerial and phrase your role naturally for the situation—for example, '<Name>'s assistant' when the person's name has been given or is in the permitted disclosures/context, 'the homeowner's assistant' for a contractor, or simply that you are an assistant helping schedule or check on this—never literally say 'the owner's assistant' where it sounds unnatural like at a restaurant or dentist. Only disclose that you are an AI assistant if explicitly asked whether you are an AI or automated system.)\n\n")
		}
	default:
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
