package main

import (
	"strings"
	"sync"
)

// AMDAction represents an action triggered by the AMD state machine.
type AMDAction int

const (
	AMDActionNone AMDAction = iota
	AMDActionReleaseGreeting
	AMDActionSwitchToVoicemail
	AMDActionSpeakVoicemailAndHangup
)

// AMDState represents the internal state of greeting delivery.
type AMDState int

const (
	AMDStateHolding AMDState = iota
	AMDStateReleased
	AMDStateMachineDetected
	AMDStateVoicemailReady
)

// AMDGreetingController manages audio release and voicemail transition based on inbound cadence and webhooks.
type AMDGreetingController struct {
	mu                 sync.Mutex
	rmsThreshold       float64
	state              AMDState
	totalElapsedMs     int
	currentSpeechMs    int
	currentSilenceMs   int
	qualifiedSpeech    bool
	conversationActive bool
}

// NewAMDGreetingController initializes an AMDGreetingController.
func NewAMDGreetingController(rmsThreshold float64) *AMDGreetingController {
	if rmsThreshold <= 0 {
		rmsThreshold = 0.015
	}
	return &AMDGreetingController{
		rmsThreshold: rmsThreshold,
		state:        AMDStateHolding,
	}
}

// State returns the current AMD state.
func (c *AMDGreetingController) State() AMDState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// IsReleased reports whether the greeting has been released to play.
func (c *AMDGreetingController) IsReleased() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state >= AMDStateReleased
}

// MarkConversationActive locks the controller in live human conversation mode so late async AMD webhooks cannot hijack the call.
func (c *AMDGreetingController) MarkConversationActive() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.conversationActive = true
}

// OnInboundFrame processes an inbound audio frame RMS and elapsed duration or timestamp.
func (c *AMDGreetingController) OnInboundFrame(rms float64, elapsedMs int) AMDAction {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.state != AMDStateHolding {
		return AMDActionNone
	}

	var delta int
	if elapsedMs > c.totalElapsedMs {
		delta = elapsedMs - c.totalElapsedMs
		c.totalElapsedMs = elapsedMs
	} else {
		delta = elapsedMs
		c.totalElapsedMs += delta
	}

	if rms >= c.rmsThreshold {
		c.currentSpeechMs += delta
		c.currentSilenceMs = 0
		if c.currentSpeechMs >= 250 {
			c.qualifiedSpeech = true
		}
	} else {
		c.currentSilenceMs += delta
		c.currentSpeechMs = 0
		if c.qualifiedSpeech && c.currentSilenceMs >= 600 {
			c.state = AMDStateReleased
			return AMDActionReleaseGreeting
		}
	}

	if c.totalElapsedMs >= 2500 {
		if c.currentSpeechMs == 0 && !c.qualifiedSpeech {
			c.state = AMDStateReleased
			return AMDActionReleaseGreeting
		}
	}

	return AMDActionNone
}

// OnAMDWebhook handles Twilio Answering Machine Detection webhook callbacks.
func (c *AMDGreetingController) OnAMDWebhook(answeredBy string) AMDAction {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conversationActive {
		return AMDActionNone
	}

	norm := strings.ToLower(strings.TrimSpace(answeredBy))
	switch {
	case norm == "human" || norm == "unknown":
		if c.state == AMDStateHolding {
			c.state = AMDStateReleased
			return AMDActionReleaseGreeting
		}
		return AMDActionNone
	case norm == "machine_start" || norm == "fax":
		c.state = AMDStateMachineDetected
		return AMDActionSwitchToVoicemail
	case strings.HasPrefix(norm, "machine_end"):
		if norm == "machine_end_other" && c.state == AMDStateReleased {
			return AMDActionNone
		}
		c.state = AMDStateVoicemailReady
		return AMDActionSpeakVoicemailAndHangup
	default:
		return AMDActionNone
	}
}
