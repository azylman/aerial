package main

import "github.com/azylman/aerial/services/prospera/internal/audio"

type AMDAction = audio.AMDAction

const (
	AMDActionNone                    = audio.AMDActionNone
	AMDActionReleaseGreeting         = audio.AMDActionReleaseGreeting
	AMDActionSwitchToVoicemail       = audio.AMDActionSwitchToVoicemail
	AMDActionSpeakVoicemailAndHangup = audio.AMDActionSpeakVoicemailAndHangup
)

type AMDState = audio.AMDState

const (
	AMDStateHolding         = audio.AMDStateHolding
	AMDStateReleased        = audio.AMDStateReleased
	AMDStateMachineDetected = audio.AMDStateMachineDetected
	AMDStateVoicemailReady  = audio.AMDStateVoicemailReady
)

type AMDGreetingController = audio.AMDGreetingController

var NewAMDGreetingController = audio.NewAMDGreetingController
