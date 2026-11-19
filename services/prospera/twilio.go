package main

import (
	"github.com/azylman/aerial/services/prospera/internal/twilio"
)

type StreamTokenManager = twilio.StreamTokenManager

type StreamTokenOption = twilio.StreamTokenOption

type CreateCallRequest = twilio.CreateCallRequest

type TwilioClient = twilio.TwilioClient

type TwilioOption = twilio.TwilioOption

type HTTPTwilioClient = twilio.HTTPTwilioClient

var ValidateTwilioSignature = twilio.ValidateTwilioSignature

var NewStreamTokenManager = twilio.NewStreamTokenManager

var WithEntropyReader = twilio.WithEntropyReader

var GenerateConnectStreamTwiML = twilio.GenerateConnectStreamTwiML

var GenerateVoicemailTwiML = twilio.GenerateVoicemailTwiML

var NewHTTPTwilioClient = twilio.NewHTTPTwilioClient

var WithTwilioBaseURL = twilio.WithTwilioBaseURL

var WithTwilioHTTPClient = twilio.WithTwilioHTTPClient

var EscapeXML = twilio.EscapeXML
