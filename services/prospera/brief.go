package main

import "github.com/azylman/aerial/services/prospera/internal/model"

type CallMode = model.CallMode

const (
	ModeConversation = model.ModeConversation
	ModeNotify       = model.ModeNotify
	ModeCallback     = model.ModeCallback
)

type CallIdentity = model.CallIdentity

type CallBrief = model.CallBrief

const defaultApprovalFirewall = model.DefaultApprovalFirewall

var (
	ValidateInitiateCallArgs = model.ValidateInitiateCallArgs
	BuildSystemPrompt        = model.BuildSystemPrompt
	ResolveVoicemailMessage  = model.ResolveVoicemailMessage
)
