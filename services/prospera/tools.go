package main

import "github.com/azylman/aerial/services/prospera/internal/tools"

type ToolHandler = tools.ToolHandler

type InitiateCallArgs = tools.InitiateCallArgs

type InitiateCallOutput = tools.InitiateCallOutput

type SteerCallArgs = tools.SteerCallArgs

type SteerCallOutput = tools.SteerCallOutput

type EndCallArgs = tools.EndCallArgs

type EndCallOutput = tools.EndCallOutput

type GetCallStatusArgs = tools.GetCallStatusArgs

type GetCallStatusOutput = tools.GetCallStatusOutput

type ListCallsArgs = tools.ListCallsArgs

type ListCallsOutput = tools.ListCallsOutput

type SendSMSArgs = tools.SendSMSArgs

type SendSMSOutput = tools.SendSMSOutput

type ListSMSArgs = tools.ListSMSArgs

type ListSMSOutput = tools.ListSMSOutput

type EnsurePhoneNumberArgs = tools.EnsurePhoneNumberArgs

type EnsurePhoneNumberOutput = tools.EnsurePhoneNumberOutput

const serverInstructions = tools.ServerInstructions

var NewToolHandler = tools.NewToolHandler

var RegisterProsperaMCPTools = tools.RegisterProsperaMCPTools
