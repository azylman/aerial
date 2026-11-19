package main

import (
	"github.com/azylman/aerial/services/prospera/internal/model"
	"github.com/azylman/aerial/services/prospera/internal/store"
)

type TranscriptTurn = model.TranscriptTurn

type OwnerInstruction = model.OwnerInstruction

type VoiceCall = model.VoiceCall

type VoiceMessage = model.VoiceMessage

const (
	callStateFailed     = "failed"
	callStateInProgress = "in-progress"
	directionInbound    = "inbound"
	directionOutbound   = "outbound"
)

type Store = store.Store

type PGStore = store.PGStore

var ErrMaxConcurrentCalls = store.ErrMaxConcurrentCalls

var ErrDuplicateActiveDest = store.ErrDuplicateActiveDest

var ErrCallNotFound = store.ErrCallNotFound

var ErrMessageNotFound = store.ErrMessageNotFound

var NewPGStore = store.NewPGStore

var OpenDB = store.OpenDB
