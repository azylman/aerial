package main

import (
	"github.com/azylman/aerial/services/prospera/internal/delivery"
	"github.com/azylman/aerial/services/prospera/internal/model"
)

type Reporter = delivery.Reporter

type ReporterOption = delivery.ReporterOption

const (
	reportStatusPending    = "pending"
	reportStatusFailed     = "failed"
	reportStatusProcessing = "processing"
	reportStatusDelivered  = "delivered"
)

var BuildPostCallSummaryPrompt = model.BuildPostCallSummaryPrompt

var (
	WithReporterRecordingProvider = delivery.WithReporterRecordingProvider
	WithReporterHTTPClient        = delivery.WithReporterHTTPClient
	WithReporterGeminiBaseURL     = delivery.WithReporterGeminiBaseURL
	NewReporter                   = delivery.NewReporter
	ResolveDiscordChannel         = delivery.ResolveDiscordChannel
)
