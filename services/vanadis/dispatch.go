package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/azylman/aerial/pkg/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// dispatchChunkTurn renders the chunk prompt (optionally prefixed by a harness notice) and dispatches it to Brain
// on the chunk's dedicated session with the capped chunk effort.
func (s *VanadisServer) dispatchChunkTurn(ctx context.Context, build VanadisBuild, chunks []VanadisChunk, activeIdx int, notice string) error {
	prompt, err := FormatChunkPrompt(build, chunks, activeIdx)
	if err != nil {
		return fmt.Errorf("format chunk prompt: %w", err)
	}
	chunk := chunks[activeIdx]
	ctx, span := tracing.Tracer().Start(ctx, "vanadis.dispatch_chunk", chunkDispatchSpanOptions(build.ID, chunk.ID, activeIdx)...)
	defer span.End()
	defer func() { markChunkDispatchSpan(span, err) }()
	err = s.dispatchBrainDispatch(ctx, BrainDispatchRequest{
		Source:         "vanadis",
		SessionID:      "vanadis-" + strings.TrimSpace(chunk.ID),
		TargetID:       strings.TrimSpace(build.ThreadID),
		VanadisChunkID: strings.TrimSpace(chunk.ID),
		Prompt:         notice + prompt,
		Effort:         capChunkEffort(chunk.Effort),
	})
	return err
}

func (s *VanadisServer) dispatchBrainDispatch(ctx context.Context, payload BrainDispatchRequest) error {
	_, err := s.postBrainJSON(ctx, "/dispatch", payload, 64<<10)
	return err
}

// chunkDispatchSpanOptions configure the span for one chunk dispatch: the root of the chunk
// turn's trace, or a child of the request that advanced the build. Only ids are recorded.
func chunkDispatchSpanOptions(buildID, chunkID string, activeIdx int) []trace.SpanStartOption {
	return []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("vanadis.build.id", buildID),
			attribute.String("vanadis.chunk.id", chunkID),
			attribute.Int("vanadis.chunk.index", activeIdx+1),
		),
	}
}

// markChunkDispatchSpan records the outcome without error text, which can echo eri responses.
func markChunkDispatchSpan(span trace.Span, err error) {
	if err != nil {
		span.SetStatus(codes.Error, "dispatch failed")
	}
}
