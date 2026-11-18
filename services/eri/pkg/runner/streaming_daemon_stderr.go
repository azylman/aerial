package runner

import (
	"bytes"
	"context"
	"io"
	"log"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// maxStderrLineBytes caps accumulated agent stderr before forced line emission to prevent pipe stalls.
const maxStderrLineBytes = 64 << 10

func (d *StreamingDaemon) activeTurn() *TurnContext {
	if d == nil {
		return nil
	}
	d.inflightMu.Lock()
	defer d.inflightMu.Unlock()
	if len(d.inflight) > 0 {
		return d.inflight[0]
	}
	return nil
}

func (d *StreamingDaemon) readStderrLoop(stderr io.ReadCloser) {
	defer d.stderrWg.Done()
	defer closeStreamQuietly(stderr, "stderr")

	buf := make([]byte, 4096)
	var lineBuf []byte

	for {
		n, err := stderr.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			if d.stderrTail != nil {
				if _, writeErr := d.stderrTail.Write(chunk); writeErr != nil {
					log.Printf("[StreamingDaemon] Warning: failed writing to stderrTail: %v", writeErr)
				}
			}
			for len(chunk) > 0 {
				idx := bytes.IndexByte(chunk, '\n')
				remainingCap := maxStderrLineBytes - len(lineBuf)
				switch {
				case idx >= 0 && idx < remainingCap:
					lineBuf = append(lineBuf, chunk[:idx]...)
					chunk = chunk[idx+1:]
					d.emitStderrLine(string(lineBuf))
					lineBuf = lineBuf[:0]
				case len(chunk) >= remainingCap:
					lineBuf = append(lineBuf, chunk[:remainingCap]...)
					chunk = chunk[remainingCap:]
					d.emitStderrLine(string(lineBuf))
					lineBuf = lineBuf[:0]
				default:
					lineBuf = append(lineBuf, chunk...)
					chunk = nil
				}
			}
		}
		if err != nil {
			if len(lineBuf) > 0 {
				d.emitStderrLine(string(lineBuf))
			}
			break
		}
	}
}

func (d *StreamingDaemon) emitStderrLine(rawLine string) {
	level, msg, sourceFile, keep := ParseAndFilterStderrLine(d.cfg.engineName(), rawLine)
	if !keep {
		return
	}

	var sc trace.SpanContext
	sessionID := d.SessionID()

	if active := d.activeTurn(); active != nil {
		if active.Ctx != nil {
			sc = trace.SpanContextFromContext(active.Ctx)
		}
		if !sc.IsValid() {
			sc = d.spawnSpanCtx
		}
		if active.SessionID != "" {
			sessionID = active.SessionID
		}
	} else {
		sc = d.spawnSpanCtx
	}

	ctx := context.Background()
	if sc.IsValid() {
		ctx = trace.ContextWithSpanContext(ctx, sc)
	}

	logger := d.cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	attrs := []slog.Attr{
		slog.String("source", "agent_stderr"),
		slog.String("engine", d.cfg.engineName()),
	}
	if d.cfg.Stack != "" {
		attrs = append(attrs, slog.String("stack", d.cfg.Stack))
	}
	if sessionID != "" {
		attrs = append(attrs, slog.String("session_id", sessionID))
	}
	if d.cfg.ThreadID != "" {
		attrs = append(attrs, slog.String("target", d.cfg.ThreadID))
	}
	if sourceFile != "" {
		attrs = append(attrs, slog.String("source_file", sourceFile))
	}

	logger.LogAttrs(ctx, level, msg, attrs...)
}
