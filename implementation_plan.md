# Implementation Plan - Streaming Sentence SSE Events for Voice Kiosk

## Overview
Enable real-time sentence streaming in Aerial Brain's `POST /voice/ask` and `POST /api/voice/ask` SSE endpoints to feed the Mirrormere Voice Hub with `event: sentence` payloads. This allows the local wall kiosk to synthesize audio via Kokoro sentence-by-sentence in real time with sub-second TTFB, rather than waiting for the entire turn to complete.

## Proposed Changes

### 1. `brain/pkg/queue/sentence_detector.go`
- Pure, deterministic `SentenceDetector` struct:
  - Accumulates incoming token deltas from `OnTextDelta`.
  - Scans for sentence boundaries: terminal punctuation (`.`, `!`, `?`), accounting for quotes/brackets (`."`, `!'`, `?)`) followed by whitespace.
  - Guards against false positives:
    - Decimal numbers (e.g. `3.14`, `1.5`)
    - Ellipses (`...`)
    - Abbreviations (`Mr.`, `Mrs.`, `Dr.`, `e.g.`, `i.e.`, `vs.`, `etc.`, `a.m.`, `p.m.`)
  - Methods:
    - `Feed(delta string, emit func(string))`
    - `Flush(emit func(string))`

### 2. `brain/pkg/queue/sentence_detector_test.go`
- Hermetic table-driven tests for `SentenceDetector`:
  - Standard sentence splits across multiple token chunks.
  - Multi-sentence deltas.
  - Abbreviations, decimals, and ellipses.
  - Incomplete sentences flushed at EOF/turn end.
  - Quotes and punctuation edge cases.

### 3. `brain/pkg/queue/pool.go`
- Update `voiceTurnSink`:
  - Store `onSentence func(sentence string)` and initialize `SentenceDetector`.
  - In `OnTextDelta(delta string)`: call `detector.Feed(delta, s.onSentence)`.
  - In `OnResult(res *runner.TurnResult)`: call `detector.Flush(s.onSentence)`.
- Update `ExecuteVoiceTurn`:
  - Accept optional variadic `onSentence ...func(sentence string)`.
  - Wire into `voiceTurnSink`.
  - Support `VoiceStreamRunnerFunc` in `WorkerPoolConfig` for mock injection in tests.

### 4. `brain/main.go`
- In `handleVoiceAsk`:
  - When `isSSE` is active:
    - Define `onSentence := func(sentence string) { emitSSE("sentence", map[string]string{"text": sentence}) }`.
    - Pass `onSentence` into `pool.ExecuteVoiceTurn(r.Context(), req.Prompt, sessionID, onStatus, onSentence)`.
    - Record TTFR metric on first `sentence` event (`event == "sentence"`).

### 5. `brain/main_test.go` & `brain/pkg/queue/sink_test.go`
- Verify `voiceTurnSink` feeds and flushes sentences.
- Verify `handleVoiceAsk` emits `event: sentence` chunks in order before `event: reply` and `event: done`.
- Verify TTFR metric records on sentence emission.

## Verification & Quality Gates
- `go test -v -race ./pkg/queue/...`
- `go test -v -race ./...` (inside `brain/`)
- `./scripts/verify.sh --staged`
- `./scripts/check-coverage.sh --service brain --check --gaps` (>= 95.0% floor)
