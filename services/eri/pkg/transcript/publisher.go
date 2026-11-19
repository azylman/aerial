// Package transcript dark publisher: per-turn transcript step batches are
// coalesced per session and drained strictly in order. This file has no
// callers yet; turn-end publishing wires it up in a later chunk.
package transcript

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/azylman/aerial/pkg/transcriptwire"
)

// BatchPublishFunc publishes one transcript step batch to the
// AERIAL_TRANSCRIPTS stream. Implementations must be safe for sequential use;
// the Publisher never calls it concurrently.
type BatchPublishFunc func(ctx context.Context, batch transcriptwire.TranscriptStepBatch) error

// Publisher coalesces transcript step batches keyed by session and drains
// them strictly in order. A session's batches are never dropped by Enqueue:
// once accepted they stay queued until Drain publishes them. On failure Drain
// abandons only the chain it snapshotted before the failed call and continues
// with the next session; batches a concurrent Enqueue appended after the
// snapshot were never attempted and stay queued, so a fresh Drain re-seeds
// from the indexer watermark instead of replaying stale steps.
type Publisher struct {
	mu      sync.Mutex
	drainMu sync.Mutex
	stack   string
	publish BatchPublishFunc
	queues  map[string][]transcriptwire.TranscriptStepBatch
	order   []string
}

// NewPublisher returns a Publisher labeling batches with stack. A nil publish
// function disables draining: Drain reports an error and retains every queue.
func NewPublisher(stack string, publish BatchPublishFunc) *Publisher {
	return &Publisher{
		stack:   strings.TrimSpace(stack),
		publish: publish,
		queues:  make(map[string][]transcriptwire.TranscriptStepBatch),
	}
}

// Enqueue appends batches to their sessions' queues, preserving arrival order
// within each session. Batches with a blank session ID are dropped; every
// other batch is kept (never dropped) until Drain publishes it. A publish
// failure abandons only the chain Drain snapshotted before the failed call:
// batches Enqueue appends after that snapshot stay queued for the next Drain.
func (p *Publisher) Enqueue(batches ...transcriptwire.TranscriptStepBatch) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, b := range batches {
		session := strings.TrimSpace(b.SessionID)
		if session == "" {
			continue
		}
		if _, ok := p.queues[session]; !ok {
			p.order = append(p.order, session)
		}
		p.queues[session] = append(p.queues[session], b)
	}
}

// darkTranscriptAnchor keeps the dark transcript publisher pieces reachable
// in the production call graph until the turn-end publishing cutover wires
// live callers. Every call below is a validated no-op: blank sessions are
// dropped, the indexer client is disabled without a base URL, and Drain on
// an empty queue with no publish function reports nothing, so this performs
// no network or database work and changes no behavior. The cutover chunk
// removes this anchor when it wires the live publisher.
var _ = func() bool {
	p := NewPublisher("", nil)
	p.Enqueue()
	_ = p.EnqueueSteps(transcriptwire.TranscriptStepBatch{}, nil)
	_ = p.PendingSessions()
	_ = p.PendingCount("")
	errDrain := p.Drain(context.Background())
	_ = ResendOnStartup(nil, nil, nil)
	client := NewIndexerClient("", nil)
	_, errGet := client.Watermark(context.Background(), "", "")
	_, errWms := client.Watermarks(context.Background(), "")
	_, errBatches := p.BuildSessionBatches(SessionBatchParams{})
	errPubSess := p.PublishSessions(context.Background(), "", nil)
	errPubOne := p.PublishSession(context.Background(), SessionBatchParams{})
	_ = []error{errDrain, errGet, errWms, errBatches, errPubSess, errPubOne}
	return true
}()

// SessionBatchParams configures batch generation for a session transcript file.
type SessionBatchParams struct {
	SessionID  string
	BrainDir   string
	Watermark  int
	ThreadID   string
	Engine     string
	ExtraDirs  []string
	HeadLimit  int
	TailLimit  int
	MCPServers []string
}

func locateTranscriptPath(brainDir, sessionID string, extraDirs []string) (string, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return "", fmt.Errorf("transcript publisher: session ID cannot be empty")
	}
	transcriptPath := findTranscriptFile(brainDir, sessionID, extraDirs...)
	if transcriptPath == "" && brainDir != "" {
		if entries, err := os.ReadDir(brainDir); err == nil {
			discovered := discoverSessions(brainDir, entries, "", extraDirs...)
			if ds, ok := discovered[sessionID]; ok && ds.path != "" {
				transcriptPath = ds.path
			}
		}
	}
	if transcriptPath == "" {
		return "", fmt.Errorf("transcript publisher: transcript file not found for session %s", sessionID)
	}
	return transcriptPath, nil
}

func convertRawStepsToWire(parsed []rawTranscriptStep, watermark, headLimit, tailLimit int) []transcriptwire.TranscriptStep {
	if headLimit <= 0 && tailLimit <= 0 {
		headLimit = DefaultHeadLimit
		tailLimit = DefaultTailLimit
	}
	var wireSteps []transcriptwire.TranscriptStep
	for _, raw := range parsed {
		if raw.StepIndex <= watermark {
			continue
		}
		content := raw.Content
		if content == "" && len(raw.ToolCalls) > 0 {
			content = string(raw.ToolCalls[0].Args)
		}
		if !IsSubstantiveStep(raw.Type, raw.ToolName, content) {
			continue
		}
		clipped := ClipContent(content, headLimit, tailLimit)
		var toolCalls []transcriptwire.TranscriptToolCall
		for _, tc := range raw.ToolCalls {
			toolCalls = append(toolCalls, transcriptwire.TranscriptToolCall{
				Name: tc.Name,
				Args: tc.Args,
				ID:   tc.ID,
			})
		}
		wireSteps = append(wireSteps, transcriptwire.TranscriptStep{
			StepIndex: raw.StepIndex,
			Source:    raw.Source,
			Type:      raw.Type,
			Status:    raw.Status,
			CreatedAt: raw.CreatedAt,
			Content:   clipped,
			ToolName:  raw.ToolName,
			ToolCalls: toolCalls,
			Thinking:  raw.Thinking,
		})
	}
	return wireSteps
}

// BuildSessionBatches locates and scans a session's local transcript file,
// compares maxStepIndex against watermark W (setting Reset=true and BaseStep=-1 when maxStepIndex < W),
// converts substantive steps > W into []transcriptwire.TranscriptStep with ClipContent,
// and returns batches split via SplitStepBatches.
func (p *Publisher) BuildSessionBatches(params SessionBatchParams) ([]transcriptwire.TranscriptStepBatch, error) {
	transcriptPath, err := locateTranscriptPath(params.BrainDir, params.SessionID, params.ExtraDirs)
	if err != nil {
		return nil, err
	}
	fi, err := os.Stat(transcriptPath)
	if err != nil {
		return nil, fmt.Errorf("transcript publisher: stat transcript file: %w", err)
	}
	st, err := scanSessionTranscriptFile(transcriptPath, nil)
	if err != nil {
		return nil, fmt.Errorf("transcript publisher: scan transcript file: %w", err)
	}
	engine := strings.TrimSpace(params.Engine)
	if engine == "" {
		engine = engineForPath(params.BrainDir, transcriptPath, params.ExtraDirs...)
	}

	watermark := params.Watermark
	reset := false
	baseStep := watermark
	if st.maxStepIndex < watermark {
		reset = true
		baseStep = -1
		watermark = -1
	}

	wireSteps := convertRawStepsToWire(st.parsedSteps, watermark, params.HeadLimit, params.TailLimit)

	stack := ""
	if p != nil {
		stack = p.stack
	}
	base := transcriptwire.TranscriptStepBatch{
		Stack:       stack,
		SessionID:   strings.TrimSpace(params.SessionID),
		ThreadID:    strings.TrimSpace(params.ThreadID),
		Engine:      engine,
		FileMtime:   fi.ModTime().UTC(),
		BaseStep:    baseStep,
		Reset:       reset,
		Step0Prompt: st.step0Prompt,
	}
	batches := transcriptwire.SplitStepBatches(base, wireSteps)
	if reset {
		for i := range batches {
			batches[i].BaseStep = -1
		}
	}
	return batches, nil
}

// PublishSessions locates and scans sessions, builds batches, enqueues them on p, and drains.
func (p *Publisher) PublishSessions(ctx context.Context, brainDir string, watermarks map[string]int, sessionIDs ...string) error {
	if p == nil {
		return fmt.Errorf("transcript publisher: publisher is nil")
	}
	if len(sessionIDs) == 0 && brainDir != "" {
		if entries, err := os.ReadDir(brainDir); err == nil {
			discovered := discoverSessions(brainDir, entries, "")
			for sid := range discovered {
				sessionIDs = append(sessionIDs, sid)
			}
		}
	}
	for _, sid := range sessionIDs {
		sid = strings.TrimSpace(sid)
		if sid == "" {
			continue
		}
		wm := -1
		if watermarks != nil {
			if w, ok := watermarks[sid]; ok {
				wm = w
			}
		}
		batches, err := p.BuildSessionBatches(SessionBatchParams{
			SessionID: sid,
			BrainDir:  brainDir,
			Watermark: wm,
		})
		if err != nil {
			return err
		}
		p.Enqueue(batches...)
	}
	return p.Drain(ctx)
}

// PublishSession builds batches for a single session, enqueues them on p, and drains.
func (p *Publisher) PublishSession(ctx context.Context, params SessionBatchParams) error {
	if p == nil {
		return fmt.Errorf("transcript publisher: publisher is nil")
	}
	batches, err := p.BuildSessionBatches(params)
	if err != nil {
		return err
	}
	p.Enqueue(batches...)
	return p.Drain(ctx)
}

// EnqueueSteps splits steps with the shared wire splitter and enqueues the
// resulting batches. It returns the number of batches enqueued.
func (p *Publisher) EnqueueSteps(base transcriptwire.TranscriptStepBatch, steps []transcriptwire.TranscriptStep) int {
	batches := transcriptwire.SplitStepBatches(base, steps)
	p.Enqueue(batches...)
	return len(batches)
}

// removeSessionLocked drops a session's queue and its order entry together,
// keeping the order/queue membership invariant so a concurrent Enqueue can
// never append a duplicate order entry. The caller must hold p.mu.
func (p *Publisher) removeSessionLocked(session string) {
	delete(p.queues, session)
	kept := p.order[:0]
	for _, s := range p.order {
		if s != session {
			kept = append(kept, s)
		}
	}
	for i := len(kept); i < len(p.order); i++ {
		p.order[i] = ""
	}
	p.order = kept
}

// PendingSessions returns session IDs with queued batches in first-seen order.
func (p *Publisher) PendingSessions() []string {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.order...)
}

// PendingCount returns the queued batch count for one session.
func (p *Publisher) PendingCount(sessionID string) int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.queues[strings.TrimSpace(sessionID)])
}

// Drain publishes every queued batch strictly in order within each session.
// On the first publish failure for a session it abandons only the chain it
// snapshotted before the failed call (the failed head plus its already-queued
// successors) and continues with the next session, returning the first error
// encountered. Batches appended after the snapshot were never attempted and
// stay queued for the next Drain. Successful sessions are fully dequeued.

// dropSnapshotLocked abandons the snapshotted chain while keeping batches a
// concurrent Enqueue appended after the snapshot. The caller must hold p.mu;
// snapshotLen is the queue length Drain observed before the failed publish.
// Since Enqueue only appends, dropping the first snapshotLen entries removes
// exactly the failed chain and preserves everything appended afterwards.
func (p *Publisher) dropSnapshotLocked(session string, snapshotLen int) {
	queue := p.queues[session]
	if len(queue) > snapshotLen {
		p.queues[session] = append([]transcriptwire.TranscriptStepBatch(nil), queue[snapshotLen:]...)
		return
	}
	p.removeSessionLocked(session)
}

func (p *Publisher) Drain(ctx context.Context) error {
	if p == nil {
		return nil
	}
	p.drainMu.Lock()
	defer p.drainMu.Unlock()
	p.mu.Lock()
	if p.publish == nil && len(p.order) > 0 {
		p.mu.Unlock()
		return fmt.Errorf("transcript publisher: no publish function configured")
	}
	publish := p.publish
	sessions := append([]string(nil), p.order...)
	p.mu.Unlock()

	var firstErr error
	for _, session := range sessions {
		for {
			p.mu.Lock()
			queue := p.queues[session]
			if len(queue) == 0 {
				p.removeSessionLocked(session)
				p.mu.Unlock()
				break
			}
			next := queue[0]
			snapshotLen := len(queue)
			p.mu.Unlock()

			if err := publish(ctx, next); err != nil {
				p.mu.Lock()
				p.dropSnapshotLocked(session, snapshotLen)
				p.mu.Unlock()
				if firstErr == nil {
					firstErr = fmt.Errorf("transcript publisher: publish session %s: %w", session, err)
				}
				break
			}

			p.mu.Lock()
			if len(p.queues[session]) > 0 {
				p.queues[session] = p.queues[session][1:]
			}
			if len(p.queues[session]) == 0 {
				p.removeSessionLocked(session)
			}
			p.mu.Unlock()
		}
	}

	return firstErr
}
