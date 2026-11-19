package queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"

	"github.com/azylman/aerial/pkg/transcriptwire"
	"github.com/azylman/aerial/services/eri/pkg/transcript"
)

func scanDirForSessionIDs(dir string, known, seen map[string]bool) []string {
	entries, err := os.ReadDir(strings.TrimSpace(dir))
	if err != nil {
		return nil
	}
	var out []string
	for _, entry := range entries {
		if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		id := strings.TrimSpace(entry.Name())
		if id == "" || (known != nil && !known[id]) || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	return out
}

func unionKnownSessionIDs(known, seen map[string]bool) []string {
	if known == nil {
		return nil
	}
	var knownList []string
	for id, ok := range known {
		if ok {
			if clean := strings.TrimSpace(id); clean != "" {
				knownList = append(knownList, clean)
			}
		}
	}
	slices.Sort(knownList)
	var out []string
	for _, id := range knownList {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

// collectStartupSessionIDs gathers session IDs eligible for transcript resend.
// It enumerates directories across brainDir and extraDirs. When known is non-nil,
// any directory entry absent from known is excluded, and all session IDs from known
// (sorted via slices.Sort) are unioned so known sessions stored as plain
// brainDir/<session>.jsonl or Claude/Codex/Muse files are discovered even if they
// lack a brainDir/<session>/ directory.
func collectStartupSessionIDs(brainDir string, extraDirs []string, known map[string]bool) []string {
	seen := make(map[string]bool)
	sessionIDs := make([]string, 0, len(known))

	for _, d := range append([]string{brainDir}, extraDirs...) {
		sessionIDs = append(sessionIDs, scanDirForSessionIDs(d, known, seen)...)
	}
	sessionIDs = append(sessionIDs, unionKnownSessionIDs(known, seen)...)
	slices.Sort(sessionIDs)
	return sessionIDs
}

func (p *WorkerPool) resolveStartupSessionDirs() (string, []string) {
	if p.sessionMgr != nil && len(p.sessionMgr.Roots()) > 0 {
		roots := p.sessionMgr.Roots()
		return roots[0], roots[1:]
	}
	if p.appCfg != nil {
		return p.appCfg.GeminiHomeDir(), nil
	}
	return "", nil
}

func (p *WorkerPool) resolveStartupThreadID(ctx context.Context, sid string) string {
	if p.cfg.Store != nil {
		if tid, err := p.cfg.Store.ResolveThreadForSession(ctx, sid); err == nil && tid != "" {
			return tid
		}
	}
	if resolver, ok := p.TranscriptStore.(interface {
		ResolveThreadForSession(ctx context.Context, sessionID string) (string, error)
	}); ok {
		if tid, err := resolver.ResolveThreadForSession(ctx, sid); err == nil {
			return tid
		}
	}
	return ""
}

func (p *WorkerPool) resolveSessionBrainDir(sid, brainDir string, extraDirs []string) string {
	if p.sessionMgr != nil && len(p.sessionMgr.Roots()) > 0 {
		if d := p.resolvePublishBrainDir(sid, extraDirs...); d != "" {
			return d
		}
	}
	for _, d := range append([]string{brainDir}, extraDirs...) {
		if d = strings.TrimSpace(d); d != "" {
			if transcript.FindTranscriptFile(d, sid, extraDirs...) != "" {
				return d
			}
		}
	}
	return brainDir
}

func (p *WorkerPool) buildResendBatches(ctx context.Context, pub *transcript.Publisher, sid, brainDir string, extraDirs, mcpServers []string, watermarks map[string]int) ([]transcriptwire.TranscriptStepBatch, error) {
	targetBrainDir := p.resolveSessionBrainDir(sid, brainDir, extraDirs)
	threadID := p.resolveStartupThreadID(ctx, sid)

	wm := -1
	if watermarks != nil {
		if w, ok := watermarks[sid]; ok {
			wm = w
		}
	}

	batches, err := pub.BuildSessionBatches(transcript.SessionBatchParams{
		SessionID:  sid,
		BrainDir:   targetBrainDir,
		Watermark:  wm,
		ThreadID:   threadID,
		ExtraDirs:  extraDirs,
		MCPServers: mcpServers,
	})
	if err != nil {
		log.Printf("[StartupResend] Skipping session %s on batch build error: %v", sid, err)
		if os.IsNotExist(err) || strings.Contains(err.Error(), "not found") {
			return nil, nil
		}
		return nil, fmt.Errorf("session %s: %w", sid, err)
	}
	return batches, nil
}

// RunStartupResend discovers local session transcripts, queries indexer watermarks,
// filters out already-indexed steps via transcript.ResendOnStartup, and drains the publisher
// to re-publish missing steps on the event bus.
func (p *WorkerPool) RunStartupResend(ctx context.Context) error {
	pub, idxClient := p.initTranscriptPublisher()

	var watermarks map[string]int
	if idxClient != nil {
		var err error
		watermarks, err = idxClient.Watermarks(ctx, p.stackName())
		if err != nil {
			return fmt.Errorf("fetch indexer watermarks: %w", err)
		}
	}

	var known map[string]bool
	if p.TranscriptStore != nil {
		var err error
		known, err = p.TranscriptStore.GetKnownTurnSessionIDs(ctx)
		if err != nil {
			return fmt.Errorf("get known session IDs: %w", err)
		}
	}

	brainDir, extraDirs := p.resolveStartupSessionDirs()
	sessionIDs := collectStartupSessionIDs(brainDir, extraDirs, known)

	mcpServers := p.mcpServerNames()
	var allBatches []transcriptwire.TranscriptStepBatch
	var scanErrs []error
	for _, sid := range sessionIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if cleanID := strings.TrimSpace(sid); cleanID != "" {
			batches, err := p.buildResendBatches(ctx, pub, cleanID, brainDir, extraDirs, mcpServers, watermarks)
			if err != nil {
				scanErrs = append(scanErrs, err)
			} else if len(batches) > 0 {
				allBatches = append(allBatches, batches...)
			}
		}
	}

	if p.transcriptPublishSem != nil {
		select {
		case p.transcriptPublishSem <- struct{}{}:
			defer func() { <-p.transcriptPublishSem }()
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	transcript.ResendOnStartup(pub, watermarks, allBatches)
	drainErr := pub.Drain(ctx)
	if len(scanErrs) > 0 {
		if drainErr != nil {
			return errors.Join(append(scanErrs, drainErr)...)
		}
		return errors.Join(scanErrs...)
	}
	return drainErr
}
