package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/transcriptwire"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/transcript"
)

func (p *WorkerPool) transcriptsIndexerBaseURL() string {
	if p == nil {
		return ""
	}
	if p.transcriptsIndexerURL != "" {
		return p.transcriptsIndexerURL
	}
	if p.appCfg != nil {
		if cur := p.appCfg.Current(); cur != nil && cur.Transcripts.IndexerURL != "" {
			return cur.Transcripts.IndexerURL
		}
	}
	return ""
}

func (p *WorkerPool) stackName() string {
	if p == nil {
		return ""
	}
	if p.appCfg != nil {
		if cur := p.appCfg.Current(); cur != nil && cur.Transcripts.Stack != "" {
			return cur.Transcripts.Stack
		}
	}
	return ""
}

func (p *WorkerPool) resolvedTranscriptStore() db.TranscriptStore {
	if p == nil {
		return nil
	}
	if p.transcriptStore != nil {
		return p.transcriptStore
	}
	if ts, ok := p.cfg.Store.(db.TranscriptStore); ok {
		return ts
	}
	return nil
}

func (p *WorkerPool) transcriptRoots() (string, []string) {
	if p == nil || p.sessionMgr == nil {
		return "", nil
	}
	roots := p.sessionMgr.Roots()
	if len(roots) == 0 {
		return "", nil
	}
	return roots[0], roots[1:]
}

func (p *WorkerPool) ensureTranscriptPublisherLocked() *transcript.Publisher {
	if p.transcriptPublisher != nil {
		return p.transcriptPublisher
	}
	bus := p.EventBus()
	if bus == nil {
		return nil
	}
	indexerURL := p.transcriptsIndexerBaseURL()
	if indexerURL == "" {
		return nil
	}
	stack := p.stackName()
	publishFunc := func(callCtx context.Context, batch transcriptwire.TranscriptStepBatch) error {
		subj := pubsub.SubjectTranscriptsStepsPrefix + batch.SessionID
		msgID := transcriptwire.MsgID(batch)
		return bus.Publish(callCtx, subj, msgID, batch)
	}
	p.transcriptPublisher = transcript.NewPublisher(stack, publishFunc)
	return p.transcriptPublisher
}

func (p *WorkerPool) ensureTranscriptPublisher() *transcript.Publisher {
	p.transcriptWorkerMu.Lock()
	defer p.transcriptWorkerMu.Unlock()
	return p.ensureTranscriptPublisherLocked()
}

func (p *WorkerPool) ensureIndexerClientLocked() *transcript.IndexerClient {
	if p.indexerClient != nil {
		return p.indexerClient
	}
	url := p.transcriptsIndexerBaseURL()
	if url == "" {
		return nil
	}
	p.indexerClient = transcript.NewIndexerClient(url, nil)
	return p.indexerClient
}

func (p *WorkerPool) ensureIndexerClient() *transcript.IndexerClient {
	p.transcriptWorkerMu.Lock()
	defer p.transcriptWorkerMu.Unlock()
	return p.ensureIndexerClientLocked()
}

func (p *WorkerPool) ensureTranscriptPublishWorker() {
	if p == nil {
		return
	}
	p.mu.Lock()
	stopped := p.stopped
	p.mu.Unlock()
	if stopped {
		return
	}

	p.transcriptWorkerMu.Lock()
	defer p.transcriptWorkerMu.Unlock()
	if p.transcriptWorkerStarted {
		return
	}
	if p.EventBus() == nil || p.transcriptsIndexerBaseURL() == "" {
		return
	}
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.transcriptWorkerStarted = true
	p.wg.Go(func() {
		p.runTranscriptPublishWorker()
	})
	p.mu.Unlock()
}

// EnqueueTranscriptPublish queues a session for background transcript step publishing.
func (p *WorkerPool) EnqueueTranscriptPublish(sessionID, threadID, source string) {
	if p == nil {
		return
	}
	cleanSessionID := strings.TrimSpace(sessionID)
	if cleanSessionID == "" {
		return
	}
	if p.EventBus() == nil || p.transcriptsIndexerBaseURL() == "" {
		return
	}
	p.mu.Lock()
	stopped := p.stopped
	p.mu.Unlock()
	if stopped {
		return
	}
	p.ensureTranscriptPublishWorker()
	if p.transcriptPublishCh == nil {
		return
	}
	job := transcriptPublishJob{
		sessionID: cleanSessionID,
		threadID:  strings.TrimSpace(threadID),
		source:    strings.TrimSpace(source),
	}
	select {
	case p.transcriptPublishCh <- job:
	default:
		log.Printf("[WorkerPool] Warning: transcriptPublishCh buffer full, dropping publish job for session %s", cleanSessionID)
	}
}

func (p *WorkerPool) runTranscriptPublishWorker() {
	var doneCh <-chan struct{}
	if p.ctx != nil {
		doneCh = p.ctx.Done()
	}
	for {
		select {
		case <-doneCh:
			return
		case job, ok := <-p.transcriptPublishCh:
			if !ok {
				return
			}
			p.processTranscriptPublishJob(job)
		}
	}
}

func (p *WorkerPool) parkTranscriptJob(job transcriptPublishJob) {
	if p == nil {
		return
	}
	job.retried = true
	delay := p.transcriptRecheckDelay
	if delay <= 0 {
		delay = 30 * time.Second
	}
	time.AfterFunc(delay, func() {
		if p.ctx != nil && p.ctx.Err() != nil {
			return
		}
		if p.transcriptPublishCh == nil {
			return
		}
		if p.ctx != nil {
			select {
			case <-p.ctx.Done():
				return
			case p.transcriptPublishCh <- job:
			}
			return
		}
		select {
		case p.transcriptPublishCh <- job:
		default:
		}
	})
}

func (p *WorkerPool) mcpServerNames() []string {
	if p == nil || p.appCfg == nil {
		return nil
	}
	cur := p.appCfg.Current()
	if cur == nil {
		return nil
	}
	var names []string
	for _, group := range []map[string]json.RawMessage{cur.McpServers.Common, cur.McpServers.Discord, cur.McpServers.Voice, cur.McpServers.Ephemeral} {
		for name := range group {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

func (p *WorkerPool) checkAllowlist(ctx context.Context, job transcriptPublishJob) bool {
	ts := p.resolvedTranscriptStore()
	if ts == nil {
		return true
	}
	known, err := ts.GetKnownTurnSessionIDs(ctx)
	if err != nil {
		log.Printf("[WorkerPool] Warning: failed to query known turn session IDs: %v", err)
		return true
	}
	if known == nil || known[job.sessionID] {
		return true
	}
	if !job.retried && (p.ctx == nil || p.ctx.Err() == nil) {
		p.parkTranscriptJob(job)
	}
	return false
}

func (p *WorkerPool) fetchWatermark(ctx context.Context, sessionID string) int {
	indexer := p.ensureIndexerClient()
	if indexer == nil {
		return -1
	}
	wm, err := indexer.Watermark(ctx, p.stackName(), sessionID)
	if err != nil {
		log.Printf("[WorkerPool] Warning: failed to fetch watermark for %s: %v", sessionID, err)
		return -1
	}
	return wm
}

func (p *WorkerPool) processTranscriptPublishJob(job transcriptPublishJob) {
	p.processTranscriptPublishJobWithContext(p.ctx, job)
}

func (p *WorkerPool) processTranscriptPublishJobWithContext(ctx context.Context, job transcriptPublishJob) {
	if p == nil || (ctx != nil && ctx.Err() != nil) {
		return
	}
	// 1. Allowlist check via store.GetKnownTurnSessionIDs
	if !p.checkAllowlist(ctx, job) {
		return
	}

	// 2. Fetch watermark from IndexerClient
	wm := p.fetchWatermark(ctx, job.sessionID)

	// 3. Locate transcript file
	brainDir, extraDirs := p.transcriptRoots()
	transcriptPath := transcript.FindTranscriptFile(brainDir, job.sessionID, extraDirs...)
	if transcriptPath == "" {
		if !job.retried && (p.ctx == nil || p.ctx.Err() == nil) {
			p.parkTranscriptJob(job)
		}
		return
	}
	fi, err := os.Stat(transcriptPath)
	if err != nil {
		return
	}
	initialMtime := fi.ModTime().UTC()
	initialSize := fi.Size()

	// 4. Publish batches to pubsub.SubjectTranscriptsStepsPrefix + sessionID on AERIAL_TRANSCRIPTS
	pub := p.ensureTranscriptPublisher()
	if pub == nil {
		return
	}
	params := transcript.SessionBatchParams{
		SessionID:  job.sessionID,
		BrainDir:   brainDir,
		Watermark:  wm,
		ThreadID:   job.threadID,
		ExtraDirs:  extraDirs,
		MCPServers: p.mcpServerNames(),
	}
	batches, err := pub.BuildSessionBatches(params)
	if err != nil {
		log.Printf("[WorkerPool] Warning: failed to build session batches for %s: %v", job.sessionID, err)
		return
	}
	if len(batches) > 0 {
		pub.Enqueue(batches...)
		if drainErr := pub.Drain(ctx); drainErr != nil {
			log.Printf("[WorkerPool] Warning: failed to drain transcript batches for %s: %v", job.sessionID, drainErr)
		}
	}

	// 5. Re-check mtime/size once after 30s
	if job.source != "recheck" && (p.ctx == nil || p.ctx.Err() == nil) {
		p.recheckSessionTranscript(job.sessionID, job.threadID, transcriptPath, initialMtime, initialSize)
	}
}

func (p *WorkerPool) recheckSessionTranscript(sessionID, threadID, path string, initialMtime time.Time, initialSize int64) {
	delay := p.transcriptRecheckDelay
	if delay <= 0 {
		delay = 30 * time.Second
	}
	time.AfterFunc(delay, func() {
		if p.ctx != nil && p.ctx.Err() != nil {
			return
		}
		fi, err := os.Stat(path)
		if err != nil {
			return
		}
		if fi.ModTime().UTC().Equal(initialMtime) && fi.Size() == initialSize {
			return
		}
		p.EnqueueTranscriptPublish(sessionID, threadID, "recheck")
	})
}

func (p *WorkerPool) drainTranscriptPublisher(callCtx context.Context) {
	if p == nil {
		return
	}
	drainCtx, cancel := context.WithTimeout(context.WithoutCancel(callCtx), 5*time.Second)
	defer cancel()

	if p.transcriptPublishCh != nil {
		for {
			select {
			case job := <-p.transcriptPublishCh:
				p.processTranscriptPublishJobWithContext(drainCtx, job)
			default:
				goto drained
			}
		}
	}
drained:
	p.transcriptWorkerMu.Lock()
	pub := p.transcriptPublisher
	p.transcriptWorkerMu.Unlock()
	if pub == nil {
		return
	}
	if err := pub.Drain(drainCtx); err != nil {
		log.Printf("[WorkerPool] Warning draining transcript publisher on stop: %v", err)
	}
}

func collectStartupSessionIDs(brainDir string, extraDirs []string, known map[string]bool) []string {
	seenSessions := make(map[string]bool)
	var sessionIDs []string
	for _, dir := range append([]string{brainDir}, extraDirs...) {
		if dir == "" {
			continue
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
				continue
			}
			sid := entry.Name()
			if seenSessions[sid] || (known != nil && !known[sid]) {
				continue
			}
			seenSessions[sid] = true
			sessionIDs = append(sessionIDs, sid)
		}
	}
	return sessionIDs
}

// RunStartupResend runs the startup transcript resend pass against IndexerClient.Watermarks.
func (p *WorkerPool) RunStartupResend(ctx context.Context) error {
	if p == nil {
		return nil
	}
	indexer := p.ensureIndexerClient()
	pub := p.ensureTranscriptPublisher()
	if indexer == nil || pub == nil {
		return nil
	}
	watermarks, err := indexer.Watermarks(ctx, p.stackName())
	if err != nil {
		return fmt.Errorf("fetch startup watermarks: %w", err)
	}
	brainDir, extraDirs := p.transcriptRoots()
	if brainDir == "" {
		return nil
	}
	var known map[string]bool
	if ts := p.resolvedTranscriptStore(); ts != nil {
		var kErr error
		known, kErr = ts.GetKnownTurnSessionIDs(ctx)
		if kErr != nil {
			log.Printf("[WorkerPool] Warning: failed to query known turn session IDs on startup resend: %v", kErr)
		}
	}
	sessionIDs := collectStartupSessionIDs(brainDir, extraDirs, known)
	var allBatches []transcriptwire.TranscriptStepBatch
	for _, sid := range sessionIDs {
		wm := -1
		if w, ok := watermarks[sid]; ok {
			wm = w
		}
		params := transcript.SessionBatchParams{
			SessionID:  sid,
			BrainDir:   brainDir,
			Watermark:  wm,
			ExtraDirs:  extraDirs,
			MCPServers: p.mcpServerNames(),
		}
		batches, bErr := pub.BuildSessionBatches(params)
		if bErr != nil {
			log.Printf("[WorkerPool] Warning: failed to build startup resend batches for %s: %v", sid, bErr)
			continue
		}
		allBatches = append(allBatches, batches...)
	}
	if len(allBatches) > 0 {
		sent := transcript.ResendOnStartup(pub, watermarks, allBatches)
		log.Printf("[WorkerPool] Startup resend enqueued %d batches across %d sessions", sent, len(sessionIDs))
		return pub.Drain(ctx)
	}
	return nil
}
