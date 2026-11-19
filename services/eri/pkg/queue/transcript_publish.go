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
	"github.com/azylman/aerial/services/eri/pkg/transcript"
)

const defaultStackName, maxTranscriptParkAttempts = "aerial", 5

type TranscriptPublishJob struct {
	SessionID, ThreadID, Engine, BrainDir string
	ExtraDirs                             []string
	LastMtime                             time.Time
	LastSize                              int64
	IsRecheck                             bool
	ParkCount                             int
}

func (p *WorkerPool) initTranscriptPublisher() {
	if p.TranscriptPublisher == nil {
		p.TranscriptPublisher = transcript.NewPublisher(p.stackName(), func(ctx context.Context, b transcriptwire.TranscriptStepBatch) error {
			if bus := p.EventBus(); bus != nil {
				return bus.Publish(ctx, pubsub.SubjectTranscriptsSteps(b.SessionID), transcriptwire.MsgID(b), b)
			}
			return fmt.Errorf("transcript publisher: event bus not configured")
		})
	}
	if p.TranscriptIndexerClient == nil && p.TranscriptsIndexerURL != "" {
		p.TranscriptIndexerClient = transcript.NewIndexerClient(p.TranscriptsIndexerURL, nil)
	}
}

func (p *WorkerPool) stackName() string {
	if p != nil && p.appCfg != nil && p.appCfg.Current() != nil && p.appCfg.Current().Transcripts.Stack != "" {
		return strings.TrimSpace(p.appCfg.Current().Transcripts.Stack)
	}
	return defaultStackName
}

func (p *WorkerPool) EnqueueTranscriptPublish(job TranscriptPublishJob) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	sID := strings.TrimSpace(job.SessionID)
	if p.stopped || p.transcriptPublishCh == nil || sID == "" {
		return false
	}
	job.SessionID = sID
	if !job.IsRecheck && job.ParkCount == 0 && p.transcriptPending != nil {
		if p.transcriptPending[sID] {
			return true
		}
		p.transcriptPending[sID] = true
	}
	select {
	case p.transcriptPublishCh <- job:
		return true
	default:
		log.Printf("[WorkerPool] Warning: transcriptPublishCh full, dropping job for session %s", sID)
		if !job.IsRecheck && job.ParkCount == 0 {
			delete(p.transcriptPending, sID)
		} else {
			p.enqueueDelayed(job)
		}
		return false
	}
}

func (p *WorkerPool) publishJobLocked(ctx context.Context, job TranscriptPublishJob, tag string) {
	select {
	case p.transcriptPublishSem <- struct{}{}:
		defer func() { <-p.transcriptPublishSem }()
	case <-ctx.Done():
		return
	}
	if err := p.processTranscriptPublishJobWithContext(ctx, job); err != nil && ctx.Err() == nil {
		log.Printf("[WorkerPool] %s publish error for session %s: %v", tag, job.SessionID, err)
	}
}

func (p *WorkerPool) setInFlight(job *TranscriptPublishJob) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if job == nil {
		p.transcriptInFlight = nil
	} else {
		cp := *job
		p.transcriptInFlight = &cp
	}
}

func (p *WorkerPool) takeInFlight() (j *TranscriptPublishJob) {
	p.mu.Lock()
	defer p.mu.Unlock()
	j, p.transcriptInFlight = p.transcriptInFlight, nil
	return j
}

func (p *WorkerPool) runTranscriptPublishWorker() {
	for p.ctx.Err() == nil {
		select {
		case <-p.ctx.Done():
			return
		case job, ok := <-p.transcriptPublishCh:
			if !ok {
				return
			}
			p.setInFlight(&job)
			jobCtx, cancel := context.WithTimeout(p.ctx, 60*time.Second)
			p.publishJobLocked(jobCtx, job, "Transcript")
			cancel()
			if p.ctx.Err() == nil {
				p.setInFlight(nil)
			}
		}
	}
}

func (p *WorkerPool) drainTranscriptPublisher(ctx context.Context) error {
	select {
	case p.transcriptPublishSem <- struct{}{}:
		defer func() { <-p.transcriptPublishSem }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if inFlight := p.takeInFlight(); inFlight != nil {
		if err := p.processTranscriptPublishJobWithContext(ctx, *inFlight); err != nil && ctx.Err() == nil {
			log.Printf("[WorkerPool] Drain publish error for session %s: %v", inFlight.SessionID, err)
		}
	}
	for ctx.Err() == nil {
		select {
		case job := <-p.transcriptPublishCh:
			if err := p.processTranscriptPublishJobWithContext(ctx, job); err != nil && ctx.Err() == nil {
				log.Printf("[WorkerPool] Drain publish error for session %s: %v", job.SessionID, err)
			}
		default:
			if p.TranscriptPublisher != nil {
				return p.TranscriptPublisher.Drain(ctx)
			}
			return nil
		}
	}
	return ctx.Err()
}

func (p *WorkerPool) setPending(id string, pending bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.transcriptPending != nil {
		if s := strings.TrimSpace(id); pending {
			p.transcriptPending[s] = true
		} else {
			delete(p.transcriptPending, s)
		}
	}
}

func (p *WorkerPool) retryOrDrop(j TranscriptPublishJob, reason string, err error) error {
	if p != nil && p.ctx.Err() != nil {
		return err
	}
	if j.ParkCount >= maxTranscriptParkAttempts {
		log.Printf("[WorkerPool] Dropping transcript publish job for session %s after %d attempts (%s): %v", j.SessionID, j.ParkCount, reason, err)
		p.setPending(j.SessionID, false)
		return err
	}
	j.ParkCount++
	p.setPending(j.SessionID, true)
	p.enqueueDelayed(j)
	return err
}

func (p *WorkerPool) enqueueDelayed(j TranscriptPublishJob) {
	d := 30 * time.Second
	if p != nil && p.TranscriptRecheckDelay > 0 {
		d = p.TranscriptRecheckDelay
	}
	time.AfterFunc(d, func() {
		if p != nil && p.ctx.Err() == nil {
			p.EnqueueTranscriptPublish(j)
		}
	})
}

func (p *WorkerPool) processTranscriptPublishJobWithContext(ctx context.Context, job TranscriptPublishJob) error {
	sID := strings.TrimSpace(job.SessionID)
	if sID == "" {
		return nil
	}
	p.setPending(sID, false)
	if p.TranscriptStore != nil {
		known, err := p.TranscriptStore.GetKnownTurnSessionIDs(ctx)
		if err != nil || (known != nil && !known[sID]) {
			return p.retryOrDrop(job, "allowlist", err)
		}
	}
	if job.BrainDir == "" && p.appCfg != nil {
		job.BrainDir = p.appCfg.GeminiHomeDir()
	}
	path := transcript.FindTranscriptFile(job.BrainDir, sID, job.ExtraDirs...)
	if path == "" {
		return p.retryOrDrop(job, "transcript file not found", nil)
	}
	fi, err := os.Stat(path)
	if err != nil {
		return p.retryOrDrop(job, "stat transcript file", err)
	}
	if job.IsRecheck && fi.ModTime().UTC().Equal(job.LastMtime) && fi.Size() == job.LastSize {
		p.setPending(sID, false)
		return nil
	}
	wm := -1
	if p.TranscriptIndexerClient != nil {
		if w, wErr := p.TranscriptIndexerClient.Watermark(ctx, p.stackName(), sID); wErr == nil {
			wm = w
		}
	}
	p.initTranscriptPublisher()
	batches, err := p.TranscriptPublisher.BuildSessionBatches(transcript.SessionBatchParams{SessionID: sID, BrainDir: job.BrainDir, Watermark: wm, ThreadID: job.ThreadID, Engine: job.Engine, ExtraDirs: job.ExtraDirs, MCPServers: p.mcpServerNames()})
	if err != nil {
		return p.retryOrDrop(job, "build batches", fmt.Errorf("build session batches: %w", err))
	}
	p.TranscriptPublisher.Enqueue(batches...)
	if err := p.TranscriptPublisher.Drain(ctx); err != nil {
		return p.retryOrDrop(job, "drain failure", err)
	}
	job.LastMtime, job.LastSize, job.IsRecheck, job.ParkCount = fi.ModTime().UTC(), fi.Size(), true, 0
	p.setPending(sID, true)
	p.enqueueDelayed(job)
	return nil
}

func (p *WorkerPool) mcpServerNames() (names []string) {
	if p != nil && p.appCfg != nil && p.appCfg.Current() != nil {
		for _, g := range []map[string]json.RawMessage{p.appCfg.Current().McpServers.Common, p.appCfg.Current().McpServers.Discord, p.appCfg.Current().McpServers.Voice, p.appCfg.Current().McpServers.Ephemeral} {
			for n := range g {
				names = append(names, n)
			}
		}
		slices.Sort(names)
		return slices.Compact(names)
	}
	return nil
}
