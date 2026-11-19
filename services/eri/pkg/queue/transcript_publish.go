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

const defaultStackName = "aerial"

var _ = func() bool {
	var p *WorkerPool
	return p != nil && p.EnqueueTranscriptPublish(TranscriptPublishJob{})
}()

type TranscriptPublishJob struct {
	SessionID, ThreadID, Engine, BrainDir string
	ExtraDirs                             []string
	LastMtime                             time.Time
	LastSize                              int64
	IsRecheck                             bool
	ParkCount                             int
}

func (p *WorkerPool) ensureTranscriptPublishWorker() {
	if p.ctx == nil {
		return
	}
	p.transcriptPublishOnce.Do(func() {
		if p.transcriptPublishCh == nil {
			p.transcriptPublishCh = make(chan TranscriptPublishJob, 256)
		}
		p.initTranscriptPublisher()
		p.wg.Go(p.runTranscriptPublishWorker)
	})
}

func (p *WorkerPool) initTranscriptPublisher() {
	if p.transcriptPublishCh == nil {
		p.transcriptPublishCh = make(chan TranscriptPublishJob, 256)
	}
	if p.TranscriptPublisher == nil {
		p.TranscriptPublisher = transcript.NewPublisher(p.stackName(), func(ctx context.Context, b transcriptwire.TranscriptStepBatch) error {
			if p.eventBus != nil {
				return p.eventBus.Publish(ctx, pubsub.SubjectTranscriptsSteps(b.SessionID), transcriptwire.MsgID(b), b)
			}
			return nil
		})
	}
	if p.TranscriptIndexerClient == nil && p.TranscriptsIndexerURL != "" {
		p.TranscriptIndexerClient = transcript.NewIndexerClient(p.TranscriptsIndexerURL, nil)
	}
}

func (p *WorkerPool) stackName() string {
	if p.appCfg != nil && p.appCfg.Current() != nil && p.appCfg.Current().Transcripts.Stack != "" {
		return strings.TrimSpace(p.appCfg.Current().Transcripts.Stack)
	}
	return defaultStackName
}

func (p *WorkerPool) EnqueueTranscriptPublish(job TranscriptPublishJob) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped || p.transcriptPublishCh == nil {
		return false
	}
	select {
	case p.transcriptPublishCh <- job:
		return true
	default:
		log.Printf("[WorkerPool] Warning: transcriptPublishCh full, dropping job for session %s", job.SessionID)
		return false
	}
}

func (p *WorkerPool) runTranscriptPublishWorker() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case job, ok := <-p.transcriptPublishCh:
			if !ok {
				return
			}
			if err := p.processTranscriptPublishJobWithContext(p.ctx, job); err != nil && p.ctx.Err() == nil {
				log.Printf("[WorkerPool] Transcript publish error for session %s: %v", job.SessionID, err)
			}
		}
	}
}

func (p *WorkerPool) drainTranscriptPublisher(ctx context.Context) error {
	for draining := p.transcriptPublishCh != nil; draining; {
		select {
		case job := <-p.transcriptPublishCh:
			if err := p.processTranscriptPublishJobWithContext(ctx, job); err != nil {
				log.Printf("[WorkerPool] Warning: drain transcript job error: %v", err)
			}
		default:
			draining = false
		}
	}
	if p.TranscriptPublisher != nil {
		return p.TranscriptPublisher.Drain(ctx)
	}
	return nil
}

func (p *WorkerPool) checkAllowlist(ctx context.Context, sessionID string) (bool, error) {
	if p.TranscriptStore == nil {
		return true, nil
	}
	known, err := p.TranscriptStore.GetKnownTurnSessionIDs(ctx)
	return known == nil || known[sessionID], err
}

func (p *WorkerPool) enqueueDelayed(job TranscriptPublishJob) {
	delay := 30 * time.Second
	if p.TranscriptRecheckDelay > 0 {
		delay = p.TranscriptRecheckDelay
	}
	time.AfterFunc(delay, func() {
		if p.ctx.Err() == nil {
			p.EnqueueTranscriptPublish(job)
		}
	})
}

func (p *WorkerPool) parkTranscriptJob(j TranscriptPublishJob) { j.ParkCount++; p.enqueueDelayed(j) }

func (p *WorkerPool) recheckSessionTranscript(j TranscriptPublishJob, m time.Time, s int64) {
	j.LastMtime, j.LastSize, j.IsRecheck = m, s, true
	p.enqueueDelayed(j)
}

func (p *WorkerPool) processTranscriptPublishJobWithContext(ctx context.Context, job TranscriptPublishJob) error {
	sessionID := strings.TrimSpace(job.SessionID)
	if sessionID == "" {
		return nil
	}
	if allowed, err := p.checkAllowlist(ctx, sessionID); err != nil {
		return fmt.Errorf("check transcript allowlist: %w", err)
	} else if !allowed {
		p.parkTranscriptJob(job)
		return nil
	}
	path := transcript.FindTranscriptFile(job.BrainDir, sessionID, job.ExtraDirs...)
	if path == "" {
		return nil
	}
	fi, err := os.Stat(path)
	if err != nil || (job.IsRecheck && fi.ModTime().UTC().Equal(job.LastMtime) && fi.Size() == job.LastSize) {
		return err
	}
	watermark := -1
	if p.TranscriptIndexerClient != nil {
		if wm, wmErr := p.TranscriptIndexerClient.Watermark(ctx, p.stackName(), sessionID); wmErr == nil {
			watermark = wm
		}
	}
	p.initTranscriptPublisher()
	batches, err := p.TranscriptPublisher.BuildSessionBatches(transcript.SessionBatchParams{
		SessionID: sessionID, BrainDir: job.BrainDir, Watermark: watermark,
		ThreadID: job.ThreadID, Engine: job.Engine, ExtraDirs: job.ExtraDirs, MCPServers: p.mcpServerNames(),
	})
	if err != nil {
		return fmt.Errorf("build session batches: %w", err)
	}
	p.TranscriptPublisher.Enqueue(batches...)
	p.recheckSessionTranscript(job, fi.ModTime().UTC(), fi.Size())
	return p.TranscriptPublisher.Drain(ctx)
}

func (p *WorkerPool) mcpServerNames() []string {
	if p.appCfg == nil || p.appCfg.Current() == nil {
		return nil
	}
	var names []string
	m := p.appCfg.Current().McpServers
	for _, g := range []map[string]json.RawMessage{m.Common, m.Discord, m.Voice, m.Ephemeral} {
		for n := range g {
			names = append(names, n)
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}
