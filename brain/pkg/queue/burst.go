package queue

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/metrics"
)

func (p *WorkerPool) Enqueue(msg db.Message) {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		log.Printf("[WorkerPool] Warning: attempted to enqueue message %s to stopped pool", msg.ID)
		return
	}

	state, exists := p.threadChs[msg.ThreadID]
	if !exists {
		state = &threadWorkerState{ch: make(chan db.Message, 100)}
		p.threadChs[msg.ThreadID] = state
		p.wg.Add(1)
		go p.runThreadWorker(msg.ThreadID, state)
	}

	// Fast path: non-blocking send under lock
	select {
	case state.ch <- msg:
		metrics.QueueDepth.Inc()
		p.mu.Unlock()
		return
	default:
	}

	// Buffer full: track active enqueuer to block worker eviction while waiting outside lock
	state.activeEnqueuers++
	ch := state.ch
	p.mu.Unlock()

	select {
	case ch <- msg:
		metrics.QueueDepth.Inc()
	case <-p.ctx.Done():
		log.Printf("[WorkerPool] Context cancelled while enqueuing message %s", msg.ID)
	}

	p.mu.Lock()
	state.activeEnqueuers--
	p.mu.Unlock()
}

func (p *WorkerPool) runThreadWorker(threadID string, state *threadWorkerState) {
	defer p.wg.Done()

	idleTimeout := p.cfg.IdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = 30 * time.Second
	}
	idleTimer := time.NewTimer(idleTimeout)
	defer idleTimer.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case msg, ok := <-state.ch:
			if !ok {
				return
			}
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(idleTimeout)

			metrics.QueueDepth.Dec()
			burst := []db.Message{msg}
		DrainLoop:
			for len(burst) < 5 {
				select {
				case extra := <-state.ch:
					metrics.QueueDepth.Dec()
					burst = append(burst, extra)
				default:
					break DrainLoop
				}
			}

			p.mu.Lock()
			state.inFlight = true
			p.mu.Unlock()

			p.processBurst(burst)

			p.mu.Lock()
			state.inFlight = false
			p.mu.Unlock()

			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(idleTimeout)

		case <-idleTimer.C:
			p.mu.Lock()
			if len(state.ch) == 0 && state.activeEnqueuers == 0 {
				delete(p.threadChs, threadID)
				p.scopeLocks.Delete(threadID)
				p.mu.Unlock()
				return
			}
			p.mu.Unlock()
			idleTimer.Reset(idleTimeout)
		}
	}
}

// CoalesceBurstPrompt formats a burst of messages into a single coalesced multi-message prompt turn.
func CoalesceBurstPrompt(burst []db.Message) string {
	if len(burst) == 0 {
		return ""
	}
	if len(burst) == 1 {
		return burst[0].Content
	}

	var sb strings.Builder
	sb.WriteString("<USER_REQUEST>\n[Multiple messages received in channel]\n")
	for i, m := range burst {
		author := m.AuthorName
		if author == "" {
			author = "user"
		}
		if !strings.HasPrefix(author, "@") {
			author = "@" + author
		}
		timeStr := m.CreatedAt.Format("15:04:05")
		if m.CreatedAt.IsZero() {
			timeStr = time.Now().UTC().Format("15:04:05")
		}
		body := extractMessageBody(m.Content)
		sb.WriteString(fmt.Sprintf("--- Message %d (by %s at %s) ---\n%s\n\n", i+1, author, timeStr, body))
	}
	sb.WriteString("</USER_REQUEST>")
	return strings.TrimSpace(sb.String())
}
