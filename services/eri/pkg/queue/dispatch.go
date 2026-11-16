package queue

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

func (p *WorkerPool) processDispatch(d db.Dispatch, scopeKey string) {
	if p.ctx.Err() != nil {
		return
	}

	metrics.ActiveWorkers.Inc()
	defer metrics.ActiveWorkers.Dec()

	lockVal, _ := p.scopeLocks.LoadOrStore(scopeKey, &sync.Mutex{})
	scopeLock, ok := lockVal.(*sync.Mutex)
	if !ok {
		scopeLock = &sync.Mutex{}
		p.scopeLocks.Store(scopeKey, scopeLock)
	}
	scopeLock.Lock()
	defer scopeLock.Unlock()

	if p.cfg.Store == nil {
		log.Printf("[WorkerPool] Store is nil, dropping dispatch %s", d.ID)
		return
	}
	claimed, claimErr := p.cfg.Store.ClaimPendingDispatch(p.ctx, d.ID)
	if claimErr != nil || !claimed {
		if claimErr != nil {
			log.Printf("[WorkerPool] Failed to claim dispatch %s: %v", d.ID, claimErr)
		}
		return
	}

	isEphem := strings.TrimSpace(d.SessionID) == ""
	leaseKey := d.SessionID
	if isEphem {
		leaseKey = "dispatch-" + d.ID
	}

	execStart := time.Now().UTC()
	triggerType := "dispatch:" + d.Source
	spanCtx, turnSpan := p.beginTurnTrace([]queuedTurnItem{dispatchTraceItem(d, p.startedAt)}, leaseKey, triggerType, execStart)
	turnCtx, turnCancel := context.WithCancel(spanCtx)
	defer turnCancel()

	te := &turnExecution{
		pool:        p,
		dispatch:    &d,
		threadID:    leaseKey,
		triggerType: triggerType,
		execStart:   execStart,
		wakeIdx:     -1,
		isEphemeral: isEphem,
		stopTyping:  func() {},
		turnCtx:     turnCtx,
		turnCancel:  turnCancel,
		turnSpan:    turnSpan,
	}

	p.registerActiveTurn(scopeKey, te)
	defer p.unregisterActiveTurn(scopeKey, te)
	defer te.endTurnSpan()
	te.beginSetupSpan()
	defer te.endSetupSpan()

	cleanupDispatch := p.setupDispatchTurn(d, te)
	defer cleanupDispatch()

	defer func() {
		if r := recover(); r != nil {
			te.handleBurstPanic(r)
		}
	}()

	if !te.acquirePoolAndLease() {
		return
	}
	defer te.releaseActivePoolLease()
	markTurnInactive := te.markTurnActive()
	defer markTurnInactive()

	te.turnAttempted = true
	promptCtx, promptSpan := te.startSetupChildSpan(te.turnCtxValue(), spanPrompt)
	te.turnPrompt = te.buildDispatchPrompt(promptCtx)
	promptSpan.End()
	te.executeWithRetries()
}

var (
	standalonePRRemediationRegex = regexp.MustCompile(`PR #(\d+)\s+on\s+([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)|(?:on|for repo)\s+([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)\s+\((?:commit: [^,]+,\s*)?PR #(\d+)`)
	vanadisPromptBuildIDRegex    = regexp.MustCompile(`(?m)^Build ID:\s*(build-[A-Za-z0-9_-]+)\s*$`)
)

func extractPRRemediationTarget(prompt string) (repo string, prNum int, ok bool) {
	m := standalonePRRemediationRegex.FindStringSubmatch(prompt)
	if len(m) < 5 {
		return "", 0, false
	}
	prNumStr, repo := m[1], m[2]
	if prNumStr == "" {
		repo, prNumStr = m[3], m[4]
	}
	prNum, err := strconv.Atoi(prNumStr)
	return repo, prNum, err == nil && prNum > 0
}

func extractVanadisBuildID(chunkID, sessionID, targetID, prompt string) string {
	if strings.TrimSpace(prompt) != "" {
		if m := vanadisPromptBuildIDRegex.FindStringSubmatch(prompt); len(m) >= 2 {
			return strings.TrimSpace(m[1])
		}
	}
	if strings.HasPrefix(chunkID, "build-") {
		if idx := strings.LastIndex(chunkID, "-chunk-"); idx > 0 {
			return chunkID[:idx]
		}
	}
	if after, ok := strings.CutPrefix(sessionID, "vanadis-"); ok {
		if idx := strings.LastIndex(after, "-chunk-"); idx > 0 {
			return after[:idx]
		}
	}
	if t := runner.NormalizeEnvTargetID(targetID); strings.HasPrefix(t, "build-") {
		if idx := strings.LastIndex(t, "-chunk-"); idx > 0 {
			return t[:idx]
		}
		return t
	}
	return ""
}

func extractVanadisDispatchInfo(d *db.Dispatch) (chunkID, buildID string, chunkIdx int) {
	if d == nil {
		return "", "", -1
	}
	chunkIdx = -1
	if chunk := strings.TrimSpace(d.VanadisChunkID); chunk != "" {
		chunkID = chunk
	}
	if chunkID == "" {
		if t := runner.NormalizeEnvTargetID(d.TargetID); strings.HasPrefix(t, "build-") || strings.HasPrefix(t, "pr-") {
			chunkID = t
		}
	}
	if chunkID == "" {
		if after, ok := strings.CutPrefix(d.SessionID, "vanadis-"); ok {
			chunkID = strings.TrimSpace(after)
		}
	}
	buildID = extractVanadisBuildID(chunkID, d.SessionID, d.TargetID, d.Prompt)
	if chunkID == "" {
		if repo, prNum, ok := extractPRRemediationTarget(d.Prompt); ok {
			prID := fmt.Sprintf("pr-%s-%d", repo, prNum)
			return prID, prID, 0
		}
	}
	return chunkID, buildID, chunkIdx
}
