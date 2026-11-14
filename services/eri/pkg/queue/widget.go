package queue

import (
	"context"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/eri/pkg/discord"
)

const (
	spanWidgetLockWait = "widget.lock_wait"
	spanWidgetDeliver  = "widget.deliver"
	spanWidgetHydrate  = "widget.hydrate"

	attrWidgetLockHolder = attribute.Key("aerial.widget.lock_holder")
	attrWidgetHasStep    = attribute.Key("aerial.widget.has_step")
	attrWidgetChunks     = attribute.Key("aerial.widget.chunks")
)

// widgetLockPath names the code path holding widgetDeliveryMu, recorded on widget.lock_wait spans.
type widgetLockPath string

const (
	widgetLockDeliver widgetLockPath = "deliver"
	widgetLockStep    widgetLockPath = "step"
	widgetLockStart   widgetLockPath = "start"
	widgetLockFinish  widgetLockPath = "finish"
)

// startWidgetSpan opens a child of the turn or request span carried by ctx. Without one it records
// nothing, so widget calls made outside a trace never open orphan root spans.
func (p *WorkerPool) startWidgetSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return ctx, trace.SpanFromContext(ctx)
	}
	return p.tracer().Start(ctx, name, trace.WithAttributes(attrs...)) //nolint:spancheck // the caller owns and ends the span
}

// lockWidgetDelivery acquires widgetDeliveryMu inside a widget.lock_wait span naming the path that
// held the lock when the wait began, then records path as the holder until unlockWidgetDelivery.
func (p *WorkerPool) lockWidgetDelivery(ctx context.Context, path widgetLockPath) {
	var attrs []attribute.KeyValue
	if holder := p.widgetLockHolder.Load(); holder != nil {
		attrs = append(attrs, attrWidgetLockHolder.String(string(*holder)))
	}
	_, span := p.startWidgetSpan(ctx, spanWidgetLockWait, attrs...)
	p.widgetDeliveryMu.Lock()
	span.End()
	p.widgetLockHolder.Store(&path)
}

func (p *WorkerPool) unlockWidgetDelivery() {
	p.widgetLockHolder.Store(nil)
	p.widgetDeliveryMu.Unlock()
}

// widgetDeliverAttrs describes a widget edit without any of its content.
func widgetDeliverAttrs(req discord.StatusWidgetRequest) []attribute.KeyValue {
	hasStep := false
	for _, c := range req.Chunks {
		if c.ActiveStep != "" {
			hasStep = true
			break
		}
	}
	return []attribute.KeyValue{attrWidgetHasStep.Bool(hasStep), attrWidgetChunks.Int(len(req.Chunks))}
}

func (p *WorkerPool) hasWarmChunkWidget(chunkID string) bool {
	p.widgetMu.Lock()
	defer p.widgetMu.Unlock()

	buildID := p.chunkToBuildID[chunkID]
	req, ok := p.widgetsByID[buildID]
	if !ok || req.MessageID == "" {
		return false
	}
	for _, c := range req.Chunks {
		if c.ID == chunkID && isWidgetChunkInProgress(c.Status) {
			return true
		}
	}
	return false
}

func (p *WorkerPool) resolveSynapseURL() string {
	if p == nil {
		return ""
	}
	if u := strings.TrimSpace(p.cfg.SynapseURL); u != "" {
		return strings.TrimRight(u, "/")
	}
	if p.appCfg != nil {
		if cur := p.appCfg.Current(); cur != nil {
			if u := strings.TrimSpace(cur.SynapseURL); u != "" {
				return strings.TrimRight(u, "/")
			}
		}
	}
	return ""
}

func (p *WorkerPool) ensureChunkWidgetHydrated(ctx context.Context, prompt, chunkID, buildID string) {
	if p.hasWarmChunkWidget(chunkID) {
		return
	}
	routerURL := p.resolveSynapseURL()
	bodyJSON, ok := extractDispatchWidgetSyncPayload(prompt, buildID)
	if routerURL == "" || !ok {
		return
	}
	ctx, span := p.startWidgetSpan(ctx, spanWidgetHydrate)
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, routerURL+"/internal/widget/sync", strings.NewReader(bodyJSON))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := tracing.Client(http.DefaultClient).Do(req)
	if err != nil {
		log.Printf("[WorkerPool] Warning syncing cold widget cache for chunk %s: %v", chunkID, err)
		return
	}
	_ = resp.Body.Close()
}

func (p *WorkerPool) ensureWidgetMapsLocked() {
	if p.widgetsByID == nil {
		p.widgetsByID = make(map[string]discord.StatusWidgetRequest)
	}
	if p.chunkToBuildID == nil {
		p.chunkToBuildID = make(map[string]string)
	}
	if p.activeChunkSteps == nil {
		p.activeChunkSteps = make(map[string]string)
	}
	if p.activeChunkTurns == nil {
		p.activeChunkTurns = make(map[string]string)
	}
}

func isWidgetChunkInProgress(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "running", "coding", "in_progress", "failed", "ci_failed", "deploy_failed", "conflict":
		return true
	default:
		return false
	}
}

func isWidgetChunkWaitingOnCI(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "open", "pr_opened", "merged", "synced", "deploying":
		return true
	default:
		return false
	}
}

func sameWidgetChunkPhase(a, b string) bool {
	normA := strings.ToLower(strings.TrimSpace(a))
	if normA == "pr_opened" {
		normA = "open"
	}
	normB := strings.ToLower(strings.TrimSpace(b))
	if normB == "pr_opened" {
		normB = "open"
	}
	return normA != "" && normA == normB
}

func cloneStatusWidgetRequest(req discord.StatusWidgetRequest, stripSteps bool) discord.StatusWidgetRequest {
	clone := req
	if len(req.Chunks) > 0 {
		clone.Chunks = make([]discord.StatusWidgetChunk, len(req.Chunks))
		for i, c := range req.Chunks {
			if len(c.DeployingJobs) > 0 {
				c.DeployingJobs = append([]string(nil), c.DeployingJobs...)
			}
			if stripSteps && !isWidgetChunkWaitingOnCI(c.Status) {
				c.ActiveStep = ""
			}
			clone.Chunks[i] = c
		}
	}
	return clone
}

func (p *WorkerPool) applyChannelPolicyRedirect(sess *discordgo.Session, req *discord.StatusWidgetRequest) {
	if p == nil || req == nil {
		return
	}
	req.ChannelID = p.redirectChannelIfNoBuildUpdatesWithSession(sess, req.ChannelID)
}

func (p *WorkerPool) clearChunkStepStateLocked(chunkID string) {
	if chunkID != "" {
		delete(p.activeChunkSteps, chunkID)
		delete(p.activeChunkTurns, chunkID)
	}
}

func (p *WorkerPool) syncSingleWidgetChunkLocked(c *discord.StatusWidgetChunk, existing *discord.StatusWidgetRequest, idx int, buildID string, isSinglePR, isTerminalBuild bool) {
	if c.ID == "" && existing != nil && idx < len(existing.Chunks) && existing.Chunks[idx].ID != "" {
		c.ID = existing.Chunks[idx].ID
	}
	if c.ID == "" && isSinglePR {
		c.ID = buildID
	}
	if c.ID != "" {
		p.chunkToBuildID[c.ID] = buildID
	}
	if isTerminalBuild {
		p.clearChunkStepStateLocked(c.ID)
		c.ActiveStep = ""
		return
	}
	if isWidgetChunkWaitingOnCI(c.Status) {
		p.clearChunkStepStateLocked(c.ID)
		c.ActiveStep = strings.TrimSpace(c.ActiveStep)
		if c.ActiveStep == "" && existing != nil && idx < len(existing.Chunks) && sameWidgetChunkPhase(existing.Chunks[idx].Status, c.Status) {
			c.ActiveStep = existing.Chunks[idx].ActiveStep
		}
		return
	}
	if !isWidgetChunkInProgress(c.Status) {
		p.clearChunkStepStateLocked(c.ID)
		c.ActiveStep = ""
		return
	}
	if c.ID != "" {
		if step, ok := p.activeChunkSteps[c.ID]; ok && step != "" {
			c.ActiveStep = step
		}
	}
}

func (p *WorkerPool) syncWidgetChunkStatesLocked(req *discord.StatusWidgetRequest, existing *discord.StatusWidgetRequest, isTerminalBuild bool) {
	isSinglePR := len(req.Chunks) == 1 && strings.HasPrefix(req.ID, "pr-")
	for i := range req.Chunks {
		p.syncSingleWidgetChunkLocked(&req.Chunks[i], existing, i, req.ID, isSinglePR, isTerminalBuild)
	}
}

func (p *WorkerPool) evictOldestWidgetLocked() {
	if len(p.widgetOrder) < 64 {
		return
	}
	oldest := p.widgetOrder[0]
	p.widgetOrder = p.widgetOrder[1:]
	delete(p.widgetsByID, oldest)
	for cID, bID := range p.chunkToBuildID {
		if bID == oldest {
			delete(p.chunkToBuildID, cID)
			delete(p.activeChunkSteps, cID)
			delete(p.activeChunkTurns, cID)
		}
	}
}

func shouldPreferCachedMessageID(reqMsgID, cachedMsgID string) bool {
	cachedTrimmed := strings.TrimSpace(cachedMsgID)
	if cachedTrimmed == "" {
		return false
	}
	reqTrimmed := strings.TrimSpace(reqMsgID)
	if reqTrimmed == "" {
		return true
	}
	cachedID, errCached := strconv.ParseUint(cachedTrimmed, 10, 64)
	reqID, errReq := strconv.ParseUint(reqTrimmed, 10, 64)
	if errCached == nil && errReq == nil {
		return cachedID > reqID
	}
	return false
}

func (p *WorkerPool) syncWidgetDeliveryStateLocked(req *discord.StatusWidgetRequest) {
	if req.ID == "" {
		return
	}
	existing, hasExisting := p.widgetsByID[req.ID]
	if hasExisting && shouldPreferCachedMessageID(req.MessageID, existing.MessageID) {
		req.MessageID = existing.MessageID
	}
	isTerminalBuild := strings.EqualFold(req.Status, "completed") || strings.EqualFold(req.Status, "cancelled")

	var existingPtr *discord.StatusWidgetRequest
	if hasExisting {
		existingPtr = &existing
	}
	p.syncWidgetChunkStatesLocked(req, existingPtr, isTerminalBuild)

	if isTerminalBuild {
		for cID, bID := range p.chunkToBuildID {
			if bID == req.ID {
				delete(p.activeChunkSteps, cID)
				delete(p.activeChunkTurns, cID)
			}
		}
	}
	if !hasExisting {
		p.evictOldestWidgetLocked()
		p.widgetOrder = append(p.widgetOrder, req.ID)
	}
	p.widgetsByID[req.ID] = cloneStatusWidgetRequest(*req, true)
}

func (p *WorkerPool) sendWidgetRequest(ctx context.Context, sess *discordgo.Session, req discord.StatusWidgetRequest) (string, error) {
	if p.cfg.WidgetDeliveryFunc != nil {
		return p.cfg.WidgetDeliveryFunc(req)
	}
	return discord.DeliverStatusWidget(sess, req, time.Now(), discordgo.WithContext(ctx))
}

func (p *WorkerPool) recordDeliveredWidgetMessageID(buildID, msgID string) {
	if buildID == "" || msgID == "" {
		return
	}
	p.widgetMu.Lock()
	if w, ok := p.widgetsByID[buildID]; ok {
		w.MessageID = msgID
		p.widgetsByID[buildID] = w
	}
	p.widgetMu.Unlock()
}

// DeliverStatusWidgetWithContext delivers or edits a status widget while respecting context cancellation, maintaining cache and active steps.
func (p *WorkerPool) DeliverStatusWidgetWithContext(ctx context.Context, req discord.StatusWidgetRequest) (string, error) {
	if p == nil {
		return "", errors.New("worker pool is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	sess := p.DiscordSession()
	if sess == nil && p.cfg.WidgetDeliveryFunc == nil {
		return "", errors.New("discord session is not connected")
	}

	ctx, span := p.startWidgetSpan(ctx, spanWidgetDeliver)
	defer span.End()

	p.lockWidgetDelivery(ctx, widgetLockDeliver)
	defer p.unlockWidgetDelivery()

	if err := ctx.Err(); err != nil {
		return "", err
	}

	p.applyChannelPolicyRedirect(sess, &req)

	p.widgetMu.Lock()
	p.ensureWidgetMapsLocked()
	p.syncWidgetDeliveryStateLocked(&req)
	p.widgetMu.Unlock()
	span.SetAttributes(widgetDeliverAttrs(req)...)

	msgID, err := p.sendWidgetRequest(ctx, sess, req)
	if err != nil {
		return "", err
	}

	p.recordDeliveredWidgetMessageID(req.ID, msgID)
	return msgID, nil
}

// StartChunkStepTurn claims ownership of step streaming for a build chunk in an active turn.
func (p *WorkerPool) StartChunkStepTurn(ctx context.Context, chunkID, buildID string, chunkIdx int, turnID string) {
	if p == nil {
		return
	}
	chunkID = strings.TrimSpace(chunkID)
	turnID = strings.TrimSpace(turnID)
	if chunkID == "" || turnID == "" {
		return
	}

	p.lockWidgetDelivery(ctx, widgetLockStart)
	defer p.unlockWidgetDelivery()

	p.widgetMu.Lock()
	p.ensureWidgetMapsLocked()
	p.activeChunkTurns[chunkID] = turnID
	if buildID != "" {
		p.chunkToBuildID[chunkID] = buildID
		if w, ok := p.widgetsByID[buildID]; ok {
			if chunkIdx >= 0 && chunkIdx < len(w.Chunks) && w.Chunks[chunkIdx].ID == "" {
				w.Chunks[chunkIdx].ID = chunkID
				p.widgetsByID[buildID] = w
			}
		}
	}
	p.widgetMu.Unlock()
}

func (p *WorkerPool) buildChunkStepDeliveryRequestLocked(chunkID, activeStep string) (*discord.StatusWidgetRequest, bool) {
	buildID := p.chunkToBuildID[chunkID]
	if buildID == "" {
		return nil, false
	}

	req, ok := p.widgetsByID[buildID]
	if !ok || req.MessageID == "" {
		return nil, false
	}

	chunkIdx := -1
	for i := range req.Chunks {
		if req.Chunks[i].ID == chunkID {
			chunkIdx = i
			break
		}
	}
	if chunkIdx == -1 || !isWidgetChunkInProgress(req.Chunks[chunkIdx].Status) {
		return nil, false
	}

	deliveryReq := cloneStatusWidgetRequest(req, false)
	deliveryReq.Chunks[chunkIdx].ActiveStep = activeStep
	for i := range deliveryReq.Chunks {
		if i != chunkIdx && isWidgetChunkInProgress(deliveryReq.Chunks[i].Status) {
			if s, hasStep := p.activeChunkSteps[deliveryReq.Chunks[i].ID]; hasStep {
				deliveryReq.Chunks[i].ActiveStep = s
			}
		}
	}
	return &deliveryReq, true
}

func (p *WorkerPool) prepareChunkStepDeliveryLocked(chunkID, turnID, stepText string) (*discord.StatusWidgetRequest, bool) {
	if p.activeChunkTurns[chunkID] != turnID {
		return nil, false
	}

	trimmedStep := strings.TrimSpace(stepText)
	p.activeChunkSteps[chunkID] = trimmedStep

	return p.buildChunkStepDeliveryRequestLocked(chunkID, trimmedStep)
}

// FlushChunkActiveStep updates the active step for a chunk and delivers an in-place widget edit if currently running.
func (p *WorkerPool) FlushChunkActiveStep(ctx context.Context, chunkID, turnID, stepText string) error {
	if p == nil {
		return errors.New("worker pool is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	turnID = strings.TrimSpace(turnID)
	if chunkID == "" || turnID == "" {
		return nil
	}

	ctx, span := p.startWidgetSpan(ctx, spanWidgetDeliver)
	defer span.End()

	p.lockWidgetDelivery(ctx, widgetLockStep)
	defer p.unlockWidgetDelivery()

	p.widgetMu.Lock()
	p.ensureWidgetMapsLocked()
	deliveryReq, ok := p.prepareChunkStepDeliveryLocked(chunkID, turnID, stepText)
	p.widgetMu.Unlock()

	if !ok {
		return nil
	}

	span.SetAttributes(widgetDeliverAttrs(*deliveryReq)...)
	return p.deliverChunkStepWidget(ctx, *deliveryReq)
}

// deliverChunkStepWidget delivers a step edit and records the returned message ID so a sunk card stays tracked.
// Callers must hold widgetDeliveryMu.
func (p *WorkerPool) deliverChunkStepWidget(ctx context.Context, req discord.StatusWidgetRequest) error {
	sess := p.DiscordSession()
	if sess == nil && p.cfg.WidgetDeliveryFunc == nil {
		return errors.New("discord session is not connected")
	}
	msgID, err := p.sendWidgetRequest(ctx, sess, req)
	if err != nil {
		return err
	}
	p.recordDeliveredWidgetMessageID(req.ID, msgID)
	return nil
}

func (p *WorkerPool) finishChunkStepTurnLocked(chunkID, turnID string) (*discord.StatusWidgetRequest, bool) {
	if p.activeChunkTurns[chunkID] != turnID {
		return nil, false
	}

	delete(p.activeChunkTurns, chunkID)
	prevStep := p.activeChunkSteps[chunkID]
	delete(p.activeChunkSteps, chunkID)

	if prevStep == "" {
		return nil, false
	}

	return p.buildChunkStepDeliveryRequestLocked(chunkID, "")
}

// FinishChunkStepTurn disowns step streaming for a chunk and clears its active step on the widget.
func (p *WorkerPool) FinishChunkStepTurn(ctx context.Context, chunkID, turnID string) {
	if p == nil {
		return
	}
	chunkID = strings.TrimSpace(chunkID)
	turnID = strings.TrimSpace(turnID)
	if chunkID == "" || turnID == "" {
		return
	}

	p.lockWidgetDelivery(ctx, widgetLockFinish)
	defer p.unlockWidgetDelivery()

	p.widgetMu.Lock()
	p.ensureWidgetMapsLocked()
	deliveryReq, ok := p.finishChunkStepTurnLocked(chunkID, turnID)
	p.widgetMu.Unlock()

	if !ok {
		return
	}

	if p.cfg.WidgetDeliveryFunc == nil && p.DiscordSession() == nil {
		return
	}
	if err := p.deliverChunkStepWidget(ctx, *deliveryReq); err != nil {
		log.Printf("[WorkerPool] Warning delivering finish chunk step widget: %v", err)
	}
}
