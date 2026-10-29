package queue

import (
	"errors"
	"log"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"

	"github.com/azylman/aerial/services/eri/pkg/discord"
)

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
	norm := strings.ToLower(strings.TrimSpace(status))
	return norm == "running" || norm == "coding" || norm == "in_progress"
}

func cloneStatusWidgetRequest(req discord.StatusWidgetRequest, stripSteps bool) discord.StatusWidgetRequest {
	clone := req
	if len(req.Chunks) > 0 {
		clone.Chunks = make([]discord.StatusWidgetChunk, len(req.Chunks))
		for i, c := range req.Chunks {
			if len(c.DeployingJobs) > 0 {
				c.DeployingJobs = append([]string(nil), c.DeployingJobs...)
			}
			if stripSteps {
				c.ActiveStep = ""
			}
			clone.Chunks[i] = c
		}
	}
	return clone
}

func (p *WorkerPool) applyChannelPolicyRedirect(sess *discordgo.Session, req *discord.StatusWidgetRequest) {
	effID, effName, _, _ := ResolveChannelAndThread(sess, req.ChannelID)
	policy := p.resolveChannelPolicy(effID, effName)
	if !policy.BlockBuildCards && req.ChannelID != effID && req.ChannelID != "" {
		if targetPolicy := p.resolveChannelPolicy(req.ChannelID, ""); targetPolicy.BlockBuildCards {
			policy.BlockBuildCards = true
		}
	}
	if !policy.BlockBuildCards {
		return
	}
	sysChan := p.systemChannel()
	if sysChan == "" {
		return
	}
	if sysChanID, err := p.ResolveChannelID(sysChan); err == nil && sysChanID != "" && IsNumericSnowflake(sysChanID) {
		req.ChannelID = sysChanID
	}
}

func (p *WorkerPool) syncWidgetChunkStatesLocked(req *discord.StatusWidgetRequest, existing *discord.StatusWidgetRequest, isTerminalBuild bool) {
	for i := range req.Chunks {
		c := &req.Chunks[i]
		if c.ID == "" && existing != nil && i < len(existing.Chunks) && existing.Chunks[i].ID != "" {
			c.ID = existing.Chunks[i].ID
		}
		if c.ID != "" {
			p.chunkToBuildID[c.ID] = req.ID
		}
		if isTerminalBuild || !isWidgetChunkInProgress(c.Status) {
			if c.ID != "" {
				delete(p.activeChunkSteps, c.ID)
				delete(p.activeChunkTurns, c.ID)
			}
			c.ActiveStep = ""
		} else if c.ID != "" {
			if step, ok := p.activeChunkSteps[c.ID]; ok && step != "" {
				c.ActiveStep = step
			}
		}
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

func (p *WorkerPool) syncWidgetDeliveryStateLocked(req *discord.StatusWidgetRequest) {
	if req.ID == "" {
		return
	}
	existing, hasExisting := p.widgetsByID[req.ID]
	if req.MessageID == "" && hasExisting && existing.MessageID != "" {
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

func (p *WorkerPool) sendWidgetRequest(sess *discordgo.Session, req discord.StatusWidgetRequest) (string, error) {
	if p.cfg.WidgetDeliveryFunc != nil {
		return p.cfg.WidgetDeliveryFunc(req)
	}
	return discord.DeliverStatusWidget(sess, req, time.Now())
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

// DeliverStatusWidget delivers or edits a status widget while maintaining cache and active steps.
func (p *WorkerPool) DeliverStatusWidget(req discord.StatusWidgetRequest) (string, error) {
	if p == nil {
		return "", errors.New("worker pool is nil")
	}
	sess := p.DiscordSession()
	if sess == nil && p.cfg.WidgetDeliveryFunc == nil {
		return "", errors.New("discord session is not connected")
	}

	p.widgetDeliveryMu.Lock()
	defer p.widgetDeliveryMu.Unlock()

	p.applyChannelPolicyRedirect(sess, &req)

	p.widgetMu.Lock()
	p.ensureWidgetMapsLocked()
	p.syncWidgetDeliveryStateLocked(&req)
	p.widgetMu.Unlock()

	msgID, err := p.sendWidgetRequest(sess, req)
	if err != nil {
		return "", err
	}

	p.recordDeliveredWidgetMessageID(req.ID, msgID)
	return msgID, nil
}

// StartChunkStepTurn claims ownership of step streaming for a build chunk in an active turn.
func (p *WorkerPool) StartChunkStepTurn(chunkID, buildID string, chunkIdx int, turnID string) {
	if p == nil {
		return
	}
	chunkID = strings.TrimSpace(chunkID)
	turnID = strings.TrimSpace(turnID)
	if chunkID == "" || turnID == "" {
		return
	}

	p.widgetDeliveryMu.Lock()
	defer p.widgetDeliveryMu.Unlock()

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

func (p *WorkerPool) prepareChunkStepDeliveryLocked(chunkID, turnID, stepText string) (*discord.StatusWidgetRequest, bool) {
	if p.activeChunkTurns[chunkID] != turnID {
		return nil, false
	}

	trimmedStep := strings.TrimSpace(stepText)
	p.activeChunkSteps[chunkID] = trimmedStep

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
	deliveryReq.SkipSink = true
	deliveryReq.Chunks[chunkIdx].ActiveStep = trimmedStep
	for i := range deliveryReq.Chunks {
		if i != chunkIdx && isWidgetChunkInProgress(deliveryReq.Chunks[i].Status) {
			if s, hasStep := p.activeChunkSteps[deliveryReq.Chunks[i].ID]; hasStep {
				deliveryReq.Chunks[i].ActiveStep = s
			}
		}
	}
	return &deliveryReq, true
}

// FlushChunkActiveStep updates the active step for a chunk and delivers an in-place widget edit if currently running.
func (p *WorkerPool) FlushChunkActiveStep(chunkID, turnID, stepText string) error {
	if p == nil {
		return errors.New("worker pool is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	turnID = strings.TrimSpace(turnID)
	if chunkID == "" || turnID == "" {
		return nil
	}

	p.widgetDeliveryMu.Lock()
	defer p.widgetDeliveryMu.Unlock()

	p.widgetMu.Lock()
	p.ensureWidgetMapsLocked()
	deliveryReq, ok := p.prepareChunkStepDeliveryLocked(chunkID, turnID, stepText)
	p.widgetMu.Unlock()

	if !ok {
		return nil
	}

	sess := p.DiscordSession()
	if p.cfg.WidgetDeliveryFunc != nil {
		_, err := p.cfg.WidgetDeliveryFunc(*deliveryReq)
		return err
	}
	if sess == nil {
		return errors.New("discord session is not connected")
	}
	_, err := discord.DeliverStatusWidget(sess, *deliveryReq, time.Now())
	return err
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
	deliveryReq.SkipSink = true
	deliveryReq.Chunks[chunkIdx].ActiveStep = ""
	for i := range deliveryReq.Chunks {
		if i != chunkIdx && isWidgetChunkInProgress(deliveryReq.Chunks[i].Status) {
			if s, hasStep := p.activeChunkSteps[deliveryReq.Chunks[i].ID]; hasStep {
				deliveryReq.Chunks[i].ActiveStep = s
			}
		}
	}
	return &deliveryReq, true
}

// FinishChunkStepTurn disowns step streaming for a chunk and clears its active step on the widget.
func (p *WorkerPool) FinishChunkStepTurn(chunkID, turnID string) {
	if p == nil {
		return
	}
	chunkID = strings.TrimSpace(chunkID)
	turnID = strings.TrimSpace(turnID)
	if chunkID == "" || turnID == "" {
		return
	}

	p.widgetDeliveryMu.Lock()
	defer p.widgetDeliveryMu.Unlock()

	p.widgetMu.Lock()
	p.ensureWidgetMapsLocked()
	deliveryReq, ok := p.finishChunkStepTurnLocked(chunkID, turnID)
	p.widgetMu.Unlock()

	if !ok {
		return
	}

	sess := p.DiscordSession()
	if p.cfg.WidgetDeliveryFunc != nil {
		if _, err := p.cfg.WidgetDeliveryFunc(*deliveryReq); err != nil {
			log.Printf("[WorkerPool] Warning delivering finish chunk step widget: %v", err)
		}
		return
	}
	if sess != nil {
		if _, err := discord.DeliverStatusWidget(sess, *deliveryReq, time.Now()); err != nil {
			log.Printf("[WorkerPool] Warning delivering finish chunk step widget: %v", err)
		}
	}
}
