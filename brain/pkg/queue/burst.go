package queue

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/azylman/aerial/brain/pkg/classifier"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/memory"
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

// WakeInfo records ambient classification telemetry and decisions for a message.
type WakeInfo struct {
	IsWake    bool
	Score     float64
	Threshold float64
	Reason    string
}

// BurstPlan encapsulates deterministic partitioning and wake decisions for a burst.
type BurstPlan struct {
	WakeIndex        int
	WakeInfos        []WakeInfo
	IsAllAmbient     bool
	LeadingAmbient   []db.Message
	ActiveMessage    db.Message
	HasActiveMessage bool
	TrailingMessages []db.Message
	TrailingInfos    []WakeInfo
}

// BurstPlanInput configures the burst execution planner.
type BurstPlanInput struct {
	Burst        []db.Message
	WakeMode     string
	Threshold    float64
	BotUserID    string
	BotRoleIDs   []string
	HookOverride string
	ClassifierFn func(msgs []db.Message) (score float64, reason string)
}

// PlanBurstExecution deterministically partitions an incoming burst into leading ambient messages,
// the active wake turn message, and trailing messages based on channel policy and wake criteria.
func PlanBurstExecution(input BurstPlanInput) BurstPlan {
	if len(input.Burst) == 0 {
		return BurstPlan{WakeIndex: -1}
	}

	wm := strings.ToLower(strings.TrimSpace(input.WakeMode))
	switch wm {
	case "mentions", "direct":
		wm = "mention"
	case "always":
		wm = "all"
	case "ambient", "":
		wm = "classifier"
	}

	classify := input.ClassifierFn
	if classify == nil {
		classify = func([]db.Message) (float64, string) {
			return 0.0, "no classifier configured"
		}
	}

	threshold := input.Threshold
	hook := strings.ToLower(strings.TrimSpace(input.HookOverride))

	switch hook {
	case "wake":
		plan := BurstPlan{
			WakeIndex:        0,
			WakeInfos:        make([]WakeInfo, len(input.Burst)),
			ActiveMessage:    input.Burst[0],
			HasActiveMessage: true,
		}
		for i := range input.Burst {
			plan.WakeInfos[i] = WakeInfo{
				IsWake:    i == 0,
				Score:     1.0,
				Threshold: threshold,
				Reason:    "hook_wake_override",
			}
		}
		if len(input.Burst) > 1 {
			plan.TrailingMessages = input.Burst[1:]
			plan.TrailingInfos = plan.WakeInfos[1:]
		}
		return plan

	case "drop":
		plan := BurstPlan{
			WakeIndex:    -1,
			IsAllAmbient: true,
			WakeInfos:    make([]WakeInfo, len(input.Burst)),
		}
		for i := range input.Burst {
			plan.WakeInfos[i] = WakeInfo{
				IsWake:    false,
				Score:     0.0,
				Threshold: threshold,
				Reason:    "hook_drop_override",
			}
		}
		return plan
	}

	wakeInfos := make([]WakeInfo, len(input.Burst))
	wakeIdx := -1

	// Safeguard 1: Tier-1 Pre-Scan (Direct Mentions / Replies / Keywords)
	for i, m := range input.Burst {
		if isTier1Wake(m, input.BotUserID, input.BotRoleIDs, wm) {
			wakeInfos[i] = WakeInfo{
				IsWake:    true,
				Score:     1.0,
				Threshold: threshold,
				Reason:    "direct_address",
			}
			if wakeIdx == -1 {
				wakeIdx = i
			}
		}
	}

	if wakeIdx != -1 {
		// Leading ambient messages prior to wake message
		for i := 0; i < wakeIdx; i++ {
			wakeInfos[i] = WakeInfo{
				IsWake:    false,
				Score:     0.0,
				Threshold: threshold,
				Reason:    "burst_prescan_leading",
			}
		}

		// Trailing messages after wake message
		for i := wakeIdx + 1; i < len(input.Burst); i++ {
			m := input.Burst[i]
			if isTier1Wake(m, input.BotUserID, input.BotRoleIDs, wm) {
				wakeInfos[i] = WakeInfo{
					IsWake:    true,
					Score:     1.0,
					Threshold: threshold,
					Reason:    "direct_address",
				}
			} else if wm == "mention" || threshold <= 0.0 || classifier.IsHeuristicSkip(extractMessageBody(m.Content)) {
				wakeInfos[i] = WakeInfo{
					IsWake:    false,
					Score:     0.0,
					Threshold: threshold,
					Reason:    "heuristic_skip",
				}
			} else {
				score, reason := classify([]db.Message{m})
				wakeInfos[i] = WakeInfo{
					IsWake:    score >= threshold,
					Score:     score,
					Threshold: threshold,
					Reason:    reason,
				}
			}
		}

		plan := BurstPlan{
			WakeIndex:        wakeIdx,
			WakeInfos:        wakeInfos,
			ActiveMessage:    input.Burst[wakeIdx],
			HasActiveMessage: true,
		}
		if wakeIdx > 0 {
			plan.LeadingAmbient = input.Burst[:wakeIdx]
		}
		if wakeIdx+1 < len(input.Burst) {
			plan.TrailingMessages = input.Burst[wakeIdx+1:]
			plan.TrailingInfos = wakeInfos[wakeIdx+1:]
		}
		return plan
	}

	if wm == "mention" {
		for i := range input.Burst {
			wakeInfos[i] = WakeInfo{
				IsWake:    false,
				Score:     0.0,
				Threshold: 0.0,
				Reason:    "mention_mode_ambient",
			}
		}
		return BurstPlan{
			WakeIndex:    -1,
			IsAllAmbient: true,
			WakeInfos:    wakeInfos,
		}
	}

	if wm == "all" {
		for i := range input.Burst {
			wakeInfos[i] = WakeInfo{
				IsWake:    i == 0,
				Score:     1.0,
				Threshold: 0.0,
				Reason:    "all_wake",
			}
		}
		plan := BurstPlan{
			WakeIndex:        0,
			WakeInfos:        wakeInfos,
			ActiveMessage:    input.Burst[0],
			HasActiveMessage: true,
		}
		if len(input.Burst) > 1 {
			plan.TrailingMessages = input.Burst[1:]
			plan.TrailingInfos = wakeInfos[1:]
		}
		return plan
	}

	// wm == "classifier"
	if threshold <= 0.0 {
		for i := range input.Burst {
			wakeInfos[i] = WakeInfo{
				IsWake:    false,
				Score:     0.0,
				Threshold: threshold,
				Reason:    "classifier disabled",
			}
		}
		return BurstPlan{
			WakeIndex:    -1,
			IsAllAmbient: true,
			WakeInfos:    wakeInfos,
		}
	}

	allSkip := true
	for _, m := range input.Burst {
		if !classifier.IsHeuristicSkip(extractMessageBody(m.Content)) {
			allSkip = false
			break
		}
	}
	if allSkip {
		for i := range input.Burst {
			wakeInfos[i] = WakeInfo{
				IsWake:    false,
				Score:     0.0,
				Threshold: threshold,
				Reason:    "heuristic_skip",
			}
		}
		return BurstPlan{
			WakeIndex:    -1,
			IsAllAmbient: true,
			WakeInfos:    wakeInfos,
		}
	}

	score, reason := classify(input.Burst)
	isWake := score >= threshold
	for i := range input.Burst {
		wakeInfos[i] = WakeInfo{
			IsWake:    i == 0 && isWake,
			Score:     score,
			Threshold: threshold,
			Reason:    reason,
		}
	}

	if isWake {
		plan := BurstPlan{
			WakeIndex:        0,
			WakeInfos:        wakeInfos,
			ActiveMessage:    input.Burst[0],
			HasActiveMessage: true,
		}
		if len(input.Burst) > 1 {
			plan.TrailingMessages = input.Burst[1:]
			plan.TrailingInfos = wakeInfos[1:]
		}
		return plan
	}

	return BurstPlan{
		WakeIndex:    -1,
		IsAllAmbient: true,
		WakeInfos:    wakeInfos,
	}
}

// TurnPromptInput collects pre-fetched context components for deterministic prompt assembly.
type TurnPromptInput struct {
	Burst               []db.Message
	ThreadSummary       string
	LookbackHistory     []HistoryMessage
	PreviousSessionID   string
	IsColdStart         bool
	SemanticMemoryFacts []db.Fact
	ChannelInstructions string
	InjectedHookContext string
}

// AssembleTurnPrompt deterministically composes the 7-layer turn prompt in exact top-to-bottom document order.
func AssembleTurnPrompt(input TurnPromptInput) string {
	basePrompt := CoalesceBurstPrompt(input.Burst)

	// Layer 6: Thread Summary (if present)
	if strings.TrimSpace(input.ThreadSummary) != "" {
		sumText := strings.TrimSpace(input.ThreadSummary)
		if !strings.HasPrefix(sumText, "<THREAD_SUMMARY>") {
			sumText = fmt.Sprintf("<THREAD_SUMMARY>\n%s\n</THREAD_SUMMARY>", sumText)
		}
		if basePrompt != "" {
			basePrompt = sumText + "\n\n" + basePrompt
		} else {
			basePrompt = sumText
		}
	}

	// Layer 5: Channel History Lookback (if present)
	if len(input.LookbackHistory) > 0 {
		if formattedHist := FormatChannelHistory(input.LookbackHistory); formattedHist != "" {
			if basePrompt != "" {
				basePrompt = formattedHist + "\n\n" + basePrompt
			} else {
				basePrompt = formattedHist
			}
		}
	}

	// Layer 4: Previous Session ID (on cold start)
	if input.IsColdStart && strings.TrimSpace(input.PreviousSessionID) != "" {
		if prevBlock := FormatPreviousSession(input.PreviousSessionID); prevBlock != "" {
			if basePrompt != "" {
				basePrompt = prevBlock + "\n\n" + basePrompt
			} else {
				basePrompt = prevBlock
			}
		}
	}

	// Layer 3: Semantic Memory Facts (if present)
	if len(input.SemanticMemoryFacts) > 0 {
		if memoryBlock := memory.FormatMemoryContext(input.SemanticMemoryFacts); memoryBlock != "" {
			if basePrompt != "" {
				basePrompt = memoryBlock + "\n\n" + basePrompt
			} else {
				basePrompt = memoryBlock
			}
		}
	}

	// Layer 2: Channel Instructions (if present)
	if strings.TrimSpace(input.ChannelInstructions) != "" {
		instText := strings.TrimSpace(input.ChannelInstructions)
		if !strings.HasPrefix(instText, "<CHANNEL_INSTRUCTIONS>") {
			instText = fmt.Sprintf("<CHANNEL_INSTRUCTIONS>\nChannel-specific guidelines for this conversation:\n\n%s\n</CHANNEL_INSTRUCTIONS>", instText)
		}
		if basePrompt != "" {
			basePrompt = instText + "\n\n" + basePrompt
		} else {
			basePrompt = instText
		}
	}

	// Layer 1: Coordination Context Hook (if present)
	if strings.TrimSpace(input.InjectedHookContext) != "" {
		hookText := strings.TrimSpace(input.InjectedHookContext)
		if !strings.HasPrefix(hookText, "<COORDINATION_CONTEXT>") {
			hookText = fmt.Sprintf("<COORDINATION_CONTEXT>\n%s\n</COORDINATION_CONTEXT>", hookText)
		}
		if basePrompt != "" {
			basePrompt = hookText + "\n\n" + basePrompt
		} else {
			basePrompt = hookText
		}
	}

	return strings.TrimSpace(basePrompt)
}

// EvaluateBurstStaleness evaluates staleness atomically across an entire message burst.
func EvaluateBurstStaleness(
	burst []db.Message,
	stalenessTTL time.Duration,
	isColdThread bool,
	lastSessionActivity time.Time,
	now time.Time,
) (isStale bool, reason string) {
	if len(burst) == 0 {
		return false, ""
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if stalenessTTL <= 0 {
		stalenessTTL = 30 * time.Minute
	}

	latestMsg := burst[0]
	for _, m := range burst[1:] {
		if m.CreatedAt.After(latestMsg.CreatedAt) {
			latestMsg = m
		}
	}

	if latestMsg.CreatedAt.IsZero() {
		return false, "zero_timestamp_retained"
	}

	var msgAge time.Duration
	if now.After(latestMsg.CreatedAt) {
		msgAge = now.Sub(latestMsg.CreatedAt)
	}

	if msgAge > MaxMessageAbsoluteAge {
		return true, fmt.Sprintf("exceeded hard absolute age cap %v: msg age %v", MaxMessageAbsoluteAge, msgAge)
	}

	hasRecovered := false
	for _, m := range burst {
		if m.RetryCount > 0 || m.RestartCount > 0 {
			hasRecovered = true
			break
		}
	}

	if !hasRecovered && msgAge > stalenessTTL {
		if isColdThread {
			return true, fmt.Sprintf("new thread and message age %v > TTL %v", msgAge, stalenessTTL)
		}
		if !lastSessionActivity.IsZero() {
			if now.Sub(lastSessionActivity) > stalenessTTL {
				return true, fmt.Sprintf("existing session inactive for %v > TTL %v", now.Sub(lastSessionActivity), stalenessTTL)
			}
			return false, fmt.Sprintf("existing session had recent activity %v ago <= TTL", now.Sub(lastSessionActivity))
		}
	}

	return false, ""
}

// EvaluateMessageStaleness evaluates staleness for a single message.
func EvaluateMessageStaleness(
	msg db.Message,
	stalenessTTL time.Duration,
	isColdThread bool,
	lastSessionActivity time.Time,
	now time.Time,
) (isStale bool, reason string) {
	return EvaluateBurstStaleness([]db.Message{msg}, stalenessTTL, isColdThread, lastSessionActivity, now)
}
