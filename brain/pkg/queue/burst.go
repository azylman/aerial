package queue

import (
	"fmt"
	"log"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/azylman/aerial/brain/pkg/classifier"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/sanitizer"
)

func (p *WorkerPool) Enqueue(msg db.Message) {
	p.EnqueueBurst(msg)
}

func resolveMessageEffort(m db.Message) string {
	effort := strings.ToLower(strings.TrimSpace(m.Effort))
	switch effort {
	case "medium", "low", "high":
		return effort
	}
	if effort != "" {
		return "high"
	}
	model := strings.ToLower(strings.TrimSpace(m.Metadata.Model))
	if strings.Contains(model, "medium") {
		return "medium"
	}
	if strings.Contains(model, "flash") {
		return "low"
	}
	return "high"
}

func (p *WorkerPool) resolveScopeKey(m db.Message) string {
	switch resolveMessageEffort(m) {
	case "medium":
		if p != nil && p.mediumEffortProcessPool != nil {
			return "medium_effort:" + m.ThreadID
		}
	case "low":
		if p != nil && p.lowEffortProcessPool != nil {
			return "low_effort:" + m.ThreadID
		}
	}
	return m.ThreadID
}

// ScopeKey returns the queue and lock partition key for a message.
func (p *WorkerPool) ScopeKey(m db.Message) string {
	return p.resolveScopeKey(m)
}

// EnqueueBurst atomically enqueues multiple messages for a thread into the queue.
func (p *WorkerPool) EnqueueBurst(msgs ...db.Message) {
	if len(msgs) == 0 {
		return
	}
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		log.Printf("[WorkerPool] Warning: attempted to enqueue %d message(s) to stopped pool", len(msgs))
		return
	}

	type newWorker struct {
		scopeKey string
		state    *threadWorkerState
	}
	var newWorkers []newWorker

	for _, msg := range msgs {
		scopeKey := p.resolveScopeKey(msg)
		state, exists := p.threadChs[scopeKey]
		if !exists {
			state = &threadWorkerState{ch: make(chan db.Message, 100)}
			p.threadChs[scopeKey] = state
			p.wg.Add(1)
			newWorkers = append(newWorkers, newWorker{scopeKey: scopeKey, state: state})
		}

		select {
		case state.ch <- msg:
			metrics.QueueDepth.Inc()
		default:
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
		}
	}
	p.mu.Unlock()

	for _, nw := range newWorkers {
		go p.runThreadWorker(nw.scopeKey, nw.state)
	}
}

func (p *WorkerPool) runThreadWorker(scopeKey string, state *threadWorkerState) {
	defer p.wg.Done()
	threadID := strings.TrimPrefix(strings.TrimPrefix(scopeKey, "low_effort:"), "medium_effort:")

	idleTimeout := p.cfg.IdleTimeout
	if idleTimeout <= 0 {
		if p.appCfg != nil {
			idleTimeout = p.appCfg.Get().DaemonIdleTimeout
		}
		if idleTimeout <= 0 {
			idleTimeout = DefaultMaxSessionIdleTime
		}
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

			p.mu.Lock()
			state.inFlight = true
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
			p.mu.Unlock()

			p.processBurst(burst, scopeKey)

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
				hasActiveTasks := false
				if d := getStreamingDaemon(p.processPool, threadID); d != nil && d.TaskTracker().ActiveCount() > 0 {
					hasActiveTasks = true
				} else if d := getStreamingDaemon(p.lowEffortProcessPool, threadID); d != nil && d.TaskTracker().ActiveCount() > 0 {
					hasActiveTasks = true
				} else if d := getStreamingDaemon(p.mediumEffortProcessPool, threadID); d != nil && d.TaskTracker().ActiveCount() > 0 {
					hasActiveTasks = true
				}
				if hasActiveTasks {
					p.mu.Unlock()
					idleTimer.Reset(idleTimeout)
					continue
				}
				delete(p.threadChs, scopeKey)
				p.scopeLocks.Delete(scopeKey)
				p.mu.Unlock()
				return
			}
			p.mu.Unlock()
			idleTimer.Reset(idleTimeout)
		}
	}
}

// FormatSingleDiscordPrompt formats a single structured Discord message into the canonical <USER_REQUEST> prompt envelope JIT.
func FormatSingleDiscordPrompt(m db.Message, tzOpt ...string) string {
	var sb strings.Builder
	sb.WriteString("<USER_REQUEST>\nHere's a message someone sent you from Discord:\n\n")
	sb.WriteString(fmt.Sprintf("- id: %s\n", m.ID))
	channelID := m.Metadata.ChannelID
	if channelID == "" {
		channelID = m.ThreadID
	}
	sb.WriteString(fmt.Sprintf("- channel_id: %s\n", channelID))
	if m.Metadata.ChannelName != "" {
		if clean := sanitizePromptHeaderField(m.Metadata.ChannelName); clean != "" {
			if !strings.HasPrefix(clean, "#") {
				clean = "#" + clean
			}
			sb.WriteString(fmt.Sprintf("- channel_name: %q\n", clean))
		}
	}
	sb.WriteString(fmt.Sprintf("- thread_id: %s\n", m.ThreadID))
	if m.Metadata.ThreadName != "" {
		if clean := sanitizePromptHeaderField(m.Metadata.ThreadName); clean != "" {
			sb.WriteString(fmt.Sprintf("- thread_name: %q\n", clean))
		}
	}
	sb.WriteString(fmt.Sprintf("- guild_id: %s\n", m.GuildID))
	sb.WriteString(fmt.Sprintf("- author_id: %s\n", m.AuthorID))
	sb.WriteString(fmt.Sprintf("- author_username: %s\n", sanitizePromptHeaderField(m.AuthorName)))
	sb.WriteString(fmt.Sprintf("- author_global_name: %s\n", sanitizePromptHeaderField(m.Metadata.AuthorGlobalName)))
	sb.WriteString(fmt.Sprintf("- author_bot: %t\n", m.Metadata.AuthorBot))
	sb.WriteString(fmt.Sprintf("- is_admin: %t\n", m.Metadata.IsAdmin))
	if m.Metadata.VanadisChunkID != "" {
		if clean := sanitizePromptHeaderField(m.Metadata.VanadisChunkID); clean != "" {
			sb.WriteString(fmt.Sprintf("- vanadis_chunk_id: %s\n", clean))
		}
	}

	if m.Metadata.ReplyingToAuthor != "" || m.Metadata.ReplyingToContent != "" {
		sb.WriteString("- replying_to:\n")
		authorName := m.Metadata.ReplyingToAuthor
		if authorName == "" {
			authorName = "Unknown"
		}
		if !strings.HasPrefix(authorName, "@") && authorName != "Unknown" {
			authorName = "@" + authorName
		}
		sb.WriteString(fmt.Sprintf("    author: %q\n", authorName))
		cleanRefContent := sanitizer.SanitizePromptTags(m.Metadata.ReplyingToContent)
		sb.WriteString(fmt.Sprintf("    content: %q\n", cleanRefContent))
	}

	sanitizedContent := sanitizer.SanitizePromptTags(m.Content)
	sb.WriteString(fmt.Sprintf("- content: %s\n", sanitizedContent))
	ts := m.CreatedAt
	if ts.IsZero() {
		ts = time.Now()
	}
	if len(tzOpt) > 0 && strings.TrimSpace(tzOpt[0]) != "" {
		if loc, err := time.LoadLocation(strings.TrimSpace(tzOpt[0])); err == nil && loc != nil {
			ts = ts.In(loc)
		}
	}
	sb.WriteString(fmt.Sprintf("- timestamp: %s\n", ts.Format(time.RFC3339)))
	sb.WriteString(fmt.Sprintf("- mentions: %v\n", nonNilSlice(m.Metadata.Mentions)))
	sb.WriteString(fmt.Sprintf("- mention_user_ids: %v\n", nonNilSlice(m.Metadata.MentionUserIDs)))
	sb.WriteString(fmt.Sprintf("- mention_role_ids: %v\n", nonNilSlice(m.Metadata.MentionRoleIDs)))
	sb.WriteString(fmt.Sprintf("- attachments: %v\n\n", nonNilSlice(m.Metadata.Attachments)))

	sb.WriteString("Only formulate and output your final response once all immediate work is complete. It will be delivered directly to Discord.\n")
	sb.WriteString("</USER_REQUEST>")
	return sb.String()
}

func nonNilSlice(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// CoalesceBurstPrompt formats a burst of messages into a single coalesced multi-message prompt turn.
func CoalesceBurstPrompt(burst []db.Message, tzOpt ...string) string {
	if len(burst) == 0 {
		return ""
	}
	if len(burst) == 1 {
		if !burst[0].Metadata.IsEmpty() {
			return FormatSingleDiscordPrompt(burst[0], tzOpt...)
		}
		return burst[0].Content
	}

	var loc *time.Location
	if len(tzOpt) > 0 && strings.TrimSpace(tzOpt[0]) != "" {
		if l, err := time.LoadLocation(strings.TrimSpace(tzOpt[0])); err == nil {
			loc = l
		}
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
		ts := m.CreatedAt
		if ts.IsZero() {
			ts = time.Now()
		}
		if loc != nil {
			ts = ts.In(loc)
		}
		timeStr := ts.Format("15:04:05")
		body := sanitizer.SanitizePromptTags(m.BodyText())
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
		infos := fillBurstWakeInfos(len(input.Burst), 0, 1.0, threshold, "hook_wake_override", "hook_wake_trailing")
		return buildTriggeredBurstPlan(input.Burst, 0, infos)
	case "drop":
		return BurstPlan{
			WakeIndex:    -1,
			IsAllAmbient: true,
			WakeInfos:    fillBurstWakeInfos(len(input.Burst), -1, 0.0, threshold, "hook_drop_override", "hook_drop_override"),
		}
	}

	wakeInfos := make([]WakeInfo, len(input.Burst))
	wakeIdx := -1

	// Safeguard 1: Tier-1 Pre-Scan (Direct Mentions / Replies / Keywords)
	for i, m := range input.Burst {
		if isTier1Wake(m, input.BotUserID, input.BotRoleIDs) {
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
			if isTier1Wake(m, input.BotUserID, input.BotRoleIDs) {
				wakeInfos[i] = WakeInfo{IsWake: true, Score: 1.0, Threshold: threshold, Reason: "direct_address"}
			} else if wm == "mention" || threshold <= 0.0 || classifier.IsHeuristicSkip(extractMessageBody(m.Content)) {
				wakeInfos[i] = WakeInfo{IsWake: false, Score: 0.0, Threshold: threshold, Reason: "heuristic_skip"}
			} else {
				score, reason := classify([]db.Message{m})
				wakeInfos[i] = WakeInfo{IsWake: score >= threshold, Score: score, Threshold: threshold, Reason: reason}
			}
		}

		return buildTriggeredBurstPlan(input.Burst, wakeIdx, wakeInfos)
	}

	if wm == "mention" {
		return BurstPlan{
			WakeIndex:    -1,
			IsAllAmbient: true,
			WakeInfos:    fillBurstWakeInfos(len(input.Burst), -1, 0.0, 0.0, "mention_mode_ambient", "mention_mode_ambient"),
		}
	}

	if wm == "all" {
		infos := fillBurstWakeInfos(len(input.Burst), 0, 1.0, 0.0, "all_wake", "all_wake_trailing")
		return buildTriggeredBurstPlan(input.Burst, 0, infos)
	}

	// wm == "classifier"
	if threshold <= 0.0 {
		return BurstPlan{
			WakeIndex:    -1,
			IsAllAmbient: true,
			WakeInfos:    fillBurstWakeInfos(len(input.Burst), -1, 0.0, threshold, "classifier disabled", "classifier disabled"),
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
		return BurstPlan{
			WakeIndex:    -1,
			IsAllAmbient: true,
			WakeInfos:    fillBurstWakeInfos(len(input.Burst), -1, 0.0, threshold, "heuristic_skip", "heuristic_skip"),
		}
	}

	score, reason := classify(input.Burst)
	if score >= threshold {
		infos := fillBurstWakeInfos(len(input.Burst), 0, score, threshold, reason, "burst_ambient")
		return buildTriggeredBurstPlan(input.Burst, 0, infos)
	}

	return BurstPlan{
		WakeIndex:    -1,
		IsAllAmbient: true,
		WakeInfos:    fillBurstWakeInfos(len(input.Burst), -1, score, threshold, reason, reason),
	}
}

func fillBurstWakeInfos(n, wakeIdx int, score, threshold float64, wakeReason, otherReason string) []WakeInfo {
	infos := make([]WakeInfo, n)
	for i := range infos {
		if i == wakeIdx {
			infos[i] = WakeInfo{
				IsWake:    true,
				Score:     score,
				Threshold: threshold,
				Reason:    wakeReason,
			}
		} else {
			infos[i] = WakeInfo{
				IsWake:    false,
				Score:     score,
				Threshold: threshold,
				Reason:    otherReason,
			}
		}
	}
	return infos
}

func buildTriggeredBurstPlan(burst []db.Message, wakeIdx int, infos []WakeInfo) BurstPlan {
	plan := BurstPlan{
		WakeIndex:        wakeIdx,
		WakeInfos:        infos,
		ActiveMessage:    burst[wakeIdx],
		HasActiveMessage: true,
	}
	if wakeIdx > 0 {
		plan.LeadingAmbient = burst[:wakeIdx]
	}
	if wakeIdx+1 < len(burst) {
		plan.TrailingMessages = burst[wakeIdx+1:]
		plan.TrailingInfos = infos[wakeIdx+1:]
	}
	return plan
}

// TurnPromptInput collects pre-fetched context components for deterministic prompt assembly.
type TurnPromptInput struct {
	Burst               []db.Message
	ThreadSummary       string
	LookbackHistory     []HistoryMessage
	PreviousSessionID   string
	IsColdStart         bool
	ScopeInstructions   string
	ChannelInstructions string // Deprecated: Use ScopeInstructions
	InjectedHookContext string
	AmbientContext      string
	Timezone            string
}

// AssembleTurnPrompt deterministically composes the turn prompt in exact top-to-bottom document order.
func AssembleTurnPrompt(input TurnPromptInput) string {
	basePrompt := CoalesceBurstPrompt(input.Burst, input.Timezone)

	// Layer 6: Thread Summary (if present)
	if strings.TrimSpace(input.ThreadSummary) != "" {
		basePrompt = prependPromptLayer(basePrompt, wrapPromptTag(input.ThreadSummary, "<THREAD_SUMMARY>", "</THREAD_SUMMARY>"))
	}

	// Layer 5: Channel History Lookback (if present)
	if len(input.LookbackHistory) > 0 {
		if formattedHist := FormatChannelHistory(input.LookbackHistory, input.Timezone); formattedHist != "" {
			basePrompt = prependPromptLayer(basePrompt, formattedHist)
		}
	}

	// Layer 4: Previous Session ID (on cold start)
	if input.IsColdStart && strings.TrimSpace(input.PreviousSessionID) != "" {
		if prevBlock := FormatPreviousSession(input.PreviousSessionID); prevBlock != "" {
			basePrompt = prependPromptLayer(basePrompt, prevBlock)
		}
	}

	// Layer 2: Scope Instructions (if present)
	if inst := strings.TrimSpace(input.ScopeInstructions); inst != "" {
		if !strings.HasPrefix(inst, "<SCOPE_INSTRUCTIONS>") {
			inst = fmt.Sprintf("<SCOPE_INSTRUCTIONS>\nScope-specific guidelines for this conversation:\n\n%s\n</SCOPE_INSTRUCTIONS>", inst)
		}
		basePrompt = prependPromptLayer(basePrompt, inst)
	} else if inst := strings.TrimSpace(input.ChannelInstructions); inst != "" {
		if !strings.HasPrefix(inst, "<CHANNEL_INSTRUCTIONS>") && !strings.HasPrefix(inst, "<SCOPE_INSTRUCTIONS>") {
			inst = fmt.Sprintf("<CHANNEL_INSTRUCTIONS>\nChannel-specific guidelines for this conversation:\n\n%s\n</CHANNEL_INSTRUCTIONS>", inst)
		}
		basePrompt = prependPromptLayer(basePrompt, inst)
	}

	// Layer 1: Coordination Context Hook (if present)
	if strings.TrimSpace(input.InjectedHookContext) != "" {
		basePrompt = prependPromptLayer(basePrompt, wrapPromptTag(input.InjectedHookContext, "<COORDINATION_CONTEXT>", "</COORDINATION_CONTEXT>"))
	}

	// Layer 0: Ambient Context (if present)
	if strings.TrimSpace(input.AmbientContext) != "" {
		basePrompt = prependPromptLayer(basePrompt, wrapPromptTag(input.AmbientContext, "<ambient_context>", "</ambient_context>"))
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

func sanitizePromptHeaderField(s string) string {
	clean := strings.ReplaceAll(strings.ReplaceAll(s, "\n", " "), "\r", "")
	return strings.TrimSpace(sanitizer.SanitizePromptTags(clean))
}

func prependPromptLayer(base, layer string) string {
	if base != "" {
		return layer + "\n\n" + base
	}
	return layer
}

func wrapPromptTag(text, openTag, closeTag string) string {
	trimmed := strings.TrimSpace(text)
	if !strings.HasPrefix(trimmed, openTag) {
		return fmt.Sprintf("%s\n%s\n%s", openTag, trimmed, closeTag)
	}
	return trimmed
}
