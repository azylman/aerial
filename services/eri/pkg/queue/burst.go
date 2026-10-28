package queue

import (
	"fmt"
	"log"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/azylman/aerial/pkg/sanitizer"
	"github.com/azylman/aerial/services/eri/pkg/classifier"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

func (p *WorkerPool) Enqueue(msg db.Message) { p.EnqueueBurst(msg) }

func resolveMessageEffort(m db.Message) string {
	effort := strings.ToLower(strings.TrimSpace(m.Effort))
	switch effort {
	case "medium", "low", "high":
		return effort
	}
	if effort != "" {
		return "high"
	}
	model := strings.ToLower(strings.TrimSpace(m.Model))
	if strings.Contains(model, "medium") {
		return "medium"
	}
	if strings.Contains(model, "flash") {
		return "low"
	}
	return "high"
}

func (p *WorkerPool) resolveScopeKey(m db.Message) string {
	baseKey := db.DiscordSessionID(m.ThreadID)
	k, err := ResolvePoolKey(m.Ephemeral, resolveMessageEffort(m))
	if err == nil && p != nil && p.Pool(k) != nil {
		switch k {
		case PoolKey{Ephemeral: true, Effort: "low"}:
			return "low_effort:" + baseKey
		case PoolKey{Ephemeral: true, Effort: "medium"}:
			return "medium_effort:" + baseKey
		case PoolKey{Ephemeral: false, Effort: "low"}:
			return "stateful_low:" + baseKey
		case PoolKey{Ephemeral: false, Effort: "medium"}:
			return "stateful_medium:" + baseKey
		}
	}
	return baseKey
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
			state = &threadWorkerState{ch: make(chan queueItem, 100)}
			p.threadChs[scopeKey] = state
			p.wg.Add(1)
			newWorkers = append(newWorkers, newWorker{scopeKey: scopeKey, state: state})
		}

		mCopy := msg
		item := queueItem{msg: &mCopy}
		select {
		case state.ch <- item:
			metrics.QueueDepth.Inc()
		default:
			state.activeEnqueuers++
			ch := state.ch
			p.mu.Unlock()
			select {
			case ch <- item:
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

// EnqueueDispatch atomically enqueues a headless dispatch run into the queue.
func (p *WorkerPool) EnqueueDispatch(d db.Dispatch) {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		log.Printf("[WorkerPool] Warning: attempted to enqueue dispatch %s to stopped pool", d.ID)
		return
	}
	scopeKey := strings.TrimSpace(d.SessionID)
	if scopeKey == "" {
		scopeKey = "dispatch-" + d.ID
	}
	state, exists := p.threadChs[scopeKey]
	type newWorker struct {
		scopeKey string
		state    *threadWorkerState
	}
	var nw *newWorker
	if !exists {
		state = &threadWorkerState{ch: make(chan queueItem, 100)}
		p.threadChs[scopeKey] = state
		p.wg.Add(1)
		nw = &newWorker{scopeKey: scopeKey, state: state}
	}

	dCopy := d
	item := queueItem{dispatch: &dCopy}
	select {
	case state.ch <- item:
		metrics.QueueDepth.Inc()
	default:
		state.activeEnqueuers++
		ch := state.ch
		p.mu.Unlock()
		select {
		case ch <- item:
			metrics.QueueDepth.Inc()
		case <-p.ctx.Done():
			log.Printf("[WorkerPool] Context cancelled while enqueuing dispatch %s", d.ID)
		}
		p.mu.Lock()
		state.activeEnqueuers--
	}
	p.mu.Unlock()

	if nw != nil {
		go p.runThreadWorker(nw.scopeKey, nw.state)
	}
}

func (p *WorkerPool) runThreadWorker(scopeKey string, state *threadWorkerState) {
	defer p.wg.Done()

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

	var heldItem *queueItem

	for {
		var item queueItem
		if heldItem != nil {
			item = *heldItem
			heldItem = nil
		} else {
			select {
			case <-p.ctx.Done():
				return
			case popped, ok := <-state.ch:
				if !ok {
					return
				}
				metrics.QueueDepth.Dec()
				item = popped
			case <-idleTimer.C:
				p.mu.Lock()
				if len(state.ch) == 0 && heldItem == nil && state.activeEnqueuers == 0 {
					delete(p.threadChs, scopeKey)
					p.scopeLocks.Delete(scopeKey)
					p.mu.Unlock()
					return
				}
				p.mu.Unlock()
				idleTimer.Reset(idleTimeout)
				continue
			}
		}

		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
		idleTimer.Reset(idleTimeout)

		if item.dispatch != nil {
			p.executeWorkerDispatch(state, item.dispatch, scopeKey)
		} else if item.msg != nil {
			heldItem = p.executeWorkerBurst(state, *item.msg, scopeKey)
		}

		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
		idleTimer.Reset(idleTimeout)
	}
}

func (p *WorkerPool) executeWorkerDispatch(state *threadWorkerState, d *db.Dispatch, scopeKey string) {
	p.mu.Lock()
	state.inFlight = true
	p.mu.Unlock()

	p.processDispatch(*d, scopeKey)

	p.mu.Lock()
	state.inFlight = false
	p.mu.Unlock()
}

func drainBurstMessages(state *threadWorkerState, initial db.Message) ([]db.Message, *queueItem) {
	burst := []db.Message{initial}
	for len(burst) < 5 {
		select {
		case extra := <-state.ch:
			metrics.QueueDepth.Dec()
			if extra.dispatch != nil {
				extraCopy := extra
				return burst, &extraCopy
			}
			if extra.msg != nil {
				burst = append(burst, *extra.msg)
			}
		default:
			return burst, nil
		}
	}
	return burst, nil
}

func (p *WorkerPool) executeWorkerBurst(state *threadWorkerState, initial db.Message, scopeKey string) *queueItem {
	p.mu.Lock()
	state.inFlight = true
	burst, heldItem := drainBurstMessages(state, initial)
	p.mu.Unlock()

	p.processBurst(burst, scopeKey)

	p.mu.Lock()
	state.inFlight = false
	p.mu.Unlock()

	return heldItem
}

// FormatSingleDiscordPrompt formats a single structured Discord message into the canonical <USER_REQUEST> prompt envelope JIT.
func FormatSingleDiscordPrompt(m db.Message, tzOpt ...string) string {
	var sb strings.Builder
	isDiscord := isDiscordMessage(m)
	if isDiscord {
		sb.WriteString("<USER_REQUEST>\nHere's a message someone sent you from Discord:\n\n")
	} else {
		sb.WriteString("<USER_REQUEST>\nHere's an internal system or scheduled prompt:\n\n")
	}
	channelID := m.ChannelID
	if channelID == "" {
		channelID = m.ThreadID
	}
	fmt.Fprintf(&sb, "- id: %s\n- channel_id: %s\n", m.ID, channelID)
	if clean := sanitizePromptHeaderField(m.ChannelName); clean != "" {
		if !strings.HasPrefix(clean, "#") {
			clean = "#" + clean
		}
		fmt.Fprintf(&sb, "- channel_name: %q\n", clean)
	}
	fmt.Fprintf(&sb, "- thread_id: %s\n", m.ThreadID)
	if clean := sanitizePromptHeaderField(m.ThreadName); clean != "" {
		fmt.Fprintf(&sb, "- thread_name: %q\n", clean)
	}
	if strings.TrimSpace(m.GuildID) != "" {
		fmt.Fprintf(&sb, "- guild_id: %s\n", m.GuildID)
	}
	fmt.Fprintf(&sb, "- author_id: %s\n- author_username: %s\n", m.AuthorID, sanitizePromptHeaderField(m.AuthorName))
	if cleanGlobal := sanitizePromptHeaderField(m.AuthorGlobalName); cleanGlobal != "" {
		fmt.Fprintf(&sb, "- author_global_name: %s\n", cleanGlobal)
	}
	fmt.Fprintf(&sb, "- author_bot: %t\n- is_admin: %t\n", m.AuthorBot, m.IsAdmin || !isDiscord)
	writeDiscordPromptReply(&sb, m)
	fmt.Fprintf(&sb, "- content: %s\n", sanitizer.SanitizePromptTags(m.Content))
	ts := resolvePromptTimestamp(m.CreatedAt, tzOpt...)
	closingInstruction := "It will be delivered directly to Discord. Keep responses proportional to the turn; for routine actions or things just discussed, confirm briefly without echoing tool IDs, exact timestamps, or context already in thread."
	if m.Silent {
		closingInstruction = "This is a silent background turn; your response will not be delivered to Discord."
	}
	fmt.Fprintf(&sb, "- timestamp: %s\n", ts.Format(time.RFC3339))
	writeDiscordPromptSlices(&sb, m)
	fmt.Fprintf(&sb, "\nOnly formulate and output your final response once all immediate work is complete. %s\n</USER_REQUEST>", closingInstruction)
	return sb.String()
}

func writeDiscordPromptReply(sb *strings.Builder, m db.Message) {
	if m.ReplyingToAuthor == "" && m.ReplyingToContent == "" {
		return
	}
	authorName := m.ReplyingToAuthor
	if authorName == "" {
		authorName = "Unknown"
	} else if !strings.HasPrefix(authorName, "@") && authorName != "Unknown" {
		authorName = "@" + authorName
	}
	fmt.Fprintf(sb, "- replying_to:\n    author: %q\n    content: %q\n", authorName, sanitizer.SanitizePromptTags(m.ReplyingToContent))
}

func resolvePromptTimestamp(ts time.Time, tzOpt ...string) time.Time {
	if ts.IsZero() {
		ts = time.Now()
	}
	if len(tzOpt) > 0 && strings.TrimSpace(tzOpt[0]) != "" {
		if loc, err := time.LoadLocation(strings.TrimSpace(tzOpt[0])); err == nil && loc != nil {
			ts = ts.In(loc)
		}
	}
	return ts
}

func writeDiscordPromptSlices(sb *strings.Builder, m db.Message) {
	for _, kv := range []struct {
		key  string
		vals []string
	}{
		{"mentions", m.Mentions},
		{"mention_user_ids", m.MentionUserIDs},
		{"mention_role_ids", m.MentionRoleIDs},
		{"attachments", m.Attachments},
	} {
		if len(kv.vals) > 0 {
			fmt.Fprintf(sb, "- %s: %v\n", kv.key, kv.vals)
		}
	}
}

func extractBurstEphemeral(burst []db.Message) (bool, bool) {
	for _, m := range burst {
		if m.Ephemeral != nil {
			return *m.Ephemeral, true
		}
	}
	return false, false
}

func extractBurstModel(burst []db.Message) string {
	for _, m := range burst {
		if model := strings.TrimSpace(m.Model); model != "" {
			return model
		}
	}
	return ""
}

func extractBurstSilent(burst []db.Message) bool {
	if len(burst) == 0 {
		return false
	}
	for _, m := range burst {
		if !m.Silent {
			return false
		}
	}
	return true
}

func resolveBurstEffort(burst []db.Message) string {
	hasLow := false
	for _, m := range burst {
		switch resolveMessageEffort(m) {
		case "medium":
			return "medium"
		case "low":
			hasLow = true
		}
	}
	if hasLow {
		return "low"
	}
	return "high"
}

// CoalesceBurstPrompt formats a burst of messages into a single coalesced multi-message prompt turn.
func CoalesceBurstPrompt(burst []db.Message, tzOpt ...string) string {
	if len(burst) == 0 {
		return ""
	}
	if len(burst) == 1 {
		if burst[0].HasStructuredFields() {
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
		fmt.Fprintf(&sb, "--- Message %d (by %s at %s) ---\n%s\n\n", i+1, author, ts.Format("15:04:05"), sanitizer.SanitizePromptTags(m.BodyText()))
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

func normalizeWakeMode(raw string) string {
	wm := strings.ToLower(strings.TrimSpace(raw))
	switch wm {
	case "mentions", "direct":
		return "mention"
	case "always":
		return "all"
	case "ambient", "":
		return "classifier"
	default:
		return wm
	}
}

func isAllBurstHeuristicSkip(burst []db.Message) bool {
	for _, m := range burst {
		if !classifier.IsHeuristicSkip(db.ExtractMessageBody(m.Content)) {
			return false
		}
	}
	return true
}

func populateTier1TrailingWakeInfos(burst []db.Message, wakeIdx int, wakeInfos []WakeInfo, wm string, threshold float64, botUserID string, botRoleIDs []string, classify func([]db.Message) (float64, string)) {
	for i := 0; i < wakeIdx; i++ {
		wakeInfos[i] = WakeInfo{IsWake: false, Score: 0.0, Threshold: threshold, Reason: "burst_prescan_leading"}
	}
	for i := wakeIdx + 1; i < len(burst); i++ {
		m := burst[i]
		if isTier1Wake(m, botUserID, botRoleIDs) {
			wakeInfos[i] = WakeInfo{IsWake: true, Score: 1.0, Threshold: threshold, Reason: "direct_address"}
		} else if wm == "mention" || threshold <= 0.0 || classifier.IsHeuristicSkip(db.ExtractMessageBody(m.Content)) {
			wakeInfos[i] = WakeInfo{IsWake: false, Score: 0.0, Threshold: threshold, Reason: "heuristic_skip"}
		} else {
			score, reason := classify([]db.Message{m})
			wakeInfos[i] = WakeInfo{IsWake: score >= threshold, Score: score, Threshold: threshold, Reason: reason}
		}
	}
}

// PlanBurstExecution deterministically partitions an incoming burst into leading ambient messages,
// the active wake turn message, and trailing messages based on channel policy and wake criteria.
func PlanBurstExecution(input BurstPlanInput) BurstPlan {
	if len(input.Burst) == 0 {
		return BurstPlan{WakeIndex: -1}
	}

	wm := normalizeWakeMode(input.WakeMode)
	classify := input.ClassifierFn
	if classify == nil {
		classify = func([]db.Message) (float64, string) { return 0.0, "no classifier configured" }
	}

	threshold := input.Threshold
	switch strings.ToLower(strings.TrimSpace(input.HookOverride)) {
	case "wake":
		return buildTriggeredBurstPlan(input.Burst, 0, fillBurstWakeInfos(len(input.Burst), 0, 1.0, threshold, "hook_wake_override", "hook_wake_trailing"))
	case "drop":
		return BurstPlan{WakeIndex: -1, IsAllAmbient: true, WakeInfos: fillBurstWakeInfos(len(input.Burst), -1, 0.0, threshold, "hook_drop_override", "hook_drop_override")}
	}

	wakeInfos := make([]WakeInfo, len(input.Burst))
	wakeIdx := -1

	for i, m := range input.Burst {
		if isTier1Wake(m, input.BotUserID, input.BotRoleIDs) {
			wakeInfos[i] = WakeInfo{IsWake: true, Score: 1.0, Threshold: threshold, Reason: "direct_address"}
			if wakeIdx == -1 {
				wakeIdx = i
			}
		}
	}

	if wakeIdx != -1 {
		populateTier1TrailingWakeInfos(input.Burst, wakeIdx, wakeInfos, wm, threshold, input.BotUserID, input.BotRoleIDs, classify)
		return buildTriggeredBurstPlan(input.Burst, wakeIdx, wakeInfos)
	}

	if wm == "mention" {
		return BurstPlan{WakeIndex: -1, IsAllAmbient: true, WakeInfos: fillBurstWakeInfos(len(input.Burst), -1, 0.0, 0.0, "mention_mode_ambient", "mention_mode_ambient")}
	}
	if wm == "all" {
		return buildTriggeredBurstPlan(input.Burst, 0, fillBurstWakeInfos(len(input.Burst), 0, 1.0, 0.0, "all_wake", "all_wake_trailing"))
	}
	if threshold <= 0.0 {
		return BurstPlan{WakeIndex: -1, IsAllAmbient: true, WakeInfos: fillBurstWakeInfos(len(input.Burst), -1, 0.0, threshold, "classifier disabled", "classifier disabled")}
	}

	if isAllBurstHeuristicSkip(input.Burst) {
		return BurstPlan{WakeIndex: -1, IsAllAmbient: true, WakeInfos: fillBurstWakeInfos(len(input.Burst), -1, 0.0, threshold, "heuristic_skip", "heuristic_skip")}
	}

	score, reason := classify(input.Burst)
	if score >= threshold {
		return buildTriggeredBurstPlan(input.Burst, 0, fillBurstWakeInfos(len(input.Burst), 0, score, threshold, reason, "burst_ambient"))
	}
	return BurstPlan{WakeIndex: -1, IsAllAmbient: true, WakeInfos: fillBurstWakeInfos(len(input.Burst), -1, score, threshold, reason, reason)}
}

func fillBurstWakeInfos(n, wakeIdx int, score, threshold float64, wakeReason, otherReason string) []WakeInfo {
	infos := make([]WakeInfo, n)
	for i := range infos {
		if i == wakeIdx {
			infos[i] = WakeInfo{IsWake: true, Score: score, Threshold: threshold, Reason: wakeReason}
		} else {
			infos[i] = WakeInfo{IsWake: false, Score: score, Threshold: threshold, Reason: otherReason}
		}
	}
	return infos
}

func buildTriggeredBurstPlan(burst []db.Message, wakeIdx int, infos []WakeInfo) BurstPlan {
	plan := BurstPlan{WakeIndex: wakeIdx, WakeInfos: infos, ActiveMessage: burst[wakeIdx], HasActiveMessage: true}
	if wakeIdx > 0 {
		plan.LeadingAmbient = burst[:wakeIdx]
	}
	if wakeIdx+1 < len(burst) {
		plan.TrailingMessages, plan.TrailingInfos = burst[wakeIdx+1:], infos[wakeIdx+1:]
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
	if strings.TrimSpace(input.ThreadSummary) != "" {
		basePrompt = prependPromptLayer(basePrompt, wrapPromptTag(input.ThreadSummary, "<THREAD_SUMMARY>", "</THREAD_SUMMARY>"))
	}
	if len(input.LookbackHistory) > 0 {
		if formattedHist := FormatChannelHistory(input.LookbackHistory, input.Timezone); formattedHist != "" {
			basePrompt = prependPromptLayer(basePrompt, formattedHist)
		}
	}
	if input.IsColdStart && strings.TrimSpace(input.PreviousSessionID) != "" {
		if prevBlock := FormatPreviousSession(input.PreviousSessionID); prevBlock != "" {
			basePrompt = prependPromptLayer(basePrompt, prevBlock)
		}
	}
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
	if strings.TrimSpace(input.InjectedHookContext) != "" {
		basePrompt = prependPromptLayer(basePrompt, wrapPromptTag(input.InjectedHookContext, "<COORDINATION_CONTEXT>", "</COORDINATION_CONTEXT>"))
	}
	if strings.TrimSpace(input.AmbientContext) != "" {
		basePrompt = prependPromptLayer(basePrompt, wrapPromptTag(input.AmbientContext, "<ambient_context>", "</ambient_context>"))
	}
	return strings.TrimSpace(basePrompt)
}

// isDiscordMessage returns true if the message originated from Discord rather than HTTP (/prompt) or the scheduler.
func isDiscordMessage(m db.Message) bool {
	return m.AuthorID != "http-client" && m.AuthorID != "scheduler" && m.ScheduleRunID == ""
}

func isDiscordBurst(burst []db.Message) bool {
	for _, m := range burst {
		if !isDiscordMessage(m) {
			return false
		}
	}
	return len(burst) > 0
}

// EvaluateBurstStaleness evaluates staleness atomically across an entire message burst.
func EvaluateBurstStaleness(burst []db.Message, stalenessTTL time.Duration, isColdThread bool, lastSessionActivity, now time.Time) (bool, string) {
	if !isDiscordBurst(burst) {
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
		if m.RetryCount > 0 || m.RestartCount > 0 || m.ErrorMessage == "deployment_drain" {
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

func resolveBurstPoolKey(burst []db.Message) (PoolKey, error) {
	var ephemPtr *bool
	if isEphem, explicit := extractBurstEphemeral(burst); explicit {
		ephemPtr = &isEphem
	}
	return ResolvePoolKey(ephemPtr, resolveBurstEffort(burst))
}

func isBurstEphemeral(burst []db.Message) bool {
	k, err := resolveBurstPoolKey(burst)
	return err != nil || k.Ephemeral
}

func resolveDispatchEffort(d db.Dispatch, isEphem bool) string {
	eff := strings.ToLower(strings.TrimSpace(d.Effort))
	if eff == "" {
		model := strings.ToLower(strings.TrimSpace(d.Model))
		if strings.Contains(model, "medium") {
			eff = "medium"
		} else if strings.Contains(model, "flash") {
			eff = "low"
		} else if strings.Contains(model, "pro") {
			eff = "high"
		} else if isEphem {
			eff = "medium"
		} else {
			eff = "low"
		}
	}
	if isEphem && eff == "high" {
		eff = "medium"
	}
	switch eff {
	case "low", "medium", "high":
		return eff
	default:
		if isEphem {
			return "medium"
		}
		return "low"
	}
}

func (te *turnExecution) resolveExecutionPool() (runner.AgentPool, PoolKey, error) {
	var k PoolKey
	var err error
	if te != nil && te.dispatch != nil {
		isEphem := strings.TrimSpace(te.dispatch.SessionID) == ""
		eff := resolveDispatchEffort(*te.dispatch, isEphem)
		k, err = ResolvePoolKey(&isEphem, eff)
	} else {
		var burst []db.Message
		if te != nil {
			burst = te.burst
		}
		k, err = resolveBurstPoolKey(burst)
	}
	if err != nil {
		return nil, k, err
	}
	if te == nil || te.pool == nil {
		return nil, k, nil
	}
	if pool := te.pool.Pool(k); pool != nil {
		return pool, k, nil
	}
	log.Printf("[Worker] Notice: pool for %+v is nil, falling back to primary processPool for thread %s", k, te.threadID)
	return te.pool.Pool(PoolKey{Ephemeral: false, Effort: "high"}), k, nil
}

func getStreamingDaemon(pool runner.AgentPool, threadID string) *runner.StreamingDaemon {
	if pool == nil || threadID == "" {
		return nil
	}
	if dPool, ok := pool.(interface {
		Get(string) (*runner.StreamingDaemon, bool)
	}); ok {
		if d, ok := dPool.Get(threadID); ok && d != nil {
			return d
		}
		if norm := db.DiscordSessionID(threadID); norm != threadID {
			if d, ok := dPool.Get(norm); ok && d != nil {
				return d
			}
		} else if trimmed := strings.TrimPrefix(threadID, "discord-"); trimmed != threadID && trimmed != "" {
			if d, ok := dPool.Get(trimmed); ok && d != nil {
				return d
			}
		}
	}
	return nil
}
