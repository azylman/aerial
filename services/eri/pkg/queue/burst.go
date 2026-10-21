package queue

import (
	"fmt"
	"log"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/azylman/aerial/services/eri/pkg/classifier"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/sanitizer"
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
				for _, pool := range []runner.AgentPool{p.processPool, p.lowEffortProcessPool, p.mediumEffortProcessPool} {
					if d := getStreamingDaemon(pool, threadID); d != nil && d.TaskTracker().ActiveCount() > 0 {
						hasActiveTasks = true
						break
					}
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
	channelID := m.Metadata.ChannelID
	if channelID == "" {
		channelID = m.ThreadID
	}
	fmt.Fprintf(&sb, "- id: %s\n- channel_id: %s\n", m.ID, channelID)
	if m.Metadata.ChannelName != "" {
		if clean := sanitizePromptHeaderField(m.Metadata.ChannelName); clean != "" {
			if !strings.HasPrefix(clean, "#") {
				clean = "#" + clean
			}
			fmt.Fprintf(&sb, "- channel_name: %q\n", clean)
		}
	}
	fmt.Fprintf(&sb, "- thread_id: %s\n", m.ThreadID)
	if m.Metadata.ThreadName != "" {
		if clean := sanitizePromptHeaderField(m.Metadata.ThreadName); clean != "" {
			fmt.Fprintf(&sb, "- thread_name: %q\n", clean)
		}
	}
	fmt.Fprintf(&sb, "- guild_id: %s\n- author_id: %s\n- author_username: %s\n- author_global_name: %s\n- author_bot: %t\n- is_admin: %t\n",
		m.GuildID, m.AuthorID, sanitizePromptHeaderField(m.AuthorName),
		sanitizePromptHeaderField(m.Metadata.AuthorGlobalName), m.Metadata.AuthorBot, m.Metadata.IsAdmin)
	if m.Metadata.VanadisChunkID != "" {
		if clean := sanitizePromptHeaderField(m.Metadata.VanadisChunkID); clean != "" {
			fmt.Fprintf(&sb, "- vanadis_chunk_id: %s\n", clean)
		}
	}
	if m.Metadata.ReplyingToAuthor != "" || m.Metadata.ReplyingToContent != "" {
		authorName := m.Metadata.ReplyingToAuthor
		if authorName == "" {
			authorName = "Unknown"
		}
		if !strings.HasPrefix(authorName, "@") && authorName != "Unknown" {
			authorName = "@" + authorName
		}
		fmt.Fprintf(&sb, "- replying_to:\n    author: %q\n    content: %q\n", authorName, sanitizer.SanitizePromptTags(m.Metadata.ReplyingToContent))
	}
	fmt.Fprintf(&sb, "- content: %s\n", sanitizer.SanitizePromptTags(m.Content))
	ts := m.CreatedAt
	if ts.IsZero() {
		ts = time.Now()
	}
	if len(tzOpt) > 0 && strings.TrimSpace(tzOpt[0]) != "" {
		if loc, err := time.LoadLocation(strings.TrimSpace(tzOpt[0])); err == nil && loc != nil {
			ts = ts.In(loc)
		}
	}
	fmt.Fprintf(&sb, "- timestamp: %s\n- mentions: %v\n- mention_user_ids: %v\n- mention_role_ids: %v\n- attachments: %v\n\nOnly formulate and output your final response once all immediate work is complete. It will be delivered directly to Discord.\n</USER_REQUEST>",
		ts.Format(time.RFC3339), nonNilSlice(m.Metadata.Mentions), nonNilSlice(m.Metadata.MentionUserIDs), nonNilSlice(m.Metadata.MentionRoleIDs), nonNilSlice(m.Metadata.Attachments))
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
		for i := 0; i < wakeIdx; i++ {
			wakeInfos[i] = WakeInfo{IsWake: false, Score: 0.0, Threshold: threshold, Reason: "burst_prescan_leading"}
		}
		for i := wakeIdx + 1; i < len(input.Burst); i++ {
			m := input.Burst[i]
			if isTier1Wake(m, input.BotUserID, input.BotRoleIDs) {
				wakeInfos[i] = WakeInfo{IsWake: true, Score: 1.0, Threshold: threshold, Reason: "direct_address"}
			} else if wm == "mention" || threshold <= 0.0 || classifier.IsHeuristicSkip(db.ExtractMessageBody(m.Content)) {
				wakeInfos[i] = WakeInfo{IsWake: false, Score: 0.0, Threshold: threshold, Reason: "heuristic_skip"}
			} else {
				score, reason := classify([]db.Message{m})
				wakeInfos[i] = WakeInfo{IsWake: score >= threshold, Score: score, Threshold: threshold, Reason: reason}
			}
		}
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

	allSkip := true
	for _, m := range input.Burst {
		if !classifier.IsHeuristicSkip(db.ExtractMessageBody(m.Content)) {
			allSkip = false
			break
		}
	}
	if allSkip {
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

// EvaluateBurstStaleness evaluates staleness atomically across an entire message burst.
func EvaluateBurstStaleness(burst []db.Message, stalenessTTL time.Duration, isColdThread bool, lastSessionActivity, now time.Time) (bool, string) {
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
