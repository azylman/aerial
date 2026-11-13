package runner

import (
	"iter"
	"maps"
)

func populateUsage(res *TurnResult, raw map[string]any) {
	if res == nil || raw == nil {
		return
	}
	var usageObj map[string]any
	if u, ok := raw["usage"].(map[string]any); ok {
		usageObj = u
	}
	if resObj, ok := raw["result"].(map[string]any); ok {
		if u, ok := resObj["usage"].(map[string]any); ok {
			usageObj = u
		}
	}
	if usageObj == nil {
		return
	}
	res.Usage = usageFromObject(usageObj)
}

// usageFromObject maps an engine usage object to AgyUsage. Claude reports
// cache_creation_input_tokens (uncached input written to cache) separately: it is added to
// InputTokens, cache_read_input_tokens is assigned to CacheReadTokens without inflating
// InputTokens, and TotalTokens defaults to InputTokens + OutputTokens when total_tokens is omitted.
func usageFromObject(usageObj map[string]any) AgyUsage {
	var u AgyUsage
	getInt := func(k string, dst *int) {
		if v, ok := usageObj[k].(float64); ok {
			*dst = int(v)
		}
	}
	getInt("input_tokens", &u.InputTokens)
	getInt("output_tokens", &u.OutputTokens)
	getInt("thinking_tokens", &u.ThinkingTokens)
	getInt("cache_read_tokens", &u.CacheReadTokens)
	getInt("total_tokens", &u.TotalTokens)

	var cacheCreation, cacheRead int
	getInt("cache_creation_input_tokens", &cacheCreation)
	getInt("cache_read_input_tokens", &cacheRead)
	if cacheCreation > 0 {
		u.InputTokens += cacheCreation
		u.CacheCreationTokens = cacheCreation
	}
	if cacheRead > 0 {
		u.CacheReadTokens = cacheRead
	}
	if u.TotalTokens == 0 && (u.InputTokens > 0 || u.OutputTokens > 0) {
		u.TotalTokens = u.InputTokens + u.OutputTokens
	}
	return u
}

// sumUsage adds token usages field by field.
func sumUsage(usages iter.Seq[AgyUsage]) AgyUsage {
	var total AgyUsage
	for u := range usages {
		total.InputTokens += u.InputTokens
		total.OutputTokens += u.OutputTokens
		total.ThinkingTokens += u.ThinkingTokens
		total.CacheReadTokens += u.CacheReadTokens
		total.CacheCreationTokens += u.CacheCreationTokens
		total.TotalTokens += u.TotalTokens
	}
	return total
}

// recordStreamUsage merges the usage reported for one assistant message (msg carries "id" and
// "usage") into the turn. Claude repeats a message's usage on every content block it streams with
// the output count frozen at message_start; only message_delta carries the final output count. So
// usage is keyed by message ID (each model call counts once) and merged field by field keeping the
// largest value.
func (d *StreamingDaemon) recordStreamUsage(turn *TurnContext, msg map[string]any) {
	d.mergeStreamUsage(turn, extractFirstString(msg, "id"), msg)
}

// recordStreamEventUsage tracks usage from Claude stream_event messages: message_start names the
// message being streamed (per parent tool call, since subagent streams interleave) and message_delta
// reports its final usage without repeating the ID.
func (d *StreamingDaemon) recordStreamEventUsage(turn *TurnContext, parentToolUseID string, ev map[string]any) {
	switch extractFirstString(ev, "type") {
	case "message_start":
		msg, ok := ev["message"].(map[string]any)
		if !ok {
			return
		}
		id := extractFirstString(msg, "id")
		if id == "" {
			return
		}
		d.inflightMu.Lock()
		if turn.streamMessageByParent == nil {
			turn.streamMessageByParent = make(map[string]string)
		}
		turn.streamMessageByParent[parentToolUseID] = id
		d.inflightMu.Unlock()
		d.mergeStreamUsage(turn, id, msg)
	case "message_delta":
		d.inflightMu.Lock()
		id := turn.streamMessageByParent[parentToolUseID]
		d.inflightMu.Unlock()
		d.mergeStreamUsage(turn, id, ev)
	}
}

func (d *StreamingDaemon) mergeStreamUsage(turn *TurnContext, id string, holder map[string]any) {
	usageObj, ok := holder["usage"].(map[string]any)
	if !ok || id == "" {
		return
	}
	usage := usageFromObject(usageObj)
	if usage == (AgyUsage{}) {
		return
	}
	d.inflightMu.Lock()
	defer d.inflightMu.Unlock()
	if turn.streamUsage == nil {
		turn.streamUsage = make(map[string]AgyUsage)
	}
	turn.streamUsage[id] = maxUsage(turn.streamUsage[id], usage)
}

// maxUsage merges two usage reports for the same model call, keeping the largest value per field.
func maxUsage(a, b AgyUsage) AgyUsage {
	m := AgyUsage{
		InputTokens:         max(a.InputTokens, b.InputTokens),
		OutputTokens:        max(a.OutputTokens, b.OutputTokens),
		ThinkingTokens:      max(a.ThinkingTokens, b.ThinkingTokens),
		CacheReadTokens:     max(a.CacheReadTokens, b.CacheReadTokens),
		CacheCreationTokens: max(a.CacheCreationTokens, b.CacheCreationTokens),
	}
	m.TotalTokens = max(a.TotalTokens, b.TotalTokens, m.InputTokens+m.OutputTokens)
	return m
}

// streamUsageTotal returns the usage of every model call streamed so far in turn.
func (d *StreamingDaemon) streamUsageTotal(turn *TurnContext) AgyUsage {
	d.inflightMu.Lock()
	defer d.inflightMu.Unlock()
	return sumUsage(maps.Values(turn.streamUsage))
}

// fillStreamUsage falls back to the turn's streamed usage when the result carries none (a
// result rescued from the transcript, or an error result).
func (d *StreamingDaemon) fillStreamUsage(turn *TurnContext, res *TurnResult) {
	if res.Usage == (AgyUsage{}) {
		res.Usage = d.streamUsageTotal(turn)
	}
}

// failTurn delivers err to the turn's sink, carrying the tokens the turn already consumed.
func (d *StreamingDaemon) failTurn(turn *TurnContext, err error) {
	if usage := d.streamUsageTotal(turn); usage != (AgyUsage{}) {
		err = &TurnError{Err: err, Usage: usage}
	}
	turn.Sink.OnError(err)
}
