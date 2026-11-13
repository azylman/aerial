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

// recordStreamUsage keeps the latest usage reported for each assistant message of the turn.
// Claude repeats a message's usage on every content block it streams, so keying by message ID
// counts each model call once.
func (d *StreamingDaemon) recordStreamUsage(turn *TurnContext, msg map[string]any) {
	usageObj, ok := msg["usage"].(map[string]any)
	id := extractFirstString(msg, "id")
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
	turn.streamUsage[id] = usage
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
