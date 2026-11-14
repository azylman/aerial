package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/metrics"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

// TurnSink receives streaming notifications and results during an active turn.
type TurnSink interface {
	OnTurnStarted()
	OnThinking()
	OnToolCall(call ToolCall)
	OnToolCompleted(call ToolCall, mcpServer string, duration time.Duration, status string)
	OnSkillActivated(skillName, source string)
	OnTextDelta(delta string)
	OnResult(res *TurnResult)
	OnError(err error)
}

// ToolCall identifies one tool invocation within a turn so sinks can correlate a call's
// start with its completion, including concurrent parallel calls of the same tool.
type ToolCall struct {
	// ID is the engine-supplied call ID (Claude tool_use id, agy step index). Empty when the
	// engine supplies none; sinks then correlate FIFO per tool name.
	ID string
	// ParentID is the ID of the enclosing subagent call when this call runs inside one.
	ParentID string
	// Name is the tool name. Completions carry the canonical (MCP-resolved) name.
	Name string
	// Command is a human-readable detail for status displays. It may echo tool arguments,
	// so it must never be attached to spans or logs.
	Command string
	// Subagent marks calls that spawn a subagent (Claude Agent/Task, agy invoke_subagent).
	Subagent bool
	// MCPServer and MCPTool name the MCP server and tool an MCP call targets (agy's
	// call_mcp_tool keeps Name "call_mcp_tool"). Both are empty for non-MCP tools.
	MCPServer string
	MCPTool   string
}

// StepAwareSink is an optional interface that TurnSinks can implement
// to receive step boundary notifications for intermediate delta purging.
type StepAwareSink interface {
	OnStepStarted(stepIndex int)
}

// TurnContext encapsulates the metadata, prompt, structured context, and callback sink for a single turn.
type TurnContext struct {
	TurnID              string
	SessionID           string
	Model               string
	Prompt              string
	MemoryQuery         string
	ScopeInstructions   string
	ChannelInstructions string // Deprecated: Use ScopeInstructions
	CoordinationContext string
	ThreadSummary       string
	TargetID            string
	VanadisChunkID      string
	Sink                TurnSink
	CreatedAt           time.Time
	Ctx                 context.Context
	lastStepIndex       int
	hasSeenStep         bool
	streamUsage         map[string]AgyUsage // assistant message ID -> merged usage; guarded by the daemon's inflightMu
	// streamMessageByParent maps a parent tool call ID ("" for the main agent) to the message it is
	// streaming, so message_delta usage lands on the right message; guarded by inflightMu.
	streamMessageByParent map[string]string
}

// tailBuffer is a thread-safe ring buffer capturing the trailing N bytes written to it.
type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func newTailBuffer(limit int) *tailBuffer {
	return &tailBuffer{max: limit}
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	if len(b.buf) > b.max {
		b.buf = b.buf[len(b.buf)-b.max:]
	}
	return len(p), nil
}

func (b *tailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

type inFlightToolCall struct {
	stepIndex int
	call      ToolCall
	mcpServer string
	startedAt time.Time
}

// completion is the ToolCall reported to sinks when this call ends: same identity, canonical name.
func (r inFlightToolCall) completion() ToolCall {
	return ToolCall{ID: r.call.ID, ParentID: r.call.ParentID, Name: r.call.Name, Subagent: r.call.Subagent}
}

// StreamingDaemon manages a long-lived streaming agy subprocess, its lifecycle state,
// and the stdout NDJSON event streaming loop.
type StreamingDaemon struct {
	cfg            DaemonConfig
	spawner        DaemonSpawner
	handle         ProcessHandle
	stdin          io.WriteCloser
	stdout         io.ReadCloser
	stderrCloser   io.Closer
	stderrTail     *tailBuffer
	mu             sync.RWMutex
	stdinMu        sync.Mutex
	state          DaemonState
	sessionID      string
	dirty          bool
	lastUsed       time.Time
	turnCount      int
	stepCount      int
	onTurnFinished func(d *StreamingDaemon)
	inflightMu     sync.Mutex
	inflight       []*TurnContext
	inFlightTools  map[int]inFlightToolCall
	claudeToolIDs  map[string]int
	seenMemoryIDs  map[string]bool
	readerWg       sync.WaitGroup
	closeOnce      sync.Once
	closed         atomic.Bool
}

func extractMemoryLineID(line string) string {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "- [id:") {
		return ""
	}
	rest := trimmed[len("- [id:"):]
	if end := strings.IndexByte(rest, ']'); end > 0 {
		return rest[:end]
	}
	return ""
}

func deduplicateRetrievedMemory(prompt string, seen map[string]bool) string {
	const openTag, closeTag = "<retrieved_memory>", "</retrieved_memory>"
	startIdx := strings.Index(prompt, openTag)
	if startIdx < 0 || seen == nil {
		return prompt
	}
	endRel := strings.Index(prompt[startIdx:], closeTag)
	if endRel < 0 {
		return prompt
	}
	blockStart := startIdx + len(openTag)
	blockEnd := startIdx + endRel
	inner := strings.Trim(prompt[blockStart:blockEnd], "\r\n")
	if inner == "" {
		return prompt
	}
	lines := strings.Split(inner, "\n")
	kept := make([]string, 0, len(lines))
	for _, line := range lines {
		if id := extractMemoryLineID(line); id != "" {
			if seen[id] {
				continue
			}
			seen[id] = true
		}
		kept = append(kept, line)
	}
	if len(kept) == len(lines) {
		return prompt
	}
	if len(kept) == 0 {
		after := strings.TrimPrefix(strings.TrimPrefix(prompt[blockEnd+len(closeTag):], "\r\n"), "\n")
		after = strings.TrimPrefix(strings.TrimPrefix(after, "\r\n"), "\n")
		return strings.TrimSpace(prompt[:startIdx] + after)
	}
	replacement := openTag + "\n" + strings.Join(kept, "\n") + "\n" + closeTag
	return prompt[:startIdx] + replacement + prompt[blockEnd+len(closeTag):]
}

func closeStreamQuietly(c io.Closer, name string) {
	if c != nil {
		if err := c.Close(); err != nil && !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, os.ErrClosed) {
			log.Printf("[StreamingDaemon] Warning: failed to close %s: %v", name, err)
		}
	}
}

func (d *StreamingDaemon) closeOnStartupError(reason string) {
	if err := d.Close(); err != nil {
		log.Printf("[StreamingDaemon] Warning: failed to close daemon during startup failure (%s): %v", reason, err)
	}
}

// DefaultHandshakeTimeout is the default maximum duration to wait for the daemon init event.
const DefaultHandshakeTimeout = 30 * time.Second

func (d *StreamingDaemon) waitForAgyHandshake(ctx context.Context, reader *bufio.Reader, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	lineChan, errChan := make(chan string, 1), make(chan error, 1)
	go func() {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			errChan <- readErr
		} else {
			lineChan <- line
		}
	}()

	select {
	case <-ctx.Done():
		d.closeOnStartupError("cancelled")
		return fmt.Errorf("daemon startup cancelled: %w", ctx.Err())
	case <-timer.C:
		d.closeOnStartupError("timeout")
		errMsg := "daemon startup timed out waiting for init event"
		if tailText := strings.TrimSpace(d.stderrTail.String()); tailText != "" {
			errMsg = fmt.Sprintf("%s: %s", errMsg, tailText)
		}
		return errors.New(errMsg)
	case readErr := <-errChan:
		d.closeOnStartupError("read error")
		errMsg := fmt.Sprintf("daemon stdout closed before init event: %v", readErr)
		if tailText := strings.TrimSpace(d.stderrTail.String()); tailText != "" {
			errMsg = fmt.Sprintf("%s: %s", errMsg, tailText)
		}
		return errors.New(errMsg)
	case line := <-lineChan:
		initSessID, ok := ParseInitEvent(line)
		if !ok || !IsValidUUID(initSessID) {
			d.closeOnStartupError("malformed init")
			errMsg := fmt.Sprintf("failed parsing init event (%q)", line)
			if tailText := strings.TrimSpace(d.stderrTail.String()); tailText != "" {
				errMsg = fmt.Sprintf("%s: %s", errMsg, tailText)
			}
			return errors.New(errMsg)
		}
		d.mu.Lock()
		d.sessionID, d.state = initSessID, StateReady
		d.mu.Unlock()
		return nil
	}
}

// StartStreamingDaemon launches a streaming daemon, waits for the initial handshake NDJSON event,
// latches the session ID, and spawns the background stdout reader loop.
func StartStreamingDaemon(ctx context.Context, cfg DaemonConfig, spawner DaemonSpawner) (*StreamingDaemon, error) {
	if spawner == nil {
		spawner = &DefaultDaemonSpawner{}
	}
	stdin, stdout, stderr, handle, err := spawner.Spawn(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed spawning daemon: %w", err)
	}

	tail := newTailBuffer(4096)
	if stderr != nil {
		go func() {
			defer closeStreamQuietly(stderr, "stderr")
			if _, copyErr := io.Copy(tail, stderr); copyErr != nil && !errors.Is(copyErr, io.EOF) && !errors.Is(copyErr, io.ErrClosedPipe) && !errors.Is(copyErr, os.ErrClosed) {
				log.Printf("[StreamingDaemon] Warning: error copying stderr: %v", copyErr)
			}
		}()
	}

	d := &StreamingDaemon{
		cfg: cfg, spawner: spawner, handle: handle, stdin: stdin,
		stdout: stdout, stderrCloser: stderr, stderrTail: tail,
		state: StateStarting, lastUsed: time.Now(),
		turnCount: cfg.InitialTurnCount, inFlightTools: make(map[int]inFlightToolCall),
		claudeToolIDs: make(map[string]int),
		seenMemoryIDs: make(map[string]bool),
	}

	reader := bufio.NewReader(stdout)

	if cfg.IsClaude() {
		sessID := cfg.SessionID
		if sessID == "" {
			sessID = uuid.New().String()
		}
		d.sessionID = sessID
		d.state = StateReady
	} else {
		timeout := DefaultHandshakeTimeout
		if cfg.Timeout > 0 {
			timeout = cfg.Timeout
		}
		if err := d.waitForAgyHandshake(ctx, reader, timeout); err != nil {
			return nil, err
		}
	}

	d.readerWg.Add(1)
	go d.readStdoutLoop(reader)
	return d, nil
}

func (d *StreamingDaemon) State() DaemonState {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.state
}

func (d *StreamingDaemon) SessionID() string {
	if d == nil {
		return ""
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.sessionID
}

func (d *StreamingDaemon) SetTranscriptRescuer(r func(string, time.Time) string) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cfg.TranscriptRescuer = r
}

func (d *StreamingDaemon) TurnCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.turnCount
}

func (d *StreamingDaemon) LastUsed() time.Time {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.lastUsed
}

func (d *StreamingDaemon) InflightCount() int {
	d.inflightMu.Lock()
	defer d.inflightMu.Unlock()
	return len(d.inflight)
}

func (d *StreamingDaemon) IsDirty() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.dirty
}

func (d *StreamingDaemon) SetSessionID(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.sessionID = id
}

func (d *StreamingDaemon) MarkDirty() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dirty = true
}

func (d *StreamingDaemon) SetOnTurnFinished(fn func(d *StreamingDaemon)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onTurnFinished = fn
}

func marshalPromptPayload(isClaude bool, payloadPrompt string) ([]byte, error) {
	if isClaude {
		type claudeUserMessage struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		}
		type claudeUserInput struct {
			Type    string            `json:"type"`
			Message claudeUserMessage `json:"message"`
		}
		return json.Marshal(claudeUserInput{
			Type: "user",
			Message: claudeUserMessage{
				Role:    "user",
				Content: payloadPrompt,
			},
		})
	}
	return json.Marshal(streamInputPayload{Event: "user", Message: streamInputMessage{Content: payloadPrompt}})
}

func (d *StreamingDaemon) syncTurnEnvFile(turnCtx *TurnContext) error {
	turnEnvFile := strings.TrimSpace(d.cfg.TurnEnvFile)
	if turnEnvFile == "" {
		return nil
	}
	targetID := d.cfg.ThreadID
	var vanadisChunkID string
	if turnCtx != nil {
		if strings.TrimSpace(turnCtx.TargetID) != "" {
			targetID = turnCtx.TargetID
		}
		vanadisChunkID = turnCtx.VanadisChunkID
	}
	if err := os.MkdirAll(filepath.Dir(turnEnvFile), 0o755); err != nil {
		return fmt.Errorf("failed creating turn env directory: %w", err)
	}
	content := FormatTurnEnvFileContent(targetID, vanadisChunkID)
	if err := os.WriteFile(turnEnvFile, []byte(content), 0o644); err != nil {
		return fmt.Errorf("failed writing turn env file: %w", err)
	}
	return os.Chmod(turnEnvFile, 0o644)
}

func (d *StreamingDaemon) Send(prompt string, turnCtx *TurnContext) error {
	d.stdinMu.Lock()
	defer d.stdinMu.Unlock()
	d.mu.RLock()
	if d.state == StateClosed || d.closed.Load() {
		d.mu.RUnlock()
		return errors.New("cannot send to closed streaming daemon")
	}
	d.mu.RUnlock()

	ctx := context.Background()
	if turnCtx != nil && turnCtx.Ctx != nil {
		ctx = turnCtx.Ctx
	}
	memQuery := prompt
	if turnCtx != nil && strings.TrimSpace(turnCtx.MemoryQuery) != "" {
		memQuery = strings.TrimSpace(turnCtx.MemoryQuery)
	}
	payloadPrompt := injectRetrievedContext(ctx, prompt, memQuery, d.cfg.AmbientContextRetriever, 500*time.Millisecond, d.cfg.MemoryRetriever, 2*time.Second, "[StreamingDaemon]")
	d.mu.Lock()
	if d.seenMemoryIDs == nil {
		d.seenMemoryIDs = make(map[string]bool)
	}
	payloadPrompt = deduplicateRetrievedMemory(payloadPrompt, d.seenMemoryIDs)
	d.mu.Unlock()

	encoded, err := marshalPromptPayload(d.cfg.IsClaude(), payloadPrompt)
	if err != nil {
		return fmt.Errorf("failed to marshal turn prompt: %w", err)
	}
	encoded = append(encoded, '\n')

	if err := d.syncTurnEnvFile(turnCtx); err != nil {
		return err
	}

	d.inflightMu.Lock()
	d.inflight = append(d.inflight, turnCtx)
	d.inflightMu.Unlock()

	d.mu.Lock()
	d.state, d.lastUsed = StateExecuting, time.Now()
	d.mu.Unlock()

	if turnCtx != nil && turnCtx.Sink != nil {
		turnCtx.Sink.OnTurnStarted()
	}

	if _, err := d.stdin.Write(encoded); err != nil {
		d.inflightMu.Lock()
		if len(d.inflight) > 0 && d.inflight[len(d.inflight)-1] == turnCtx {
			d.inflight = d.inflight[:len(d.inflight)-1]
		}
		d.inflightMu.Unlock()
		d.mu.Lock()
		d.dirty, d.state = true, StateClosed
		d.mu.Unlock()
		return fmt.Errorf("failed writing prompt to daemon stdin: %w", err)
	}
	return nil
}

func (d *StreamingDaemon) Close() error {
	var closeErr error
	d.closeOnce.Do(func() {
		d.closed.Store(true)
		d.mu.Lock()
		d.state = StateClosed
		d.mu.Unlock()

		closeStreamQuietly(d.stdin, "stdin")
		closeStreamQuietly(d.stdout, "stdout")
		closeStreamQuietly(d.stderrCloser, "stderr")
		if turnEnvFile := strings.TrimSpace(d.cfg.TurnEnvFile); turnEnvFile != "" {
			if err := os.Remove(turnEnvFile); err != nil && !os.IsNotExist(err) {
				log.Printf("[StreamingDaemon] Warning: failed to remove turn env file %q: %v", turnEnvFile, err)
			}
		}
		if d.handle != nil {
			if err := d.handle.Kill(); err != nil {
				log.Printf("[StreamingDaemon] Warning: failed to kill process handle: %v", err)
				closeErr = err
			}
			if waitErr := d.handle.Wait(); waitErr != nil {
				log.Printf("[StreamingDaemon] Process wait finished with: %v", waitErr)
			}
		}
		d.readerWg.Wait()

		d.inflightMu.Lock()
		remaining := d.inflight
		d.inflight = nil
		d.inflightMu.Unlock()

		for _, turn := range remaining {
			if turn != nil && turn.Sink != nil {
				d.failTurn(turn, errors.New("daemon closed while turn was in-flight"))
			}
		}
	})
	return closeErr
}

func (d *StreamingDaemon) drainInFlightToolsLocked() []inFlightToolCall {
	if len(d.inFlightTools) == 0 {
		return nil
	}
	dangling := make([]inFlightToolCall, 0, len(d.inFlightTools))
	for _, rec := range d.inFlightTools {
		dangling = append(dangling, rec)
	}
	d.inFlightTools = make(map[int]inFlightToolCall)
	return dangling
}

func (d *StreamingDaemon) tryRescueTurnOnEOF(turn *TurnContext) bool {
	if d.cfg.TranscriptRescuer == nil || d.SessionID() == "" {
		return false
	}
	rescued := d.cfg.TranscriptRescuer(d.SessionID(), turn.CreatedAt)
	if !IsSubstantiveResponse(rescued) || strings.HasPrefix(rescued, "[Tool Call Requested]:") {
		return false
	}
	log.Printf("[StreamingDaemon] Recovered substantive response directly from session %s transcript after unexpected EOF", d.SessionID())
	res := &TurnResult{ConversationID: d.SessionID(), Response: strings.TrimSpace(rescued), Duration: time.Since(turn.CreatedAt)}
	d.fillStreamUsage(turn, res)
	turn.Sink.OnResult(res)
	return true
}

func (d *StreamingDaemon) formatEOFErr(err error) error {
	if d.stderrTail == nil {
		return fmt.Errorf("daemon stdout unexpected EOF: %w", err)
	}
	tailText := strings.TrimSpace(d.stderrTail.String())
	if tailText == "" {
		return fmt.Errorf("daemon stdout unexpected EOF: %w", err)
	}
	return fmt.Errorf("daemon stdout unexpected EOF: %w: %s", err, tailText)
}

func (d *StreamingDaemon) handleStdoutEOF(err error) {
	if d.closed.Load() {
		return
	}
	d.mu.Lock()
	d.state = StateClosed
	d.mu.Unlock()
	d.inflightMu.Lock()
	remaining, dangling := d.inflight, d.drainInFlightToolsLocked()
	d.inflight = nil
	d.inflightMu.Unlock()

	for _, rec := range dangling {
		if len(remaining) > 0 && remaining[0] != nil && remaining[0].Sink != nil {
			remaining[0].Sink.OnToolCompleted(rec.completion(), rec.mcpServer, time.Since(rec.startedAt), "aborted")
		}
	}
	for _, turn := range remaining {
		if turn == nil || turn.Sink == nil {
			continue
		}
		if !d.tryRescueTurnOnEOF(turn) {
			d.failTurn(turn, d.formatEOFErr(err))
		}
	}
}

func (d *StreamingDaemon) readStdoutLoop(r *bufio.Reader) {
	defer d.readerWg.Done()
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			d.handleStdoutEOF(err)
			return
		}
		d.dispatchNDJSONLine(line)
	}
}

type stepUpdateFields struct {
	su            map[string]any
	toolName      string
	cmdName       string
	toolParams    map[string]any
	stepIdx       int
	hasStepIdx    bool
	isToolStart   bool
	isToolEnd     bool
	isSubagent    bool
	toolEndStatus string
}

// toolCall builds the sink identity of an agy tool step. agy has no call IDs, so the step
// index stands in when present; otherwise sinks correlate FIFO per tool name.
func (f stepUpdateFields) toolCall(name string) ToolCall {
	call := ToolCall{Name: name, Subagent: f.isSubagent || isAgySubagentTool(f.toolName)}
	if f.hasStepIdx {
		call.ID = fmt.Sprintf("step-%d", f.stepIdx)
	}
	return call
}

func isAgySubagentTool(toolName string) bool {
	return strings.TrimPrefix(strings.TrimSpace(toolName), "mcp_") == "invoke_subagent"
}

// isClaudeSubagentTool reports whether a Claude tool_use spawns a subagent.
func isClaudeSubagentTool(toolName string) bool {
	return toolName == "Agent" || toolName == "Task"
}

func extractToolInfoParams(su map[string]any) map[string]any {
	if info, ok := su["tool_info"].(map[string]any); ok {
		if params, ok := info["parameters"].(map[string]any); ok {
			return params
		}
	}
	return nil
}

func extractFirstToolCallEntry(su map[string]any, currentToolName string, currentParams map[string]any) (string, map[string]any) {
	tcs, ok := su["tool_calls"].([]any)
	if !ok || len(tcs) == 0 {
		return currentToolName, currentParams
	}
	firstTc, ok := tcs[0].(map[string]any)
	if !ok {
		return currentToolName, currentParams
	}
	toolName := currentToolName
	if n := extractFirstString(firstTc, "name"); n != "" && toolName == "" {
		toolName = n
	}
	toolParams := currentParams
	if args, ok := firstTc["args"].(map[string]any); ok && toolParams == nil {
		toolParams = args
	}
	return toolName, toolParams
}

// ExtractToolDetail extracts the primary target or summary detail from a tool's parameters.
func ExtractToolDetail(toolName string, params map[string]any, fallbackCmd string) string {
	if len(params) == 0 {
		return fallbackCmd
	}
	clean := strings.ToLower(strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(toolName), "mcp_"), "call_mcp_tool_"))
	switch clean {
	case "edit_file", "write_file", "read_file", "view_file", "replace_file_content":
		if p := extractFirstString(params, "Path", "path", "filePath", "file_path", "target_file", "AbsolutePath", "Target", "target"); p != "" {
			return filepath.Base(p)
		}
	case "run_command", "execute_command", "bash", "sh":
		if ts := extractFirstString(params, "toolSummary", "tool_summary", "summary"); ts != "" {
			return ts
		}
		if cl := extractFirstString(params, "CommandLine", "command", "cmd"); cl != "" {
			if cmd := ExtractCommandName(cl); cmd != "" {
				return cmd
			}
			return ""
		}
	case "call_mcp_tool":
		if tn := extractFirstString(params, "ToolName", "tool_name", "tool"); tn != "" {
			return tn
		}
	case "grep_search", "find_by_name", "search_web", "web_search":
		if q := extractFirstString(params, "Query", "query", "Pattern", "pattern"); q != "" {
			return q
		}
	case "invoke_subagent", "define_subagent":
		if r := extractFirstString(params, "Role", "role", "TypeName", "type_name", "Name", "name"); r != "" {
			return r
		}
	case "manage_task":
		if a := extractFirstString(params, "Action", "action"); a != "" {
			return a
		}
	}

	if ts := extractFirstString(params, "toolSummary", "tool_summary"); ts != "" {
		return ts
	}
	if p := extractFirstString(params, "Path", "path", "filePath", "file_path"); p != "" {
		return filepath.Base(p)
	}
	if cl := extractFirstString(params, "CommandLine", "command", "cmd"); cl != "" {
		if cmd := ExtractCommandName(cl); cmd != "" {
			return cmd
		}
		return ""
	}
	return fallbackCmd
}

func applyStepUpdateSubmap(f *stepUpdateFields, su map[string]any) {
	f.su = su
	state := strings.ToUpper(extractFirstString(su, "state", "status"))
	typ := strings.ToLower(extractFirstString(su, "type", "step_type"))
	f.stepIdx, f.hasStepIdx = extractStepIndex(su)
	if tn := extractFirstString(su, "tool_name"); tn != "" {
		f.toolName = tn
	}
	if params := extractToolInfoParams(su); params != nil {
		f.toolParams = params
	}
	f.toolName, f.toolParams = extractFirstToolCallEntry(su, f.toolName, f.toolParams)
	f.cmdName = ExtractToolDetail(f.toolName, f.toolParams, f.cmdName)
	switch state {
	case "DONE":
		f.toolEndStatus = "ok"
	case "ERROR":
		f.toolEndStatus = "error"
	}
	f.isSubagent = typ == "subagent"
	if typ == "tool_call" || typ == "tool" || typ == "subagent" {
		f.isToolStart = f.toolEndStatus == ""
		f.isToolEnd = f.toolEndStatus != ""
	}
}

func parseStepUpdateEvent(raw map[string]any) stepUpdateFields {
	var f stepUpdateFields
	if tc, ok := raw["tool_call"].(map[string]any); ok {
		if n, ok := tc["name"].(string); ok {
			f.toolName = n
		}
		if c, ok := tc["command"].(string); ok {
			f.cmdName = c
		}
		f.isToolStart = true
	}
	if m, ok := raw["step_update"].(map[string]any); ok && m != nil {
		applyStepUpdateSubmap(&f, m)
	}
	if !f.hasStepIdx {
		f.stepIdx, f.hasStepIdx = extractStepIndex(raw)
	}
	if f.cmdName == "" && len(f.toolParams) > 0 {
		f.cmdName = ExtractToolDetail(f.toolName, f.toolParams, "")
	}
	return f
}

func (d *StreamingDaemon) notifyStepBoundary(activeTurn *TurnContext, stepIdx int) {
	d.inflightMu.Lock()
	isNewStep := !activeTurn.hasSeenStep || stepIdx != activeTurn.lastStepIndex
	if isNewStep {
		activeTurn.hasSeenStep, activeTurn.lastStepIndex = true, stepIdx
	}
	d.inflightMu.Unlock()
	if isNewStep {
		if sa, ok := activeTurn.Sink.(StepAwareSink); ok {
			sa.OnStepStarted(stepIdx)
		}
	}
}

func isShellReadCommand(cmd string) bool {
	switch cmd {
	case "cat", "head", "tail", "sed", "less", "more", "bat":
		return true
	default:
		return false
	}
}

func extractActivatedSkill(toolName string, toolParams map[string]any, cmdName string) (string, bool) {
	clean := strings.ToLower(strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(toolName), "mcp_"), "call_mcp_tool_"))
	switch clean {
	case "edit_file", "write_file", "replace_file_content", "multi_replace_file_content", "delete_file", "grep_search", "find_by_name", "call_mcp_tool":
		return "", false
	case "run_command", "execute_command", "bash", "sh":
		cl := extractFirstString(toolParams, "CommandLine", "command_line", "command", "cmd")
		if isShellReadCommand(ExtractCommandName(cl)) {
			return ExtractSkillFromTarget(cl)
		}
		return "", false
	}
	if p := extractFirstString(toolParams, "Path", "path", "filePath", "file_path", "target_file", "AbsolutePath", "Target", "target"); p != "" {
		if skill, ok := ExtractSkillFromTarget(p); ok {
			return skill, true
		}
	}
	if cmdName != "" {
		return ExtractSkillFromTarget(cmdName)
	}
	return "", false
}

// recordToolStart registers call (whose Name is the canonical tool name) as in flight.
func (d *StreamingDaemon) recordToolStart(stepIdx int, hasStepIdx bool, call ToolCall, mcpServer string) bool {
	var isNewToolStart bool
	d.inflightMu.Lock()
	defer d.inflightMu.Unlock()
	if d.inFlightTools == nil {
		d.inFlightTools = make(map[int]inFlightToolCall)
	}
	if hasStepIdx {
		if _, exists := d.inFlightTools[stepIdx]; !exists {
			d.inFlightTools[stepIdx] = inFlightToolCall{stepIndex: stepIdx, call: call, mcpServer: mcpServer, startedAt: time.Now()}
			isNewToolStart = true
		}
	} else {
		alreadyInFlight := false
		for _, rec := range d.inFlightTools {
			if rec.call.Name == call.Name {
				alreadyInFlight = true
				break
			}
		}
		if !alreadyInFlight {
			d.inFlightTools[-1] = inFlightToolCall{stepIndex: -1, call: call, mcpServer: mcpServer, startedAt: time.Now()}
			isNewToolStart = true
		}
	}
	return isNewToolStart
}

func matchFallbackInFlightTool(tools map[int]inFlightToolCall, toolName string, params map[string]any) (int, inFlightToolCall, bool) {
	cTool, _ := ExtractMCPToolInfo(toolName, params)
	for idx, rec := range tools {
		if rec.call.Name == toolName || rec.call.Name == cTool || toolName == "call_mcp_tool" || len(tools) == 1 {
			return idx, rec, true
		}
	}
	return 0, inFlightToolCall{}, false
}

func (d *StreamingDaemon) recordToolEnd(f stepUpdateFields) (inFlightToolCall, bool, bool) {
	d.inflightMu.Lock()
	defer d.inflightMu.Unlock()
	if d.inFlightTools == nil {
		return inFlightToolCall{}, false, f.isToolEnd
	}

	var completedRecord inFlightToolCall
	var hasCompletedRecord bool
	isToolEnd := f.isToolEnd

	if f.hasStepIdx {
		if rec, exists := d.inFlightTools[f.stepIdx]; exists {
			completedRecord, hasCompletedRecord, isToolEnd = rec, true, true
			delete(d.inFlightTools, f.stepIdx)
		}
	}
	if isToolEnd && !hasCompletedRecord && len(d.inFlightTools) > 0 {
		if idx, rec, ok := matchFallbackInFlightTool(d.inFlightTools, f.toolName, f.toolParams); ok {
			completedRecord, hasCompletedRecord = rec, true
			delete(d.inFlightTools, idx)
		}
	}
	return completedRecord, hasCompletedRecord, isToolEnd
}

func extractStepTextDelta(raw, su map[string]any) string {
	for _, m := range []map[string]any{raw, su} {
		if m == nil {
			continue
		}
		for _, k := range []string{"delta", "text_delta"} {
			if d, ok := m[k].(string); ok && d != "" {
				return d
			}
		}
	}
	return ""
}

func hasThinkingField(raw, su map[string]any) bool {
	if _, ok := raw["thinking"]; ok {
		return true
	}
	if su != nil {
		if _, ok := su["thinking"]; ok {
			return true
		}
	}
	return false
}

func (d *StreamingDaemon) handleStepUpdateEvent(activeTurn *TurnContext, raw map[string]any) {
	d.mu.Lock()
	d.stepCount++
	d.mu.Unlock()
	if activeTurn.Sink == nil {
		return
	}

	f := parseStepUpdateEvent(raw)
	if f.hasStepIdx {
		d.notifyStepBoundary(activeTurn, f.stepIdx)
	}

	var (
		canonicalTool      string
		activatedSkill     string
		hasSkill           bool
		completedRecord    inFlightToolCall
		hasCompletedRecord bool
		isToolEnd          = f.isToolEnd
		isNewToolStart     bool
		mcpCallServer      string
		mcpCallTool        string
	)

	if f.isToolStart {
		var mcpServer string
		canonicalTool, mcpServer = ExtractMCPToolInfo(f.toolName, f.toolParams)
		activatedSkill, hasSkill = extractActivatedSkill(f.toolName, f.toolParams, f.cmdName)
		isNewToolStart = d.recordToolStart(f.stepIdx, f.hasStepIdx, f.toolCall(canonicalTool), mcpServer)
		mcpCallServer, mcpCallTool = mcpCallTarget(canonicalTool, mcpServer)
	} else if f.isToolEnd || (f.hasStepIdx && f.toolEndStatus != "") {
		completedRecord, hasCompletedRecord, isToolEnd = d.recordToolEnd(f)
	}

	if isNewToolStart && (canonicalTool != "" || f.cmdName != "") {
		toolForSink := canonicalTool
		cmdForSink := f.cmdName
		if f.toolName == "call_mcp_tool" {
			toolForSink = "call_mcp_tool"
		} else if canonicalTool == cmdForSink {
			cmdForSink = ""
		}
		call := f.toolCall(toolForSink)
		call.Command = cmdForSink
		call.MCPServer, call.MCPTool = mcpCallServer, mcpCallTool
		activeTurn.Sink.OnToolCall(call)
	}
	if isNewToolStart && hasSkill {
		activeTurn.Sink.OnSkillActivated(activatedSkill, "discord")
	}
	if isToolEnd && hasCompletedRecord {
		activeTurn.Sink.OnToolCompleted(completedRecord.completion(), completedRecord.mcpServer, time.Since(completedRecord.startedAt), f.toolEndStatus)
	}
	if delta := extractStepTextDelta(raw, f.su); delta != "" {
		activeTurn.Sink.OnTextDelta(delta)
	}
	if hasThinkingField(raw, f.su) {
		activeTurn.Sink.OnThinking()
	}
}

func extractErrorsArray(errs []any) string {
	var errStrs []string
	for _, e := range errs {
		if es, ok := e.(string); ok && strings.TrimSpace(es) != "" {
			errStrs = append(errStrs, es)
			continue
		}
		if em, ok := e.(map[string]any); ok {
			if msg := extractFirstString(em, "message", "error", "text"); msg != "" {
				errStrs = append(errStrs, msg)
			}
		}
	}
	return strings.Join(errStrs, "; ")
}

func isClaudeResultError(raw, resObj map[string]any) bool {
	if b, ok := raw["is_error"].(bool); ok && b {
		return true
	}
	if resObj != nil {
		if b, ok := resObj["is_error"].(bool); ok && b {
			return true
		}
	}
	sub := extractFirstString(raw, "subtype")
	if sub == "" && resObj != nil {
		sub = extractFirstString(resObj, "subtype")
	}
	return strings.HasPrefix(strings.ToLower(sub), "error")
}

func extractResultErrors(raw, resObj map[string]any, isErr bool) string {
	if isErr {
		if s, ok := raw["result"].(string); ok && strings.TrimSpace(s) != "" {
			return s
		}
	}
	for _, m := range []map[string]any{resObj, raw} {
		if m == nil {
			continue
		}
		if errs, ok := m["errors"].([]any); ok && len(errs) > 0 {
			if joined := extractErrorsArray(errs); joined != "" {
				return joined
			}
		}
	}
	return ""
}

func extractResultStatusAndError(raw map[string]any) (status, errMsg string) {
	var resObj map[string]any
	if r, ok := raw["result"].(map[string]any); ok {
		resObj = r
	}
	for _, m := range []map[string]any{resObj, raw} {
		if m == nil {
			continue
		}
		if status == "" {
			status = extractFirstString(m, "status")
		}
		if errMsg == "" {
			errMsg = extractFirstString(m, "error")
		}
	}

	isErr := isClaudeResultError(raw, resObj)
	if isErr && status == "" {
		status = "ERROR"
	}
	if errMsg == "" {
		errMsg = extractResultErrors(raw, resObj, isErr)
	}

	return status, errMsg
}

func (d *StreamingDaemon) tryRescueTurnOnResult(activeTurn *TurnContext, raw map[string]any, errMsg, currentModel string) bool {
	if d.cfg.TranscriptRescuer == nil || d.SessionID() == "" {
		return false
	}
	rescued := d.cfg.TranscriptRescuer(d.SessionID(), activeTurn.CreatedAt)
	if rescued == "" || !IsSubstantiveResponse(rescued) || strings.HasPrefix(rescued, "[Tool Call Requested]:") {
		return false
	}
	log.Printf("[StreamingDaemon] Recovered substantive response directly from session %s transcript after error/empty result: %s", d.SessionID(), rescued)
	recordStreamErrorMetric(errMsg, currentModel, false)
	res := &TurnResult{ConversationID: d.SessionID(), Response: strings.TrimSpace(rescued), Stderr: errMsg, Duration: time.Since(activeTurn.CreatedAt)}
	populateUsage(res, raw)
	d.fillStreamUsage(activeTurn, res)
	activeTurn.Sink.OnResult(res)
	return true
}

func (d *StreamingDaemon) handleResultError(activeTurn *TurnContext, respStr, status, errMsg, currentModel string) bool {
	if !strings.EqualFold(status, "ERROR") {
		return false
	}
	if strings.TrimSpace(respStr) == "" || strings.HasPrefix(respStr, "[Tool Call Requested]:") {
		recordStreamErrorMetric(errMsg, currentModel, true)
		if errMsg == "" {
			errMsg = "daemon execution failed"
		}
		d.failTurn(activeTurn, errors.New(errMsg))
		return true
	}
	if errMsg != "" {
		log.Printf("[StreamingDaemon] Notice: daemon reported error with substantive response: %s", errMsg)
		recordStreamErrorMetric(errMsg, currentModel, false)
	}
	return false
}

func (d *StreamingDaemon) deliverResultToSink(activeTurn *TurnContext, raw map[string]any) {
	currentModel := d.cfg.Model
	if activeTurn != nil && strings.TrimSpace(activeTurn.Model) != "" {
		currentModel = strings.TrimSpace(activeTurn.Model)
	}
	if currentModel == "" {
		currentModel = "default"
	}

	respStr := extractResponseString(raw)
	status, errMsg := extractResultStatusAndError(raw)

	if strings.TrimSpace(respStr) == "" || strings.HasPrefix(respStr, "[Tool Call Requested]:") || strings.EqualFold(status, "ERROR") {
		if d.tryRescueTurnOnResult(activeTurn, raw, errMsg, currentModel) {
			return
		}
		if d.handleResultError(activeTurn, respStr, status, errMsg, currentModel) {
			return
		}
	}

	res := &TurnResult{ConversationID: d.SessionID(), Response: respStr, Stderr: errMsg, Duration: time.Since(activeTurn.CreatedAt)}
	populateUsage(res, raw)
	d.fillStreamUsage(activeTurn, res)
	activeTurn.Sink.OnResult(res)
}

func (d *StreamingDaemon) handleResultEvent(raw map[string]any) {
	d.inflightMu.Lock()
	var activeTurn *TurnContext
	if len(d.inflight) > 0 {
		activeTurn = d.inflight[0]
		d.inflight = d.inflight[1:]
	}
	hasMoreInflight := len(d.inflight) > 0
	dangling := d.drainInFlightToolsLocked()
	d.inflightMu.Unlock()

	for _, rec := range dangling {
		if activeTurn != nil && activeTurn.Sink != nil {
			activeTurn.Sink.OnToolCompleted(rec.completion(), rec.mcpServer, time.Since(rec.startedAt), "aborted")
		}
	}

	d.mu.Lock()
	d.turnCount++
	d.lastUsed = time.Now()
	if !hasMoreInflight && d.state != StateClosed {
		d.state = StateReady
	}
	var turnFinishedCb func(d *StreamingDaemon)
	if !hasMoreInflight {
		turnFinishedCb = d.onTurnFinished
	}
	d.mu.Unlock()

	if turnFinishedCb != nil {
		go turnFinishedCb(d)
	}
	if activeTurn == nil || activeTurn.Sink == nil {
		return
	}

	d.deliverResultToSink(activeTurn, raw)
}

// recordDaemonInit marks Claude's init message on the in-flight turn's span: StartStreamingDaemon
// returns before Claude has connected its MCP servers, so this event closes that gap in the trace.
func (d *StreamingDaemon) recordDaemonInit() {
	d.inflightMu.Lock()
	var turn *TurnContext
	if len(d.inflight) > 0 {
		turn = d.inflight[0]
	}
	d.inflightMu.Unlock()
	if turn != nil && turn.Ctx != nil {
		trace.SpanFromContext(turn.Ctx).AddEvent(eventDaemonInit)
	}
}

func (d *StreamingDaemon) dispatchNDJSONLine(line string) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(trimmed), &raw); err != nil {
		log.Printf("[StreamingDaemon] Malformed NDJSON line: %v", err)
		return
	}

	event, ok := raw["event"].(string)
	if !ok {
		event, ok = raw["type"].(string)
	}
	if !ok {
		log.Printf("[StreamingDaemon] NDJSON line missing string event/type field: %v", raw)
		return
	}

	// Handle Claude "system" init event before inflight check, as it may latch the session ID
	if event == "system" {
		if extractFirstString(raw, "subtype") == "init" {
			if sessID := extractFirstString(raw, "session_id"); sessID != "" && IsValidUUID(sessID) {
				d.mu.Lock()
				d.sessionID = sessID
				d.mu.Unlock()
			}
			d.recordDaemonInit()
		}
		return
	}

	d.inflightMu.Lock()
	var activeTurn *TurnContext
	if len(d.inflight) > 0 {
		activeTurn = d.inflight[0]
	}
	d.inflightMu.Unlock()
	if activeTurn == nil {
		return
	}

	switch event {
	case "step_update":
		d.handleStepUpdateEvent(activeTurn, raw)
	case "result":
		d.handleResultEvent(raw)
	case "stream_event":
		d.handleClaudeStreamEvent(activeTurn, raw)
	case "assistant":
		d.handleClaudeAssistantEvent(activeTurn, raw)
	case "user":
		d.handleClaudeUserEvent(activeTurn, raw)
	}
}

func (d *StreamingDaemon) handleClaudeStreamEvent(activeTurn *TurnContext, raw map[string]any) {
	ev, ok := raw["event"].(map[string]any)
	if !ok || ev == nil {
		return
	}
	d.recordStreamEventUsage(activeTurn, extractFirstString(raw, "parent_tool_use_id"), ev)
	switch extractFirstString(ev, "type") {
	case "message_start":
		d.mu.Lock()
		d.stepCount++
		stepIdx := d.stepCount
		d.mu.Unlock()
		d.notifyStepBoundary(activeTurn, stepIdx)
	case "content_block_delta":
		if delta, ok := ev["delta"].(map[string]any); ok && delta != nil {
			switch extractFirstString(delta, "type") {
			case "text_delta":
				if text := extractFirstString(delta, "text"); text != "" && activeTurn.Sink != nil {
					activeTurn.Sink.OnTextDelta(text)
				}
			case "thinking_delta":
				if activeTurn.Sink != nil {
					activeTurn.Sink.OnThinking()
				}
			}
		}
	}
}

func (d *StreamingDaemon) handleClaudeAssistantEvent(activeTurn *TurnContext, raw map[string]any) {
	msg, ok := raw["message"].(map[string]any)
	if !ok || msg == nil {
		return
	}
	d.recordStreamUsage(activeTurn, msg)
	contents, ok := msg["content"].([]any)
	if !ok {
		return
	}
	// Messages emitted by a subagent carry the tool_use id of the Agent/Task call that spawned it.
	parentToolUseID := extractFirstString(raw, "parent_tool_use_id")

	for _, block := range contents {
		bMap, ok := block.(map[string]any)
		if !ok || bMap == nil {
			continue
		}
		if extractFirstString(bMap, "type") != "tool_use" {
			continue
		}

		toolID := extractFirstString(bMap, "id")
		toolName := extractFirstString(bMap, "name")
		var inputMap map[string]any
		if in, ok := bMap["input"].(map[string]any); ok {
			inputMap = in
		}

		d.mu.Lock()
		d.stepCount++
		stepIdx := d.stepCount
		d.mu.Unlock()

		d.inflightMu.Lock()
		if d.claudeToolIDs == nil {
			d.claudeToolIDs = make(map[string]int)
		}
		if toolID != "" {
			d.claudeToolIDs[toolID] = stepIdx
		}
		d.inflightMu.Unlock()

		d.notifyStepBoundary(activeTurn, stepIdx)

		canonicalTool, mcpServer := ExtractMCPToolInfo(toolName, inputMap)
		cmdDetail := ExtractToolDetail(toolName, inputMap, "")
		activatedSkill, hasSkill := extractActivatedSkill(toolName, inputMap, cmdDetail)

		call := ToolCall{ID: toolID, ParentID: parentToolUseID, Name: canonicalTool, Subagent: isClaudeSubagentTool(toolName)}
		isNewStart := d.recordToolStart(stepIdx, true, call, mcpServer)
		call.MCPServer, call.MCPTool = mcpCallTarget(canonicalTool, mcpServer)
		if isNewStart && activeTurn.Sink != nil {
			toolForSink := canonicalTool
			cmdForSink := cmdDetail
			if toolName == "call_mcp_tool" {
				toolForSink = "call_mcp_tool"
			} else if canonicalTool == cmdForSink {
				cmdForSink = ""
			}
			call.Name, call.Command = toolForSink, cmdForSink
			activeTurn.Sink.OnToolCall(call)
			if hasSkill {
				activeTurn.Sink.OnSkillActivated(activatedSkill, "discord")
			}
		}
	}
}

func (d *StreamingDaemon) handleClaudeUserEvent(activeTurn *TurnContext, raw map[string]any) {
	msg, ok := raw["message"].(map[string]any)
	if !ok || msg == nil {
		return
	}
	contents, ok := msg["content"].([]any)
	if !ok {
		return
	}

	for _, block := range contents {
		bMap, ok := block.(map[string]any)
		if !ok || bMap == nil {
			continue
		}
		if extractFirstString(bMap, "type") != "tool_result" {
			continue
		}

		toolUseID := extractFirstString(bMap, "tool_use_id")
		status := "ok"
		if isErr, ok := bMap["is_error"].(bool); ok && isErr {
			status = "error"
		}

		d.inflightMu.Lock()
		stepIdx, hasStep := d.claudeToolIDs[toolUseID]
		if hasStep {
			delete(d.claudeToolIDs, toolUseID)
		}
		d.inflightMu.Unlock()

		fields := stepUpdateFields{
			isToolEnd:     true,
			toolEndStatus: status,
			hasStepIdx:    hasStep,
			stepIdx:       stepIdx,
		}
		rec, hasRec, isEnd := d.recordToolEnd(fields)
		if isEnd && hasRec && activeTurn.Sink != nil {
			activeTurn.Sink.OnToolCompleted(rec.completion(), rec.mcpServer, time.Since(rec.startedAt), status)
		}
	}
}

func extractFirstString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := m[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}

func extractStepIndex(m map[string]any) (int, bool) {
	if idx, ok := m["step_index"].(float64); ok {
		return int(idx), true
	}
	if idx, ok := m["step_index"].(int); ok {
		return idx, true
	}
	return 0, false
}

func extractResponseString(raw map[string]any) string {
	// If it's an error result, the "result" string is the error, not the substantive response.
	isErr := false
	if b, ok := raw["is_error"].(bool); ok && b {
		isErr = true
	}
	if sub := extractFirstString(raw, "subtype"); strings.HasPrefix(strings.ToLower(sub), "error") {
		isErr = true
	}

	keys := []string{"response", "result", "content", "text"}
	if resObj, ok := raw["result"].(map[string]any); ok {
		if b, ok := resObj["is_error"].(bool); ok && b {
			isErr = true
		}
		if sub := extractFirstString(resObj, "subtype"); strings.HasPrefix(strings.ToLower(sub), "error") {
			isErr = true
		}
		for _, k := range keys {
			if resp, ok := resObj[k].(string); ok && strings.TrimSpace(resp) != "" {
				return resp
			}
		}
	}
	for _, k := range []string{"response", "result", "content", "text"} {
		if k == "result" && isErr {
			continue
		}
		if resp, ok := raw[k].(string); ok && strings.TrimSpace(resp) != "" {
			return resp
		}
	}
	return ""
}

func recordStreamErrorMetric(errMsg, model string, recordProcessErr bool) {
	if errMsg == "" && !recordProcessErr {
		return
	}
	switch {
	case IsCapacityBlip(errMsg, ""):
		metrics.RecordRunnerError("capacity_throttle", model)
	case IsQuotaPause(errMsg, ""):
		metrics.RecordRunnerError("quota_paused", model)
	case recordProcessErr:
		metrics.RecordRunnerError("process_error", model)
	}
}
