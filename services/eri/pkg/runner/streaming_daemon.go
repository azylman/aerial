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

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/services/eri/pkg/session"
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
	// SubagentConversationID is the spawned child conversation ID when an async subagent
	// launch completes (e.g. agy invoke_subagent), keeping the invoke_agent span open until
	// the child conversation finishes.
	SubagentConversationID string
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
	// sawModelReply records that a real (model-produced) assistant message arrived this turn, and
	// syntheticReply holds the latest Claude CLI-authored one; guarded by inflightMu.
	sawModelReply          bool
	syntheticReply         *claudeSyntheticReply
	seenReasoningItemIDs   map[string]bool
	codexResponseText      string
	itemTypes              map[string]string
	museSubagentStack      []string
	museChildParentID      map[string]string
	museItemChildSession   map[string]string
	museSeenChildUsage     map[string]bool
	museSubscribedChildren map[string]bool
	pendingAgySubagents    []pendingAgySubagent
	lastAgyProbeStep       int
}

// ClaudeSyntheticModel is the message.model of assistant messages the Claude CLI writes itself.
const ClaudeSyntheticModel = session.ClaudeSyntheticModel

// claudeSyntheticReply is a Claude CLI-authored assistant message: its text and the stream event's
// typed error (e.g. "rate_limit"; empty for the resume placeholder).
type claudeSyntheticReply struct {
	text, errType string
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
	return ToolCall{
		ID:                     r.call.ID,
		ParentID:               r.call.ParentID,
		Name:                   r.call.Name,
		Subagent:               r.call.Subagent,
		SubagentConversationID: r.call.SubagentConversationID,
	}
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
	spawnSpanCtx   trace.SpanContext
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
	lastStdoutNano atomic.Int64
	closeCh        chan struct{}
	readerWg       sync.WaitGroup
	stderrWg       sync.WaitGroup
	watchdogWg     sync.WaitGroup
	closeOnce      sync.Once
	closed         atomic.Bool
}

func closeStreamQuietly(c io.Closer, name string) {
	if c != nil {
		if err := c.Close(); err != nil && !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, os.ErrClosed) {
			log.Printf("[StreamingDaemon] Warning: failed to close %s: %v", name, err)
		}
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
		return errors.New(d.appendStderrTail("daemon startup timed out waiting for init event"))
	case readErr := <-errChan:
		d.closeOnStartupError("read error")
		return errors.New(d.appendStderrTail(fmt.Sprintf("daemon stdout closed before init event: %v", readErr)))
	case line := <-lineChan:
		initSessID, ok := ParseInitEvent(line)
		if !ok || !IsValidUUID(initSessID) {
			d.closeOnStartupError("malformed init")
			return errors.New(d.appendStderrTail(fmt.Sprintf("failed parsing init event (%q)", line)))
		}
		d.mu.Lock()
		d.sessionID, d.state = initSessID, StateReady
		d.mu.Unlock()
		return nil
	}
}

func (d *StreamingDaemon) performHandshake(startup context.Context, reader *bufio.Reader, timeout time.Duration) error {
	switch {
	case d.cfg.IsClaude():
		sessID := d.cfg.SessionID
		if sessID == "" {
			sessID = uuid.New().String()
		}
		d.sessionID, d.state = sessID, StateReady
		return nil
	case d.cfg.IsCodex():
		return d.waitForCodexHandshake(startup, reader, timeout)
	case d.cfg.IsMuse():
		return d.waitForMuseHandshake(startup, reader, timeout)
	default:
		return d.waitForAgyHandshake(startup, reader, timeout)
	}
}

// StartStreamingDaemon starts a process on lifetime. The startup context bounds
// retries and handshakes without shortening the successful process lifetime.
func StartStreamingDaemon(lifetime, startup context.Context, cfg DaemonConfig, spawner DaemonSpawner) (*StreamingDaemon, error) {
	if spawner == nil {
		spawner = &DefaultDaemonSpawner{}
	}
	stdin, stdout, stderr, handle, err := spawnDaemon(lifetime, startup, cfg, spawner)
	if err != nil {
		return nil, fmt.Errorf("failed spawning daemon: %w", err)
	}

	tail := newTailBuffer(4096)
	d := &StreamingDaemon{
		cfg: cfg, spawner: spawner, handle: handle, stdin: stdin,
		stdout: stdout, stderrCloser: stderr, stderrTail: tail,
		spawnSpanCtx: trace.SpanContextFromContext(startup),
		state:        StateStarting, lastUsed: time.Now(),
		turnCount: cfg.InitialTurnCount, inFlightTools: make(map[int]inFlightToolCall),
		claudeToolIDs: make(map[string]int),
		seenMemoryIDs: make(map[string]bool),
		closeCh:       make(chan struct{}),
	}
	d.lastStdoutNano.Store(time.Now().UnixNano())

	if stderr != nil {
		d.stderrWg.Add(1)
		go d.readStderrLoop(stderr) //nolint:contextcheck // Correlation resolves per active TurnContext.Ctx, not daemon startup.
	}

	if err := startup.Err(); err != nil {
		d.closeOnStartupError("cancelled")
		return nil, fmt.Errorf("daemon startup cancelled: %w", err)
	}
	reader := bufio.NewReader(stdout)

	timeout := DefaultHandshakeTimeout
	if cfg.Timeout > 0 {
		timeout = cfg.Timeout
	}
	if err := d.performHandshake(startup, reader, timeout); err != nil {
		return nil, err
	}

	// A successful handshake can race with startup budget expiration.
	if err := startup.Err(); err != nil {
		d.closeOnStartupError("cancelled")
		return nil, fmt.Errorf("daemon startup cancelled: %w", err)
	}
	d.readerWg.Add(1)
	go d.readStdoutLoop(reader) //nolint:contextcheck // Recovery uses each queued TurnContext.Ctx, not the daemon lifetime.
	if !d.cfg.IsClaude() && !d.cfg.IsCodex() && !d.cfg.IsMuse() {
		d.watchdogWg.Add(1)
		go d.watchdogLoop(lifetime)
	}
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

func (d *StreamingDaemon) SetTranscriptRescuer(r func(context.Context, string, time.Time) string) {
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
			Type: eventUser,
			Message: claudeUserMessage{
				Role:    roleUser,
				Content: payloadPrompt,
			},
		})
	}
	return json.Marshal(streamInputPayload{Event: eventUser, Message: streamInputMessage{Content: payloadPrompt}})
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
	saTokenDest := filepath.Join(filepath.Dir(turnEnvFile), "sa-token")
	syncedTokenPath, syncErr := session.SyncTurnSATokenFile(os.Getenv("AERIAL_K8S_SA_TOKEN_SRC"), saTokenDest, getEuid(), os.Chown)
	if syncErr != nil {
		log.Printf("[StreamingDaemon] Warning: failed syncing SA token file %q: %v", saTokenDest, syncErr)
	}
	content := session.FormatTurnEnvFileContent(targetID, vanadisChunkID, syncedTokenPath)
	if err := os.WriteFile(turnEnvFile, []byte(content), 0o644); err != nil {
		return fmt.Errorf("failed writing turn env file: %w", err)
	}
	return os.Chmod(turnEnvFile, 0o644)
}

// Send hands prompt to the daemon as a new turn. The turn's context travels in turnCtx.Ctx (set
// by every lease's Execute); contextcheck cannot
// see through that, so Send is exempt from its caller analysis.
//
//nolint:contextcheck,nolintlint // contextcheck reads this doc directive; golangci sees no in-body issue.
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

	encoded, err := d.marshalTurnPrompt(turnCtx, payloadPrompt)
	if err != nil {
		return fmt.Errorf("failed to marshal turn prompt: %w", err)
	}
	encoded = append(encoded, '\n')

	if err := d.syncTurnEnvFile(turnCtx); err != nil {
		return err
	}

	if turnCtx != nil && turnCtx.CreatedAt.IsZero() {
		turnCtx.CreatedAt = time.Now()
	}
	d.lastStdoutNano.Store(time.Now().UnixNano())

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

func (d *StreamingDaemon) closeInternal() error {
	if d == nil {
		return nil
	}
	var closeErr error
	d.closeOnce.Do(func() {
		d.closed.Store(true)
		d.mu.Lock()
		d.state = StateClosed
		d.mu.Unlock()

		if d.closeCh != nil {
			close(d.closeCh)
		}

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
		d.stderrWg.Wait()

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

func (d *StreamingDaemon) Close() error {
	if d == nil {
		return nil
	}
	closeErr := d.closeInternal()
	d.watchdogWg.Wait()
	return closeErr
}

func (d *StreamingDaemon) tryRescueTurnOnEOF(turn *TurnContext) bool {
	if d.cfg.TranscriptRescuer == nil || d.SessionID() == "" {
		return false
	}
	rescued := d.rescueTranscript(turn)
	if !IsSubstantiveResponse(rescued) || strings.HasPrefix(rescued, "[Tool Call Requested]:") {
		return false
	}
	log.Printf("[StreamingDaemon] Recovered substantive response directly from session %s transcript after unexpected EOF", d.SessionID())
	res := &TurnResult{ConversationID: d.SessionID(), Response: strings.TrimSpace(rescued), Duration: time.Since(turn.CreatedAt)}
	d.fillStreamUsage(turn, res)
	d.completePendingAgySubagents(turn)
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
		d.lastStdoutNano.Store(time.Now().UnixNano())
		d.dispatchNDJSONLine(line)
	}
}
