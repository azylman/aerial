//go:build restateintegration

package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/jackc/pgx/v5/pgconn"
	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/restatetest"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

// The coordinator + TurnRunner loop end to end against a real restate-server: ThreadSession /
// DispatchSession coordinators and the TurnRunner service bound over a WorkerPool whose agent,
// store and pre_turn hook are scripted, driven only through Restate ingress.

const (
	itThreadKey       = "thread-it"
	itSessionKey      = "sess-it"
	itWait            = 90 * time.Second
	itPreTurnDelay    = 2 * time.Second
	itOutageAttempts  = 20
	itKilledErr       = "signal: killed"
	itWatchdogErr     = "[watchdog] inactivity timeout exceeded after 5m"
	itNonTransientErr = "invalid api key"
)

// The short server default retry policy: Restate retries fast and far longer than the old
// 10-attempt runner policy allowed, and pauses (never kills) once exhausted.
var itServerEnv = []string{
	"RESTATE_DEFAULT_RETRY_POLICY__INITIAL_INTERVAL=50ms",
	"RESTATE_DEFAULT_RETRY_POLICY__MAX_INTERVAL=200ms",
	"RESTATE_DEFAULT_RETRY_POLICY__MAX_ATTEMPTS=1000",
}

// itAgent is one scripted agent run: a reply, an error, or (block) a run that only ends when its
// context is cancelled (killed by stop-turn, or dropped with the invocation stream).
type itAgent struct {
	reply string
	err   string
	block bool
}

func itReply(s string) itAgent { return itAgent{reply: s} }
func itErr(s string) itAgent   { return itAgent{err: s} }

var itBlock = itAgent{block: true}

// itFault fails count calls of a store method with SQLSTATE code, starting at the first call
// (armOn "") or at the first call after the armOn point ("Method:n"). The failed call returns a
// *pgconn.PgError carrying code without reaching Postgres, so classifyStoreError sees the same
// SQLSTATE a real failure carries; every other call runs the production SQL.
type itFault struct {
	method string
	code   string
	count  int
	armOn  string
}

// itStep is a test action taken at a point: a gate ("agent:n" or "<StoreMethod>:n", the n-th
// call blocks until every consecutive step at that point ran) or "waiting" (the coordinator's
// active turn is a rescheduled Waiting turn).
type itStep struct {
	at   string
	do   string // enqueue, cancel, restart, outage, awaitQueued, awaitWaiting
	ids  []string
	skip bool // enqueue with SkipIfActive
}

type itRow struct {
	status  string // "" = no row
	errSub  string
	reply   string
	retry   int
	restart int
}

type itThreadCase struct {
	name            string
	seed            []string
	enqueue         []string
	agent           []itAgent
	preTurn         []string
	faults          []itFault
	steps           []itStep
	wantRows        map[string]itRow
	wantRequeued    map[string]int
	wantDone        []TurnOutcome
	wantDelivered   []string
	wantAgentCalls  int
	wantKilled      int
	wantMinHookGap  time.Duration
	wantNoFailWrite bool
}

type itDispatchCase struct {
	name              string
	enqueue           []string
	agent             []itAgent
	steps             []itStep
	wantRows          map[string]itRow
	wantDone          []TurnOutcome
	wantPublished     []string
	wantEnqueueStatus map[string]string
	wantAgentCalls    int
}

func TestCoordinatorRunnerIntegration(t *testing.T) {
	t.Parallel()
	tests := turnQueueCases()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runIntegrationCase(t, tc)
		})
	}
}

func TestCoordinatorRunnerIntegration_Dispatches(t *testing.T) {
	t.Parallel()
	tests := turnQueueDispatchCases()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDispatchIntegrationCase(t, tc)
		})
	}
}

// turnQueueCases run on one coordinator deployment for thread bursts.
func turnQueueCases() []itThreadCase {
	completed, failed := db.StatusCompleted, db.StatusFailed
	return []itThreadCase{
		{
			name: "BurstHappyPath", enqueue: []string{"a"}, agent: []itAgent{itReply("reply-a")},
			wantRows:      map[string]itRow{"a": {status: completed, reply: "reply-a", retry: 1}},
			wantDone:      []TurnOutcome{OutcomeCompleted},
			wantDelivered: []string{"reply-a"}, wantAgentCalls: 1,
		},
		{
			name: "CoalescesFollowUpsUpToFive", enqueue: []string{"a"},
			agent: []itAgent{itReply("reply-a"), itReply("reply-2"), itReply("reply-3")},
			steps: []itStep{{at: "agent:1", do: "enqueue", ids: []string{"b", "c", "d", "e", "f", "g"}}},
			wantRows: map[string]itRow{
				"a": {status: completed, reply: "reply-a", retry: 1},
				"b": {status: completed, reply: "reply-2", retry: 1}, "c": {status: completed, reply: "reply-2", retry: 1},
				"d": {status: completed, reply: "reply-2", retry: 1}, "e": {status: completed, reply: "reply-2", retry: 1},
				"f": {status: completed, reply: "reply-2", retry: 1}, "g": {status: completed, reply: "reply-3", retry: 1},
			},
			wantDone:      []TurnOutcome{OutcomeCompleted, OutcomeCompleted, OutcomeCompleted},
			wantDelivered: []string{"reply-a", "reply-2", "reply-3"}, wantAgentCalls: 3,
		},
		{
			name: "CancelMidRunStartsNextBurst", enqueue: []string{"a"}, agent: []itAgent{itBlock, itReply("reply-b")},
			steps: []itStep{{at: "agent:1", do: "cancel"}, {at: "agent:1", do: "enqueue", ids: []string{"b"}}},
			wantRows: map[string]itRow{
				"a": {status: failed, errSub: CancelledByOperatorReason, retry: 1},
				"b": {status: completed, reply: "reply-b", retry: 1},
			},
			wantDone:      []TurnOutcome{OutcomeCancelled, OutcomeCompleted},
			wantDelivered: []string{"reply-b"}, wantAgentCalls: 2, wantKilled: 1,
		},
		{
			name: "CrashRightAfterCancelFinalizesOnce", enqueue: []string{"a"}, agent: []itAgent{itBlock},
			steps:    []itStep{{at: "agent:1", do: "cancel"}, {at: "FailMessagesIfActive:1", do: "restart"}},
			wantRows: map[string]itRow{"a": {status: failed, errSub: CancelledByOperatorReason, retry: 1}},
			wantDone: []TurnOutcome{OutcomeCancelled}, wantAgentCalls: 1, wantKilled: 1,
		},
		{
			name: "PreTurnRetryLaterKicksAfterDelay", preTurn: []string{"retry", "allow"}, enqueue: []string{"a"}, agent: []itAgent{itReply("reply-ab")},
			steps: []itStep{
				{at: "waiting", do: "enqueue", ids: []string{"b"}},
				{at: "waiting", do: "awaitWaiting", ids: []string{"a", "b"}},
			},
			wantRequeued: map[string]int{"a": 1},
			wantRows: map[string]itRow{
				"a": {status: completed, reply: "reply-ab", retry: 2},
				"b": {status: completed, reply: "reply-ab", retry: 2},
			},
			wantDone:      []TurnOutcome{OutcomeReschedule, OutcomeCompleted},
			wantDelivered: []string{"reply-ab"}, wantAgentCalls: 1, wantMinHookGap: itPreTurnDelay,
		},
		{
			name: "RecordStartedRetriedAfterPartialClaim", seed: []string{"a", "b", "c"}, agent: []itAgent{itReply("reply-abc")},
			steps: []itStep{{at: "ClaimPendingMessage:2", do: "restart"}},
			wantRows: map[string]itRow{
				"a": {status: completed, reply: "reply-abc", retry: 1},
				"b": {status: completed, reply: "reply-abc", retry: 1},
				"c": {status: completed, reply: "reply-abc", retry: 1},
			},
			wantDone:      []TurnOutcome{OutcomeCompleted},
			wantDelivered: []string{"reply-abc"}, wantAgentCalls: 1,
		},
		{
			name: "CancelBeforeClaimFailsActiveIDs", enqueue: []string{"a"},
			steps:    []itStep{{at: "ClaimPendingMessage:1", do: "cancel"}},
			wantRows: map[string]itRow{"a": {status: failed, errSub: CancelledByOperatorReason}},
			wantDone: []TurnOutcome{OutcomeCancelled},
		},
		{
			name: "BurstAttemptTimeoutNeverRetried", enqueue: []string{"a"}, agent: []itAgent{itErr(itWatchdogErr), itReply("reply-b")},
			steps: []itStep{{at: "agent:1", do: "enqueue", ids: []string{"b"}}},
			wantRows: map[string]itRow{
				"a": {status: failed, errSub: attemptTimeoutReason, retry: 1},
				"b": {status: completed, reply: "reply-b", retry: 1},
			},
			wantDone:      []TurnOutcome{OutcomeTimeout, OutcomeCompleted},
			wantDelivered: []string{"execution timed out", "reply-b"}, wantAgentCalls: 2,
		},
		{
			name: "OutageLongerThanOldPolicyResumes", enqueue: []string{"a"}, agent: []itAgent{itBlock, itReply("reply-a")},
			steps:         []itStep{{at: "agent:1", do: "outage"}},
			wantRows:      map[string]itRow{"a": {status: completed, reply: "reply-a", retry: 1, restart: 1}},
			wantDone:      []TurnOutcome{OutcomeCompleted},
			wantDelivered: []string{"reply-a"}, wantNoFailWrite: true, wantAgentCalls: 2, wantKilled: 1,
		},
		{
			name: "PostgresDownRetriesUntilRecovery", enqueue: []string{"a"}, agent: []itAgent{itReply("reply-a")},
			faults:        []itFault{{method: "ClaimPendingMessage", code: "08006", count: 50}},
			wantRows:      map[string]itRow{"a": {status: completed, reply: "reply-a", retry: 1}},
			wantDone:      []TurnOutcome{OutcomeCompleted},
			wantDelivered: []string{"reply-a"}, wantNoFailWrite: true, wantAgentCalls: 1,
		},
		{
			name: "TerminalRecordStartedFailsTurn", enqueue: []string{"a"}, agent: []itAgent{itReply("reply-b")},
			faults: []itFault{{method: "ClaimPendingMessage", code: "23505", count: 1}},
			steps:  []itStep{{at: "ClaimPendingMessage:1", do: "enqueue", ids: []string{"b"}}},
			wantRows: map[string]itRow{
				"a": {status: failed, errSub: "23505"},
				"b": {status: completed, reply: "reply-b", retry: 1},
			},
			wantDone:      []TurnOutcome{OutcomeFailed, OutcomeCompleted},
			wantDelivered: []string{"NOTICE: execution failed", "reply-b"}, wantAgentCalls: 1,
		},
		{
			name: "TerminalRecordOutcomeNotRetried", enqueue: []string{"a"}, agent: []itAgent{itErr(itNonTransientErr)},
			faults:        []itFault{{method: "GetMessagesByID", code: "23505", count: 1, armOn: "FailMessagesIfActive:1"}},
			wantRows:      map[string]itRow{"a": {status: failed, errSub: itNonTransientErr, retry: 1}},
			wantDone:      []TurnOutcome{OutcomeFailed},
			wantDelivered: []string{"non-transient error"}, wantAgentCalls: 1,
		},
		{
			name: "StreamDropMidAttemptRerunsOnce", enqueue: []string{"a"}, agent: []itAgent{itBlock, itReply("reply-a")},
			steps:         []itStep{{at: "agent:1", do: "restart"}},
			wantRows:      map[string]itRow{"a": {status: completed, reply: "reply-a", retry: 1, restart: 1}},
			wantDone:      []TurnOutcome{OutcomeCompleted},
			wantDelivered: []string{"reply-a"}, wantNoFailWrite: true, wantAgentCalls: 2, wantKilled: 1,
		},
	}
}

// turnQueueDispatchCases run on one coordinator deployment for dispatch sessions.
func turnQueueDispatchCases() []itDispatchCase {
	completed, failed := db.StatusCompleted, db.StatusFailed
	return []itDispatchCase{
		{
			name: "DispatchAttemptTimeoutPublishesTimeout", enqueue: []string{"d1"}, agent: []itAgent{itErr(itWatchdogErr), itReply("reply-d2")},
			steps: []itStep{{at: "agent:1", do: "enqueue", ids: []string{"d2"}}},
			wantRows: map[string]itRow{
				"d1": {status: failed, errSub: attemptTimeoutReason, retry: 1},
				"d2": {status: completed, retry: 1},
			},
			wantDone:      []TurnOutcome{OutcomeTimeout, OutcomeCompleted},
			wantPublished: []string{"timeout", "completed"}, wantAgentCalls: 2,
		},
		{
			name: "DispatchHappyPath", enqueue: []string{"d1"}, agent: []itAgent{itReply("reply-d1")},
			wantRows:      map[string]itRow{"d1": {status: completed, retry: 1}},
			wantDone:      []TurnOutcome{OutcomeCompleted},
			wantPublished: []string{"completed"}, wantAgentCalls: 1,
		},
		{
			name: "DispatchSkipIfActiveAnswersSkipped", enqueue: []string{"d1"}, agent: []itAgent{itReply("reply-d1")},
			steps:             []itStep{{at: "agent:1", do: "enqueue", ids: []string{"d2"}, skip: true}},
			wantRows:          map[string]itRow{"d1": {status: completed, retry: 1}, "d2": {}},
			wantEnqueueStatus: map[string]string{"d2": eriwire.StatusSkippedActive},
			wantDone:          []TurnOutcome{OutcomeCompleted},
			wantPublished:     []string{"completed"}, wantAgentCalls: 1,
		},
	}
}

func runIntegrationCase(t *testing.T, tc itThreadCase) {
	t.Helper()
	env := newThreadITEnv(t, tc)
	env.primary = env.definitions()
	env.h = restatetest.StartWithOptions(t, restatetest.Options{Env: itServerEnv}, env.primary...)

	if len(tc.seed) > 0 {
		env.seedRows(tc.seed)
		env.h.Send(env.service(), env.key(), HandlerAppend, AppendRequest{IDs: tc.seed})
	}
	for _, id := range tc.enqueue {
		env.enqueue(id, false)
	}
	env.runSteps()
	env.awaitIdle("the key to go idle with every row recorded", env.idle)
	env.assertCase()
}

func runDispatchIntegrationCase(t *testing.T, tc itDispatchCase) {
	t.Helper()
	env := newDispatchITEnv(t, tc)
	env.primary = env.definitions()
	env.h = restatetest.StartWithOptions(t, restatetest.Options{Env: itServerEnv}, env.primary...)

	for _, id := range tc.enqueue {
		env.enqueue(id, false)
	}
	env.runSteps()
	env.awaitIdle("the key to go idle with every row recorded", env.idle)
	env.assertCase()
}

// itEnv is one case: the scripted boundaries every deployment's WorkerPool shares, and what the
// test observed through them.
type itEnv struct {
	t     *testing.T
	h     *restatetest.Harness
	store *itStore
	sqlDB *sql.DB // the case's private Postgres database, for seeding and the FAILED-write audit
	bus   pubsub.Bus
	outs  *outcomeRecorder
	gates map[string]*itGate

	primary []restate.ServiceDefinition

	mu            sync.Mutex
	agentCalls    int
	running       int
	maxRunning    int
	killed        int
	delivered     []string
	hookCalls     []time.Time
	doneByInv     map[string]TurnDoneRequest
	doneOrder     []string
	enqueueStatus map[string]string

	dispatch          bool
	agent             []itAgent
	preTurn           []string
	steps             []itStep
	wantRows          map[string]itRow
	wantRequeued      map[string]int
	wantDone          []TurnOutcome
	wantDelivered     []string
	wantPublished     []string
	wantEnqueueStatus map[string]string
	wantAgentCalls    int
	wantKilled        int
	wantMinHookGap    time.Duration
	wantNoFailWrite   bool
}

type itGate struct {
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func newThreadITEnv(t *testing.T, tc itThreadCase) *itEnv {
	t.Helper()
	bus := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = bus.Close() })
	env := &itEnv{
		t: t, bus: bus, outs: newOutcomeRecorder(t, bus),
		gates: map[string]*itGate{}, doneByInv: map[string]TurnDoneRequest{}, enqueueStatus: map[string]string{},
		dispatch:        false,
		agent:           tc.agent,
		preTurn:         tc.preTurn,
		steps:           tc.steps,
		wantRows:        tc.wantRows,
		wantRequeued:    tc.wantRequeued,
		wantDone:        tc.wantDone,
		wantDelivered:   tc.wantDelivered,
		wantAgentCalls:  tc.wantAgentCalls,
		wantKilled:      tc.wantKilled,
		wantMinHookGap:  tc.wantMinHookGap,
		wantNoFailWrite: tc.wantNoFailWrite,
	}
	sqlStore, sqlDB := newTestStore(t)
	execSQL(t, sqlDB, itFailedWritesSQL)
	env.sqlDB = sqlDB
	env.store = &itStore{Store: sqlStore, env: env, calls: map[string]int{}}
	for _, f := range tc.faults {
		env.store.faults = append(env.store.faults, &itFaultState{itFault: f, armed: f.armOn == ""})
	}
	for _, st := range tc.steps {
		if isGatePoint(st.at) && env.gates[st.at] == nil {
			env.gates[st.at] = &itGate{reached: make(chan struct{}), release: make(chan struct{})}
		}
	}
	return env
}

func newDispatchITEnv(t *testing.T, tc itDispatchCase) *itEnv {
	t.Helper()
	bus := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = bus.Close() })
	env := &itEnv{
		t: t, bus: bus, outs: newOutcomeRecorder(t, bus),
		gates: map[string]*itGate{}, doneByInv: map[string]TurnDoneRequest{}, enqueueStatus: map[string]string{},
		dispatch:          true,
		agent:             tc.agent,
		steps:             tc.steps,
		wantRows:          tc.wantRows,
		wantDone:          tc.wantDone,
		wantPublished:     tc.wantPublished,
		wantEnqueueStatus: tc.wantEnqueueStatus,
		wantAgentCalls:    tc.wantAgentCalls,
	}
	sqlStore, sqlDB := newTestStore(t)
	execSQL(t, sqlDB, itFailedWritesSQL)
	env.sqlDB = sqlDB
	env.store = &itStore{Store: sqlStore, env: env, calls: map[string]int{}}
	for _, st := range tc.steps {
		if isGatePoint(st.at) && env.gates[st.at] == nil {
			env.gates[st.at] = &itGate{reached: make(chan struct{}), release: make(chan struct{})}
		}
	}
	return env
}

func isGatePoint(at string) bool { return strings.Contains(at, ":") }

// pass blocks the n-th call at point until the test released its gate.
func (e *itEnv) pass(point string) bool {
	g := e.gates[point]
	if g == nil {
		return false
	}
	g.once.Do(func() { close(g.reached) })
	select {
	case <-g.release:
	case <-time.After(itWait):
		e.t.Errorf("gate %s never released", point)
	}
	return true
}

func (e *itEnv) service() string {
	if e.dispatch {
		return DispatchSessionServiceName("")
	}
	return ThreadSessionServiceName("")
}

func (e *itEnv) key() string {
	if e.dispatch {
		return itSessionKey
	}
	return itThreadKey
}

// definitions is one deployment ("pod"): its own WorkerPool over the shared scripted boundaries,
// bound with the same services and handler names as production.
func (e *itEnv) definitions() []restate.ServiceDefinition {
	pool := e.newPool()
	hour := []restate.ServiceDefinitionOption{restate.WithInactivityTimeout(time.Hour), restate.WithAbortTimeout(time.Hour)}
	return []restate.ServiceDefinition{
		restate.Reflect(itThread{ThreadCoordinator: NewThreadCoordinator(e.store, pool, ""), env: e}, hour...),
		restate.Reflect(itDispatch{DispatchCoordinator: NewDispatchCoordinator(e.store, ""), env: e}, hour...),
		ReflectTurnRunner(NewTurnRunnerService(pool, "")),
	}
}

func (e *itEnv) newPool() *WorkerPool {
	preTurn := &config.WebhookEndpoint{URL: "http://127.0.0.1:1/pre-turn", TimeoutMs: 1000}
	pool := New(nil, WorkerPoolConfig{
		Store: e.store, MaxAttempts: 3, BackoffBase: 10 * time.Millisecond, RetryDelayOverride: itPreTurnDelay,
		EventBus: e.bus,
		ResolveChannelPolicy: func(string, string) config.ChannelPolicy {
			p := config.ChannelPolicy{Mode: "thread"}
			if len(e.preTurn) > 0 {
				p.Hooks.PreTurn = preTurn
			}
			return p
		},
		WebhookDispatcher: &mockWebhookDispatcher{callPreTurnHookFunc: e.preTurnHook},
		RunnerFunc:        e.runAgent,
		DeliveryFunc:      e.deliver,
		NotifierFunc:      func(_, _, desc string) string { return "NOTICE: " + desc },
	})
	pool.Start()
	e.t.Cleanup(pool.Stop)
	return pool
}

func (e *itEnv) runAgent(ctx context.Context, _, _, _, _, _ string, _ int) (string, string, int, error) {
	e.mu.Lock()
	e.agentCalls++
	n := e.agentCalls
	e.mu.Unlock()
	e.pass("agent:" + strconv.Itoa(n))
	if n > len(e.agent) {
		return "", "", 1, fmt.Errorf("unscripted agent run %d", n)
	}
	sc := e.agent[n-1]
	if sc.block && ctx.Err() != nil {
		// Already killed (stop-turn or a dropped stream) before it got to run.
		e.mu.Lock()
		e.killed++
		e.mu.Unlock()
		return "", "", 1, errors.New(itKilledErr)
	}
	e.mu.Lock()
	e.running++
	e.maxRunning = max(e.maxRunning, e.running)
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		e.running--
		e.mu.Unlock()
	}()
	switch {
	case sc.block:
		<-ctx.Done()
		e.mu.Lock()
		e.killed++
		e.mu.Unlock()
		return "", "", 1, errors.New(itKilledErr)
	case sc.err != "":
		return "", "", 1, errors.New(sc.err)
	default:
		return mockJSONResponseWithUsage(attemptConvID, sc.reply, runner.TokenUsage{InputTokens: 3, OutputTokens: 2, TotalTokens: 5}), "", 0, nil
	}
}

func (e *itEnv) deliver(_ *discordgo.Session, _, text string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.delivered = append(e.delivered, text)
	return nil
}

func (e *itEnv) preTurnHook(context.Context, *config.WebhookEndpoint, *PreTurnRequest) (*PreTurnResponse, error) {
	e.mu.Lock()
	e.hookCalls = append(e.hookCalls, time.Now())
	n := len(e.hookCalls)
	e.mu.Unlock()
	if n <= len(e.preTurn) && e.preTurn[n-1] == "retry" {
		return &PreTurnResponse{Allow: false, Action: "retry"}, nil
	}
	return &PreTurnResponse{Allow: true}, nil
}

// recordDone records one OnTurnDone invocation; a replay of the same invocation is not a second report.
func (e *itEnv) recordDone(invocationID string, req TurnDoneRequest) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, seen := e.doneByInv[invocationID]; seen {
		return
	}
	e.doneByInv[invocationID] = req
	e.doneOrder = append(e.doneOrder, invocationID)
}

// seedRows inserts ids PENDING on itThreadKey through the fixture seed helper (production
// InsertMessage on the case's database), bypassing the coordinator and the store decorator.
func (e *itEnv) seedRows(ids []string) {
	e.t.Helper()
	msgs := make([]db.Message, 0, len(ids))
	for _, id := range ids {
		msgs = append(msgs, db.Message{
			ID: id, ThreadID: itThreadKey, ChannelID: itThreadKey, AuthorID: "user-1", AuthorName: "user",
			Content: "hello " + id, Status: db.StatusPending, CreatedAt: time.Now().UTC(),
		})
	}
	sqlStore, ok := e.store.Store.(*db.SQLStore)
	if !ok {
		e.t.Fatalf("itStore wraps %T, want *db.SQLStore", e.store.Store)
	}
	seedMessageRows(e.t, sqlStore, msgs)
}

// itFailedWritesSQL records, in the case's private database, every messages row ever written
// FAILED, so wantNoFailWrite asserts persisted history rather than store call counts.
var itFailedWritesSQL = fmt.Sprintf(`CREATE TABLE it_failed_writes (id TEXT NOT NULL);
CREATE FUNCTION it_record_failed_write() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	INSERT INTO it_failed_writes (id) VALUES (NEW.id);
	RETURN NEW;
END $$;
CREATE TRIGGER it_record_failed_write AFTER INSERT OR UPDATE OF status ON messages
	FOR EACH ROW WHEN (NEW.status = '%s') EXECUTE FUNCTION it_record_failed_write();`, db.StatusFailed)

// failedWrites lists the message ids ever written FAILED in this case.
func (e *itEnv) failedWrites() []string {
	e.t.Helper()
	rows, err := e.sqlDB.QueryContext(context.Background(), "SELECT id FROM it_failed_writes ORDER BY id")
	if err != nil {
		e.t.Fatalf("query FAILED writes: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			e.t.Fatalf("scan FAILED write: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		e.t.Fatalf("read FAILED writes: %v", err)
	}
	return ids
}

func (e *itEnv) enqueue(id string, skip bool) {
	e.t.Helper()
	var status string
	if e.dispatch {
		var resp eriwire.DispatchEnqueueResponse
		req := eriwire.BrainDispatchRequest{ID: id, Source: "test", Prompt: "do " + id, SkipIfActive: skip}
		if code := e.h.Call(e.service(), e.key(), HandlerEnqueue, req, &resp); code != 200 {
			e.t.Fatalf("enqueue %s: status %d", id, code)
		}
		status = resp.Status
	} else {
		var resp ThreadEnqueueResponse
		msg := &db.Message{ID: id, ThreadID: itThreadKey, ChannelID: itThreadKey, AuthorID: "user-1", AuthorName: "user", Content: "hello " + id, CreatedAt: time.Now().UTC()}
		if code := e.h.Call(e.service(), e.key(), HandlerEnqueue, ThreadEnqueueRequest{Message: msg, SkipIfActive: skip}, &resp); code != 200 {
			e.t.Fatalf("enqueue %s: status %d", id, code)
		}
		status = resp.Status
	}
	e.mu.Lock()
	e.enqueueStatus[id] = status
	e.mu.Unlock()
}

func (e *itEnv) status() CoordinatorStatus {
	e.t.Helper()
	var st CoordinatorStatus
	if code := e.h.Call(e.service(), e.key(), HandlerStatus, nil, &st); code != 200 {
		e.t.Fatalf("status: %d", code)
	}
	return st
}

// runSteps takes every step at its point; a gate is released once the consecutive steps at it ran.
func (e *itEnv) runSteps() {
	e.t.Helper()
	steps := e.steps
	for i := 0; i < len(steps); {
		at := steps[i].at
		j := i
		for j < len(steps) && steps[j].at == at {
			j++
		}
		e.waitPoint(at)
		for _, st := range steps[i:j] {
			e.do(st)
		}
		if g := e.gates[at]; g != nil {
			close(g.release)
		}
		i = j
	}
}

func (e *itEnv) waitPoint(at string) {
	e.t.Helper()
	switch {
	case isGatePoint(at):
		select {
		case <-e.gates[at].reached:
		case <-time.After(itWait):
			e.t.Fatalf("point %s never reached", at)
		}
	case at == "waiting":
		e.h.Eventually("a Waiting (rescheduled) active turn", itWait, func() bool {
			st := e.status()
			return st.Active != nil && st.Active.Waiting
		})
		e.assertRequeued()
	default:
		e.t.Fatalf("unknown point %q", at)
	}
}

func (e *itEnv) do(st itStep) {
	e.t.Helper()
	switch st.do {
	case "enqueue":
		for _, id := range st.ids {
			e.enqueue(id, st.skip)
		}
	case "cancel":
		e.cancel()
	case "restart":
		e.h.RestartEndpoint(e.primary...)
	case "outage":
		e.h.StopEndpoint()
		e.h.WaitDownAttempts(itOutageAttempts, itWait)
		e.t.Logf("endpoint down for %d invocation attempts", e.h.DownAttempts())
		e.h.RestartEndpoint(e.primary...)
	case "awaitQueued":
		e.h.Eventually(fmt.Sprintf("%v queued on the coordinator", st.ids), itWait, func() bool {
			return containsAll(e.status().Pending, st.ids)
		})
	case "awaitWaiting":
		e.h.Eventually(fmt.Sprintf("%v pending behind the Waiting turn", st.ids), itWait, func() bool {
			s := e.status()
			return s.Active != nil && s.Active.Waiting && containsAll(s.Pending, st.ids)
		})
	default:
		e.t.Fatalf("unknown step action %q", st.do)
	}
}

// cancel cancels through the coordinator and waits until ClearPending acted on the active turn.
func (e *itEnv) cancel() {
	e.t.Helper()
	prev := ""
	if st := e.status(); st.Active != nil {
		prev = st.Active.TurnID
	}
	var code int
	if e.dispatch {
		code = e.h.Call(e.service(), e.key(), HandlerCancel, CancelDispatchRequest{}, nil)
	} else {
		code = e.h.Call(e.service(), e.key(), HandlerCancelTurn, CancelTurnRequest{}, nil)
	}
	if code != 200 {
		e.t.Fatalf("cancel: status %d", code)
	}
	e.h.Eventually("ClearPending to end the cancelled turn", itWait, func() bool {
		st := e.status()
		return st.Active == nil || st.Active.TurnID != prev
	})
}

func containsAll(have, want []string) bool {
	for _, id := range want {
		if !slices.Contains(have, id) {
			return false
		}
	}
	return true
}

// rowStatus returns an item's status and row, "" when it has no row.
func (e *itEnv) row(id string) (itRow, bool) {
	ctx := context.Background()
	if e.dispatch {
		d, err := e.store.GetDispatch(ctx, id)
		if err != nil || d == nil {
			return itRow{}, false
		}
		return itRow{status: d.Status, errSub: d.ErrorMessage, reply: "", retry: d.RetryCount, restart: d.RestartCount}, true
	}
	m, err := e.store.GetMessage(ctx, id)
	if err != nil || m == nil {
		return itRow{}, false
	}
	return itRow{status: m.Status, errSub: m.ErrorMessage, reply: m.ResponseText, retry: m.RetryCount, restart: m.RestartCount}, true
}

// idle: every expected row reached its final status, every expected OnTurnDone arrived and the
// key is quiescent. A runner completes a few ms before Restate creates the OnTurnDone it sent
// (cross-partition send), so quiescence alone can pass in that window.
func (e *itEnv) idle() bool {
	for id, want := range e.wantRows {
		if got, ok := e.row(id); ok != (want.status != "") || got.status != want.status {
			return false
		}
	}
	e.mu.Lock()
	done := len(e.doneOrder)
	e.mu.Unlock()
	if done < len(e.wantDone) {
		return false
	}
	return e.quiescent()
}

// quiescent: no row of the case is still PENDING or PROCESSING, the key holds no Restate state
// and no invocation of any service is in flight (so nothing is left to happen).
func (e *itEnv) quiescent() bool {
	for id := range e.wantRows {
		if got, ok := e.row(id); ok && isActiveStatus(got.status) {
			return false
		}
	}
	if len(e.stateKeys()) > 0 {
		return false
	}
	return len(e.h.Query("SELECT id FROM sys_invocation WHERE status <> 'completed'")) == 0
}

// awaitIdle waits for idle and, on timeout, dumps what is still open before failing.
func (e *itEnv) awaitIdle(what string, cond func() bool) {
	e.t.Helper()
	if restatetest.WaitFor(itWait, cond) {
		return
	}
	for id := range e.wantRows {
		got, ok := e.row(id)
		e.t.Logf("row %s (exists=%v): %+v", id, ok, got)
	}
	e.t.Logf("state keys: %v", e.stateKeys())
	e.t.Logf("open invocations: %v", e.h.Query("SELECT id, target, status, last_failure FROM sys_invocation WHERE status <> 'completed'"))
	e.t.Fatalf("timed out after %s waiting for %s", itWait, what)
}

func (e *itEnv) stateKeys() []string {
	rows := e.h.Query(fmt.Sprintf("SELECT key FROM state WHERE service_name = '%s' AND service_key = '%s'", e.service(), e.key()))
	keys := make([]string, 0, len(rows))
	for _, r := range rows {
		keys = append(keys, fmt.Sprint(r["key"]))
	}
	return keys
}

func (e *itEnv) assertRequeued() {
	e.t.Helper()
	for id, retry := range e.wantRequeued {
		got, ok := e.row(id)
		if !ok || got.status != db.StatusPending || got.retry != retry {
			e.t.Errorf("row %s while waiting = {%s retry=%d}, want {PENDING retry=%d}", id, got.status, got.retry, retry)
		}
	}
}

func (e *itEnv) assertCase() {
	e.t.Helper()
	e.assertRows()
	if keys := e.stateKeys(); len(keys) > 0 {
		e.t.Errorf("coordinator state keys left: %v", keys)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.assertDone()
	e.assertDeliveries()
	e.assertAgent()
	for id, want := range e.wantEnqueueStatus {
		if got := e.enqueueStatus[id]; got != want {
			e.t.Errorf("enqueue %s answered %q, want %q", id, got, want)
		}
	}
	if published := e.publishedStatuses(); !slices.Equal(published, e.wantPublished) {
		e.t.Errorf("published dispatch outcomes = %v, want %v", published, e.wantPublished)
	}
	e.store.assertFaults(e.t)
	if failed := e.failedWrites(); e.wantNoFailWrite && len(failed) > 0 {
		e.t.Errorf("rows %v were written FAILED, want no FAILED write", failed)
	}
}

func (e *itEnv) assertRows() {
	e.t.Helper()
	for id, want := range e.wantRows {
		got, ok := e.row(id)
		if want.status == "" {
			if ok {
				e.t.Errorf("row %s exists (%+v), want none", id, got)
			}
			continue
		}
		if got.status != want.status || !strings.Contains(got.errSub, want.errSub) || (!e.dispatch && got.reply != want.reply) || got.retry != want.retry || got.restart != want.restart {
			e.t.Errorf("row %s = %+v, want %+v", id, got, want)
		}
	}
}

// assertDone checks the OnTurnDone outcomes in arrival order and that no turn reported twice.
func (e *itEnv) assertDone() {
	e.t.Helper()
	outcomes := make([]TurnOutcome, 0, len(e.doneOrder))
	perTurn := map[string]int{}
	for _, inv := range e.doneOrder {
		req := e.doneByInv[inv]
		outcomes = append(outcomes, req.Outcome)
		perTurn[req.TurnID]++
	}
	if !slices.Equal(outcomes, e.wantDone) {
		e.t.Errorf("OnTurnDone outcomes = %v, want %v", outcomes, e.wantDone)
	}
	for turn, n := range perTurn {
		if n != 1 {
			e.t.Errorf("turn %s reported OnTurnDone %d times, want once", turn, n)
		}
	}
}

func (e *itEnv) assertDeliveries() {
	e.t.Helper()
	if len(e.delivered) != len(e.wantDelivered) {
		e.t.Errorf("deliveries = %q, want %d matching %q", e.delivered, len(e.wantDelivered), e.wantDelivered)
		return
	}
	for i, sub := range e.wantDelivered {
		if !strings.Contains(e.delivered[i], sub) {
			e.t.Errorf("delivery %d = %q, want it to contain %q", i, e.delivered[i], sub)
		}
	}
}

func (e *itEnv) assertAgent() {
	e.t.Helper()
	if e.agentCalls != e.wantAgentCalls || e.killed != e.wantKilled {
		e.t.Errorf("agent runs = %d (killed %d), want %d (killed %d)", e.agentCalls, e.killed, e.wantAgentCalls, e.wantKilled)
	}
	if e.maxRunning > 1 {
		e.t.Errorf("%d agent runs overlapped on one key, want at most 1", e.maxRunning)
	}
	if gap := e.wantMinHookGap; gap > 0 && (len(e.hookCalls) < 2 || e.hookCalls[1].Sub(e.hookCalls[0]) < gap) {
		e.t.Errorf("pre_turn calls at %v, want the second at least %s after the first", e.hookCalls, gap)
	}
}

func (e *itEnv) publishedStatuses() []string {
	e.outs.mu.Lock()
	defer e.outs.mu.Unlock()
	out := make([]string, 0, len(e.outs.delivered))
	for _, d := range e.outs.delivered {
		out = append(out, d.status)
	}
	return out
}

// itStore decorates the production SQLStore over the case's private Postgres database: every read
// and write runs the production SQL, and the decorator only schedules scripted SQLSTATE faults and
// gates on the calls the cases target. calls numbers those calls for "Method:n" points; it is
// never asserted on.
type itStore struct {
	db.Store

	env *itEnv

	mu     sync.Mutex
	calls  map[string]int
	faults []*itFaultState
}

type itFaultState struct {
	itFault

	armed    bool
	injected int
}

// hit numbers one call, applies any armed fault, arms faults waiting on this call and passes the
// call's gate. A gated call whose context ended while it waited (its stream dropped) returns the
// context error, as a real Postgres call would.
func (s *itStore) hit(ctx context.Context, method string) error {
	s.mu.Lock()
	s.calls[method]++
	point := method + ":" + strconv.Itoa(s.calls[method])
	var err error
	for _, f := range s.faults {
		if f.armed && f.method == method && f.injected < f.count {
			f.injected++
			err = &pgconn.PgError{Code: f.code, Message: "scripted store failure"}
		}
	}
	for _, f := range s.faults {
		if !f.armed && f.armOn == point {
			f.armed = true
		}
	}
	s.mu.Unlock()
	if s.env.pass(point) && err == nil {
		return ctx.Err()
	}
	return err
}

func (s *itStore) assertFaults(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.faults {
		if f.injected != f.count {
			t.Errorf("fault %s/%s injected %d times, want %d", f.method, f.code, f.injected, f.count)
		}
	}
}

func (s *itStore) ClaimPendingMessage(ctx context.Context, id string) (bool, error) {
	if err := s.hit(ctx, "ClaimPendingMessage"); err != nil {
		return false, err
	}
	return s.Store.ClaimPendingMessage(ctx, id)
}

func (s *itStore) GetMessagesByID(ctx context.Context, ids []string) ([]db.Message, error) {
	if err := s.hit(ctx, "GetMessagesByID"); err != nil {
		return nil, err
	}
	return s.Store.GetMessagesByID(ctx, ids)
}

func (s *itStore) FailMessagesIfActive(ctx context.Context, ids []string, reason string) ([]string, error) {
	if err := s.hit(ctx, "FailMessagesIfActive"); err != nil {
		return nil, err
	}
	return s.Store.FailMessagesIfActive(ctx, ids, reason)
}

// Coordinator wrappers record every OnTurnDone a deployment receives, then run the real handler.

type itThread struct {
	*ThreadCoordinator

	env *itEnv
}

func (c itThread) OnTurnDone(ctx restate.ObjectContext, req TurnDoneRequest) error {
	c.env.recordDone(ctx.Request().ID, req)
	return c.ThreadCoordinator.OnTurnDone(ctx, req)
}

type itDispatch struct {
	*DispatchCoordinator

	env *itEnv
}

func (c itDispatch) OnTurnDone(ctx restate.ObjectContext, req TurnDoneRequest) error {
	c.env.recordDone(ctx.Request().ID, req)
	return c.DispatchCoordinator.OnTurnDone(ctx, req)
}
