package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/bwmarrin/discordgo"
)

type mockErrSessionStore struct {
	db.Store
	findErr error
}

func (m *mockErrSessionStore) FindUnrotatedSessions(ctx context.Context, minTurns int, maxAge ...time.Duration) ([]db.SessionInfo, error) {
	if m.findErr != nil {
		return nil, m.findErr
	}
	if m.Store != nil {
		return m.Store.FindUnrotatedSessions(ctx, minTurns, maxAge...)
	}
	return nil, nil
}

func TestWorkerPool_CheckUnrotatedSessions_Scenarios(t *testing.T) {
	tests := []struct {
		name              string
		nilPool           bool
		nilStore          bool
		storeErr          error
		cancelContext     bool
		dormantBackdate   time.Duration
		sessionTurns      []int
		expectAlertCalls  int
		expectReportCount int
		expectOverflowMsg string
	}{
		{name: "nil worker pool", nilPool: true},
		{name: "nil store", nilStore: true},
		{name: "store error handled gracefully", storeErr: errors.New("database connection failed")},
		{name: "cancelled context exits early", cancelContext: true},
		{name: "excludes dormant sessions older than horizon", sessionTurns: []int{15}, dormantBackdate: 3 * time.Hour},
		{name: "top 5 display cap with overflow message", sessionTurns: []int{10, 11, 12, 13, 14, 15, 16}, expectAlertCalls: 1, expectReportCount: 7, expectOverflowMsg: "...and 2 more runaway session(s)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.nilPool {
				var nilPool *WorkerPool
				nilPool.checkUnrotatedSessions(context.Background(), make(map[string]int))
				return
			}
			if tc.nilStore {
				p := &WorkerPool{cfg: WorkerPoolConfig{}}
				p.checkUnrotatedSessions(context.Background(), make(map[string]int))
				return
			}

			ctx := context.Background()
			if tc.cancelContext {
				cCtx, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cCtx
			}

			var mu sync.Mutex
			var alertCalls int
			var lastBody string
			recordAlert := func(s *discordgo.Session, channelNameOrID, title, alertBody string) error {
				mu.Lock()
				defer mu.Unlock()
				alertCalls++
				lastBody = alertBody
				return nil
			}

			var store db.Store
			if tc.storeErr != nil {
				store = &mockErrSessionStore{findErr: tc.storeErr}
			} else {
				fake := db.NewFakeStore()
				defer func() { _ = fake.Close() }()
				store = fake
				for i, turns := range tc.sessionTurns {
					thID := fmt.Sprintf("th-scen-%d", i+1)
					_ = fake.SaveSessionID(context.Background(), thID, fmt.Sprintf("sess-scen-%d", i+1))
					for j := 0; j < turns; j++ {
						_, _ = fake.IncrementSessionTurnCount(context.Background(), thID)
					}
					if tc.dormantBackdate > 0 {
						fake.SetSessionUpdatedAt(thID, time.Now().UTC().Add(-tc.dormantBackdate))
					}
				}
			}

			pool := &WorkerPool{cfg: WorkerPoolConfig{Store: store, SystemAlertFunc: recordAlert}}
			lastReported := make(map[string]int)
			pool.checkUnrotatedSessions(ctx, lastReported)

			mu.Lock()
			calls := alertCalls
			body := lastBody
			mu.Unlock()

			if calls != tc.expectAlertCalls {
				t.Fatalf("expected %d alert calls, got %d", tc.expectAlertCalls, calls)
			}
			if tc.expectReportCount > 0 && len(lastReported) != tc.expectReportCount {
				t.Fatalf("expected %d recorded sessions, got %d", tc.expectReportCount, len(lastReported))
			}
			if tc.expectOverflowMsg != "" && !strings.Contains(body, tc.expectOverflowMsg) {
				t.Fatalf("expected overflow message %q in alert body:\n%s", tc.expectOverflowMsg, body)
			}
		})
	}
}

func TestWorkerPool_CheckUnrotatedSessions_DeduplicationAndPruning(t *testing.T) {
	fake := db.NewFakeStore()
	defer func() { _ = fake.Close() }()
	ctx := context.Background()

	var mu sync.Mutex
	var alertCalls int
	var lastTitle, lastBody string
	recordAlert := func(s *discordgo.Session, channelNameOrID, title, alertBody string) error {
		mu.Lock()
		defer mu.Unlock()
		alertCalls++
		lastTitle = title
		lastBody = alertBody
		return nil
	}

	p := &WorkerPool{cfg: WorkerPoolConfig{Store: fake, SystemAlertFunc: recordAlert}}
	lastReported := make(map[string]int)

	// Step 1: Session below limit (5 turns) -> no alert
	_ = fake.SaveSessionID(ctx, "th-test", "sess-1")
	for i := 0; i < 5; i++ {
		_, _ = fake.IncrementSessionTurnCount(ctx, "th-test")
	}
	p.checkUnrotatedSessions(ctx, lastReported)
	mu.Lock()
	if alertCalls != 0 || len(lastReported) != 0 {
		t.Fatalf("expected 0 alerts, got %d", alertCalls)
	}
	mu.Unlock()

	// Step 2: Session breaches limit (9 turns) -> 1 alert sent
	for i := 0; i < 4; i++ {
		_, _ = fake.IncrementSessionTurnCount(ctx, "th-test")
	}
	p.checkUnrotatedSessions(ctx, lastReported)
	mu.Lock()
	if alertCalls != 1 || !strings.Contains(lastBody, "th-test") || !strings.Contains(lastBody, "**9** turns") || lastTitle != "Unrotated Runaway Sessions" {
		t.Fatalf("unexpected alert state: calls=%d body=%s title=%s", alertCalls, lastBody, lastTitle)
	}
	mu.Unlock()
	if lastReported["th-test"] != 9 {
		t.Errorf("expected lastReported['th-test']=9, got %d", lastReported["th-test"])
	}

	// Step 3: Session is dormant (still 9 turns) -> deduplication skips alert
	p.checkUnrotatedSessions(ctx, lastReported)
	mu.Lock()
	if alertCalls != 1 {
		t.Fatalf("expected alertCalls to remain 1, got %d", alertCalls)
	}
	mu.Unlock()

	// Step 4: Session takes another turn unrotated (10 turns) -> 2nd alert sent
	_, _ = fake.IncrementSessionTurnCount(ctx, "th-test")
	p.checkUnrotatedSessions(ctx, lastReported)
	mu.Lock()
	if alertCalls != 2 || !strings.Contains(lastBody, "**10** turns") {
		t.Fatalf("expected 2 alerts with 10 turns, got calls=%d body=%s", alertCalls, lastBody)
	}
	mu.Unlock()
	if lastReported["th-test"] != 10 {
		t.Errorf("expected lastReported['th-test']=10, got %d", lastReported["th-test"])
	}

	// Step 5: Session rotates -> turn count reset to 0 in DB and pruned from lastReported
	_ = fake.RotateSessionID(ctx, "th-test", "sess-fresh")
	p.checkUnrotatedSessions(ctx, lastReported)
	mu.Lock()
	if alertCalls != 2 {
		t.Fatalf("expected no new alert on rotated session, got %d", alertCalls)
	}
	mu.Unlock()
	if _, exists := lastReported["th-test"]; exists {
		t.Errorf("expected rotated session pruned from lastReported: %v", lastReported)
	}
}

func TestWorkerPool_MaintenanceTicker_FiresCheck(t *testing.T) {
	fake := db.NewFakeStore()
	defer func() { _ = fake.Close() }()
	ctx := context.Background()
	_ = fake.SaveSessionID(ctx, "th-ticker-test", "sess-ticker")
	for i := 0; i < 11; i++ {
		_, _ = fake.IncrementSessionTurnCount(ctx, "th-ticker-test")
	}

	alertCh := make(chan string, 5)
	p := NewWorkerPool(WorkerPoolConfig{
		Store:               fake,
		MaintenanceInterval: 10 * time.Millisecond,
		SystemAlertFunc: func(s *discordgo.Session, channelNameOrID, title, alertBody string) error {
			select {
			case alertCh <- alertBody:
			default:
			}
			return nil
		},
	})
	p.Start()

	select {
	case body := <-alertCh:
		if !strings.Contains(body, "th-ticker-test") || !strings.Contains(body, "**11** turns") {
			t.Errorf("unexpected alert body: %s", body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for maintenance ticker to fire unrotated session check")
	}
	p.StopWithTimeout(1 * time.Second)
}

func TestFormatUnrotatedSessionLine(t *testing.T) {
	tests := []struct {
		name        string
		cachedChans []*discordgo.Channel
		session     db.SessionInfo
		expected    string
	}{
		{
			name:     "non-numeric snowflake fallback",
			session:  db.SessionInfo{ThreadID: "th-custom-session", TurnCount: 9, InternalSessionID: "sess-uuid-1234"},
			expected: "- Thread `th-custom-session`: **9** turns (session: `sess-uuid-1234`)",
		},
		{
			name:        "channel snowflake resolved via cache",
			cachedChans: []*discordgo.Channel{{ID: "1464358486849622120", Type: discordgo.ChannelTypeGuildText, Name: "general"}},
			session:     db.SessionInfo{ThreadID: "1464358486849622120", TurnCount: 10, InternalSessionID: "sess-chan-5678"},
			expected:    "- Channel <#1464358486849622120>: **10** turns (session: `sess-chan-5678`)",
		},
		{
			name: "thread snowflake with parent channel",
			cachedChans: []*discordgo.Channel{
				{ID: "1542423172400291873", Type: discordgo.ChannelTypeGuildText, Name: "aerial-dev"},
				{ID: "1557949887948005398", ParentID: "1542423172400291873", Type: discordgo.ChannelTypeGuildPublicThread, Name: "Resolve Notification Snowflake IDs"},
			},
			session:  db.SessionInfo{ThreadID: "1557949887948005398", TurnCount: 12, InternalSessionID: "sess-thread-9999"},
			expected: "- Thread <#1557949887948005398> (in <#1542423172400291873>): **12** turns (session: `sess-thread-9999`)",
		},
		{
			name:        "thread snowflake without parent channel",
			cachedChans: []*discordgo.Channel{{ID: "1557949887948005399", Type: discordgo.ChannelTypeGuildPublicThread, Name: "Orphan Thread"}},
			session:     db.SessionInfo{ThreadID: "1557949887948005399", TurnCount: 15, InternalSessionID: "sess-orphan-0000"},
			expected:    "- Thread <#1557949887948005399>: **15** turns (session: `sess-orphan-0000`)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, ch := range tc.cachedChans {
				CacheDiscordChannel(ch)
				defer InvalidateChannelCache(ch.ID)
			}
			if got := formatUnrotatedSessionLine(nil, tc.session); got != tc.expected {
				t.Errorf("got %q, want %q", got, tc.expected)
			}
		})
	}
}
