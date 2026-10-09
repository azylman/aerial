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

func TestWorkerPool_CheckUnrotatedSessions_NilAndErrorHandling(t *testing.T) {
	// 1. Nil WorkerPool should not panic
	var nilPool *WorkerPool
	nilPool.checkUnrotatedSessions(context.Background(), make(map[string]int))

	// 2. WorkerPool with nil Store should not panic
	poolNoStore := &WorkerPool{cfg: WorkerPoolConfig{}}
	poolNoStore.checkUnrotatedSessions(context.Background(), make(map[string]int))

	// 3. Store error should be handled gracefully without panic or alert
	var alertCalled bool
	storeWithErr := &mockErrSessionStore{
		findErr: errors.New("database connection failed"),
	}
	poolErr := &WorkerPool{
		cfg: WorkerPoolConfig{
			Store: storeWithErr,
			SystemAlertFunc: func(s *discordgo.Session, channelNameOrID, title, alertBody string) error {
				alertCalled = true
				return nil
			},
		},
	}
	poolErr.checkUnrotatedSessions(context.Background(), make(map[string]int))
	if alertCalled {
		t.Error("expected no system alert when store returns an error")
	}
}

func TestWorkerPool_CheckUnrotatedSessions_DeduplicationAndPruning(t *testing.T) {
	fake := db.NewFakeStore()
	defer func() { _ = fake.Close() }()

	ctx := context.Background()

	var (
		mu         sync.Mutex
		alertCalls int
		lastTitle  string
		lastBody   string
	)

	recordAlert := func(s *discordgo.Session, channelNameOrID, title, alertBody string) error {
		mu.Lock()
		defer mu.Unlock()
		alertCalls++
		lastTitle = title
		lastBody = alertBody
		return nil
	}

	p := &WorkerPool{
		cfg: WorkerPoolConfig{
			Store:           fake,
			SystemAlertFunc: recordAlert,
		},
	}

	lastReported := make(map[string]int)

	// Step 1: Session below limit (5 turns) -> no alert
	_ = fake.SaveSessionID(ctx, "th-test", "sess-1")
	for i := 0; i < 5; i++ {
		_, _ = fake.IncrementSessionTurnCount(ctx, "th-test")
	}

	p.checkUnrotatedSessions(ctx, lastReported)

	mu.Lock()
	if alertCalls != 0 {
		t.Fatalf("expected 0 alerts for 5 turns, got %d", alertCalls)
	}
	mu.Unlock()
	if len(lastReported) != 0 {
		t.Fatalf("expected lastReported to be empty, got %v", lastReported)
	}

	// Step 2: Session breaches limit (9 turns) -> 1 alert sent
	for i := 0; i < 4; i++ {
		_, _ = fake.IncrementSessionTurnCount(ctx, "th-test")
	}

	p.checkUnrotatedSessions(ctx, lastReported)

	mu.Lock()
	if alertCalls != 1 {
		t.Fatalf("expected 1 alert for 9 turns, got %d", alertCalls)
	}
	if !strings.Contains(lastBody, "th-test") || !strings.Contains(lastBody, "**9** turns") {
		t.Errorf("unexpected alert body: %s", lastBody)
	}
	if lastTitle != "Unrotated Runaway Sessions" {
		t.Errorf("unexpected title: %s", lastTitle)
	}
	mu.Unlock()

	if lastReported["th-test"] != 9 {
		t.Errorf("expected lastReported['th-test']=9, got %d", lastReported["th-test"])
	}

	// Step 3: Session is dormant (still 9 turns) -> deduplication skips alert
	p.checkUnrotatedSessions(ctx, lastReported)

	mu.Lock()
	if alertCalls != 1 {
		t.Fatalf("expected alertCalls to remain 1 (deduplicated), got %d", alertCalls)
	}
	mu.Unlock()

	// Step 4: Session takes another turn unrotated (10 turns) -> 2nd alert sent
	_, _ = fake.IncrementSessionTurnCount(ctx, "th-test")

	p.checkUnrotatedSessions(ctx, lastReported)

	mu.Lock()
	if alertCalls != 2 {
		t.Fatalf("expected alertCalls to be 2 for 10 turns, got %d", alertCalls)
	}
	if !strings.Contains(lastBody, "**10** turns") {
		t.Errorf("expected alert to report 10 turns, got: %s", lastBody)
	}
	mu.Unlock()

	if lastReported["th-test"] != 10 {
		t.Errorf("expected lastReported['th-test']=10, got %d", lastReported["th-test"])
	}

	// Step 5: Session rotates -> turn count reset to 0 in DB
	_ = fake.RotateSessionID(ctx, "th-test", "sess-fresh")

	p.checkUnrotatedSessions(ctx, lastReported)

	mu.Lock()
	if alertCalls != 2 {
		t.Fatalf("expected no new alert on rotated session, got %d calls", alertCalls)
	}
	mu.Unlock()

	// Verify pruned from lastReported map
	if _, exists := lastReported["th-test"]; exists {
		t.Errorf("expected rotated session 'th-test' to be pruned from lastReported, but was present: %v", lastReported)
	}
}

func TestWorkerPool_CheckUnrotatedSessions_Top5Cap(t *testing.T) {
	fake := db.NewFakeStore()
	defer func() { _ = fake.Close() }()

	ctx := context.Background()

	var (
		mu       sync.Mutex
		lastBody string
	)

	recordAlert := func(s *discordgo.Session, channelNameOrID, title, alertBody string) error {
		mu.Lock()
		defer mu.Unlock()
		lastBody = alertBody
		return nil
	}

	p := &WorkerPool{
		cfg: WorkerPoolConfig{
			Store:           fake,
			SystemAlertFunc: recordAlert,
		},
	}

	// Insert 7 unrotated sessions (threads 1 to 7)
	for i := 1; i <= 7; i++ {
		thID := fmt.Sprintf("th-%d", i)
		_ = fake.SaveSessionID(ctx, thID, fmt.Sprintf("sess-%d", i))
		for j := 0; j < 9+i; j++ {
			_, _ = fake.IncrementSessionTurnCount(ctx, thID)
		}
	}

	lastReported := make(map[string]int)
	p.checkUnrotatedSessions(ctx, lastReported)

	mu.Lock()
	body := lastBody
	mu.Unlock()

	if !strings.Contains(body, "...and 2 more runaway session(s)") {
		t.Errorf("expected overflow indicator for top 5 cap, got:\n%s", body)
	}
	// Verify that all 7 were recorded in lastReported
	if len(lastReported) != 7 {
		t.Errorf("expected all 7 unrotated sessions recorded in lastReported, got %d", len(lastReported))
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

	recordAlert := func(s *discordgo.Session, channelNameOrID, title, alertBody string) error {
		select {
		case alertCh <- alertBody:
		default:
		}
		return nil
	}

	p := NewWorkerPool(WorkerPoolConfig{
		Store:               fake,
		MaintenanceInterval: 10 * time.Millisecond,
		SystemAlertFunc:     recordAlert,
	})
	p.Start()

	select {
	case body := <-alertCh:
		if !strings.Contains(body, "th-ticker-test") || !strings.Contains(body, "**11** turns") {
			t.Errorf("unexpected alert body from maintenance ticker: %s", body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for maintenance ticker to fire unrotated session check")
	}

	p.StopWithTimeout(1 * time.Second)
}

func TestWorkerPool_CheckUnrotatedSessions_ContextCancelled(t *testing.T) {
	fake := db.NewFakeStore()
	defer func() { _ = fake.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	p := &WorkerPool{
		cfg: WorkerPoolConfig{Store: fake},
	}
	lastReported := make(map[string]int)
	p.checkUnrotatedSessions(ctx, lastReported)
	if len(lastReported) != 0 {
		t.Errorf("expected empty lastReported on cancelled context, got %d entries", len(lastReported))
	}
}

func TestWorkerPool_CheckUnrotatedSessions_ExcludesDormantSessions(t *testing.T) {
	fake := db.NewFakeStore()
	defer func() { _ = fake.Close() }()

	ctx := context.Background()

	var alertCalled bool
	recordAlert := func(s *discordgo.Session, channelNameOrID, title, alertBody string) error {
		alertCalled = true
		return nil
	}

	p := &WorkerPool{
		cfg: WorkerPoolConfig{
			Store:           fake,
			SystemAlertFunc: recordAlert,
		},
	}

	// Seed an unrotated session with 15 turns
	_ = fake.SaveSessionID(ctx, "th-dormant", "sess-dormant")
	for i := 0; i < 15; i++ {
		_, _ = fake.IncrementSessionTurnCount(ctx, "th-dormant")
	}

	// Backdate session UpdatedAt to 3 hours ago (beyond 2-hour default horizon)
	dormantTime := time.Now().UTC().Add(-3 * time.Hour)
	fake.SetSessionUpdatedAt("th-dormant", dormantTime)

	lastReported := make(map[string]int)
	p.checkUnrotatedSessions(ctx, lastReported)

	if alertCalled {
		t.Error("expected no system alert for dormant session older than 2 hours")
	}
	if len(lastReported) != 0 {
		t.Errorf("expected lastReported to remain empty for dormant session, got %v", lastReported)
	}
}

func TestFormatUnrotatedSessionLine(t *testing.T) {
	// 1. Non-numeric snowflake (fallback format)
	nonSnowflake := db.SessionInfo{
		ThreadID:          "th-custom-session",
		TurnCount:         9,
		InternalSessionID: "sess-uuid-1234",
	}
	if got := formatUnrotatedSessionLine(nil, nonSnowflake); got != "- Thread `th-custom-session`: **9** turns (session: `sess-uuid-1234`)" {
		t.Errorf("unexpected non-snowflake line: %q", got)
	}

	// 2. Channel snowflake resolved via cache
	chanID := "1464358486849622120"
	CacheDiscordChannel(&discordgo.Channel{
		ID:   chanID,
		Type: discordgo.ChannelTypeGuildText,
		Name: "general",
	})
	defer InvalidateChannelCache(chanID)

	chanSession := db.SessionInfo{
		ThreadID:          chanID,
		TurnCount:         10,
		InternalSessionID: "sess-chan-5678",
	}
	if got := formatUnrotatedSessionLine(nil, chanSession); got != "- Channel <#1464358486849622120>: **10** turns (session: `sess-chan-5678`)" {
		t.Errorf("unexpected channel line: %q", got)
	}

	// 3. Thread snowflake with parent channel
	threadID := "1557949887948005398"
	parentID := "1542423172400291873"
	CacheDiscordChannel(&discordgo.Channel{
		ID:   parentID,
		Type: discordgo.ChannelTypeGuildText,
		Name: "aerial-dev",
	})
	defer InvalidateChannelCache(parentID)

	CacheDiscordChannel(&discordgo.Channel{
		ID:       threadID,
		ParentID: parentID,
		Type:     discordgo.ChannelTypeGuildPublicThread,
		Name:     "Resolve Notification Snowflake IDs",
	})
	defer InvalidateChannelCache(threadID)

	threadSession := db.SessionInfo{
		ThreadID:          threadID,
		TurnCount:         12,
		InternalSessionID: "sess-thread-9999",
	}
	expectedThread := "- Thread <#1557949887948005398> (in <#1542423172400291873>): **12** turns (session: `sess-thread-9999`)"
	if got := formatUnrotatedSessionLine(nil, threadSession); got != expectedThread {
		t.Errorf("unexpected thread line with parent: %q, expected: %q", got, expectedThread)
	}

	// 4. Thread snowflake without parent channel
	orphanID := "1557949887948005399"
	CacheDiscordChannel(&discordgo.Channel{
		ID:   orphanID,
		Type: discordgo.ChannelTypeGuildPublicThread,
		Name: "Orphan Thread",
	})
	defer InvalidateChannelCache(orphanID)

	orphanThreadSession := db.SessionInfo{
		ThreadID:          orphanID,
		TurnCount:         15,
		InternalSessionID: "sess-orphan-0000",
	}
	expectedOrphan := "- Thread <#1557949887948005399>: **15** turns (session: `sess-orphan-0000`)"
	if got := formatUnrotatedSessionLine(nil, orphanThreadSession); got != expectedOrphan {
		t.Errorf("unexpected thread line without parent: %q, expected: %q", got, expectedOrphan)
	}
}
