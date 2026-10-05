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

func (m *mockErrSessionStore) FindUnrotatedSessions(ctx context.Context, minTurns int) ([]db.SessionInfo, error) {
	if m.findErr != nil {
		return nil, m.findErr
	}
	if m.Store != nil {
		return m.Store.FindUnrotatedSessions(ctx, minTurns)
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
