package scheduler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/queue"
	"github.com/bwmarrin/discordgo"
)

type mockThreadCreator struct {
	createdThreads map[string]string
	mu             sync.Mutex
}

func newMockThreadCreator() *mockThreadCreator {
	return &mockThreadCreator{
		createdThreads: make(map[string]string),
	}
}

func (m *mockThreadCreator) CreatePublicThread(channelID, name string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	threadID := fmt.Sprintf("th-%s-%d", channelID, len(m.createdThreads)+1)
	m.createdThreads[channelID] = threadID
	return threadID, nil
}

type errThreadCreator struct{}

func (e *errThreadCreator) CreatePublicThread(channelID, name string) (string, error) {
	return "", fmt.Errorf("failed to create thread")
}

type mockEnqueuer struct {
	messages []db.Message
	mu       sync.Mutex
}

func newMockEnqueuer() *mockEnqueuer {
	return &mockEnqueuer{}
}

func (m *mockEnqueuer) Enqueue(msg db.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msg)
}

func (m *mockEnqueuer) getMessages() []db.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]db.Message, len(m.messages))
	copy(copied, m.messages)
	return copied
}

type mockRoundTripper struct {
	roundTrip func(req *http.Request) (*http.Response, error)
}

func (m *mockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTrip(req)
}

func newTestConfig() *config.Config {
	return config.NewFromData(config.DefaultConfigData())
}

func setupTestStore(t *testing.T) *db.FakeStore {
	t.Helper()
	return db.NewFakeStore()
}

type mockStoreWrapper struct {
	db.Store
	getDueCronSchedulesFn            func(ctx context.Context) ([]db.CronSchedule, error)
	updateCronNextRunFn              func(ctx context.Context, id string, nextRunAt time.Time) error
	createScheduleRunFn              func(ctx context.Context, run db.ScheduleRun) error
	insertMessageFn                  func(ctx context.Context, msg db.Message) error
	getDueOneShotSchedulesFn         func(ctx context.Context) ([]db.OneShotSchedule, error)
	insertMessageAndConsumeOneShotFn func(ctx context.Context, scheduleID string, msg db.Message) error
}

func (m *mockStoreWrapper) GetDueCronSchedules(ctx context.Context) ([]db.CronSchedule, error) {
	if m.getDueCronSchedulesFn != nil {
		return m.getDueCronSchedulesFn(ctx)
	}
	if m.Store != nil {
		return m.Store.GetDueCronSchedules(ctx)
	}
	return nil, nil
}

func (m *mockStoreWrapper) UpdateCronNextRun(ctx context.Context, id string, nextRunAt time.Time) error {
	if m.updateCronNextRunFn != nil {
		return m.updateCronNextRunFn(ctx, id, nextRunAt)
	}
	if m.Store != nil {
		return m.Store.UpdateCronNextRun(ctx, id, nextRunAt)
	}
	return nil
}

func (m *mockStoreWrapper) CreateScheduleRun(ctx context.Context, run db.ScheduleRun) error {
	if m.createScheduleRunFn != nil {
		return m.createScheduleRunFn(ctx, run)
	}
	if m.Store != nil {
		return m.Store.CreateScheduleRun(ctx, run)
	}
	return nil
}

func (m *mockStoreWrapper) InsertMessage(ctx context.Context, msg db.Message) error {
	if m.insertMessageFn != nil {
		return m.insertMessageFn(ctx, msg)
	}
	if m.Store != nil {
		return m.Store.InsertMessage(ctx, msg)
	}
	return nil
}

func (m *mockStoreWrapper) GetDueOneShotSchedules(ctx context.Context) ([]db.OneShotSchedule, error) {
	if m.getDueOneShotSchedulesFn != nil {
		return m.getDueOneShotSchedulesFn(ctx)
	}
	if m.Store != nil {
		return m.Store.GetDueOneShotSchedules(ctx)
	}
	return nil, nil
}

func (m *mockStoreWrapper) InsertMessageAndConsumeOneShot(ctx context.Context, scheduleID string, msg db.Message) error {
	if m.insertMessageAndConsumeOneShotFn != nil {
		return m.insertMessageAndConsumeOneShotFn(ctx, scheduleID, msg)
	}
	if m.Store != nil {
		return m.Store.InsertMessageAndConsumeOneShot(ctx, scheduleID, msg)
	}
	return nil
}

func TestNew_AndOptions(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	tests := []struct {
		name      string
		cfg       *config.Config
		withOpts  bool
		wantErr   bool
		wantRoots int
		wantBrain string
	}{
		{"nil config returns error", nil, false, true, 0, ""},
		{"valid config standard", newTestConfig(), false, false, 0, ""},
		{"options runner and roots and brain", newTestConfig(), true, false, 2, "/tmp/brain"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var opts []Option
			var embedderCalled, llmCalled bool
			if tc.withOpts {
				opts = []Option{
					WithRunnerFunc(func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (string, string, int, error) {
						return "", "", 0, nil
					}),
					WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
						return "llm", nil
					}),
					WithSessionRoots(" /root1 ", "", " /root2 "),
					WithBrainDir(tc.wantBrain),
					WithTranscriptEmbedder(func(ctx context.Context, text string) ([]float32, error) {
						embedderCalled = true
						return make([]float32, db.ExpectedEmbeddingDim), nil
					}),
					WithTranscriptLLMFunc(func(ctx context.Context, prompt string) (string, error) {
						llmCalled = true
						return "summary", nil
					}),
					nil,
				}
			}
			s, err := New(tc.cfg, store, newMockEnqueuer(), newMockThreadCreator(), opts...)
			if (err != nil) != tc.wantErr {
				t.Fatalf("wantErr=%v got err=%v", tc.wantErr, err)
			}
			if tc.wantErr {
				return
			}
			if tc.withOpts {
				if len(s.sessionRoots) != tc.wantRoots || s.sessionRoots[0] != "/root1" || s.sessionRoots[1] != "/root2" {
					t.Errorf("roots mismatch: got %v", s.sessionRoots)
				}
				if s.brainDir != tc.wantBrain {
					t.Errorf("brainDir: got %q, want %q", s.brainDir, tc.wantBrain)
				}
				_, _ = s.transcriptEmbedder(context.Background(), "t")
				_, _ = s.transcriptLLMFunc(context.Background(), "p")
				if !embedderCalled || !llmCalled {
					t.Errorf("expected transcript functions to be wired and callable")
				}
			}
		})
	}
}

func TestDiscordThreadCreator_CreatePublicThread(t *testing.T) {
	mockOKSession, _ := discordgo.New("Bot mock_token")
	mockOKSession.Client = &http.Client{
		Transport: &mockRoundTripper{
			roundTrip: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(`{"id":"th-mock-999","name":"mock title"}`)),
					Header:     make(http.Header),
				}, nil
			},
		},
	}
	mockErrSession, _ := discordgo.New("Bot mock_token")
	mockErrSession.Client = &http.Client{
		Transport: &mockRoundTripper{
			roundTrip: func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Body:       io.NopCloser(strings.NewReader(`{"message":"Bad request"}`)),
					Header:     make(http.Header),
				}, nil
			},
		},
	}

	tests := []struct {
		name       string
		creator    *DiscordThreadCreator
		channelID  string
		wantThread string
		wantErr    bool
	}{
		{"nil creator fallback", nil, "chan-nil", "chan-nil", false},
		{"nil session fallback", NewDiscordThreadCreator(nil), "chan-nosess", "chan-nosess", false},
		{"api success", NewDiscordThreadCreator(mockOKSession), "chan-1", "th-mock-999", false},
		{"api error", NewDiscordThreadCreator(mockErrSession), "chan-1", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.creator.CreatePublicThread(tc.channelID, "title")
			if (err != nil) != tc.wantErr {
				t.Fatalf("wantErr=%v got err=%v", tc.wantErr, err)
			}
			if !tc.wantErr && got != tc.wantThread {
				t.Errorf("got %q, want %q", got, tc.wantThread)
			}
		})
	}
}

func TestProcessDueSchedules_Cron(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name             string
		cron             db.CronSchedule
		channelMode      string
		isAlreadyThread  bool
		threadCreatorErr bool
		wantMsgs         int
		wantThreadID     string
		wantEffort       string
	}{
		{
			name:         "timezone inheritance",
			cron:         mkCron("c-inherit", "chan-tz", "Routine", "0 9 * * *", "run routine", "", "", now.Add(-time.Minute)),
			channelMode:  "threads",
			wantMsgs:     1,
			wantThreadID: "th-chan-tz-1",
		},
		{
			name:         "threads mode thread created",
			cron:         mkCron("c-threads", "chan-dev", "Health", "0 12 * * *", "report", "UTC", "", now.Add(-time.Minute)),
			channelMode:  "threads",
			wantMsgs:     1,
			wantThreadID: "th-chan-dev-1",
		},
		{
			name:         "channel mode no thread created",
			cron:         mkCron("c-chan", "chan-gen", "Weather", "0 6 * * *", "forecast", "UTC", "", now.Add(-time.Minute)),
			channelMode:  "channel",
			wantMsgs:     1,
			wantThreadID: "chan-gen",
		},
		{
			name:            "target already thread",
			cron:            mkCron("c-already-th", "th-target-1", "Update", "0 15 * * *", "update", "UTC", "", now.Add(-time.Minute)),
			channelMode:     "threads",
			isAlreadyThread: true,
			wantMsgs:        1,
			wantThreadID:    "th-target-1",
		},
		{
			name:        "stale overdue >24h advances without firing",
			cron:        mkCron("c-stale", "chan-stale", "Stale", "0 20 * * 5", "stale", "UTC", "", now.Add(-26*time.Hour)),
			channelMode: "threads",
			wantMsgs:    0,
		},
		{
			name:         "invalid cron 24h fallback fires",
			cron:         mkCron("c-invalid", "chan-inv", "Invalid", "invalid-syntax", "due inv", "UTC", "", now.Add(-5*time.Minute)),
			channelMode:  "threads",
			wantMsgs:     1,
			wantThreadID: "th-chan-inv-1",
		},
		{
			name:        "stale cron with invalid syntax advances without firing",
			cron:        mkCron("c-stale-inv", "chan-stale-inv", "StaleInv", "invalid-syntax", "stale inv", "UTC", "", now.Add(-48*time.Hour)),
			channelMode: "threads",
			wantMsgs:    0,
		},
		{
			name:             "thread creator error falls back to channel id",
			cron:             mkCron("c-err-th", "chan-fallback", "", "0 9 * * *", "fallback", "UTC", "", now.Add(-time.Minute)),
			channelMode:      "threads",
			threadCreatorErr: true,
			wantMsgs:         1,
			wantThreadID:     "chan-fallback",
		},
		{
			name:         "effort propagation and schedule run creation",
			cron:         mkCron("c-effort", "chan-eff", "LowEff", "0 9 * * *", "low effort", "UTC", "low", now.Add(-time.Minute)),
			channelMode:  "threads",
			wantMsgs:     1,
			wantThreadID: "th-chan-eff-1",
			wantEffort:   "low",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := setupTestStore(t)
			defer store.Close()
			_ = store.CreateCronSchedule(context.Background(), tc.cron)

			chanType := discordgo.ChannelTypeGuildText
			if tc.isAlreadyThread {
				chanType = discordgo.ChannelTypeGuildPublicThread
			}
			queue.CacheDiscordChannel(&discordgo.Channel{
				ID:   tc.cron.TargetID,
				Name: tc.cron.TargetID,
				Type: chanType,
			})

			cfg := config.NewFromData(&config.ConfigData{
				Timezone: "America/New_York",
				Channels: map[string]config.ChannelPolicy{
					"default":        {Mode: "threads"},
					tc.cron.TargetID: {Mode: tc.channelMode},
				},
			})

			enqueuer := newMockEnqueuer()
			var tcCreator ThreadCreator
			if tc.threadCreatorErr {
				tcCreator = &errThreadCreator{}
			} else {
				tcCreator = newMockThreadCreator()
			}

			s, err := New(cfg, store, enqueuer, tcCreator)
			if err != nil {
				t.Fatalf("New failed: %v", err)
			}
			if err := s.ProcessDueSchedules(context.Background()); err != nil {
				t.Fatalf("ProcessDueSchedules failed: %v", err)
			}

			msgs := enqueuer.getMessages()
			if len(msgs) != tc.wantMsgs {
				t.Fatalf("expected %d messages, got %d", tc.wantMsgs, len(msgs))
			}
			if tc.wantMsgs > 0 {
				msg := msgs[0]
				if msg.ThreadID != tc.wantThreadID {
					t.Errorf("msg ThreadID: got %q, want %q", msg.ThreadID, tc.wantThreadID)
				}
				if tc.wantEffort != "" && msg.Effort != tc.wantEffort {
					t.Errorf("msg Effort: got %q, want %q", msg.Effort, tc.wantEffort)
				}
				runs, total, err := store.GetScheduleRunsPaginated(context.Background(), 10, 0, tc.cron.ID, "")
				if err != nil || total != 1 || len(runs) != 1 {
					t.Fatalf("expected 1 schedule run, got total=%d err=%v", total, err)
				}
				if runs[0].ThreadID != tc.wantThreadID || runs[0].Effort != tc.wantEffort {
					t.Errorf("run mismatch: %+v", runs[0])
				}
			}
			dueCrons, _ := store.GetDueCronSchedules(context.Background())
			if len(dueCrons) != 0 {
				t.Errorf("expected 0 due crons after run, got %d", len(dueCrons))
			}
		})
	}
}

func TestProcessDueSchedules_OneShot(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name         string
		oneShot      db.OneShotSchedule
		futureSched  db.OneShotSchedule
		wantRollback bool
		wantMsgs     int
		wantThreadID string
		wantEffort   string
	}{
		{
			name:         "atomic consumption and run creation",
			oneShot:      mkOneShot("os-1", "th-os-1", "Check stove", "standard", now.Add(-time.Minute)),
			futureSched:  mkOneShot("os-future", "th-os-1", "Future stove", "standard", now.Add(30*time.Minute)),
			wantMsgs:     1,
			wantThreadID: "th-os-1",
			wantEffort:   "standard",
		},
		{
			name:         "effort propagation low",
			oneShot:      mkOneShot("os-effort", "th-os-2", "Oven reminder", "low", now.Add(-time.Minute)),
			wantMsgs:     1,
			wantThreadID: "th-os-2",
			wantEffort:   "low",
		},
		{
			name:         "rollback on missing schedule",
			oneShot:      mkOneShot("os-rb", "th-os-rb", "Prompt", "standard", now.Add(-time.Minute)),
			wantRollback: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := setupTestStore(t)
			defer store.Close()

			if tc.wantRollback {
				msg := db.Message{ID: "msg-rb-1", ThreadID: tc.oneShot.ThreadID, Content: tc.oneShot.Prompt, Status: db.StatusPending, CreatedAt: now}
				err := store.InsertMessageAndConsumeOneShot(context.Background(), "missing-id", msg)
				if err == nil {
					t.Fatal("expected error when consuming non-existent one-shot")
				}
				dbMsg, _ := store.GetMessage(context.Background(), "msg-rb-1")
				if dbMsg != nil {
					t.Errorf("expected message to not exist after rollback")
				}
				return
			}

			_ = store.CreateOneShotSchedule(context.Background(), tc.oneShot)
			if tc.futureSched.ID != "" {
				_ = store.CreateOneShotSchedule(context.Background(), tc.futureSched)
			}

			enqueuer := newMockEnqueuer()
			s, err := New(newTestConfig(), store, enqueuer, newMockThreadCreator())
			if err != nil {
				t.Fatalf("New failed: %v", err)
			}
			if err := s.ProcessDueSchedules(context.Background()); err != nil {
				t.Fatalf("ProcessDueSchedules failed: %v", err)
			}

			msgs := enqueuer.getMessages()
			if len(msgs) != tc.wantMsgs {
				t.Fatalf("expected %d messages, got %d", tc.wantMsgs, len(msgs))
			}
			if msgs[0].ThreadID != tc.wantThreadID || msgs[0].Effort != tc.wantEffort {
				t.Errorf("msg mismatch: %+v", msgs[0])
			}

			runs, total, err := store.GetScheduleRunsPaginated(context.Background(), 10, 0, tc.oneShot.ID, "")
			if err != nil || total != 1 || len(runs) != 1 {
				t.Fatalf("expected 1 schedule run, got total=%d err=%v", total, err)
			}
			if runs[0].ScheduleType != "one_shot" || runs[0].Effort != tc.wantEffort {
				t.Errorf("run mismatch: %+v", runs[0])
			}

			dueAfter, _ := store.GetDueOneShotSchedules(context.Background())
			if len(dueAfter) != 0 {
				t.Errorf("expected 0 due one-shots after processing, got %d", len(dueAfter))
			}
			if tc.futureSched.ID != "" {
				all, _ := store.GetAllOneShotSchedules(context.Background(), tc.oneShot.ThreadID)
				if len(all) != 1 || all[0].ID != tc.futureSched.ID {
					t.Errorf("expected only future schedule to remain, got %+v", all)
				}
			}
		})
	}
}

func TestProcessDueSchedules_GuardsAndStoreErrors(t *testing.T) {
	now := time.Now().UTC()
	baseCfg := newTestConfig()
	baseStore := setupTestStore(t)
	defer baseStore.Close()

	dueCron := mkCron("due-c1", "chan-1", "Due", "0 9 * * *", "due", "UTC", "", now.Add(-time.Minute))
	staleCron := mkCron("stale-c1", "chan-1", "Stale", "0 9 * * *", "stale", "UTC", "", now.Add(-48*time.Hour))
	dueOneShot := mkOneShot("os-1", "th-1", "oneshot", "", now.Add(-time.Minute))

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name       string
		sched      *Scheduler
		ctx        context.Context
		wrapper    *mockStoreWrapper
		wantErr    bool
		errContain string
	}{
		{"nil receiver", nil, context.Background(), nil, false, ""},
		{"nil store", &Scheduler{cfg: baseCfg}, context.Background(), nil, false, ""},
		{"nil config", &Scheduler{store: baseStore}, context.Background(), nil, true, "config cannot be nil"},
		{"cron query error", &Scheduler{cfg: baseCfg}, context.Background(), &mockStoreWrapper{
			getDueCronSchedulesFn: func(ctx context.Context) ([]db.CronSchedule, error) {
				return nil, fmt.Errorf("cron query failed")
			},
		}, true, "cron query failed"},
		{"oneshot query error", &Scheduler{cfg: baseCfg}, context.Background(), &mockStoreWrapper{
			getDueCronSchedulesFn: func(ctx context.Context) ([]db.CronSchedule, error) { return nil, nil },
			getDueOneShotSchedulesFn: func(ctx context.Context) ([]db.OneShotSchedule, error) {
				return nil, fmt.Errorf("oneshot query error")
			},
		}, true, "oneshot query error"},
		{"cron loop context canceled", &Scheduler{cfg: baseCfg}, cancelCtx, &mockStoreWrapper{
			getDueCronSchedulesFn: func(ctx context.Context) ([]db.CronSchedule, error) {
				return []db.CronSchedule{dueCron}, nil
			},
		}, true, "context canceled"},
		{"oneshot loop context canceled", &Scheduler{cfg: baseCfg}, cancelCtx, &mockStoreWrapper{
			getDueCronSchedulesFn: func(ctx context.Context) ([]db.CronSchedule, error) { return nil, nil },
			getDueOneShotSchedulesFn: func(ctx context.Context) ([]db.OneShotSchedule, error) {
				return []db.OneShotSchedule{dueOneShot}, nil
			},
		}, true, "context canceled"},
		{"stale cron update next run error tolerated", &Scheduler{cfg: baseCfg}, context.Background(), &mockStoreWrapper{
			getDueCronSchedulesFn: func(ctx context.Context) ([]db.CronSchedule, error) {
				return []db.CronSchedule{staleCron}, nil
			},
			updateCronNextRunFn: func(ctx context.Context, id string, nextRunAt time.Time) error {
				return fmt.Errorf("stale update error")
			},
			getDueOneShotSchedulesFn: func(ctx context.Context) ([]db.OneShotSchedule, error) { return nil, nil },
		}, false, ""},
		{"due cron mutations error tolerated", &Scheduler{cfg: baseCfg, threadCreator: newMockThreadCreator()}, context.Background(), &mockStoreWrapper{
			getDueCronSchedulesFn: func(ctx context.Context) ([]db.CronSchedule, error) {
				return []db.CronSchedule{dueCron}, nil
			},
			updateCronNextRunFn: func(ctx context.Context, id string, nextRunAt time.Time) error {
				return fmt.Errorf("cron update error")
			},
			createScheduleRunFn: func(ctx context.Context, run db.ScheduleRun) error {
				return fmt.Errorf("create run error")
			},
			insertMessageFn: func(ctx context.Context, msg db.Message) error {
				return fmt.Errorf("insert msg error")
			},
			getDueOneShotSchedulesFn: func(ctx context.Context) ([]db.OneShotSchedule, error) { return nil, nil },
		}, false, ""},
		{"due oneshot consume and run error tolerated", &Scheduler{cfg: baseCfg}, context.Background(), &mockStoreWrapper{
			getDueCronSchedulesFn: func(ctx context.Context) ([]db.CronSchedule, error) { return nil, nil },
			getDueOneShotSchedulesFn: func(ctx context.Context) ([]db.OneShotSchedule, error) {
				return []db.OneShotSchedule{dueOneShot}, nil
			},
			insertMessageAndConsumeOneShotFn: func(ctx context.Context, scheduleID string, msg db.Message) error {
				return fmt.Errorf("consume error")
			},
			createScheduleRunFn: func(ctx context.Context, run db.ScheduleRun) error {
				return fmt.Errorf("oneshot run error")
			},
		}, false, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.sched
			if tc.wrapper != nil {
				s.store = tc.wrapper
			}
			err := s.ProcessDueSchedules(tc.ctx)
			if (err != nil) != tc.wantErr {
				t.Fatalf("wantErr=%v got err=%v", tc.wantErr, err)
			}
			if tc.errContain != "" && (err == nil || !strings.Contains(err.Error(), tc.errContain)) {
				t.Errorf("expected error containing %q, got %v", tc.errContain, err)
			}
		})
	}
}

func TestMaintenanceJobs_RetentionAndTranscript(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()

	// 1. RunPruneRetentionWithContext
	RunPruneRetentionWithContext(context.Background(), nil)
	closedStore := setupTestStore(t)
	_ = closedStore.Close()
	RunPruneRetentionWithContext(context.Background(), closedStore)

	oldTime := time.Now().UTC().Add(-60 * 24 * time.Hour)
	_ = store.CreateScheduleRun(context.Background(), db.ScheduleRun{
		ID: "old-1", ScheduleID: "s1", ScheduleType: "cron", TargetID: "c1", ThreadID: "th1",
		StartedAt: oldTime, CompletedAt: &oldTime,
	})
	RunPruneRetentionWithContext(context.Background(), store)

	// 2. RunTranscriptSync & runTranscriptSync method
	stats, err := RunTranscriptSync(context.Background(), nil, "/tmp", nil, nil)
	if err != nil || stats.Synced != 0 {
		t.Fatalf("expected nil err on nil store")
	}

	if _, err := RunTranscriptSync(context.Background(), store, "", nil, nil); err == nil {
		t.Fatalf("expected error on empty brain dir")
	}

	tempBrain := t.TempDir()
	logDir := filepath.Join(tempBrain, "sess-1", ".system_generated", "logs")
	_ = os.MkdirAll(logDir, 0o755)
	_ = os.WriteFile(filepath.Join(logDir, "transcript.jsonl"), []byte(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","content":"sync test"}`+"\n"), 0o644)
	oldMtime := time.Now().Add(-time.Hour)
	_ = os.Chtimes(filepath.Join(logDir, "transcript.jsonl"), oldMtime, oldMtime)

	mockEmbedder := func(ctx context.Context, text string) ([]float32, error) {
		return make([]float32, db.ExpectedEmbeddingDim), nil
	}
	mockLLM := func(ctx context.Context, prompt string) (string, error) {
		return "sync summary", nil
	}
	stats, err = RunTranscriptSync(context.Background(), store, tempBrain, mockEmbedder, mockLLM)
	if err != nil || stats.Synced != 1 {
		t.Fatalf("RunTranscriptSync failed: %v", err)
	}

	transcriptSyncMutex.Lock()
	statsOverlap, errOverlap := RunTranscriptSync(context.Background(), store, tempBrain, nil, nil)
	transcriptSyncMutex.Unlock()
	if errOverlap != nil || statsOverlap.Synced != 0 {
		t.Errorf("expected overlap to skip cleanly")
	}

	emptySched := &Scheduler{}
	emptySched.runTranscriptSync(context.Background())
	emptySched.wg.Wait()

	cfg := newTestConfig()
	schedMethod, _ := New(cfg, store, nil, nil, WithBrainDir(tempBrain), WithLLMFunc(func(ctx context.Context, model, prompt string) (string, error) {
		return "summary", nil
	}))
	schedMethod.runTranscriptSync(context.Background())
	schedMethod.wg.Wait()

	cancCtx2, canc2 := context.WithCancel(context.Background())
	canc2()
	schedMethod.runTranscriptSync(cancCtx2)
	schedMethod.wg.Wait()
}

func TestScheduler_RunAndStartLifecycle(t *testing.T) {
	store := setupTestStore(t)
	defer store.Close()
	cfg := newTestConfig()

	// 1. Start and stop idempotency
	schedStart, err := New(cfg, store, nil, nil)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	ctxStart, cancelStart := context.WithCancel(context.Background())
	defer cancelStart()
	stop := schedStart.Start(ctxStart)
	stop()
	stop()

	// 2. Run guards (nil scheduler, nil cfg)
	var nilSched *Scheduler
	nilSched.Run(context.Background(), time.Second)

	schedNilCfg := &Scheduler{}
	schedNilCfg.Run(context.Background(), time.Second)

	// 3. Initial schedule check error in Run
	mockStoreInitErr := &mockStoreWrapper{
		Store: store,
		getDueCronSchedulesFn: func(ctx context.Context) ([]db.CronSchedule, error) {
			return nil, fmt.Errorf("init check error")
		},
	}
	schedInitErr, _ := New(cfg, mockStoreInitErr, nil, nil)
	ctxCancel, cancelInit := context.WithCancel(context.Background())
	cancelInit()
	schedInitErr.Run(ctxCancel, 10*time.Millisecond)

	// 4. handleTick evaluations
	nilSched.handleTick(context.Background(), 120)

	mockStoreFast := &mockStoreWrapper{
		Store:                    store,
		getDueCronSchedulesFn:    func(ctx context.Context) ([]db.CronSchedule, error) { return nil, nil },
		getDueOneShotSchedulesFn: func(ctx context.Context) ([]db.OneShotSchedule, error) { return nil, nil },
	}
	schedFast, _ := New(cfg, mockStoreFast, nil, nil)
	schedFast.handleTick(context.Background(), 10)
	schedFast.handleTick(context.Background(), 60)
	schedFast.handleTick(context.Background(), 120)
	schedFast.handleTick(context.Background(), 2880)

	// 5. Short ticker run loop terminates cleanly on cancellation without time.Sleep
	var ticks atomic.Int64
	ctxFast, cancelFast := context.WithCancel(context.Background())
	mockStoreFast.getDueCronSchedulesFn = func(ctx context.Context) ([]db.CronSchedule, error) {
		if ticks.Add(1) >= 2 {
			cancelFast()
		}
		return nil, nil
	}
	schedFast.Run(ctxFast, 1*time.Millisecond)
}
