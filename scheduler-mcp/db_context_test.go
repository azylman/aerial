package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDB_ContextCancellationAndNilGuards(t *testing.T) {
	db, err := InitDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer db.Close()

	// 1. Test nil context guards (should normalize to context.Background() without panic)
	cron := CronSchedule{
		ID:        "cron-nil-ctx",
		TargetID:  "chan-nil",
		CronExpr:  "0 0 * * *",
		Prompt:    "nil ctx prompt",
		NextRunAt: time.Now().UTC(),
		Enabled:   true,
	}
	if err := InsertCronSchedule(nil, db, cron); err != nil {
		t.Errorf("InsertCronSchedule with nil ctx failed: %v", err)
	}

	oneShot := OneShotSchedule{
		ID:        "oneshot-nil-ctx",
		ThreadID:  "chan-nil",
		Prompt:    "nil ctx oneshot",
		RunAt:     time.Now().UTC(),
	}
	if err := InsertOneShotSchedule(nil, db, oneShot); err != nil {
		t.Errorf("InsertOneShotSchedule with nil ctx failed: %v", err)
	}

	crons, err := ListCronSchedules(nil, db, "chan-nil")
	if err != nil || len(crons) != 1 {
		t.Errorf("ListCronSchedules with nil ctx failed: %v, count: %d", err, len(crons))
	}

	shots, err := ListOneShotSchedules(nil, db, "chan-nil")
	if err != nil || len(shots) != 1 {
		t.Errorf("ListOneShotSchedules with nil ctx failed: %v, count: %d", err, len(shots))
	}

	newPrompt := "updated prompt"
	if err := UpdateCronSchedule(nil, db, "cron-nil-ctx", nil, nil, &newPrompt, nil, nil, nil); err != nil {
		t.Errorf("UpdateCronSchedule with nil ctx failed: %v", err)
	}

	// Empty sets with nil ctx
	if err := UpdateCronSchedule(nil, db, "cron-nil-ctx", nil, nil, nil, nil, nil, nil); err != nil {
		t.Errorf("UpdateCronSchedule empty sets with nil ctx failed: %v", err)
	}

	deleted, err := DeleteSchedule(nil, db, "cron-nil-ctx")
	if err != nil || !deleted {
		t.Errorf("DeleteSchedule with nil ctx failed: %v, deleted: %v", err, deleted)
	}

	// 2. Pre-cancelled context tests
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	// UpdateCronSchedule with empty sets and canceled ctx
	if err := UpdateCronSchedule(canceledCtx, db, "some-id", nil, nil, nil, nil, nil, nil); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled on empty sets with canceled ctx, got %v", err)
	}

	// Table-driven cancellation tests across all 6 DB functions
	tests := []struct {
		name string
		op   func() error
	}{
		{
			name: "InsertCronSchedule",
			op: func() error {
				return InsertCronSchedule(canceledCtx, db, cron)
			},
		},
		{
			name: "InsertOneShotSchedule",
			op: func() error {
				return InsertOneShotSchedule(canceledCtx, db, oneShot)
			},
		},
		{
			name: "ListCronSchedules",
			op: func() error {
				_, err := ListCronSchedules(canceledCtx, db, "")
				return err
			},
		},
		{
			name: "ListOneShotSchedules",
			op: func() error {
				_, err := ListOneShotSchedules(canceledCtx, db, "")
				return err
			},
		},
		{
			name: "UpdateCronSchedule",
			op: func() error {
				p := "new"
				return UpdateCronSchedule(canceledCtx, db, "any-id", nil, nil, &p, nil, nil, nil)
			},
		},
		{
			name: "DeleteSchedule",
			op: func() error {
				_, err := DeleteSchedule(canceledCtx, db, "any-id")
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.op()
			if !errors.Is(err, context.Canceled) {
				t.Errorf("expected context.Canceled for %s, got: %v", tt.name, err)
			}
		})
	}
}
