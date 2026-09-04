package skills

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/google/uuid"
)

type ScheduleTools struct {
	db *sql.DB
}

func NewScheduleTools(database *sql.DB) *ScheduleTools {
	return &ScheduleTools{db: database}
}

func (s *ScheduleTools) ScheduleOneShot(ctx context.Context, threadID, prompt string, runAt time.Time) (string, error) {
	if s.db == nil {
		return "", fmt.Errorf("database not initialized")
	}
	id := uuid.New().String()
	sched := db.OneShotSchedule{
		ID:        id,
		ThreadID:  threadID,
		Prompt:    prompt,
		RunAt:     runAt,
		CreatedAt: time.Now().UTC(),
	}
	if err := db.CreateOneShotSchedule(s.db, sched); err != nil {
		return "", err
	}
	return fmt.Sprintf("Scheduled reminder %s for %s", id, runAt.Format(time.RFC3339)), nil
}

func (s *ScheduleTools) ScheduleCron(ctx context.Context, targetID, cronExpr, prompt string, nextRunAt time.Time) (string, error) {
	if s.db == nil {
		return "", fmt.Errorf("database not initialized")
	}
	id := uuid.New().String()
	sched := db.CronSchedule{
		ID:        id,
		TargetID:  targetID,
		CronExpr:  cronExpr,
		Prompt:    prompt,
		NextRunAt: nextRunAt,
		Enabled:   true,
		CreatedAt: time.Now().UTC(),
	}
	if err := db.CreateCronSchedule(s.db, sched); err != nil {
		return "", err
	}
	return fmt.Sprintf("Scheduled cron %s (expr: '%s')", id, cronExpr), nil
}

func (s *ScheduleTools) CancelSchedule(ctx context.Context, schedType, id string) (string, error) {
	if s.db == nil {
		return "", fmt.Errorf("database not initialized")
	}
	if schedType == "cron" {
		if err := db.DeleteCronSchedule(s.db, id); err != nil {
			return "", err
		}
	} else {
		if err := db.DeleteOneShotSchedule(s.db, id); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("Cancelled %s schedule %s", schedType, id), nil
}
