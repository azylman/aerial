package db

import (
	"context"
	"errors"
	"testing"
)

func TestWithTxCommit(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	err := store.WithTx(ctx, func(txStore Store) error {
		_, err := txStore.InsertFact(ctx, "test", "Transactional fact", 1.0, "thread-tx-1", nil)
		return err
	})
	if err != nil {
		t.Fatalf("WithTx failed: %v", err)
	}

	facts, err := store.GetFactsPaginated(ctx, FactsFilter{Limit: 10})
	if err != nil {
		t.Fatalf("GetFactsPaginated failed: %v", err)
	}
	if len(facts.Facts) != 1 {
		t.Fatalf("Expected 1 fact committed, got %d", len(facts.Facts))
	}
	if facts.Facts[0].FactText != "Transactional fact" {
		t.Errorf("Expected 'Transactional fact', got %q", facts.Facts[0].FactText)
	}
}

func TestWithTxRollback(t *testing.T) {
	store := NewTestStore(t)
	ctx := context.Background()

	expectedErr := errors.New("simulated transaction failure")
	err := store.WithTx(ctx, func(txStore Store) error {
		_, err := txStore.InsertFact(ctx, "test", "Uncommitted fact", 1.0, "thread-tx-2", nil)
		if err != nil {
			return err
		}
		return expectedErr
	})

	if !errors.Is(err, expectedErr) {
		t.Fatalf("Expected %v, got %v", expectedErr, err)
	}

	facts, err := store.GetFactsPaginated(ctx, FactsFilter{Limit: 10})
	if err != nil {
		t.Fatalf("GetFactsPaginated failed: %v", err)
	}
	if len(facts.Facts) != 0 {
		t.Fatalf("Expected 0 facts after rollback, got %d", len(facts.Facts))
	}
}
