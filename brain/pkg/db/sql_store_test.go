package db

import (
	"context"
	"database/sql"
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

func TestNewTxStoreTypedNil(t *testing.T) {
	var tx *sql.Tx = nil
	store := NewTxStore(tx, false)
	if store != nil {
		t.Fatalf("Expected NewTxStore to return nil for typed nil *sql.Tx, got %v", store)
	}

	var nilStore *SQLStore = nil
	ctx := context.Background()
	_, err := nilStore.InsertFact(ctx, "cat", "text", 1.0, "th", nil)
	if err == nil {
		t.Fatalf("Expected error when calling InsertFact on nil *SQLStore, got nil")
	}
}
