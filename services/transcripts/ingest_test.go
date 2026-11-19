package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/transcriptwire"
)

type fakeBatchMessage struct {
	data      []byte
	delivered uint64
	termed    bool
	termErr   error
}

func (m *fakeBatchMessage) Data() []byte                     { return m.data }
func (*fakeBatchMessage) Subject() string                    { return "aerial.transcripts.steps.sess-1" }
func (*fakeBatchMessage) Header() nats.Header                { return nil }
func (m *fakeBatchMessage) NumDelivered() uint64             { return m.delivered }
func (*fakeBatchMessage) Ack() error                         { return nil }
func (*fakeBatchMessage) NakWithDelay(_ time.Duration) error { return nil }
func (m *fakeBatchMessage) Term() error {
	m.termed = true
	return m.termErr
}
func (*fakeBatchMessage) Snooze(_ time.Duration) error { return nil }
func (*fakeBatchMessage) InProgress() error            { return nil }

func marshalBatch(t *testing.T, b transcriptwire.TranscriptStepBatch) []byte {
	t.Helper()
	raw, err := json.Marshal(b)
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}
	return raw
}

func TestHandleBatchMessage(t *testing.T) {
	t.Parallel()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	seedBatch := testBatch("sess-1", epoch, 0, 4, false, false)
	finalSeed := finalSeedBatch("sess-1", epoch)
	emptySession := testBatch("sess-1", epoch.Add(time.Hour), 5, 6, false, false)
	emptySession.SessionID = "  "
	tests := []struct {
		name          string
		seed          []transcriptwire.TranscriptStepBatch
		payload       []byte
		publishErr    error
		wantErr       bool
		wantTerm      bool
		wantPublished bool
		wantIndexed   int
	}{
		{"append publishes indexed event", []transcriptwire.TranscriptStepBatch{seedBatch}, marshalBatch(t, testBatch("sess-1", epoch.Add(time.Hour), 5, 6, true, false)), nil, false, false, true, 2},
		{"stale gap batch acks silently", []transcriptwire.TranscriptStepBatch{seedBatch}, marshalBatch(t, testBatch("sess-1", epoch.Add(-time.Hour), 5, 6, false, false)), nil, false, false, false, 0},
		{"duplicate final re-publishes", []transcriptwire.TranscriptStepBatch{finalSeed}, marshalBatch(t, testBatch("sess-1", epoch, 0, 4, true, false)), nil, false, false, true, 0},
		{"publish error naks", []transcriptwire.TranscriptStepBatch{seedBatch}, marshalBatch(t, testBatch("sess-1", epoch.Add(time.Hour), 5, 6, false, false)), errors.New("nats down"), true, false, false, 0},
		{"undecodable terms", []transcriptwire.TranscriptStepBatch{seedBatch}, []byte("{broken"), nil, false, true, false, 0},
		{"empty session terms", []transcriptwire.TranscriptStepBatch{seedBatch}, marshalBatch(t, emptySession), nil, false, true, false, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newTestStore(t, "aerial")
			ctx := context.Background()
			for _, s := range tc.seed {
				if _, err := store.ApplyBatch(ctx, s); err != nil {
					t.Fatalf("seed ApplyBatch: %v", err)
				}
			}
			var published []pubsub.TranscriptsIndexedEvent
			ingester := NewIngester(store, func(_ context.Context, _ string, _ string, data any) error {
				if tc.publishErr != nil {
					return tc.publishErr
				}
				ev, ok := data.(pubsub.TranscriptsIndexedEvent)
				if !ok {
					return errors.New("published data is not a TranscriptsIndexedEvent")
				}
				published = append(published, ev)
				return nil
			})
			msg := &fakeBatchMessage{data: tc.payload}
			err := ingester.HandleBatchMessage(ctx, msg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleBatchMessage() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if msg.termed != tc.wantTerm {
				t.Errorf("termed = %v, want %v", msg.termed, tc.wantTerm)
			}
			if (len(published) > 0) != tc.wantPublished {
				t.Fatalf("published %d events, wantPublished = %v", len(published), tc.wantPublished)
			}
			if tc.wantPublished && published[0].StepsIndexed != tc.wantIndexed {
				t.Errorf("published StepsIndexed = %d, want %d", published[0].StepsIndexed, tc.wantIndexed)
			}
		})
	}
}

func TestHandleBatchMessageFirstBatch(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	var published []pubsub.TranscriptsIndexedEvent
	ingester := NewIngester(store, func(_ context.Context, _ string, _ string, data any) error {
		ev, ok := data.(pubsub.TranscriptsIndexedEvent)
		if !ok {
			return errors.New("published data is not a TranscriptsIndexedEvent")
		}
		published = append(published, ev)
		return nil
	})
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	msg := &fakeBatchMessage{data: marshalBatch(t, testBatch("sess-new", epoch, 0, 2, false, false))}
	if err := ingester.HandleBatchMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleBatchMessage() err = %v", err)
	}
	if len(published) != 1 || published[0].SessionID != "sess-new" || published[0].Stack != "aerial" {
		t.Fatalf("published = %+v, want one event for sess-new on aerial", published)
	}
}

func TestHandleBatchMessageNilStore(t *testing.T) {
	t.Parallel()
	ingester := NewIngester(nil, nil)
	if err := ingester.HandleBatchMessage(context.Background(), &fakeBatchMessage{data: []byte("{}")}); err == nil {
		t.Errorf("expected error from nil store")
	}
	var nilIngester *Ingester
	if err := nilIngester.HandleBatchMessage(context.Background(), &fakeBatchMessage{data: []byte("{}")}); err == nil {
		t.Errorf("expected error from nil ingester")
	}
}

func TestHandleBatchMessageTermErrors(t *testing.T) {
	t.Parallel()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	blankSession := testBatch("s", epoch, 0, 0, false, false)
	blankSession.SessionID = "  "
	tests := []struct {
		name    string
		payload []byte
		termErr error
		wantSub string
	}{
		{"undecodable term failure", []byte("{broken"), errors.New("term down"), "term undecodable transcript batch"},
		{"empty session term failure", marshalBatch(t, blankSession), errors.New("term down"), "term empty-session transcript batch"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newTestStore(t, "aerial")
			ingester := NewIngester(store, nil)
			msg := &fakeBatchMessage{data: tc.payload, termErr: tc.termErr}
			err := ingester.HandleBatchMessage(context.Background(), msg)
			if err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("err = %v, want substring %q", err, tc.wantSub)
			}
		})
	}
}

func TestHandleBatchMessageNilPublish(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	ingester := NewIngester(store, nil)
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	msg := &fakeBatchMessage{data: marshalBatch(t, testBatch("sess-nilpub", epoch, 0, 1, false, false))}
	if err := ingester.HandleBatchMessage(context.Background(), msg); err != nil {
		t.Errorf("nil publish err = %v", err)
	}
}

func TestHandleBatchMessageNil(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	ingester := NewIngester(store, nil)
	if err := ingester.HandleBatchMessage(context.Background(), nil); err == nil {
		t.Errorf("expected error for nil message")
	}
}

func TestHandleBatchMessageApplyError(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	if err := store.db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	ingester := NewIngester(store, nil)
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	msg := &fakeBatchMessage{data: marshalBatch(t, testBatch("sess-err", epoch, 0, 1, false, false))}
	if err := ingester.HandleBatchMessage(context.Background(), msg); err == nil {
		t.Errorf("expected apply error")
	}
}
