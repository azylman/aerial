package pubsub

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/propagation"
)

// MemoryBus provides a hermetic, in-memory implementation of Bus for zero-socket unit tests.
type MemoryBus struct {
	mu           sync.RWMutex
	subscribers  []*memSubscription
	duplicates   map[string]time.Time
	coalescedSeq map[string]uint64
	roundRobin   map[string]uint64
	streams      map[string]bool
	closed       bool
	subCounter   uint64
}

// NewMemoryBus creates a new hermetic MemoryBus.
func NewMemoryBus() *MemoryBus {
	return &MemoryBus{
		duplicates:   make(map[string]time.Time),
		coalescedSeq: make(map[string]uint64),
		roundRobin:   make(map[string]uint64),
		streams:      make(map[string]bool),
	}
}

// Publish publishes a message to MemoryBus with trace injection and deduplication.
func (b *MemoryBus) Publish(ctx context.Context, subject string, msgID string, data any) error {
	return b.PublishWithHeader(ctx, subject, msgID, data, nil)
}

// PublishWithHeader publishes a message with custom headers and trace context.
func (b *MemoryBus) PublishWithHeader(ctx context.Context, subject string, msgID string, data any, header nats.Header) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return fmt.Errorf("pubsub: memory bus closed")
	}

	// 1. Deduplication window check (30 minutes)
	if msgID != "" {
		if seenAt, exists := b.duplicates[msgID]; exists && time.Since(seenAt) < 30*time.Minute {
			b.mu.Unlock()
			return nil // Benign deduplication
		}
		b.duplicates[msgID] = time.Now()
	}

	// 2. Coalescing tracking for aerial.coalesced.>
	var currentSeq uint64
	if strings.HasPrefix(subject, SubjectAerialCoalescedPrefix) {
		b.coalescedSeq[subject]++
		currentSeq = b.coalescedSeq[subject]
	}

	// 3. Serialize payload
	payload, err := serializePayload(data)
	if err != nil {
		b.mu.Unlock()
		return err
	}

	// 4. Trace injection
	mergedHeader := nats.Header{}
	for k, vv := range header {
		for _, v := range vv {
			mergedHeader.Add(k, v)
		}
	}
	if msgID != "" {
		mergedHeader.Set(HeaderMsgID, msgID)
	}

	carrier := propagation.HeaderCarrier(http.Header(mergedHeader))
	propagation.TraceContext{}.Inject(ctx, carrier)

	// 5. Snapshot subscribers under read lock
	subs := make([]*memSubscription, len(b.subscribers))
	copy(subs, b.subscribers)
	b.mu.Unlock()

	// 6. Fan-out pub/sub subscribers (grouped by durable)
	durableGroups := make(map[string][]*memSubscription)
	for _, sub := range subs {
		if sub.queue == "" && matchSubject(sub.subject, subject) {
			key := sub.durable
			if key == "" {
				key = "direct-" + sub.id
			}
			durableGroups[key] = append(durableGroups[key], sub)
		}
	}

	for key, group := range durableGroups {
		b.mu.Lock()
		idx := int(b.roundRobin[key] % uint64(len(group)))
		b.roundRobin[key]++
		b.mu.Unlock()
		targetSub := group[idx]
		subHeader := cloneHeader(mergedHeader)
		go b.dispatchToSub(targetSub, subject, payload, subHeader, currentSeq, 1)
	}

	// 7. Competing consumer queue workers (grouped by queue)
	queueGroups := make(map[string][]*memSubscription)
	for _, sub := range subs {
		if sub.queue != "" && matchSubject(sub.subject, subject) {
			queueGroups[sub.queue] = append(queueGroups[sub.queue], sub)
		}
	}

	for qName, group := range queueGroups {
		b.mu.Lock()
		idx := int(b.roundRobin[qName] % uint64(len(group)))
		b.roundRobin[qName]++
		b.mu.Unlock()
		targetSub := group[idx]
		subHeader := cloneHeader(mergedHeader)
		go b.dispatchToSub(targetSub, subject, payload, subHeader, currentSeq, 1)
	}

	return nil
}

func cloneHeader(h nats.Header) nats.Header {
	if h == nil {
		return nats.Header{}
	}
	cloned := make(nats.Header, len(h))
	for k, vv := range h {
		cp := make([]string, len(vv))
		copy(cp, vv)
		cloned[k] = cp
	}
	return cloned
}

func (b *MemoryBus) dispatchToSub(sub *memSubscription, subject string, data []byte, header nats.Header, coalescedSeq uint64, delivered uint64) {
	ctx := context.Background()
	carrier := propagation.HeaderCarrier(http.Header(header))
	ctx = propagation.TraceContext{}.Extract(ctx, carrier)

	msg := &memMessage{
		bus:          b,
		sub:          sub,
		data:         data,
		subject:      subject,
		header:       header,
		coalescedSeq: coalescedSeq,
		delivered:    delivered,
	}

	if progErr := msg.InProgress(); progErr != nil {
		log.Printf("[pubsub/memory] warning: initial in-progress failed: %v", progErr)
	}

	err := sub.handler(ctx, msg)

	msg.mu.Lock()
	settled := msg.settled
	msg.mu.Unlock()

	if settled {
		return
	}
	if err == nil {
		if ackErr := msg.Ack(); ackErr != nil {
			log.Printf("[pubsub/memory] warning: automatic ack failed: %v", ackErr)
		}
	} else {
		if nakErr := msg.NakWithDelay(0); nakErr != nil {
			log.Printf("[pubsub/memory] warning: automatic nak failed: %v", nakErr)
		}
	}
}

// Subscribe registers a pub/sub subscription.
func (b *MemoryBus) Subscribe(ctx context.Context, subject string, durable string, handler Handler) (Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, fmt.Errorf("pubsub: memory bus closed")
	}

	b.subCounter++
	sub := &memSubscription{
		id:      fmt.Sprintf("sub-%d", b.subCounter),
		bus:     b,
		subject: subject,
		durable: durable,
		handler: handler,
	}
	b.subscribers = append(b.subscribers, sub)
	return sub, nil
}

// ConsumeQueue registers a competing-consumer work queue subscription.
func (b *MemoryBus) ConsumeQueue(ctx context.Context, subject string, queue string, handler Handler) (Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, fmt.Errorf("pubsub: memory bus closed")
	}

	b.subCounter++
	sub := &memSubscription{
		id:      fmt.Sprintf("queue-sub-%d", b.subCounter),
		bus:     b,
		subject: subject,
		queue:   queue,
		handler: handler,
	}
	b.subscribers = append(b.subscribers, sub)
	return sub, nil
}

// EnsureCoreStreams records the 3 primary streams as registered.
func (b *MemoryBus) EnsureCoreStreams(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.streams[StreamAerialEvents] = true
	b.streams[StreamAerialJobs] = true
	b.streams[StreamAerialCoalesced] = true
	return nil
}

// Close closes the memory bus and unregisters all subscribers.
func (b *MemoryBus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	b.subscribers = nil
	return nil
}

func (b *MemoryBus) removeSubscription(target *memSubscription) {
	b.mu.Lock()
	defer b.mu.Unlock()
	filtered := b.subscribers[:0]
	for _, s := range b.subscribers {
		if s != target {
			filtered = append(filtered, s)
		}
	}
	b.subscribers = filtered
}

type memSubscription struct {
	id        string
	bus       *MemoryBus
	subject   string
	durable   string
	queue     string
	handler   Handler
	unsubOnce sync.Once
}

func (s *memSubscription) Unsubscribe() error {
	s.unsubOnce.Do(func() {
		s.bus.removeSubscription(s)
	})
	return nil
}

type memMessage struct {
	bus          *MemoryBus
	sub          *memSubscription
	data         []byte
	subject      string
	header       nats.Header
	coalescedSeq uint64
	delivered    uint64
	mu           sync.Mutex
	settled      bool
}

func (m *memMessage) Data() []byte {
	return m.data
}

func (m *memMessage) Subject() string {
	return m.subject
}

func (m *memMessage) Header() nats.Header {
	if m.header == nil {
		m.header = nats.Header{}
	}
	return m.header
}

func (m *memMessage) NumDelivered() uint64 {
	return atomic.LoadUint64(&m.delivered)
}

func (m *memMessage) InProgress() error {
	m.mu.Lock()
	if m.settled {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	// Check coalesced stream sequence
	if m.coalescedSeq > 0 {
		m.bus.mu.RLock()
		activeSeq := m.bus.coalescedSeq[m.subject]
		m.bus.mu.RUnlock()
		if activeSeq > m.coalescedSeq {
			return nats.ErrMsgNotFound
		}
	}
	return nil
}

func (m *memMessage) Ack() error {
	m.mu.Lock()
	if m.settled {
		m.mu.Unlock()
		return nil
	}
	m.settled = true
	m.mu.Unlock()

	if m.coalescedSeq > 0 {
		m.bus.mu.RLock()
		activeSeq := m.bus.coalescedSeq[m.subject]
		m.bus.mu.RUnlock()
		if activeSeq > m.coalescedSeq {
			return nil // Benign no-op on superseded sequence
		}
	}
	return nil
}

func (m *memMessage) NakWithDelay(delay time.Duration) error {
	m.mu.Lock()
	if m.settled {
		m.mu.Unlock()
		return nil
	}
	m.settled = true
	m.mu.Unlock()

	// Redeliver immediately to test worker without sleep
	go func() {
		m.bus.dispatchToSub(m.sub, m.subject, m.data, m.header, m.coalescedSeq, atomic.AddUint64(&m.delivered, 1))
	}()
	return nil
}

func (m *memMessage) Term() error {
	m.mu.Lock()
	if m.settled {
		m.mu.Unlock()
		return nil
	}
	m.settled = true
	m.mu.Unlock()
	return nil
}

func (m *memMessage) Snooze(delay time.Duration) error {
	m.mu.Lock()
	if m.settled {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	// 1. Calculate next snooze sequence
	seq := 0
	if seqStr := m.Header().Get(HeaderSnoozeSeq); seqStr != "" {
		if s, err := strconv.Atoi(seqStr); err == nil {
			seq = s
		}
	}
	nextSeq := seq + 1

	// 2. Derive base msg ID and rotate suffix
	msgID := m.Header().Get(HeaderMsgID)
	if idx := strings.Index(msgID, ":snooze:"); idx != -1 {
		msgID = msgID[:idx]
	}
	if msgID == "" {
		msgID = fmt.Sprintf("msg-%d", time.Now().UnixNano())
	}
	rotatedMsgID := fmt.Sprintf("%s:snooze:%d", msgID, nextSeq)

	// 3. Clone headers and set rotated values
	newHeader := cloneHeader(m.Header())
	newHeader.Set(HeaderMsgID, rotatedMsgID)
	newHeader.Set(HeaderSnoozeSeq, strconv.Itoa(nextSeq))

	publishContinuation := func() error {
		ctx := context.Background()
		return m.bus.PublishWithHeader(ctx, m.Subject(), rotatedMsgID, m.Data(), newHeader)
	}

	if delay > 0 {
		m.mu.Lock()
		m.settled = true
		m.mu.Unlock()

		time.AfterFunc(delay, func() {
			if pubErr := publishContinuation(); pubErr != nil {
				log.Printf("[pubsub/memory] warning: background snooze publish failed: %v", pubErr)
			}
		})
	} else {
		if err := publishContinuation(); err != nil {
			return fmt.Errorf("snooze publish continuation: %w", err)
		}
		m.mu.Lock()
		m.settled = true
		m.mu.Unlock()
	}

	// 5. Ack current delivery with coalesced check
	if m.coalescedSeq > 0 {
		m.bus.mu.RLock()
		activeSeq := m.bus.coalescedSeq[m.subject]
		m.bus.mu.RUnlock()
		if activeSeq > m.coalescedSeq {
			return nil // Benign no-op on superseded sequence
		}
	}
	return nil
}

func matchSubject(pattern, subject string) bool {
	if pattern == subject || pattern == ">" {
		return true
	}
	pTokens := strings.Split(pattern, ".")
	sTokens := strings.Split(subject, ".")
	for i := range pTokens {
		if pTokens[i] == ">" {
			return true
		}
		if i >= len(sTokens) {
			return false
		}
		if pTokens[i] != "*" && pTokens[i] != sTokens[i] {
			return false
		}
	}
	return len(pTokens) == len(sTokens)
}
