package k8sauth

import (
	"container/list"
	"sync"
	"time"
)

type cacheEntry[V any] struct {
	key       string
	value     V
	expiresAt time.Time
}

// LRUCache is a thread-safe bounded LRU cache with TTL expiry and an injectable clock.
type LRUCache[V any] struct {
	mu         sync.Mutex
	maxEntries int
	ttl        time.Duration
	now        func() time.Time
	ll         *list.List
	items      map[string]*list.Element
}

// NewLRUCache constructs a bounded LRU cache with a given capacity, TTL, and clock function.
func NewLRUCache[V any](maxEntries int, ttl time.Duration, now func() time.Time) *LRUCache[V] {
	if maxEntries <= 0 {
		maxEntries = 1024
	}
	if ttl <= 0 {
		ttl = 45 * time.Second
	}
	if now == nil {
		now = time.Now
	}
	return &LRUCache[V]{
		maxEntries: maxEntries,
		ttl:        ttl,
		now:        now,
		ll:         list.New(),
		items:      make(map[string]*list.Element, maxEntries),
	}
}

// Get retrieves an item by key if it exists and has not expired.
// Expired items are evicted immediately on access.
func (c *LRUCache[V]) Get(key string) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.items[key]
	if !ok {
		var zero V
		return zero, false
	}

	entry, ok := el.Value.(*cacheEntry[V])
	if !ok {
		var zero V
		return zero, false
	}
	if !c.now().Before(entry.expiresAt) {
		c.ll.Remove(el)
		delete(c.items, key)
		var zero V
		return zero, false
	}

	c.ll.MoveToFront(el)
	return entry.value, true
}

// Set adds or updates an item in the cache with the configured TTL.
// When capacity is exceeded, the least recently used entry is evicted.
func (c *LRUCache[V]) Set(key string, val V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	expiresAt := c.now().Add(c.ttl)

	if el, ok := c.items[key]; ok {
		if entry, ok := el.Value.(*cacheEntry[V]); ok {
			entry.value = val
			entry.expiresAt = expiresAt
			c.ll.MoveToFront(el)
			return
		}
	}

	if c.ll.Len() >= c.maxEntries {
		oldest := c.ll.Back()
		if oldest != nil {
			c.ll.Remove(oldest)
			if oldEntry, ok := oldest.Value.(*cacheEntry[V]); ok {
				delete(c.items, oldEntry.key)
			}
		}
	}

	entry := &cacheEntry[V]{
		key:       key,
		value:     val,
		expiresAt: expiresAt,
	}
	el := c.ll.PushFront(entry)
	c.items[key] = el
}

// Delete removes an item from the cache.
func (c *LRUCache[V]) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		c.ll.Remove(el)
		delete(c.items, key)
	}
}

// Len returns the number of active entries currently stored in the cache.
func (c *LRUCache[V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}

// Clear removes all entries from the cache.
func (c *LRUCache[V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ll.Init()
	c.items = make(map[string]*list.Element, c.maxEntries)
}
