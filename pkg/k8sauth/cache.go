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

// LRUCache is a thread-safe bounded LRU cache with per-item TTL expiration.
type LRUCache[V any] struct {
	mu         sync.Mutex
	maxEntries int
	ttl        time.Duration
	now        func() time.Time
	entries    map[string]*list.Element
	lruList    *list.List
}

// NewLRUCache constructs a bounded LRU cache with a default TTL and max entry limit.
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
		entries:    make(map[string]*list.Element),
		lruList:    list.New(),
	}
}

// Get returns the cached value for key if present and not expired.
func (c *LRUCache[V]) Get(key string) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.entries[key]
	if !ok {
		var zero V
		return zero, false
	}

	entry, ok := elem.Value.(*cacheEntry[V])
	if !ok || !c.now().Before(entry.expiresAt) {
		c.lruList.Remove(elem)
		delete(c.entries, key)
		var zero V
		return zero, false
	}

	c.lruList.MoveToFront(elem)
	return entry.value, true
}

// Set stores or updates key with value and resets its expiration to now + ttl.
func (c *LRUCache[V]) Set(key string, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	expiresAt := c.now().Add(c.ttl)

	if elem, ok := c.entries[key]; ok {
		if entry, ok := elem.Value.(*cacheEntry[V]); ok {
			entry.value = value
			entry.expiresAt = expiresAt
			c.lruList.MoveToFront(elem)
			return
		}
	}

	if c.lruList.Len() >= c.maxEntries {
		oldest := c.lruList.Back()
		if oldest != nil {
			c.lruList.Remove(oldest)
			if oldestEntry, ok := oldest.Value.(*cacheEntry[V]); ok {
				delete(c.entries, oldestEntry.key)
			}
		}
	}

	entry := &cacheEntry[V]{
		key:       key,
		value:     value,
		expiresAt: expiresAt,
	}
	elem := c.lruList.PushFront(entry)
	c.entries[key] = elem
}

// Delete removes an entry from the cache.
func (c *LRUCache[V]) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.entries[key]; ok {
		c.lruList.Remove(elem)
		delete(c.entries, key)
	}
}

// Purge removes all entries from the cache.
func (c *LRUCache[V]) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make(map[string]*list.Element)
	c.lruList.Init()
}

// Len returns the current number of items in the cache (including potentially un-evicted expired items).
func (c *LRUCache[V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.lruList.Len()
}
