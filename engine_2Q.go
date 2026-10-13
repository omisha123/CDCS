package main

import (
	"container/list"
	"sync"
)

const (
	qA1in = iota
	qAm
	qA1out
)

// CacheEntry holds the payload for active queues.
// queue records which list the entry currently lives in, so we never
// have to scan a list to find out.
type CacheEntry struct {
	Key   string
	Data  []byte
	queue int
}

// InspectResult packages the queue states for the orchestrator's dashboard.
type InspectResult struct {
	A1in  []string
	Am    []string
	A1out []string
}

// TwoQEngine blends FIFO (A1in) and LRU (Am) queues with a ghost cache (A1out).
type TwoQEngine struct {
	mu sync.RWMutex

	capacity int // max real entries: a1in + am
	a1inCap  int // target size of A1in (only enforced when the cache is full)
	a1outCap int // max ghost keys

	a1in  *list.List
	a1out *list.List
	am    *list.List

	items map[string]*list.Element
}

func NewTwoQEngine(totalCapacity int) *TwoQEngine {
	if totalCapacity < 4 {
		totalCapacity = 4
	}
	return &TwoQEngine{
		capacity: totalCapacity,
		a1inCap:  totalCapacity / 4,
		a1outCap: totalCapacity / 2,
		a1in:     list.New(),
		a1out:    list.New(),
		am:       list.New(),
		items:    make(map[string]*list.Element),
	}
}

// Get returns data only for real (in-memory) entries. Ghosts are misses.
func (c *TwoQEngine) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, found := c.items[key]
	if !found {
		return nil, false
	}
	entry := el.Value.(*CacheEntry)

	switch entry.queue {
	case qAm:
		c.am.MoveToFront(el)
		return entry.Data, true
	case qA1in:
		// FIFO: a hit in A1in does not change its position.
		return entry.Data, true
	default: // ghost
		return nil, false
	}
}

// Put inserts or updates a key.
func (c *TwoQEngine) Put(key string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, found := c.items[key]; found {
		entry := el.Value.(*CacheEntry)
		switch entry.queue {
		case qA1out:
			// Seen again after leaving A1in: genuinely hot, promote to Am.
			c.a1out.Remove(el)
			entry.Data = data
			entry.queue = qAm
			c.items[key] = c.am.PushFront(entry)
			c.reclaim()
		case qAm:
			entry.Data = data
			c.am.MoveToFront(el)
		default: // qA1in
			entry.Data = data
		}
		return
	}

	entry := &CacheEntry{Key: key, Data: data, queue: qA1in}
	c.items[key] = c.a1in.PushFront(entry)
	c.reclaim()
}

// Delete removes a key entirely (real entry or ghost).
func (c *TwoQEngine) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, found := c.items[key]
	if !found {
		return
	}
	switch el.Value.(*CacheEntry).queue {
	case qA1in:
		c.a1in.Remove(el)
	case qAm:
		c.am.Remove(el)
	default:
		c.a1out.Remove(el)
	}
	delete(c.items, key)
}

// Inspect returns a snapshot of the current queue states for the UI.
func (c *TwoQEngine) Inspect() InspectResult {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return InspectResult{
		A1in:  c.extractKeys(c.a1in),
		Am:    c.extractKeys(c.am),
		A1out: c.extractKeys(c.a1out),
	}
}

// reclaim frees space ONLY when real entries exceed total capacity.
// Until then, A1in is allowed to use any free space, so the cache never
// ghosts entries while RAM sits empty.
func (c *TwoQEngine) reclaim() {
	for c.a1in.Len()+c.am.Len() > c.capacity {
		if c.a1in.Len() > c.a1inCap || c.am.Len() == 0 {
			// A1in is over its share: demote its oldest entry to a ghost.
			el := c.a1in.Back()
			c.a1in.Remove(el)
			entry := el.Value.(*CacheEntry)
			entry.Data = nil
			entry.queue = qA1out
			c.items[entry.Key] = c.a1out.PushFront(entry)
		} else {
			// Otherwise evict the least recently used hot entry outright.
			el := c.am.Back()
			c.am.Remove(el)
			delete(c.items, el.Value.(*CacheEntry).Key)
		}
	}

	for c.a1out.Len() > c.a1outCap {
		el := c.a1out.Back()
		c.a1out.Remove(el)
		delete(c.items, el.Value.(*CacheEntry).Key)
	}
}

func (c *TwoQEngine) extractKeys(l *list.List) []string {
	keys := make([]string, 0, l.Len())
	for e := l.Front(); e != nil; e = e.Next() {
		keys = append(keys, e.Value.(*CacheEntry).Key)
	}
	return keys
}