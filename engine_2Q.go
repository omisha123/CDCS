package main

import (
	"container/list"
	"sync"
)

// CacheEntry holds the payload for active queues.
type CacheEntry struct {
	Key  string
	Data []byte
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

	capacity int
	a1inCap  int
	a1outCap int
	amCap    int

	a1in  *list.List
	a1out *list.List
	am    *list.List

	items map[string]*list.Element
}

// NewTwoQEngine initializes the cache engine with proportional queue limits.
func NewTwoQEngine(totalCapacity int) *TwoQEngine {
	// Standard 2Q ratio: 25% A1in, 50% A1out, 25% Am
	return &TwoQEngine{
		capacity: totalCapacity,
		a1inCap:  totalCapacity / 4,
		a1outCap: totalCapacity / 2,
		amCap:    totalCapacity / 4,
		a1in:     list.New(),
		a1out:    list.New(),
		am:       list.New(),
		items:    make(map[string]*list.Element),
	}
}

// Get retrieves data and handles queue promotion if a ghost key is accessed.
func (c *TwoQEngine) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, found := c.items[key]
	if !found {
		return nil, false
	}

	// If it's in the A1out ghost cache, it's genuinely hot. Promote to Am (LRU).
	if el.Value.(*CacheEntry).Data == nil {
		return nil, false // Data isn't actually in memory, it's just a ghost record
	}

	// If it's already in Am, mark it as recently used by moving it to the front
	// Note: We leave A1in items alone until they are pushed out to A1out
	if c.isAm(el) {
		c.am.MoveToFront(el)
	}

	return el.Value.(*CacheEntry).Data, true
}

// Put inserts new data, managing capacity and cascading evictions across the queues.
func (c *TwoQEngine) Put(key string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, found := c.items[key]; found {
		entry := el.Value.(*CacheEntry)
		
		// If it's a ghost record (A1out), promote it to the hot LRU queue (Am)
		if entry.Data == nil {
			c.a1out.Remove(el)
			entry.Data = data
			c.items[key] = c.am.PushFront(entry)
			c.enforceAmCapacity()
			return
		}

		// Update existing hot data
		entry.Data = data
		if c.isAm(el) {
			c.am.MoveToFront(el)
		}
		return
	}

	// Brand new item: push to A1in (FIFO)
	entry := &CacheEntry{Key: key, Data: data}
	c.items[key] = c.a1in.PushFront(entry)
	c.enforceA1inCapacity()
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

// -- Internal Capacity Management & Helpers --

func (c *TwoQEngine) enforceA1inCapacity() {
	for c.a1in.Len() > c.a1inCap {
		evicted := c.a1in.Back()
		c.a1in.Remove(evicted)
		
		// Strip the data to save RAM, but keep the key in the ghost cache (A1out)
		entry := evicted.Value.(*CacheEntry)
		entry.Data = nil 
		c.items[entry.Key] = c.a1out.PushFront(entry)
	}
	
	// Ensure the ghost cache doesn't grow infinitely
	for c.a1out.Len() > c.a1outCap {
		evicted := c.a1out.Back()
		c.a1out.Remove(evicted)
		delete(c.items, evicted.Value.(*CacheEntry).Key)
	}
}

func (c *TwoQEngine) enforceAmCapacity() {
	for c.am.Len() > c.amCap {
		evicted := c.am.Back()
		c.am.Remove(evicted)
		delete(c.items, evicted.Value.(*CacheEntry).Key)
	}
}

func (c *TwoQEngine) isAm(el *list.Element) bool {
	// A quick check to see if an element belongs to the Am queue
	for e := c.am.Front(); e != nil; e = e.Next() {
		if e == el {
			return true
		}
	}
	return false
}

func (c *TwoQEngine) extractKeys(l *list.List) []string {
	var keys []string
	for e := l.Front(); e != nil; e = e.Next() {
		keys = append(keys, e.Value.(*CacheEntry).Key)
	}
	return keys
}