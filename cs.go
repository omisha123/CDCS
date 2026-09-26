package main

import "sync"

// SingleFlightGroup collapses concurrent calls for the same key into a
// single execution of the work function - the classic "singleflight"
// pattern. Used by downloadFileAdaptive (backend.go) so a burst of repeat
// download requests for the same fileID only does the shard fetch +
// Reed-Solomon reconstruct once; every other caller just waits on the
// result of the call already in flight.
//
// This is a small self-contained implementation of the same idea as
// golang.org/x/sync/singleflight, kept local so the project doesn't need a
// new module dependency for it.
type SingleFlightGroup struct {
	mu sync.Mutex
	m  map[string]*sfCall
}

// sfCall tracks one in-flight (or just-completed) execution for a key.
type sfCall struct {
	wg   sync.WaitGroup
	val  []byte
	name string
	err  error
}

// NewSingleFlightGroup returns a ready-to-use group.
func NewSingleFlightGroup() *SingleFlightGroup {
	return &SingleFlightGroup{m: make(map[string]*sfCall)}
}

// Do executes fn for key, unless a call for that key is already in flight -
// in which case it waits for and returns that call's result instead of
// running fn again. The returned shared bool reports whether this caller
// piggybacked on someone else's in-flight call (true) or actually triggered
// fn itself (false).
func (g *SingleFlightGroup) Do(key string, fn func() ([]byte, string, error)) (val []byte, name string, err error, shared bool) {
	g.mu.Lock()
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.name, c.err, true
	}

	c := new(sfCall)
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.name, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()

	return c.val, c.name, c.err, false
}