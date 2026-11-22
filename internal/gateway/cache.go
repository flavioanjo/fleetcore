package gateway

import (
	"container/list"

	"github.com/flavioanjo/fleetcore/internal/eventstore"
)

// dedupCache is a bounded LRU of identities known to be stored, with the
// fingerprint of the stored payload. It only short-circuits exact
// retransmissions: a miss, or a hit with a different fingerprint, goes to
// the store, which is authoritative. Entries are added only from store
// results, so the cache never claims an event is stored when it is not.
//
// It is used by the single writer goroutine and is not safe for concurrent
// use.
type dedupCache struct {
	max   int
	items map[eventstore.Key]*list.Element
	lru   *list.List // of cacheEntry; front = most recent
}

type cacheEntry struct {
	key eventstore.Key
	fp  eventstore.Fingerprint
}

// newDedupCache returns a cache holding up to max entries; max <= 0
// disables it.
func newDedupCache(max int) *dedupCache {
	return &dedupCache{max: max, items: map[eventstore.Key]*list.Element{}, lru: list.New()}
}

func (c *dedupCache) isStoredDuplicate(k eventstore.Key, fp eventstore.Fingerprint) bool {
	el, ok := c.items[k]
	if !ok || el.Value.(cacheEntry).fp != fp {
		return false
	}
	c.lru.MoveToFront(el)
	return true
}

func (c *dedupCache) add(k eventstore.Key, fp eventstore.Fingerprint) {
	if c.max <= 0 {
		return
	}
	if el, ok := c.items[k]; ok {
		el.Value = cacheEntry{key: k, fp: fp}
		c.lru.MoveToFront(el)
		return
	}
	c.items[k] = c.lru.PushFront(cacheEntry{key: k, fp: fp})
	for c.lru.Len() > c.max {
		old := c.lru.Remove(c.lru.Back()).(cacheEntry)
		delete(c.items, old.key)
	}
}
