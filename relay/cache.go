package relay

import (
	"sync"
	"sync/atomic"
	"time"
)

// CachedObject is a MoQ object received from the upstream publisher.
type CachedObject struct {
	GroupID    uint64
	SubGroupID uint64
	ObjectID   uint64
	Payload    []byte
	RxTime     time.Time
}

const subscriberChanSize = 2048

// TrackCache fans out objects to per-subscriber channels and buffers history
// so late subscribers can replay.
type TrackCache struct {
	mu           sync.Mutex
	subs         []chan *CachedObject
	history      []*CachedObject
	closed       bool
	Ingested     atomic.Uint64 // total objects added via Put
	ChanDrops    atomic.Uint64 // objects dropped due to full subscriber channel
}

func NewTrackCache() *TrackCache {
	return &TrackCache{}
}

// Put fans obj out to all subscriber channels, dropping for any subscriber whose
// channel is full.
func (c *TrackCache) Put(obj *CachedObject) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Ingested.Add(1)
	c.history = append(c.history, obj)
	for _, ch := range c.subs {
		select {
		case ch <- obj:
		default:
			c.ChanDrops.Add(1)
		}
	}
}

// Subscribe returns a channel that replays the buffered history and then
// receives new objects. If the cache is already closed, the channel is
// returned pre-closed after the replay.
func (c *TrackCache) Subscribe() <-chan *CachedObject {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan *CachedObject, len(c.history)+subscriberChanSize)
	for _, obj := range c.history {
		ch <- obj
	}
	if c.closed {
		close(ch)
	} else {
		c.subs = append(c.subs, ch)
	}
	return ch
}

func (c *TrackCache) Unsubscribe(ch <-chan *CachedObject) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, s := range c.subs {
		if s == ch {
			c.subs = append(c.subs[:i], c.subs[i+1:]...)
			close(s)
			return
		}
	}
}

// CloseAll closes all subscriber channels. Future Subscribe calls return a
// pre-closed channel.
func (c *TrackCache) CloseAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	for _, ch := range c.subs {
		close(ch)
	}
	c.subs = nil
}
