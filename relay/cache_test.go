package relay

import (
	"testing"
	"time"
)

func TestCache_PutFansOutToSubscribers(t *testing.T) {
	c := NewTrackCache()

	ch1 := c.Subscribe()
	ch2 := c.Subscribe()

	obj := &CachedObject{
		GroupID:    1,
		SubGroupID: 0,
		ObjectID:   0,
		Payload:    []byte("hello"),
		RxTime:     time.Now(),
	}
	c.Put(obj)

	select {
	case got := <-ch1:
		if got.GroupID != 1 || string(got.Payload) != "hello" {
			t.Fatalf("ch1: unexpected object: %+v", got)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ch1: timed out waiting for object")
	}

	select {
	case got := <-ch2:
		if got.GroupID != 1 || string(got.Payload) != "hello" {
			t.Fatalf("ch2: unexpected object: %+v", got)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ch2: timed out waiting for object")
	}
}

func TestCache_PutDropsWhenChannelFull(t *testing.T) {
	c := NewTrackCache()
	ch := c.Subscribe()

	for i := range cap(ch) {
		c.Put(&CachedObject{ObjectID: uint64(i), Payload: []byte("x")})
	}

	done := make(chan struct{})
	go func() {
		c.Put(&CachedObject{ObjectID: 999, Payload: []byte("overflow")})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Put blocked on full channel")
	}
}

func TestCache_UnsubscribeRemovesChannel(t *testing.T) {
	c := NewTrackCache()
	ch := c.Subscribe()
	c.Unsubscribe(ch)

	c.Put(&CachedObject{ObjectID: 0, Payload: []byte("after unsub")})

	_, ok := <-ch
	if ok {
		t.Fatal("expected channel to be closed")
	}
}

func TestCache_CloseAllClosesAllChannels(t *testing.T) {
	c := NewTrackCache()
	ch1 := c.Subscribe()
	ch2 := c.Subscribe()

	c.CloseAll()

	_, ok1 := <-ch1
	_, ok2 := <-ch2
	if ok1 || ok2 {
		t.Fatal("expected all channels to be closed")
	}
}
