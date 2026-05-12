package main_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"moq-relay/publisher"
	"moq-relay/relay"
	"moq-relay/subscriber"
)

type slowFrameSource struct {
	frames []*publisher.Frame
	idx    int
	delay  time.Duration
	ready  chan struct{} // blocks first call until closed
}

func (s *slowFrameSource) NextFrame() (*publisher.Frame, error) {
	if s.ready != nil && s.idx == 0 {
		<-s.ready
	}
	if s.idx >= len(s.frames) {
		return nil, io.EOF
	}
	if s.idx > 0 {
		time.Sleep(s.delay)
	}
	f := s.frames[s.idx]
	s.idx++
	return f, nil
}

// testFrames returns 6 frames: GoP 0 (IDR, P, P), GoP 1 (IDR, P, P).
func testFrames() []*publisher.Frame {
	return []*publisher.Frame{
		{NALType: 5, IsIDR: true, Data: []byte{0, 0, 0, 1, 5, 0x10}, FrameNum: 0},
		{NALType: 1, IsIDR: false, Data: []byte{0, 0, 0, 1, 1, 0x11}, FrameNum: 1},
		{NALType: 1, IsIDR: false, Data: []byte{0, 0, 0, 1, 1, 0x12}, FrameNum: 2},
		{NALType: 5, IsIDR: true, Data: []byte{0, 0, 0, 1, 5, 0x20}, FrameNum: 3},
		{NALType: 1, IsIDR: false, Data: []byte{0, 0, 0, 1, 1, 0x21}, FrameNum: 4},
		{NALType: 1, IsIDR: false, Data: []byte{0, 0, 0, 1, 1, 0x22}, FrameNum: 5},
	}
}

func TestTimeoutStrategy_None(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	testTimeoutStrategy(t, relay.StrategyNone, 50*time.Millisecond, 200*time.Millisecond, 6)
}

func TestTimeoutStrategy_ResetStream(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	got := testTimeoutStrategyAged(t, relay.StrategyResetStream, 50*time.Millisecond)
	if got != 0 {
		t.Errorf("expected 0 objects with reset-stream (all expired), got %d", got)
	}
	t.Logf("received %d/6 objects with reset-stream strategy", got)
}

func TestTimeoutStrategy_ResetAtKeyframe(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	got := testTimeoutStrategyAged(t, relay.StrategyResetAtKeyframe, 50*time.Millisecond)
	if got != 0 {
		t.Errorf("expected 0 objects with reset-at-keyframe (all expired), got %d", got)
	}
	t.Logf("received %d/6 objects with reset-at-keyframe strategy", got)
}

func TestTimeoutStrategy_ResetAtObject_Fresh(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	testTimeoutStrategy(t, relay.StrategyResetAtObject, 50*time.Millisecond, 0, 6)
}

func testTimeoutStrategy(
	t *testing.T,
	strategy relay.TimeoutStrategy,
	timeout time.Duration,
	frameDelay time.Duration,
	wantExact int,
) int {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tlsConf := generateTLS(t)
	pubAddr := pickPort(t)
	relayAddr := pickPort(t)
	namespace := []string{"test"}
	trackName := "video"
	dir := t.TempDir()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	startPub := make(chan struct{})

	pub := publisher.NewPublisher(publisher.Config{
		ListenAddr: pubAddr,
		Namespace:  namespace,
		TrackName:  trackName,
		Realtime:   false,
		TLSConfig:  tlsConf,
		Source:     &slowFrameSource{frames: testFrames(), delay: frameDelay, ready: startPub},
		FrameRate:  30.0,
	}, logger)
	go pub.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	r := relay.New(relay.Config{
		ListenAddr:     relayAddr,
		UpstreamAddr:   pubAddr,
		Namespace:      namespace,
		TrackName:      trackName,
		TLSConfig:      tlsConf,
		Strategy:       strategy,
		DefaultTimeout: timeout,
		MetricsPath:    fmt.Sprintf("%s/metrics.csv", dir),
	}, logger)

	if err := r.ConnectUpstream(); err != nil {
		t.Fatal(err)
	}
	go r.ListenAndServe()
	time.Sleep(100 * time.Millisecond)

	sink := newCollectSink()
	sub := subscriber.NewSubscriber(subscriber.SubConfig{
		RelayAddr:      relayAddr,
		Namespace:      namespace,
		TrackName:      trackName,
		RecvLogPath:    fmt.Sprintf("%s/recv_log.csv", dir),
		DisplayLogPath: fmt.Sprintf("%s/display_log.csv", dir),
		Sink:           sink,
	}, logger)

	subDone := make(chan error, 1)
	go func() {
		subDone <- sub.Run(ctx)
	}()

	time.Sleep(200 * time.Millisecond)
	close(startPub)

	deadline := time.After(10 * time.Second)
	lastCount := 0
	stableFor := 0
	for {
		n := sink.Len()
		if wantExact >= 0 && n >= wantExact {
			break
		}
		if n == lastCount {
			stableFor++
		} else {
			stableFor = 0
		}
		lastCount = n
		if stableFor > 40 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for objects, got %d", n)
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	got := sink.Len()
	if wantExact >= 0 && got != wantExact {
		t.Errorf("got %d objects, want %d", got, wantExact)
	}

	cancel()
	r.Shutdown()
	<-subDone

	return got
}

func testTimeoutStrategyAged(
	t *testing.T,
	strategy relay.TimeoutStrategy,
	timeout time.Duration,
) int {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tlsConf := generateTLS(t)
	pubAddr := pickPort(t)
	relayAddr := pickPort(t)
	namespace := []string{"test"}
	trackName := "video"
	dir := t.TempDir()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Publish all frames instantly, no pacing.
	pub := publisher.NewPublisher(publisher.Config{
		ListenAddr: pubAddr,
		Namespace:  namespace,
		TrackName:  trackName,
		Realtime:   false,
		TLSConfig:  tlsConf,
		Source:     &slowFrameSource{frames: testFrames(), delay: 0},
		FrameRate:  30.0,
	}, logger)
	go pub.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	r := relay.New(relay.Config{
		ListenAddr:     relayAddr,
		UpstreamAddr:   pubAddr,
		Namespace:      namespace,
		TrackName:      trackName,
		TLSConfig:      tlsConf,
		Strategy:       strategy,
		DefaultTimeout: timeout,
		MetricsPath:    fmt.Sprintf("%s/metrics.csv", dir),
	}, logger)

	if err := r.ConnectUpstream(); err != nil {
		t.Fatal(err)
	}
	go r.ListenAndServe()

	// Wait for objects to age past the timeout before connecting the
	// subscriber, so that cache replay delivers aged objects.
	time.Sleep(timeout + 200*time.Millisecond)

	sink := newCollectSink()
	sub := subscriber.NewSubscriber(subscriber.SubConfig{
		RelayAddr:      relayAddr,
		Namespace:      namespace,
		TrackName:      trackName,
		RecvLogPath:    fmt.Sprintf("%s/recv_log.csv", dir),
		DisplayLogPath: fmt.Sprintf("%s/display_log.csv", dir),
		Sink:           sink,
	}, logger)

	subDone := make(chan error, 1)
	go func() {
		subDone <- sub.Run(ctx)
	}()

	time.Sleep(2 * time.Second)

	got := sink.Len()
	t.Logf("received %d/6 objects (strategy=%s, timeout=%v)", got, strategy, timeout)

	cancel()
	r.Shutdown()
	<-subDone

	return got
}
