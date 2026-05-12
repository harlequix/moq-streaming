package main_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"moq-relay/publisher"
	"moq-relay/relay"
	"moq-relay/subscriber"
)

func generateTLS(t *testing.T) *tls.Config {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"moq-00"},
	}
}

func pickPort(t *testing.T) string {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	port := conn.LocalAddr().(*net.UDPAddr).Port
	conn.Close()
	return fmt.Sprintf("127.0.0.1:%d", port)
}

type collectSink struct {
	mu      sync.Mutex
	data    [][]byte
	frameCh chan subscriber.MpvFrame
}

func newCollectSink() *collectSink {
	return &collectSink{frameCh: make(chan subscriber.MpvFrame, 256)}
}

func (s *collectSink) Start() error { return nil }

func (s *collectSink) Write(data []byte) (int, error) {
	cp := make([]byte, len(data))
	copy(cp, data)
	s.mu.Lock()
	s.data = append(s.data, cp)
	n := len(s.data)
	s.mu.Unlock()
	s.frameCh <- subscriber.MpvFrame{PTS: float64(n) * 0.033}
	return len(data), nil
}

func (s *collectSink) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.data)
}

func (s *collectSink) Frames() <-chan subscriber.MpvFrame { return s.frameCh }

func (s *collectSink) Close() error {
	close(s.frameCh)
	return nil
}

// mockFrameSource replays predetermined frames; if ready is non-nil, the first
// NextFrame blocks until it is closed.
type mockFrameSource struct {
	frames []*publisher.Frame
	idx    int
	ready  chan struct{}
}

func (m *mockFrameSource) NextFrame() (*publisher.Frame, error) {
	if m.ready != nil && m.idx == 0 {
		<-m.ready
	}
	if m.idx >= len(m.frames) {
		return nil, io.EOF
	}
	f := m.frames[m.idx]
	m.idx++
	return f, nil
}

func TestEndToEnd_PublisherRelaySubscriber(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tlsConf := generateTLS(t)
	pubAddr := pickPort(t)
	relayAddr := pickPort(t)
	namespace := []string{"test"}
	trackName := "video"
	dir := t.TempDir()

	frames := []*publisher.Frame{
		{NALType: 5, IsIDR: true, Data: []byte{0, 0, 0, 1, 5, 0x10}, FrameNum: 0},
		{NALType: 1, IsIDR: false, Data: []byte{0, 0, 0, 1, 1, 0x11}, FrameNum: 1},
		{NALType: 1, IsIDR: false, Data: []byte{0, 0, 0, 1, 1, 0x12}, FrameNum: 2},
		{NALType: 5, IsIDR: true, Data: []byte{0, 0, 0, 1, 5, 0x20}, FrameNum: 3},
		{NALType: 1, IsIDR: false, Data: []byte{0, 0, 0, 1, 1, 0x21}, FrameNum: 4},
		{NALType: 1, IsIDR: false, Data: []byte{0, 0, 0, 1, 1, 0x22}, FrameNum: 5},
	}

	// Publisher blocks on ready channel so data isn't sent before subscriber connects.
	startPub := make(chan struct{})

	pub := publisher.NewPublisher(publisher.Config{
		ListenAddr:   pubAddr,
		Namespace:    namespace,
		TrackName:    trackName,
		Realtime:     false,
		ManifestPath: fmt.Sprintf("%s/manifest.csv", dir),
		TLSConfig:    tlsConf,
		Source:       &mockFrameSource{frames: frames, ready: startPub},
		FrameRate:    30.0,
	}, slog.Default())

	go pub.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	r := relay.New(relay.Config{
		ListenAddr:   relayAddr,
		UpstreamAddr: pubAddr,
		Namespace:    namespace,
		TrackName:    trackName,
		TLSConfig:    tlsConf,
	}, slog.Default())

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
	}, slog.Default())

	subDone := make(chan error, 1)
	go func() {
		subDone <- sub.Run(ctx)
	}()

	time.Sleep(200 * time.Millisecond)
	close(startPub)

	deadline := time.After(10 * time.Second)
	for sink.Len() < 6 {
		select {
		case <-deadline:
			t.Fatalf("timeout: got %d objects, want 6", sink.Len())
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	if sink.Len() != 6 {
		t.Errorf("got %d objects, want 6", sink.Len())
	}

	cancel()
	r.Shutdown()
	<-subDone
}
