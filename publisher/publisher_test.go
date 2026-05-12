package publisher

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/csv"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/mengelbart/moqtransport"
	"github.com/mengelbart/moqtransport/quicmoq"
	"github.com/quic-go/quic-go"
)

func testTLS(t *testing.T) *tls.Config {
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

func freePort(t *testing.T) string {
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

type mockSource struct {
	frames []*Frame
	idx    int
}

func (m *mockSource) NextFrame() (*Frame, error) {
	if m.idx >= len(m.frames) {
		return nil, io.EOF
	}
	f := m.frames[m.idx]
	m.idx++
	return f, nil
}

func TestPublisher_GoPMapping(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addr := freePort(t)
	dir := t.TempDir()
	manifest := filepath.Join(dir, "manifest.csv")
	namespace := []string{"test"}
	trackName := "video"

	src := &mockSource{frames: []*Frame{
		{NALType: 5, IsIDR: true, Data: []byte{0, 0, 0, 1, 5, 0xAA}, FrameNum: 0},
		{NALType: 1, IsIDR: false, Data: []byte{0, 0, 0, 1, 1, 0xBB}, FrameNum: 1},
		{NALType: 5, IsIDR: true, Data: []byte{0, 0, 0, 1, 5, 0xCC}, FrameNum: 2},
		{NALType: 1, IsIDR: false, Data: []byte{0, 0, 0, 1, 1, 0xDD}, FrameNum: 3},
	}}

	pub := NewPublisher(Config{
		ListenAddr:   addr,
		Namespace:    namespace,
		TrackName:    trackName,
		Realtime:     false,
		ManifestPath: manifest,
		TLSConfig:    testTLS(t),
		Source:       src,
		FrameRate:    30.0,
	}, slog.Default())

	pubErr := make(chan error, 1)
	go func() {
		pubErr <- pub.Run(ctx)
	}()
	time.Sleep(100 * time.Millisecond)

	conn, err := quic.DialAddr(ctx, addr, &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"moq-00"},
	}, &quic.Config{EnableDatagrams: true})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseWithError(0, "test done")

	session := &moqtransport.Session{
		Handler: moqtransport.HandlerFunc(func(w moqtransport.ResponseWriter, m *moqtransport.Message) {
			if m.Method == moqtransport.MessageAnnounce {
				w.Accept()
			}
		}),
		InitialMaxRequestID: 100,
	}
	if err := session.Run(quicmoq.NewClient(conn)); err != nil {
		t.Fatal(err)
	}

	rt, err := session.Subscribe(ctx, namespace, trackName)
	if err != nil {
		t.Fatal(err)
	}

	type obj struct {
		GroupID  uint64
		ObjectID uint64
		Payload  []byte
	}
	var received []obj
	for i := 0; i < 4; i++ {
		o, err := rt.ReadObject(ctx)
		if err != nil {
			t.Fatalf("ReadObject %d: %v", i, err)
		}
		received = append(received, obj{o.GroupID, o.ObjectID, o.Payload})
	}

	// QUIC subgroups may arrive out of order; sort by (group, object).
	sort.Slice(received, func(i, j int) bool {
		if received[i].GroupID != received[j].GroupID {
			return received[i].GroupID < received[j].GroupID
		}
		return received[i].ObjectID < received[j].ObjectID
	})

	expected := []obj{
		{0, 0, []byte{0, 0, 0, 1, 5, 0xAA}},
		{0, 1, []byte{0, 0, 0, 1, 1, 0xBB}},
		{1, 0, []byte{0, 0, 0, 1, 5, 0xCC}},
		{1, 1, []byte{0, 0, 0, 1, 1, 0xDD}},
	}
	for i, want := range expected {
		got := received[i]
		if got.GroupID != want.GroupID || got.ObjectID != want.ObjectID {
			t.Errorf("object %d: got group=%d obj=%d, want group=%d obj=%d",
				i, got.GroupID, got.ObjectID, want.GroupID, want.ObjectID)
		}
		if string(got.Payload) != string(want.Payload) {
			t.Errorf("object %d payload mismatch", i)
		}
	}

	cancel()
	time.Sleep(100 * time.Millisecond)

	f, err := os.Open(manifest)
	if err != nil {
		t.Fatalf("open manifest: %v", err)
	}
	defer f.Close()
	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	// Header + 4 data rows = 5
	if len(records) != 5 {
		t.Errorf("manifest rows: got %d, want 5 (1 header + 4 data)", len(records))
	}
}

func TestPublisher_EmbedPTS(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addr := freePort(t)
	namespace := []string{"test"}
	trackName := "video"

	src := &mockSource{frames: []*Frame{
		{NALType: 5, IsIDR: true, Data: []byte{0, 0, 0, 1, 5, 0xAA}, FrameNum: 0},
		{NALType: 1, IsIDR: false, Data: []byte{0, 0, 0, 1, 1, 0xBB}, FrameNum: 1},
		{NALType: 1, IsIDR: false, Data: []byte{0, 0, 0, 1, 1, 0xCC}, FrameNum: 2},
	}}

	pub := NewPublisher(Config{
		ListenAddr: addr,
		Namespace:  namespace,
		TrackName:  trackName,
		Realtime:   false,
		TLSConfig:  testTLS(t),
		Source:     src,
		FrameRate:  30.0,
		EmbedPTS:   true,
	}, slog.Default())

	go pub.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	conn, err := quic.DialAddr(ctx, addr, &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"moq-00"},
	}, &quic.Config{EnableDatagrams: true})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseWithError(0, "test done")

	session := &moqtransport.Session{
		Handler: moqtransport.HandlerFunc(func(w moqtransport.ResponseWriter, m *moqtransport.Message) {
			if m.Method == moqtransport.MessageAnnounce {
				w.Accept()
			}
		}),
		InitialMaxRequestID: 100,
	}
	if err := session.Run(quicmoq.NewClient(conn)); err != nil {
		t.Fatal(err)
	}

	rt, err := session.Subscribe(ctx, namespace, trackName)
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		pts     float64
		payload []byte
	}

	var results []result
	for i := 0; i < 3; i++ {
		o, err := rt.ReadObject(ctx)
		if err != nil {
			t.Fatalf("ReadObject %d: %v", i, err)
		}
		if len(o.Payload) < 8 {
			t.Fatalf("object %d: payload too short (%d bytes) for PTS header", i, len(o.Payload))
		}
		pts := math.Float64frombits(binary.BigEndian.Uint64(o.Payload[:8]))
		videoData := o.Payload[8:]
		results = append(results, result{pts: pts, payload: videoData})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].pts < results[j].pts
	})

	expectedPTS := []float64{0.0 / 30.0, 1.0 / 30.0, 2.0 / 30.0}
	for i, want := range expectedPTS {
		if math.Abs(results[i].pts-want) > 1e-9 {
			t.Errorf("frame %d PTS: got %f, want %f", i, results[i].pts, want)
		}
	}

	expectedPayloads := [][]byte{
		{0, 0, 0, 1, 5, 0xAA},
		{0, 0, 0, 1, 1, 0xBB},
		{0, 0, 0, 1, 1, 0xCC},
	}
	for i, want := range expectedPayloads {
		if string(results[i].payload) != string(want) {
			t.Errorf("frame %d payload mismatch: got %v, want %v", i, results[i].payload, want)
		}
	}

	cancel()
	time.Sleep(100 * time.Millisecond)
}
