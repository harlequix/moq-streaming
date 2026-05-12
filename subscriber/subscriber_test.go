package subscriber

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
	"log/slog"
	"math"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
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

type mockSink struct {
	mu      sync.Mutex
	data    [][]byte
	frameCh chan MpvFrame
}

func newMockSink() *mockSink {
	return &mockSink{frameCh: make(chan MpvFrame, 256)}
}

func (m *mockSink) Start() error { return nil }

func (m *mockSink) Write(data []byte) (int, error) {
	cp := make([]byte, len(data))
	copy(cp, data)
	m.mu.Lock()
	m.data = append(m.data, cp)
	n := len(m.data)
	m.mu.Unlock()
	m.frameCh <- MpvFrame{PTS: float64(n) * 0.033, Frametime: 33.0}
	return len(data), nil
}

func (m *mockSink) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.data)
}

func (m *mockSink) DataAt(i int) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data[i]
}

func (m *mockSink) Frames() <-chan MpvFrame {
	return m.frameCh
}

func (m *mockSink) Close() error {
	close(m.frameCh)
	return nil
}

func TestSubscriber_ReceivesObjects(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pubAddr := freePort(t)
	namespace := []string{"test"}
	trackName := "video"
	tlsConf := testTLS(t)
	dir := t.TempDir()
	recvLog := filepath.Join(dir, "recv_log.csv")
	displayLog := filepath.Join(dir, "display_log.csv")

	startSub := make(chan struct{})
	pubListener, err := quic.ListenAddr(pubAddr, tlsConf, &quic.Config{EnableDatagrams: true})
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		conn, err := pubListener.Accept(ctx)
		if err != nil {
			return
		}
		session := &moqtransport.Session{
			Handler: moqtransport.HandlerFunc(func(w moqtransport.ResponseWriter, m *moqtransport.Message) {
				if m.Method == moqtransport.MessageAnnounce {
					w.Accept()
				}
			}),
			SubscribeHandler: moqtransport.SubscribeHandlerFunc(
				func(w *moqtransport.SubscribeResponseWriter, m *moqtransport.SubscribeMessage) {
					w.Accept()
					go func() {
						select {
						case <-startSub:
						case <-ctx.Done():
							return
						}
						for g := uint64(0); g < 2; g++ {
							sg, err := w.OpenSubgroup(g, 0, 0)
							if err != nil {
								return
							}
							for o := uint64(0); o < 2; o++ {
								sg.WriteObject(o, []byte(fmt.Sprintf("g%d-o%d", g, o)))
							}
							sg.Close()
						}
					}()
				}),
			InitialMaxRequestID: 100,
		}
		session.Run(quicmoq.NewServer(conn))
		session.Announce(ctx, namespace)
		<-ctx.Done()
	}()

	time.Sleep(100 * time.Millisecond)

	sink := newMockSink()
	sub := NewSubscriber(SubConfig{
		RelayAddr:      pubAddr,
		Namespace:      namespace,
		TrackName:      trackName,
		RecvLogPath:    recvLog,
		DisplayLogPath: displayLog,
		Sink:           sink,
	}, slog.Default())

	close(startSub)

	subDone := make(chan error, 1)
	go func() {
		subDone <- sub.Run(ctx)
	}()

	deadline := time.After(5 * time.Second)
	for sink.Len() < 4 {
		select {
		case <-deadline:
			t.Fatalf("timeout: sink received %d writes, want 4", sink.Len())
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	cancel()
	<-subDone

	f, err := os.Open(recvLog)
	if err != nil {
		t.Fatalf("open recv_log: %v", err)
	}
	defer f.Close()
	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("read recv_log: %v", err)
	}
	// Header + 4 data rows = 5
	if len(records) < 5 {
		t.Errorf("recv_log rows: got %d, want >= 5", len(records))
	}

	df, err := os.Open(displayLog)
	if err != nil {
		t.Fatalf("open display_log: %v", err)
	}
	defer df.Close()
	dRecords, err := csv.NewReader(df).ReadAll()
	if err != nil {
		t.Fatalf("read display_log: %v", err)
	}
	// Header + 4 display events
	if len(dRecords) < 5 {
		t.Errorf("display_log rows: got %d, want >= 5", len(dRecords))
	}

	// QUIC subgroups may arrive out of order, so sort before comparing.
	var payloads []string
	for i := 0; i < sink.Len(); i++ {
		payloads = append(payloads, string(sink.DataAt(i)))
	}
	sort.Strings(payloads)
	expected := []string{"g0-o0", "g0-o1", "g1-o0", "g1-o1"}
	sort.Strings(expected)
	for i, want := range expected {
		if i >= len(payloads) {
			break
		}
		if payloads[i] != want {
			t.Errorf("payload[%d] (sorted): got %q, want %q", i, payloads[i], want)
		}
	}
}

// makePTSPayload prepends an 8-byte PTS header (float64 seconds) to data.
func makePTSPayload(ptsS float64, data []byte) []byte {
	buf := make([]byte, 8+len(data))
	binary.BigEndian.PutUint64(buf[:8], math.Float64bits(ptsS))
	copy(buf[8:], data)
	return buf
}

func TestSubscriber_EmbeddedPTS(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pubAddr := freePort(t)
	namespace := []string{"test"}
	trackName := "video"
	tlsConf := testTLS(t)
	dir := t.TempDir()
	recvLog := filepath.Join(dir, "recv_log.csv")
	displayLog := filepath.Join(dir, "display_log.csv")

	startSub := make(chan struct{})
	pubListener, err := quic.ListenAddr(pubAddr, tlsConf, &quic.Config{EnableDatagrams: true})
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		conn, err := pubListener.Accept(ctx)
		if err != nil {
			return
		}
		session := &moqtransport.Session{
			Handler: moqtransport.HandlerFunc(func(w moqtransport.ResponseWriter, m *moqtransport.Message) {
				if m.Method == moqtransport.MessageAnnounce {
					w.Accept()
				}
			}),
			SubscribeHandler: moqtransport.SubscribeHandlerFunc(
				func(w *moqtransport.SubscribeResponseWriter, m *moqtransport.SubscribeMessage) {
					w.Accept()
					go func() {
						select {
						case <-startSub:
						case <-ctx.Done():
							return
						}
						fps := 30.0
						frameNum := 0
						for g := uint64(0); g < 2; g++ {
							sg, err := w.OpenSubgroup(g, 0, 0)
							if err != nil {
								return
							}
							for o := uint64(0); o < 2; o++ {
								pts := float64(frameNum) / fps
								payload := makePTSPayload(pts, []byte(fmt.Sprintf("g%d-o%d", g, o)))
								sg.WriteObject(o, payload)
								frameNum++
							}
							sg.Close()
						}
					}()
				}),
			InitialMaxRequestID: 100,
		}
		session.Run(quicmoq.NewServer(conn))
		session.Announce(ctx, namespace)
		<-ctx.Done()
	}()

	time.Sleep(100 * time.Millisecond)

	sink := newMockSink()
	sub := NewSubscriber(SubConfig{
		RelayAddr:      pubAddr,
		Namespace:      namespace,
		TrackName:      trackName,
		RecvLogPath:    recvLog,
		DisplayLogPath: displayLog,
		Sink:           sink,
		EmbeddedPTS:    true,
	}, slog.Default())

	close(startSub)

	subDone := make(chan error, 1)
	go func() {
		subDone <- sub.Run(ctx)
	}()

	// Wait for at least 2 objects (QUIC subgroup delivery can be partial in tests)
	deadline := time.After(5 * time.Second)
	for sink.Len() < 2 {
		select {
		case <-deadline:
			t.Fatalf("timeout: sink received %d writes, want >= 2", sink.Len())
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
	time.Sleep(200 * time.Millisecond)

	cancel()
	<-subDone

	n := sink.Len()
	t.Logf("received %d/4 objects", n)

	for i := 0; i < n; i++ {
		data := string(sink.DataAt(i))
		if len(data) < 4 || data[0] != 'g' {
			t.Errorf("sink payload[%d]: got %q, PTS header not stripped?", i, data)
		}
	}

	f, err := os.Open(recvLog)
	if err != nil {
		t.Fatalf("open recv_log: %v", err)
	}
	defer f.Close()
	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("read recv_log: %v", err)
	}

	if len(records) < 1 {
		t.Fatal("recv_log empty")
	}
	header := records[0]
	if header[len(header)-1] != "source_pts_s" {
		t.Errorf("recv_log header: last column is %q, want source_pts_s", header[len(header)-1])
	}

	if len(records) < 3 {
		t.Fatalf("recv_log rows: got %d, want >= 3", len(records))
	}
	for _, row := range records[1:] {
		ptsStr := row[len(row)-1]
		pts, err := strconv.ParseFloat(ptsStr, 64)
		if err != nil {
			t.Errorf("recv_log PTS parse error: %v (value=%q)", err, ptsStr)
			continue
		}
		if pts < 0 {
			t.Errorf("recv_log PTS is negative: %f", pts)
		}
	}

	df, err := os.Open(displayLog)
	if err != nil {
		t.Fatalf("open display_log: %v", err)
	}
	defer df.Close()
	dRecords, err := csv.NewReader(df).ReadAll()
	if err != nil {
		t.Fatalf("read display_log: %v", err)
	}

	if len(dRecords) < 1 {
		t.Fatal("display_log empty")
	}
	dHeader := dRecords[0]
	expectedHeader := []string{"display_seq", "group_id", "object_id", "source_pts_s", "mpv_pts", "frametime_ms"}
	if len(dHeader) != len(expectedHeader) {
		t.Fatalf("display_log header: got %v, want %v", dHeader, expectedHeader)
	}
	for i, want := range expectedHeader {
		if dHeader[i] != want {
			t.Errorf("display_log header[%d]: got %q, want %q", i, dHeader[i], want)
		}
	}

	if len(dRecords) < 3 {
		t.Fatalf("display_log rows: got %d, want >= 3", len(dRecords))
	}
	for _, row := range dRecords[1:] {
		pts, err := strconv.ParseFloat(row[3], 64) // source_pts_s column
		if err != nil {
			t.Errorf("display_log source_pts_s parse: %v", err)
		}
		if pts < 0 {
			t.Errorf("display_log source_pts_s negative: %f", pts)
		}
	}
}

func TestSubscriber_NoEmbeddedPTS_OriginalFormat(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pubAddr := freePort(t)
	namespace := []string{"test"}
	trackName := "video"
	tlsConf := testTLS(t)
	dir := t.TempDir()
	recvLog := filepath.Join(dir, "recv_log.csv")
	displayLog := filepath.Join(dir, "display_log.csv")

	startSub := make(chan struct{})
	pubListener, err := quic.ListenAddr(pubAddr, tlsConf, &quic.Config{EnableDatagrams: true})
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		conn, err := pubListener.Accept(ctx)
		if err != nil {
			return
		}
		session := &moqtransport.Session{
			Handler: moqtransport.HandlerFunc(func(w moqtransport.ResponseWriter, m *moqtransport.Message) {
				if m.Method == moqtransport.MessageAnnounce {
					w.Accept()
				}
			}),
			SubscribeHandler: moqtransport.SubscribeHandlerFunc(
				func(w *moqtransport.SubscribeResponseWriter, m *moqtransport.SubscribeMessage) {
					w.Accept()
					go func() {
						select {
						case <-startSub:
						case <-ctx.Done():
							return
						}
						sg, _ := w.OpenSubgroup(0, 0, 0)
						sg.WriteObject(0, []byte("raw-data"))
						sg.Close()
					}()
				}),
			InitialMaxRequestID: 100,
		}
		session.Run(quicmoq.NewServer(conn))
		session.Announce(ctx, namespace)
		<-ctx.Done()
	}()

	time.Sleep(100 * time.Millisecond)

	sink := newMockSink()
	sub := NewSubscriber(SubConfig{
		RelayAddr:      pubAddr,
		Namespace:      namespace,
		TrackName:      trackName,
		RecvLogPath:    recvLog,
		DisplayLogPath: displayLog,
		Sink:           sink,
		EmbeddedPTS:    false, // explicitly off
	}, slog.Default())

	close(startSub)

	subDone := make(chan error, 1)
	go func() {
		subDone <- sub.Run(ctx)
	}()

	deadline := time.After(5 * time.Second)
	for sink.Len() < 1 {
		select {
		case <-deadline:
			t.Fatalf("timeout: sink received %d writes, want 1", sink.Len())
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	cancel()
	<-subDone

	f, err := os.Open(recvLog)
	if err != nil {
		t.Fatalf("open recv_log: %v", err)
	}
	defer f.Close()
	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("read recv_log: %v", err)
	}
	if len(records[0]) != 5 {
		t.Errorf("recv_log columns: got %d, want 5 (original format)", len(records[0]))
	}

	df, err := os.Open(displayLog)
	if err != nil {
		t.Fatalf("open display_log: %v", err)
	}
	defer df.Close()
	dRecords, err := csv.NewReader(df).ReadAll()
	if err != nil {
		t.Fatalf("read display_log: %v", err)
	}
	if len(dRecords[0]) != 3 {
		t.Errorf("display_log columns: got %d, want 3 (original format)", len(dRecords[0]))
	}
}
