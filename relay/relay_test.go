package relay_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"sort"
	"testing"
	"time"

	"moq-relay/relay"

	"github.com/mengelbart/moqtransport"
	"github.com/mengelbart/moqtransport/quicmoq"
	"github.com/quic-go/quic-go"
)

func generateTestTLS(t *testing.T) *tls.Config {
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

func TestRelayEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	tlsConfig := generateTestTLS(t)
	pubAddr := freePort(t)
	relayAddr := freePort(t)
	namespace := []string{"test"}
	trackName := "video"

	pubListener, err := quic.ListenAddr(pubAddr, tlsConfig, &quic.Config{EnableDatagrams: true})
	if err != nil {
		t.Fatal(err)
	}

	startPub := make(chan struct{})
	pubConnCh := make(chan *quic.Conn, 1)

	go func() {
		conn, err := pubListener.Accept(ctx)
		if err != nil {
			return
		}
		pubConnCh <- conn
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
						case <-startPub:
						case <-ctx.Done():
							return
						}
						for g := uint64(0); g < 2; g++ {
							sg, err := w.OpenSubgroup(g, 0, 0)
							if err != nil {
								return
							}
							for o := uint64(0); o < 3; o++ {
								payload := []byte(fmt.Sprintf("g%d-o%d", g, o))
								sg.WriteObject(o, payload)
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

	r := relay.New(relay.Config{
		ListenAddr:   relayAddr,
		UpstreamAddr: pubAddr,
		Namespace:    namespace,
		TrackName:    trackName,
		TLSConfig:    tlsConfig,
	}, slog.Default())

	if err := r.ConnectUpstream(); err != nil {
		t.Fatal(err)
	}
	go r.ListenAndServe()

	time.Sleep(100 * time.Millisecond)

	subConn, err := quic.DialAddr(ctx, relayAddr, &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"moq-00"},
	}, &quic.Config{EnableDatagrams: true})
	if err != nil {
		t.Fatal(err)
	}

	subSession := &moqtransport.Session{
		Handler: moqtransport.HandlerFunc(func(w moqtransport.ResponseWriter, m *moqtransport.Message) {
			if m.Method == moqtransport.MessageAnnounce {
				w.Accept()
			}
		}),
		InitialMaxRequestID: 100,
	}
	if err := subSession.Run(quicmoq.NewClient(subConn)); err != nil {
		t.Fatal(err)
	}

	rt, err := subSession.Subscribe(ctx, namespace, trackName)
	if err != nil {
		t.Fatal(err)
	}

	close(startPub)

	var received []string
	for i := 0; i < 6; i++ {
		obj, err := rt.ReadObject(ctx)
		if err != nil {
			t.Fatalf("ReadObject %d: %v", i, err)
		}
		received = append(received, string(obj.Payload))
	}

	if len(received) != 6 {
		t.Fatalf("expected 6 objects, got %d", len(received))
	}

	// QUIC streams (subgroups) may arrive out of order, so sort and compare
	sort.Strings(received)
	expected := []string{"g0-o0", "g0-o1", "g0-o2", "g1-o0", "g1-o1", "g1-o2"}
	for i, want := range expected {
		if received[i] != want {
			t.Errorf("object[%d] (sorted): got %q, want %q", i, received[i], want)
		}
	}

	subConn.CloseWithError(0, "test done")
	if pubConn := <-pubConnCh; pubConn != nil {
		pubConn.CloseWithError(0, "test done")
	}
	pubListener.Close()
	cancel()
	r.Shutdown()
}
