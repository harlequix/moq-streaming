package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"moq-relay/publisher"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		listen    string
		video     string
		namespace string
		track     string
		manifest  string
		realtime  bool
		embedPTS  bool
		certFile  string
		keyFile   string
		debug     bool
	)

	flag.StringVar(&listen, "listen", ":4443", "QUIC listener address")
	flag.StringVar(&video, "video", "", "path to video file (required)")
	flag.StringVar(&namespace, "namespace", "", "MoQ namespace (required)")
	flag.StringVar(&track, "track", "", "MoQ track name (required)")
	flag.StringVar(&manifest, "manifest", "manifest.csv", "manifest CSV output path")
	flag.BoolVar(&realtime, "realtime", true, "pace output at video framerate")
	flag.BoolVar(&embedPTS, "embed-pts", false, "prepend 8-byte PTS header to each object payload")
	flag.StringVar(&certFile, "cert", "", "TLS certificate file (auto-generated if empty)")
	flag.StringVar(&keyFile, "key", "", "TLS private key file (auto-generated if empty)")
	flag.BoolVar(&debug, "debug", false, "enable debug logging")
	flag.Parse()

	if video == "" || namespace == "" || track == "" {
		flag.Usage()
		return fmt.Errorf("--video, --namespace, and --track are required")
	}

	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	tlsConfig, err := loadOrGenerateTLS(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("TLS setup: %w", err)
	}

	cfg := publisher.Config{
		ListenAddr:   listen,
		Namespace:    strings.Split(namespace, "/"),
		TrackName:    track,
		Realtime:     realtime,
		EmbedPTS:     embedPTS,
		ManifestPath: manifest,
		TLSConfig:    tlsConfig,
		VideoPath:    video,
	}

	pub := publisher.NewPublisher(cfg, logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	return pub.Run(ctx)
}

func loadOrGenerateTLS(certFile, keyFile string) (*tls.Config, error) {
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		return &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"moq-00"},
		}, nil
	}

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{"moq-00"},
	}, nil
}
