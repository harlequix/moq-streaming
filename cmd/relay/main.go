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
	"time"

	"moq-relay/relay"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		listen         string
		upstream       string
		namespace      string
		track          string
		certFile       string
		keyFile        string
		strategyStr    string
		defaultTimeout time.Duration
		metricsPath    string
		qlogDir        string
		debug          bool
	)

	flag.StringVar(&listen, "listen", ":4443", "address for downstream subscribers")
	flag.StringVar(&upstream, "upstream", "", "upstream publisher address (required)")
	flag.StringVar(&namespace, "namespace", "", "MoQ namespace (required)")
	flag.StringVar(&track, "track", "", "MoQ track name (required)")
	flag.StringVar(&certFile, "cert", "", "TLS certificate file (auto-generated if empty)")
	flag.StringVar(&keyFile, "key", "", "TLS private key file (auto-generated if empty)")
	flag.StringVar(&strategyStr, "strategy", "none", "timeout strategy: none, reset-stream, reset-at-object, reset-at-keyframe")
	flag.DurationVar(&defaultTimeout, "default-timeout", 0, "default delivery timeout (e.g. 200ms, 2s)")
	flag.StringVar(&metricsPath, "metrics", "", "path to write relay metrics CSV on shutdown")
	flag.StringVar(&qlogDir, "qlog-dir", "", "directory for qlog trace files (disabled if empty)")
	flag.BoolVar(&debug, "debug", false, "enable debug logging")
	flag.Parse()

	if upstream == "" || namespace == "" || track == "" {
		flag.Usage()
		return fmt.Errorf("--upstream, --namespace, and --track are required")
	}

	strategy, err := relay.ParseStrategy(strategyStr)
	if err != nil {
		return err
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

	cfg := relay.Config{
		ListenAddr:     listen,
		UpstreamAddr:   upstream,
		Namespace:      strings.Split(namespace, "/"),
		TrackName:      track,
		TLSConfig:      tlsConfig,
		Strategy:       strategy,
		DefaultTimeout: defaultTimeout,
		MetricsPath:    metricsPath,
		QlogDir:        qlogDir,
	}

	r := relay.New(cfg, logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownDone := make(chan struct{})
	go func() {
		<-ctx.Done()
		logger.Info("shutting down")
		r.Shutdown()
		close(shutdownDone)
	}()

	err = r.ListenAndServe()
	<-shutdownDone
	return err
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
