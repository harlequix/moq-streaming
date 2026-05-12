package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"moq-relay/subscriber"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		relay       string
		namespace   string
		track       string
		recvLog     string
		displayLog  string
		luaScript   string
		nullSink    bool
		embeddedPTS bool
		fps         float64
		container   string
		debug       bool
	)

	flag.StringVar(&relay, "relay", "", "relay address to connect to (required)")
	flag.StringVar(&namespace, "namespace", "", "MoQ namespace (required)")
	flag.StringVar(&track, "track", "", "MoQ track name (required)")
	flag.StringVar(&recvLog, "recv-log", "recv_log.csv", "receive log CSV output path")
	flag.StringVar(&displayLog, "display-log", "display_log.csv", "display log CSV output path")
	flag.StringVar(&luaScript, "lua", "", "path to framelog.lua (default: bundled)")
	flag.BoolVar(&nullSink, "null-sink", false, "discard video data (no mpv)")
	flag.BoolVar(&embeddedPTS, "embedded-pts", false, "extract 8-byte PTS header from object payloads")
	flag.Float64Var(&fps, "fps", 0, "explicit framerate for mpv (0 = auto-detect)")
	flag.StringVar(&container, "container", "none", "container format for mpv: none (raw h264) or mpegts")
	flag.BoolVar(&debug, "debug", false, "enable debug logging")
	flag.Parse()

	if relay == "" || namespace == "" || track == "" {
		flag.Usage()
		return fmt.Errorf("--relay, --namespace, and --track are required")
	}

	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	// Lua script lookup: explicit flag > next to binary > next to source file.
	if luaScript == "" {
		exe, _ := os.Executable()
		candidate := filepath.Join(filepath.Dir(exe), "framelog.lua")
		if _, err := os.Stat(candidate); err == nil {
			luaScript = candidate
		}
	}
	if luaScript == "" {
		_, thisFile, _, _ := runtime.Caller(0)
		candidate := filepath.Join(filepath.Dir(thisFile), "..", "..", "subscriber", "framelog.lua")
		if _, err := os.Stat(candidate); err == nil {
			luaScript = candidate
		}
	}
	if luaScript == "" {
		return fmt.Errorf("cannot find framelog.lua; use --lua flag")
	}

	cfg := subscriber.SubConfig{
		RelayAddr:      relay,
		Namespace:      strings.Split(namespace, "/"),
		TrackName:      track,
		RecvLogPath:    recvLog,
		DisplayLogPath: displayLog,
		LuaScript:      luaScript,
		EmbeddedPTS:    embeddedPTS,
		FPS:            fps,
		Container:      container,
	}
	if nullSink {
		cfg.Sink = subscriber.NewNullSink()
	}

	sub := subscriber.NewSubscriber(cfg, logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	return sub.Run(ctx)
}
