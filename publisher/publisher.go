package publisher

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/mengelbart/moqtransport"
	"github.com/mengelbart/moqtransport/quicmoq"
	"github.com/quic-go/quic-go"
)

// Config holds publisher configuration.
type Config struct {
	ListenAddr   string
	Namespace    []string
	TrackName    string
	Realtime     bool
	ManifestPath string
	TLSConfig    *tls.Config

	// Source provides frames. If nil, VideoPath is used to create a Demuxer+NALParser.
	Source    FrameSource
	FrameRate float64 // frames per second (used for PTS calculation)
	VideoPath string  // used when Source is nil
	EmbedPTS  bool    // prepend 8-byte PTS (float64 seconds, big-endian) to each object payload
}

// Publisher serves video frames as MoQ objects.
type Publisher struct {
	cfg      Config
	logger   *slog.Logger
	done     chan struct{} // closed when publishLoop finishes (video EOF)
	doneOnce sync.Once
}

func NewPublisher(cfg Config, logger *slog.Logger) *Publisher {
	return &Publisher{cfg: cfg, logger: logger, done: make(chan struct{})}
}

// Run blocks until ctx is cancelled or an error occurs.
func (p *Publisher) Run(ctx context.Context) error {
	if p.cfg.Source != nil {
		return p.serve(ctx, p.cfg.Source, p.cfg.FrameRate)
	}
	// Demuxer is started lazily inside the subscribe handler so that in
	// bulk mode we don't produce frames before a subscriber is connected.
	return p.serve(ctx, nil, 0)
}

func (p *Publisher) serve(ctx context.Context, src FrameSource, frameRate float64) error {
	listener, err := quic.ListenAddr(p.cfg.ListenAddr, p.cfg.TLSConfig, &quic.Config{
		EnableDatagrams: true,
	})
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	p.logger.Info("publisher listening", "addr", p.cfg.ListenAddr)

	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	conn, err := listener.Accept(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("accept: %w", err)
	}
	p.logger.Info("relay connected", "remote", conn.RemoteAddr())

	session := &moqtransport.Session{
		Handler: moqtransport.HandlerFunc(func(w moqtransport.ResponseWriter, m *moqtransport.Message) {
			if m.Method == moqtransport.MessageAnnounce {
				w.Accept()
			}
		}),
		SubscribeHandler:    p.makeSubscribeHandler(ctx, src, frameRate),
		InitialMaxRequestID: 100,
	}
	if err := session.Run(quicmoq.NewServer(conn)); err != nil {
		return fmt.Errorf("session handshake: %w", err)
	}
	p.logger.Info("session established")

	announceCtx, announceCancel := context.WithTimeout(ctx, 5*time.Second)
	defer announceCancel()
	if err := session.Announce(announceCtx, p.cfg.Namespace); err != nil {
		return fmt.Errorf("announce: %w", err)
	}
	p.logger.Info("announced", "namespace", p.cfg.Namespace)

	select {
	case <-p.done:
		p.logger.Info("video stream ended, draining")
		// Let QUIC flush the last subgroup's stream data before closing.
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
		case <-conn.Context().Done():
		}
	case <-ctx.Done():
	case <-conn.Context().Done():
	}
	conn.CloseWithError(0, "publisher done")
	return nil
}

func (p *Publisher) makeSubscribeHandler(
	ctx context.Context,
	src FrameSource,
	frameRate float64,
) moqtransport.SubscribeHandler {
	return moqtransport.SubscribeHandlerFunc(
		func(w *moqtransport.SubscribeResponseWriter, m *moqtransport.SubscribeMessage) {
			if err := w.Accept(); err != nil {
				p.logger.Error("failed to accept subscription", "err", err)
				return
			}
			p.logger.Info("subscription accepted",
				"namespace", m.Namespace, "track", m.Track)

			fsrc, fr := src, frameRate
			if fsrc == nil {
				demux, err := NewDemuxer(p.cfg.VideoPath, p.cfg.Realtime)
				if err != nil {
					p.logger.Error("failed to create demuxer", "err", err)
					return
				}
				r, err := demux.Start(ctx)
				if err != nil {
					demux.Close()
					p.logger.Error("failed to start demuxer", "err", err)
					return
				}
				parser, err := NewNALParser(r)
				if err != nil {
					demux.Close()
					p.logger.Error("failed to create NAL parser", "err", err)
					return
				}
				fsrc = parser
				fr = demux.FrameRate()
				go func() {
					p.publishLoop(ctx, w, fsrc, fr)
					demux.Close()
				}()
				return
			}

			go p.publishLoop(ctx, w, fsrc, fr)
		})
}

func (p *Publisher) publishLoop(
	ctx context.Context,
	pub moqtransport.Publisher,
	src FrameSource,
	frameRate float64,
) {
	defer p.doneOnce.Do(func() { close(p.done) })

	var csvW *csv.Writer
	var csvFile *os.File
	if p.cfg.ManifestPath != "" {
		var err error
		csvFile, err = os.Create(p.cfg.ManifestPath)
		if err != nil {
			p.logger.Error("failed to create manifest", "err", err)
			return
		}
		defer csvFile.Close()
		csvW = csv.NewWriter(csvFile)
		defer csvW.Flush()
		csvW.Write([]string{"frame_num", "group_id", "object_id", "pts_ms", "publish_ts_ns", "frame_type", "payload_bytes"})
	}

	var groupID uint64
	var objectID uint64
	var sg *moqtransport.Subgroup
	first := true

	for {
		select {
		case <-ctx.Done():
			if sg != nil {
				sg.Close()
			}
			return
		default:
		}

		// With ffmpeg -re, NextFrame naturally paces at the video's framerate.
		frame, err := src.NextFrame()
		if err != nil {
			p.logger.Info("frame source ended", "err", err)
			if sg != nil {
				sg.Close()
			}
			return
		}

		if frame.IsIDR {
			if sg != nil {
				sg.Close()
			}
			if !first {
				groupID++
			}
			first = false
			objectID = 0
			var err error
			sg, err = pub.OpenSubgroup(groupID, 0, 0)
			if err != nil {
				p.logger.Error("failed to open subgroup", "err", err, "group", groupID)
				return
			}
		}

		if sg == nil {
			p.logger.Warn("skipping frame before first IDR", "frame", frame.FrameNum)
			continue
		}

		publishTS := time.Now().UnixNano()
		payload := frame.Data
		if p.cfg.EmbedPTS {
			ptsS := float64(frame.FrameNum) / frameRate
			ptsBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(ptsBuf, math.Float64bits(ptsS))
			payload = append(ptsBuf, frame.Data...)
		}
		if _, err := sg.WriteObject(objectID, payload); err != nil {
			p.logger.Error("failed to write object", "err", err,
				"group", groupID, "object", objectID)
			return
		}
		p.logger.Debug("published",
			"group", groupID, "object", objectID,
			"type", frame.NALType, "size", len(frame.Data))

		if csvW != nil {
			ptsMs := float64(frame.FrameNum) / frameRate * 1000.0
			frameType := "P"
			if frame.IsIDR {
				frameType = "I"
			}
			csvW.Write([]string{
				strconv.FormatUint(frame.FrameNum, 10),
				strconv.FormatUint(groupID, 10),
				strconv.FormatUint(objectID, 10),
				strconv.FormatFloat(ptsMs, 'f', 3, 64),
				strconv.FormatInt(publishTS, 10),
				frameType,
				strconv.Itoa(len(frame.Data)),
			})
			csvW.Flush()
		}

		objectID++
	}
}
