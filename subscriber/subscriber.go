package subscriber

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

// SubConfig holds subscriber configuration.
type SubConfig struct {
	RelayAddr      string
	Namespace      []string
	TrackName      string
	RecvLogPath    string
	DisplayLogPath string

	// Sink receives video data. If nil, an MpvProcess is created.
	Sink      VideoSink
	LuaScript string // path to framelog.lua (used when Sink is nil)

	EmbeddedPTS bool    // extract 8-byte PTS header from object payloads
	FPS         float64 // explicit framerate for mpv (0 = auto-detect)
	Container   string  // "none" (default) or "mpegts"
	Untimed     bool    // mpv shows frames as soon as they are decoded
}

type sinkFrame struct {
	videoData []byte
	sourcePTS float64 // seconds, -1 if unavailable
	groupID   uint64
	objectID  uint64
}

type displayInfo struct {
	sourcePTS float64
	groupID   uint64
	objectID  uint64
}

// Subscriber receives MoQ objects and pipes them to a video sink.
type Subscriber struct {
	cfg    SubConfig
	logger *slog.Logger
}

func NewSubscriber(cfg SubConfig, logger *slog.Logger) *Subscriber {
	return &Subscriber{cfg: cfg, logger: logger}
}

// Run blocks until ctx is cancelled or an error occurs.
func (s *Subscriber) Run(ctx context.Context) error {
	sink := s.cfg.Sink
	if sink == nil {
		luaPath := s.cfg.LuaScript
		if luaPath == "" {
			return fmt.Errorf("lua script path required when no Sink provided")
		}
		mpv := NewMpvProcess(luaPath, s.cfg.FPS, s.cfg.Container, s.cfg.Untimed)
		sink = mpv
	}
	if err := sink.Start(); err != nil {
		return fmt.Errorf("start sink: %w", err)
	}
	var closeSinkOnce sync.Once
	closeSink := func() { closeSinkOnce.Do(func() { sink.Close() }) }
	defer closeSink()

	conn, err := quic.DialAddr(ctx, s.cfg.RelayAddr, &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"moq-00"},
	}, &quic.Config{
		EnableDatagrams:                  true,
		EnableStreamResetPartialDelivery: true,
	})
	if err != nil {
		return fmt.Errorf("dial relay: %w", err)
	}
	defer conn.CloseWithError(0, "subscriber done")
	s.logger.Info("connected to relay", "addr", s.cfg.RelayAddr)

	session := &moqtransport.Session{
		Handler: moqtransport.HandlerFunc(func(w moqtransport.ResponseWriter, m *moqtransport.Message) {
			if m.Method == moqtransport.MessageAnnounce {
				if err := w.Accept(); err != nil {
					s.logger.Error("failed to accept announcement", "err", err)
				}
				s.logger.Info("accepted announcement", "namespace", m.Namespace)
			}
		}),
		InitialMaxRequestID: 100,
	}
	if err := session.Run(quicmoq.NewClient(conn)); err != nil {
		return fmt.Errorf("session handshake: %w", err)
	}
	s.logger.Info("session established")

	rt, err := session.Subscribe(ctx, s.cfg.Namespace, s.cfg.TrackName)
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	s.logger.Info("subscribed", "namespace", s.cfg.Namespace, "track", s.cfg.TrackName)

	recvHeader := []string{"recv_seq", "group_id", "object_id", "receive_ts_ns", "payload_bytes"}
	if s.cfg.EmbeddedPTS {
		recvHeader = append(recvHeader, "source_pts_s")
	}
	recvFile, recvCSV, err := openCSV(s.cfg.RecvLogPath, recvHeader)
	if err != nil {
		return fmt.Errorf("recv log: %w", err)
	}
	defer recvFile.Close()
	defer recvCSV.Flush()

	dispHeader := []string{"display_seq", "mpv_pts", "frametime_ms"}
	if s.cfg.EmbeddedPTS {
		dispHeader = []string{"display_seq", "group_id", "object_id", "source_pts_s", "mpv_pts", "frametime_ms"}
	}
	dispFile, dispCSV, err := openCSV(s.cfg.DisplayLogPath, dispHeader)
	if err != nil {
		return fmt.Errorf("display log: %w", err)
	}
	defer dispFile.Close()
	defer dispCSV.Flush()

	// 1:1 correlation between sink writes and display events (EmbeddedPTS only).
	var infoCh chan displayInfo
	if s.cfg.EmbeddedPTS {
		infoCh = make(chan displayInfo, 256)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		displaySeq := uint64(0)
		for frame := range sink.Frames() {
			displaySeq++
			if infoCh != nil {
				if info, ok := <-infoCh; ok {
					dispCSV.Write([]string{
						strconv.FormatUint(displaySeq, 10),
						strconv.FormatUint(info.groupID, 10),
						strconv.FormatUint(info.objectID, 10),
						strconv.FormatFloat(info.sourcePTS, 'f', 6, 64),
						strconv.FormatFloat(frame.PTS, 'f', 6, 64),
						strconv.FormatFloat(frame.Frametime, 'f', 1, 64),
					})
					continue
				}
			}
			dispCSV.Write([]string{
				strconv.FormatUint(displaySeq, 10),
				strconv.FormatFloat(frame.PTS, 'f', 6, 64),
				strconv.FormatFloat(frame.Frametime, 'f', 1, 64),
			})
		}
		dispCSV.Flush()
	}()

	// Async sink writer decouples the receive loop from mpv decode speed.
	sinkCh := make(chan sinkFrame, 256)
	var sinkWg sync.WaitGroup
	sinkWg.Add(1)
	go func() {
		defer sinkWg.Done()
		if infoCh != nil {
			defer close(infoCh)
		}
		for sf := range sinkCh {
			if mpvProc, ok := sink.(*MpvProcess); ok && sf.sourcePTS >= 0 {
				mpvProc.SetNextPTS(sf.sourcePTS)
			}
			if _, err := sink.Write(sf.videoData); err != nil {
				s.logger.Error("sink write failed", "err", err)
				return
			}
			if infoCh != nil {
				select {
				case infoCh <- displayInfo{
					sourcePTS: sf.sourcePTS,
					groupID:   sf.groupID,
					objectID:  sf.objectID,
				}:
				default:
					s.logger.Warn("infoCh full, display correlation lost",
						"group", sf.groupID, "object", sf.objectID)
				}
			}
		}
	}()

	// Main receive loop, never blocked by mpv.
	recvSeq := uint64(0)
	for {
		obj, err := rt.ReadObject(ctx)
		if err != nil {
			s.logger.Info("receive loop ended", "err", err)
			break
		}

		receiveTS := time.Now().UnixNano()

		videoData := obj.Payload
		sourcePTS := float64(-1)
		if s.cfg.EmbeddedPTS && len(obj.Payload) >= 8 {
			sourcePTS = math.Float64frombits(binary.BigEndian.Uint64(obj.Payload[:8]))
			videoData = obj.Payload[8:]
		}

		recvRow := []string{
			strconv.FormatUint(recvSeq, 10),
			strconv.FormatUint(obj.GroupID, 10),
			strconv.FormatUint(obj.ObjectID, 10),
			strconv.FormatInt(receiveTS, 10),
			strconv.Itoa(len(obj.Payload)),
		}
		if s.cfg.EmbeddedPTS {
			recvRow = append(recvRow, strconv.FormatFloat(sourcePTS, 'f', 6, 64))
		}
		recvCSV.Write(recvRow)
		recvCSV.Flush()
		recvSeq++

		// Drop the frame for display if mpv is behind; the CSV still records it.
		sf := sinkFrame{
			videoData: videoData,
			sourcePTS: sourcePTS,
			groupID:   obj.GroupID,
			objectID:  obj.ObjectID,
		}
		select {
		case sinkCh <- sf:
		default:
			s.logger.Debug("sink slow, dropping frame for display",
				"group", obj.GroupID, "object", obj.ObjectID)
		}

		s.logger.Debug("received",
			"group", obj.GroupID, "object", obj.ObjectID,
			"size", len(videoData))
	}
	close(sinkCh)
	sinkWg.Wait()

	closeSink()
	wg.Wait()

	return nil
}

func openCSV(path string, header []string) (*os.File, *csv.Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	w := csv.NewWriter(f)
	w.Write(header)
	return f, w, nil
}
