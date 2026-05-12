package relay

import (
	"context"
	"crypto/tls"
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mengelbart/moqtransport"
	"github.com/mengelbart/moqtransport/quicmoq"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlogwriter"
)

// Config holds relay configuration.
type Config struct {
	ListenAddr   string
	UpstreamAddr string
	Namespace    []string
	TrackName    string
	TLSConfig    *tls.Config

	Strategy TimeoutStrategy

	// DefaultTimeout applies when the subscriber's SUBSCRIBE omits
	// DELIVERY_TIMEOUT. Zero means no timeout (reliable).
	DefaultTimeout time.Duration

	// MetricsPath, if set, receives per-subscriber metrics on shutdown.
	MetricsPath string

	// QlogDir enables qlog tracing for downstream connections (one file each).
	QlogDir string
}

// Relay forwards MoQ objects from an upstream publisher to downstream subscribers.
type Relay struct {
	cfg    Config
	cache  *TrackCache
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
	wg     sync.WaitGroup

	mu          sync.Mutex
	subscribers []*subscriberMetrics

	upstreamOnce sync.Once
	upstreamErr  error
}

type subscriberMetrics struct {
	id             int
	strategy       TimeoutStrategy
	timeout        time.Duration
	objectsSent    atomic.Uint64
	objectsDropped atomic.Uint64
	groupsReset    atomic.Uint64
	groupsComplete atomic.Uint64
}

func New(cfg Config, logger *slog.Logger) *Relay {
	ctx, cancel := context.WithCancel(context.Background())
	return &Relay{
		cfg:    cfg,
		cache:  NewTrackCache(),
		ctx:    ctx,
		cancel: cancel,
		logger: logger,
	}
}

func trackKey(namespace []string, track string) string {
	return strings.Join(namespace, "/") + "/" + track
}

// ConnectUpstream dials the publisher and ingests into the cache; only the
// first call connects.
func (r *Relay) ConnectUpstream() error {
	r.upstreamOnce.Do(func() {
		r.upstreamErr = r.connectUpstream()
	})
	return r.upstreamErr
}

func (r *Relay) connectUpstream() error {
	conn, err := quic.DialAddr(r.ctx, r.cfg.UpstreamAddr, &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"moq-00"},
	}, &quic.Config{
		EnableDatagrams: true,
	})
	if err != nil {
		return fmt.Errorf("dial upstream: %w", err)
	}
	r.logger.Info("connected to upstream", "addr", r.cfg.UpstreamAddr)

	session := &moqtransport.Session{
		Handler: moqtransport.HandlerFunc(func(w moqtransport.ResponseWriter, m *moqtransport.Message) {
			switch m.Method {
			case moqtransport.MessageAnnounce:
				if err := w.Accept(); err != nil {
					r.logger.Error("failed to accept announcement", "err", err)
				}
				r.logger.Info("accepted upstream announcement", "namespace", m.Namespace)
			default:
				r.logger.Debug("unhandled upstream message", "method", m.Method)
			}
		}),
		InitialMaxRequestID: 100,
	}
	if err := session.Run(quicmoq.NewClient(conn)); err != nil {
		return fmt.Errorf("upstream session handshake: %w", err)
	}
	r.logger.Info("upstream session established")

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		<-conn.Context().Done()
		r.logger.Info("upstream connection lost")
		r.cache.CloseAll()
	}()

	rt, err := session.Subscribe(r.ctx, r.cfg.Namespace, r.cfg.TrackName)
	if err != nil {
		return fmt.Errorf("subscribe upstream: %w", err)
	}
	r.logger.Info("subscribed to upstream track",
		"namespace", r.cfg.Namespace, "track", r.cfg.TrackName)

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.ingestLoop(rt)
	}()

	return nil
}

func (r *Relay) ingestLoop(rt *moqtransport.RemoteTrack) {
	for {
		obj, err := rt.ReadObject(r.ctx)
		if err != nil {
			r.logger.Info("upstream ingest loop ended", "err", err)
			r.cache.CloseAll()
			return
		}
		r.logger.Debug("ingested object",
			"group", obj.GroupID, "subgroup", obj.SubGroupID,
			"object", obj.ObjectID, "size", len(obj.Payload))
		r.cache.Put(&CachedObject{
			GroupID:    obj.GroupID,
			SubGroupID: obj.SubGroupID,
			ObjectID:   obj.ObjectID,
			Payload:    obj.Payload,
			RxTime:     obj.RxTime,
		})
	}
}

// ListenAndServe blocks until the relay context is cancelled.
func (r *Relay) ListenAndServe() error {
	qcfg := &quic.Config{
		EnableDatagrams:                  true,
		EnableStreamResetPartialDelivery: true,
	}
	if r.cfg.QlogDir != "" {
		if err := os.MkdirAll(r.cfg.QlogDir, 0o755); err != nil {
			return fmt.Errorf("create qlog dir: %w", err)
		}
		qcfg.Tracer = func(ctx context.Context, isClient bool, connID quic.ConnectionID) qlogwriter.Trace {
			fname := fmt.Sprintf("%s/subscriber_%x.sqlog", r.cfg.QlogDir, connID)
			f, err := os.Create(fname)
			if err != nil {
				r.logger.Error("failed to create qlog file", "err", err)
				return nil
			}
			r.logger.Info("qlog tracing enabled", "file", fname)
			t := qlogwriter.NewConnectionFileSeq(f, isClient, connID, nil)
			go t.Run()
			return t
		}
	}
	listener, err := quic.ListenAddr(r.cfg.ListenAddr, r.cfg.TLSConfig, qcfg)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	r.logger.Info("listening for subscribers", "addr", r.cfg.ListenAddr,
		"strategy", r.cfg.Strategy, "default_timeout", r.cfg.DefaultTimeout)

	go func() {
		<-r.ctx.Done()
		listener.Close()
	}()

	for {
		conn, err := listener.Accept(r.ctx)
		if err != nil {
			if r.ctx.Err() != nil {
				return nil // clean shutdown
			}
			return fmt.Errorf("accept: %w", err)
		}
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.handleDownstream(conn)
		}()
	}
}

func (r *Relay) handleDownstream(conn *quic.Conn) {
	r.logger.Info("downstream connected", "remote", conn.RemoteAddr())

	session := &moqtransport.Session{
		Handler: moqtransport.HandlerFunc(func(w moqtransport.ResponseWriter, m *moqtransport.Message) {
			r.logger.Debug("unhandled downstream message", "method", m.Method)
		}),
		SubscribeHandler:    r.makeSubscribeHandler(conn.Context()),
		InitialMaxRequestID: 100,
	}
	if err := session.Run(quicmoq.NewServer(conn)); err != nil {
		r.logger.Error("downstream session handshake failed", "err", err)
		return
	}
	r.logger.Info("downstream session established", "remote", conn.RemoteAddr())

	if err := session.Announce(r.ctx, r.cfg.Namespace); err != nil {
		r.logger.Error("failed to announce to downstream", "err", err)
		return
	}

	select {
	case <-conn.Context().Done():
	case <-r.ctx.Done():
		conn.CloseWithError(0, "relay shutting down")
	}
	r.logger.Info("downstream disconnected", "remote", conn.RemoteAddr())
}

func (r *Relay) makeSubscribeHandler(connCtx context.Context) moqtransport.SubscribeHandler {
	return moqtransport.SubscribeHandlerFunc(func(w *moqtransport.SubscribeResponseWriter, m *moqtransport.SubscribeMessage) {
		expected := trackKey(r.cfg.Namespace, r.cfg.TrackName)
		got := trackKey(m.Namespace, m.Track)
		if got != expected {
			r.logger.Warn("rejecting subscription for unknown track", "got", got, "expected", expected)
			if err := w.Reject(moqtransport.ErrorCodeSubscribeTrackDoesNotExist, "unknown track"); err != nil {
				r.logger.Error("failed to reject subscription", "err", err)
			}
			return
		}

		// Delay upstream connect until a subscriber arrives so the publisher's
		// ffmpeg isn't producing into an empty forward loop.
		if err := r.ConnectUpstream(); err != nil {
			r.logger.Error("failed to connect upstream", "err", err)
			if err := w.Reject(moqtransport.ErrorCodeSubscribeInternal, "upstream unavailable"); err != nil {
				r.logger.Error("failed to reject subscription", "err", err)
			}
			return
		}

		if err := w.Accept(); err != nil {
			r.logger.Error("failed to accept subscription", "err", err)
			return
		}

		timeout := r.cfg.DefaultTimeout
		if dt, ok := m.Parameters.GetDeliveryTimeout(); ok {
			timeout = dt
		}

		strategy := r.cfg.Strategy
		if timeout == 0 {
			strategy = StrategyNone
		}

		r.mu.Lock()
		subID := len(r.subscribers)
		sm := &subscriberMetrics{
			id:       subID,
			strategy: strategy,
			timeout:  timeout,
		}
		r.subscribers = append(r.subscribers, sm)
		r.mu.Unlock()

		r.logger.Info("accepted subscription",
			"namespace", m.Namespace, "track", m.Track,
			"request_id", m.RequestID,
			"strategy", strategy, "timeout", timeout,
			"subscriber_id", subID)

		ch := r.cache.Subscribe()
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			r.forwardLoop(connCtx, w, ch, strategy, timeout, sm)
		}()
	})
}

const timeoutErrorCode = 0x01

func (r *Relay) forwardLoop(
	connCtx context.Context,
	pub moqtransport.Publisher,
	ch <-chan *CachedObject,
	strategy TimeoutStrategy,
	timeout time.Duration,
	metrics *subscriberMetrics,
) {
	defer r.cache.Unsubscribe(ch)

	type sgState struct {
		sg      *moqtransport.Subgroup
		expired bool
	}

	type sgKey struct {
		GroupID    uint64
		SubGroupID uint64
	}

	// A map (not a single current subgroup) is needed because interleaved
	// objects from the upstream ReadObject buffer would otherwise open the
	// same group on multiple streams and lose objects.
	sgs := map[sgKey]*sgState{}

	closeSG := func(state *sgState) {
		if state == nil {
			return
		}
		if !state.expired {
			state.sg.Close()
			metrics.groupsComplete.Add(1)
		}
	}
	defer func() {
		for _, state := range sgs {
			closeSG(state)
		}
	}()

	for {
		select {
		case <-connCtx.Done():
			r.logger.Info("forward loop ended (subscriber disconnected)",
				"subscriber", metrics.id)
			return
		case obj, ok := <-ch:
			if !ok {
				r.logger.Info("forward loop ended (channel closed)",
					"subscriber", metrics.id)
				return
			}

			key := sgKey{obj.GroupID, obj.SubGroupID}

			state := sgs[key]
			if state == nil {
				sg, err := pub.OpenSubgroup(obj.GroupID, obj.SubGroupID, 0)
				if err != nil {
					r.logger.Error("failed to open subgroup", "err", err,
						"group", obj.GroupID, "subgroup", obj.SubGroupID)
					return
				}
				state = &sgState{sg: sg}
				sgs[key] = state
			}

			if state.expired {
				metrics.objectsDropped.Add(1)
				r.logger.Debug("skipping object (subgroup expired)",
					"group", obj.GroupID, "object", obj.ObjectID)
				continue
			}

			// Per-object delivery timeout (draft-ietf-moq-transport §9.3.3):
			// deadline is based on the object header's receive time.
			if strategy != StrategyNone && timeout > 0 {
				deadline := obj.RxTime.Add(timeout)
				if time.Now().After(deadline) {
					r.logger.Info("delivery timeout exceeded",
						"subscriber", metrics.id,
						"group", obj.GroupID, "object", obj.ObjectID,
						"strategy", strategy,
						"age", time.Since(obj.RxTime).Round(time.Millisecond))

					switch strategy {
					case StrategyResetAtObject:
						state.sg.SetReliableBoundary()
					case StrategyResetAtKeyframe:
						if obj.ObjectID == 0 {
							// I-frame itself timed out, nothing to protect.
						}
						// Boundary was set after I-frame write (if it was written).
					}

					state.sg.Reset(timeoutErrorCode)
					state.expired = true
					metrics.groupsReset.Add(1)
					metrics.objectsDropped.Add(1)
					continue
				}
			}

			if _, err := state.sg.WriteObject(obj.ObjectID, obj.Payload); err != nil {
				r.logger.Error("failed to write object", "err", err,
					"group", obj.GroupID, "object", obj.ObjectID)
				return
			}
			metrics.objectsSent.Add(1)

			switch strategy {
			case StrategyResetAtObject:
				state.sg.SetReliableBoundary()
			case StrategyResetAtKeyframe:
				if obj.ObjectID == 0 {
					state.sg.SetReliableBoundary()
				}
			}

			r.logger.Debug("forwarded object",
				"group", obj.GroupID, "subgroup", obj.SubGroupID,
				"object", obj.ObjectID, "size", len(obj.Payload))
		}
	}
}

func (r *Relay) Wait() {
	r.wg.Wait()
}

func (r *Relay) Shutdown() {
	r.cancel()
	r.cache.CloseAll()
	r.wg.Wait()

	r.logger.Info("relay stats",
		"ingested", r.cache.Ingested.Load(),
		"chan_drops", r.cache.ChanDrops.Load())

	if r.cfg.MetricsPath != "" {
		r.writeMetrics()
	}
}

func (r *Relay) writeMetrics() {
	r.mu.Lock()
	subs := r.subscribers
	r.mu.Unlock()

	f, err := os.Create(r.cfg.MetricsPath)
	if err != nil {
		r.logger.Error("failed to create metrics file", "err", err)
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{
		"subscriber_id", "strategy", "timeout_ms",
		"objects_sent", "objects_dropped", "groups_reset", "groups_complete",
	})
	for _, sm := range subs {
		w.Write([]string{
			strconv.Itoa(sm.id),
			sm.strategy.String(),
			strconv.FormatInt(sm.timeout.Milliseconds(), 10),
			strconv.FormatUint(sm.objectsSent.Load(), 10),
			strconv.FormatUint(sm.objectsDropped.Load(), 10),
			strconv.FormatUint(sm.groupsReset.Load(), 10),
			strconv.FormatUint(sm.groupsComplete.Load(), 10),
		})
	}
	r.logger.Info("wrote metrics", "path", r.cfg.MetricsPath)
}
