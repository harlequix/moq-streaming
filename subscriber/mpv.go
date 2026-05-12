package subscriber

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
)

// MpvFrame is a frame display event reported by mpv via IPC.
type MpvFrame struct {
	PTS       float64 // seconds (from mpv's time-pos)
	Frametime float64 // milliseconds since previous frame event
}

// VideoSink receives raw H.264 data and reports frame display events.
type VideoSink interface {
	Start() error
	Write(data []byte) (int, error)
	Frames() <-chan MpvFrame
	Close() error
}

// NullSink discards video data, isolating object delivery from mpv decode backpressure.
type NullSink struct {
	frameCh chan MpvFrame
}

func NewNullSink() *NullSink {
	return &NullSink{frameCh: make(chan MpvFrame)}
}

func (n *NullSink) Start() error                    { return nil }
func (n *NullSink) Write(data []byte) (int, error)  { return len(data), nil }
func (n *NullSink) Frames() <-chan MpvFrame          { return n.frameCh }
func (n *NullSink) Close() error                     { close(n.frameCh); return nil }

// MpvProcess manages an mpv subprocess reading video from stdin.
type MpvProcess struct {
	luaScript string
	fps       float64 // explicit framerate (0 = auto-detect)
	container string  // "none" or "mpegts"
	cmd       *exec.Cmd
	stdin     io.WriteCloser
	muxer     *TSMuxer // non-nil when container == "mpegts"
	ipcPath   string
	frameCh   chan MpvFrame
	ipcConn   net.Conn
}

// NewMpvProcess prepares an mpv process. fps=0 means auto-detect; container
// is "none" or "mpegts".
func NewMpvProcess(luaScript string, fps float64, container string) *MpvProcess {
	if container == "" {
		container = "none"
	}
	ipcPath := filepath.Join(os.TempDir(), fmt.Sprintf("moq-mpv-%d.sock", os.Getpid()))
	return &MpvProcess{
		luaScript: luaScript,
		fps:       fps,
		container: container,
		ipcPath:   ipcPath,
		frameCh:   make(chan MpvFrame, 256),
	}
}

func (m *MpvProcess) Start() error {
	os.Remove(m.ipcPath)

	args := []string{
		"--no-cache",
		"--input-ipc-server=" + m.ipcPath,
		"--script=" + m.luaScript,
		"--no-terminal",
		"--vo=null",
	}

	switch m.container {
	case "mpegts":
		args = append(args, "--demuxer-lavf-format=mpegts")
	default:
		args = append(args, "--demuxer-lavf-format=h264")
		if m.fps > 0 {
			args = append(args, fmt.Sprintf("--demuxer-lavf-o=framerate=%g", m.fps))
		}
	}
	args = append(args, "-")

	m.cmd = exec.Command("mpv", args...)
	m.cmd.Stderr = nil

	stdinPipe, err := m.cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("stdin pipe: %w", err)
	}
	m.stdin = stdinPipe

	if m.container == "mpegts" {
		m.muxer = NewTSMuxer(stdinPipe)
	}

	if err := m.cmd.Start(); err != nil {
		return fmt.Errorf("start mpv: %w", err)
	}

	if m.muxer != nil {
		if err := m.muxer.WriteHeaders(); err != nil {
			m.cmd.Process.Kill()
			return fmt.Errorf("write TS headers: %w", err)
		}
	}

	var conn net.Conn
	for i := 0; i < 20; i++ {
		conn, err = net.Dial("unix", m.ipcPath)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		m.cmd.Process.Kill()
		return fmt.Errorf("connect IPC: %w", err)
	}
	m.ipcConn = conn

	go m.readIPC()
	return nil
}

func (m *MpvProcess) readIPC() {
	defer close(m.frameCh)
	scanner := bufio.NewScanner(m.ipcConn)
	var lastTime time.Time

	for scanner.Scan() {
		line := scanner.Bytes()

		var raw map[string]any
		if err := json.Unmarshal(line, &raw); err != nil {
			continue
		}
		event, _ := raw["event"].(string)
		if event != "client-message" {
			continue
		}

		argsRaw, ok := raw["args"].([]any)
		if !ok || len(argsRaw) < 3 {
			continue
		}
		prefix, _ := argsRaw[0].(string)
		if prefix != "frame-event" {
			continue
		}

		_, _ = argsRaw[1].(string) // frame_num is unused; display_seq is tracked in Go
		ptsStr, _ := argsRaw[2].(string)
		pts, err := strconv.ParseFloat(ptsStr, 64)
		if err != nil {
			continue
		}

		now := time.Now()
		var frametime float64
		if !lastTime.IsZero() {
			frametime = float64(now.Sub(lastTime).Milliseconds())
		}
		lastTime = now

		m.frameCh <- MpvFrame{PTS: pts, Frametime: frametime}
	}
}

func (m *MpvProcess) Frames() <-chan MpvFrame {
	return m.frameCh
}

// Write sends video data to mpv's stdin, wrapped in MPEG-TS when in mpegts mode.
func (m *MpvProcess) Write(data []byte) (int, error) {
	if m.muxer != nil {
		return len(data), m.muxer.WriteFrame(data)
	}
	return m.stdin.Write(data)
}

// SetNextPTS applies only in mpegts mode.
func (m *MpvProcess) SetNextPTS(pts float64) {
	if m.muxer != nil {
		m.muxer.SetPTS(pts)
	}
}

func (m *MpvProcess) Close() error {
	if m.stdin != nil {
		m.stdin.Close()
	}
	if m.ipcConn != nil {
		m.ipcConn.Close()
	}
	var err error
	if m.cmd != nil && m.cmd.Process != nil {
		err = m.cmd.Wait()
	}
	os.Remove(m.ipcPath)
	return err
}
