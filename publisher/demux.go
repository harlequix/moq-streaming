package publisher

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// Demuxer extracts H.264 annex-B data from a video file using ffmpeg.
type Demuxer struct {
	path      string
	frameRate float64
	realtime  bool // if true, ffmpeg paces output at native framerate (-re)
	cmd       *exec.Cmd
	stdout    io.ReadCloser
	cancel    context.CancelFunc
}

// NewDemuxer probes the video file for its framerate.
func NewDemuxer(path string, realtime bool) (*Demuxer, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("video file: %w", err)
	}

	out, err := exec.Command("ffprobe",
		"-v", "quiet",
		"-select_streams", "v:0",
		"-show_entries", "stream=r_frame_rate",
		"-of", "csv=p=0",
		path,
	).Output()
	if err != nil {
		return nil, fmt.Errorf("ffprobe: %w", err)
	}

	fr, err := parseFrameRate(strings.TrimSpace(string(out)))
	if err != nil {
		return nil, fmt.Errorf("parse framerate %q: %w", strings.TrimSpace(string(out)), err)
	}

	return &Demuxer{path: path, frameRate: fr, realtime: realtime}, nil
}

// parseFrameRate handles ffprobe's r_frame_rate output: either a plain number
// or a ratio like "30000/1001".
func parseFrameRate(s string) (float64, error) {
	parts := strings.Split(s, "/")
	if len(parts) == 1 {
		return strconv.ParseFloat(parts[0], 64)
	}
	if len(parts) != 2 {
		return 0, fmt.Errorf("unexpected format: %s", s)
	}
	num, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return 0, err
	}
	den, err := strconv.ParseFloat(parts[1], 64)
	if err != nil {
		return 0, err
	}
	if den == 0 {
		return 0, fmt.Errorf("zero denominator")
	}
	return num / den, nil
}

func (d *Demuxer) FrameRate() float64 {
	return d.frameRate
}

// Start launches ffmpeg to extract annex-B H.264. The process is killed when
// ctx is cancelled.
func (d *Demuxer) Start(ctx context.Context) (io.Reader, error) {
	ctx, d.cancel = context.WithCancel(ctx)
	args := []string{}
	if d.realtime {
		args = append(args, "-re", "-readrate_initial_burst", "0.001")
	}
	args = append(args,
		"-i", d.path,
		"-c:v", "copy",
		"-bsf:v", "h264_mp4toannexb",
		"-f", "h264",
		"pipe:1",
	)
	d.cmd = exec.CommandContext(ctx, "ffmpeg", args...)
	d.cmd.Stderr = nil // suppress ffmpeg stderr

	stdout, err := d.cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	d.stdout = stdout

	if err := d.cmd.Start(); err != nil {
		return nil, fmt.Errorf("start ffmpeg: %w", err)
	}

	return stdout, nil
}

func (d *Demuxer) Close() {
	if d.cancel != nil {
		d.cancel()
	}
	if d.cmd != nil && d.cmd.Process != nil {
		d.cmd.Wait()
	}
}
