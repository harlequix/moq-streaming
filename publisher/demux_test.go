package publisher

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func requireFFmpeg(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not found, skipping integration test")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe not found, skipping integration test")
	}
}

// generateTestVideo creates a tiny H.264 MP4 at the given path.
// 1 second, 30fps, GoP size 10, 160x120.
func generateTestVideo(t *testing.T, path string) {
	t.Helper()
	cmd := exec.Command("ffmpeg",
		"-y", "-f", "lavfi",
		"-i", "testsrc=duration=1:size=160x120:rate=30",
		"-c:v", "libx264", "-g", "10",
		"-pix_fmt", "yuv420p",
		path,
	)
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		t.Fatalf("generate test video: %v", err)
	}
}

func TestDemuxer_FrameRate(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	requireFFmpeg(t)

	dir := t.TempDir()
	video := filepath.Join(dir, "test.mp4")
	generateTestVideo(t, video)

	d, err := NewDemuxer(video, false)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	fr := d.FrameRate()
	if fr < 29.0 || fr > 31.0 {
		t.Errorf("framerate: got %f, want ~30", fr)
	}
}

func TestDemuxer_ProducesAnnexB(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	requireFFmpeg(t)

	dir := t.TempDir()
	video := filepath.Join(dir, "test.mp4")
	generateTestVideo(t, video)

	d, err := NewDemuxer(video, false)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	ctx := context.Background()
	r, err := d.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}

	p, err := NewNALParser(r)
	if err != nil {
		t.Fatal(err)
	}

	var frames []*Frame
	for {
		f, err := p.NextFrame()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		frames = append(frames, f)
	}

	// 1 second at 30fps is 30 frames.
	if len(frames) < 25 || len(frames) > 35 {
		t.Errorf("expected ~30 frames, got %d", len(frames))
	}

	if len(frames) > 0 && !frames[0].IsIDR {
		t.Error("first frame should be IDR")
	}

	// GoP size 10: IDR frames at 0, 10, 20.
	idrCount := 0
	for _, f := range frames {
		if f.IsIDR {
			idrCount++
		}
	}
	if idrCount < 2 {
		t.Errorf("expected at least 2 IDR frames (GoP=10), got %d", idrCount)
	}
}

func TestDemuxer_BadPath(t *testing.T) {
	_, err := NewDemuxer("/nonexistent/video.mp4", false)
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestDemuxer_ContextCancel(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	requireFFmpeg(t)

	dir := t.TempDir()
	video := filepath.Join(dir, "test.mp4")
	generateTestVideo(t, video)

	d, err := NewDemuxer(video, false)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	ctx, cancel := context.WithCancel(context.Background())
	_, err = d.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cancel()
	d.Close()
}

func TestNewDemuxer_FileExists(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	requireFFmpeg(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "notavideo.txt")
	os.WriteFile(path, []byte("hello"), 0644)

	_, err := NewDemuxer(path, false)
	if err == nil {
		t.Error("expected error for non-video file")
	}
}
