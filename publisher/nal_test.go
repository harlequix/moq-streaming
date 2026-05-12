package publisher

import (
	"bytes"
	"io"
	"testing"
)

// annexBNAL builds a single annex-B NAL unit: 4-byte start code + header + payload.
func annexBNAL(nalType uint8, payloadSize int) []byte {
	data := []byte{0x00, 0x00, 0x00, 0x01, nalType & 0x1F}
	for i := 0; i < payloadSize; i++ {
		data = append(data, byte(i%251))
	}
	return data
}

func concatNALs(nals ...[]byte) []byte {
	var buf bytes.Buffer
	for _, n := range nals {
		buf.Write(n)
	}
	return buf.Bytes()
}

func readAllFrames(t *testing.T, p *NALParser) []*Frame {
	t.Helper()
	var frames []*Frame
	for {
		f, err := p.NextFrame()
		if err == io.EOF {
			return frames
		}
		if err != nil {
			t.Fatalf("NextFrame: %v", err)
		}
		frames = append(frames, f)
	}
}

func TestNALParser_SingleIDRFrame(t *testing.T) {
	// SPS + PPS + IDR is one access unit.
	data := concatNALs(
		annexBNAL(7, 10),  // SPS
		annexBNAL(8, 5),   // PPS
		annexBNAL(5, 100), // IDR slice
	)
	p, err := NewNALParser(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	frames := readAllFrames(t, p)
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(frames))
	}
	if !frames[0].IsIDR {
		t.Error("frame 0 should be IDR")
	}
	if frames[0].NALType != 5 {
		t.Errorf("frame 0 NALType: got %d, want 5", frames[0].NALType)
	}
	if frames[0].FrameNum != 0 {
		t.Errorf("frame 0 FrameNum: got %d, want 0", frames[0].FrameNum)
	}
	if !bytes.Equal(frames[0].Data, data) {
		t.Errorf("frame data mismatch: got %d bytes, want %d", len(frames[0].Data), len(data))
	}
}

func TestNALParser_IDRThenPFrame(t *testing.T) {
	idrData := concatNALs(
		annexBNAL(7, 10),
		annexBNAL(8, 5),
		annexBNAL(5, 100),
	)
	pData := annexBNAL(1, 50)
	stream := concatNALs(idrData, pData)

	p, err := NewNALParser(bytes.NewReader(stream))
	if err != nil {
		t.Fatal(err)
	}
	frames := readAllFrames(t, p)
	if len(frames) != 2 {
		t.Fatalf("expected 2 frames, got %d", len(frames))
	}

	if !frames[0].IsIDR {
		t.Error("frame 0 should be IDR")
	}
	if !bytes.Equal(frames[0].Data, idrData) {
		t.Errorf("frame 0 data: got %d bytes, want %d", len(frames[0].Data), len(idrData))
	}

	if frames[1].IsIDR {
		t.Error("frame 1 should not be IDR")
	}
	if frames[1].NALType != 1 {
		t.Errorf("frame 1 NALType: got %d, want 1", frames[1].NALType)
	}
	if frames[1].FrameNum != 1 {
		t.Errorf("frame 1 FrameNum: got %d, want 1", frames[1].FrameNum)
	}
}

func TestNALParser_TwoGoPs(t *testing.T) {
	// GoP 0: SPS+PPS+IDR, P, P. GoP 1: SPS+PPS+IDR.
	gop0IDR := concatNALs(annexBNAL(7, 10), annexBNAL(8, 5), annexBNAL(5, 100))
	p0 := annexBNAL(1, 50)
	p1 := annexBNAL(1, 60)
	gop1IDR := concatNALs(annexBNAL(7, 8), annexBNAL(8, 4), annexBNAL(5, 80))
	stream := concatNALs(gop0IDR, p0, p1, gop1IDR)

	p, err := NewNALParser(bytes.NewReader(stream))
	if err != nil {
		t.Fatal(err)
	}
	frames := readAllFrames(t, p)
	if len(frames) != 4 {
		t.Fatalf("expected 4 frames, got %d", len(frames))
	}

	if !frames[0].IsIDR {
		t.Error("frame 0 should be IDR")
	}
	if !bytes.Equal(frames[0].Data, gop0IDR) {
		t.Error("frame 0 data mismatch")
	}
	if frames[1].IsIDR {
		t.Error("frame 1 should not be IDR")
	}
	if frames[2].IsIDR {
		t.Error("frame 2 should not be IDR")
	}
	if !frames[3].IsIDR {
		t.Error("frame 3 should be IDR")
	}
	if !bytes.Equal(frames[3].Data, gop1IDR) {
		t.Error("frame 3 data mismatch")
	}
}

func TestNALParser_ThreeByteStartCode(t *testing.T) {
	data := []byte{0x00, 0x00, 0x01, 0x05, 0xAA, 0xBB}
	p, err := NewNALParser(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	frames := readAllFrames(t, p)
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(frames))
	}
	if !frames[0].IsIDR {
		t.Error("frame should be IDR")
	}
}

func TestNALParser_EmptyReader(t *testing.T) {
	p, err := NewNALParser(bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.NextFrame()
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}
