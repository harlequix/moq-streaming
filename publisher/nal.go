package publisher

import (
	"bytes"
	"io"
)

// Frame is one H.264 access unit (one decoded picture).
type Frame struct {
	NALType  uint8  // primary VCL NAL type (5=IDR, 1=non-IDR slice)
	IsIDR    bool   // true if any NAL in this access unit is type 5
	Data     []byte // complete annex-B data including start codes
	FrameNum uint64 // sequential counter (0-based)
}

// FrameSource provides video frames for publishing.
type FrameSource interface {
	NextFrame() (*Frame, error)
}

// NALParser reads annex-B H.264 chunk-by-chunk, compatible with ffmpeg -re pacing.
type NALParser struct {
	r        io.Reader
	buf      []byte
	nals     chan []byte
	done     chan error
	frameNum uint64

	pendingData []byte
	pendingType uint8
	pendingIDR  bool
	pendingVCL  bool
}

// NewNALParser starts a background goroutine that reads from r and splits into NAL units.
func NewNALParser(r io.Reader) (*NALParser, error) {
	p := &NALParser{
		r:    r,
		nals: make(chan []byte, 64),
		done: make(chan error, 1),
	}
	go p.readLoop()
	return p, nil
}

func (p *NALParser) readLoop() {
	defer close(p.nals)

	buf := make([]byte, 0, 512*1024)
	tmp := make([]byte, 64*1024)

	for {
		n, err := p.r.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
			buf = p.extractNALs(buf)
		}
		if err != nil {
			if len(buf) > 0 {
				nal := make([]byte, len(buf))
				copy(nal, buf)
				p.nals <- nal
			}
			if err != io.EOF {
				p.done <- err
			}
			return
		}
	}
}

func (p *NALParser) extractNALs(buf []byte) []byte {
	first := findStartCode(buf)
	if first < 0 {
		// Keep the last few bytes in case they're a partial start code.
		if len(buf) > 4 {
			return buf[len(buf)-4:]
		}
		return buf
	}

	buf = buf[first:]

	for {
		scLen := startCodeLen(buf)
		next := findStartCode(buf[scLen:])
		if next < 0 {
			return buf
		}
		next += scLen

		nal := make([]byte, next)
		copy(nal, buf[:next])
		p.nals <- nal

		buf = buf[next:]
	}
}

func findStartCode(data []byte) int {
	i := 0
	for i+2 < len(data) {
		if data[i] == 0 && data[i+1] == 0 {
			if data[i+2] == 1 {
				return i
			}
			if i+3 < len(data) && data[i+2] == 0 && data[i+3] == 1 {
				return i
			}
		}
		i++
	}
	return -1
}

func startCodeLen(data []byte) int {
	if len(data) >= 4 && data[0] == 0 && data[1] == 0 && data[2] == 0 && data[3] == 1 {
		return 4
	}
	if len(data) >= 3 && data[0] == 0 && data[1] == 0 && data[2] == 1 {
		return 3
	}
	return 0
}

// NextFrame returns the next access unit, or io.EOF when the stream ends.
func (p *NALParser) NextFrame() (*Frame, error) {
	for {
		nal, ok := <-p.nals
		if !ok {
			if p.pendingVCL {
				return p.emitPending(), nil
			}
			select {
			case err := <-p.done:
				return nil, err
			default:
				return nil, io.EOF
			}
		}

		t := nalTypeOf(nal)
		vcl := t >= 1 && t <= 5

		// A VCL or AU-delimiting NAL (SPS/PPS/AUD) after an accumulated VCL
		// NAL marks an access unit boundary.
		if p.pendingVCL && (vcl || t == 7 || t == 8 || t == 9) {
			frame := p.emitPending()
			p.accumulateNAL(nal, t, vcl)
			return frame, nil
		}

		p.accumulateNAL(nal, t, vcl)
	}
}

func (p *NALParser) accumulateNAL(nal []byte, t uint8, vcl bool) {
	p.pendingData = append(p.pendingData, nal...)
	if vcl {
		p.pendingVCL = true
		p.pendingType = t
		if t == 5 {
			p.pendingIDR = true
		}
	}
}

func (p *NALParser) emitPending() *Frame {
	f := &Frame{
		NALType:  p.pendingType,
		IsIDR:    p.pendingIDR,
		Data:     p.pendingData,
		FrameNum: p.frameNum,
	}
	p.frameNum++
	p.pendingData = nil
	p.pendingType = 0
	p.pendingIDR = false
	p.pendingVCL = false
	return f
}

func nalTypeOf(nal []byte) uint8 {
	idx := bytes.Index(nal, []byte{0, 0, 1})
	if idx >= 0 && idx+3 < len(nal) {
		return nal[idx+3] & 0x1F
	}
	return 0
}
