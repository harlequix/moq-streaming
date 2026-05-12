package subscriber

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

func TestEncodePTS(t *testing.T) {
	tests := []struct {
		name string
		pts  uint64
	}{
		{"zero", 0},
		{"one_second", 90000},
		{"half_second", 45000},
		{"large", 30 * 90000}, // 30 seconds
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, 5)
			encodePTS(buf, tt.pts)

			got := decodePTS(buf)
			if got != tt.pts {
				t.Errorf("round-trip: encoded %d, decoded %d", tt.pts, got)
			}

			if buf[0]&0x01 != 1 {
				t.Error("marker bit 0 not set")
			}
			if buf[2]&0x01 != 1 {
				t.Error("marker bit 2 not set")
			}
			if buf[4]&0x01 != 1 {
				t.Error("marker bit 4 not set")
			}
		})
	}
}

// decodePTS extracts a 33-bit PTS from the 5-byte encoding.
func decodePTS(buf []byte) uint64 {
	pts := uint64(buf[0]&0x0E) << 29
	pts |= uint64(buf[1]) << 22
	pts |= uint64(buf[2]&0xFE) << 14
	pts |= uint64(buf[3]) << 7
	pts |= uint64(buf[4]&0xFE) >> 1
	return pts
}

func TestCRC32MPEG2(t *testing.T) {
	// Known test vector: CRC32 of "123456789" is 0x0376E6E7.
	data := []byte("123456789")
	got := crc32MPEG2(data)
	want := uint32(0x0376E6E7)
	if got != want {
		t.Errorf("crc32MPEG2(%q) = 0x%08X, want 0x%08X", data, got, want)
	}
}

func TestTSMuxer_WriteHeaders(t *testing.T) {
	var buf bytes.Buffer
	m := NewTSMuxer(&buf)

	if err := m.WriteHeaders(); err != nil {
		t.Fatalf("WriteHeaders: %v", err)
	}

	// WriteHeaders should produce exactly PAT + PMT.
	if buf.Len() != 2*tsPacketSize {
		t.Fatalf("got %d bytes, want %d (2 packets)", buf.Len(), 2*tsPacketSize)
	}

	data := buf.Bytes()

	if data[0] != tsSyncByte {
		t.Errorf("PAT sync byte: got 0x%02X, want 0x47", data[0])
	}
	patPID := uint16(data[1]&0x1F)<<8 | uint16(data[2])
	if patPID != pidPAT {
		t.Errorf("PAT PID: got 0x%04X, want 0x%04X", patPID, pidPAT)
	}

	if data[tsPacketSize] != tsSyncByte {
		t.Errorf("PMT sync byte: got 0x%02X, want 0x47", data[tsPacketSize])
	}
	pmtPID := uint16(data[tsPacketSize+1]&0x1F)<<8 | uint16(data[tsPacketSize+2])
	if pmtPID != pidPMT {
		t.Errorf("PMT PID: got 0x%04X, want 0x%04X", pmtPID, pidPMT)
	}
}

func TestTSMuxer_WriteFrame(t *testing.T) {
	var buf bytes.Buffer
	m := NewTSMuxer(&buf)
	if err := m.WriteHeaders(); err != nil {
		t.Fatal(err)
	}
	buf.Reset()

	frame := []byte{0, 0, 0, 1, 5, 0xAA, 0xBB}
	m.SetPTS(1.5)
	if err := m.WriteFrame(frame); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	data := buf.Bytes()
	if len(data) < 3*tsPacketSize {
		t.Fatalf("got %d bytes, want >= %d (PAT+PMT+video)", len(data), 3*tsPacketSize)
	}

	if len(data)%tsPacketSize != 0 {
		t.Fatalf("output not 188-byte aligned: %d bytes", len(data))
	}

	found := false
	for off := 0; off < len(data); off += tsPacketSize {
		pid := uint16(data[off+1]&0x1F)<<8 | uint16(data[off+2])
		if pid == pidVideo {
			found = true
			if data[off+1]&0x40 == 0 {
				t.Error("first video packet: PUSI not set")
			}
			break
		}
	}
	if !found {
		t.Error("no video PID packet found")
	}
}

func TestTSMuxer_PTSInPES(t *testing.T) {
	var buf bytes.Buffer
	m := NewTSMuxer(&buf)
	m.WriteHeaders()
	buf.Reset()

	// 2.0 seconds = 180000 ticks.
	m.SetPTS(2.0)
	frame := []byte{0, 0, 0, 1, 5, 0xFF}
	m.WriteFrame(frame)

	data := buf.Bytes()

	for off := 0; off < len(data); off += tsPacketSize {
		pid := uint16(data[off+1]&0x1F)<<8 | uint16(data[off+2])
		pusi := data[off+1] & 0x40
		if pid != pidVideo || pusi == 0 {
			continue
		}

		pkt := data[off : off+tsPacketSize]
		pesIdx := bytes.Index(pkt, []byte{0x00, 0x00, 0x01, streamIDVideo})
		if pesIdx < 0 {
			t.Fatal("PES start code not found in video packet")
		}

		// PES header: [start_code(3)][stream_id(1)][length(2)][flags(2)][header_len(1)][PTS(5)]
		ptsOffset := pesIdx + 9
		if ptsOffset+5 > len(pkt) {
			t.Fatal("PTS bytes outside packet")
		}

		gotPTS := decodePTS(pkt[ptsOffset : ptsOffset+5])
		wantPTS := uint64(2.0 * ptsTicksPerSecond)
		if gotPTS != wantPTS {
			t.Errorf("PTS: got %d, want %d", gotPTS, wantPTS)
		}
		return
	}
	t.Fatal("no video packet with PUSI found")
}

func TestTSMuxer_FallbackPTS(t *testing.T) {
	var buf bytes.Buffer
	m := NewTSMuxer(&buf)
	m.WriteHeaders()
	buf.Reset()

	// Skipping SetPTS, so the first frame should fall back to PTS=0.
	frame := []byte{0, 0, 0, 1, 5, 0xFF}
	m.WriteFrame(frame)

	if buf.Len() == 0 {
		t.Fatal("no output written")
	}

	data := buf.Bytes()
	for off := 0; off < len(data); off += tsPacketSize {
		pid := uint16(data[off+1]&0x1F)<<8 | uint16(data[off+2])
		pusi := data[off+1] & 0x40
		if pid != pidVideo || pusi == 0 {
			continue
		}
		pkt := data[off : off+tsPacketSize]
		pesIdx := bytes.Index(pkt, []byte{0x00, 0x00, 0x01, streamIDVideo})
		if pesIdx < 0 {
			t.Fatal("PES start code not found")
		}
		ptsOffset := pesIdx + 9
		gotPTS := decodePTS(pkt[ptsOffset : ptsOffset+5])
		if gotPTS != 0 {
			t.Errorf("first fallback PTS: got %d, want 0", gotPTS)
		}
		return
	}
	t.Fatal("no video packet with PUSI found")
}

func TestTSMuxer_LargeFrame(t *testing.T) {
	var buf bytes.Buffer
	m := NewTSMuxer(&buf)
	m.WriteHeaders()
	buf.Reset()

	frame := make([]byte, 1000)
	frame[0] = 0x00
	frame[1] = 0x00
	frame[2] = 0x00
	frame[3] = 0x01
	frame[4] = 0x05
	for i := 5; i < len(frame); i++ {
		frame[i] = byte(i)
	}

	m.SetPTS(0.0)
	if err := m.WriteFrame(frame); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	data := buf.Bytes()
	if len(data)%tsPacketSize != 0 {
		t.Fatalf("not 188-aligned: %d bytes", len(data))
	}

	nPackets := 0
	for off := 0; off < len(data); off += tsPacketSize {
		if data[off] != tsSyncByte {
			t.Fatalf("packet at offset %d: bad sync byte 0x%02X", off, data[off])
		}
		pid := uint16(data[off+1]&0x1F)<<8 | uint16(data[off+2])
		if pid == pidVideo {
			nPackets++
		}
	}

	// PES header (~14 bytes) + 1000-byte frame exceeds one TS payload (184 bytes),
	// so the muxer must split across multiple packets.
	if nPackets < 2 {
		t.Errorf("expected multiple video packets for 1000-byte frame, got %d", nPackets)
	}
}

func TestPTSHeaderEncoding(t *testing.T) {
	ptsS := 1.5
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, math.Float64bits(ptsS))

	decoded := math.Float64frombits(binary.BigEndian.Uint64(buf))
	if decoded != ptsS {
		t.Errorf("round-trip: got %f, want %f", decoded, ptsS)
	}
}
