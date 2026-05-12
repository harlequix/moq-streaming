package subscriber

import (
	"encoding/binary"
	"io"
)

// Minimal MPEG-TS muxer wrapping H.264 Annex B frames with PTS. Single video
// elementary stream only.

const (
	tsPacketSize = 188
	tsSyncByte   = 0x47

	pidPAT   = 0x0000
	pidPMT   = 0x1000
	pidVideo = 0x0100

	streamIDVideo = 0xE0
	streamTypeAVC = 0x1B

	ptsTicksPerSecond = 90000 // 90kHz clock
)

// TSMuxer writes H.264 frames as MPEG-TS packets to an io.Writer.
type TSMuxer struct {
	w          io.Writer
	ccPAT      uint8 // continuity counter for PAT
	ccPMT      uint8 // continuity counter for PMT
	ccVideo    uint8 // continuity counter for video PID
	nextPTS    uint64
	ptsSet     bool
	frameCount uint64
}

func NewTSMuxer(w io.Writer) *TSMuxer {
	return &TSMuxer{w: w}
}

// SetPTS applies to the next WriteFrame call.
func (m *TSMuxer) SetPTS(seconds float64) {
	m.nextPTS = uint64(seconds * ptsTicksPerSecond)
	m.ptsSet = true
}

func (m *TSMuxer) WriteHeaders() error {
	if err := m.writePAT(); err != nil {
		return err
	}
	return m.writePMT()
}

// WriteFrame wraps one H.264 access unit in PES/TS packets; PAT/PMT are
// resent periodically.
func (m *TSMuxer) WriteFrame(data []byte) error {
	// Re-send PAT/PMT every 64 frames for robustness.
	if m.frameCount%64 == 0 {
		if err := m.writePAT(); err != nil {
			return err
		}
		if err := m.writePMT(); err != nil {
			return err
		}
	}
	pts := m.nextPTS
	if !m.ptsSet {
		// Fallback PTS assumes 30fps.
		pts = m.frameCount * ptsTicksPerSecond / 30
	}
	m.frameCount++
	m.ptsSet = false

	pesHeader := buildPESHeader(pts, len(data))
	payload := append(pesHeader, data...)

	first := true
	for len(payload) > 0 {
		pkt := make([]byte, tsPacketSize)
		pkt[0] = tsSyncByte

		pkt[1] = byte((pidVideo >> 8) & 0x1F)
		pkt[2] = byte(pidVideo & 0xFF)
		if first {
			pkt[1] |= 0x40 // payload unit start indicator
			first = false
		}

		maxPayload := tsPacketSize - 4
		if len(payload) < maxPayload {
			stuffLen := maxPayload - len(payload)
			pkt[3] = 0x30 | (m.ccVideo & 0x0F) // adaptation + payload
			m.ccVideo++
			if stuffLen == 1 {
				pkt[4] = 0 // adaptation_field_length = 0
				copy(pkt[5:], payload)
			} else {
				pkt[4] = byte(stuffLen - 1)   // adaptation_field_length
				pkt[5] = 0x00                 // flags
				for i := 6; i < 4+stuffLen; i++ {
					pkt[i] = 0xFF // stuffing bytes
				}
				copy(pkt[4+stuffLen:], payload)
			}
			payload = nil
		} else {
			pkt[3] = 0x10 | (m.ccVideo & 0x0F) // payload only
			m.ccVideo++
			copy(pkt[4:], payload[:maxPayload])
			payload = payload[maxPayload:]
		}

		if _, err := m.w.Write(pkt); err != nil {
			return err
		}
	}
	return nil
}

func buildPESHeader(pts90kHz uint64, dataLen int) []byte {
	// PES header: 6 bytes fixed + 3 bytes optional header + 5 bytes PTS = 14 bytes
	pesHeaderDataLen := byte(5) // PTS only
	totalPESHeaderLen := 9      // 6 + 3 bytes before optional data

	h := make([]byte, totalPESHeaderLen+int(pesHeaderDataLen))
	// Start code prefix
	h[0] = 0x00
	h[1] = 0x00
	h[2] = 0x01
	// Stream ID (video)
	h[3] = streamIDVideo
	// PES packet length (0 = unbounded, valid for video)
	h[4] = 0x00
	h[5] = 0x00
	// Flags: marker bits, no scrambling, no alignment
	h[6] = 0x80
	// PTS flag set
	h[7] = 0x80
	// PES header data length
	h[8] = pesHeaderDataLen

	// Encode PTS (5 bytes)
	encodePTS(h[9:14], pts90kHz)

	return h
}

// encodePTS writes a 33-bit PTS value into 5 bytes per MPEG-TS spec.
func encodePTS(buf []byte, pts uint64) {
	// Format: 0010 xxx1 xxxx xxxx xxxx xxx1 xxxx xxxx xxxx xxx1
	//         marker(4) pts[32:30](3) marker(1) pts[29:15](15) marker(1) pts[14:0](15) marker(1)
	pts33 := pts & 0x1FFFFFFFF
	buf[0] = 0x21 | byte((pts33>>29)&0x0E) // 0010 + pts[32:30] + 1
	buf[1] = byte(pts33 >> 22)
	buf[2] = byte((pts33>>14)&0xFE) | 0x01
	buf[3] = byte(pts33 >> 7)
	buf[4] = byte((pts33&0x7F)<<1) | 0x01
}

func (m *TSMuxer) writePAT() error {
	pkt := make([]byte, tsPacketSize)
	pkt[0] = tsSyncByte
	pkt[1] = 0x40 // PUSI
	pkt[2] = 0x00 // PID 0
	pkt[3] = 0x10 | (m.ccPAT & 0x0F)
	m.ccPAT++

	// Pointer field
	pkt[4] = 0x00

	// PAT table
	pat := pkt[5:]
	pat[0] = 0x00 // table_id
	// section_syntax_indicator=1, reserved, section_length
	sectionLen := 9 // 5 bytes after length + 4 CRC
	pat[1] = 0xB0 | byte(sectionLen>>8)
	pat[2] = byte(sectionLen)
	// transport_stream_id
	pat[3] = 0x00
	pat[4] = 0x01
	// version=0, current_next=1
	pat[5] = 0xC1
	// section_number
	pat[6] = 0x00
	// last_section_number
	pat[7] = 0x00
	// program_number=1, PMT PID
	binary.BigEndian.PutUint16(pat[8:], 1) // program 1
	pat[10] = 0xE0 | byte((pidPMT >> 8) & 0x1F)
	pat[11] = byte(pidPMT & 0xFF)

	// CRC32
	crc := crc32MPEG2(pkt[5 : 5+sectionLen+3-4])
	binary.BigEndian.PutUint32(pkt[5+sectionLen+3-4:], crc)

	// Fill remainder with 0xFF
	for i := 5 + sectionLen + 3; i < tsPacketSize; i++ {
		pkt[i] = 0xFF
	}

	_, err := m.w.Write(pkt)
	return err
}

func (m *TSMuxer) writePMT() error {
	pkt := make([]byte, tsPacketSize)
	pkt[0] = tsSyncByte
	pkt[1] = 0x40 | byte((pidPMT >> 8) & 0x1F)
	pkt[2] = byte(pidPMT & 0xFF)
	pkt[3] = 0x10 | (m.ccPMT & 0x0F)
	m.ccPMT++

	// Pointer field
	pkt[4] = 0x00

	// PMT table
	pmt := pkt[5:]
	pmt[0] = 0x02 // table_id
	// section_syntax_indicator=1, section_length
	// program_number(2) + version(1) + section_num(1) + last_section(1) = 5
	// PCR_PID(2) + program_info_length(2) = 4
	// stream_type(1) + ES_PID(2) + ES_info_length(2) = 5
	// CRC(4) = total 18
	sectionLen := 18
	pmt[1] = 0xB0 | byte(sectionLen>>8)
	pmt[2] = byte(sectionLen)
	// program_number
	binary.BigEndian.PutUint16(pmt[3:], 1)
	// version=0, current_next=1
	pmt[5] = 0xC1
	pmt[6] = 0x00 // section_number
	pmt[7] = 0x00 // last_section_number
	// PCR PID
	pmt[8] = 0xE0 | byte((pidVideo>>8)&0x1F)
	pmt[9] = byte(pidVideo & 0xFF)
	// program_info_length = 0
	pmt[10] = 0xF0
	pmt[11] = 0x00
	// Stream entry: H.264
	pmt[12] = streamTypeAVC
	pmt[13] = 0xE0 | byte((pidVideo>>8)&0x1F)
	pmt[14] = byte(pidVideo & 0xFF)
	// ES_info_length = 0
	pmt[15] = 0xF0
	pmt[16] = 0x00

	// CRC32 covers from table_id through ES_info_length.
	crc := crc32MPEG2(pkt[5 : 5+3+sectionLen-4])
	binary.BigEndian.PutUint32(pkt[5+3+sectionLen-4:], crc)

	// Fill remainder with 0xFF
	for i := 5 + 3 + sectionLen; i < tsPacketSize; i++ {
		pkt[i] = 0xFF
	}

	_, err := m.w.Write(pkt)
	return err
}

func crc32MPEG2(data []byte) uint32 {
	crc := uint32(0xFFFFFFFF)
	for _, b := range data {
		crc ^= uint32(b) << 24
		for i := 0; i < 8; i++ {
			if crc&0x80000000 != 0 {
				crc = (crc << 1) ^ 0x04C11DB7
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
