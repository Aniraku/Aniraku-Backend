package streaming

import "testing"

func TestTsAudioLangs(t *testing.T) {
	pat := tsPacket(0x0000, true, patSection(1, 0x0100))
	pmt := tsPacket(0x0100, true, pmtSection(0x0101, "eng"))
	seg := append(append([]byte{}, pat...), pmt...)
	if got := tsAudioLangs(seg); !got["en"] || len(got) != 1 {
		t.Fatalf("eng TS = %v, want {en}", got)
	}
	pmtJ := tsPacket(0x0100, true, pmtSection(0x0101, "jpn"))
	if got := tsAudioLangs(append(pat, pmtJ...)); !got["ja"] || len(got) != 1 {
		t.Fatalf("jpn TS = %v, want {ja}", got)
	}
	if got := tsAudioLangs([]byte{0x47, 0x40, 0x00, 0x10}); len(got) != 0 {
		t.Fatalf("truncated TS must be unknown, got %v", got)
	}
	if got := tsAudioLangs([]byte("not transport stream")); len(got) != 0 {
		t.Fatalf("garbage must be unknown, got %v", got)
	}
	if got := tsAudioLangs(nil); len(got) != 0 {
		t.Fatalf("empty must be unknown, got %v", got)
	}
}

// tsPacket builds one 188-byte packet: header (PID, PUSI, CC) + pointer 0
// + section, padded with 0xFF.
func tsPacket(pid int, pusi bool, section []byte) []byte {
	p := make([]byte, 188)
	p[0] = 0x47
	p[1] = byte(pid>>8) & 0x1F
	p[2] = byte(pid)
	p[3] = 0x10
	if pusi {
		p[1] |= 0x40
	}
	for i := range p[4:] {
		p[4+i] = 0xFF
	}
	p[4] = 0x00 // pointer_field
	copy(p[5:], section)
	return p
}

// patSection maps program 1 to pmtPID.
func patSection(program, pmtPID int) []byte {
	s := []byte{0x00, 0xB0, 0x0D, 0x00, 0x01, 0xC1, 0x00, 0x00,
		byte(program >> 8), byte(program), byte(pmtPID>>8)&0x1F | 0xE0, byte(pmtPID),
		0x00, 0x00, 0x00, 0x00}
	return s
}

// pmtSection declares one AAC stream with an ISO-639 language descriptor.
func pmtSection(audioPID int, lang string) []byte {
	desc := []byte{0x0A, 0x04, lang[0], lang[1], lang[2], 0x00}
	s := []byte{0x02, 0xB0, 0x00, 0x00, 0x01, 0xC1, 0x00, 0x00,
		0xE0, 0x00, 0xF0, 0x00}
	s = append(s, 0x0F, byte(audioPID>>8)&0x1F|0xE0, byte(audioPID),
		byte(len(desc)>>8)&0x0F|0xF0, byte(len(desc)))
	s = append(s, desc...)
	s = append(s, 0x00, 0x00, 0x00, 0x00) // CRC (unchecked)
	s[1] = 0xB0 | byte((len(s)-3)>>8)
	s[2] = byte(len(s) - 3)
	return s
}
