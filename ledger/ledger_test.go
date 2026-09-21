package ledger

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func tempDir(t *testing.T) string {
	t.Helper()
	d := t.TempDir()
	return filepath.Join(d, "ledger")
}

func mkBatch(client, batch string, evs ...EventInput) *Batch {
	return &Batch{ClientID: client, BatchID: batch, Events: evs}
}

func ev(key, typ, ts string, payload map[string]string) EventInput {
	return EventInput{Key: key, Type: typ, Timestamp: ts, Payload: payload}
}

func TestAppendAndQuery(t *testing.T) {
	s, err := Open(tempDir(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	r, err := s.Append(mkBatch("c1", "b1",
		ev("k1", "created", "2026-09-21T10:00:00Z", map[string]string{"a": "1"}),
		ev("k2", "created", "2026-09-21T10:00:01Z", map[string]string{"a": "2"}),
	))
	if err != nil {
		t.Fatal(err)
	}
	if !r.Accepted || r.StartSeq != 1 || r.EndSeq != 2 {
		t.Fatalf("unexpected receipt %+v", r)
	}

	v1, err := s.GetBySeq(1)
	if err != nil {
		t.Fatal(err)
	}
	if v1.Key != "k1" || v1.Hash == "" {
		t.Fatalf("bad view %+v", v1)
	}
	vk, err := s.GetByKey("k2")
	if err != nil {
		t.Fatal(err)
	}
	if vk.Seq != 2 {
		t.Fatalf("key lookup returned seq %d", vk.Seq)
	}
	if _, err := s.GetBySeq(99); err == nil {
		t.Fatal("expected not found")
	}
	if _, err := s.GetByKey("missing"); err == nil {
		t.Fatal("expected key not found")
	}
}

func TestCanonicalStableAcrossOrderEmptyAndTimeFormats(t *testing.T) {
	base := Event{Seq: 1, Key: "k", Type: "t", Payload: map[string]string{"z": "1", "a": "", "m": "x"}}
	t1, _ := ParseTime("2026-09-21T10:00:00Z")
	t2, _ := ParseTime("2026-09-21 10:00:00")
	t3, _ := ParseTime("2026-09-21T18:00:00+08:00")
	base.Timestamp = t1
	h1 := eventHash(genesisAnchor("c"), &base)
	reordered := Event{
		Seq: 1, Key: "k", Type: "t", Timestamp: t2,
		Payload: map[string]string{"m": "x", "a": "", "z": "1"},
	}
	h2 := eventHash(genesisAnchor("c"), &reordered)
	if string(h1) != string(h2) {
		t.Fatalf("map order changed hash")
	}
	withTZ := Event{Seq: 1, Key: "k", Type: "t", Timestamp: t3, Payload: map[string]string{"a": "", "m": "x", "z": "1"}}
	h3 := eventHash(genesisAnchor("c"), &withTZ)
	if string(h1) != string(h3) {
		t.Fatalf("equivalent time formats changed hash")
	}
	// A genuinely different payload value must differ even when empty fields.
	diff := withTZ
	diff.Payload = map[string]string{"a": "", "m": "y", "z": "1"}
	if string(eventHash(genesisAnchor("c"), &diff)) == string(h1) {
		t.Fatal("distinct payload hashed identically")
	}
}

func TestDuplicateBatchReplayIsIdempotent(t *testing.T) {
	s, err := Open(tempDir(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	b := mkBatch("c1", "b1", ev("k1", "t", "2026-09-21T10:00:00Z", nil))
	r1, err := s.Append(b)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := s.Append(b)
	if err != nil {
		t.Fatal(err)
	}
	if r2.Accepted || r2.StartSeq != r1.StartSeq || r2.EndSeq != r1.EndSeq || r2.TailHash != r1.TailHash {
		t.Fatalf("retry not idempotent: %+v vs %+v", r1, r2)
	}
	if st := s.ChainStatus(); st.TailSeq != 1 {
		t.Fatalf("event appended twice, tail=%d", st.TailSeq)
	}

	// Same batch id, different body -> conflict.
	conflict := mkBatch("c1", "b1", ev("k1", "t", "2026-09-21T10:00:00Z", nil), ev("k9", "t", "2026-09-21T10:00:01Z", nil))
	if _, err := s.Append(conflict); err == nil {
		t.Fatal("expected batch conflict")
	}

	// Duplicate business key across batches -> conflict.
	if _, err := s.Append(mkBatch("c1", "b2", ev("k1", "t", "2026-09-21T11:00:00Z", nil))); err == nil {
		t.Fatal("expected key conflict")
	}
}

func TestVerifyFromArbitraryPositionAndBreak(t *testing.T) {
	d := tempDir(t)
	s, err := Open(d)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		_, err := s.Append(mkBatch("c", string(rune('a'+i)),
			ev("k"+string(rune('a'+i)), "t", "2026-09-21T10:00:0"+string(rune('0'+i))+"Z", nil)))
		if err != nil {
			t.Fatal(err)
		}
	}
	s.Close()

	// Tamper the stored bytes of event 3 while keeping the frame structurally
	// valid (recompute CRC), so recovery reaches a semantic hash mismatch
	// rather than frame truncation.
	logBytes, err := os.ReadFile(filepath.Join(d, logFileName))
	if err != nil {
		t.Fatal(err)
	}
	// Locate event frames: genesis then events. Parse sequentially.
	off, frame := locateEventFrame(t, logBytes, 3)
	// Flip one payload byte inside the JSON body (timestamp digit).
	body := frame[frameHeaderLen : len(frame)-frameCRCLen]
	pos := strings.Index(string(body), "10:00:02")
	if pos < 0 {
		t.Fatal("timestamp marker not found")
	}
	frameCopy := append([]byte(nil), frame...)
	frameCopy[frameHeaderLen+pos+len("10:00:0")] = '9'
	recomputed := encodeFrame(frameCopy[2], frameCopy[frameHeaderLen:len(frameCopy)-frameCRCLen])
	copy(logBytes[off:], recomputed)
	if err := os.WriteFile(filepath.Join(d, logFileName), logBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	// Full reopen must report hash mismatch at seq 3 with expected (old chain)
	// and actual (recomputed) values.
	_, err = Open(d)
	if err == nil {
		t.Fatal("expected corruption error")
	}
	le := &Error{}
	if !asError(err, le) || le.Kind != ErrHashMismatch {
		t.Fatalf("want hash_mismatch, got %v", err)
	}
	if le.Seq != 3 || le.Expected == le.Actual {
		t.Fatalf("bad break detail %+v", le)
	}
}

func asError(err error, target *Error) bool {
	le, ok := err.(*Error)
	if !ok {
		return false
	}
	*target = *le
	return true
}

// locateEventFrame walks frames and returns the offset/bytes of the nth event.
func locateEventFrame(t *testing.T, data []byte, wantSeq int) (int, []byte) {
	t.Helper()
	r := bytes.NewReader(data)
	// skip genesis
	if _, _, err := readOneFrame(r); err != nil {
		t.Fatal(err)
	}
	for {
		start64, _ := r.Seek(0, io.SeekCurrent)
		start := int(start64)
		k, b, err := readOneFrame(r)
		if err != nil {
			t.Fatal(err)
		}
		if k == frameEvent {
			e, err := unmarshalEvent(b)
			if err != nil {
				t.Fatal(err)
			}
			if e.Seq == int64(wantSeq) {
				end64, _ := r.Seek(0, io.SeekCurrent)
				return start, data[start:int(end64)]
			}
		}
	}
}
