package ledger

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func appendN(t *testing.T, s *Store, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		_, err := s.Append(mkBatch("c", "batch"+string(rune('a'+i)),
			ev("k"+string(rune('a'+i)), "t", "2026-09-21T10:00:0"+string(rune('0'+i))+"Z", nil)))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func reopen(t *testing.T, d string) *Store {
	t.Helper()
	s, err := Open(d)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestTornHalfFrameTruncated(t *testing.T) {
	d := tempDir(t)
	s := reopen(t, d)
	appendN(t, s, 3)
	_, tailHash := s.Tail()
	s.Close()

	p := filepath.Join(d, logFileName)
	raw, _ := os.ReadFile(p)
	goodLen := len(raw)
	// Simulate a half-landed fourth batch: partial event frame header+body.
	body, _ := marshalEvent(&Event{Seq: 4, Key: "k4", Timestamp: mustParse(t, "2026-09-21T10:00:04Z")})
	partial := encodeFrame(frameEvent, body)
	raw = append(raw, partial[:len(partial)-8]...)
	if err := os.WriteFile(p, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	s2 := reopen(t, d)
	if seq, h := s2.Tail(); seq != 3 || h != tailHash {
		t.Fatalf("recovered tail seq=%d hash match=%v (goodLen=%d)", seq, h == tailHash, goodLen)
	}
	// Damaged tail must be physically gone; damaged info reported in status.
	st := s2.ChainStatus()
	if st.Damage == nil || st.Damage.Offset != int64(goodLen) {
		t.Fatalf("damage not reported at %d: %+v", goodLen, st.Damage)
	}
	got, _ := os.ReadFile(p)
	if len(got) != goodLen {
		t.Fatalf("file size %d want %d", len(got), goodLen)
	}
	s2.Close()

	// Reopening again is a no-op and yields identical state.
	s3 := reopen(t, d)
	seq3, h3 := s3.Tail()
	if seq3 != 3 || h3 != tailHash {
		t.Fatal("second reopen changed state")
	}
	if d3 := s3.ChainStatus().Damage; d3 != nil {
		t.Fatalf("damage persisted after clean reopen: %+v", d3)
	}
	s3.Close()
}

func TestDirtyTrailingBytesTruncated(t *testing.T) {
	d := tempDir(t)
	s := reopen(t, d)
	appendN(t, s, 2)
	_, tailHash := s.Tail()
	s.Close()

	p := filepath.Join(d, logFileName)
	raw, _ := os.ReadFile(p)
	goodLen := len(raw)
	raw = append(raw, 0xDE, 0xAD, 0xBE, 0xEF, 'X', 'Y', 'Z')
	if err := os.WriteFile(p, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	s2 := reopen(t, d)
	if seq, h := s2.Tail(); seq != 2 || h != tailHash {
		t.Fatal("dirty tail not recovered")
	}
	got, _ := os.ReadFile(p)
	if len(got) != goodLen {
		t.Fatalf("dirty bytes retained: %d vs %d", len(got), goodLen)
	}
	s2.Close()
}

func TestUncommittedBatchDropped(t *testing.T) {
	d := tempDir(t)
	s := reopen(t, d)
	appendN(t, s, 1)
	_, h1 := s.Tail()
	s.Close()

	p := filepath.Join(d, logFileName)
	raw, _ := os.ReadFile(p)
	// A structurally valid event with no commit frame follows the tail.
	e := &Event{Seq: 2, Key: "ghost", Timestamp: mustParse(t, "2026-09-21T10:00:09Z")}
	body, _ := marshalEvent(e)
	raw = append(raw, encodeFrame(frameEvent, body)...)
	if err := os.WriteFile(p, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	s2 := reopen(t, d)
	if seq, h := s2.Tail(); seq != 1 || h != h1 {
		t.Fatalf("uncommitted event kept: seq=%d", seq)
	}
	if _, err := s2.GetByKey("ghost"); err == nil {
		t.Fatal("ghost event queryable")
	}
	s2.Close()
}

func TestForeignChainCheckpointRejected(t *testing.T) {
	d := tempDir(t)
	s := reopen(t, d)
	appendN(t, s, 1)
	s.Close()

	cp := Checkpoint{ChainID: "some-other-chain", TailSeq: 1, TailHash: "deadbeef", LogSize: 1}
	writeRawCheckpoint(t, d, cp)
	_, err := Open(d)
	le := expectKind(t, err, ErrForeignChain)
	if le.Kind != ErrForeignChain {
		t.Fatalf("got %v", err)
	}
}

func TestMismatchedCheckpointRejected(t *testing.T) {
	d := tempDir(t)
	s := reopen(t, d)
	appendN(t, s, 2)
	s.Close()

	// Same chain id but bogus tail.
	raw, _ := os.ReadFile(filepath.Join(d, logFileName))
	cp := Checkpoint{ChainID: s.chainIDForTest(), TailSeq: 99, TailHash: "00", LogSize: int64(len(raw))}
	writeRawCheckpoint(t, d, cp)
	_, err := Open(d)
	expectKind(t, err, ErrCheckpointInvalid)
}

func TestSequenceJumpRejected(t *testing.T) {
	d := tempDir(t)
	s := reopen(t, d)
	appendN(t, s, 2)
	s.Close()

	p := filepath.Join(d, logFileName)
	raw, _ := os.ReadFile(p)
	// Valid frame, but seq jumps from 2 to 4.
	e := &Event{Seq: 4, Key: "jump", Timestamp: mustParse(t, "2026-09-21T10:00:09Z")}
	body, _ := marshalEvent(e)
	raw = append(raw, encodeFrame(frameEvent, body)...)
	if err := os.WriteFile(p, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Open(d)
	// An uncommitted trailing event would normally be dropped, but the seq
	// jump is a semantic error detected while scanning, so it must surface.
	expectKind(t, err, ErrSequenceJump)
}

func TestRecoveryAllowsFurtherAppends(t *testing.T) {
	d := tempDir(t)
	s := reopen(t, d)
	appendN(t, s, 2)
	s.Close()

	p := filepath.Join(d, logFileName)
	raw, _ := os.ReadFile(p)
	raw = append(raw, 0xBA, 0xDD)
	_ = os.WriteFile(p, raw, 0o644)

	s2 := reopen(t, d)
	r, err := s2.Append(mkBatch("c", "after", ev("k3", "t", "2026-09-21T10:00:02Z", nil)))
	if err != nil {
		t.Fatal(err)
	}
	if !r.Accepted || r.StartSeq != 3 {
		t.Fatalf("append after recovery mis-sequenced: %+v", r)
	}
	v, err := s2.Verify(1)
	if err != nil || !v.OK {
		t.Fatalf("post-recovery verify failed: %+v err=%v", v, err)
	}
	s2.Close()
}

func mustParse(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := ParseTime(s)
	if err != nil {
		t.Fatal(err)
	}
	return tm
}

func writeRawCheckpoint(t *testing.T, d string, cp Checkpoint) {
	t.Helper()
	data, err := json.Marshal(cp)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, cpFileName), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func expectKind(t *testing.T, err error, kind ErrorKind) *Error {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error %s, got nil", kind)
	}
	le, ok := err.(*Error)
	if !ok {
		t.Fatalf("error %v is not a ledger error", err)
	}
	if le.Kind != kind {
		t.Fatalf("want %s got %s (%v)", kind, le.Kind, err)
	}
	return le
}

// chainIDForTest exposes the genesis chain id for white-box tests.
func (s *Store) chainIDForTest() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.chainID
}
