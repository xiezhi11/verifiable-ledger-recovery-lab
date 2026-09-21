package ledger

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Open opens or creates a ledger in dir, recovering any torn/dirty tail. It
// never rewrites confirmed chain history; opening twice yields identical
// state (idempotent recovery).
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, logFileName), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	s := &Store{
		dir:     dir,
		f:       f,
		byKey:   map[string]int64{},
		bySeq:   map[int64]int{},
		batches: map[string]CommitRecord{},
	}
	if err := s.recover(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the log file.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Close()
}

func (s *Store) loadCheckpointLocked() (*Checkpoint, error) {
	data, err := os.ReadFile(filepath.Join(s.dir, cpFileName))
	if errors.Is(err, os.ErrNotExist) || len(data) == 0 {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, ledgerError(ErrCheckpointInvalid, "checkpoint parse: %v", err)
	}
	return &cp, nil
}

func (s *Store) persistCheckpointLocked(cp *Checkpoint) error {
	cp.ChainID = s.chainID
	cp.Created = time.Now().UTC().Format(time.RFC3339Nano)
	tmp := filepath.Join(s.dir, cpFileName+".tmp")
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, cpFileName)); err != nil {
		return err
	}
	s.cp = cp
	return nil
}

// recover implements the open protocol:
//
//  1. read or create the genesis frame and derive the chain anchor;
//  2. load the checkpoint; reject one from another chain;
//  3. scan frames verifying sequence continuity and chained hashes;
//  4. structural damage (torn half-frame, dirty bytes, bad CRC) truncates the
//     uncommitted tail back to the last commit boundary; semantic damage
//     (hash mismatch, sequence jump, bad commit) is a typed error and the log
//     is left untouched;
//  5. rebuild in-memory indexes from the retained committed prefix.
func (s *Store) recover() error {
	st, err := s.f.Stat()
	if err != nil {
		return err
	}
	size := st.Size()

	if size == 0 {
		cid := newChainID()
		body, _ := json.Marshal(genesisBody{ChainID: cid})
		if _, err := s.f.Write(encodeFrame(frameGenesis, body)); err != nil {
			return err
		}
		if err := s.f.Sync(); err != nil {
			return err
		}
		s.chainID = cid
		s.prevHash = genesisAnchor(cid)
		st2, _ := s.f.Stat()
		s.writeOff = st2.Size()
		return s.persistCheckpointLocked(&Checkpoint{
			TailSeq:  0,
			TailHash: encodeHash(s.prevHash),
			LogSize:  s.writeOff,
		})
	}

	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	kind, body, err := readOneFrame(s.f)
	if err != nil {
		return ledgerError(ErrFrameCorrupt, "cannot read genesis: %v", err)
	}
	if kind != frameGenesis {
		return ledgerError(ErrFrameCorrupt, "first frame is not genesis")
	}
	var gen genesisBody
	if err := json.Unmarshal(body, &gen); err != nil || gen.ChainID == "" {
		return ledgerError(ErrFrameCorrupt, "bad genesis body: %v", err)
	}
	s.chainID = gen.ChainID

	cp, err := s.loadCheckpointLocked()
	if err != nil {
		return err
	}
	if cp != nil && cp.ChainID != s.chainID {
		return ledgerError(ErrForeignChain, "checkpoint chain %s != log chain %s", cp.ChainID, s.chainID)
	}

	off, err := s.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	genesisEnd := off

	var (
		events        []storedEvent
		commits       []CommitRecord
		prevHash      = genesisAnchor(s.chainID)
		tailSeq       int64
		committedOff  = genesisEnd
		committedSeq  int64
		committedHash = cloneBytes(prevHash)
		pendingStart  = -1
	)

	for {
		frameStart := off
		k, b, rerr := readOneFrame(s.f)
		if rerr != nil {
			if errors.Is(rerr, io.EOF) && frameStart == size {
				break
			}
			// Torn half-write, dirty bytes, bad magic/CRC, short length:
			// retainable prefix ends at the last commit boundary.
			if errors.Is(rerr, io.EOF) || errors.Is(rerr, io.ErrUnexpectedEOF) || isFrameStructural(rerr) {
				s.damage = &Damage{
					Kind:    ErrFrameCorrupt,
					Offset:  frameStart,
					Message: fmt.Sprintf("unreadable frame at offset %d: %v", frameStart, rerr),
				}
				break
			}
			return ledgerError(ErrFrameCorrupt, "read failure at offset %d: %v", frameStart, rerr)
		}
		curOff, _ := s.f.Seek(0, io.SeekCurrent)

		switch k {
		case frameEvent:
			ev, perr := unmarshalEvent(b)
			if perr != nil {
				return ledgerError(ErrFrameCorrupt, "bad event body at offset %d: %v", frameStart, perr)
			}
			if ev.Seq != tailSeq+1 {
				e := ledgerError(ErrSequenceJump, "want seq %d got %d", tailSeq+1, ev.Seq)
				e.Seq = ev.Seq
				e.Offset = frameStart
				e.Expected = fmt.Sprintf("seq=%d", tailSeq+1)
				e.Actual = fmt.Sprintf("seq=%d", ev.Seq)
				return e
			}
			h := eventHash(prevHash, ev)
			prevHash = h
			tailSeq = ev.Seq
			events = append(events, storedEvent{event: *ev, hash: h, offset: frameStart})
			if pendingStart == -1 {
				pendingStart = len(events) - 1
			}
		case frameCommit:
			var cr CommitRecord
			if jerr := json.Unmarshal(b, &cr); jerr != nil {
				return ledgerError(ErrFrameCorrupt, "bad commit body at offset %d: %v", frameStart, jerr)
			}
			pending := 0
			if pendingStart != -1 {
				pending = len(events) - pendingStart
			}
			if cr.Count != pending {
				e := ledgerError(ErrHashMismatch, "commit count mismatch")
				e.Offset = frameStart
				e.Seq = tailSeq
				e.Expected = fmt.Sprintf("count=%d", pending)
				e.Actual = fmt.Sprintf("count=%d", cr.Count)
				return e
			}
			if cr.TailSeq != tailSeq {
				e := ledgerError(ErrHashMismatch, "commit tail_seq mismatch")
				e.Seq = tailSeq
				e.Offset = frameStart
				e.Expected = fmt.Sprintf("tail_seq=%d", tailSeq)
				e.Actual = fmt.Sprintf("tail_seq=%d", cr.TailSeq)
				return e
			}
			wantHash := encodeHash(prevHash)
			if cr.TailHash != wantHash {
				e := ledgerError(ErrHashMismatch, "commit tail_hash mismatch at seq %d", tailSeq)
				e.Seq = tailSeq
				e.Offset = frameStart
				e.Expected = wantHash
				e.Actual = cr.TailHash
				return e
			}
			committedOff = curOff
			committedSeq = tailSeq
			committedHash = cloneBytes(prevHash)
			commits = append(commits, cr)
			pendingStart = -1
		default:
			s.damage = &Damage{
				Kind:    ErrFrameCorrupt,
				Offset:  frameStart,
				Message: fmt.Sprintf("unknown frame kind 0x%02x at offset %d", k, frameStart),
			}
		}
		off = curOff
		if s.damage != nil {
			break
		}
	}

	// Checkpoint must agree with the committed prefix.
	if cp != nil {
		if cp.TailSeq != committedSeq || cp.TailHash != encodeHash(committedHash) || cp.LogSize != committedOff {
			e := ledgerError(ErrCheckpointInvalid,
				"checkpoint does not match retained log prefix")
			e.Expected = fmt.Sprintf("seq=%d,size=%d,hash=%s", committedSeq, committedOff, encodeHash(committedHash))
			e.Actual = fmt.Sprintf("seq=%d,size=%d,hash=%s", cp.TailSeq, cp.LogSize, cp.TailHash)
			return e
		}
		s.cp = cp
	} else {
		s.cp = &Checkpoint{
			TailSeq:  committedSeq,
			TailHash: encodeHash(committedHash),
			LogSize:  committedOff,
		}
	}

	// Drop uncommitted/damaged tail and fsync.
	if committedOff != size {
		if err := s.f.Truncate(committedOff); err != nil {
			return err
		}
		if err := s.f.Sync(); err != nil {
			return err
		}
	}

	// Rebuild indexes from the retained committed prefix.
	keep := int(committedSeq)
	if keep > len(events) {
		keep = len(events)
	}
	runHash := genesisAnchor(s.chainID)
	pos := genesisEnd
	for i := 0; i < keep; i++ {
		ev := events[i].event
		h := eventHash(runHash, &ev)
		runHash = h
		eb, _ := marshalEvent(&ev)
		s.events = append(s.events, storedEvent{event: ev, hash: h, offset: pos})
		s.byKey[ev.Key] = ev.Seq
		s.bySeq[ev.Seq] = len(s.events) - 1
		pos += int64(frameHeaderLen + len(eb) + frameCRCLen)
	}
	for _, cr := range commits {
		s.batches[batchKey(cr.ClientID, cr.BatchID)] = cr
	}
	s.prevHash = committedHash
	s.tailSeq = committedSeq
	s.writeOff = committedOff

	// Persist/refresh checkpoint so the next open and this one agree even when
	// there was no checkpoint file.
	if cp == nil || committedOff != cp.LogSize {
		if err := s.persistCheckpointLocked(&Checkpoint{
			TailSeq:  committedSeq,
			TailHash: encodeHash(committedHash),
			LogSize:  committedOff,
		}); err != nil {
			return err
		}
	}
	return nil
}
