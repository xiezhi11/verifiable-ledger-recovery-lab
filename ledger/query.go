package ledger

import "fmt"

// EventView is the externally returned event, including chain metadata.
type EventView struct {
	Seq       int64             `json:"seq"`
	Key       string            `json:"key"`
	Type      string            `json:"type"`
	Timestamp string            `json:"timestamp"`
	Payload   map[string]string `json:"payload"`
	Hash      string            `json:"hash"`
}

func (s *Store) viewOf(st storedEvent) EventView {
	return EventView{
		Seq:       st.event.Seq,
		Key:       st.event.Key,
		Type:      st.event.Type,
		Timestamp: st.event.Timestamp.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
		Payload:   st.event.Payload,
		Hash:      encodeHash(st.hash),
	}
}

// GetBySeq returns one event by sequence number.
func (s *Store) GetBySeq(seq int64) (*EventView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx, ok := s.bySeq[seq]
	if !ok || idx < 0 || idx >= len(s.events) {
		return nil, ledgerError(ErrNotFound, "no event with seq %d", seq)
	}
	v := s.viewOf(s.events[idx])
	return &v, nil
}

// GetByKey returns one event by business key.
func (s *Store) GetByKey(key string) (*EventView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seq, ok := s.byKey[key]
	if !ok {
		return nil, ledgerError(ErrNotFound, "no event with key %q", key)
	}
	v := s.viewOf(s.events[s.bySeq[seq]])
	return &v, nil
}

// ChainStatus reports current tail, checkpoint, conflict count and any damage
// recorded during the last open/recovery.
func (s *Store) ChainStatus() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := Status{
		ChainID:        s.chainID,
		TailSeq:        s.tailSeq,
		TailHash:       encodeHash(s.prevHash),
		LastCheckpoint: s.cp,
		Conflicts:      s.conflict,
		Damage:         s.damage,
	}
	return st
}

// VerifyResult is the outcome of verifying a chain range from an arbitrary
// starting position to the current tail.
type VerifyResult struct {
	FromSeq  int64   `json:"from_seq"`
	ToSeq    int64   `json:"to_seq"`
	OK       bool    `json:"ok"`
	TailHash string  `json:"tail_hash"`
	Break    *Damage `json:"break,omitempty"`
}

// Verify re-chains events from fromSeq to the current tail using the same
// canonical/order rules as append and recovery. fromSeq<=1 starts at the
// genesis-anchored first event; a later start is chained from the stored
// predecessor hash. The first break (gap or changed content) is reported with
// the original stored value and the recomputed present value.
func (s *Store) Verify(fromSeq int64) (*VerifyResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.events) == 0 {
		return &VerifyResult{FromSeq: 1, ToSeq: 0, OK: true, TailHash: encodeHash(s.prevHash)}, nil
	}
	if fromSeq <= 1 {
		fromSeq = 1
	}
	startIdx, ok := s.bySeq[fromSeq]
	if !ok {
		return nil, ledgerError(ErrNotFound, "cannot start verify at seq %d", fromSeq)
	}

	var prev []byte
	if fromSeq == 1 {
		prev = genesisAnchor(s.chainID)
	} else {
		prev = cloneBytes(s.events[startIdx-1].hash)
	}

	res := &VerifyResult{FromSeq: fromSeq, ToSeq: s.tailSeq}
	var prevSeq int64 = fromSeq - 1
	for i := startIdx; i < len(s.events); i++ {
		st := s.events[i]
		if st.event.Seq != prevSeq+1 {
			res.Break = &Damage{
				Kind:     ErrSequenceJump,
				Seq:      st.event.Seq,
				Offset:   st.offset,
				Expected: fmt.Sprintf("seq=%d", prevSeq+1),
				Actual:   fmt.Sprintf("seq=%d", st.event.Seq),
				Message:  "sequence gap during verification",
			}
			res.OK = false
			return res, nil
		}
		want := eventHash(prev, &st.event)
		if encodeHash(want) != encodeHash(st.hash) {
			res.Break = &Damage{
				Kind:     ErrHashMismatch,
				Seq:      st.event.Seq,
				Offset:   st.offset,
				Expected: encodeHash(st.hash), // original chained value
				Actual:   encodeHash(want),    // value implied by present bytes
				Message:  "stored bytes no longer match the chain",
			}
			res.OK = false
			return res, nil
		}
		prev = want
		prevSeq = st.event.Seq
	}
	res.OK = true
	res.TailHash = encodeHash(prev)
	return res, nil
}
