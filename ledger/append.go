package ledger

import "encoding/json"

func validateInput(in *EventInput) (*Event, error) {
	if in.Key == "" {
		return nil, ledgerError(ErrInvalidEvent, "event key is empty")
	}
	t, err := ParseTime(in.Timestamp)
	if err != nil {
		return nil, ledgerError(ErrInvalidEvent, "event %s: %v", in.Key, err)
	}
	payload := in.Payload
	if payload == nil {
		payload = map[string]string{}
	}
	return &Event{
		Key:           in.Key,
		Type:          in.Type,
		Timestamp:     t,
		Payload:       payload,
		TimestampText: in.Timestamp,
	}, nil
}

// Append writes one batch atomically: either all event frames plus the commit
// frame are durable (Accepted=true) or nothing was appended. It is safe for
// concurrent writers; identical retries (same client+batch id and body) return
// the original receipt instead of appending twice.
func (s *Store) Append(b *Batch) (*AppendResult, error) {
	if b == nil || b.ClientID == "" || b.BatchID == "" {
		return nil, ledgerError(ErrInvalidEvent, "client_id and batch_id are required")
	}
	if len(b.Events) == 0 {
		return nil, ledgerError(ErrInvalidEvent, "batch contains no events")
	}
	events := make([]*Event, len(b.Events))
	for i := range b.Events {
		e, err := validateInput(&b.Events[i])
		if err != nil {
			return nil, err
		}
		events[i] = e
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	idk := batchKey(b.ClientID, b.BatchID)
	if existing, ok := s.batches[idk]; ok {
		// Idempotent replay: the stored span must canonicalise to exactly the
		// same bytes as the retried body. Same id, different body is a
		// conflict rather than a second append.
		startSeq := existing.TailSeq - int64(existing.Count) + 1
		if existing.Count == len(events) {
			idx, found := s.bySeq[startSeq]
			if found && idx+len(events) <= len(s.events) {
				same := true
				for i, e := range events {
					stored := s.events[idx+i]
					t, _ := ParseTime(e.TimestampText)
					e.Seq = startSeq + int64(i)
					e.Timestamp = t
					if string(CanonicalBytes(e)) != string(CanonicalBytes(&stored.event)) {
						same = false
						break
					}
				}
				if same {
					return &AppendResult{
						Accepted: false,
						BatchID:  b.BatchID,
						StartSeq: startSeq,
						EndSeq:   existing.TailSeq,
						TailHash: existing.TailHash,
						Existing: &existing,
					}, nil
				}
			}
		}
		s.conflict++
		return nil, ledgerError(ErrBatchConflict, "batch %s/%s already committed with a different body", b.ClientID, b.BatchID)
	}

	// Reject duplicate business keys across the chain and within the batch.
	seen := map[string]bool{}
	for _, e := range events {
		if _, ok := s.byKey[e.Key]; ok {
			s.conflict++
			return nil, ledgerError(ErrKeyConflict, "business key %q already exists", e.Key)
		}
		if seen[e.Key] {
			s.conflict++
			return nil, ledgerError(ErrKeyConflict, "business key %q repeated within batch", e.Key)
		}
		seen[e.Key] = true
	}

	startSeq := s.tailSeq + 1
	var buf []byte
	prev := cloneBytes(s.prevHash)
	for i, e := range events {
		e.Seq = startSeq + int64(i)
		prev = eventHash(prev, e)
		body, err := marshalEvent(e)
		if err != nil {
			return nil, err
		}
		buf = append(buf, encodeFrame(frameEvent, body)...)
	}
	cr := CommitRecord{
		ClientID: b.ClientID,
		BatchID:  b.BatchID,
		Count:    len(events),
		TailSeq:  startSeq + int64(len(events)) - 1,
		TailHash: encodeHash(prev),
	}
	cbody, err := json.Marshal(cr)
	if err != nil {
		return nil, err
	}
	buf = append(buf, encodeFrame(frameCommit, cbody)...)

	// One positional write plus fsync is the success boundary: after this
	// returns, the whole batch (including its commit frame) is durable.
	if _, err := s.f.WriteAt(buf, s.writeOff); err != nil {
		return nil, err
	}
	if err := s.f.Sync(); err != nil {
		return nil, err
	}

	// Durable commit frame: update in-memory indexes, then checkpoint. A crash
	// between sync and checkpoint is still recoverable from the log itself.
	pos := s.writeOff
	for i, e := range events {
		e.Seq = startSeq + int64(i)
		s.prevHash = eventHash(s.prevHash, e)
		s.tailSeq = e.Seq
		eb, _ := marshalEvent(e)
		s.events = append(s.events, storedEvent{event: *e, hash: s.prevHash, offset: pos})
		s.byKey[e.Key] = e.Seq
		s.bySeq[e.Seq] = len(s.events) - 1
		pos += int64(frameHeaderLen + len(eb) + frameCRCLen)
	}
	pos += int64(frameHeaderLen + len(cbody) + frameCRCLen)
	s.writeOff = pos
	s.batches[idk] = cr

	// Best-effort checkpoint: if it cannot be persisted, the commit in the
	// log is still the source of truth and recovery re-derives the tail.
	_ = s.persistCheckpointLocked(&Checkpoint{
		TailSeq:  cr.TailSeq,
		TailHash: cr.TailHash,
		LogSize:  s.writeOff,
		BatchID:  cr.BatchID,
	})

	return &AppendResult{
		Accepted: true,
		BatchID:  b.BatchID,
		StartSeq: startSeq,
		EndSeq:   cr.TailSeq,
		TailHash: cr.TailHash,
	}, nil
}
