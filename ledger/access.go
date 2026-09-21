package ledger

// Dir returns the on-disk directory of the ledger.
func (s *Store) Dir() string { return s.dir }

// ChainID returns the chain identifier from the genesis frame.
func (s *Store) ChainID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.chainID
}

// Tail returns the current tail sequence and hash hex.
func (s *Store) Tail() (int64, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tailSeq, encodeHash(s.prevHash)
}
