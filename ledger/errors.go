package ledger

import "fmt"

// ErrorKind enumerates the typed failures callers can branch on.
type ErrorKind string

const (
	ErrHashMismatch      ErrorKind = "hash_mismatch"      // a frame's content no longer hashes to its chained hash
	ErrSequenceJump      ErrorKind = "sequence_jump"      // event sequence numbers are not contiguous
	ErrForeignChain      ErrorKind = "foreign_chain"      // checkpoint belongs to another chain
	ErrCheckpointInvalid ErrorKind = "checkpoint_invalid" // checkpoint cannot be located or verified
	ErrFrameCorrupt      ErrorKind = "frame_corrupt"      // CRC / magic / length damage inside the retained region
	ErrNotFound          ErrorKind = "not_found"          // no such sequence or business key
	ErrInvalidEvent      ErrorKind = "invalid_event"      // event failed validation/canonicalisation
	ErrBatchConflict     ErrorKind = "batch_conflict"     // same batch id retried with a different body
	ErrKeyConflict       ErrorKind = "key_conflict"       // business key already exists with different content
)

// Error is the typed ledger error. Pos fields pinpoint the first break:
// sequence number, byte offset and, for hash failures, the expected and actual
// values.
type Error struct {
	Kind     ErrorKind `json:"kind"`
	Message  string    `json:"message"`
	Seq      int64     `json:"seq,omitempty"`
	Offset   int64     `json:"offset,omitempty"`
	Expected string    `json:"expected,omitempty"`
	Actual   string    `json:"actual,omitempty"`
}

func (e *Error) Error() string {
	s := string(e.Kind)
	if e.Message != "" {
		s += ": " + e.Message
	}
	if e.Seq != 0 {
		s += fmt.Sprintf(" (seq=%d)", e.Seq)
	}
	if e.Offset != 0 {
		s += fmt.Sprintf(" (offset=%d)", e.Offset)
	}
	if e.Expected != "" || e.Actual != "" {
		s += fmt.Sprintf(" expected=%s actual=%s", e.Expected, e.Actual)
	}
	return s
}

func ledgerError(kind ErrorKind, format string, args ...any) *Error {
	return &Error{Kind: kind, Message: fmt.Sprintf(format, args...)}
}
