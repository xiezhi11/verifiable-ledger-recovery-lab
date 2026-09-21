package eventlog

import "fmt"

// ValidationError is returned when an event or request fails validation.
// Nothing is written to the log when this error is returned.
type ValidationError struct{ Message string }

func (e *ValidationError) Error() string { return e.Message }

// ConflictError reports an idempotency conflict.
type ConflictError struct{ Message string }

func (e *ConflictError) Error() string { return e.Message }

// CorruptionError describes the first integrity problem found in the log or
// in a checkpoint. Kind is a stable machine-readable discriminator:
// "torn_tail", "dirty_tail", "sequence_gap", "prev_mismatch",
// "digest_mismatch", "canonical_mismatch", "checkpoint_mismatch".
type CorruptionError struct {
	Position      int64  `json:"position"`
	Sequence      int64  `json:"sequence,omitempty"`
	Kind          string `json:"kind"`
	Message       string `json:"message"`
	Expected      string `json:"expected,omitempty"`
	Actual        string `json:"actual,omitempty"`
	OriginalValue string `json:"originalValue,omitempty"`
	CurrentValue  string `json:"currentValue,omitempty"`
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("chain corruption (%s) at offset %d: %s", e.Kind, e.Position, e.Message)
}

// ForeignChainError is returned when a checkpoint or index belongs to a
// different chain than the log being opened.
type ForeignChainError struct{ Message string }

func (e *ForeignChainError) Error() string { return e.Message }

// SequenceGapError reports a jump in record numbering.
type SequenceGapError struct {
	Position int64
	Expected int64
	Actual   int64
}

func (e *SequenceGapError) Error() string {
	return fmt.Sprintf("sequence gap at offset %d: expected %d, got %d", e.Position, e.Expected, e.Actual)
}
