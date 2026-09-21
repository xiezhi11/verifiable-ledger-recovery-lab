package eventlog

import "encoding/json"

// Record is one appended, replayable log entry. Each record is stored as a
// single JSON line in events.log.
type Record struct {
	Sequence       int64           `json:"sequence"`
	Previous       string          `json:"previous"`
	Event          Event           `json:"event"`
	Canonical      json.RawMessage `json:"canonical"`
	Digest         string          `json:"digest"`
	BatchID        string          `json:"batchId"`
	RecordTime     string          `json:"recordTime"`
	IdempotencyKey string          `json:"idempotencyKey,omitempty"`
}

// BatchEvent is one element of an append batch.
type BatchEvent struct {
	IdempotencyKey string `json:"idempotencyKey,omitempty"`
	Event          Event  `json:"event"`
}

// BatchResult is the success boundary of a committed batch: every sequence
// listed here is durably written and fsynced.
type BatchResult struct {
	BatchID   string  `json:"batchId"`
	Status    string  `json:"status"` // "committed" or "duplicate"
	Sequences []int64 `json:"sequences"`
}

// Checkpoint captures the chain tail at a durable point in time.
type Checkpoint struct {
	Version   string `json:"version"`
	ChainID   string `json:"chainId"`
	Sequence  int64  `json:"sequence"`
	Digest    string `json:"digest"`
	Offset    int64  `json:"offset"`
	UpdatedAt string `json:"updatedAt"`
}

// VerifyIssue pinpoints the first chain break found during verification.
type VerifyIssue struct {
	Position int64  `json:"position"`
	Sequence int64  `json:"sequence"`
	Kind     string `json:"kind"`
	Expected string `json:"expected,omitempty"`
	Actual   string `json:"actual,omitempty"`
	Original string `json:"original,omitempty"`
	Current  string `json:"current,omitempty"`
}

// VerifyReport is the result of verifying the chain from a position to the tail.
type VerifyReport struct {
	OK       bool         `json:"ok"`
	Sequence int64        `json:"sequence"`
	Digest   string       `json:"digest"`
	Count    int          `json:"count"`
	First    *VerifyIssue `json:"firstIssue,omitempty"`
}

// Status reports the service state: tail, checkpoint, conflicts, corruption.
type Status struct {
	TailSequence      int64            `json:"tailSequence"`
	TailDigest        string           `json:"tailDigest"`
	LastCheckpoint    *Checkpoint      `json:"lastCheckpoint,omitempty"`
	Conflicts         int64            `json:"conflicts"`
	Corruption        *CorruptionError `json:"corruption,omitempty"`
	RecoverablePrefix int64            `json:"recoverablePrefix"`
}

// RecoveryReport describes what opening a store did to the on-disk log.
type RecoveryReport struct {
	KeptRecords  int              `json:"keptRecords"`
	DroppedBytes int64            `json:"droppedBytes"`
	Truncated    bool             `json:"truncated"`
	IndexRebuilt bool             `json:"indexRebuilt"`
	Corruption   *CorruptionError `json:"corruption,omitempty"`
}

type indexFile struct {
	ChainID  string                `json:"chainId"`
	Sequence int64                 `json:"sequence"`
	Digest   string                `json:"digest"`
	Offset   int64                 `json:"offset"`
	Records  int                   `json:"records"`
	Keys     map[string]indexEntry `json:"keys"`
}

type indexEntry struct {
	Sequence int64 `json:"sequence"`
	Offset   int64 `json:"offset"`
}
