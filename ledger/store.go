package ledger

import (
	"crypto/sha256"
	"errors"
	"os"
	"sync"
)
import (
	"crypto/rand"
	"encoding/hex"
)

// CommitRecord is a batch boundary frame.
type CommitRecord struct {
	ClientID string `json:"client_id"`
	BatchID  string `json:"batch_id"`
	Count    int    `json:"count"`
	TailSeq  int64  `json:"tail_seq"`
	TailHash string `json:"tail_hash"`
}

// Checkpoint is a durable marker of a confirmed chain tail.
type Checkpoint struct {
	ChainID  string `json:"chain_id"`
	TailSeq  int64  `json:"tail_seq"`
	TailHash string `json:"tail_hash"`
	LogSize  int64  `json:"log_size"` // offset after the commit frame
	BatchID  string `json:"batch_id,omitempty"`
	Created  string `json:"created,omitempty"`
}

// Damage describes the first broken spot found during recovery/verification.
type Damage struct {
	Kind     ErrorKind `json:"kind"`
	Message  string    `json:"message"`
	Seq      int64     `json:"seq,omitempty"`
	Offset   int64     `json:"offset,omitempty"`
	Expected string    `json:"expected,omitempty"`
	Actual   string    `json:"actual,omitempty"`
}

// Status is the current service/store view.
type Status struct {
	ChainID        string      `json:"chain_id"`
	TailSeq        int64       `json:"tail_seq"`
	TailHash       string      `json:"tail_hash"`
	LastCheckpoint *Checkpoint `json:"last_checkpoint"`
	Conflicts      int64       `json:"conflicts"`
	Damage         *Damage     `json:"damage,omitempty"`
}

// AppendResult reports a batch append outcome. Accepted==false means a
// conflicting retry; Existing describes the prior identical receipt so a
// retried client can tell received from not received.
type AppendResult struct {
	Accepted bool          `json:"accepted"`
	BatchID  string        `json:"batch_id"`
	StartSeq int64         `json:"start_seq"`
	EndSeq   int64         `json:"end_seq"`
	TailHash string        `json:"tail_hash"`
	Existing *CommitRecord `json:"existing,omitempty"`
}

// Batch is a client append request. ClientID+BatchID is the idempotency key.
type Batch struct {
	ClientID string       `json:"client_id"`
	BatchID  string       `json:"batch_id"`
	Events   []EventInput `json:"events"`
}

// EventInput is one user supplied event; the store assigns sequence numbers.
type EventInput struct {
	Key       string            `json:"key"`
	Type      string            `json:"type"`
	Timestamp string            `json:"timestamp"`
	Payload   map[string]string `json:"payload"`
}

type storedEvent struct {
	event  Event
	hash   []byte
	offset int64
}

// Store is a concurrency-safe ledger handle.
type Store struct {
	mu sync.Mutex

	dir     string
	f       *os.File
	chainID string

	events   []storedEvent
	byKey    map[string]int64 // business key -> seq
	bySeq    map[int64]int    // seq -> index
	batches  map[string]CommitRecord
	prevHash []byte
	tailSeq  int64
	writeOff int64
	cp       *Checkpoint
	conflict int64
	damage   *Damage
}

const (
	logFileName = "log.dat"
	cpFileName  = "checkpoint.dat"
)

func newChainID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// genesisAnchor is the hash event number one chains from.
func genesisAnchor(chainID string) []byte {
	sum := sha256.Sum256([]byte("genesis\x00" + chainID))
	return sum[:]
}

type genesisBody struct {
	ChainID string `json:"chain_id"`
}

func isFrameStructural(err error) bool {
	var fre *frameReadError
	return errors.As(err, &fre)
}

func batchKey(client, batch string) string { return client + "\x00" + batch }

func cloneBytes(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
