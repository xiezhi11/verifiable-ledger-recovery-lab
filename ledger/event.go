// Package ledger implements an append-only, hash-chained event log with
// idempotent appends, batch commit boundaries, checkpoints and crash recovery.
//
// On-disk layout (directory):
//
//	log.dat        length-prefixed, CRC32-C protected frames (event + commit)
//	checkpoint.dat latest confirmed chain-tail checkpoint (atomically renamed)
//
// The very first frame of every log is a genesis frame carrying the chain id.
// Checkpoints may never belong to a different chain: on open, a checkpoint
// whose chain id differs from the genesis frame is a fatal, typed error.
package ledger

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Event is a single business event. Fields are user supplied. The ledger is
// responsible for assigning the sequence number and for producing canonical
// bytes that are insensitive to map ordering, missing/empty fields and the
// concrete textual time format used by the client.
type Event struct {
	Seq           int64             `json:"seq"`
	Key           string            `json:"key"`
	Type          string            `json:"type,omitempty"`
	Timestamp     time.Time         `json:"-"`
	Payload       map[string]string `json:"payload,omitempty"`
	TimestampText string            `json:"timestamp,omitempty"`
}

// CanonicalBytes renders an event as deterministic bytes.
//
// Rules shared by append, verification and recovery:
//
//   - payload keys are sorted lexicographically;
//   - empty payload values are rendered as empty strings (never dropped);
//   - the timestamp is normalised to UTC RFC3339 with nanosecond precision;
//   - seq, key and type are always present so that a later empty/absent field
//     cannot make two distinct events hash identically.
func CanonicalBytes(e *Event) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "seq=%d\nkey=%s\ntype=%s\ntime=%s\n", e.Seq, e.Key, e.Type, e.Timestamp.UTC().Format(time.RFC3339Nano))
	keys := make([]string, 0, len(e.Payload))
	for k := range e.Payload {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b.WriteString("payload:\n")
	for _, k := range keys {
		fmt.Fprintf(&b, "  %s=%s\n", k, e.Payload[k])
	}
	return b.Bytes()
}

// Hash renders the chain hash contributed by an event. The domain prefix keeps
// event hashes distinct from commit/genesis frames even if their bodies match.
func eventHash(prevHash []byte, e *Event) []byte {
	h := sha256.New()
	h.Write([]byte("event\x00"))
	h.Write(prevHash)
	h.Write(CanonicalBytes(e))
	sum := h.Sum(nil)
	return sum
}

func encodeHash(h []byte) string { return hex.EncodeToString(h) }

// ParseTime accepts a variety of common timestamp spellings and always yields
// a single time.Time, so different time formats cannot split dedup results.
func ParseTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
		time.RFC1123,
	}
	var lastErr error
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			return t, nil
		} else {
			lastErr = err
		}
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	} else {
		lastErr = err
	}
	return time.Time{}, fmt.Errorf("unrecognised timestamp %q: %w", s, lastErr)
}

// jsonEvent is the wire/disk representation of an event.
type jsonEvent struct {
	Seq       int64             `json:"seq"`
	Key       string            `json:"key"`
	Type      string            `json:"type"`
	Timestamp string            `json:"timestamp"`
	Payload   map[string]string `json:"payload"`
}

func (e *Event) toDisk() *jsonEvent {
	return &jsonEvent{
		Seq:       e.Seq,
		Key:       e.Key,
		Type:      e.Type,
		Timestamp: e.Timestamp.UTC().Format(time.RFC3339Nano),
		Payload:   e.Payload,
	}
}

func (d *jsonEvent) toEvent() (*Event, error) {
	t, err := ParseTime(d.Timestamp)
	if err != nil {
		return nil, err
	}
	return &Event{
		Seq:           d.Seq,
		Key:           d.Key,
		Type:          d.Type,
		Timestamp:     t,
		Payload:       d.Payload,
		TimestampText: d.Timestamp,
	}, nil
}

func marshalEvent(e *Event) ([]byte, error) {
	return json.Marshal(e.toDisk())
}

func unmarshalEvent(data []byte) (*Event, error) {
	var d jsonEvent
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, err
	}
	return d.toEvent()
}
