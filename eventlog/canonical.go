package eventlog

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

// Event is a business event supplied by a client.
type Event struct {
	Key  string          `json:"key"`
	Type string          `json:"type"`
	Time string          `json:"time"`
	Data json.RawMessage `json:"data"`
}

type orderedObject [][2]any

// CanonicalEvent returns the deterministic byte representation of an event.
// JSON object key order, insignificant whitespace, empty vs missing objects
// and equivalent RFC3339 timezone representations all produce identical
// bytes, so retries and reordered payloads hash to the same digest.
func CanonicalEvent(in Event) ([]byte, error) {
	if strings.TrimSpace(in.Key) == "" {
		return nil, &ValidationError{Message: "event key is required"}
	}
	if len(in.Data) > 0 && !json.Valid(in.Data) {
		return nil, &ValidationError{Message: "event data is not valid JSON"}
	}
	tm := ""
	if in.Time != "" {
		t, err := time.Parse(time.RFC3339, in.Time)
		if err != nil {
			return nil, &ValidationError{Message: "unsupported event time: " + err.Error()}
		}
		tm = t.UTC().Format(time.RFC3339Nano)
	}
	var data any
	if len(in.Data) == 0 {
		data = orderedObject{}
	} else if err := json.Unmarshal(in.Data, &data); err != nil {
		return nil, &ValidationError{Message: "event data is not valid JSON"}
	} else {
		data = normalize(data)
	}
	return marshalOrdered([]orderedField{
		{"key", in.Key},
		{"type", in.Type},
		{"time", tm},
		{"data", data},
	})
}

type orderedField struct {
	Name  string
	Value any
}

func normalize(v any) any {
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fields := make([]orderedField, 0, len(keys))
		for _, k := range keys {
			fields = append(fields, orderedField{k, normalize(x[k])})
		}
		return orderedObject(fieldsToPairs(fields))
	case []any:
		for i := range x {
			x[i] = normalize(x[i])
		}
		return x
	default:
		return v
	}
}

func fieldsToPairs(fields []orderedField) [][2]any {
	pairs := make([][2]any, len(fields))
	for i, f := range fields {
		pairs[i] = [2]any{f.Name, f.Value}
	}
	return pairs
}

func marshalOrdered(fields []orderedField) ([]byte, error) {
	pairs := fieldsToPairs(fields)
	var b bytes.Buffer
	b.WriteByte('{')
	for i, kv := range pairs {
		if i > 0 {
			b.WriteByte(',')
		}
		k, err := json.Marshal(kv[0])
		if err != nil {
			return nil, err
		}
		v, err := json.Marshal(kv[1])
		if err != nil {
			return nil, err
		}
		b.Write(k)
		b.WriteByte(':')
		b.Write(v)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

func (o orderedObject) MarshalJSON() ([]byte, error) {
	fields := make([]orderedField, len(o))
	for i, kv := range o {
		fields[i] = orderedField{kv[0].(string), kv[1]}
	}
	return marshalOrdered(fields)
}

// EnvelopeDigest chains a canonical event onto the previous digest.
// The genesis record uses a nil previous digest.
func EnvelopeDigest(prev []byte, canonical []byte) []byte {
	h := sha256.New()
	h.Write([]byte("chainlog-envelope-v1\n"))
	h.Write([]byte(fmt.Sprintf("prev=%064x\n", prev)))
	h.Write([]byte("event="))
	h.Write(canonical)
	h.Write([]byte{0})
	return h.Sum(nil)
}

func hexDigest(b []byte) string { return hex.EncodeToString(b) }
