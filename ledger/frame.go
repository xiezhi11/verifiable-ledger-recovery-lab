package ledger

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// Frame kinds.
const (
	frameGenesis byte = 'G' // body: chain id json
	frameEvent   byte = 'E' // body: jsonEvent
	frameCommit  byte = 'C' // body: commitRecord json
)

// Frame wire layout, big endian:
//
//	magic   2 bytes = 'L' 'D'
//	kind    1 byte
//	length  uint32 (body length)
//	body     length bytes
//	crc32   uint32 (IEEE, over magic|kind|length|body)
//
// Fixed overhead is 11 bytes. A frame is atomic from a reader's point of
// view: torn tails, stray garbage or length overruns simply end the readable
// prefix at the last good frame boundary.
const frameHeaderLen = 7
const frameCRCLen = 4

var frameMagic = [2]byte{'L', 'D'}

var crcTable = crc32.MakeTable(crc32.IEEE)

func encodeFrame(kind byte, body []byte) []byte {
	out := make([]byte, frameHeaderLen+len(body)+frameCRCLen)
	out[0] = frameMagic[0]
	out[1] = frameMagic[1]
	out[2] = kind
	binary.BigEndian.PutUint32(out[3:7], uint32(len(body)))
	copy(out[7:], body)
	crc := crc32.Checksum(out[:frameHeaderLen+len(body)], crcTable)
	binary.BigEndian.PutUint32(out[frameHeaderLen+len(body):], crc)
	return out
}

// frameReadError marks structural damage (bad magic, length overrun, CRC).
type frameReadError struct{ msg string }

func (e *frameReadError) Error() string { return e.msg }

// readOneFrame reads exactly one frame starting at r's current position.
// io.ErrUnexpectedEOF means a torn/short frame at the tail.
func readOneFrame(r io.Reader) (kind byte, body []byte, err error) {
	header := make([]byte, frameHeaderLen)
	if _, err = io.ReadFull(r, header); err != nil {
		return 0, nil, err
	}
	if header[0] != frameMagic[0] || header[1] != frameMagic[1] {
		return 0, nil, &frameReadError{msg: fmt.Sprintf("bad frame magic %02x %02x", header[0], header[1])}
	}
	kind = header[2]
	n := binary.BigEndian.Uint32(header[3:7])
	body = make([]byte, n)
	if _, err = io.ReadFull(r, body); err != nil {
		return 0, nil, err
	}
	crcBytes := make([]byte, frameCRCLen)
	if _, err = io.ReadFull(r, crcBytes); err != nil {
		return 0, nil, err
	}
	var covered [frameHeaderLen]byte
	copy(covered[:], header)
	want := crc32.Checksum(append(covered[:], body...), crcTable)
	got := binary.BigEndian.Uint32(crcBytes)
	if want != got {
		return 0, nil, &frameReadError{msg: "frame crc mismatch"}
	}
	return kind, body, nil
}

// errShortFrame reports torn trailing data during recovery scanning.
var errShortFrame = errors.New("incomplete frame at tail")
