# Verifiable Ledger Recovery Lab (Go, stdlib only)

An append-only, hash-chained event log with idempotent batch appends,
checkpoints, crash recovery and a concurrency-safe HTTP service. The project
uses only the Go standard library.

## What it does

- **Append and summary chain.** Every business event is a replayable append
  record. The store assigns contiguous sequence numbers and produces canonical
  bytes, a per-event SHA-256 chain hash and a batch commit frame. Canonical
  bytes sort payload keys, keep empty values, and normalise timestamps to UTC
  RFC3339 nanoseconds, so missing fields, map order and different time formats
  hash to one unique result.
- **Idempotency.** Appends are batches keyed by `client_id` + `batch_id`.
  Retrying an identical batch returns the original receipt
  (`200`, `accepted:false`, prior commit attached) without appending twice;
  the same id with a different body is a typed conflict. Business keys are
  also unique chain-wide.
- **Queries.** Fetch a single record by sequence number or business key, or
  verify from any position to the current tail. Verification reports the first
  break: sequence number, byte offset, original chained value and the value
  recomputed from present bytes.
- **Snapshots and recovery.** Each durable batch writes a checkpoint carrying
  the chain id, tail sequence, tail hash and exact log offset. On reopen the
  shared scan distinguishes the complete committable prefix from a discarded
  tail: half-written frames, dirty trailing bytes and committed-but-indexless
  state all resolve to the last commit boundary, while hash mismatches,
  sequence jumps and foreign-chain checkpoints return typed errors without
  rewriting history. Recovery is idempotent: reopening twice yields identical
  state, and appends continue with the next sequence number.
- **Concurrency.** Writers, readers and verifiers share one service. Readers
  observe only committed records; a slow verification excludes new appends for
  its duration, while status reads stay available. `/status` reports current
  tail, latest checkpoint, conflict count and the last damage location.

## On-disk layout

Inside the data directory (default `ledgerdata`):

- `log.dat` - length-prefixed frames with CRC32 protection:
  magic `LD`, one-byte kind (`G` genesis, `E` event, `C` commit), uint32 body
  length, JSON body, uint32 CRC32 (IEEE). The first frame is the genesis frame
  carrying the chain id; events chain from `SHA256("genesis" NUL chainID)`.
- `checkpoint.dat` - JSON checkpoint, written to a temp file and atomically
  renamed. A checkpoint whose chain id, tail hash, sequence or offset does not
  match the retained prefix is a `checkpoint_invalid` error; a different chain
  id is `foreign_chain`.

A batch is one positional write followed by `fsync`; its trailing commit
  frame is the success boundary. Anything after the last commit frame on disk
  (torn frame, garbage bytes, events without a commit) is truncated once on
  recovery.

## HTTP API

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/events` | Append a batch; `201` accepted, `200` identical retry, `409` conflict |
| GET | `/events/seq/{n}` | One record by sequence number |
| GET | `/events/key/{k}` | One record by business key |
| POST/GET | `/verify?from=1` | Verify a range to the tail; first break is reported |
| POST | `/recover` | Reopen and revalidate the on-disk prefix |
| GET | `/status` | Tail, checkpoint, conflicts, damage |

Append body:

```json
{
  "client_id": "worker-7",
  "batch_id": "2026-09-21-0001",
  "events": [
    {
      "key": "order-1001",
      "type": "created",
      "timestamp": "2026-09-21T10:00:00Z",
      "payload": {"amount": "42.50", "note": ""}
    }
  ]
}
```

Client retry rule: after a disconnect, repeat the same `client_id`/`batch_id`
and body. A `201` means newly received; a `200` with `accepted:false` and an
`existing` commit means it was already durable before the disconnect.

## Run on a clean machine

Requires only Go 1.21+ (no third-party modules, no code generation):

```sh
# all automated checks: normal flow, retries, interleaving,
# truncation, dirty tails, restart recovery and typed errors
go test ./... -count=1

# race detector for the concurrency tests
go test -race ./... -count=1

go vet ./...

# start the service
go run ./cmd/ledgerd -addr :8080 -dir ledgerdata
```

Quick smoke test while it runs:

```sh
curl -sS -XPOST localhost:8080/events -d '{
  "client_id":"demo","batch_id":"b1",
  "events":[{"key":"k1","type":"created",
    "timestamp":"2026-09-21 10:00:00","payload":{"a":""}}]}'
curl -sS localhost:8080/events/key/k1
curl -sS -XPOST localhost:8080/verify
curl -sS localhost:8080/status
```

## Tests

- `ledger/ledger_test.go` - canonical ordering/empty-field/time-format
  stability, appends, key and seq lookup, identical retry, batch/key conflicts,
  and tampered-bytes detection with expected/actual hash values.
- `ledger/recovery_test.go` - torn half frames, dirty trailing bytes,
  uncommitted events, foreign and mismatched checkpoints, sequence jumps,
  double reopen idempotence and append after recovery.
- `server/server_test.go` - HTTP happy path, duplicate retry semantics, 8-way
  interleaved writers with concurrent readers, same-batch fan-out (exactly one
  create), dirty-tail restart, range verification, explicit recovery and bad
  requests.
