# compaction-check

Consumes a Kafka-compacted topic end to end and proves whether it has
**fully compacted**, instead of inferring that from waiting a while and
hoping.

## What it does

A fully compacted log has *at most one* live record per key — that's the
actual definition of "compaction finished." So this tool:

1. Snapshots the topic's current log-start-offset and high watermark via
   the Kafka admin API.
2. Consumes every record in that range.
3. Counts total live records (including tombstones) and distinct keys.
4. Reports `FULLY COMPACTED` if they're equal, or the exact number of
   stale/superseded records still sitting in the log if they're not.

It also prints a breakdown by `keytype` (read from each key's JSON
`keytype` field, if present — this was built for Kafka Schema Registry's
internal `_schemas` topic, where that convention holds, but the record/key
counting itself works for any compacted topic).

This replaces doing the same check ad hoc with `rpk topic consume` piped
through `jq`/Python — same logic, now a reusable binary with no `rpk`
dependency (it talks the Kafka protocol directly via
[`franz-go`](https://github.com/twmb/franz-go)).

**Important assumption:** the topic is idle (no concurrent writes) while
this runs. It takes one high-watermark snapshot up front and consumes up
to it; it does not keep polling for new records. That's the right
assumption for checking convergence on a static dataset, not for a topic
under live write traffic.

## Requirements

- Go 1.23+ to build.
- Network access to the Kafka broker(s) you're pointing it at.
- Read access to the target topic (plus whatever SASL credentials that
  requires, if any).

## Building

```bash
go build -o compaction-check .
```

## Running

```bash
./compaction-check -brokers localhost:9092 -topic _schemas
```

Flags:

| Flag | Default | Meaning |
|---|---|---|
| `-brokers` | `localhost:9092` | Comma-separated broker addresses |
| `-topic` | `_schemas` | Topic to check |
| `-tls` | `false` | Connect over TLS |
| `-tls-ca` | — | Path to a CA certificate (PEM) to trust, in addition to system roots |
| `-tls-cert` / `-tls-key` | — | Client certificate/key (PEM), for mTLS |
| `-tls-insecure-skip-verify` | `false` | Skip server certificate verification (test/dev only — never for a real cluster) |
| `-sasl-mechanism` | — | `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512`; unset means no SASL |
| `-sasl-user` / `-sasl-pass` | — | SASL credentials (required if `-sasl-mechanism` is set) |

### Examples

Plaintext, no auth (the common case against a local test cluster):

```bash
./compaction-check -brokers localhost:29094 -topic _schemas
```

TLS + SCRAM-SHA-256 against a secured cluster:

```bash
./compaction-check -brokers broker.example.com:9092 -topic _schemas \
  -tls -tls-ca /path/to/ca.pem \
  -sasl-mechanism SCRAM-SHA-256 -sasl-user myuser -sasl-pass mypass
```

## Output

```
scanning topic "_schemas", target high watermarks: map[0:2241834]

records (live, including tombstones): 20834
tombstones:                            2025
unique keys:                           20834
keytype breakdown:
  CONFIG               2268
  DELETE_SUBJECT       136
  MODE                 2
  NOOP                 1
  SCHEMA               18427

FULLY COMPACTED: every live record has a unique key.
```

Exit code is `0` when fully compacted, `1` otherwise (so it's usable as a
CI-style gate, not just an interactive report).
