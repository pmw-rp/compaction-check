// compaction-check consumes a compacted topic (default: _schemas) end to
// end and counts records vs. distinct keys. A fully compacted log has at
// most one live record per key, so records == unique keys is the actual
// proof; anything less means stale/superseded records are still present.
//
// Assumes the topic is idle (no concurrent writes) - it snapshots the
// current high watermark once at startup via the admin API and stops
// consuming each partition once it reaches that offset. That's the right
// assumption for checking compaction convergence on a static/replayed
// dataset; on a live topic, re-run it or extend it to keep polling.
//
// Usage:
//
//	compaction-check -brokers localhost:9092 -topic _schemas
//	compaction-check -brokers broker:9092 -tls -sasl-mechanism SCRAM-SHA-256 -sasl-user U -sasl-pass P
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	var (
		brokers = flag.String("brokers", "localhost:9092", "comma-separated Kafka broker addresses")
		topic   = flag.String("topic", "_schemas", "topic to check")

		useTLS        = flag.Bool("tls", false, "connect to brokers over TLS")
		tlsCA         = flag.String("tls-ca", "", "path to a CA certificate (PEM) to trust, in addition to system roots")
		tlsCert       = flag.String("tls-cert", "", "path to a client certificate (PEM), for mTLS")
		tlsKey        = flag.String("tls-key", "", "path to a client private key (PEM), for mTLS")
		tlsSkipVerify = flag.Bool("tls-insecure-skip-verify", false, "skip server certificate verification (test/dev only)")

		saslMechanism = flag.String("sasl-mechanism", "", "SASL mechanism: PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512 (unset = no SASL)")
		saslUser      = flag.String("sasl-user", "", "SASL username")
		saslPass      = flag.String("sasl-pass", "", "SASL password")
	)
	flag.Parse()

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumeTopics(*topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}

	if *useTLS || *tlsCA != "" || *tlsCert != "" || *tlsKey != "" {
		tlsCfg, err := buildTLSConfig(*tlsCA, *tlsCert, *tlsKey, *tlsSkipVerify)
		if err != nil {
			log.Fatalf("tls: %v", err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	if *saslMechanism != "" {
		mech, err := buildSASLMechanism(*saslMechanism, *saslUser, *saslPass)
		if err != nil {
			log.Fatalf("sasl: %v", err)
		}
		opts = append(opts, kgo.SASL(mech))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer cl.Close()

	ctx := context.Background()
	adm := kadm.NewClient(cl)

	startOffsets, err := adm.ListStartOffsets(ctx, *topic)
	if err != nil {
		log.Fatalf("list start offsets: %v", err)
	}
	endOffsets, err := adm.ListEndOffsets(ctx, *topic)
	if err != nil {
		log.Fatalf("list end offsets: %v", err)
	}

	// remaining[partition] = high watermark (exclusive); the last live
	// record in a non-empty partition is always at highWatermark-1 (it's
	// the newest write in the log, so compaction can never supersede it).
	remaining := map[int32]int64{}
	endOffsets.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			log.Fatalf("end offset for partition %d: %v", o.Partition, o.Err)
		}
		remaining[o.Partition] = o.Offset
	})
	startOffsets.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			log.Fatalf("start offset for partition %d: %v", o.Partition, o.Err)
		}
		if o.Offset >= remaining[o.Partition] {
			delete(remaining, o.Partition) // empty partition, nothing to consume
		}
	})

	if len(remaining) == 0 {
		fmt.Printf("topic %q is empty\n", *topic)
		return
	}

	fmt.Printf("scanning topic %q, target high watermarks: %v\n\n", *topic, remaining)

	var (
		totalRecords int
		tombstones   int
		uniqueKeys   = map[string]struct{}{}
		keytypes     = map[string]int{}
	)

	for len(remaining) > 0 {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				log.Fatalf("fetch error (topic %s partition %d): %v", e.Topic, e.Partition, e.Err)
			}
		}

		fetches.EachRecord(func(r *kgo.Record) {
			if _, ok := remaining[r.Partition]; !ok {
				return // already reached this partition's target offset
			}

			totalRecords++
			uniqueKeys[string(r.Key)] = struct{}{}
			if r.Value == nil {
				tombstones++
			}

			var k struct {
				Keytype string `json:"keytype"`
			}
			if json.Unmarshal(r.Key, &k) == nil && k.Keytype != "" {
				keytypes[k.Keytype]++
			} else {
				keytypes["<unrecognized key>"]++
			}

			if r.Offset >= remaining[r.Partition]-1 {
				delete(remaining, r.Partition)
			}
		})
	}

	fmt.Printf("records (live, including tombstones): %d\n", totalRecords)
	fmt.Printf("tombstones:                            %d\n", tombstones)
	fmt.Printf("unique keys:                           %d\n", len(uniqueKeys))
	fmt.Println("keytype breakdown:")
	kts := make([]string, 0, len(keytypes))
	for k := range keytypes {
		kts = append(kts, k)
	}
	sort.Strings(kts)
	for _, k := range kts {
		fmt.Printf("  %-20s %d\n", k, keytypes[k])
	}

	fmt.Println()
	if totalRecords == len(uniqueKeys) {
		fmt.Println("FULLY COMPACTED: every live record has a unique key.")
		return
	}
	fmt.Printf("NOT FULLY COMPACTED: %d record(s) share a key with another still-live record.\n", totalRecords-len(uniqueKeys))
	os.Exit(1)
}
