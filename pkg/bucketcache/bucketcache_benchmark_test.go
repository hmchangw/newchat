package bucketcache

import (
	"fmt"
	"testing"
	"time"

	"github.com/hmchangw/chat/pkg/model/cassandra"
)

func benchBucket(n int) []cassandra.Message {
	at := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	out := make([]cassandra.Message, n)
	for i := range out {
		out[i] = cassandra.Message{
			MessageID: fmt.Sprintf("01970a4f8c2d7c9a%04d", i),
			RoomID:    "01970a4f8c2d7c9aabcde0123456789f",
			CreatedAt: at.Add(-time.Duration(i) * time.Minute),
			Sender:    cassandra.Participant{ID: "u1", Account: "alice", EngName: "Alice Example", CompanyName: "Acme"},
			SiteID:    "site-a",
			Type:      "text",
			// At-rest rows carry ciphertext, which is what the cache actually stores.
			EncPayload: []byte("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
			EncMeta:    &cassandra.EncMeta{Nonce: []byte("012345678901")},
		}
	}
	return out
}

// BenchmarkDecode is the per-Get cost paid on BOTH tiers: Cache.Get runs
// interpret(blob) on an L1 hit and an L2 hit alike, because L1 stores []byte.
func BenchmarkDecode(b *testing.B) {
	for _, n := range []int{10, 50} {
		blob, err := encode(benchBucket(n))
		if err != nil {
			b.Fatal(err)
		}
		body := blob[1:]
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := decode(body); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
