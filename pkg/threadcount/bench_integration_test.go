//go:build integration

package threadcount

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
)

// benchRows is the partition depth the scan benchmarks read from; each smaller
// read takes a prefix of the same partition.
const benchRows = 2000

// setupBenchThread seeds one thread and stamps its parent, reusing the same
// fixtures the tests use so the two cannot describe different schemas.
func setupBenchThread(b *testing.B) (*gocql.Session, Parent) {
	b.Helper()
	s := setupThreadTable(b)
	seedReplies(b, s, "bench-thread", "r", benchRows, nil)
	seedStamp(b, s, "bench-parent", benchRows, nil)
	return s, Parent{MessageID: "bench-parent", RoomID: "bench-room", Bucket: 0}
}

// BenchmarkScan measures the bounded partition read at each candidate scan
// limit. This is what a reply pays under the limit, and what it stops paying
// above it — the marginal difference between the two regimes.
func BenchmarkScan(b *testing.B) {
	sess, _ := setupBenchThread(b)
	ctx := context.Background()
	for _, k := range []int{100, 250, 500, 1000, 2000} {
		b.Run(fmt.Sprintf("rows=%d", k), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, _, _, err := countAndLatest(ctx, sess, "bench-thread", k); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkStampParent is the batched write of the authority row and its
// mirror — paid identically by both regimes, so it is the constant either side
// of the scan-limit decision.
func BenchmarkStampParent(b *testing.B) {
	sess, p := setupBenchThread(b)
	ctx := context.Background()
	now := time.Now().UTC()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := stampParent(ctx, sess, p, i, &now, true); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMaintainPastLimit is a whole reply on the approximate path: the
// point read that picks the regime plus the batched stamp, with no partition
// scan at all. Compare against BenchmarkScan to see what skipping it is worth.
func BenchmarkMaintainPastLimit(b *testing.B) {
	sess, p := setupBenchThread(b)
	ctx := context.Background()
	now := time.Now().UTC()
	pol := Policy{ScanLimit: 1, ReanchorBudget: 0} // always approximate, never re-anchor
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Maintain(ctx, sess, "bench-thread", p, pol, +1, &now, false); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMaintainUnderLimit is a whole reply on the exact path: the point
// read, the bounded scan, and the batched stamp.
func BenchmarkMaintainUnderLimit(b *testing.B) {
	sess, p := setupBenchThread(b)
	ctx := context.Background()
	now := time.Now().UTC()
	pol := Policy{ScanLimit: benchRows * 10, ReanchorBudget: 0} // always exact
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Maintain(ctx, sess, "bench-thread", p, pol, +1, &now, false); err != nil {
			b.Fatal(err)
		}
	}
}
