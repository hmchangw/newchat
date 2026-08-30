package roomsubcache_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/hmchangw/chat/pkg/roomsubcache"
	"github.com/hmchangw/chat/pkg/valkeyfake"
)

// buildMembers builds a deterministic slice of size n for benchmark fixtures.
func buildMembers(n int) []roomsubcache.Member {
	out := make([]roomsubcache.Member, n)
	for i := range out {
		idx := strconv.Itoa(i)
		out[i] = roomsubcache.Member{ID: "u" + idx, Account: "user" + idx}
	}
	return out
}

func BenchmarkValkeyCache_Get(b *testing.B) {
	for _, size := range []int{10, 100, 1000, 10000} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			ctx := context.Background()
			client := valkeyfake.New()
			cache := roomsubcache.NewValkeyCache(client)
			if err := cache.Set(ctx, "room", buildMembers(size), time.Minute); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, ok := cache.Get(ctx, "room"); !ok {
					b.Fatal("expected a cache hit")
				}
			}
		})
	}
}

func BenchmarkValkeyCache_Set(b *testing.B) {
	for _, size := range []int{10, 100, 1000, 10000} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			ctx := context.Background()
			client := valkeyfake.New()
			cache := roomsubcache.NewValkeyCache(client)
			members := buildMembers(size)

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := cache.Set(ctx, "room", members, time.Minute); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
