package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestJoinRegistry(t *testing.T) {
	base := time.Date(2026, 3, 26, 9, 0, 0, 0, time.UTC)

	t.Run("unknown room is empty and allocates nothing", func(t *testing.T) {
		r := newJoinRegistry(30*time.Second, func() time.Time { return base })
		assert.Empty(t, r.Fresh("room-1"))
		assert.Zero(t, r.Len())
	})

	t.Run("recorded joins are fresh inside the window", func(t *testing.T) {
		now := base
		r := newJoinRegistry(30*time.Second, func() time.Time { return now })
		r.Record("room-1", []string{"alice", "bob"}, base)

		now = base.Add(29 * time.Second)
		assert.ElementsMatch(t, []string{"alice", "bob"}, r.Fresh("room-1"))
	})

	t.Run("joins expire once the window passes", func(t *testing.T) {
		now := base
		r := newJoinRegistry(30*time.Second, func() time.Time { return now })
		r.Record("room-1", []string{"alice"}, base)

		now = base.Add(31 * time.Second)
		assert.Empty(t, r.Fresh("room-1"))
		assert.Zero(t, r.Len(), "an emptied room must be dropped, not left behind")
	})

	t.Run("a later join extends only that account", func(t *testing.T) {
		now := base
		r := newJoinRegistry(30*time.Second, func() time.Time { return now })
		r.Record("room-1", []string{"alice"}, base)
		r.Record("room-1", []string{"bob"}, base.Add(20*time.Second))

		now = base.Add(35 * time.Second)
		assert.ElementsMatch(t, []string{"bob"}, r.Fresh("room-1"))
	})

	t.Run("recording sweeps rooms that went quiet", func(t *testing.T) {
		now := base
		r := newJoinRegistry(30*time.Second, func() time.Time { return now })
		r.Record("room-1", []string{"alice"}, base)

		now = base.Add(time.Hour)
		r.Record("room-2", []string{"bob"}, now)
		assert.Equal(t, 1, r.Len(), "the stale room must not survive an unrelated join")
		assert.Empty(t, r.Fresh("room-1"))
		assert.ElementsMatch(t, []string{"bob"}, r.Fresh("room-2"))
	})

	t.Run("disabled registry records nothing", func(t *testing.T) {
		r := newJoinRegistry(0, func() time.Time { return base })
		r.Record("room-1", []string{"alice"}, base)
		assert.Empty(t, r.Fresh("room-1"))
	})

	t.Run("empty account list is a no-op", func(t *testing.T) {
		r := newJoinRegistry(30*time.Second, func() time.Time { return base })
		r.Record("room-1", nil, base)
		assert.Zero(t, r.Len())
	})
}
