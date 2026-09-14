package msgbucket

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewerRow(t *testing.T) {
	base := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name         string
		at           time.Time
		id           string
		curAt        time.Time
		curID        string
		want         bool
		whyItMatters string
	}{
		{
			name: "a later timestamp is newer regardless of id",
			at:   base.Add(time.Second), id: "a",
			curAt: base, curID: "z",
			want:         true,
			whyItMatters: "created_at is the first clustering column",
		},
		{
			name: "an earlier timestamp is older regardless of id",
			at:   base, id: "z",
			curAt: base.Add(time.Second), curID: "a",
			want:         false,
			whyItMatters: "created_at is the first clustering column",
		},
		{
			name: "at the same millisecond the higher message id wins",
			at:   base, id: "m-2",
			curAt: base, curID: "m-1",
			want:         true,
			whyItMatters: "message_id is the tiebreaker clustering column",
		},
		{
			name: "at the same millisecond the lower message id loses",
			at:   base, id: "m-1",
			curAt: base, curID: "m-2",
			want:         false,
			whyItMatters: "message_id is the tiebreaker clustering column",
		},
		{
			name: "an identical row is not newer than itself",
			at:   base, id: "m-1",
			curAt: base, curID: "m-1",
			want:         false,
			whyItMatters: "the comparator is strict, so ties keep the incumbent",
		},
		{
			// The whole reason this is not a plain time comparison. created_at is a
			// Cassandra timestamp — milliseconds — so two Go times that differ only
			// in sub-millisecond digits occupy ONE clustering position there. Taking
			// the timestamp branch on that difference would skip the id tiebreaker
			// that exists to match the position, and two writers comparing the same
			// pair could disagree about which row is newest.
			name: "sub-millisecond differences do not order two rows",
			at:   base.Add(400 * time.Microsecond), id: "m-1",
			curAt: base, curID: "m-2",
			want:         false,
			whyItMatters: "created_at is stored at millisecond precision",
		},
		{
			name: "sub-millisecond differences still let the id decide",
			at:   base.Add(400 * time.Microsecond), id: "m-3",
			curAt: base, curID: "m-2",
			want:         true,
			whyItMatters: "created_at is stored at millisecond precision",
		},
		{
			name: "any row beats the zero incumbent",
			at:   base, id: "m-1",
			curAt: time.Time{}, curID: "",
			want:         true,
			whyItMatters: "an empty buffer slot must accept the first row",
		},
		{
			// Two writers may hold the same instant in different locations; the
			// comparator must not order rows by where the clock was read.
			name: "the zone a timestamp is expressed in does not order rows",
			at:   base.In(time.FixedZone("UTC+8", 8*3600)), id: "m-1",
			curAt: base, curID: "m-1",
			want:         false,
			whyItMatters: "unix millis are absolute",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, NewerRow(tt.at, tt.id, tt.curAt, tt.curID), tt.whyItMatters)
		})
	}
}

// The comparator must be a strict order over distinct clustering positions: for
// any two, exactly one direction holds. Two services coalescing the same stream
// independently rely on that — a comparator where both directions can be true
// lets them pick different "newest" rows for the same pair.
//
// Note what counts as distinct: (base, "m-2") and (base+400µs, "m-2") are ONE
// position, so they are deliberately not both in this list. That case is covered
// as a tie below, which is the property that makes the two writers agree.
func TestNewerRow_IsAStrictOrderOverDistinctPositions(t *testing.T) {
	base := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	rows := []struct {
		at time.Time
		id string
	}{
		{base, "m-1"},
		{base, "m-2"},
		{base.Add(time.Millisecond), "m-1"},
		{base.Add(-time.Hour), "m-9"},
	}
	for i, a := range rows {
		for j, b := range rows {
			ab := NewerRow(a.at, a.id, b.at, b.id)
			ba := NewerRow(b.at, b.id, a.at, a.id)
			if i == j {
				assert.False(t, ab, "a row is never newer than itself")
				continue
			}
			assert.NotEqual(t, ab, ba, "rows %d and %d must order in exactly one direction", i, j)
		}
	}
}

// Two Go times inside one millisecond carrying the same id are the same
// Cassandra row, so neither is newer — in either direction. This is what lets
// broadcast-worker and roomlist-worker, comparing the same pair from their own
// decode of the event, land on the same message.
func TestNewerRow_SameMillisecondSameIDIsOneRow(t *testing.T) {
	base := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	later := base.Add(400 * time.Microsecond)

	assert.False(t, NewerRow(later, "m-1", base, "m-1"))
	assert.False(t, NewerRow(base, "m-1", later, "m-1"))
}
