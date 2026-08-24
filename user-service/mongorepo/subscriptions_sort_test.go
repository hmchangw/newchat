package mongorepo

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// row builds a listRow for the sort tests; a nil at means an undated room.
func row(id, name string, at *time.Time, selfDM bool) listRow {
	return listRow{sub: subLite{ID: id, Name: name}, sortAt: at, selfDM: selfDM}
}

func rowIDs(rows []listRow) []string {
	ids := make([]string, 0, len(rows))
	for i := range rows {
		ids = append(ids, rows[i].sub.ID)
	}
	return ids
}

// Offset paging reads the same sorted sequence across separate requests, so
// rows that tie on every visible key must still order deterministically —
// otherwise the phase-one Find's arbitrary order decides, and a page boundary
// can duplicate one row while dropping another. The subscription _id is
// immutable, so it breaks the tie the same way on every request.
func TestSortListRows_TiesBreakOnSubscriptionID(t *testing.T) {
	at := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)

	t.Run("equal sortAt and name", func(t *testing.T) {
		rows := []listRow{row("s-c", "Eng", &at, false), row("s-a", "Eng", &at, false), row("s-b", "Eng", &at, false)}
		sortListRows(rows)
		assert.Equal(t, []string{"s-a", "s-b", "s-c"}, rowIDs(rows))
	})

	t.Run("undated rows with equal names", func(t *testing.T) {
		rows := []listRow{row("s-z", "Eng", nil, false), row("s-y", "Eng", nil, false)}
		sortListRows(rows)
		assert.Equal(t, []string{"s-y", "s-z"}, rowIDs(rows))
	})

	t.Run("the same input in any order sorts the same way", func(t *testing.T) {
		forward := []listRow{row("s-a", "Eng", &at, false), row("s-b", "Eng", &at, false)}
		reversed := []listRow{row("s-b", "Eng", &at, false), row("s-a", "Eng", &at, false)}
		sortListRows(forward)
		sortListRows(reversed)
		assert.Equal(t, rowIDs(forward), rowIDs(reversed))
	})
}

// The ID tiebreak is the last key only — it must not disturb the ordering the
// old Mongo sort produced.
func TestSortListRows_KeyPrecedence(t *testing.T) {
	newer := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	older := newer.Add(-time.Hour)

	tests := []struct {
		name string
		rows []listRow
		want []string
	}{
		{
			name: "newest activity first, whatever the id",
			rows: []listRow{row("s-a", "Eng", &older, false), row("s-z", "Eng", &newer, false)},
			want: []string{"s-z", "s-a"},
		},
		{
			name: "undated rows sort after every dated row",
			rows: []listRow{row("s-a", "Eng", nil, false), row("s-z", "Eng", &older, false)},
			want: []string{"s-z", "s-a"},
		},
		{
			name: "name beats id when the activity ties",
			rows: []listRow{row("s-a", "Ops", &newer, false), row("s-z", "Eng", &newer, false)},
			want: []string{"s-z", "s-a"},
		},
		{
			name: "the pinned self-DM beats a newer room",
			rows: []listRow{row("s-a", "Eng", &newer, false), row("s-z", "alice", &older, true)},
			want: []string{"s-z", "s-a"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sortListRows(tc.rows)
			assert.Equal(t, tc.want, rowIDs(tc.rows))
		})
	}
}
