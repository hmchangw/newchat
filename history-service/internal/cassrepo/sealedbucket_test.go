package cassrepo

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/models"
)

// descRows builds messages ordered created_at DESC (t0 newest), ids "0".."n-1".
func descRows(times ...time.Time) []models.Message {
	out := make([]models.Message, len(times))
	for i, ts := range times {
		out[i] = models.Message{MessageID: itoa(i), CreatedAt: ts}
	}
	return out
}

func itoa(i int) string { return string(rune('0' + i)) }

func ids(msgs []models.Message) []string {
	out := make([]string, len(msgs))
	for i := range msgs {
		out[i] = msgs[i].MessageID
	}
	return out
}

func ptr(t time.Time) *time.Time { return &t }

func TestSliceBounded(t *testing.T) {
	// t0 > t1 > t2 > t3 > t4 (DESC order in the slice).
	t0 := time.Unix(500, 0).UTC()
	t1 := time.Unix(400, 0).UTC()
	t2 := time.Unix(300, 0).UTC()
	t3 := time.Unix(200, 0).UTC()
	t4 := time.Unix(100, 0).UTC()
	rows := descRows(t0, t1, t2, t3, t4)

	tests := []struct {
		name   string
		before *time.Time
		since  *time.Time
		limit  int
		want   []string
	}{
		{"no bounds, high limit", nil, nil, 100, []string{"0", "1", "2", "3", "4"}},
		{"limit caps", nil, nil, 2, []string{"0", "1"}},
		{"before excludes anchor and newer", ptr(t2), nil, 100, []string{"3", "4"}},
		{"since excludes anchor and older", nil, ptr(t2), 100, []string{"0", "1"}},
		{"before+since window", ptr(t1), ptr(t3), 100, []string{"2"}},
		{"before below all", ptr(t4), nil, 100, []string{}},
		{"since equals newest excludes all", nil, ptr(t0), 100, []string{}},
		{"since below all keeps all", nil, ptr(time.Unix(50, 0).UTC()), 100, []string{"0", "1", "2", "3", "4"}},
		{"window with limit", ptr(t0), ptr(t4), 2, []string{"1", "2"}},
		{"before newer than newest keeps all", ptr(time.Unix(999, 0).UTC()), nil, 100, []string{"0", "1", "2", "3", "4"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := sliceBounded(rows, tc.before, tc.since, tc.limit)
			assert.Equal(t, tc.want, ids(got))
		})
	}
}

func TestSliceBounded_EmptyInput(t *testing.T) {
	assert.Empty(t, sliceBounded(nil, nil, nil, 10))
}

func TestSliceBounded_DoesNotMutateInput(t *testing.T) {
	t0 := time.Unix(500, 0).UTC()
	t1 := time.Unix(400, 0).UTC()
	rows := descRows(t0, t1)
	got := sliceBounded(rows, ptr(t0), nil, 100)
	require.Equal(t, []string{"1"}, ids(got))
	// The original slice is untouched (sliceBounded returns a sub-slice view or copy,
	// never reorders/overwrites the backing array).
	assert.Equal(t, []string{"0", "1"}, ids(rows))
}
