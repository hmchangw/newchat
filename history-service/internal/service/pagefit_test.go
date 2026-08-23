package service_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/service"
	"github.com/hmchangw/chat/pkg/pagefit"
)

// fatMessages builds rows whose encoded size is dominated by Msg, so a test can
// pick a budget that admits a known number of them.
func fatMessages(n, width int) []models.Message {
	out := make([]models.Message, n)
	for i := range out {
		out[i] = models.Message{
			MessageID: fmt.Sprintf("m%02d", i),
			RoomID:    "r1",
			// DESC: index 0 is newest, so the tail is the oldest.
			CreatedAt: joinTime.Add(time.Duration(n-i) * time.Minute),
			Msg:       strings.Repeat("x", width),
		}
	}
	return out
}

func encodedSize(t *testing.T, v any) int {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return len(b)
}

// budgetFor returns a budget that admits about n of the given rows.
func budgetFor(t *testing.T, msgs []models.Message, n int) pagefit.Budget {
	t.Helper()
	return pagefit.NewBudget(int64(encodedSize(t, msgs[:n])), 0)
}

func TestLoadHistory_TrimsOversizePageAndSetsHasNext(t *testing.T) {
	all := fatMessages(20, 512)
	svc, msgs, subs, _, _ := newService(t, service.WithPageBudget(budgetFor(t, all, 5)))

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(all, false), nil)

	resp, err := svc.LoadHistory(testContext(), models.LoadHistoryRequest{})
	require.NoError(t, err)

	assert.Less(t, len(resp.Messages), len(all), "an oversize page must be trimmed")
	assert.NotEmpty(t, resp.Messages)
	assert.True(t, resp.HasNext, "dropped rows must be advertised as more")
	assert.Equal(t, all[0].MessageID, resp.Messages[0].MessageID, "the newest row is kept")
}

func TestLoadHistory_PageThatFitsIsUntouched(t *testing.T) {
	all := fatMessages(4, 64)
	svc, msgs, subs, _, _ := newService(t, service.WithPageBudget(pagefit.NewBudget(1<<20, 0)))

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(all, false), nil)

	resp, err := svc.LoadHistory(testContext(), models.LoadHistoryRequest{})
	require.NoError(t, err)
	assert.Len(t, resp.Messages, 4)
	assert.False(t, resp.HasNext)
}

// The whole point of the trim: walking the pages must yield every row exactly
// once. Page 2 is requested the way the client does it — before = the oldest
// createdAt page 1 returned.
func TestLoadHistory_TrimmedPaginationLosesNoRows(t *testing.T) {
	all := fatMessages(20, 512)
	svc, msgs, subs, _, _ := newService(t, service.WithPageBudget(budgetFor(t, all, 5)))

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil).Times(2)
	// The store answers with whatever is strictly older than the requested bound.
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ any, _ string, _ time.Time, before time.Time, _ any) (any, error) {
			var rest []models.Message
			for _, m := range all {
				if m.CreatedAt.Before(before) {
					rest = append(rest, m)
				}
			}
			return makePage(rest, false), nil
		}).Times(2)

	first, err := svc.LoadHistory(testContext(), models.LoadHistoryRequest{})
	require.NoError(t, err)
	require.True(t, first.HasNext)

	oldestKept := first.Messages[len(first.Messages)-1].CreatedAt.UnixMilli()
	second, err := svc.LoadHistory(testContext(), models.LoadHistoryRequest{Before: &oldestKept})
	require.NoError(t, err)
	require.NotEmpty(t, second.Messages)

	assert.Equal(t, all[len(first.Messages)].MessageID, second.Messages[0].MessageID,
		"page 2 must resume at the first row page 1 dropped")

	seen := map[string]int{}
	for _, m := range append(append([]models.Message{}, first.Messages...), second.Messages...) {
		seen[m.MessageID]++
	}
	for id, n := range seen {
		assert.Equal(t, 1, n, "row %s must appear exactly once across pages", id)
	}
}

// A row that alone exceeds the budget is blanked rather than dropped or
// errored, so the client can page past it instead of dead-ending.
func TestLoadHistory_SingleOversizeRowIsBlankedNotDropped(t *testing.T) {
	at := joinTime.Add(time.Minute)
	huge := models.Message{
		MessageID: "m-huge", RoomID: "r1", CreatedAt: at,
		Msg:                 strings.Repeat("x", 4096),
		Type:                "important",
		Sender:              models.Participant{ID: "u-1", Account: "alice"},
		Mentions:            []models.Participant{{ID: "u-2", Account: "bob"}},
		SysMsgData:          []byte(`{"a":1}`),
		QuotedParentMessage: &models.QuotedParentMessage{MessageID: "m-parent"},
	}
	svc, msgs, subs, _, _ := newService(t, service.WithPageBudget(pagefit.NewBudget(256, 0)))

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage([]models.Message{huge}, false), nil)

	resp, err := svc.LoadHistory(testContext(), models.LoadHistoryRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1, "the row must survive so pagination can advance")

	got := resp.Messages[0]
	assert.True(t, got.Truncated, "the client needs to know this row was blanked")
	assert.Empty(t, got.Msg)
	assert.Nil(t, got.Mentions)
	assert.Nil(t, got.QuotedParentMessage)
	assert.Nil(t, got.SysMsgData)
	assert.Nil(t, got.Reactions)

	assert.Equal(t, "m-huge", got.MessageID, "identifiers are kept for placeholder rendering")
	assert.Equal(t, at, got.CreatedAt, "the client pages by createdAt — it must survive")
	assert.Equal(t, "alice", got.Sender.Account)
	assert.Equal(t, "important", got.Type, "type is kept so the client can pick a placeholder")
}

// The centred window trims from both ends and reports which end lost rows, so
// the client can page outward in the direction it wants.
func TestLoadSurrounding_TrimsBothEndsAndKeepsTheCentralMessage(t *testing.T) {
	before := fatMessages(10, 512) // DESC as the store returns it
	after := fatMessages(10, 512)
	for i := range after {
		after[i].MessageID = fmt.Sprintf("a%02d", i)
	}
	central := models.Message{MessageID: "mC", RoomID: "r1", CreatedAt: joinTime.Add(2 * time.Minute)}

	svc, msgs, subs, _, _ := newService(t, service.WithPageBudget(pagefit.NewBudget(int64(encodedSize(t, before[:3])), 0)))

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "mC").Return(&central, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, central.CreatedAt, gomock.Any()).
		Return(makePage(before, false), nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", central.CreatedAt, gomock.Any(), gomock.Any()).
		Return(makePage(after, false), nil)

	resp, err := svc.LoadSurroundingMessages(testContext(), models.LoadSurroundingMessagesRequest{MessageID: "mC", Limit: 30})
	require.NoError(t, err)

	assert.Less(t, len(resp.Messages), len(before)+len(after)+1, "an oversize window must be trimmed")
	assert.True(t, resp.MoreBefore, "rows dropped from the front are advertised as moreBefore")
	assert.True(t, resp.MoreAfter, "rows dropped from the back are advertised as moreAfter")

	var found bool
	for _, m := range resp.Messages {
		if m.MessageID == "mC" {
			found = true
		}
	}
	assert.True(t, found, "the central message is what the caller asked to centre on — it must survive")
}

func TestLoadSurrounding_WindowThatFitsIsUntouched(t *testing.T) {
	central := models.Message{MessageID: "mC", RoomID: "r1", CreatedAt: joinTime.Add(2 * time.Minute)}
	before := fatMessages(2, 32)
	svc, msgs, subs, _, _ := newService(t, service.WithPageBudget(pagefit.NewBudget(1<<20, 0)))

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "mC").Return(&central, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, central.CreatedAt, gomock.Any()).
		Return(makePage(before, false), nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", central.CreatedAt, gomock.Any(), gomock.Any()).
		Return(makePage(nil, false), nil)

	resp, err := svc.LoadSurroundingMessages(testContext(), models.LoadSurroundingMessagesRequest{MessageID: "mC", Limit: 10})
	require.NoError(t, err)
	assert.Len(t, resp.Messages, 3)
	assert.False(t, resp.MoreBefore)
	assert.False(t, resp.MoreAfter)
}

// Timestamp mode has no central row; the pivot is the boundary between the
// before-page and the after-page.
func TestLoadSurrounding_TimestampModeTrimsAroundThePivot(t *testing.T) {
	before := fatMessages(8, 512)
	after := fatMessages(8, 512)
	for i := range after {
		after[i].MessageID = fmt.Sprintf("a%02d", i)
	}
	pivot := joinTime.Add(5 * time.Minute).UnixMilli()

	svc, msgs, subs, _, _ := newService(t, service.WithPageBudget(pagefit.NewBudget(int64(encodedSize(t, before[:3])), 0)))

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(before, false), nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(makePage(after, false), nil)

	resp, err := svc.LoadSurroundingMessages(testContext(), models.LoadSurroundingMessagesRequest{Timestamp: &pivot, Limit: 30})
	require.NoError(t, err)
	assert.NotEmpty(t, resp.Messages)
	assert.Less(t, len(resp.Messages), 16)
	assert.True(t, resp.MoreBefore || resp.MoreAfter)
}

// Cassandra timestamps are millisecond-precision and the resume bound is a
// strict `created_at < before`, so a trim that ends mid-millisecond drops every
// tied row for good. The cut must land on a timestamp boundary.
func TestLoadHistory_TrimNeverSplitsATimestampGroup(t *testing.T) {
	// Rows 4..7 share one millisecond; a budget near five rows would otherwise
	// cut inside that group.
	all := fatMessages(12, 512)
	shared := joinTime.Add(30 * time.Minute)
	for i := 4; i < 8; i++ {
		all[i].CreatedAt = shared
	}
	svc, msgs, subs, _, _ := newService(t, service.WithPageBudget(budgetFor(t, all, 6)))

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(all, false), nil)

	resp, err := svc.LoadHistory(testContext(), models.LoadHistoryRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Messages)
	require.True(t, resp.HasNext)

	last := resp.Messages[len(resp.Messages)-1]
	firstDropped := all[len(resp.Messages)]
	assert.False(t, last.CreatedAt.Equal(firstDropped.CreatedAt),
		"the page must not end on the same millisecond it resumes from — the tied rows would be skipped")
}

// A whole page inside one millisecond cannot be cut at all. Skipping rows would
// lose them silently, so the group is kept whole and shrunk to fit instead.
func TestLoadHistory_UnsplittableTimestampGroupIsKeptWholeAndBlanked(t *testing.T) {
	shared := joinTime.Add(30 * time.Minute)
	all := fatMessages(5, 2048)
	for i := range all {
		all[i].CreatedAt = shared
	}
	svc, msgs, subs, _, _ := newService(t, service.WithPageBudget(pagefit.NewBudget(1024, 0)))

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(all, false), nil)

	resp, err := svc.LoadHistory(testContext(), models.LoadHistoryRequest{})
	require.NoError(t, err)

	assert.Len(t, resp.Messages, 5, "no row sharing the millisecond may be dropped")
	for _, m := range resp.Messages {
		assert.True(t, m.Truncated, "an unsplittable group is shrunk rather than split")
		assert.Empty(t, m.Msg)
	}
}

// The window pages outward by timestamp at both ends, so both edges need the
// same boundary rule.
func TestLoadSurrounding_TrimNeverSplitsATimestampGroupAtEitherEdge(t *testing.T) {
	before := fatMessages(8, 512)
	after := fatMessages(8, 512)
	shared := joinTime.Add(90 * time.Minute)
	for i := range after {
		after[i].MessageID = fmt.Sprintf("a%02d", i)
		if i < 3 {
			after[i].CreatedAt = shared
		}
	}
	for i := range before {
		if i < 3 {
			before[i].CreatedAt = joinTime.Add(2 * time.Minute)
		}
	}
	central := models.Message{MessageID: "mC", RoomID: "r1", CreatedAt: joinTime.Add(60 * time.Minute)}

	svc, msgs, subs, _, _ := newService(t, service.WithPageBudget(pagefit.NewBudget(int64(encodedSize(t, before[:4])), 0)))
	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessageByID(gomock.Any(), "mC").Return(&central, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, central.CreatedAt, gomock.Any()).
		Return(makePage(before, false), nil)
	msgs.EXPECT().GetMessagesAfter(gomock.Any(), "r1", central.CreatedAt, gomock.Any(), gomock.Any()).
		Return(makePage(after, false), nil)

	resp, err := svc.LoadSurroundingMessages(testContext(), models.LoadSurroundingMessagesRequest{MessageID: "mC", Limit: 30})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Messages)

	// Rebuild the pre-trim assembly to locate the kept span's neighbours.
	assembled := make([]models.Message, 0, len(before)+1+len(after))
	for i := len(before) - 1; i >= 0; i-- {
		assembled = append(assembled, before[i])
	}
	assembled = append(assembled, central)
	assembled = append(assembled, after...)

	first, last := resp.Messages[0], resp.Messages[len(resp.Messages)-1]
	for i, m := range assembled {
		if m.MessageID == first.MessageID && i > 0 {
			assert.False(t, assembled[i-1].CreatedAt.Equal(m.CreatedAt), "older edge must not split a millisecond")
		}
		if m.MessageID == last.MessageID && i < len(assembled)-1 {
			assert.False(t, assembled[i+1].CreatedAt.Equal(m.CreatedAt), "newer edge must not split a millisecond")
		}
	}
}

// Keeping an unsplittable millisecond run whole grows the page past the length
// pagefit approved, so the blanked result must be measured again.
func TestLoadHistory_BlankedTimestampRunFitsTheBudget(t *testing.T) {
	shared := joinTime.Add(30 * time.Minute)
	all := fatMessages(20, 4096)
	for i := range all {
		all[i].CreatedAt = shared
	}
	budget := pagefit.NewBudget(8192, 0)
	svc, msgs, subs, _, _ := newService(t, service.WithPageBudget(budget))

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(all, false), nil)

	resp, err := svc.LoadHistory(testContext(), models.LoadHistoryRequest{})
	require.NoError(t, err)

	assert.Len(t, resp.Messages, 20, "no row sharing the millisecond may be dropped")
	assert.LessOrEqual(t, encodedSize(t, resp.Messages), budget.Bytes(),
		"blanking must bring the run back under the budget")
}

// When even the blanked run will not fit, the rows are still all returned:
// trimming would split the millisecond and lose them for good, so an explicit
// transport error beats a silent skip.
func TestLoadHistory_UnfittableTimestampRunKeepsEveryRow(t *testing.T) {
	shared := joinTime.Add(30 * time.Minute)
	all := fatMessages(100, 4096)
	for i := range all {
		all[i].CreatedAt = shared
	}
	svc, msgs, subs, _, _ := newService(t, service.WithPageBudget(pagefit.NewBudget(2048, 0)))

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(all, false), nil)

	resp, err := svc.LoadHistory(testContext(), models.LoadHistoryRequest{})
	require.NoError(t, err)

	assert.Len(t, resp.Messages, 100, "every tied row must survive — skipping one loses it permanently")
	for _, m := range resp.Messages {
		assert.True(t, m.Truncated)
	}
}

// oversize and the timestamp cut are orthogonal, and a huge row can sit inside
// a tie run. Returning only the blanked row would leave its tied sibling on the
// far side of a strict `created_at < before` resume — skipped for good.
func TestLoadHistory_OversizeRowStillShipsItsTiedSiblings(t *testing.T) {
	shared := joinTime.Add(30 * time.Minute)
	all := []models.Message{
		{MessageID: "m0-huge", RoomID: "r1", CreatedAt: shared, Msg: strings.Repeat("x", 4096)},
		{MessageID: "m1-tied", RoomID: "r1", CreatedAt: shared, Msg: "small"},
		{MessageID: "m2-older", RoomID: "r1", CreatedAt: joinTime.Add(10 * time.Minute), Msg: "small"},
	}
	svc, msgs, subs, _, _ := newService(t, service.WithPageBudget(pagefit.NewBudget(256, 0)))

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(&joinTime, true, nil)
	msgs.EXPECT().GetMessagesBetweenDesc(gomock.Any(), "r1", joinTime, gomock.Any(), gomock.Any()).
		Return(makePage(all, false), nil)

	resp, err := svc.LoadHistory(testContext(), models.LoadHistoryRequest{})
	require.NoError(t, err)

	require.Len(t, resp.Messages, 2, "the tied sibling must ship with the oversize row, not after it")
	assert.Equal(t, "m0-huge", resp.Messages[0].MessageID)
	assert.Equal(t, "m1-tied", resp.Messages[1].MessageID)
	assert.True(t, resp.Messages[0].Truncated)
	assert.True(t, resp.Messages[1].Truncated, "the whole run ships blanked so it fits")
	assert.True(t, resp.HasNext, "the older row is still pending")
}
