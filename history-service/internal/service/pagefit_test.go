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
	svc, msgs, subs, _, _ := newServiceWithBudget(t, budgetFor(t, all, 5))

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
	svc, msgs, subs, _, _ := newServiceWithBudget(t, pagefit.NewBudget(1<<20, 0))

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
	svc, msgs, subs, _, _ := newServiceWithBudget(t, budgetFor(t, all, 5))

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
	svc, msgs, subs, _, _ := newServiceWithBudget(t, pagefit.NewBudget(256, 0))

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
