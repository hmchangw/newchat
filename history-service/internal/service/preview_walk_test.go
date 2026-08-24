package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/subject"
)

// These tests drive the mutation walk through DeleteMessage, the only way it is reachable.

// walkedPreview deletes msg-target in r1, returning the event's preview and any clear.
func walkedPreview(t *testing.T, pages ...cassrepo.Page[models.Message]) (*model.PreviewMessage, bool) {
	t.Helper()
	return walkedPreviewErr(t, nil, pages...)
}

// walkedPreviewErr is walkedPreview with an optional read failure on the first page.
func walkedPreviewErr(t *testing.T, readErr error, pages ...cassrepo.Page[models.Message]) (*model.PreviewMessage, bool) {
	t.Helper()
	got, gone, _ := walkedPreviewPages(t, readErr, pages...)
	return got, gone
}

// walkedPreviewPages is walkedPreviewErr plus the PageSize of each read, so a test can
// assert the escalation itself rather than the preview it happened to land on.
func walkedPreviewPages(t *testing.T, readErr error, pages ...cassrepo.Page[models.Message]) (*model.PreviewMessage, bool, []int) {
	t.Helper()
	svc, msgs, subs, rooms, pub, _, _, _ := newServiceWithRoomMock(t)
	c := testContext()
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	rooms.EXPECT().UpdatePreviewBody(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	subs.EXPECT().GetHistorySharedSince(gomock.Any(), "u1", "r1").Return(nil, true, nil)
	hydrated := &models.Message{
		MessageID: "msg-target",
		RoomID:    "r1",
		Sender:    models.Participant{Account: "u1", ID: "u1-id"},
		CreatedAt: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC),
		Msg:       "target",
	}
	msgs.EXPECT().GetMessageByID(gomock.Any(), "msg-target").Return(hydrated, nil)
	msgs.EXPECT().
		SoftDeleteMessage(gomock.Any(), hydrated, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *models.Message, deletedAt time.Time) (time.Time, bool, *int, *time.Time, error) {
			return deletedAt, true, nil, nil, nil
		})

	// The walk is sequential within one room, so recording sizes needs no lock.
	var pageSizes []int
	if readErr != nil {
		msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
			Return(cassrepo.Page[models.Message]{}, readErr)
	} else {
		for i := range pages {
			msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, _ string, _, _ time.Time, req cassrepo.PageRequest) (cassrepo.Page[models.Message], error) {
					pageSizes = append(pageSizes, req.PageSize)
					return pages[i], nil
				})
		}
	}

	// "gone" is no longer a wire flag: the walk that establishes it is the one that
	// acts on it, so the observable is the destructive write itself.
	var gone bool
	rooms.EXPECT().ClearPreview(gomock.Any(), "r1", gomock.Any()).
		DoAndReturn(func(context.Context, string, int64) error { gone = true; return nil }).AnyTimes()

	var got *model.PreviewMessage
	pub.EXPECT().
		Publish(gomock.Any(), subject.MsgCanonicalDeleted("site-test"), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte, _ string) error {
			var evt model.MessageEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			got = evt.PreviewMessage
			return nil
		})

	_, err := svc.DeleteMessage(c, "site-test", models.DeleteMessageRequest{MessageID: "msg-target"})
	require.NoError(t, err)
	return got, gone, pageSizes
}

func msgAt(id string, minutesAgo int) models.Message {
	return models.Message{
		MessageID: id,
		RoomID:    "r1",
		Msg:       "content of " + id,
		CreatedAt: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC).Add(-time.Duration(minutesAgo) * time.Minute),
		Sender:    models.Participant{Account: "u1", ID: "u1-id", EngName: "User One"},
	}
}

func typed(m models.Message, t string) models.Message { //nolint:gocritic // hugeParam: a value-in/value-out fixture builder reads far better than pointer plumbing in table literals.
	m.Type = t
	return m
}

func deleted(m models.Message) models.Message { //nolint:gocritic // hugeParam: see typed.
	m.Deleted = true
	return m
}

// The walk's eligibility rule, exercised one tail shape at a time.
func TestPreviewWalk_Eligibility(t *testing.T) {
	tests := []struct {
		name string
		page []models.Message
		want string // "" => no preview
		gone bool
	}{
		{
			name: "newest eligible message wins",
			page: []models.Message{msgAt("m-3", 1), msgAt("m-2", 2)},
			want: "m-3",
		},
		{
			name: "a deleted tail is skipped",
			page: []models.Message{deleted(msgAt("m-3", 1)), deleted(msgAt("m-2", 2)), msgAt("m-1", 3)},
			want: "m-1",
		},
		{
			name: "a system tail is skipped",
			page: []models.Message{typed(msgAt("m-3", 1), model.MessageTypeMembersAdded), msgAt("m-1", 3)},
			want: "m-1",
		},
		{
			name: "important is client-set, not system, so it previews",
			page: []models.Message{typed(msgAt("m-3", 1), model.MessageTypeImportant)},
			want: "m-3",
		},
		{
			name: "a mixed ineligible tail skips to the first eligible message",
			page: []models.Message{
				typed(msgAt("m-4", 1), model.MessageTypeRoomRenamed),
				deleted(msgAt("m-3", 2)),
				typed(msgAt("m-2", 3), model.MessageTypeMemberLeft),
				msgAt("m-1", 4),
			},
			want: "m-1",
		},
		{
			name: "an empty room reports gone",
			page: nil,
			gone: true,
		},
		{
			name: "an all-ineligible room that the walk exhausts reports gone",
			page: []models.Message{deleted(msgAt("m-2", 1)), typed(msgAt("m-1", 2), model.MessageTypeRoomCreated)},
			gone: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, gone := walkedPreview(t, makePage(tc.page, false))
			assert.Equal(t, tc.gone, gone)
			if tc.want == "" {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			assert.Equal(t, tc.want, got.MessageID)
		})
	}
}

// Enumerated, not sampled: a newly added system type fails here instead of shipping.
func TestPreviewWalk_SkipsEverySystemType(t *testing.T) {
	systemTypes := []string{
		model.MessageTypeRoomCreated,
		model.MessageTypeMembersAdded,
		model.MessageTypeMemberRemoved,
		model.MessageTypeMemberLeft,
		model.MessageTypeRoomRenamed,
		model.MessageTypeRoomRestricted,
		model.MessageTypeTeamsMeetStarted,
	}
	for _, st := range systemTypes {
		t.Run(st, func(t *testing.T) {
			require.True(t, model.IsSystemMessageType(st), "fixture must actually be a system type")
			got, _ := walkedPreview(t, makePage([]models.Message{
				typed(msgAt("m-2", 1), st),
				msgAt("m-1", 2),
			}, false))
			require.NotNil(t, got)
			assert.Equal(t, "m-1", got.MessageID)
		})
	}
}

// The walk grows geometrically, so a long ineligible run costs queries, not one per message.
func TestPreviewWalk_EscalatesPastAnIneligibleTail(t *testing.T) {
	first := makePage([]models.Message{deleted(msgAt("m-9", 1))}, true)
	second := makePage([]models.Message{
		deleted(msgAt("m-8", 2)),
		msgAt("m-7", 3),
	}, true)

	got, gone, pageSizes := walkedPreviewPages(t, nil, first, second)
	require.NotNil(t, got)
	assert.Equal(t, "m-7", got.MessageID)
	assert.False(t, gone)
	// The preview assertion above passes under a fixed page size too; this pins the
	// escalation. Literals, not constants: external test package.
	assert.Equal(t, []int{1, 8}, pageSizes,
		"the walk must open with one row and grow ×8 to skip an ineligible tail")
}

// A tail longer than the scan budget is unknown, not empty — clearing on it would
// destroy a preview that very likely still exists further back.
func TestPreviewWalk_BudgetExhausted_IsNotGone(t *testing.T) {
	const budget = 250
	pages := []cassrepo.Page[models.Message]{}
	remaining := budget
	size := 1
	for remaining > 0 {
		n := min(size, remaining, 100)
		batch := make([]models.Message, n)
		for i := range batch {
			batch[i] = deleted(msgAt("m-"+strconv.Itoa(remaining-i), remaining-i))
		}
		pages = append(pages, makePage(batch, true))
		remaining -= n
		size *= 8
	}

	got, gone := walkedPreview(t, pages...)
	assert.Nil(t, got)
	assert.False(t, gone, "the walk gave up; it did not establish that the room is empty")
}

// A read failure is likewise unknown.
func TestPreviewWalk_ReadFailure_IsNotGone(t *testing.T) {
	got, gone := walkedPreviewErr(t, errors.New("cassandra unavailable"))
	assert.Nil(t, got)
	assert.False(t, gone)
}

// The preview carries render-ready enrichment: mapped participants, content capped.
func TestPreviewWalk_EnrichesAndTruncates(t *testing.T) {
	m := msgAt("m-1", 1)
	m.Msg = strings.Repeat("x", 900)
	m.Sender = models.Participant{ID: "u-1", Account: "alice", EngName: "Alice", CompanyName: "愛麗絲"}
	m.Mentions = []cassandra.Participant{{ID: "u-2", Account: "bob", EngName: "Bob"}}

	got, _ := walkedPreview(t, makePage([]models.Message{m}, false))

	require.NotNil(t, got)
	assert.Equal(t, "alice", got.Sender.Account)
	assert.Equal(t, "愛麗絲", got.Sender.ChineseName, "companyName maps to the wire chineseName")
	assert.Equal(t, "Alice 愛麗絲", got.Sender.DisplayName)
	require.Len(t, got.Mentions, 1)
	assert.Equal(t, "bob", got.Mentions[0].Account)
	assert.Len(t, []rune(got.Content), 500, "the room list renders a snippet, not the whole body")
}

// A quoted reply is ordinary user content and previews like any other message.
func TestPreviewWalk_QuotedReplyIsEligible(t *testing.T) {
	m := msgAt("m-1", 1)
	m.QuotedParentMessage = &cassandra.QuotedParentMessage{MessageID: "m-0"}

	got, _ := walkedPreview(t, makePage([]models.Message{m}, false))
	require.NotNil(t, got)
	assert.Equal(t, "m-1", got.MessageID)
}
