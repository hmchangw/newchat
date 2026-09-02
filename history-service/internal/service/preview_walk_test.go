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
	"github.com/hmchangw/chat/history-service/internal/service"
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

// walkCall is one GetMessagesBefore the walk issued, for tests that assert on how the
// walk continued rather than on the preview it landed on.
type walkCall struct {
	before   time.Time
	pageSize int
	cursor   string
}

func pageSizesOf(calls []walkCall) []int {
	out := make([]int, len(calls))
	for i := range calls {
		out[i] = calls[i].pageSize
	}
	return out
}

// walkedPreviewPages is walkedPreviewErr plus each read's arguments, so a test can
// assert the escalation itself rather than the preview it happened to land on.
func walkedPreviewPages(t *testing.T, readErr error, pages ...cassrepo.Page[models.Message]) (*model.PreviewMessage, bool, []walkCall) {
	t.Helper()
	return walkedPreviewOpts(t, readErr, nil, pages...)
}

// walkedPreviewWith drives the same delete with a preview cache installed.
func walkedPreviewWith(t *testing.T, cache service.PreviewCache, pages ...cassrepo.Page[models.Message]) {
	t.Helper()
	walkedPreviewOpts(t, nil, service.WithPreviewCache(cache), pages...)
}

func walkedPreviewOpts(t *testing.T, readErr error, opt service.Option, pages ...cassrepo.Page[models.Message]) (*model.PreviewMessage, bool, []walkCall) {
	t.Helper()
	var opts []service.Option
	if opt != nil {
		opts = append(opts, opt)
	}
	svc, msgs, subs, rooms, pub, _, _, _ := newServiceWithRoomMock(t, opts...)
	c := testContext()
	rooms.EXPECT().GetMinUserLastSeenAt(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	rooms.EXPECT().UpdatePreviewBody(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	rooms.EXPECT().InvalidatePreviewKey(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

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

	// The walk is sequential within one room, so recording calls needs no lock.
	var calls []walkCall
	if readErr != nil {
		msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
			Return(cassrepo.Page[models.Message]{}, readErr)
	} else {
		for i := range pages {
			msgs.EXPECT().GetMessagesBefore(gomock.Any(), "r1", gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, _ string, before, _ time.Time, req cassrepo.PageRequest) (cassrepo.Page[models.Message], error) {
					cursor := ""
					if req.Cursor != nil {
						cursor = req.Cursor.Encode()
					}
					calls = append(calls, walkCall{before: before, pageSize: req.PageSize, cursor: cursor})
					return pages[i], nil
				})
		}
	}

	// "gone" is no longer a wire flag: the walk that establishes it is the one that
	// acts on it, so the observable is the destructive write itself.
	var gone bool
	rooms.EXPECT().ClearPreview(gomock.Any(), "r1", gomock.Any()).
		DoAndReturn(func(context.Context, string, int64) (bool, error) { gone = true; return true, nil }).AnyTimes()

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
	return got, gone, calls
}

// countingPreviewCache records invalidations; Get always misses so the walk still runs.
type countingPreviewCache struct{ invalidated []string }

func (c *countingPreviewCache) Get(ctx context.Context, _ string, load func(context.Context) (model.PreviewMessage, bool, error)) (model.PreviewMessage, bool, error) {
	return load(ctx)
}

func (c *countingPreviewCache) Invalidate(roomID string) {
	c.invalidated = append(c.invalidated, roomID)
}

// The mutation has already committed in Cassandra, so whatever the cache holds for the
// room describes a message that changed. Every mutation outcome must drop it -- a
// degraded walk especially, since it writes nothing and would otherwise leave the entry
// as the only surviving description of the room (#292).
func TestPreviewWalk_MutationInvalidatesTheCache(t *testing.T) {
	for _, tc := range []struct {
		name string
		page cassrepo.Page[models.Message]
	}{
		{"a resolved walk", makePage([]models.Message{msgAt("m-1", 1)}, false)},
		{"an empty room", makePage(nil, false)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := &countingPreviewCache{}
			walkedPreviewWith(t, cache, tc.page)
			assert.Equal(t, []string{"r1"}, cache.invalidated,
				"the mutation must drop the room's cached preview")
		})
	}
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

func restricted(m models.Message, to string) models.Message { //nolint:gocritic // hugeParam: see typed.
	m.VisibleTo = to
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
			// Store-and-surface: visible_to is an opaque client-set marker the backend
			// never filters on, so a message carrying it is a valid preview (the client
			// interprets the scope).
			name: "a visibility-marked message is eligible",
			page: []models.Message{restricted(msgAt("m-3", 1), "u-9"), msgAt("m-1", 3)},
			want: "m-3",
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

// The surfaced preview carries the message's visible_to marker verbatim, so the client
// can interpret the scope the backend never filters on.
func TestPreviewWalk_SurfacesVisibleTo(t *testing.T) {
	got, gone := walkedPreview(t, makePage([]models.Message{restricted(msgAt("m-3", 1), "u-9")}, false))
	assert.False(t, gone)
	require.NotNil(t, got)
	assert.Equal(t, "m-3", got.MessageID)
	assert.Equal(t, "u-9", got.VisibleTo)
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
		// Migrated rows. The walk reads Cassandra directly, so it classifies by the
		// STORED type — the wire-side rewrite in normalizeLegacySysMsgs never runs here.
		model.MessageTypeLegacyMembersRemoved,
		model.MessageTypeLegacyMembersLeft,
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

	got, gone, calls := walkedPreviewPages(t, nil, first, second)
	require.NotNil(t, got)
	assert.Equal(t, "m-7", got.MessageID)
	assert.False(t, gone)
	// The preview assertion above passes under a fixed page size too; this pins the
	// escalation. Literals, not constants: external test package.
	assert.Equal(t, []int{1, 8}, pageSizesOf(calls),
		"the walk must open with one row and grow ×8 to skip an ineligible tail")
}

// created_at does not identify a row — messages_by_room clusters by (created_at,
// message_id). Continuing by timestamp against a strict `created_at < ?` therefore
// drops every row that shares the boundary timestamp, and with a first page of one
// that is any two messages in the same millisecond. The walk must continue on the
// page cursor, holding `before` at the original ceiling for the whole walk.
func TestPreviewWalk_ContinuesOnTheCursorNotTheTimestamp(t *testing.T) {
	first := makePage([]models.Message{deleted(msgAt("m-9", 1))}, true)
	second := makePage([]models.Message{msgAt("m-8", 1)}, true) // same minute as m-9

	got, gone, calls := walkedPreviewPages(t, nil, first, second)
	require.NotNil(t, got)
	assert.Equal(t, "m-8", got.MessageID, "the sibling sharing a timestamp must still be reachable")
	assert.False(t, gone)

	require.Len(t, calls, 2)
	assert.Empty(t, calls[0].cursor, "the first read opens the walk")
	assert.Equal(t, first.NextCursor, calls[1].cursor,
		"the continuation must carry the page cursor, not rebuild a bare request")
	assert.Equal(t, calls[0].before, calls[1].before,
		"`before` must stay at the ceiling: moving it re-applies `created_at < ?` and strands tied rows")
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
