//go:build integration

package cassrepo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/model"
	cassmodel "github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/msgbucket"
)

// testPreviewLookback mirrors the MESSAGE_PREVIEW_LOOKBACK_ROWS default; the
// budget-boundary tests build their repositories with it explicitly.
const testPreviewLookback = 10

// seedLastMsgRow inserts one messages_by_room row with explicit deleted/type
// values so tests can build mixed survivor/tombstone/system partitions.
func seedLastMsgRow(t *testing.T, session *gocql.Session, roomID, messageID string, createdAt time.Time, deleted bool, msgType, msg string) {
	t.Helper()
	sizer := msgbucket.New(24 * time.Hour)
	sender := models.Participant{ID: "u1", Account: "alice", EngName: "Alice"}
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, deleted, type, site_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, sizer.Of(createdAt), createdAt, messageID, sender, msg, deleted, msgType, "site-a",
	).Exec())
}

func TestRepository_GetLastRoomMessage_DeletedLastFallsBackToPriorSurvivor(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, 10, nil)
	ctx := context.Background()

	roomID := "r-last-deleted"
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	seedLastMsgRow(t, session, roomID, "m-survivor", base, false, "", "still here")
	seedLastMsgRow(t, session, roomID, "m-deleted", base.Add(time.Minute), true, "", "gone")

	_, got, err := repo.GetLastRoomMessage(ctx, roomID, base.Add(time.Hour), base.Add(-time.Hour))
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "m-survivor", got.MessageID)
	assert.Equal(t, "still here", got.Msg)
}

func TestRepository_GetLastRoomMessage_EditedLastCarriesCurrentContentAndEditedAt(t *testing.T) {
	session := setupCassandra(t)
	sizer := msgbucket.New(24 * time.Hour)
	repo := NewRepository(session, sizer, 365, 10, nil)
	ctx := context.Background()

	roomID := "r-last-edited"
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	editedAt := base.Add(5 * time.Minute)
	sender := models.Participant{ID: "u1", Account: "alice", EngName: "Alice"}
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, edited_at, site_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, sizer.Of(base), base, "m-edited", sender, "edited body", editedAt, "site-a",
	).Exec())

	_, got, err := repo.GetLastRoomMessage(ctx, roomID, base.Add(time.Hour), base.Add(-time.Hour))
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "m-edited", got.MessageID)
	assert.Equal(t, "edited body", got.Msg, "preview must carry the post-edit content")
	require.NotNil(t, got.EditedAt)
	assert.Equal(t, editedAt, got.EditedAt.UTC())
}

func TestRepository_GetLastRoomMessage_AllDeletedReturnsNil(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, 10, nil)
	ctx := context.Background()

	roomID := "r-last-all-deleted"
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		seedLastMsgRow(t, session, roomID, fmt.Sprintf("m%d", i), base.Add(time.Duration(i)*time.Minute), true, "", "gone")
	}

	_, got, err := repo.GetLastRoomMessage(ctx, roomID, base.Add(time.Hour), base.Add(-time.Hour))
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestRepository_GetLastRoomMessage_SystemMessageNewestIsSkipped(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, 10, nil)
	ctx := context.Background()

	roomID := "r-last-system"
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	seedLastMsgRow(t, session, roomID, "m-user", base, false, "", "user message")
	seedLastMsgRow(t, session, roomID, "m-sys-1", base.Add(time.Minute), false, model.MessageTypeMembersAdded, "alice added bob")
	seedLastMsgRow(t, session, roomID, "m-sys-2", base.Add(2*time.Minute), false, model.MessageTypeRoomRenamed, "renamed")

	_, got, err := repo.GetLastRoomMessage(ctx, roomID, base.Add(time.Hour), base.Add(-time.Hour))
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "m-user", got.MessageID)
}

func TestRepository_GetLastRoomMessage_OnlySystemAndDeletedReturnsNil(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, 10, nil)
	ctx := context.Background()

	roomID := "r-last-sys-deleted"
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	seedLastMsgRow(t, session, roomID, "m-sys", base, false, model.MessageTypeRoomCreated, "created")
	seedLastMsgRow(t, session, roomID, "m-del", base.Add(time.Minute), true, "", "gone")
	// A removed thread parent carries the placeholder type AND deleted=true; seed
	// one with deleted=false anyway to prove the type alone disqualifies it.
	seedLastMsgRow(t, session, roomID, "m-removed", base.Add(2*time.Minute), false, MessageTypeRemoved, "")

	_, got, err := repo.GetLastRoomMessage(ctx, roomID, base.Add(time.Hour), base.Add(-time.Hour))
	require.NoError(t, err)
	assert.Nil(t, got)
}

// Budget boundary, inclusive side: a survivor sitting right AT the lookback
// edge (testPreviewLookback-1 tombstones above it) is still found — the
// budget covers the tombstones plus the survivor row itself.
func TestRepository_GetLastRoomMessage_SurvivorAtBudgetEdgeFound(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, 10, nil)
	ctx := context.Background()

	roomID := "r-last-budget-edge"
	base := time.Date(2026, 6, 1, 6, 0, 0, 0, time.UTC)
	seedLastMsgRow(t, session, roomID, "m-survivor", base, false, "", "just in reach")
	for i := 0; i < testPreviewLookback-1; i++ {
		seedLastMsgRow(t, session, roomID, fmt.Sprintf("m-del-%03d", i), base.Add(time.Duration(i+1)*time.Second), true, "", "gone")
	}

	_, got, err := repo.GetLastRoomMessage(ctx, roomID, base.Add(time.Hour), base.Add(-time.Hour))
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "m-survivor", got.MessageID)
}

// Budget boundary, exclusive side: exactly testPreviewLookback tombstones
// exhaust the lookback before the survivor — empty preview, and that's the
// deal (the next real message self-heals it).
func TestRepository_GetLastRoomMessage_TombstonesFillBudgetReturnsNil(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, 10, nil)
	ctx := context.Background()

	roomID := "r-last-budget-full"
	base := time.Date(2026, 6, 1, 6, 0, 0, 0, time.UTC)
	seedLastMsgRow(t, session, roomID, "m-survivor", base, false, "", "one row too deep")
	for i := 0; i < testPreviewLookback; i++ {
		seedLastMsgRow(t, session, roomID, fmt.Sprintf("m-del-%03d", i), base.Add(time.Duration(i+1)*time.Second), true, "", "gone")
	}

	ptr, got, err := repo.GetLastRoomMessage(ctx, roomID, base.Add(time.Hour), base.Add(-time.Hour))
	require.NoError(t, err)
	assert.Nil(t, got, "all lookback rows deleted ⇒ empty preview")
	assert.Nil(t, ptr)
}

// The survivor may sit buckets below the tombstones — the walk must cross
// bucket boundaries (including an empty middle bucket) to reach it.
func TestRepository_GetLastRoomMessage_SurvivorInOlderBucket(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, 10, nil)
	ctx := context.Background()

	roomID := "r-last-older-bucket"
	newest := time.Date(2026, 6, 3, 12, 0, 0, 0, time.UTC)
	survivorAt := newest.AddDate(0, 0, -2) // two buckets down; day -1 stays empty
	seedLastMsgRow(t, session, roomID, "m-survivor", survivorAt, false, "", "old but alive")
	for i := 0; i < 5; i++ {
		seedLastMsgRow(t, session, roomID, fmt.Sprintf("m-del-%d", i), newest.Add(time.Duration(i)*time.Minute), true, "", "gone")
	}

	_, got, err := repo.GetLastRoomMessage(ctx, roomID, newest.Add(time.Hour), newest.AddDate(0, 0, -10))
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "m-survivor", got.MessageID)
}

// messages_by_room only holds main-timeline rows (thread replies live in
// thread_messages_by_thread; only TShow replies are dual-written), so a
// thread-only reply must never surface as the room's last message.
func TestRepository_GetLastRoomMessage_ThreadOnlyReplyDoesNotSurface(t *testing.T) {
	session := setupCassandra(t)
	sizer := msgbucket.New(24 * time.Hour)
	repo := NewRepository(session, sizer, 365, 10, nil)
	ctx := context.Background()

	roomID := "r-last-thread-only"
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	seedLastMsgRow(t, session, roomID, "m-main", base, false, "", "main timeline")

	// Newer reply written ONLY to the thread table — as message-worker does for
	// a non-TShow reply.
	replyAt := base.Add(time.Minute)
	sender := models.Participant{ID: "u2", Account: "bob"}
	require.NoError(t, session.Query(
		`INSERT INTO thread_messages_by_thread (thread_room_id, created_at, message_id, room_id, sender, msg, thread_parent_id, site_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"tr-1", replyAt, "m-reply", roomID, sender, "thread reply", "m-main", "site-a",
	).Exec())

	_, got, err := repo.GetLastRoomMessage(ctx, roomID, replyAt.Add(time.Hour), base.Add(-time.Hour))
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "m-main", got.MessageID, "a thread-only reply must not win the room preview")
}

// An at-rest-encrypted survivor must come back decrypted, exactly like the
// paginated read paths.
func TestRepository_GetLastRoomMessage_DecryptsEncryptedSurvivor(t *testing.T) {
	ctx := context.Background()
	session := setupCassandra(t)
	mongoDB := setupMongo(t)

	wrapper := newTestVaultWrapper(t, ctx)
	cipher := atrest.NewCipher(wrapper, atrest.NewMongoDEKStore(mongoDB.Collection(atrest.CollectionName)),
		atrest.Config{DEKCacheSize: 100, DEKCacheTTL: time.Hour})
	sizer := msgbucket.New(24 * time.Hour)
	repo := NewRepository(session, sizer, 365, 10, cipher)

	roomID := "r-last-encrypted"
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	payload, meta, err := cipher.Encrypt(ctx, roomID, atrest.EncryptedFields{Msg: "secret body"})
	require.NoError(t, err)
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, enc_payload, enc_meta, site_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		roomID, sizer.Of(base), base, "m-enc", payload, &cassmodel.EncMeta{Nonce: meta.Nonce}, "site-a",
	).Exec())

	_, got, err := repo.GetLastRoomMessage(ctx, roomID, base.Add(time.Hour), base.Add(-time.Hour))
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "secret body", got.Msg)
	assert.Empty(t, got.EncPayload, "enc_* must never leak above the repo layer")
	assert.Nil(t, got.EncMeta)
}

func TestRepository_GetLastRoomMessage_EmptyRoomReturnsNil(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, 10, nil)
	ctx := context.Background()

	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	_, got, err := repo.GetLastRoomMessage(ctx, "r-last-empty", base, base.AddDate(0, 0, -5))
	require.NoError(t, err)
	assert.Nil(t, got)
}

// A deleted tail longer than the total row-scan budget must abort the walk
// with (nil, nil) — matching the rooms.get ponytail precedent — instead of
// draining arbitrarily deep partitions on every delete fan-out.
func TestRepository_GetLastRoomMessage_RowScanCapAbortsDeepTombstoneTail(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, 10, nil)
	ctx := context.Background()

	roomID := "r-last-rowcap"
	base := time.Date(2026, 6, 1, 6, 0, 0, 0, time.UTC)
	// Survivor buried one row deeper than the budget allows.
	seedLastMsgRow(t, session, roomID, "m-survivor", base, false, "", "too deep to find")
	for i := 0; i < testPreviewLookback+1; i++ {
		seedLastMsgRow(t, session, roomID, fmt.Sprintf("m-del-%04d", i), base.Add(time.Duration(i+1)*time.Second), true, "", "gone")
	}

	_, got, err := repo.GetLastRoomMessage(ctx, roomID, base.Add(time.Hour), base.Add(-time.Hour))
	require.NoError(t, err)
	assert.Nil(t, got, "row budget exhausted must degrade to no preview, never keep scanning")
}

// #4: a qualifying row OLDER than floor inside the floor bucket must not
// leak out — DESC order proves nothing newer survives, so the walk answers
// "no preview".
func TestRepository_GetLastRoomMessage_RowBelowFloorNeverLeaks(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, 10, nil)
	ctx := context.Background()

	roomID := "r-last-floor-leak"
	floor := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	// Same bucket as floor, but 30s older — outside the [floor, before) window.
	seedLastMsgRow(t, session, roomID, "m-too-old", floor.Add(-30*time.Second), false, "", "before the window")
	seedLastMsgRow(t, session, roomID, "m-deleted", floor.Add(time.Minute), true, "", "gone")

	ptr, got, err := repo.GetLastRoomMessage(ctx, roomID, floor.Add(time.Hour), floor)
	require.NoError(t, err)
	assert.Nil(t, got, "a row below floor must never be returned as the preview")
	assert.Nil(t, ptr, "nor as the pointer")
}

// #6: the pointer tracks the newest surviving row of ANY type (system
// included), while the preview skips system types — one walk, two answers.
func TestRepository_GetLastRoomMessage_PointerIncludesSystemPreviewSkips(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, 10, nil)
	ctx := context.Background()

	roomID := "r-last-pointer"
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	seedLastMsgRow(t, session, roomID, "m-user", base, false, "", "user message")
	seedLastMsgRow(t, session, roomID, "m-sys", base.Add(time.Minute), false, model.MessageTypeMembersAdded, "alice added bob")
	seedLastMsgRow(t, session, roomID, "m-del", base.Add(2*time.Minute), true, "", "gone")

	ptr, got, err := repo.GetLastRoomMessage(ctx, roomID, base.Add(time.Hour), base.Add(-time.Hour))
	require.NoError(t, err)
	require.NotNil(t, ptr)
	assert.Equal(t, "m-sys", ptr.MessageID, "pointer = newest surviving row incl. system")
	assert.True(t, ptr.CreatedAt.Equal(base.Add(time.Minute)))
	require.NotNil(t, got)
	assert.Equal(t, "m-user", got.MessageID, "preview = newest surviving non-system row")
}

// Only system messages survive: pointer set, preview nil — the room still
// sorts by the system notice but shows no snippet.
func TestRepository_GetLastRoomMessage_OnlySystemSurvives_PointerWithoutPreview(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, 10, nil)
	ctx := context.Background()

	roomID := "r-last-sys-only"
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	seedLastMsgRow(t, session, roomID, "m-del", base, true, "", "gone")
	seedLastMsgRow(t, session, roomID, "m-sys", base.Add(time.Minute), false, model.MessageTypeRoomCreated, "created")

	ptr, got, err := repo.GetLastRoomMessage(ctx, roomID, base.Add(time.Hour), base.Add(-time.Hour))
	require.NoError(t, err)
	require.NotNil(t, ptr)
	assert.Equal(t, "m-sys", ptr.MessageID)
	assert.Nil(t, got)
}

// The removed-parent placeholder qualifies for NEITHER pointer nor preview:
// re-pointing lastMsgId at the message that was just deleted would undo the
// rewind the walk exists to serve.
func TestRepository_GetLastRoomMessage_RemovedPlaceholderNeverPointer(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365, 10, nil)
	ctx := context.Background()

	roomID := "r-last-removed-ptr"
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	seedLastMsgRow(t, session, roomID, "m-user", base, false, "", "still here")
	seedLastMsgRow(t, session, roomID, "m-removed", base.Add(time.Minute), false, MessageTypeRemoved, "")

	ptr, got, err := repo.GetLastRoomMessage(ctx, roomID, base.Add(time.Hour), base.Add(-time.Hour))
	require.NoError(t, err)
	require.NotNil(t, ptr)
	assert.Equal(t, "m-user", ptr.MessageID)
	require.NotNil(t, got)
	assert.Equal(t, "m-user", got.MessageID)
}
