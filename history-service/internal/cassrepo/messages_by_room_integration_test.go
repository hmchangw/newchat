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
	"github.com/hmchangw/chat/pkg/msgbucket"
)

// seedMessages inserts count messages into messages_by_room spaced 1 minute apart
// starting from base. bucket is derived from each message's created_at.
func seedMessages(t *testing.T, session *gocql.Session, roomID string, base time.Time, count int) {
	t.Helper()
	sizer := msgbucket.New(24 * time.Hour)
	sender := models.Participant{ID: "u1", Account: "user1"}
	for i := 0; i < count; i++ {
		ts := base.Add(time.Duration(i) * time.Minute)
		bucket := sizer.Of(ts)
		err := session.Query(
			`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg) VALUES (?, ?, ?, ?, ?, ?)`,
			roomID, bucket, ts, fmt.Sprintf("m%d", i), sender, fmt.Sprintf("msg-%d", i),
		).Exec()
		require.NoError(t, err)
	}
}

// seedMessage inserts a single message with an explicit createdAt and messageID.
func seedMessage(t *testing.T, session *gocql.Session, roomID, messageID string, createdAt time.Time) {
	t.Helper()
	sizer := msgbucket.New(24 * time.Hour)
	bucket := sizer.Of(createdAt)
	err := session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, site_id, updated_at, type)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		roomID, bucket, createdAt, messageID,
		map[string]interface{}{"id": "u1", "account": "alice"},
		"m", "site-A", createdAt, "text",
	).Exec()
	require.NoError(t, err)
}

func TestRepository_GetMessagesBefore(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seedMessages(t, session, "r1", base, 5)

	q, err := ParsePageRequest("", 3)
	require.NoError(t, err)

	page, err := repo.GetMessagesBefore(ctx, "r1", base.Add(10*time.Minute), time.Time{}, q)
	require.NoError(t, err)
	assert.Len(t, page.Data, 3)
	assert.True(t, page.Data[0].CreatedAt.After(page.Data[1].CreatedAt))
}

func TestRepository_GetMessagesBetweenDesc(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seedMessages(t, session, "r1", base, 5)

	q, err := ParsePageRequest("", 10)
	require.NoError(t, err)

	page, err := repo.GetMessagesBetweenDesc(ctx, "r1", base.Add(1*time.Minute), base.Add(4*time.Minute), q)
	require.NoError(t, err)
	assert.Len(t, page.Data, 2)                                          // m2 (2min), m3 (3min) — excludes 1min and 4min
	assert.True(t, page.Data[0].CreatedAt.After(page.Data[1].CreatedAt)) // DESC order
}

func TestRepository_GetMessagesAfter(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seedMessages(t, session, "r1", base, 5)

	q, err := ParsePageRequest("", 10)
	require.NoError(t, err)

	page, err := repo.GetMessagesAfter(ctx, "r1", base.Add(2*time.Minute), time.Now().UTC().Add(time.Hour), q)
	require.NoError(t, err)
	assert.Len(t, page.Data, 2)                                           // m3 (3min), m4 (4min) — strictly after 2min
	assert.True(t, page.Data[0].CreatedAt.Before(page.Data[1].CreatedAt)) // ASC order
}

func TestRepository_GetAllMessagesAsc(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seedMessages(t, session, "r1", base, 5)

	q, err := ParsePageRequest("", 3)
	require.NoError(t, err)

	page, err := repo.GetAllMessagesAsc(ctx, "r1", base.Add(-time.Hour), time.Now().UTC().Add(time.Hour), q)
	require.NoError(t, err)
	assert.Len(t, page.Data, 3)
	assert.True(t, page.Data[0].CreatedAt.Before(page.Data[1].CreatedAt)) // ASC order
	assert.True(t, page.HasNext)
}

func TestRepository_GetMessagesBefore_ThreadRoomID(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	sizer := msgbucket.New(24 * time.Hour)
	sender := models.Participant{ID: "u1", Account: "user1"}
	ts := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	bucket := sizer.Of(ts)
	require.NoError(t, session.Query(
		`INSERT INTO messages_by_room (room_id, bucket, created_at, message_id, sender, msg, thread_room_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"r-thread", bucket, ts, "m-thread", sender, "reply", "tr-42",
	).Exec())

	q, err := ParsePageRequest("", 10)
	require.NoError(t, err)

	page, err := repo.GetMessagesBefore(ctx, "r-thread", ts.Add(1*time.Minute), time.Time{}, q)
	require.NoError(t, err)
	require.Len(t, page.Data, 1)
	assert.Equal(t, "tr-42", page.Data[0].ThreadRoomID)
}

// --- New cross-bucket / floor / cap integration tests ---

func TestGetMessagesBefore_CrossBucketWalkDESC(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	roomID := "room-walk-desc"
	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		seedMessage(t, session, roomID, fmt.Sprintf("m%d", i), base.AddDate(0, 0, -i))
	}

	floor := base.AddDate(0, 0, -10)

	page, err := repo.GetMessagesBefore(ctx, roomID, base.Add(time.Second), floor, PageRequest{PageSize: 3})
	require.NoError(t, err)
	require.Len(t, page.Data, 3)
	assert.Equal(t, "m0", page.Data[0].MessageID)
	assert.Equal(t, "m1", page.Data[1].MessageID)
	assert.Equal(t, "m2", page.Data[2].MessageID)
	assert.True(t, page.HasNext)

	cursor, err := NewCursor(page.NextCursor)
	require.NoError(t, err)
	page2, err := repo.GetMessagesBefore(ctx, roomID, base.Add(time.Second), floor, PageRequest{Cursor: cursor, PageSize: 3})
	require.NoError(t, err)
	require.Len(t, page2.Data, 2)
	assert.Equal(t, "m3", page2.Data[0].MessageID)
	assert.Equal(t, "m4", page2.Data[1].MessageID)
	assert.False(t, page2.HasNext)
}

func TestGetMessagesBefore_FloorTerminates(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 365)
	ctx := context.Background()

	roomID := "room-floor"
	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		seedMessage(t, session, roomID, fmt.Sprintf("m%d", i), base.AddDate(0, 0, -i))
	}
	floor := base.AddDate(0, 0, -2) // only m0, m1, m2 survive the floor

	page, err := repo.GetMessagesBefore(ctx, roomID, base.Add(time.Second), floor, PageRequest{PageSize: 10})
	require.NoError(t, err)
	require.Len(t, page.Data, 3)
	assert.False(t, page.HasNext)
}

func TestGetMessagesBefore_MaxBucketsCap(t *testing.T) {
	session := setupCassandra(t)
	repo := NewRepository(session, msgbucket.New(24*time.Hour), 2) // cap at 2 buckets
	ctx := context.Background()

	roomID := "room-cap"
	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	floor := base.AddDate(0, 0, -10)

	page, err := repo.GetMessagesBefore(ctx, roomID, base, floor, PageRequest{PageSize: 50})
	require.NoError(t, err)
	assert.Empty(t, page.Data)
	assert.True(t, page.HasNext, "non-terminal cursor when cap reached")
}
