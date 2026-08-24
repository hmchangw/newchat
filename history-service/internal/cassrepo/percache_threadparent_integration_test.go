//go:build integration

package cassrepo

import (
	"context"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/msgbucket"
)

// TestPerBucketCache_ThreadParentMutatedByAnotherService pins the cache to the
// mutations history-service does not perform.
//
// The design rests on "a sealed bucket changes only through the mutation paths
// (edit, delete, pin, react)", each of which busts the bucket it touched. That
// does not hold: message-worker and bot-message-worker UPDATE messages_by_room
// at the *thread parent's* bucket, which is arbitrary and usually sealed —
//
//	message-worker/store_cassandra.go:363 (setParentTcountAndTlm)
//	    UPDATE messages_by_room SET tcount = ?, thread_last_msg_at = ? WHERE ...
//	message-worker/store_cassandra.go:410 (thread_room_id stamp, first reply)
//	    UPDATE messages_by_room SET thread_room_id = ? WHERE ... IF EXISTS
//
// tcount, thread_last_msg_at and thread_room_id are all in baseColumns, so the
// cache serves them. Neither worker has a bust hook into history-service, so
// replying in a thread on an older message leaves every replica serving a stale
// reply count — and, for a first reply, no thread_room_id at all, meaning the
// thread affordance does not render — until the TTL expires.
//
// history-service's own delete path already busts the thread parent's bucket
// (write.go:378) for exactly this reason; the increment path just lives in
// another service.
//
// Each case reads once to populate the cache, applies the mutation the worker
// applies (no bust, because none exists), then re-reads and asserts the fresh
// value is served.
func TestPerBucketCache_ThreadParentMutatedByAnotherService(t *testing.T) {
	base := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	now := base.Add(48 * time.Hour) // two windows on, so base's bucket is sealed
	parentBucket := msgbucket.New(percacheWindow).Of(base)
	replyAt := now.Add(-time.Minute)

	tests := []struct {
		name string
		// mutate is the UPDATE the other service issues against the parent row.
		mutate func(t *testing.T, session *gocql.Session, roomID, messageID string)
		// assertFresh checks the re-read row reflects that UPDATE.
		assertFresh func(t *testing.T, got models.Message)
	}{
		{
			name: "thread reply updates parent tcount and thread_last_msg_at",
			mutate: func(t *testing.T, session *gocql.Session, roomID, messageID string) {
				t.Helper()
				require.NoError(t, session.Query(
					`UPDATE messages_by_room SET tcount = ?, thread_last_msg_at = ?
					 WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
					1, replyAt, roomID, parentBucket, base, messageID,
				).Exec())
			},
			assertFresh: func(t *testing.T, got models.Message) {
				t.Helper()
				require.NotNil(t, got.TCount, "cached read must serve the reply count the worker wrote")
				assert.Equal(t, 1, *got.TCount)
				require.NotNil(t, got.ThreadLastMsgAt)
				assert.WithinDuration(t, replyAt, *got.ThreadLastMsgAt, time.Second)
			},
		},
		{
			name: "first reply stamps thread_room_id on the parent",
			mutate: func(t *testing.T, session *gocql.Session, roomID, messageID string) {
				t.Helper()
				applied, err := session.Query(
					`UPDATE messages_by_room SET thread_room_id = ?
					 WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ? IF EXISTS`,
					"thread-room-1", roomID, parentBucket, base, messageID,
				).ScanCAS()
				require.NoError(t, err)
				require.True(t, applied, "parent row must exist for the stamp to apply")
			},
			assertFresh: func(t *testing.T, got models.Message) {
				t.Helper()
				assert.Equal(t, "thread-room-1", got.ThreadRoomID,
					"without a thread_room_id the client cannot open the thread")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			session := setupCassandra(t)
			roomID := "r-threadparent-" + t.Name()
			seedMessage(t, session, roomID, "parent", base)

			repo, _ := newBucketCachedRepo(t, session, 2000, now)
			ctx := context.Background()
			floor := base.Add(-30 * percacheWindow)
			pr := PageRequest{PageSize: 20}

			// Populate the cache for the parent's (now sealed) bucket.
			first, err := repo.GetMessagesBefore(ctx, roomID, now, floor, pr)
			require.NoError(t, err)
			require.Len(t, first.Data, 1)

			// The other service mutates the parent row. It has no way to bust.
			tt.mutate(t, session, roomID, "parent")

			second, err := repo.GetMessagesBefore(ctx, roomID, now, floor, pr)
			require.NoError(t, err)
			require.Len(t, second.Data, 1)
			tt.assertFresh(t, second.Data[0])
		})
	}
}
