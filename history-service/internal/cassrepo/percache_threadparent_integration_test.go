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

// TestPerBucketCache_ThreadParentMutatedByAnotherService_StaysStale records an
// ACCEPTED GAP. It asserts what the cache does today, not what it should do.
//
// The design rests on sealed buckets changing only through history-service's own
// mutation paths (edit, delete, pin, react), each of which busts the bucket it
// touched. That does not hold. message-worker and bot-message-worker UPDATE
// messages_by_room at the *thread parent's* bucket, which is arbitrary and
// usually sealed —
//
//	message-worker/store_cassandra.go     setParentTcountAndTlm
//	    UPDATE messages_by_room SET tcount = ?, thread_last_msg_at = ? WHERE ...
//	message-worker/store_cassandra.go     UpdateParentMessageThreadRoomID
//	    UPDATE messages_by_room SET thread_room_id = ? WHERE ... IF EXISTS
//	bot-message-worker/store_cassandra.go countAndSetParentTcount
//
// tcount, thread_last_msg_at and thread_room_id are all in baseColumns, so the
// cache serves them, and neither worker has a bust hook into history-service.
// Replying in a thread on a parent older than the current bucket window
// therefore leaves a stale reply count on every read served from cache, bounded
// by the entry's TTL rather than by the mutation.
//
// User-visible effect: chat-frontend gates the whole thread affordance on
// `message.tcount > 0` (MessageRow.jsx), so a FIRST reply on such a parent
// renders no "N replies" button at all and the thread is unreachable from the
// room view; later replies undercount. The client's live tcount bump does not
// cover it — that requires the parent to already be in the loaded message
// buffer, which for a parent this old it usually is not.
//
// history-service's own delete path already busts the thread parent's bucket
// (write.go, SoftDeleteMessage) for exactly this reason; the increment path just
// lives in another service.
//
// This is why HISTORY_BUCKET_CACHE_ENABLED defaults to false. Closing the gap
// needs cross-replica invalidation, not just a shared-tier DEL: Cache.Get
// returns on an L1 hit before consulting L2, so a replica already holding the
// bucket in its in-process L1 keeps serving the stale row whatever happens in
// Valkey.
//
// WHEN THIS IS FIXED: invert the assertions — assertStale becomes assertFresh
// (tcount 1, thread_room_id stamped) — and drop this header.
func TestPerBucketCache_ThreadParentMutatedByAnotherService_StaysStale(t *testing.T) {
	base := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	now := base.Add(48 * time.Hour) // two windows on, so base's bucket is sealed
	parentBucket := msgbucket.New(percacheWindow).Of(base)
	replyAt := now.Add(-time.Minute)

	tests := []struct {
		name string
		// mutate is the UPDATE the other service issues against the parent row.
		mutate func(t *testing.T, session *gocql.Session, roomID, messageID string)
		// assertStale is what a cached read serves after that UPDATE: the row as
		// it was before the worker touched it.
		assertStale func(t *testing.T, got models.Message)
		// assertFresh is what an uncached read of the same row serves. Pinning
		// both sides proves the write landed and isolates the staleness to the
		// cache, so this test also fails if the write path itself changes.
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
			assertStale: func(t *testing.T, got models.Message) {
				t.Helper()
				assert.Nil(t, got.TCount, "accepted gap: cached read misses the worker's reply count")
				assert.Nil(t, got.ThreadLastMsgAt, "accepted gap: cached read misses thread_last_msg_at")
			},
			assertFresh: func(t *testing.T, got models.Message) {
				t.Helper()
				require.NotNil(t, got.TCount)
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
			assertStale: func(t *testing.T, got models.Message) {
				t.Helper()
				assert.Empty(t, got.ThreadRoomID, "accepted gap: cached read misses the thread_room_id stamp")
			},
			assertFresh: func(t *testing.T, got models.Message) {
				t.Helper()
				assert.Equal(t, "thread-room-1", got.ThreadRoomID)
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

			cached, err := repo.GetMessagesBefore(ctx, roomID, now, floor, pr)
			require.NoError(t, err)
			require.Len(t, cached.Data, 1)
			tt.assertStale(t, cached.Data[0])

			uncached := NewRepository(session, msgbucket.New(percacheWindow), 365, nil)
			live, err := uncached.GetMessagesBefore(ctx, roomID, now, floor, pr)
			require.NoError(t, err)
			require.Len(t, live.Data, 1)
			tt.assertFresh(t, live.Data[0])
		})
	}
}
