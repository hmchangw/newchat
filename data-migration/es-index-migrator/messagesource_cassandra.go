package main

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/msgbucket"
)

// messageColumns is the explicit set of messages_by_room columns this
// service needs, excluding columns no search doc ever uses (mentions,
// quoted_parent_message, card_action, sys_msg_data, pinned_at/pinned_by,
// visible_to, reactions, type) and excluding enc_payload/enc_meta — this
// job's source rows were written directly into the plaintext msg/
// attachments/card columns, never through the live at-rest-encryption
// write path, so there is nothing to decrypt here.
const messageColumns = "room_id, created_at, message_id, sender, msg, attachments, card, " +
	"tshow, thread_parent_id, thread_parent_created_at, deleted, site_id, edited_at, updated_at"

const messagesByRoomQuery = "SELECT " + messageColumns + " FROM messages_by_room " +
	"WHERE room_id = ? AND bucket = ? AND created_at >= ? AND created_at < ?"

type cassandraMessageSource struct {
	session *gocql.Session
	sizer   msgbucket.Sizer
}

// Compile-time assertion that *cassandraMessageSource satisfies MessageSource.
var _ MessageSource = (*cassandraMessageSource)(nil)

func newCassandraMessageSource(session *gocql.Session, sizer msgbucket.Sizer) *cassandraMessageSource {
	return &cassandraMessageSource{session: session, sizer: sizer}
}

// bucketRange returns every bucket value that can contain a row with
// created_at in [from, to). Half-open: a bucket starting exactly at `to`
// holds no row < to, so it is excluded.
func bucketRange(sizer msgbucket.Sizer, from, to time.Time) []int64 {
	var buckets []int64
	for b := sizer.Of(from); b < to.UnixMilli(); b = sizer.Next(b) {
		buckets = append(buckets, b)
	}
	return buckets
}

func (s *cassandraMessageSource) StreamMessages(
	ctx context.Context, siteID, roomID string, from, to time.Time, fn func(cassandra.Message) error,
) error {
	for _, bucket := range bucketRange(s.sizer, from, to) {
		iter := s.session.Query(messagesByRoomQuery, roomID, bucket, from, to).WithContext(ctx).Iter()

		var row cassandra.Message
		for iter.Scan(&row.RoomID, &row.CreatedAt, &row.MessageID, &row.Sender, &row.Msg, &row.Attachments,
			&row.Card, &row.TShow, &row.ThreadParentID, &row.ThreadParentCreatedAt, &row.Deleted, &row.SiteID,
			&row.EditedAt, &row.UpdatedAt) {

			if row.Deleted {
				row = cassandra.Message{}
				continue
			}
			if row.SiteID == "" {
				row.SiteID = siteID
			}

			if err := fn(row); err != nil {
				_ = iter.Close()
				return fmt.Errorf("handle message %s in room %s: %w", row.MessageID, roomID, err)
			}

			row = cassandra.Message{}
		}
		if err := iter.Close(); err != nil {
			return fmt.Errorf("read messages_by_room for room %s bucket %d: %w", roomID, bucket, err)
		}
	}
	return nil
}
