package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/msgbucket"
)

// errEncryptedMessage is returned by rejectEncryptedMessage when a row
// carries an at-rest-encrypted payload — see its doc comment for why this
// migrator does not support that case.
var errEncryptedMessage = errors.New("message has an at-rest-encrypted payload; this migrator only reads the plaintext msg/attachments/card columns")

// messageColumns is the explicit set of messages_by_room columns this
// service needs, excluding columns no search doc ever uses (mentions,
// quoted_parent_message, card_action, sys_msg_data, pinned_at/pinned_by,
// visible_to, reactions, type). enc_payload is included solely so
// rejectEncryptedMessage can detect and hard-fail on encrypted rows below —
// this job otherwise reads only the plaintext msg/attachments/card columns.
// It relies on an operational assumption, not a structural guarantee: the
// source rows for a one-time backfill are expected to have been written
// directly into those plaintext columns (e.g. by a prior data migration
// into Cassandra), never through the live at-rest-encryption write path.
// This service is not intended to be re-run once a site is taking live
// traffic with ATREST_ENABLED=true — see docs/search_index_migration_spec.md.
const messageColumns = "room_id, created_at, message_id, sender, msg, attachments, card, " +
	"tshow, thread_parent_id, thread_parent_created_at, deleted, site_id, edited_at, updated_at, enc_payload"

// rejectEncryptedMessage fails loudly instead of silently indexing blank
// content when a row's plaintext columns were never populated (i.e. it was
// written through the at-rest-encryption path this migrator does not
// support decrypting).
func rejectEncryptedMessage(row *cassandra.Message) error {
	if len(row.EncPayload) > 0 {
		return fmt.Errorf("message %s in room %s: %w", row.MessageID, row.RoomID, errEncryptedMessage)
	}
	return nil
}

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
			&row.EditedAt, &row.UpdatedAt, &row.EncPayload) {

			if row.Deleted {
				row = cassandra.Message{}
				continue
			}
			if row.SiteID == "" {
				row.SiteID = siteID
			}
			if err := rejectEncryptedMessage(&row); err != nil {
				_ = iter.Close()
				return err
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
