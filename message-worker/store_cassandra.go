package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// errMessageNotFound is returned by GetMessageSender when the message row is
// missing from Cassandra. Handler code checks for this sentinel to ack-and-skip
// instead of NAK'ing (which would cause infinite JetStream redelivery).
var errMessageNotFound = errors.New("message not found")

// cassParticipant maps to the Cassandra "Participant" UDT.
// cql struct tags tell gocql's reflection-based UDT marshaler how to map each
// Go field to its Cassandra UDT field name. Without these tags, gocql would
// lowercase the Go field names (e.g. "EngName" → "engname") which would not
// match the snake_case UDT fields (e.g. "eng_name").
type cassParticipant struct {
	ID          string `cql:"id"`
	EngName     string `cql:"eng_name"`
	CompanyName string `cql:"company_name"` // ChineseName
	Account     string `cql:"account"`
	AppID       string `cql:"app_id"`
	AppName     string `cql:"app_name"`
	IsBot       bool   `cql:"is_bot"`
}

// toMentionSet converts []model.Participant to []*cassParticipant for binding
// to a Cassandra SET<FROZEN<"Participant">> column.
func toMentionSet(mentions []model.Participant) []*cassParticipant {
	if len(mentions) == 0 {
		return nil
	}
	result := make([]*cassParticipant, len(mentions))
	for i, m := range mentions {
		result[i] = &cassParticipant{
			ID:          m.UserID,
			EngName:     m.EngName,
			CompanyName: m.ChineseName,
			Account:     m.Account,
		}
	}
	return result
}

// CassandraStore implements Store using a Cassandra session.
type CassandraStore struct {
	cassSession *gocql.Session
	bucket      msgbucket.Sizer
}

func NewCassandraStore(session *gocql.Session, bucket msgbucket.Sizer) *CassandraStore {
	return &CassandraStore{cassSession: session, bucket: bucket}
}

// SaveMessage inserts msg into both messages_by_room and messages_by_id via a
// single UnloggedBatch so the two denormalized writes share one coordinator
// round-trip. UnloggedBatch (not LoggedBatch) because we don't need batch-log
// atomicity: each INSERT is idempotent on its primary key, and on partial
// failure JetStream redelivers and both INSERTs re-run safely.
func (s *CassandraStore) SaveMessage(ctx context.Context, msg *model.Message, sender *cassParticipant, siteID string) error {
	b := s.bucket.Of(msg.CreatedAt)
	mentions := toMentionSet(msg.Mentions)

	batch := s.cassSession.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	batch.Query(
		`INSERT INTO messages_by_room
		   (room_id, bucket, created_at, message_id, sender, msg, site_id, updated_at,
		    mentions, type, sys_msg_data, tshow, quoted_parent_message)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.RoomID, b, msg.CreatedAt, msg.ID, sender, msg.Content, siteID, msg.CreatedAt,
		mentions, msg.Type, msg.SysMsgData, msg.TShow, msg.QuotedParentMessage,
	)
	batch.Query(
		`INSERT INTO messages_by_id
		   (message_id, created_at, room_id, sender, msg, site_id, updated_at,
		    mentions, type, sys_msg_data, tshow, quoted_parent_message)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.ID, msg.CreatedAt, msg.RoomID, sender, msg.Content, siteID, msg.CreatedAt,
		mentions, msg.Type, msg.SysMsgData, msg.TShow, msg.QuotedParentMessage,
	)
	if err := s.cassSession.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("save message %s: %w", msg.ID, err)
	}
	return nil
}

// SaveThreadMessage batches the two regular inserts (messages_by_id and
// thread_messages_by_room) into one round-trip via UnloggedBatch — same
// rationale as SaveMessage. incrementParentTcount stays separate because
// it uses Lightweight Transactions (CAS), which cannot be combined with
// non-LWT statements in a single batch.
func (s *CassandraStore) SaveThreadMessage(ctx context.Context, msg *model.Message, sender *cassParticipant, siteID string, threadRoomID string) error {
	b := s.bucket.Of(msg.CreatedAt)
	mentions := toMentionSet(msg.Mentions)

	batch := s.cassSession.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	batch.Query(
		`INSERT INTO messages_by_id
		 (message_id, created_at, room_id, sender, msg, site_id, updated_at, mentions,
		  thread_room_id, thread_parent_id, thread_parent_created_at, type, sys_msg_data, tshow, quoted_parent_message)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.ID, msg.CreatedAt, msg.RoomID, sender, msg.Content, siteID, msg.CreatedAt, mentions,
		threadRoomID, msg.ThreadParentMessageID, msg.ThreadParentMessageCreatedAt, msg.Type, msg.SysMsgData, msg.TShow, msg.QuotedParentMessage,
	)
	batch.Query(
		`INSERT INTO thread_messages_by_room
		 (room_id, bucket, thread_room_id, created_at, message_id, thread_parent_id, sender, msg,
		  site_id, updated_at, mentions, type, sys_msg_data, quoted_parent_message)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.RoomID, b, threadRoomID, msg.CreatedAt, msg.ID, msg.ThreadParentMessageID,
		sender, msg.Content, siteID, msg.CreatedAt, mentions,
		msg.Type, msg.SysMsgData, msg.QuotedParentMessage,
	)
	if err := s.cassSession.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("save thread message %s: %w", msg.ID, err)
	}

	if err := s.incrementParentTcount(ctx, msg); err != nil {
		return err
	}

	return nil
}

// casMaxRetries is the maximum number of CAS attempts per tcount increment.
// A conflict means another thread-reply landed between our read and write;
// 16 attempts is sufficient for any realistic burst while preventing an
// infinite loop if something unexpected keeps the row locked.
const casMaxRetries = 16

// casIncrement atomically increments the nullable INT counter starting at
// initial by calling update(newVal, expected) in a retry loop. On conflict
// (applied==false) it retries with the value returned by update.  Returns an
// error after maxRetries consecutive failures.
func casIncrement(maxRetries int, initial *int, update func(newVal int, expected *int) (applied bool, current *int, err error)) error {
	tcount := initial
	for range maxRetries {
		newVal := 1
		if tcount != nil {
			newVal = *tcount + 1
		}
		applied, current, err := update(newVal, tcount)
		if err != nil {
			return err
		}
		if applied {
			return nil
		}
		tcount = current
	}
	return fmt.Errorf("cas increment exceeded %d retries", maxRetries)
}

// incrementParentTcount increments tcount on the parent message row in both
// messages_by_id and messages_by_room using Cassandra Lightweight Transactions
// (IF tcount = ?). Each table is incremented independently via casIncrement,
// which retries up to casMaxRetries times on CAS conflict.
// Binding a nil *int as the IF condition evaluates to IF tcount = null, which
// handles the initial case where tcount has never been set on the parent row.
// If ThreadParentMessageCreatedAt is nil the increment is silently skipped —
// tcount cannot be updated without the full primary key of the parent row.
func (s *CassandraStore) incrementParentTcount(ctx context.Context, msg *model.Message) error {
	if msg.ThreadParentMessageCreatedAt == nil {
		return nil
	}
	parentID := msg.ThreadParentMessageID
	parentCreatedAt := *msg.ThreadParentMessageCreatedAt
	parentBucket := s.bucket.Of(parentCreatedAt)

	// CAS increment on messages_by_id (no bucket — table unchanged).
	var tcount *int
	if err := s.cassSession.Query(
		`SELECT tcount FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		parentID, parentCreatedAt,
	).WithContext(ctx).Scan(&tcount); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("read tcount for parent message %s: %w", parentID, err)
	}
	if err := casIncrement(casMaxRetries, tcount, func(newVal int, expected *int) (bool, *int, error) {
		var current *int
		applied, err := s.cassSession.Query(
			`UPDATE messages_by_id SET tcount = ? WHERE message_id = ? AND created_at = ? IF tcount = ?`,
			newVal, parentID, parentCreatedAt, expected,
		).WithContext(ctx).ScanCAS(&current)
		return applied, current, err
	}); err != nil {
		return fmt.Errorf("cas tcount in messages_by_id for parent %s: %w", parentID, err)
	}

	if err := s.cassSession.Query(
		`SELECT tcount FROM messages_by_room WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		msg.RoomID, parentBucket, parentCreatedAt, parentID,
	).WithContext(ctx).Scan(&tcount); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("read tcount in messages_by_room for parent %s: %w", parentID, err)
	}
	if err := casIncrement(casMaxRetries, tcount, func(newVal int, expected *int) (bool, *int, error) {
		var current *int
		applied, err := s.cassSession.Query(
			`UPDATE messages_by_room SET tcount = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ? IF tcount = ?`,
			newVal, msg.RoomID, parentBucket, parentCreatedAt, parentID, expected,
		).WithContext(ctx).ScanCAS(&current)
		return applied, current, err
	}); err != nil {
		return fmt.Errorf("cas tcount in messages_by_room for parent %s: %w", parentID, err)
	}

	return nil
}

// IF EXISTS prevents phantom rows on missing parents; misses log at ERROR
// because a silent miss permanently breaks thread reads for that parent.
func (s *CassandraStore) UpdateParentMessageThreadRoomID(ctx context.Context, parentMessageID, roomID string, parentCreatedAt time.Time, threadRoomID string) error {
	parentBucket := s.bucket.Of(parentCreatedAt)

	applied, err := s.cassSession.Query(
		`UPDATE messages_by_id SET thread_room_id = ? WHERE message_id = ? AND created_at = ? IF EXISTS`,
		threadRoomID, parentMessageID, parentCreatedAt,
	).WithContext(ctx).ScanCAS()
	if err != nil {
		return fmt.Errorf("set thread_room_id on parent %s in messages_by_id: %w", parentMessageID, err)
	}
	if !applied {
		slog.Error("thread_room_id stamp on messages_by_id missed: parent row not found at the given (message_id, created_at) coordinates",
			"request_id", natsutil.RequestIDFromContext(ctx),
			"messageID", parentMessageID,
			"parentCreatedAt", parentCreatedAt,
			"threadRoomID", threadRoomID,
		)
	}

	applied, err = s.cassSession.Query(
		`UPDATE messages_by_room SET thread_room_id = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ? IF EXISTS`,
		threadRoomID, roomID, parentBucket, parentCreatedAt, parentMessageID,
	).WithContext(ctx).ScanCAS()
	if err != nil {
		return fmt.Errorf("set thread_room_id on parent %s in messages_by_room: %w", parentMessageID, err)
	}
	if !applied {
		slog.Error("thread_room_id stamp on messages_by_room missed: parent row not found at the given (room_id, bucket, created_at, message_id) coordinates",
			"request_id", natsutil.RequestIDFromContext(ctx),
			"messageID", parentMessageID,
			"roomID", roomID,
			"bucket", parentBucket,
			"parentCreatedAt", parentCreatedAt,
			"threadRoomID", threadRoomID,
		)
	}
	return nil
}

// GetMessageSender reads the sender UDT from messages_by_id for the given message ID.
// Returns an error if the message does not exist.
func (s *CassandraStore) GetMessageSender(ctx context.Context, messageID string) (*cassParticipant, error) {
	var sender cassParticipant
	if err := s.cassSession.Query(
		`SELECT sender FROM messages_by_id WHERE message_id = ? LIMIT 1`,
		messageID,
	).WithContext(ctx).Scan(&sender); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return nil, fmt.Errorf("get sender for message %s: %w", messageID, errMessageNotFound)
		}
		return nil, fmt.Errorf("get sender for message %s: %w", messageID, err)
	}
	return &sender, nil
}
