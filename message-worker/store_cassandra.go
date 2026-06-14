package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
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
	cipher      atrest.Cipher // nil when ATREST_ENABLED=false
}

func NewCassandraStore(session *gocql.Session, bucket msgbucket.Sizer, cipher atrest.Cipher) *CassandraStore {
	return &CassandraStore{cassSession: session, bucket: bucket, cipher: cipher}
}

// SaveMessage inserts msg into both messages_by_room and messages_by_id via a
// single UnloggedBatch so the two denormalized writes share one coordinator
// round-trip. UnloggedBatch (not LoggedBatch) because we don't need batch-log
// atomicity: each INSERT is idempotent on its primary key, and on partial
// failure JetStream redelivers and both INSERTs re-run safely.
//
// When s.cipher is non-nil, the user-authored body fields (msg, sys_msg_data,
// quoted_parent_message body) are encrypted into enc_payload + enc_meta and
// the legacy plaintext columns are left null. When s.cipher is nil the
// legacy plaintext batch runs unchanged.
func (s *CassandraStore) SaveMessage(ctx context.Context, msg *model.Message, sender *cassParticipant, siteID string) error {
	if s.cipher != nil {
		return s.saveMessageEncrypted(ctx, msg, sender, siteID)
	}
	b := s.bucket.Of(msg.CreatedAt)
	mentions := toMentionSet(msg.Mentions)

	batch := s.cassSession.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	batch.Query(
		`INSERT INTO messages_by_room
		   (room_id, bucket, created_at, message_id, sender, msg, site_id, updated_at,
		    mentions, type, sys_msg_data, tshow, quoted_parent_message,
		    attachments, card, card_action, file)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.RoomID, b, msg.CreatedAt, msg.ID, sender, msg.Content, siteID, msg.CreatedAt,
		mentions, msg.Type, msg.SysMsgData, msg.TShow, msg.QuotedParentMessage,
		msg.Attachments, msg.Card, msg.CardAction, msg.File,
	)
	batch.Query(
		`INSERT INTO messages_by_id
		   (message_id, created_at, room_id, sender, msg, site_id, updated_at,
		    mentions, type, sys_msg_data, tshow, quoted_parent_message,
		    attachments, card, card_action, file)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.ID, msg.CreatedAt, msg.RoomID, sender, msg.Content, siteID, msg.CreatedAt,
		mentions, msg.Type, msg.SysMsgData, msg.TShow, msg.QuotedParentMessage,
		msg.Attachments, msg.Card, msg.CardAction, msg.File,
	)
	if err := s.cassSession.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("save message %s: %w", msg.ID, err)
	}
	return nil
}

// saveMessageEncrypted is the cipher-enabled counterpart to SaveMessage.
// It encrypts the user-authored body fields once and writes the resulting
// payload + nonce into both rows via the same UnloggedBatch the legacy
// path uses.
func (s *CassandraStore) saveMessageEncrypted(ctx context.Context, msg *model.Message, sender *cassParticipant, siteID string) error {
	cm := buildCassandraMessage(msg)
	enc := atrest.SplitForEncryption(&cm)
	payload, meta, err := s.cipher.Encrypt(ctx, cm.RoomID, enc)
	if err != nil {
		return fmt.Errorf("encrypt message %s in room %s: %w", cm.MessageID, cm.RoomID, err)
	}
	atrest.StripEncryptedFields(&cm)
	encMeta := &cassandra.EncMeta{Nonce: meta.Nonce}
	b := s.bucket.Of(msg.CreatedAt)
	mentions := toMentionSet(msg.Mentions)

	// Encrypted INSERTs explicitly bind NULL for every encrypted body column
	// so a JetStream redelivery (or federation replay) of a pre-rollout legacy
	// message can't leave the row in a hybrid plaintext+encrypted state. CQL
	// INSERT does not null unspecified columns on key collision, so without
	// these explicit NULLs an upsert over a legacy row would preserve plaintext
	// attachments/card alongside the new enc_payload, and decryptIfNeeded would
	// later overwrite them with empty fields from the bundle. sys_msg_data is
	// NOT encrypted, so it is written as plaintext like any other metadata column.
	batch := s.cassSession.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	batch.Query(
		`INSERT INTO messages_by_room
		   (room_id, bucket, created_at, message_id, sender, site_id, updated_at,
		    mentions, type, tshow, quoted_parent_message, sys_msg_data,
		    msg, attachments, card, card_action, file,
		    enc_payload, enc_meta)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null, null, null, null, null, ?, ?)`,
		msg.RoomID, b, msg.CreatedAt, msg.ID, sender, siteID, msg.CreatedAt,
		mentions, msg.Type, msg.TShow, cm.QuotedParentMessage, msg.SysMsgData, payload, encMeta,
	)
	batch.Query(
		`INSERT INTO messages_by_id
		   (message_id, created_at, room_id, sender, site_id, updated_at,
		    mentions, type, tshow, quoted_parent_message, sys_msg_data,
		    msg, attachments, card, card_action, file,
		    enc_payload, enc_meta)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null, null, null, null, null, ?, ?)`,
		msg.ID, msg.CreatedAt, msg.RoomID, sender, siteID, msg.CreatedAt,
		mentions, msg.Type, msg.TShow, cm.QuotedParentMessage, msg.SysMsgData, payload, encMeta,
	)
	if err := s.cassSession.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("save message %s: %w", msg.ID, err)
	}
	return nil
}

// SaveThreadMessage writes the reply to messages_by_id and then inserts into
// thread_messages_by_thread. Both writes are plain INSERTs (no LWT): JetStream
// MsgID dedup prevents double-delivery at the consumer level, so re-inserting
// an identical row is safe and avoids the 5–10× Paxos overhead of IF NOT EXISTS.
// countAndSetParentTcount derives tcount from a COUNT query and blind-SETs it,
// which is idempotent on redelivery without any CAS.
func (s *CassandraStore) SaveThreadMessage(ctx context.Context, msg *model.Message, sender *cassParticipant, siteID string, threadRoomID string) (*int, error) {
	if s.cipher != nil {
		return s.saveThreadMessageEncrypted(ctx, msg, sender, siteID, threadRoomID)
	}

	mentions := toMentionSet(msg.Mentions)

	if err := s.cassSession.Query(
		`INSERT INTO messages_by_id
		 (message_id, created_at, room_id, sender, msg, site_id, updated_at, mentions,
		  thread_room_id, thread_parent_id, thread_parent_created_at, type, sys_msg_data, tshow, quoted_parent_message,
		  attachments, card, card_action, file)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.ID, msg.CreatedAt, msg.RoomID, sender, msg.Content, siteID, msg.CreatedAt, mentions,
		threadRoomID, msg.ThreadParentMessageID, msg.ThreadParentMessageCreatedAt, msg.Type, msg.SysMsgData, msg.TShow, msg.QuotedParentMessage,
		msg.Attachments, msg.Card, msg.CardAction, msg.File,
	).WithContext(ctx).Exec(); err != nil {
		return nil, fmt.Errorf("insert thread message %s into messages_by_id: %w", msg.ID, err)
	}

	if err := s.cassSession.Query(
		`INSERT INTO thread_messages_by_thread
		 (thread_room_id, created_at, message_id, room_id, thread_parent_id, sender, msg,
		  site_id, updated_at, mentions, type, sys_msg_data, quoted_parent_message,
		  attachments, card, card_action, file)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		threadRoomID, msg.CreatedAt, msg.ID, msg.RoomID, msg.ThreadParentMessageID,
		sender, msg.Content, siteID, msg.CreatedAt, mentions,
		msg.Type, msg.SysMsgData, msg.QuotedParentMessage,
		msg.Attachments, msg.Card, msg.CardAction, msg.File,
	).WithContext(ctx).Exec(); err != nil {
		return nil, fmt.Errorf("insert thread message %s into thread_messages_by_thread: %w", msg.ID, err)
	}

	// TShow ("also send to channel"): dual-write the reply into messages_by_room
	// so it shows up in the parent room's channel timeline on history loads.
	// A third plain INSERT — NOT a SaveMessage call, which would double-write
	// messages_by_id. The row uses the reply's own created_at (interleaves
	// correctly in the timeline) and the same bucket sizer as the channel path.
	// tshow + thread_parent_id + thread_parent_created_at must be populated:
	// history-service's quote access-window logic redacts TShow rows that lack
	// the parent fields (legacyTShowMissingParentTime).
	if msg.TShow {
		if err := s.cassSession.Query(
			`INSERT INTO messages_by_room
			 (room_id, bucket, created_at, message_id, sender, msg, site_id, updated_at, mentions,
			  thread_room_id, thread_parent_id, thread_parent_created_at, type, sys_msg_data, tshow, quoted_parent_message,
			  attachments, card, card_action, file)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			msg.RoomID, s.bucket.Of(msg.CreatedAt), msg.CreatedAt, msg.ID, sender, msg.Content, siteID, msg.CreatedAt, mentions,
			threadRoomID, msg.ThreadParentMessageID, msg.ThreadParentMessageCreatedAt, msg.Type, msg.SysMsgData, msg.TShow, msg.QuotedParentMessage,
			msg.Attachments, msg.Card, msg.CardAction, msg.File,
		).WithContext(ctx).Exec(); err != nil {
			return nil, fmt.Errorf("insert tshow thread message %s into messages_by_room: %w", msg.ID, err)
		}
	}

	return s.countAndSetParentTcount(ctx, msg, threadRoomID)
}

// saveThreadMessageEncrypted is the cipher-enabled counterpart to
// SaveThreadMessage. Both writes are plain INSERTs — see SaveThreadMessage for
// the rationale (JetStream MsgID dedup + idempotent countAndSetParentTcount).
//
// Encrypted body columns (msg, attachments, card, card_action, file) are bound
// to NULL so a redelivered pre-encryption row cannot end up in a hybrid
// plaintext+encrypted state. sys_msg_data is unencrypted and written as
// plaintext in both rows.
func (s *CassandraStore) saveThreadMessageEncrypted(ctx context.Context, msg *model.Message, sender *cassParticipant, siteID string, threadRoomID string) (*int, error) {
	cm := buildCassandraMessage(msg)
	enc := atrest.SplitForEncryption(&cm)
	payload, meta, err := s.cipher.Encrypt(ctx, cm.RoomID, enc)
	if err != nil {
		return nil, fmt.Errorf("encrypt message %s in room %s: %w", cm.MessageID, cm.RoomID, err)
	}
	atrest.StripEncryptedFields(&cm)
	encMeta := &cassandra.EncMeta{Nonce: meta.Nonce}
	mentions := toMentionSet(msg.Mentions)

	if err = s.cassSession.Query(
		`INSERT INTO messages_by_id
		 (message_id, created_at, room_id, sender, site_id, updated_at, mentions,
		  thread_room_id, thread_parent_id, thread_parent_created_at, type, tshow,
		  quoted_parent_message, sys_msg_data,
		  msg, attachments, card, card_action, file,
		  enc_payload, enc_meta)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null, null, null, null, null, ?, ?)`,
		msg.ID, msg.CreatedAt, msg.RoomID, sender, siteID, msg.CreatedAt, mentions,
		threadRoomID, msg.ThreadParentMessageID, msg.ThreadParentMessageCreatedAt, msg.Type, msg.TShow,
		cm.QuotedParentMessage, msg.SysMsgData, payload, encMeta,
	).WithContext(ctx).Exec(); err != nil {
		return nil, fmt.Errorf("insert thread message %s into messages_by_id: %w", msg.ID, err)
	}

	if err := s.cassSession.Query(
		`INSERT INTO thread_messages_by_thread
		 (thread_room_id, created_at, message_id, room_id, thread_parent_id,
		  sender, site_id, updated_at, mentions, type, quoted_parent_message, sys_msg_data,
		  msg, attachments, card, card_action, file,
		  enc_payload, enc_meta)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null, null, null, null, null, ?, ?)`,
		threadRoomID, msg.CreatedAt, msg.ID, msg.RoomID, msg.ThreadParentMessageID,
		sender, siteID, msg.CreatedAt, mentions, msg.Type, cm.QuotedParentMessage, msg.SysMsgData,
		payload, encMeta,
	).WithContext(ctx).Exec(); err != nil {
		return nil, fmt.Errorf("insert thread message %s into thread_messages_by_thread: %w", msg.ID, err)
	}

	// TShow dual-write into messages_by_room — see SaveThreadMessage for the
	// rationale. Reuses the same encrypted bundle (payload + nonce) the two
	// writes above bind, matching saveMessageEncrypted's both-tables pattern;
	// plaintext body columns are bound NULL for the same hybrid-state reason.
	if msg.TShow {
		if err := s.cassSession.Query(
			`INSERT INTO messages_by_room
			 (room_id, bucket, created_at, message_id, sender, site_id, updated_at, mentions,
			  thread_room_id, thread_parent_id, thread_parent_created_at, type, tshow,
			  quoted_parent_message, sys_msg_data,
			  msg, attachments, card, card_action, file,
			  enc_payload, enc_meta)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null, null, null, null, null, ?, ?)`,
			msg.RoomID, s.bucket.Of(msg.CreatedAt), msg.CreatedAt, msg.ID, sender, siteID, msg.CreatedAt, mentions,
			threadRoomID, msg.ThreadParentMessageID, msg.ThreadParentMessageCreatedAt, msg.Type, msg.TShow,
			cm.QuotedParentMessage, msg.SysMsgData, payload, encMeta,
		).WithContext(ctx).Exec(); err != nil {
			return nil, fmt.Errorf("insert tshow thread message %s into messages_by_room: %w", msg.ID, err)
		}
	}

	return s.countAndSetParentTcount(ctx, msg, threadRoomID)
}

// buildCassandraMessage projects the user-authored fields of msg into a
// cassandra.Message for encryption. The encrypted content fields are Msg
// (Content), Attachments, Card, CardAction, File and the QuotedParentMessage
// body. sys_msg_data is not encrypted; columns bound by SaveMessage directly
// are left out.
//
// The returned QuotedParentMessage is a fresh struct so that
// StripEncryptedFields nulling its Msg/Attachments fields does not mutate
// the caller's *model.Message.
func buildCassandraMessage(msg *model.Message) cassandra.Message {
	cm := cassandra.Message{
		RoomID:      msg.RoomID,
		MessageID:   msg.ID,
		Msg:         msg.Content,
		Attachments: msg.Attachments,
		Card:        msg.Card,
		CardAction:  msg.CardAction,
		File:        msg.File,
	}
	if msg.QuotedParentMessage != nil {
		q := *msg.QuotedParentMessage
		cm.QuotedParentMessage = &q
	}
	return cm
}

// countThreadReplies counts non-deleted rows in the thread_messages_by_thread
// partition for threadRoomID. message-worker does not write the deleted column
// on INSERT (it remains NULL), so the Go-side filter treats NULL the same as
// false — only rows where deleted is explicitly true are excluded.
func (s *CassandraStore) countThreadReplies(ctx context.Context, threadRoomID string) (int, error) {
	iter := s.cassSession.Query(
		`SELECT deleted FROM thread_messages_by_thread WHERE thread_room_id = ?`,
		threadRoomID,
	).WithContext(ctx).Iter()
	var deleted *bool
	n := 0
	for iter.Scan(&deleted) {
		if deleted == nil || !*deleted {
			n++
		}
	}
	if err := iter.Close(); err != nil {
		return 0, fmt.Errorf("count thread replies for thread %s: %w", threadRoomID, err)
	}
	return n, nil
}

// setParentTcount blind-SETs tcount on the parent row in both messages_by_id
// and messages_by_room. No IF clause — the value is always derived from the
// authoritative COUNT, so overwrites are idempotent on any redelivery.
func (s *CassandraStore) setParentTcount(ctx context.Context, msg *model.Message, n int) error {
	parentID := msg.ThreadParentMessageID
	parentCreatedAt := *msg.ThreadParentMessageCreatedAt
	parentBucket := s.bucket.Of(parentCreatedAt)
	if err := s.cassSession.Query(
		`UPDATE messages_by_id SET tcount = ? WHERE message_id = ? AND created_at = ?`,
		n, parentID, parentCreatedAt,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("set tcount on parent %s in messages_by_id: %w", parentID, err)
	}
	if err := s.cassSession.Query(
		`UPDATE messages_by_room SET tcount = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		n, msg.RoomID, parentBucket, parentCreatedAt, parentID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("set tcount on parent %s in messages_by_room: %w", parentID, err)
	}
	return nil
}

// countAndSetParentTcount derives tcount from the thread partition COUNT and
// blind-SETs it on the parent row in both Cassandra tables. Returns (nil, nil)
// when ThreadParentMessageCreatedAt is unset (no parent key available).
// This approach is crash-safe: COUNT + blind SET is idempotent on redelivery,
// avoiding the 2PC window of the old CAS increment.
func (s *CassandraStore) countAndSetParentTcount(ctx context.Context, msg *model.Message, threadRoomID string) (*int, error) {
	if msg.ThreadParentMessageCreatedAt == nil {
		return nil, nil
	}
	n, err := s.countThreadReplies(ctx, threadRoomID)
	if err != nil {
		return nil, fmt.Errorf("count thread replies: %w", err)
	}
	if err := s.setParentTcount(ctx, msg, n); err != nil {
		return nil, err
	}
	return &n, nil
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
			"room_id", roomID,
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
