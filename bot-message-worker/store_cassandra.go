package main

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/bucketcache"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/threadcount"
)

// cassParticipant maps to the Cassandra "Participant" UDT (shared with user messages).
type cassParticipant struct {
	ID          string `cql:"id"`
	EngName     string `cql:"eng_name"`
	CompanyName string `cql:"company_name"`
	Account     string `cql:"account"`
	AppID       string `cql:"app_id"`
	AppName     string `cql:"app_name"`
	IsBot       bool   `cql:"is_bot"`
}

func toSender(msg *model.Message) *cassParticipant {
	return &cassParticipant{
		ID:      msg.UserID,
		Account: msg.UserAccount,
		EngName: msg.UserDisplayName,
		IsBot:   true,
	}
}

func toMentionSet(mentions []model.Participant) []*cassParticipant {
	if len(mentions) == 0 {
		return nil
	}
	out := make([]*cassParticipant, len(mentions))
	for i, m := range mentions {
		out[i] = &cassParticipant{
			ID:          m.UserID,
			EngName:     m.EngName,
			CompanyName: m.ChineseName,
			Account:     m.Account,
		}
	}
	return out
}

// CassandraStore implements Store; mirrors message-worker's UnloggedBatch write path.
type CassandraStore struct {
	sess   *gocql.Session
	bucket msgbucket.Sizer
	cipher atrest.Cipher
	// bucketCache invalidates history-service's per-bucket read cache after this
	// worker writes into a bucket that service may have cached. Nil disables it
	// (staleness then bounded by the cache TTL) — see bustParentBucket.
	bucketCache *bucketcache.Invalidator
}

func NewCassandraStore(sess *gocql.Session, bucket msgbucket.Sizer, cipher atrest.Cipher, opts ...func(*CassandraStore)) *CassandraStore {
	s := &CassandraStore{sess: sess, bucket: bucket, cipher: cipher}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// WithBucketCacheInvalidator wires the history-service bucket-cache invalidator.
// Pass nil (or omit) to leave invalidation off.
func WithBucketCacheInvalidator(inv *bucketcache.Invalidator) func(*CassandraStore) {
	return func(s *CassandraStore) { s.bucketCache = inv }
}

// bustParentBucket invalidates history-service's cached copy of the bucket
// holding a thread parent this worker just rewrote. A thread parent is usually
// old enough that its bucket has sealed, and history-service caches sealed
// buckets whole — including the tcount and thread_last_msg_at this path writes.
//
// Best-effort: the Cassandra write has already committed, so a failed DEL must
// not fail the handler. The cache TTL is the backstop.
func (s *CassandraStore) bustParentBucket(ctx context.Context, roomID string, parentCreatedAt time.Time) {
	if s.bucketCache == nil {
		return
	}
	s.bucketCache.Bust(ctx, roomID, s.bucket.Of(parentCreatedAt))
}

// SaveMessage inserts into messages_by_room + messages_by_id via one UnloggedBatch.
func (s *CassandraStore) SaveMessage(ctx context.Context, msg *model.Message, siteID string) error {
	if s.cipher != nil {
		return s.saveEncrypted(ctx, msg, siteID)
	}
	sender := toSender(msg)
	mentions := toMentionSet(msg.Mentions)
	b := s.bucket.Of(msg.CreatedAt)

	batch := s.sess.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	batch.Query(
		`INSERT INTO messages_by_room
		   (room_id, bucket, created_at, message_id, sender, msg, site_id, updated_at,
		    mentions, type, sys_msg_data, tshow, quoted_parent_message,
		    attachments, card, card_action)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.RoomID, b, msg.CreatedAt, msg.ID, sender, msg.Content, siteID, msg.CreatedAt,
		mentions, msg.Type, msg.SysMsgData, msg.TShow, msg.QuotedParentMessage,
		msg.Attachments, msg.Card, msg.CardAction,
	)
	batch.Query(
		`INSERT INTO messages_by_id
		   (message_id, created_at, room_id, sender, msg, site_id, updated_at,
		    mentions, type, sys_msg_data, tshow, quoted_parent_message,
		    attachments, card, card_action)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.ID, msg.CreatedAt, msg.RoomID, sender, msg.Content, siteID, msg.CreatedAt,
		mentions, msg.Type, msg.SysMsgData, msg.TShow, msg.QuotedParentMessage,
		msg.Attachments, msg.Card, msg.CardAction,
	)
	if err := s.sess.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("save bot message %s: %w", msg.ID, err)
	}
	return nil
}

// saveEncrypted binds body columns NULL and stores ciphertext in enc_payload/enc_meta; NULLs defeat hybrid states on redelivery of pre-encryption rows.
func (s *CassandraStore) saveEncrypted(ctx context.Context, msg *model.Message, siteID string) error {
	cm := buildCassandraMessage(msg)
	enc := atrest.SplitForEncryption(&cm)
	payload, meta, err := s.cipher.Encrypt(ctx, cm.RoomID, enc)
	if err != nil {
		return fmt.Errorf("encrypt bot message %s in room %s: %w", cm.MessageID, cm.RoomID, err)
	}
	atrest.StripEncryptedFields(&cm)
	encMeta := &cassandra.EncMeta{Nonce: meta.Nonce}
	sender := toSender(msg)
	mentions := toMentionSet(msg.Mentions)
	b := s.bucket.Of(msg.CreatedAt)

	batch := s.sess.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	batch.Query(
		`INSERT INTO messages_by_room
		   (room_id, bucket, created_at, message_id, sender, site_id, updated_at,
		    mentions, type, tshow, quoted_parent_message, sys_msg_data,
		    msg, attachments, card, card_action,
		    enc_payload, enc_meta)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null, null, null, null, ?, ?)`,
		msg.RoomID, b, msg.CreatedAt, msg.ID, sender, siteID, msg.CreatedAt,
		mentions, msg.Type, msg.TShow, cm.QuotedParentMessage, msg.SysMsgData, payload, encMeta,
	)
	batch.Query(
		`INSERT INTO messages_by_id
		   (message_id, created_at, room_id, sender, site_id, updated_at,
		    mentions, type, tshow, quoted_parent_message, sys_msg_data,
		    msg, attachments, card, card_action,
		    enc_payload, enc_meta)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null, null, null, null, ?, ?)`,
		msg.ID, msg.CreatedAt, msg.RoomID, sender, siteID, msg.CreatedAt,
		mentions, msg.Type, msg.TShow, cm.QuotedParentMessage, msg.SysMsgData, payload, encMeta,
	)
	if err := s.sess.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("save encrypted bot message %s: %w", msg.ID, err)
	}
	return nil
}

// SaveThreadMessage inserts into messages_by_id + thread_messages_by_thread, mirroring to
// messages_by_room when TShow is true, then blind-SETs tcount/tlm from a bounded partition COUNT.
func (s *CassandraStore) SaveThreadMessage(ctx context.Context, msg *model.Message, siteID, threadRoomID string) error {
	if s.cipher != nil {
		return s.saveThreadEncrypted(ctx, msg, siteID, threadRoomID)
	}
	sender := toSender(msg)
	mentions := toMentionSet(msg.Mentions)

	batch := s.sess.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	batch.Query(
		`INSERT INTO messages_by_id
		 (message_id, created_at, room_id, sender, msg, site_id, updated_at, mentions,
		  thread_room_id, thread_parent_id, thread_parent_created_at, type, sys_msg_data, tshow, quoted_parent_message,
		  attachments, card, card_action)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.ID, msg.CreatedAt, msg.RoomID, sender, msg.Content, siteID, msg.CreatedAt, mentions,
		threadRoomID, msg.ThreadParentMessageID, msg.ThreadParentMessageCreatedAt, msg.Type, msg.SysMsgData, msg.TShow, msg.QuotedParentMessage,
		msg.Attachments, msg.Card, msg.CardAction,
	)
	batch.Query(
		`INSERT INTO thread_messages_by_thread
		 (thread_room_id, created_at, message_id, room_id, thread_parent_id, sender, msg,
		  site_id, updated_at, mentions, type, sys_msg_data, tshow, quoted_parent_message,
		  attachments, card, card_action)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		threadRoomID, msg.CreatedAt, msg.ID, msg.RoomID, msg.ThreadParentMessageID,
		sender, msg.Content, siteID, msg.CreatedAt, mentions,
		msg.Type, msg.SysMsgData, msg.TShow, msg.QuotedParentMessage,
		msg.Attachments, msg.Card, msg.CardAction,
	)
	if msg.TShow {
		batch.Query(
			`INSERT INTO messages_by_room
			 (room_id, bucket, created_at, message_id, sender, msg, site_id, updated_at, mentions,
			  thread_room_id, thread_parent_id, thread_parent_created_at, type, sys_msg_data, tshow, quoted_parent_message,
			  attachments, card, card_action)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			msg.RoomID, s.bucket.Of(msg.CreatedAt), msg.CreatedAt, msg.ID, sender, msg.Content, siteID, msg.CreatedAt, mentions,
			threadRoomID, msg.ThreadParentMessageID, msg.ThreadParentMessageCreatedAt, msg.Type, msg.SysMsgData, msg.TShow, msg.QuotedParentMessage,
			msg.Attachments, msg.Card, msg.CardAction,
		)
	}
	if err := s.sess.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("save bot thread message %s: %w", msg.ID, err)
	}
	return s.countAndSetParentTcount(ctx, msg, threadRoomID)
}

func (s *CassandraStore) saveThreadEncrypted(ctx context.Context, msg *model.Message, siteID, threadRoomID string) error {
	cm := buildCassandraMessage(msg)
	enc := atrest.SplitForEncryption(&cm)
	payload, meta, err := s.cipher.Encrypt(ctx, cm.RoomID, enc)
	if err != nil {
		return fmt.Errorf("encrypt bot thread %s in room %s: %w", cm.MessageID, cm.RoomID, err)
	}
	atrest.StripEncryptedFields(&cm)
	encMeta := &cassandra.EncMeta{Nonce: meta.Nonce}
	sender := toSender(msg)
	mentions := toMentionSet(msg.Mentions)

	batch := s.sess.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	batch.Query(
		`INSERT INTO messages_by_id
		 (message_id, created_at, room_id, sender, site_id, updated_at, mentions,
		  thread_room_id, thread_parent_id, thread_parent_created_at, type, tshow,
		  quoted_parent_message, sys_msg_data,
		  msg, attachments, card, card_action,
		  enc_payload, enc_meta)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null, null, null, null, ?, ?)`,
		msg.ID, msg.CreatedAt, msg.RoomID, sender, siteID, msg.CreatedAt, mentions,
		threadRoomID, msg.ThreadParentMessageID, msg.ThreadParentMessageCreatedAt, msg.Type, msg.TShow,
		cm.QuotedParentMessage, msg.SysMsgData, payload, encMeta,
	)
	batch.Query(
		`INSERT INTO thread_messages_by_thread
		 (thread_room_id, created_at, message_id, room_id, thread_parent_id,
		  sender, site_id, updated_at, mentions, type, tshow, quoted_parent_message, sys_msg_data,
		  msg, attachments, card, card_action,
		  enc_payload, enc_meta)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null, null, null, null, ?, ?)`,
		threadRoomID, msg.CreatedAt, msg.ID, msg.RoomID, msg.ThreadParentMessageID,
		sender, siteID, msg.CreatedAt, mentions, msg.Type, msg.TShow, cm.QuotedParentMessage, msg.SysMsgData,
		payload, encMeta,
	)
	if msg.TShow {
		batch.Query(
			`INSERT INTO messages_by_room
			 (room_id, bucket, created_at, message_id, sender, site_id, updated_at, mentions,
			  thread_room_id, thread_parent_id, thread_parent_created_at, type, tshow, quoted_parent_message, sys_msg_data,
			  msg, attachments, card, card_action,
			  enc_payload, enc_meta)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null, null, null, null, ?, ?)`,
			msg.RoomID, s.bucket.Of(msg.CreatedAt), msg.CreatedAt, msg.ID, sender, siteID, msg.CreatedAt, mentions,
			threadRoomID, msg.ThreadParentMessageID, msg.ThreadParentMessageCreatedAt, msg.Type, msg.TShow, cm.QuotedParentMessage, msg.SysMsgData,
			payload, encMeta,
		)
	}
	if err := s.sess.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("save encrypted bot thread %s: %w", msg.ID, err)
	}
	return s.countAndSetParentTcount(ctx, msg, threadRoomID)
}

// countAndSetParentTcount blind-SETs tcount/thread_last_msg_at on the parent from an
// authoritative partition COUNT (idempotent on redelivery); no-op for legacy replies without ThreadParentMessageCreatedAt.
func (s *CassandraStore) countAndSetParentTcount(ctx context.Context, msg *model.Message, threadRoomID string) error {
	if msg.ThreadParentMessageCreatedAt == nil {
		return nil
	}
	n, err := threadcount.Count(ctx, s.sess, threadRoomID)
	if err != nil {
		return fmt.Errorf("count bot thread replies: %w", err)
	}
	tlm := msg.CreatedAt
	parentID := msg.ThreadParentMessageID
	parentCreatedAt := *msg.ThreadParentMessageCreatedAt
	parentBucket := s.bucket.Of(parentCreatedAt)
	if err := s.sess.Query(
		`UPDATE messages_by_id SET tcount = ?, thread_last_msg_at = ? WHERE message_id = ?`,
		n, tlm, parentID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("set tcount/tlm on parent %s in messages_by_id: %w", parentID, err)
	}
	if err := s.sess.Query(
		`UPDATE messages_by_room SET tcount = ?, thread_last_msg_at = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		n, tlm, msg.RoomID, parentBucket, parentCreatedAt, parentID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("set tcount/tlm on parent %s in messages_by_room: %w", parentID, err)
	}
	s.bustParentBucket(ctx, msg.RoomID, parentCreatedAt)
	return nil
}

// buildCassandraMessage projects the wire Message onto the encryption bundle expected by pkg/atrest.
func buildCassandraMessage(msg *model.Message) cassandra.Message {
	return cassandra.Message{
		RoomID:              msg.RoomID,
		MessageID:           msg.ID,
		Msg:                 msg.Content,
		Attachments:         msg.Attachments,
		Card:                msg.Card,
		CardAction:          msg.CardAction,
		QuotedParentMessage: msg.QuotedParentMessage,
	}
}
