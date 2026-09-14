package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	o11ycassandra "github.com/flywindy/o11y/cassandra"
	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/threadcount"
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
	cassSession  *gocql.Session
	bucket       msgbucket.Sizer
	cipher       atrest.Cipher // nil when ATREST_ENABLED=false
	newBatch     func(context.Context) *gocql.Batch
	executeBatch func(context.Context, *gocql.Batch) error
}

func NewCassandraStore(session *gocql.Session, bucket msgbucket.Sizer, cipher atrest.Cipher) *CassandraStore {
	return &CassandraStore{
		cassSession: session,
		bucket:      bucket,
		cipher:      cipher,
		newBatch: func(ctx context.Context) *gocql.Batch {
			return session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
		},
		executeBatch: func(ctx context.Context, batch *gocql.Batch) error {
			return o11ycassandra.ExecuteBatch(ctx, session, batch)
		},
	}
}

// writeTS is the write timestamp every *plaintext* create INSERT binds via
// USING TIMESTAMP. Cassandra takes microseconds since the epoch.
//
// Pinning it to the message's own CreatedAt is what makes a redelivery re-execute
// the *identical* write rather than a newer one. Cassandra resolves conflicts per
// cell by write timestamp, and gocql stamps every statement with the client clock
// at execution time (ClusterConfig.DefaultTimestamp defaults to true), so an
// unpinned create that commits, NAKs on a later step, and redelivers minutes or
// hours afterwards outranks any edit made in between — silently restoring the
// original body. message-worker's outage retry budget spreads redeliveries over
// roughly an hour, which is exactly when someone fixes a typo.
//
// Only plaintext creates are pinned. Edits, deletes and the derived tcount/tlm
// SETs keep the client clock, so each stays strictly above the create it
// supersedes.
//
// The pin is sound for the message body, which is fixed by the canonical event.
// It is NOT sound for the enrichment columns: the handler re-resolves sender and
// mentions on every delivery, and both fail open, so a degraded attempt and a
// healthy retry bind different values under one timestamp. Cassandra then breaks
// that tie per cell by comparing values rather than by attempt order, so the
// healthy retry does not reliably replace the degraded one, and sender may be
// taken from one attempt while mentions come from the other. Unlike the encrypted
// case this always yields a readable row -- the cost is degraded or mixed
// enrichment, not an unrecoverable one -- so the pin stays, because unpinning
// would trade it for the create-outranks-edit hazard the pin exists to prevent.
// Closing it properly means making the bound values deterministic from the
// canonical event, or writing enrichment as its own unpinned mutation.
//
// The encrypted create paths deliberately do NOT pin, because their bound values
// are not identical across attempts: atrest.Encrypt draws a fresh random nonce
// per call, so enc_payload and enc_meta differ on every redelivery. They are
// separate cells, and Cassandra breaks a same-timestamp tie per cell by comparing
// values — independently — so two attempts sharing one timestamp can leave the
// ciphertext of one beside the nonce of the other, which AES-GCM can never open.
// On the client clock each redelivery is strictly newer, so one attempt wins both
// cells and the pair stays coherent. See store_cassandra_writetime_test.go.
func writeTS(createdAt time.Time) int64 { return createdAt.UnixMicro() }

// The encrypted create paths clear the plaintext body columns of any row already
// at the key, so a JetStream redelivery (or federation replay) of a pre-rollout
// legacy message cannot leave it in a hybrid plaintext+encrypted state: CQL INSERT
// does not null unspecified columns on key collision, and a leftover plaintext
// attachments/card beside the new enc_payload is overwritten with empty fields
// from the bundle by ApplyDecryptedFields on read — silently losing them.
//
// These clears are deliberately NOT pinned. They are tombstones, so they only
// take effect if they outrank the row they clear, and a legacy row was written at
// execution time — after CreatedAt. Pinned, they would land before it and clear
// nothing. On the client clock they always win, and re-running one is harmless:
// an encrypted edit already nulls these same four columns, so a redelivered strip
// only re-nulls already-null cells.
//
// The encrypted INSERT beside them is unpinned too, so binding these NULLs into it
// would carry the same timestamp and would also clear. They stay separate
// statements to keep the requirement local: this clear must ride the client clock
// whatever the INSERT does, so it cannot be silently re-pinned along with it.
//
// Mixed timestamps inside one batch are legal as long as no batch-level timestamp
// is set; gocql's protocol default timestamp applies to whichever statements carry
// no USING TIMESTAMP of their own.
const (
	stripLegacyPlaintextByRoom = `UPDATE messages_by_room SET msg = null, attachments = null, card = null, card_action = null WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`
	stripLegacyPlaintextByID   = `UPDATE messages_by_id SET msg = null, attachments = null, card = null, card_action = null WHERE message_id = ?`
	stripLegacyPlaintextThread = `UPDATE thread_messages_by_thread SET msg = null, attachments = null, card = null, card_action = null WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`
)

// SaveMessage inserts msg into both messages_by_room and messages_by_id via a
// single UnloggedBatch so the two denormalized writes share one coordinator
// round-trip. UnloggedBatch (not LoggedBatch) because we don't need batch-log
// atomicity: each INSERT is idempotent on its primary key, and on partial
// failure JetStream redelivers and both INSERTs re-run safely — see writeTS for
// why every INSERT below pins its own write timestamp.
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

	batch := s.newBatch(ctx)
	batch.Query(
		`INSERT INTO messages_by_room
		   (room_id, bucket, created_at, message_id, sender, msg, site_id, updated_at,
		    mentions, type, sys_msg_data, tshow, quoted_parent_message,
		    attachments, card, card_action, visible_to)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?`,
		msg.RoomID, b, msg.CreatedAt, msg.ID, sender, msg.Content, siteID, msg.CreatedAt,
		mentions, msg.Type, msg.SysMsgData, msg.TShow, msg.QuotedParentMessage,
		msg.Attachments, msg.Card, msg.CardAction, msg.VisibleTo,
		writeTS(msg.CreatedAt),
	)
	batch.Query(
		`INSERT INTO messages_by_id
		   (message_id, created_at, room_id, sender, msg, site_id, updated_at,
		    mentions, type, sys_msg_data, tshow, quoted_parent_message,
		    attachments, card, card_action, visible_to)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?`,
		msg.ID, msg.CreatedAt, msg.RoomID, sender, msg.Content, siteID, msg.CreatedAt,
		mentions, msg.Type, msg.SysMsgData, msg.TShow, msg.QuotedParentMessage,
		msg.Attachments, msg.Card, msg.CardAction, msg.VisibleTo,
		writeTS(msg.CreatedAt),
	)
	if err := s.executeBatch(ctx, batch); err != nil {
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

	// sys_msg_data is NOT encrypted, so it is written as plaintext like any other
	// metadata column. The encrypted body columns are cleared by the strip that
	// follows each INSERT — see stripLegacyPlaintext for why that is a separate,
	// unpinned statement rather than NULLs bound into the INSERT itself.
	batch := s.newBatch(ctx)
	batch.Query(
		`INSERT INTO messages_by_room
		   (room_id, bucket, created_at, message_id, sender, site_id, updated_at,
		    mentions, type, tshow, quoted_parent_message, sys_msg_data, visible_to,
		    enc_payload, enc_meta)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.RoomID, b, msg.CreatedAt, msg.ID, sender, siteID, msg.CreatedAt,
		mentions, msg.Type, msg.TShow, cm.QuotedParentMessage, msg.SysMsgData, msg.VisibleTo, payload, encMeta,
	)
	batch.Query(stripLegacyPlaintextByRoom, msg.RoomID, b, msg.CreatedAt, msg.ID)
	batch.Query(
		`INSERT INTO messages_by_id
		   (message_id, created_at, room_id, sender, site_id, updated_at,
		    mentions, type, tshow, quoted_parent_message, sys_msg_data, visible_to,
		    enc_payload, enc_meta)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.ID, msg.CreatedAt, msg.RoomID, sender, siteID, msg.CreatedAt,
		mentions, msg.Type, msg.TShow, cm.QuotedParentMessage, msg.SysMsgData, msg.VisibleTo, payload, encMeta,
	)
	batch.Query(stripLegacyPlaintextByID, msg.ID)
	if err := s.executeBatch(ctx, batch); err != nil {
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

	// One UnloggedBatch (same pattern as SaveMessage) groups the messages_by_id +
	// thread_messages_by_thread writes (plus the conditional TShow mirror); each INSERT
	// is idempotent so redelivery is safe. countAndSetParentTcount runs after commit.
	batch := s.newBatch(ctx)
	batch.Query(
		`INSERT INTO messages_by_id
		 (message_id, created_at, room_id, sender, msg, site_id, updated_at, mentions,
		  thread_room_id, thread_parent_id, thread_parent_created_at, type, sys_msg_data, tshow, quoted_parent_message,
		  attachments, card, card_action, visible_to)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?`,
		msg.ID, msg.CreatedAt, msg.RoomID, sender, msg.Content, siteID, msg.CreatedAt, mentions,
		threadRoomID, msg.ThreadParentMessageID, msg.ThreadParentMessageCreatedAt, msg.Type, msg.SysMsgData, msg.TShow, msg.QuotedParentMessage,
		msg.Attachments, msg.Card, msg.CardAction, msg.VisibleTo,
		writeTS(msg.CreatedAt),
	)
	batch.Query(
		`INSERT INTO thread_messages_by_thread
		 (thread_room_id, created_at, message_id, room_id, thread_parent_id, sender, msg,
		  site_id, updated_at, mentions, type, sys_msg_data, tshow, quoted_parent_message,
		  attachments, card, card_action, visible_to)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?`,
		threadRoomID, msg.CreatedAt, msg.ID, msg.RoomID, msg.ThreadParentMessageID,
		sender, msg.Content, siteID, msg.CreatedAt, mentions,
		msg.Type, msg.SysMsgData, msg.TShow, msg.QuotedParentMessage,
		msg.Attachments, msg.Card, msg.CardAction, msg.VisibleTo,
		writeTS(msg.CreatedAt),
	)
	// TShow ("also send to channel"): dual-write the reply into messages_by_room
	// so it shows up in the parent room's channel timeline on history loads.
	// A third INSERT — NOT a SaveMessage call, which would double-write
	// messages_by_id. The row uses the reply's own created_at (interleaves
	// correctly in the timeline) and the same bucket sizer as the channel path.
	// tshow + thread_parent_id + thread_parent_created_at must be populated:
	// history-service's quote access-window logic redacts TShow rows that lack
	// the parent fields (legacyTShowMissingParentTime).
	if msg.TShow {
		batch.Query(
			`INSERT INTO messages_by_room
			 (room_id, bucket, created_at, message_id, sender, msg, site_id, updated_at, mentions,
			  thread_room_id, thread_parent_id, thread_parent_created_at, type, sys_msg_data, tshow, quoted_parent_message,
			  attachments, card, card_action, visible_to)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?`,
			msg.RoomID, s.bucket.Of(msg.CreatedAt), msg.CreatedAt, msg.ID, sender, msg.Content, siteID, msg.CreatedAt, mentions,
			threadRoomID, msg.ThreadParentMessageID, msg.ThreadParentMessageCreatedAt, msg.Type, msg.SysMsgData, msg.TShow, msg.QuotedParentMessage,
			msg.Attachments, msg.Card, msg.CardAction, msg.VisibleTo,
			writeTS(msg.CreatedAt),
		)
	}
	if err := s.executeBatch(ctx, batch); err != nil {
		return nil, fmt.Errorf("save thread message %s: %w", msg.ID, err)
	}

	return s.countAndSetParentTcount(ctx, msg, threadRoomID)
}

// saveThreadMessageEncrypted is the cipher-enabled counterpart to
// SaveThreadMessage. Both writes are plain INSERTs — see SaveThreadMessage for
// the rationale (JetStream MsgID dedup + idempotent countAndSetParentTcount).
//
// Encrypted body columns (msg, attachments, card, card_action) are bound
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
	// One expression for the TShow mirror's partition key: the INSERT and the strip
	// below must address the same row, so they must not derive it separately.
	b := s.bucket.Of(msg.CreatedAt)

	// Single UnloggedBatch for both encrypted writes (plus the conditional TShow
	// mirror) — same rationale as SaveThreadMessage. Each INSERT is followed by its
	// table's strip, which clears the plaintext body columns of any legacy row at
	// the same key; see stripLegacyPlaintext for why the strip is unpinned.
	batch := s.newBatch(ctx)
	batch.Query(
		`INSERT INTO messages_by_id
		 (message_id, created_at, room_id, sender, site_id, updated_at, mentions,
		  thread_room_id, thread_parent_id, thread_parent_created_at, type, tshow,
		  quoted_parent_message, sys_msg_data, visible_to,
		  enc_payload, enc_meta)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.ID, msg.CreatedAt, msg.RoomID, sender, siteID, msg.CreatedAt, mentions,
		threadRoomID, msg.ThreadParentMessageID, msg.ThreadParentMessageCreatedAt, msg.Type, msg.TShow,
		cm.QuotedParentMessage, msg.SysMsgData, msg.VisibleTo, payload, encMeta,
	)
	batch.Query(stripLegacyPlaintextByID, msg.ID)
	batch.Query(
		`INSERT INTO thread_messages_by_thread
		 (thread_room_id, created_at, message_id, room_id, thread_parent_id,
		  sender, site_id, updated_at, mentions, type, tshow, quoted_parent_message, sys_msg_data, visible_to,
		  enc_payload, enc_meta)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		threadRoomID, msg.CreatedAt, msg.ID, msg.RoomID, msg.ThreadParentMessageID,
		sender, siteID, msg.CreatedAt, mentions, msg.Type, msg.TShow, cm.QuotedParentMessage, msg.SysMsgData, msg.VisibleTo,
		payload, encMeta,
	)
	batch.Query(stripLegacyPlaintextThread, threadRoomID, msg.CreatedAt, msg.ID)
	// TShow dual-write into messages_by_room — see SaveThreadMessage for the
	// rationale. Reuses the same encrypted bundle (payload + nonce) the two
	// writes above bind, matching saveMessageEncrypted's both-tables pattern,
	// and carries its own strip for the same hybrid-state reason.
	if msg.TShow {
		batch.Query(
			`INSERT INTO messages_by_room
			 (room_id, bucket, created_at, message_id, sender, site_id, updated_at, mentions,
			  thread_room_id, thread_parent_id, thread_parent_created_at, type, tshow,
			  quoted_parent_message, sys_msg_data, visible_to,
			  enc_payload, enc_meta)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			msg.RoomID, b, msg.CreatedAt, msg.ID, sender, siteID, msg.CreatedAt, mentions,
			threadRoomID, msg.ThreadParentMessageID, msg.ThreadParentMessageCreatedAt, msg.Type, msg.TShow,
			cm.QuotedParentMessage, msg.SysMsgData, msg.VisibleTo, payload, encMeta,
		)
		batch.Query(stripLegacyPlaintextByRoom, msg.RoomID, b, msg.CreatedAt, msg.ID)
	}
	if err := s.executeBatch(ctx, batch); err != nil {
		return nil, fmt.Errorf("save thread message %s: %w", msg.ID, err)
	}

	return s.countAndSetParentTcount(ctx, msg, threadRoomID)
}

// buildCassandraMessage projects the user-authored fields of msg into a
// cassandra.Message for encryption. The encrypted content fields are Msg
// (Content), Attachments, Card, CardAction and the QuotedParentMessage
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
	}
	if msg.QuotedParentMessage != nil {
		q := *msg.QuotedParentMessage
		// gocql persists the LIST<BLOB> attachments column from the raw Attachments
		// field; only DecodedAttachments crosses the canonical wire, so re-encode
		// it here (before encryption — Attachments is an encrypted field).
		q.Attachments = cassandra.EncodeAttachments(q.DecodedAttachments)
		cm.QuotedParentMessage = &q
	}
	return cm
}

// countThreadReplies returns the exact, soft-delete-aware reply count for the
// thread. It delegates to pkg/threadcount so this add-path writer and the
// history-service delete-path writer compute an identical value.
func (s *CassandraStore) countThreadReplies(ctx context.Context, threadRoomID string) (int, error) {
	return threadcount.Count(ctx, s.cassSession, threadRoomID)
}

// setParentTcountAndTlm co-SETs tcount and tlm on the parent row in both tables
// (one UPDATE). Blind-SET from the authoritative COUNT → idempotent on redelivery.
// On the add path tlm is the reply's own CreatedAt (always the newest).
func (s *CassandraStore) setParentTcountAndTlm(ctx context.Context, msg *model.Message, n int, tlm *time.Time) error {
	parentID := msg.ThreadParentMessageID
	parentCreatedAt := *msg.ThreadParentMessageCreatedAt
	parentBucket := s.bucket.Of(parentCreatedAt)
	if err := s.cassSession.Query(
		`UPDATE messages_by_id SET tcount = ?, thread_last_msg_at = ? WHERE message_id = ?`,
		n, tlm, parentID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("set tcount/tlm on parent %s in messages_by_id: %w", parentID, err)
	}
	if err := s.cassSession.Query(
		`UPDATE messages_by_room SET tcount = ?, thread_last_msg_at = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`,
		n, tlm, msg.RoomID, parentBucket, parentCreatedAt, parentID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("set tcount/tlm on parent %s in messages_by_room: %w", parentID, err)
	}
	return nil
}

// countAndSetParentTcount recomputes tcount from the partition COUNT and co-sets
// tcount+tlm on the parent (tlm = the reply's CreatedAt, newest on the add path).
// Returns (nil, nil) when ThreadParentMessageCreatedAt is unset.
func (s *CassandraStore) countAndSetParentTcount(ctx context.Context, msg *model.Message, threadRoomID string) (*int, error) {
	if msg.ThreadParentMessageCreatedAt == nil {
		return nil, nil
	}
	n, err := s.countThreadReplies(ctx, threadRoomID)
	if err != nil {
		return nil, fmt.Errorf("count thread replies: %w", err)
	}
	tlm := msg.CreatedAt
	if err := s.setParentTcountAndTlm(ctx, msg, n, &tlm); err != nil {
		return nil, fmt.Errorf("set parent tcount/tlm: %w", err)
	}
	return &n, nil
}

// IF EXISTS prevents phantom rows on missing parents; misses log at ERROR
// because a silent miss permanently breaks thread reads for that parent.
func (s *CassandraStore) UpdateParentMessageThreadRoomID(ctx context.Context, parentMessageID, roomID string, parentCreatedAt time.Time, threadRoomID string) error {
	parentBucket := s.bucket.Of(parentCreatedAt)

	applied, err := s.cassSession.Query(
		`UPDATE messages_by_id SET thread_room_id = ? WHERE message_id = ? IF EXISTS`,
		threadRoomID, parentMessageID,
	).WithContext(ctx).ScanCAS()
	if err != nil {
		return fmt.Errorf("set thread_room_id on parent %s in messages_by_id: %w", parentMessageID, err)
	}
	if !applied {
		slog.Error("thread_room_id stamp on messages_by_id missed: parent row not found for message_id",
			"request_id", natsutil.RequestIDFromContext(ctx),
			"messageID", parentMessageID,
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

// GetQuotedParentSnapshot re-projects the authoritative quoted-parent snapshot for
// messageID from messages_by_id. Metadata lives in plaintext columns; the body is
// decrypted from enc_payload in the cipher-enabled path. Returns (nil, false, nil)
// when the row is absent so the caller can drop an unverifiable quote. MessageLink
// and Attachments are left to the caller.
func (s *CassandraStore) GetQuotedParentSnapshot(ctx context.Context, messageID string) (*cassandra.QuotedParentMessage, bool, error) {
	var (
		roomID                string
		sender                cassandra.Participant
		createdAt             time.Time
		mentions              []cassandra.Participant
		threadParentID        string
		threadParentCreatedAt *time.Time
		msg                   string
		encPayload            []byte
		encMeta               *cassandra.EncMeta
	)
	if err := s.cassSession.Query(
		`SELECT room_id, sender, created_at, mentions, thread_parent_id, thread_parent_created_at, msg, enc_payload, enc_meta
		   FROM messages_by_id WHERE message_id = ? LIMIT 1`,
		messageID,
	).WithContext(ctx).Scan(
		&roomID, &sender, &createdAt, &mentions, &threadParentID, &threadParentCreatedAt, &msg, &encPayload, &encMeta,
	); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get quoted parent snapshot for message %s: %w", messageID, err)
	}

	if s.cipher != nil && len(encPayload) > 0 {
		if encMeta == nil {
			// An encrypted write always co-writes enc_meta (the nonce) alongside
			// enc_payload. A nil nonce here means a corrupt/legacy row; fail with an
			// explicit contract error rather than handing a nil nonce to AEAD decrypt.
			return nil, false, fmt.Errorf("quoted parent %s has enc_payload but no enc_meta", messageID)
		}
		fields, err := s.cipher.Decrypt(ctx, roomID, encPayload, atrest.EncMeta{Nonce: encMeta.Nonce})
		if err != nil {
			return nil, false, fmt.Errorf("decrypt quoted parent %s: %w", messageID, err)
		}
		msg = fields.Msg
	}

	return &cassandra.QuotedParentMessage{
		MessageID:             messageID,
		RoomID:                roomID,
		Sender:                sender,
		CreatedAt:             createdAt,
		Msg:                   msg,
		Mentions:              mentions,
		ThreadParentID:        threadParentID,
		ThreadParentCreatedAt: threadParentCreatedAt,
	}, true, nil
}

// GetMessageCreatedAt point-reads created_at from messages_by_id. Returns
// (zero, false, nil) when absent; a Cassandra failure errors so the worker NAKs.
func (s *CassandraStore) GetMessageCreatedAt(ctx context.Context, messageID string) (time.Time, bool, error) {
	var createdAt time.Time
	if err := s.cassSession.Query(
		`SELECT created_at FROM messages_by_id WHERE message_id = ? LIMIT 1`,
		messageID,
	).WithContext(ctx).Scan(&createdAt); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, fmt.Errorf("get createdAt for message %s: %w", messageID, err)
	}
	return createdAt, true, nil
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
