package cassrepo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/atrest"
	pkgmodel "github.com/hmchangw/chat/pkg/model"
	cassmodel "github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/threadcount"
)

const (
	// Plaintext-path edits. enc_payload/enc_meta are nulled to keep a
	// cipher-disabled (rollback) edit from leaving stale ciphertext that
	// would silently override the new plaintext on re-enabled reads. These
	// are plain (non-LWT) UPDATEs: the service layer's findMessage already
	// gates existence and the not-deleted check before this is reached, and
	// messages are only edited by their owner, so a CAS gate isn't warranted.
	editMsgByID   = `UPDATE messages_by_id SET msg = ?, mentions = ?, enc_payload = null, enc_meta = null, edited_at = ?, updated_at = ? WHERE message_id = ?`
	editMsgByRoom = `UPDATE messages_by_room SET msg = ?, mentions = ?, enc_payload = null, enc_meta = null, edited_at = ?, updated_at = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`
	editThreadMsg = `UPDATE thread_messages_by_thread SET msg = ?, mentions = ?, enc_payload = null, enc_meta = null, edited_at = ?, updated_at = ? WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`
	editPinnedMsg = `UPDATE pinned_messages_by_room SET msg = ?, mentions = ?, enc_payload = null, enc_meta = null, edited_at = ?, updated_at = ? WHERE room_id = ? AND pinned_at = ? AND message_id = ?`

	// Encrypted-path edits null the encrypted legacy body columns (msg,
	// attachments, card, card_action) — buildEditPayload has promoted those
	// into the bundle, and leaving plaintext behind would defeat the at-rest
	// goal on edited legacy rows. quoted_parent_message is bound (not nulled):
	// only its body sub-fields move into the bundle, while its non-sensitive
	// metadata (message_id, sender, …) must stay in the plaintext column or a
	// read-back can't restore it. sys_msg_data is NOT encrypted, so it is preserved.
	editMsgByIDEncrypted   = `UPDATE messages_by_id SET enc_payload = ?, enc_meta = ?, mentions = ?, msg = null, attachments = null, card = null, card_action = null, quoted_parent_message = ?, edited_at = ?, updated_at = ? WHERE message_id = ?`
	editMsgByRoomEncrypted = `UPDATE messages_by_room SET enc_payload = ?, enc_meta = ?, mentions = ?, msg = null, attachments = null, card = null, card_action = null, quoted_parent_message = ?, edited_at = ?, updated_at = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`
	editThreadMsgEncrypted = `UPDATE thread_messages_by_thread SET enc_payload = ?, enc_meta = ?, mentions = ?, msg = null, attachments = null, card = null, card_action = null, quoted_parent_message = ?, edited_at = ?, updated_at = ? WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`
	editPinnedMsgEncrypted = `UPDATE pinned_messages_by_room SET enc_payload = ?, enc_meta = ?, mentions = ?, msg = null, attachments = null, card = null, card_action = null, quoted_parent_message = ?, edited_at = ?, updated_at = ? WHERE room_id = ? AND pinned_at = ? AND message_id = ?`

	deleteMsgByIDCAS = `UPDATE messages_by_id SET deleted = true, enc_payload = null, enc_meta = null, updated_at = ? WHERE message_id = ? IF deleted != true`
	deleteMsgByRoom  = `UPDATE messages_by_room SET deleted = true, enc_payload = null, enc_meta = null, updated_at = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?`
	deleteThreadMsg  = `UPDATE thread_messages_by_thread SET deleted = true, enc_payload = null, enc_meta = null, updated_at = ? WHERE thread_room_id = ? AND created_at = ? AND message_id = ?`
	deletePinnedMsg  = `UPDATE pinned_messages_by_room SET deleted = true, enc_payload = null, enc_meta = null, updated_at = ? WHERE room_id = ? AND pinned_at = ? AND message_id = ?`
)

// ErrMessageNotFound is returned by edit operations when the target
// (message_id, created_at) does not exist. Cassandra's UPDATE is upsert-
// equivalent, so callers must short-circuit on this error instead of
// issuing the UPDATE — otherwise the edit would materialise a partial
// ghost row with no sender/room_id/type/mentions.
var ErrMessageNotFound = errors.New("message not found")

// MessageTypeRemoved is the Cassandra type value written to thread parent messages
// when they are soft-deleted, signalling to the frontend that the thread's
// parent message has been removed.
const MessageTypeRemoved = "message_removed"

// Thread-parent delete queries — identical to the regular delete queries but also
// set type = MessageTypeRemoved. Used when msg.TCount != nil && *msg.TCount > 0.
const (
	deleteThreadParentMsgByIDCAS = "UPDATE messages_by_id SET deleted = true, enc_payload = null, enc_meta = null, type = '" + MessageTypeRemoved + "', updated_at = ? WHERE message_id = ? IF deleted != true"
	deleteThreadParentMsgByRoom  = "UPDATE messages_by_room SET deleted = true, enc_payload = null, enc_meta = null, type = '" + MessageTypeRemoved + "', updated_at = ? WHERE room_id = ? AND bucket = ? AND created_at = ? AND message_id = ?"
	deleteThreadParentThreadMsg  = "UPDATE thread_messages_by_thread SET deleted = true, enc_payload = null, enc_meta = null, type = '" + MessageTypeRemoved + "', updated_at = ? WHERE thread_room_id = ? AND created_at = ? AND message_id = ?"
	deleteThreadParentPinnedMsg  = "UPDATE pinned_messages_by_room SET deleted = true, enc_payload = null, enc_meta = null, type = '" + MessageTypeRemoved + "', updated_at = ? WHERE room_id = ? AND pinned_at = ? AND message_id = ?"
)

// editPayload is the shared, pre-prepared edit payload passed to each
// per-table UPDATE. It carries either the new plaintext body (cipher
// disabled) or a pre-encrypted bundle (cipher enabled). Building it once
// and reusing it across the 2-3 mirror-table UPDATEs avoids repeated
// encryption and preserves any other encrypted fields that already
// existed on the row (attachments, sysMsgData, quoted parent body).
type editPayload struct {
	plain   string
	payload []byte             // nil when cipher is disabled
	meta    *cassmodel.EncMeta // nil when cipher is disabled
	// mentions is the @-mentions re-resolved from the edited content, bound to
	// the plaintext `mentions` column on both paths (mentions is never encrypted).
	// nil clears the column, so an edit that drops all @mentions drops them here.
	mentions []cassmodel.Participant
	// quotedMeta is the existing quoted-parent UDT with its body sub-fields
	// blanked (the body moves into payload). Bound to the encrypted UPDATE's
	// quoted_parent_message column so the parent's metadata survives the edit;
	// nil leaves the column null (no quote). Unused on the plaintext path.
	quotedMeta *cassmodel.QuotedParentMessage
}

// buildEditPayload prepares the payload for an edit. When the cipher is
// enabled, it reads the existing encrypted row from messages_by_id,
// decrypts it, replaces only Msg with newMsg, and re-encrypts so that
// previously-encrypted fields (attachments, card, sysMsgData, quoted
// parent body) are preserved. Editing a legacy plaintext row produces a
// fresh bundle containing just Msg.
func (r *Repository) buildEditPayload(ctx context.Context, msg *models.Message, newMsg string, mentions []pkgmodel.Participant) (editPayload, error) {
	mentionSet := toMentionSet(mentions)
	if r.cipher == nil {
		return editPayload{plain: newMsg, mentions: mentionSet}, nil
	}
	fields, quoted, err := r.readEncryptedFields(ctx, msg)
	if err != nil {
		return editPayload{}, err
	}
	fields.Msg = newMsg
	payload, meta, err := r.cipher.Encrypt(ctx, msg.RoomID, fields)
	if err != nil {
		return editPayload{}, fmt.Errorf("encrypt edit body for message %s in room %s: %w", msg.MessageID, msg.RoomID, err)
	}
	return editPayload{
		plain:      newMsg,
		payload:    payload,
		meta:       &cassmodel.EncMeta{Nonce: meta.Nonce},
		mentions:   mentionSet,
		quotedMeta: blankQuotedBody(quoted),
	}, nil
}

// toMentionSet converts resolved wire participants to the Cassandra
// SET<FROZEN<"Participant">> element type. nil in → nil out (column cleared).
func toMentionSet(mentions []pkgmodel.Participant) []cassmodel.Participant {
	if len(mentions) == 0 {
		return nil
	}
	out := make([]cassmodel.Participant, len(mentions))
	for i, m := range mentions {
		out[i] = cassmodel.Participant{
			ID:          m.UserID,
			EngName:     m.EngName,
			CompanyName: m.ChineseName,
			Account:     m.Account,
		}
	}
	return out
}

// blankQuotedBody returns a copy of the quoted-parent UDT with only its body
// sub-fields cleared, mirroring atrest.StripEncryptedFields — the body moves
// into enc_payload while message_id/sender/timestamps stay in the plaintext
// column. nil in → nil out (no quote, column stays null).
func blankQuotedBody(quoted *cassmodel.QuotedParentMessage) *cassmodel.QuotedParentMessage {
	if quoted == nil {
		return nil
	}
	stripped := *quoted
	stripped.Msg = ""
	stripped.Attachments = nil
	return &stripped
}

// readEncryptedFields fetches the existing row body from messages_by_id
// and produces an EncryptedFields struct that the caller can re-encrypt.
// When the row already has enc_payload set, that ciphertext is decrypted
// and returned. When it doesn't (legacy plaintext row written before the
// at-rest rollout), the plaintext body columns are promoted into the
// returned EncryptedFields so the next encrypt produces a complete
// bundle — without this, editing a legacy row would silently drop its
// attachments / card because ApplyDecryptedFields unconditionally
// overwrites those fields with the (empty) bundle. sys_msg_data is not
// encrypted, so it is neither read here nor carried in the bundle.
func (r *Repository) readEncryptedFields(ctx context.Context, msg *models.Message) (atrest.EncryptedFields, *cassmodel.QuotedParentMessage, error) {
	var (
		encPayload  []byte
		encMeta     *cassmodel.EncMeta
		msgText     string
		attachments [][]byte
		card        *cassmodel.Card
		cardAction  *cassmodel.CardAction
		quoted      *cassmodel.QuotedParentMessage
	)
	err := r.session.Query(
		`SELECT enc_payload, enc_meta, msg, attachments, card, card_action, quoted_parent_message
		 FROM messages_by_id WHERE message_id = ?`,
		msg.MessageID,
	).WithContext(ctx).Scan(&encPayload, &encMeta, &msgText, &attachments, &card, &cardAction, &quoted)
	if err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return atrest.EncryptedFields{}, nil, ErrMessageNotFound
		}
		return atrest.EncryptedFields{}, nil, fmt.Errorf("read existing fields for message %s: %w", msg.MessageID, err)
	}
	// quoted-parent metadata lives in the plaintext column on both the
	// already-encrypted and legacy paths; the caller re-binds it body-blanked.
	if len(encPayload) > 0 {
		meta := atrest.EncMeta{}
		if encMeta != nil {
			meta.Nonce = encMeta.Nonce
		}
		fields, err := r.cipher.Decrypt(ctx, msg.RoomID, encPayload, meta)
		if err != nil {
			return atrest.EncryptedFields{}, nil, fmt.Errorf("decrypt existing enc_payload for message %s in room %s: %w", msg.MessageID, msg.RoomID, err)
		}
		return fields, quoted, nil
	}
	// Legacy plaintext row — promote the plaintext body columns into the
	// bundle so the subsequent encrypt carries them forward. The legacy
	// columns become stale after the UPDATE but are harmless: the read
	// path branches on enc_payload != nil and ApplyDecryptedFields will
	// overwrite the structScan'd plaintext fields with the bundle's.
	fields := atrest.EncryptedFields{
		Msg:         msgText,
		Attachments: attachments,
		Card:        card,
		CardAction:  cardAction,
	}
	if quoted != nil && (quoted.Msg != "" || len(quoted.Attachments) > 0) {
		fields.QuotedParentContent = &atrest.QuotedParentEncrypted{
			Msg:         quoted.Msg,
			Attachments: quoted.Attachments,
		}
	}
	return fields, quoted, nil
}

// editOne runs the appropriate plaintext or encrypted UPDATE for one of
// the four message tables. plainQ binds (newMsg, mentions, editedAt, editedAt,
// whereArgs...); encQ binds (encPayload, encMeta, mentions, quotedMeta, editedAt,
// editedAt, whereArgs...).
func (r *Repository) editOne(ctx context.Context, plainQ, encQ string, ep *editPayload, editedAt time.Time, whereArgs ...any) error {
	if ep.payload == nil {
		args := append([]any{ep.plain, ep.mentions, editedAt, editedAt}, whereArgs...)
		return r.session.Query(plainQ, args...).WithContext(ctx).Exec()
	}
	args := append([]any{ep.payload, ep.meta, ep.mentions, ep.quotedMeta, editedAt, editedAt}, whereArgs...)
	return r.session.Query(encQ, args...).WithContext(ctx).Exec()
}

// editInMessagesByID runs the edit on the canonical row. Existence and the
// not-deleted check are already enforced by the service layer's findMessage,
// so this is a plain UPDATE rather than an LWT.
func (r *Repository) editInMessagesByID(ctx context.Context, msg *models.Message, ep *editPayload, editedAt time.Time) error {
	return r.editOne(ctx, editMsgByID, editMsgByIDEncrypted, ep, editedAt, msg.MessageID)
}

func (r *Repository) editInMessagesByRoom(ctx context.Context, msg *models.Message, ep *editPayload, editedAt time.Time) error {
	b := r.bucket.Of(msg.CreatedAt)
	return r.editOne(ctx, editMsgByRoom, editMsgByRoomEncrypted, ep, editedAt, msg.RoomID, b, msg.CreatedAt, msg.MessageID)
}

// editInThreadMessagesByThread edits the thread mirror row. thread_messages_by_thread
// is partitioned by thread_room_id alone, so room_id/bucket no longer enter the key.
func (r *Repository) editInThreadMessagesByThread(ctx context.Context, msg *models.Message, ep *editPayload, editedAt time.Time) error {
	return r.editOne(ctx, editThreadMsg, editThreadMsgEncrypted, ep, editedAt, msg.ThreadRoomID, msg.CreatedAt, msg.MessageID)
}

func (r *Repository) editInPinnedMessagesByRoom(ctx context.Context, msg *models.Message, ep *editPayload, editedAt time.Time) error {
	return r.editOne(ctx, editPinnedMsg, editPinnedMsgEncrypted, ep, editedAt, msg.RoomID, *msg.PinnedAt, msg.MessageID)
}

func (r *Repository) deleteInMessagesByRoom(ctx context.Context, q string, msg *models.Message, deletedAt time.Time) error {
	b := r.bucket.Of(msg.CreatedAt)
	return r.session.Query(q, deletedAt, msg.RoomID, b, msg.CreatedAt, msg.MessageID).WithContext(ctx).Exec()
}

func (r *Repository) deleteInPinnedMessagesByRoom(ctx context.Context, q string, msg *models.Message, deletedAt time.Time) error {
	return r.session.Query(q, deletedAt, msg.RoomID, *msg.PinnedAt, msg.MessageID).WithContext(ctx).Exec()
}

func (r *Repository) UpdateMessageContent(ctx context.Context, msg *models.Message, newMsg string, mentions []pkgmodel.Participant, editedAt time.Time) error {
	if msg.ThreadParentID != "" && msg.ThreadRoomID == "" {
		return fmt.Errorf("edit thread message %s: ThreadParentID %q is set but ThreadRoomID is empty", msg.MessageID, msg.ThreadParentID)
	}

	// The service layer's findMessage already gates existence and the
	// not-deleted check before this is reached, and a message is only edited
	// by its owner, so the per-table UPDATEs run without a CAS. On the
	// cipher-enabled path, buildEditPayload still reads the canonical row to
	// re-encrypt while preserving the other encrypted fields; a missing row
	// there surfaces as ErrMessageNotFound.
	ep, err := r.buildEditPayload(ctx, msg, newMsg, mentions)
	if err != nil {
		if errors.Is(err, ErrMessageNotFound) {
			return fmt.Errorf("edit message %s: %w", msg.MessageID, ErrMessageNotFound)
		}
		return fmt.Errorf("prepare edit payload for message %s: %w", msg.MessageID, err)
	}

	if err := r.editInMessagesByID(ctx, msg, &ep, editedAt); err != nil {
		return fmt.Errorf("update messages_by_id for message %s: %w", msg.MessageID, err)
	}

	if msg.ThreadParentID == "" {
		if err := r.editInMessagesByRoom(ctx, msg, &ep, editedAt); err != nil {
			return fmt.Errorf("update messages_by_room for message %s in room %s: %w", msg.MessageID, msg.RoomID, err)
		}
	} else {
		if err := r.editInThreadMessagesByThread(ctx, msg, &ep, editedAt); err != nil {
			return fmt.Errorf("update thread_messages_by_thread for message %s thread %s: %w", msg.MessageID, msg.ThreadRoomID, err)
		}
		// A TShow ("also send to channel") thread reply is dual-written into
		// messages_by_room at create time, so the edit must also land there or
		// the channel-timeline copy goes stale. Additive on top of the thread
		// edit — same shape as the PinnedAt branch below.
		if msg.TShow {
			if err := r.editInMessagesByRoom(ctx, msg, &ep, editedAt); err != nil {
				return fmt.Errorf("update messages_by_room for tshow thread message %s in room %s: %w", msg.MessageID, msg.RoomID, err)
			}
		}
	}

	if msg.PinnedAt != nil {
		if err := r.editInPinnedMessagesByRoom(ctx, msg, &ep, editedAt); err != nil {
			return fmt.Errorf("update pinned_messages_by_room for message %s in room %s: %w", msg.MessageID, msg.RoomID, err)
		}
	}

	return nil
}

// SoftDeleteMessage uses a Cassandra LWT on messages_by_id as a one-shot gate so only
// the winning goroutine runs mirror-table updates and tcount decrement, preventing double-decrement.
// `IF deleted != true` matches NULL (message-worker never writes deleted) and false, excluding true.
// The returned newThreadLastMsgAt is the newest surviving reply's createdAt (nil when none survive),
// so the caller can publish it on the canonical event without a second read.
func (r *Repository) SoftDeleteMessage(ctx context.Context, msg *models.Message, deletedAt time.Time) (time.Time, bool, *int, *time.Time, error) {
	if msg.ThreadParentID != "" && msg.ThreadRoomID == "" {
		return time.Time{}, false, nil, nil, fmt.Errorf("delete thread message %s: ThreadParentID %q is set but ThreadRoomID is empty", msg.MessageID, msg.ThreadParentID)
	}

	isThreadParent := msg.TCount != nil && *msg.TCount > 0

	casQuery := deleteMsgByIDCAS
	if isThreadParent {
		casQuery = deleteThreadParentMsgByIDCAS
	}

	var current bool
	applied, err := r.session.Query(
		casQuery,
		deletedAt, msg.MessageID,
	).WithContext(ctx).ScanCAS(&current)
	if err != nil {
		return time.Time{}, false, nil, nil, fmt.Errorf("cas update messages_by_id for message %s: %w", msg.MessageID, err)
	}
	if !applied {
		// Concurrent delete won. Read the existing updated_at so the caller
		// can return an accurate response timestamp.
		var existing time.Time
		if err := r.session.Query(
			`SELECT updated_at FROM messages_by_id WHERE message_id = ?`,
			msg.MessageID,
		).WithContext(ctx).Scan(&existing); err != nil {
			if errors.Is(err, gocql.ErrNotFound) {
				// Row vanished between the CAS and the follow-up SELECT — abnormal race.
				return time.Time{}, false, nil, nil, fmt.Errorf("message %s vanished after cas miss: %w", msg.MessageID, gocql.ErrNotFound)
			}
			return time.Time{}, false, nil, nil, fmt.Errorf("read updated_at after cas miss for message %s: %w", msg.MessageID, err)
		}
		r.reconcileAfterCASMiss(ctx, msg)
		return existing, false, nil, nil, nil
	}

	msgByRoomQ := deleteMsgByRoom
	threadMsgQ := deleteThreadMsg
	pinnedMsgQ := deletePinnedMsg
	if isThreadParent {
		msgByRoomQ = deleteThreadParentMsgByRoom
		threadMsgQ = deleteThreadParentThreadMsg
		pinnedMsgQ = deleteThreadParentPinnedMsg
	}

	if msg.ThreadParentID == "" {
		if err := r.deleteInMessagesByRoom(ctx, msgByRoomQ, msg, deletedAt); err != nil {
			return time.Time{}, false, nil, nil, fmt.Errorf("update messages_by_room for message %s in room %s: %w", msg.MessageID, msg.RoomID, err)
		}
	} else {
		if err := r.session.Query(threadMsgQ, deletedAt, msg.ThreadRoomID, msg.CreatedAt, msg.MessageID).WithContext(ctx).Exec(); err != nil {
			return time.Time{}, false, nil, nil, fmt.Errorf("update thread_messages_by_thread for message %s thread %s: %w", msg.MessageID, msg.ThreadRoomID, err)
		}
		// A TShow ("also send to channel") thread reply is dual-written into
		// messages_by_room at create time; soft-delete must also hit that copy
		// or it stays visible in the channel timeline. Additive on top of the
		// thread delete — same shape as the PinnedAt branch below.
		if msg.TShow {
			if err := r.deleteInMessagesByRoom(ctx, msgByRoomQ, msg, deletedAt); err != nil {
				return time.Time{}, false, nil, nil, fmt.Errorf("update messages_by_room for tshow thread message %s in room %s: %w", msg.MessageID, msg.RoomID, err)
			}
		}
	}

	if msg.PinnedAt != nil {
		if err := r.deleteInPinnedMessagesByRoom(ctx, pinnedMsgQ, msg, deletedAt); err != nil {
			return time.Time{}, false, nil, nil, fmt.Errorf("update pinned_messages_by_room for message %s in room %s: %w", msg.MessageID, msg.RoomID, err)
		}
	}

	if msg.ThreadParentID == "" {
		return deletedAt, true, nil, nil, nil
	}
	newTcount, newTlm, err := r.countAndSetParentTcount(ctx, msg)
	if err != nil {
		// The LWT delete already committed — return applied=true so callers correctly
		// identify this as a count-set failure rather than a concurrent-winner race.
		return deletedAt, true, nil, nil, fmt.Errorf("count and set parent tcount for message %s: %w", msg.MessageID, err)
	}
	return deletedAt, true, newTcount, newTlm, nil
}

// countAndSetParentTcount stamps the parent's reply count after this reply's
// soft-delete; the policy lives in pkg/threadcount, shared with the add-path
// writers. Returns (nil, nil, nil) when ThreadParentCreatedAt is unset.
func (r *Repository) countAndSetParentTcount(ctx context.Context, msg *models.Message) (*int, *time.Time, error) {
	if msg.ThreadParentCreatedAt == nil {
		return nil, nil, nil
	}
	res, err := threadcount.Maintain(ctx, r.session, msg.ThreadRoomID, r.parent(msg), r.threadPolicy, -1, nil, false)
	if err != nil {
		return nil, nil, fmt.Errorf("maintain parent tcount: %w", err)
	}
	return &res.Count, res.TLM, nil
}

// parent locates the thread parent's two rows for pkg/threadcount.
func (r *Repository) parent(msg *models.Message) threadcount.Parent {
	parentCreatedAt := *msg.ThreadParentCreatedAt
	return threadcount.Parent{
		MessageID: msg.ThreadParentID,
		RoomID:    msg.RoomID,
		CreatedAt: parentCreatedAt,
		Bucket:    r.bucket.Of(parentCreatedAt),
	}
}

// reconcileAfterCASMiss repairs the parent count when a thread reply's delete
// LWT reports "already deleted".
//
// Usually a concurrent delete won and did the decrement itself, so there is
// nothing to do. But it is also what the retry of a partly applied delete sees
// — LWT committed, count not adjusted — which above the scan limit nothing
// else would ever correct. Gated by ShouldReanchor so the full scan it costs
// stays inside the same amortized budget as every other re-anchor, and
// best-effort throughout: the delete has already committed, so a failure here
// must not fail the request.
func (r *Repository) reconcileAfterCASMiss(ctx context.Context, msg *models.Message) {
	if msg.ThreadParentID == "" || msg.ThreadRoomID == "" || msg.ThreadParentCreatedAt == nil {
		return
	}
	if _, err := threadcount.ReanchorIfDue(ctx, r.session, msg.ThreadRoomID, r.parent(msg), r.threadPolicy); err != nil {
		slog.WarnContext(ctx, "thread tcount re-anchor after delete cas miss failed — parent count left as stamped",
			"error", err, "thread_room_id", msg.ThreadRoomID, "parent_message_id", msg.ThreadParentID)
	}
}
