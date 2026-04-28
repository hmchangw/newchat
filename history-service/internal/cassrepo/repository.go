package cassrepo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/history-service/internal/models"
)

// baseColumns are shared across messages_by_room and messages_by_id.
const baseColumns = "room_id, created_at, message_id, thread_room_id, sender, target_user, " +
	"msg, mentions, attachments, file, card, card_action, tshow, tcount, " +
	"thread_parent_id, thread_parent_created_at, quoted_parent_message, " +
	"visible_to, unread, reactions, deleted, " +
	"type, sys_msg_data, site_id, edited_at, updated_at"

// messageByIDExtraColumns are the columns only present in messages_by_id.
const messageByIDExtraColumns = ", pinned_at, pinned_by"

const messageByRoomQuery = "SELECT " + baseColumns + " FROM messages_by_room"
const messageByIDQuery = "SELECT " + baseColumns + messageByIDExtraColumns + " FROM messages_by_id"

// baseScanDest returns Scan destination pointers for the baseColumns in order.
func baseScanDest(m *models.Message) []any {
	return []any{
		&m.RoomID, &m.CreatedAt, &m.MessageID, &m.ThreadRoomID,
		&m.Sender, &m.TargetUser, &m.Msg,
		&m.Mentions, &m.Attachments, &m.File,
		&m.Card, &m.CardAction, &m.TShow, &m.TCount,
		&m.ThreadParentID, &m.ThreadParentCreatedAt, &m.QuotedParentMessage,
		&m.VisibleTo, &m.Unread, &m.Reactions,
		&m.Deleted, &m.Type, &m.SysMsgData,
		&m.SiteID, &m.EditedAt, &m.UpdatedAt,
	}
}

// messageByIDScanDest returns Scan destination pointers for all messages_by_id columns.
func messageByIDScanDest(m *models.Message) []any {
	return append(baseScanDest(m), &m.PinnedAt, &m.PinnedBy)
}

// casMaxRetries mirrors the constant used by message-worker's tcount
// increment. A conflict means another thread-reply increment or decrement
// landed between our read and CAS; 16 retries are sufficient for realistic
// bursts while bounding the loop.
const casMaxRetries = 16

// casDecrement atomically decrements a nullable INT counter toward zero
// (clamping at zero). Mirrors the shape of message-worker's casIncrement at
// message-worker/store_cassandra.go:127 but decrements instead of increments.
func casDecrement(maxRetries int, initial *int, update func(newVal int, expected *int) (applied bool, current *int, err error)) error {
	tcount := initial
	for range maxRetries {
		newVal := 0
		if tcount != nil && *tcount > 0 {
			newVal = *tcount - 1
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
	return fmt.Errorf("cas decrement exceeded %d retries", maxRetries)
}

// Repository implements service.MessageRepository using Cassandra.
type Repository struct {
	session *gocql.Session
}

// NewRepository creates a new Cassandra repository.
func NewRepository(session *gocql.Session) *Repository {
	return &Repository{session: session}
}

func scanMessages(iter *gocql.Iter) []models.Message {
	messages := make([]models.Message, 0)
	for {
		var m models.Message
		if !iter.Scan(baseScanDest(&m)...) {
			break
		}
		messages = append(messages, m)
	}
	return messages
}

// GetMessagesBefore returns a paginated set of messages strictly before `before`, newest-first.
func (r *Repository) GetMessagesBefore(ctx context.Context, roomID string, before time.Time, q PageRequest) (Page[models.Message], error) {
	var messages []models.Message

	nextCursor, err := NewQueryBuilder(
		r.session.Query(
			messageByRoomQuery+` WHERE room_id = ? AND created_at < ? ORDER BY created_at DESC`,
			roomID, before,
		).WithContext(ctx),
	).
		WithCursor(q.Cursor).
		WithPageSize(q.PageSize).
		Fetch(func(iter *gocql.Iter) {
			messages = scanMessages(iter)
		})
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("querying messages before: %w", err)
	}

	return Page[models.Message]{
		Data:       messages,
		NextCursor: nextCursor,
		HasNext:    nextCursor != "",
	}, nil
}

// GetMessagesBetweenDesc returns a paginated set of messages between `since` and `before`, newest-first.
// Used when a lower-bound access restriction (e.g. historySharedSince) must be enforced.
func (r *Repository) GetMessagesBetweenDesc(ctx context.Context, roomID string, since, before time.Time, q PageRequest) (Page[models.Message], error) {
	var messages []models.Message

	nextCursor, err := NewQueryBuilder(
		r.session.Query(
			messageByRoomQuery+` WHERE room_id = ? AND created_at > ? AND created_at < ? ORDER BY created_at DESC`,
			roomID, since, before,
		).WithContext(ctx),
	).
		WithCursor(q.Cursor).
		WithPageSize(q.PageSize).
		Fetch(func(iter *gocql.Iter) {
			messages = scanMessages(iter)
		})
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("querying messages between desc: %w", err)
	}

	return Page[models.Message]{
		Data:       messages,
		NextCursor: nextCursor,
		HasNext:    nextCursor != "",
	}, nil
}

// GetMessagesAfter returns a paginated set of messages strictly after `after`, oldest-first.
func (r *Repository) GetMessagesAfter(ctx context.Context, roomID string, after time.Time, q PageRequest) (Page[models.Message], error) {
	var messages []models.Message

	nextCursor, err := NewQueryBuilder(
		r.session.Query(
			messageByRoomQuery+` WHERE room_id = ? AND created_at > ? ORDER BY created_at ASC`,
			roomID, after,
		).WithContext(ctx),
	).
		WithCursor(q.Cursor).
		WithPageSize(q.PageSize).
		Fetch(func(iter *gocql.Iter) {
			messages = scanMessages(iter)
		})
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("querying messages after: %w", err)
	}

	return Page[models.Message]{
		Data:       messages,
		NextCursor: nextCursor,
		HasNext:    nextCursor != "",
	}, nil
}

// GetAllMessagesAsc returns a paginated set of all messages in the room, oldest-first.
// Used when no lower-bound cursor exists.
func (r *Repository) GetAllMessagesAsc(ctx context.Context, roomID string, q PageRequest) (Page[models.Message], error) {
	var messages []models.Message

	nextCursor, err := NewQueryBuilder(
		r.session.Query(
			messageByRoomQuery+` WHERE room_id = ? ORDER BY created_at ASC`,
			roomID,
		).WithContext(ctx),
	).
		WithCursor(q.Cursor).
		WithPageSize(q.PageSize).
		Fetch(func(iter *gocql.Iter) {
			messages = scanMessages(iter)
		})
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("querying all messages asc: %w", err)
	}

	return Page[models.Message]{
		Data:       messages,
		NextCursor: nextCursor,
		HasNext:    nextCursor != "",
	}, nil
}

// GetMessageByID returns a single message from the messages_by_id lookup table.
// Returns (nil, nil) if the message is not found.
func (r *Repository) GetMessageByID(ctx context.Context, messageID string) (*models.Message, error) {
	var m models.Message
	err := r.session.Query(
		messageByIDQuery+` WHERE message_id = ?`,
		messageID,
	).WithContext(ctx).Scan(messageByIDScanDest(&m)...)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying message by id: %w", err)
	}
	return &m, nil
}

// UpdateMessageContent updates the msg, edited_at, and updated_at fields
// across the Cassandra tables that actually hold the row, determined from
// msg's own metadata. Top-level messages (msg.ThreadParentID == "") land in
// messages_by_room; thread replies land in thread_messages_by_room; pinned
// messages additionally land in pinned_messages_by_room. messages_by_id is
// always updated. All UPDATEs use the full PK; none is a no-op against a
// missing row — see spec doc for the Cassandra phantom-row rationale.
// Idempotent with respect to msg content; timestamps advance per call.
func (r *Repository) UpdateMessageContent(ctx context.Context, msg *models.Message, newMsg string, editedAt time.Time) error {
	// Always: messages_by_id
	if err := r.session.Query(
		`UPDATE messages_by_id SET msg = ?, edited_at = ?, updated_at = ? WHERE message_id = ? AND created_at = ?`,
		newMsg, editedAt, editedAt, msg.MessageID, msg.CreatedAt,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("update messages_by_id: %w", err)
	}

	// Top-level vs thread-reply: mutually exclusive.
	if msg.ThreadParentID == "" {
		if err := r.session.Query(
			`UPDATE messages_by_room SET msg = ?, edited_at = ?, updated_at = ? WHERE room_id = ? AND created_at = ? AND message_id = ?`,
			newMsg, editedAt, editedAt, msg.RoomID, msg.CreatedAt, msg.MessageID,
		).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("update messages_by_room: %w", err)
		}
	} else {
		if err := r.session.Query(
			`UPDATE thread_messages_by_room SET msg = ?, edited_at = ?, updated_at = ? WHERE room_id = ? AND thread_room_id = ? AND created_at = ? AND message_id = ?`,
			newMsg, editedAt, editedAt, msg.RoomID, msg.ThreadRoomID, msg.CreatedAt, msg.MessageID,
		).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("update thread_messages_by_room: %w", err)
		}
	}

	// Pinned mirror — additive to either of the above.
	if msg.PinnedAt != nil {
		if err := r.session.Query(
			`UPDATE pinned_messages_by_room SET msg = ?, edited_at = ?, updated_at = ? WHERE room_id = ? AND created_at = ? AND message_id = ?`,
			newMsg, editedAt, editedAt, msg.RoomID, *msg.PinnedAt, msg.MessageID,
		).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("update pinned_messages_by_room: %w", err)
		}
	}

	return nil
}

// SoftDeleteMessage marks the given message deleted across all applicable
// Cassandra tables. For thread replies it also decrements the parent
// message's tcount via lightweight transactions. See the delete spec for the
// table-membership + tcount semantics.
//
// The messages_by_id UPDATE is a Cassandra LWT (`IF deleted != true`) used as
// a one-shot gate: only the goroutine that flips deleted to true runs the
// mirror-table UPDATEs and the parent-tcount decrement. A concurrent second
// delete observes applied=false and returns without performing the side
// effects, preventing tcount double-decrement on the parent.
//
// `IF deleted != true` matches both NULL (the value at INSERT time —
// message-worker doesn't write deleted) and false, while excluding true.
//
// Returns:
//   - actualDeletedAt: the timestamp now in messages_by_id.updated_at. When
//     applied, equals deletedAt; when a concurrent delete won, equals the
//     value the winning goroutine wrote (read back via SELECT).
//   - applied: true if this call performed the soft-delete (and ran the
//     mirror-table + tcount work). false if the LWT did not apply.
//   - err: a real Cassandra error.
//
// Partial failure between the LWT and the tcount decrement can still drift
// tcount by one — same model as the worker-side increment drift.
func (r *Repository) SoftDeleteMessage(ctx context.Context, msg *models.Message, deletedAt time.Time) (time.Time, bool, error) {
	// Step 1 — CAS gate on messages_by_id.
	var current bool
	applied, err := r.session.Query(
		`UPDATE messages_by_id SET deleted = true, updated_at = ? WHERE message_id = ? AND created_at = ? IF deleted != true`,
		deletedAt, msg.MessageID, msg.CreatedAt,
	).WithContext(ctx).ScanCAS(&current)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("cas update messages_by_id: %w", err)
	}
	if !applied {
		// Concurrent delete won. Read the existing updated_at so the caller
		// can return an accurate response timestamp.
		var existing time.Time
		if err := r.session.Query(
			`SELECT updated_at FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
			msg.MessageID, msg.CreatedAt,
		).WithContext(ctx).Scan(&existing); err != nil {
			if errors.Is(err, gocql.ErrNotFound) {
				return time.Time{}, false, nil
			}
			return time.Time{}, false, fmt.Errorf("read updated_at after cas miss: %w", err)
		}
		return existing, false, nil
	}

	// Step 2 — top-level vs thread-reply mirror update (mutually exclusive).
	if msg.ThreadParentID == "" {
		if err := r.session.Query(
			`UPDATE messages_by_room SET deleted = true, updated_at = ? WHERE room_id = ? AND created_at = ? AND message_id = ?`,
			deletedAt, msg.RoomID, msg.CreatedAt, msg.MessageID,
		).WithContext(ctx).Exec(); err != nil {
			return time.Time{}, false, fmt.Errorf("update messages_by_room: %w", err)
		}
	} else {
		if err := r.session.Query(
			`UPDATE thread_messages_by_room SET deleted = true, updated_at = ? WHERE room_id = ? AND thread_room_id = ? AND created_at = ? AND message_id = ?`,
			deletedAt, msg.RoomID, msg.ThreadRoomID, msg.CreatedAt, msg.MessageID,
		).WithContext(ctx).Exec(); err != nil {
			return time.Time{}, false, fmt.Errorf("update thread_messages_by_room: %w", err)
		}
	}

	// Step 3 — pinned mirror, additive.
	if msg.PinnedAt != nil {
		if err := r.session.Query(
			`UPDATE pinned_messages_by_room SET deleted = true, updated_at = ? WHERE room_id = ? AND created_at = ? AND message_id = ?`,
			deletedAt, msg.RoomID, *msg.PinnedAt, msg.MessageID,
		).WithContext(ctx).Exec(); err != nil {
			return time.Time{}, false, fmt.Errorf("update pinned_messages_by_room: %w", err)
		}
	}

	// Step 4 — tcount decrement on the parent for thread-reply deletes.
	if msg.ThreadParentID != "" {
		if err := r.decrementParentTcount(ctx, msg); err != nil {
			return time.Time{}, false, fmt.Errorf("decrement parent tcount: %w", err)
		}
	}

	return deletedAt, true, nil
}

// decrementParentTcount decrements tcount on the parent message row in both
// messages_by_id and messages_by_room using Cassandra Lightweight Transactions
// (IF tcount = ?). Silently skips if ThreadParentCreatedAt is nil or if the
// parent row is missing (ErrNotFound).
func (r *Repository) decrementParentTcount(ctx context.Context, msg *models.Message) error {
	if msg.ThreadParentCreatedAt == nil {
		return nil
	}
	parentID := msg.ThreadParentID
	parentCreatedAt := *msg.ThreadParentCreatedAt

	// CAS decrement on messages_by_id.
	var tcount *int
	if err := r.session.Query(
		`SELECT tcount FROM messages_by_id WHERE message_id = ? AND created_at = ?`,
		parentID, parentCreatedAt,
	).WithContext(ctx).Scan(&tcount); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("read tcount for parent %s in messages_by_id: %w", parentID, err)
	}
	if err := casDecrement(casMaxRetries, tcount, func(newVal int, expected *int) (bool, *int, error) {
		var current *int
		applied, err := r.session.Query(
			`UPDATE messages_by_id SET tcount = ? WHERE message_id = ? AND created_at = ? IF tcount = ?`,
			newVal, parentID, parentCreatedAt, expected,
		).WithContext(ctx).ScanCAS(&current)
		return applied, current, err
	}); err != nil {
		return fmt.Errorf("cas tcount decrement in messages_by_id for parent %s: %w", parentID, err)
	}

	// CAS decrement on messages_by_room.
	if err := r.session.Query(
		`SELECT tcount FROM messages_by_room WHERE room_id = ? AND created_at = ? AND message_id = ?`,
		msg.RoomID, parentCreatedAt, parentID,
	).WithContext(ctx).Scan(&tcount); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("read tcount for parent %s in messages_by_room: %w", parentID, err)
	}
	if err := casDecrement(casMaxRetries, tcount, func(newVal int, expected *int) (bool, *int, error) {
		var current *int
		applied, err := r.session.Query(
			`UPDATE messages_by_room SET tcount = ? WHERE room_id = ? AND created_at = ? AND message_id = ? IF tcount = ?`,
			newVal, msg.RoomID, parentCreatedAt, parentID, expected,
		).WithContext(ctx).ScanCAS(&current)
		return applied, current, err
	}); err != nil {
		return fmt.Errorf("cas tcount decrement in messages_by_room for parent %s: %w", parentID, err)
	}

	return nil
}
