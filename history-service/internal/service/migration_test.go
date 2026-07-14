package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// migrationLocator matches a *models.Message whose MessageID/RoomID/CreatedAt equal the request's.
func migrationLocator(messageID, roomID string, createdAt time.Time) gomock.Matcher {
	return gomock.Cond(func(x any) bool {
		m, ok := x.(*models.Message)
		if !ok {
			return false
		}
		return m.MessageID == messageID && m.RoomID == roomID && m.CreatedAt.Equal(createdAt)
	})
}

func TestHistoryService_MigrationEditMessage_Success(t *testing.T) {
	svc, msgs, _, pub, _ := newService(t)
	c := testContext()

	createdAt := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	editedAt := time.Date(2026, 5, 15, 9, 0, 0, 0, time.UTC)

	msgs.EXPECT().
		UpdateMessageContent(gomock.Any(), migrationLocator("msg-1", "r1", createdAt), "new body", editedAt).
		Return(nil)

	pub.EXPECT().
		PublishMigration(gomock.Any(), subject.MsgCanonicalUpdated("site-test"), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte, msgID string) error {
			var evt model.MessageEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			assert.Equal(t, model.EventUpdated, evt.Event)
			assert.Equal(t, "msg-1", evt.Message.ID)
			assert.Equal(t, "r1", evt.Message.RoomID)
			assert.True(t, evt.Message.CreatedAt.Equal(createdAt))
			assert.Equal(t, "new body", evt.Message.Content)
			require.NotNil(t, evt.Message.EditedAt)
			assert.True(t, evt.Message.EditedAt.Equal(editedAt))
			assert.Equal(t, "site-test", evt.SiteID)
			// Event-level Timestamp is publish-time (now), distinct from the historical
			// domain editedAt carried inside Message.
			assert.Greater(t, evt.Timestamp, editedAt.UnixMilli())
			assert.Equal(t, natsutil.CanonicalDedupID(&evt), msgID)
			return nil
		})

	ack, err := svc.MigrationEditMessage(c, "site-test", model.MigrationEditRequest{
		MessageID: "msg-1",
		RoomID:    "r1",
		CreatedAt: createdAt,
		Content:   "new body",
		EditedAt:  editedAt,
	})
	require.NoError(t, err)
	require.NotNil(t, ack)
	assert.True(t, ack.OK)
}

func TestHistoryService_MigrationEditMessage_WriterError(t *testing.T) {
	svc, msgs, _, pub, _ := newService(t)
	c := testContext()

	createdAt := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	msgs.EXPECT().
		UpdateMessageContent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.New("cassandra down"))
	// No publish on writer failure.
	pub.EXPECT().PublishMigration(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	ack, err := svc.MigrationEditMessage(c, "site-test", model.MigrationEditRequest{
		MessageID: "msg-1",
		RoomID:    "r1",
		CreatedAt: createdAt,
		Content:   "new body",
		EditedAt:  time.Now().UTC(),
	})
	require.Error(t, err)
	assert.Nil(t, ack)
}

func TestHistoryService_MigrationDeleteMessage_Success(t *testing.T) {
	svc, msgs, _, pub, _ := newService(t)
	c := testContext()

	createdAt := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	deletedAt := time.Date(2026, 5, 16, 8, 0, 0, 0, time.UTC)

	msgs.EXPECT().
		GetMessageByID(gomock.Any(), "msg-2").
		Return(&models.Message{MessageID: "msg-2", RoomID: "r1", CreatedAt: createdAt}, nil)
	msgs.EXPECT().
		SoftDeleteMessage(gomock.Any(), migrationLocator("msg-2", "r1", createdAt), deletedAt).
		Return(deletedAt, true, nil, nil)

	pub.EXPECT().
		PublishMigration(gomock.Any(), subject.MsgCanonicalDeleted("site-test"), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, data []byte, msgID string) error {
			var evt model.MessageEvent
			require.NoError(t, json.Unmarshal(data, &evt))
			assert.Equal(t, model.EventDeleted, evt.Event)
			assert.Equal(t, "msg-2", evt.Message.ID)
			assert.Equal(t, "r1", evt.Message.RoomID)
			assert.True(t, evt.Message.CreatedAt.Equal(createdAt))
			require.NotNil(t, evt.Message.UpdatedAt)
			assert.True(t, evt.Message.UpdatedAt.Equal(deletedAt))
			assert.Equal(t, "site-test", evt.SiteID)
			// Event-level Timestamp is publish-time (now), distinct from the historical
			// domain deletedAt carried inside Message.
			assert.Greater(t, evt.Timestamp, deletedAt.UnixMilli())
			assert.Equal(t, natsutil.CanonicalDedupID(&evt), msgID)
			return nil
		})

	ack, err := svc.MigrationDeleteMessage(c, "site-test", model.MigrationDeleteRequest{
		MessageID: "msg-2",
		DeletedAt: deletedAt,
	})
	require.NoError(t, err)
	require.NotNil(t, ack)
	assert.True(t, ack.OK)
}

func TestHistoryService_MigrationDeleteMessage_WriterError(t *testing.T) {
	svc, msgs, _, pub, _ := newService(t)
	c := testContext()

	createdAt := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	msgs.EXPECT().
		GetMessageByID(gomock.Any(), "msg-2").
		Return(&models.Message{MessageID: "msg-2", RoomID: "r1", CreatedAt: createdAt}, nil)
	msgs.EXPECT().
		SoftDeleteMessage(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(time.Time{}, false, nil, errors.New("cassandra down"))
	pub.EXPECT().PublishMigration(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	ack, err := svc.MigrationDeleteMessage(c, "site-test", model.MigrationDeleteRequest{
		MessageID: "msg-2",
		DeletedAt: time.Now().UTC(),
	})
	require.Error(t, err)
	assert.Nil(t, ack)
}

// Delete-before-insert race: the soft-delete reaches history-service before the insert is persisted,
// so the row is absent. The handler must return a retryable error (not OK) so the transformer Naks.
func TestHistoryService_MigrationDeleteMessage_AbsentRowRetries(t *testing.T) {
	svc, msgs, _, pub, _ := newService(t)
	c := testContext()

	// Row not yet persisted by message-worker.
	msgs.EXPECT().
		GetMessageByID(gomock.Any(), "msg-3").
		Return(nil, nil)
	// Must NOT attempt the soft-delete on an absent row, and must NOT publish.
	msgs.EXPECT().SoftDeleteMessage(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	pub.EXPECT().PublishMigration(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	ack, err := svc.MigrationDeleteMessage(c, "site-test", model.MigrationDeleteRequest{
		MessageID: "msg-3",
		DeletedAt: time.Now().UTC(),
	})
	require.Error(t, err)
	assert.Nil(t, ack)
}

// Idempotent redelivery: the row is already soft-deleted. The handler must short-circuit to {OK:true}
// (no error, no re-delete, no re-publish) so a redelivered op doesn't loop forever.
func TestHistoryService_MigrationDeleteMessage_AlreadyDeletedAcksOK(t *testing.T) {
	svc, msgs, _, pub, _ := newService(t)
	c := testContext()

	createdAt := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)

	msgs.EXPECT().
		GetMessageByID(gomock.Any(), "msg-4").
		Return(&models.Message{MessageID: "msg-4", RoomID: "r1", CreatedAt: createdAt, Deleted: true}, nil)
	// Already deleted → no CAS, no publish.
	msgs.EXPECT().SoftDeleteMessage(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	pub.EXPECT().PublishMigration(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	ack, err := svc.MigrationDeleteMessage(c, "site-test", model.MigrationDeleteRequest{
		MessageID: "msg-4",
		DeletedAt: time.Now().UTC(),
	})
	require.NoError(t, err)
	require.NotNil(t, ack)
	assert.True(t, ack.OK)
}

// Edit-before-insert race: the edit reaches history-service before the insert is persisted, so
// UpdateMessageContent surfaces ErrMessageNotFound — propagated as an error so the transformer Naks.
func TestHistoryService_MigrationEditMessage_AbsentRowRetries(t *testing.T) {
	svc, msgs, _, pub, _ := newService(t)
	c := testContext()

	createdAt := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	msgs.EXPECT().
		UpdateMessageContent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("edit message msg-5: %w", cassrepo.ErrMessageNotFound))
	pub.EXPECT().PublishMigration(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	ack, err := svc.MigrationEditMessage(c, "site-test", model.MigrationEditRequest{
		MessageID: "msg-5",
		RoomID:    "r1",
		CreatedAt: createdAt,
		Content:   "new body",
		EditedAt:  time.Now().UTC(),
	})
	require.Error(t, err)
	assert.Nil(t, ack)
	// Not-yet-persisted maps to a 4xx (NotFound), mirroring the delete path —
	// not internal/5xx — so the benign race doesn't log as a server error.
	var ec *errcode.Error
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, errcode.CodeNotFound, ec.Code)
}
