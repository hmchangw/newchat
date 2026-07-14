package model

import "time"

// MigrationEditRequest is the oplog-transformer's payload to history-service's migration-edit handler.
// It carries the Cassandra locator plus new content, since the oplog update event lacks roomId/createdAt.
type MigrationEditRequest struct {
	MessageID string    `json:"messageId" bson:"messageId"`
	RoomID    string    `json:"roomId"    bson:"roomId"`
	CreatedAt time.Time `json:"createdAt" bson:"createdAt"`
	Content   string    `json:"content"   bson:"content"`
	EditedAt  time.Time `json:"editedAt"  bson:"editedAt"`
}

// MigrationDeleteRequest is the payload for the migration-delete handler. It carries only the message
// id and deletion time — history-service resolves the Cassandra locator from the id via GetMessageByID.
type MigrationDeleteRequest struct {
	MessageID string    `json:"messageId" bson:"messageId"`
	DeletedAt time.Time `json:"deletedAt" bson:"deletedAt"`
}

// MigrationAck is the reply from the internal migration handlers.
type MigrationAck struct {
	OK bool `json:"ok" bson:"ok"`
}
