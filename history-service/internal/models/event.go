package models

// MessageEditedEvent is the live event published to chat.room.{roomID}.event
// after a successful edit. Per CLAUDE.md, every NATS event carries a
// Timestamp (event publish time). EditedAt is the domain time when the edit
// occurred; both are populated from a single time.Now().UTC() in the handler.
type MessageEditedEvent struct {
	Type      string `json:"type"      bson:"type"`      // always "message_edited"
	Timestamp int64  `json:"timestamp" bson:"timestamp"` // UTC millis, event publish time
	RoomID    string `json:"roomId"    bson:"roomId"`
	MessageID string `json:"messageId" bson:"messageId"`
	NewMsg    string `json:"newMsg"    bson:"newMsg"`
	EditedBy  string `json:"editedBy"  bson:"editedBy"` // actor account (always == message.sender.account under sender-only auth)
	EditedAt  int64  `json:"editedAt"  bson:"editedAt"` // UTC millis, domain time when edit occurred
}

// MessageDeletedEvent is the live event published to chat.room.{roomID}.event
// after a successful soft delete. Per CLAUDE.md, every NATS event carries a
// Timestamp (event publish time). DeletedAt is the domain time when the
// delete occurred; both are populated from a single time.Now().UTC() in the
// handler. DeletedBy equals the sender account under sender-only auth and is
// included for client rendering convenience — it is not persisted to Cassandra.
type MessageDeletedEvent struct {
	Type      string `json:"type"      bson:"type"`
	Timestamp int64  `json:"timestamp" bson:"timestamp"`
	RoomID    string `json:"roomId"    bson:"roomId"`
	MessageID string `json:"messageId" bson:"messageId"`
	DeletedBy string `json:"deletedBy" bson:"deletedBy"`
	DeletedAt int64  `json:"deletedAt" bson:"deletedAt"`
}
