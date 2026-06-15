package model

// PushNotificationEvent is the batched push envelope published to PUSH_NOTIFICATION_{siteID}.
// Accounts carries up to PUSH_RECIPIENT_BATCH_SIZE recipients sharing one canonical message;
// ID is "{messageID}-b{batchIndex}" and doubles as the Nats-Msg-Id dedup key.
type PushNotificationEvent struct {
	ID        string               `json:"id"        bson:"id"`
	Accounts  []string             `json:"accounts"  bson:"accounts"`
	Title     string               `json:"title"     bson:"title"`
	Body      string               `json:"body"      bson:"body"`
	Data      PushNotificationData `json:"data"      bson:"data"`
	RoomID    string               `json:"roomId"    bson:"roomId"`
	Timestamp int64                `json:"timestamp" bson:"timestamp"`
}

// PushNotificationData is the push payload; short legacy tag names (rid/tmid/prid) are spelled
// out to camelCase, and chineseName/engName are collapsed into *Participant Sender.
type PushNotificationData struct {
	RoomID            string       `json:"roomId"                      bson:"roomId"`
	MessageID         string       `json:"messageId"                   bson:"messageId"`
	Type              string       `json:"type"                        bson:"type"`
	Sender            *Participant `json:"sender,omitempty"            bson:"sender,omitempty"`
	ThreadMessageID   string       `json:"threadMessageId,omitempty"   bson:"threadMessageId,omitempty"`
	FileName          string       `json:"fileName,omitempty"          bson:"fileName,omitempty"`
	FileType          string       `json:"fileType,omitempty"          bson:"fileType,omitempty"`
	ParentRoomID      string       `json:"parentRoomId,omitempty"      bson:"parentRoomId,omitempty"`
	PushTime          string       `json:"pushTime"                    bson:"pushTime"`
	AlsoSendToChannel bool         `json:"alsoSendToChannel,omitempty" bson:"alsoSendToChannel,omitempty"`
}
