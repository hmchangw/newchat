package model

import (
	"time"

	"github.com/hmchangw/chat/pkg/model/cassandra"
)

// Bot pipeline shared wire schemas, consumed by botplatform-service and bot-{msg,room}-service.

// Bot NATS headers. BP stamps identity on every outbound RPC.
const (
	HeaderBotIdentity  = "X-Bot-Identity"
	HeaderBotMessageID = "X-Bot-Message-ID"
	HeaderBotCreatedAt = "X-Bot-Created-At"
)

// BotContentMaxBytes caps content payload size; enforced at BP ingress and in bot-message-handler.
const BotContentMaxBytes = 20 * 1024

// BotIdentity is the JSON-encoded caller identity stamped into X-Bot-Identity.
// Downstream trusts this header because NATS account permissions gate chat.server.bot.request.>.
type BotIdentity struct {
	ID          string `json:"id"                    bson:"id"`
	Account     string `json:"account"               bson:"account"`
	SiteID      string `json:"siteId"                bson:"siteId"`
	EngName     string `json:"engName,omitempty"     bson:"engName,omitempty"`
	ChineseName string `json:"chineseName,omitempty" bson:"chineseName,omitempty"`
	AppID       string `json:"appId,omitempty"       bson:"appId,omitempty"`
	AppName     string `json:"appName,omitempty"     bson:"appName,omitempty"`
}

// BotSendMessageRequest is the send-in-room and send-DM body. Sender comes from X-Bot-Identity; attachments are omitted and strict decoders reject them.
type BotSendMessageRequest struct {
	Content  string          `json:"content"            bson:"content"`
	Mentions []Participant   `json:"mentions,omitempty" bson:"mentions,omitempty"`
	Card     *cassandra.Card `json:"card,omitempty"     bson:"card,omitempty"`

	ThreadParentMessageID        string     `json:"threadParentMessageId,omitempty"        bson:"threadParentMessageId,omitempty"`
	ThreadParentMessageCreatedAt *time.Time `json:"threadParentMessageCreatedAt,omitempty" bson:"threadParentMessageCreatedAt,omitempty"`
	TShow                        bool       `json:"tshow,omitempty"                        bson:"tshow,omitempty"`
}

// BotSendResponse is the reply: the canonical Message that landed on BOT_MESSAGES_CANONICAL.
type BotSendResponse struct {
	Message Message `json:"message" bson:"message"`
}

// BotCreateRoomRequest is the create-room body; empty payload creates an empty channel.
type BotCreateRoomRequest struct {
	Name    string   `json:"name"              bson:"name"`
	Topic   string   `json:"topic,omitempty"   bson:"topic,omitempty"`
	Members []string `json:"members,omitempty" bson:"members,omitempty"`
	Orgs    []string `json:"orgs,omitempty"    bson:"orgs,omitempty"`
}

type BotCreateRoomResponse struct {
	ID        string       `json:"id"        bson:"id"`
	Name      string       `json:"name"      bson:"name"`
	Owner     BotOwnerResp `json:"owner"     bson:"owner"`
	Members   []string     `json:"members"   bson:"members"`
	CreatedAt time.Time    `json:"createdAt" bson:"createdAt"`
}

type BotOwnerResp struct {
	ID      string `json:"id"                bson:"id"`
	IsBot   bool   `json:"isBot"             bson:"isBot"`
	AppID   string `json:"appId,omitempty"   bson:"appId,omitempty"`
	AppName string `json:"appName,omitempty" bson:"appName,omitempty"`
}

// BotMembersBatchRequest is the shared add/remove body.
type BotMembersBatchRequest struct {
	UserIDs []string `json:"userIds,omitempty" bson:"userIds,omitempty"`
	OrgIDs  []string `json:"orgIds,omitempty"  bson:"orgIds,omitempty"`
}

type BotAddResponse struct {
	Added BotAddedRemoved `json:"added" bson:"added"`
}

type BotRemoveResponse struct {
	Removed BotAddedRemoved `json:"removed" bson:"removed"`
}

type BotAddedRemoved struct {
	UserIDs []string `json:"userIds" bson:"userIds"`
	OrgIDs  []string `json:"orgIds"  bson:"orgIds"`
}

// BotRoomGetRequest / Response is the internal room.get RPC BP uses to enrich reply payloads.
type BotRoomGetRequest struct {
	RoomID string `json:"roomId" bson:"roomId"`
}

type BotRoomGetResponse struct {
	ID        string    `json:"id"              bson:"id"`
	Type      string    `json:"type"            bson:"type"`
	Name      string    `json:"name,omitempty"  bson:"name,omitempty"`
	Topic     string    `json:"topic,omitempty" bson:"topic,omitempty"`
	SiteID    string    `json:"siteId"          bson:"siteId"`
	CreatedAt time.Time `json:"createdAt"       bson:"createdAt"`
}

// BotDMEnsureRequest / Response is the internal dm.ensure RPC BP calls on first-DM lookup miss.
type BotDMEnsureRequest struct {
	TargetUserID string `json:"targetUserId" bson:"targetUserId"`
}

type BotDMEnsureResponse struct {
	RoomID    string    `json:"roomId"    bson:"roomId"`
	CreatedAt time.Time `json:"createdAt" bson:"createdAt"`
}
