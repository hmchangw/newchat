package model

import (
	"encoding/json"
	"fmt"
)

// Bot-platform delivery contract (chat.server.bot.delivery.{siteID}): a FOREIGN wire
// shape mirroring the platform's decoder.

// BotEvent.Type values (the inner `event` the bot dispatches on).
const (
	BotEventRoomJoin  = "room_join"
	BotEventRoomLeave = "room_leave"
)

// BotEvent is the bot-facing part the platform forwards to the webhook
// verbatim; Data is pre-marshaled so per-event payloads need no untyped map.
type BotEvent struct {
	Type string          `json:"event"`
	Data json.RawMessage `json:"data"`
}

// NatsEvent is the delivery envelope. BotEvent is embedded BY VALUE: it
// flattens `event`/`data` and a decoded envelope can never nil-panic on them.
type NatsEvent struct {
	BotEvent
	ID            string   `json:"eventId"`
	Timestamp     string   `json:"ts"` // RFC3339 — platform contract, NOT our UnixMilli int64
	Origin        string   `json:"origin"`
	PublishID     string   `json:"publishId"`
	Subscriptions []string `json:"subscriptions"`
}

// BotRoomRef is the `room` object inside a room_join/room_leave data payload.
// The bot keys on _id; name is cosmetic and omitted on room_leave.
type BotRoomRef struct {
	ID   string   `json:"_id"`
	Name string   `json:"name,omitempty"`
	Type RoomType `json:"type"`
}

// BotRoomUser is the `user` object — the member the event is about. Mirrors the
// legacy 1.0 producer's field set (no createdAt). _id is omitted on the
// org-sweep leave path, which does not carry it.
type BotRoomUser struct {
	ID       string `json:"_id,omitempty"`
	Username string `json:"username"`
	Name     string `json:"name,omitempty"`
	EngName  string `json:"engName,omitempty"`
}

// BotRoomData is the `data` payload for room_join/room_leave: {room, user},
// forwarded to each bot verbatim. Bots read data.room / data.user.
type BotRoomData struct {
	Room BotRoomRef  `json:"room"`
	User BotRoomUser `json:"user"`
}

// BotRoomEventParams bundles the inputs for a room_join/room_leave NatsEvent.
type BotRoomEventParams struct {
	EventType string // BotEventRoomJoin | BotEventRoomLeave
	EventID   string
	Timestamp string // RFC3339
	SiteID    string

	Room BotRoomRef
	User BotRoomUser

	// Subscriptions is the set of bot accounts to notify (the wire
	// `subscriptions` list) — the room's bot roster for this event. The dedup
	// subject is User.Username, the member the event is about.
	Subscriptions []string
}

// NewBotRoomNatsEvent builds the envelope for a membership change; publishId is
// the room id, Subscriptions holds every bot the event fans out to.
func NewBotRoomNatsEvent(p *BotRoomEventParams) (NatsEvent, error) {
	data, err := json.Marshal(BotRoomData{Room: p.Room, User: p.User})
	if err != nil {
		return NatsEvent{}, fmt.Errorf("marshal bot room data: %w", err)
	}
	return NatsEvent{
		BotEvent:      BotEvent{Type: p.EventType, Data: data},
		ID:            p.EventID,
		Timestamp:     p.Timestamp,
		Origin:        p.SiteID,
		PublishID:     p.Room.ID,
		Subscriptions: p.Subscriptions,
	}, nil
}
