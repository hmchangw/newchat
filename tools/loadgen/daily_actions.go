package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// subscriptionListRequest mirrors user-service's subscription.list wire body; local copy avoids cross-service import.
type subscriptionListRequest struct {
	Type              string `json:"type"`
	Favorite          *bool  `json:"favorite,omitempty"`
	UpdatedWithinDays *int   `json:"updatedWithinDays,omitempty"`
}

// publishFn matches the existing Publisher interface used by generator.go.
type publishFn func(ctx context.Context, subj string, data []byte) error

// requestFn does a NATS request/reply.
type requestFn func(ctx context.Context, subj string, data []byte, timeout time.Duration) ([]byte, error)

// actionCtx bundles everything every action handler needs. Keeps function
// signatures small and tests easy to write.
type actionCtx struct {
	Ctx       context.Context
	Publish   publishFn
	Request   requestFn
	SiteID    string
	Collector *Collector // optional; for latency correlation
	Rand      *rand.Rand // optional; falls back to a per-call source
}

func (a actionCtx) rand() *rand.Rand {
	if a.Rand != nil {
		return a.Rand
	}
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

const defaultRequestTimeout = 5 * time.Second

// sendMessage publishes a SendMessageRequest on the frontdoor subject for a
// random room the user belongs to. If u has no rooms, returns nil (noop).
func sendMessage(a actionCtx, u *userState, content string) error {
	if len(u.Rooms) == 0 {
		return nil
	}
	roomID := u.Rooms[a.rand().Intn(len(u.Rooms))]
	msgID := idgen.GenerateMessageID()
	reqID := idgen.GenerateRequestID()
	req := model.SendMessageRequest{ID: msgID, Content: content, RequestID: reqID}
	data, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal send-message: %w", err)
	}
	if a.Collector != nil {
		a.Collector.RecordPublish(reqID, msgID, time.Now())
	}
	if err := a.Publish(a.Ctx, subject.MsgSend(u.Account, roomID, a.SiteID), data); err != nil {
		if a.Collector != nil {
			a.Collector.RecordPublishFailed(reqID, msgID)
		}
		return fmt.Errorf("publish send-message: %w", err)
	}
	return nil
}

// markRead marks a random room as read via room-service's MessageRead handler. Must be a Request (not Publish)
// because natsMessageRead calls msg.Respond unconditionally.
func markRead(a actionCtx, u *userState, lastMsgID string) error {
	if len(u.Rooms) == 0 {
		return nil
	}
	roomID := u.Rooms[a.rand().Intn(len(u.Rooms))]
	payload, err := json.Marshal(map[string]string{"messageId": lastMsgID})
	if err != nil {
		return fmt.Errorf("marshal mark-read: %w", err)
	}
	if _, err := a.Request(a.Ctx, subject.MessageRead(u.Account, roomID, a.SiteID), payload, defaultRequestTimeout); err != nil {
		return fmt.Errorf("request mark-read: %w", err)
	}
	return nil
}

// refreshRoomList does a NATS request/reply for the user's subscription list.
func refreshRoomList(a actionCtx, u *userState) error {
	payload, err := json.Marshal(subscriptionListRequest{Type: "rooms"})
	if err != nil {
		return fmt.Errorf("marshal room-list: %w", err)
	}
	_, err = a.Request(a.Ctx, subject.UserSubscriptionList(u.Account, a.SiteID), payload, defaultRequestTimeout)
	if err != nil {
		return fmt.Errorf("request room-list: %w", err)
	}
	return nil
}

// scrollHistory does a NATS request/reply for a random room's recent history.
func scrollHistory(a actionCtx, u *userState) error {
	if len(u.Rooms) == 0 {
		return nil
	}
	roomID := u.Rooms[a.rand().Intn(len(u.Rooms))]
	_, err := a.Request(a.Ctx, subject.MsgGet(u.Account, roomID, a.SiteID), nil, defaultRequestTimeout)
	if err != nil {
		return fmt.Errorf("request scroll-history: %w", err)
	}
	return nil
}

// muteToggle requests the mute toggle for a random room.
func muteToggle(a actionCtx, u *userState) error {
	if len(u.Rooms) == 0 {
		return nil
	}
	roomID := u.Rooms[a.rand().Intn(len(u.Rooms))]
	_, err := a.Request(a.Ctx, subject.MuteToggle(u.Account, roomID, a.SiteID), nil, defaultRequestTimeout)
	if err != nil {
		return fmt.Errorf("request mute-toggle: %w", err)
	}
	return nil
}

// roomCreate creates a new channel room and invites u.Neighbor; room-service rejects channel-create with no invitees.
// The resulting roomID is intentionally not added to u.Rooms (new room not immediately active in the same window).
func roomCreate(a actionCtx, u *userState) error {
	users := []string{}
	if u.Neighbor != "" {
		users = append(users, u.Neighbor)
	}
	payload, err := json.Marshal(map[string]any{
		"name":  fmt.Sprintf("loadtest-%s-%d", u.ID, time.Now().UnixNano()),
		"users": users,
	})
	if err != nil {
		return fmt.Errorf("marshal room-create: %w", err)
	}
	_, err = a.Request(a.Ctx, subject.RoomCreate(u.Account, a.SiteID), payload, defaultRequestTimeout)
	if err != nil {
		return fmt.Errorf("request room-create: %w", err)
	}
	return nil
}

// memberAdd adds a target account to a random channel room; picks from u.ChannelRooms (not u.Rooms) because
// room-service rejects member-add on DM rooms with "cannot add members to a non-channel room".
func memberAdd(a actionCtx, u *userState, targetAccount string) error {
	if len(u.ChannelRooms) == 0 {
		return nil
	}
	roomID := u.ChannelRooms[a.rand().Intn(len(u.ChannelRooms))]
	payload, err := json.Marshal(map[string]any{"accounts": []string{targetAccount}})
	if err != nil {
		return fmt.Errorf("marshal member-add: %w", err)
	}
	_, err = a.Request(a.Ctx, subject.MemberAdd(u.Account, roomID, a.SiteID), payload, defaultRequestTimeout)
	if err != nil {
		return fmt.Errorf("request member-add: %w", err)
	}
	return nil
}

// threadReply publishes a SendMessageRequest with ThreadParentMessageID set; goes through the same pipeline as send.
func threadReply(a actionCtx, u *userState, parentID, content string) error {
	if len(u.Rooms) == 0 {
		return nil
	}
	roomID := u.Rooms[a.rand().Intn(len(u.Rooms))]
	msgID := idgen.GenerateMessageID()
	reqID := idgen.GenerateRequestID()
	req := model.SendMessageRequest{
		ID: msgID, Content: content, RequestID: reqID,
		ThreadParentMessageID: parentID,
	}
	data, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal thread-reply: %w", err)
	}
	if a.Collector != nil {
		a.Collector.RecordPublish(reqID, msgID, time.Now())
	}
	if err := a.Publish(a.Ctx, subject.MsgSend(u.Account, roomID, a.SiteID), data); err != nil {
		if a.Collector != nil {
			a.Collector.RecordPublishFailed(reqID, msgID)
		}
		return fmt.Errorf("publish thread-reply: %w", err)
	}
	return nil
}
