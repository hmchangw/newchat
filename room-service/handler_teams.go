package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/displayfmt"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// teamsDeepLinkBase is the Teams 1:1/group call deep-link prefix. The callable
// users are appended as a comma-joined `users` query param.
const teamsDeepLinkBase = "https://teams.microsoft.com/l/call/0/0"

// teamsEmail derives a member's email from their account using the configured
// corporate domain — the only identity available at the NATS layer (the OIDC
// email claim exists only at auth-service token exchange). Mirrors the dev
// auth-service derivation (account + "@dev.local").
func teamsEmail(account, domain string) string {
	return account + "@" + domain
}

// buildTeamsCallDeepLink builds a Teams call deep link for the given attendee
// emails. Order is preserved; the caller is responsible for excluding self.
func buildTeamsCallDeepLink(emails []string) string {
	v := url.Values{}
	v.Set("users", strings.Join(emails, ","))
	return teamsDeepLinkBase + "?" + v.Encode()
}

// teamsRoomCall builds a Teams deep link for a call to every other member of the
// room (self excluded). No Microsoft Graph call — pure string building from the
// member list. Enforces roomMembersCallLimit.
func (h *Handler) teamsRoomCall(c *natsrouter.Context, _ model.TeamsRoomCallRequest) (*model.TeamsCallReply, error) { //nolint:gocritic // hugeParam: req passed by value to satisfy natsrouter.Register
	var ctx context.Context = c
	requesterAccount := c.Param("account")
	roomID := c.Param("roomID")

	if requesterAccount == "" {
		return nil, errTeamsRequesterMissing
	}
	if roomID == "" {
		return nil, errTeamsRoomIDRequired
	}

	if _, err := h.requireMembershipAndGetRoom(ctx, requesterAccount, roomID); err != nil {
		return nil, err
	}

	members, err := h.store.ListRoomMembers(ctx, roomID, nil, nil, false)
	if err != nil {
		return nil, fmt.Errorf("list room members: %w", err)
	}

	emails := membersToCallEmails(members, requesterAccount, h.teamsEmailDomain)
	if len(emails) == 0 {
		return nil, errTeamsNoCallableMembers
	}
	if len(emails) > h.roomMembersCallLimit {
		return nil, errTeamsCallTooManyMembers
	}

	return &model.TeamsCallReply{JoinURL: buildTeamsCallDeepLink(emails)}, nil
}

// teamsUserCall builds a Teams 1:1 call deep link for the target account. No
// Graph call. The target email is account@domain (same derivation as everywhere
// else in this integration).
func (h *Handler) teamsUserCall(c *natsrouter.Context, req model.TeamsUserCallRequest) (*model.TeamsCallReply, error) { //nolint:gocritic // hugeParam: req passed by value to satisfy natsrouter.Register
	requesterAccount := c.Param("account")
	if requesterAccount == "" {
		return nil, errTeamsRequesterMissing
	}
	if req.AccountName == "" {
		return nil, errTeamsAccountRequired
	}

	email := teamsEmail(req.AccountName, h.teamsEmailDomain)
	return &model.TeamsCallReply{JoinURL: buildTeamsCallDeepLink([]string{email})}, nil
}

// teamsMeeting creates (or returns the existing) Teams onlineMeeting for the
// room, idempotent per room: Graph createOrGet keyed on a stable per-room
// externalId yields one meeting even under concurrent/retried calls, and a
// teams_meetings record with a UNIQUE (roomId, siteId) index gates the
// teams_meet_started system message so it publishes exactly once. Enforces
// roomMembersLimit.
func (h *Handler) teamsMeeting(c *natsrouter.Context, _ model.TeamsMeetingRequest) (*model.TeamsMeetingReply, error) { //nolint:gocritic // hugeParam: req passed by value to satisfy natsrouter.Register
	var ctx context.Context = c
	requestID := natsutil.RequestIDFromContext(c)
	requesterAccount := c.Param("account")
	roomID := c.Param("roomID")

	if requesterAccount == "" {
		return nil, errTeamsRequesterMissing
	}
	if roomID == "" {
		return nil, errTeamsRoomIDRequired
	}
	if h.graphClient == nil || h.teamsMeetingStore == nil {
		return nil, errTeamsNotConfigured
	}

	room, err := h.requireMembershipAndGetRoom(ctx, requesterAccount, roomID)
	if err != nil {
		return nil, err
	}

	// Fast-path: an existing record short-circuits Graph + insert + publish.
	if rec, found, err := h.teamsMeetingStore.GetTeamsMeeting(ctx, roomID, h.siteID); err != nil {
		return nil, fmt.Errorf("read teams meeting record: %w", err)
	} else if found && rec != nil && rec.JoinURL != "" {
		return &model.TeamsMeetingReply{ID: rec.MeetingID, JoinURL: rec.JoinURL}, nil
	}

	members, err := h.store.ListRoomMembers(ctx, roomID, nil, nil, false)
	if err != nil {
		return nil, fmt.Errorf("list room members: %w", err)
	}
	if countIndividualMembers(members) > h.roomMembersLimit {
		return nil, errTeamsMeetingTooManyMembers
	}

	attendeeEmails := membersToAttendeeEmails(members, h.teamsEmailDomain)
	organizerEmail := teamsEmail(requesterAccount, h.teamsEmailDomain)

	// Graph createOrGet: concurrent callers get the same meeting back.
	meeting, err := h.graphClient.CreateOnlineMeeting(ctx, msgraph.CreateOnlineMeetingRequest{
		ExternalID:     teamsMeetingExternalID(h.siteID, roomID),
		Subject:        meetingSubject(room),
		OrganizerEmail: organizerEmail,
		AttendeeEmails: attendeeEmails,
	})
	if err != nil {
		return nil, fmt.Errorf("create online meeting: %w", err)
	}

	// Insert the idempotency record; the unique (roomId, siteId) index gates the
	// publish — the loser of an insert race reads back the winner and skips it.
	record := model.TeamsMeetingRecord{
		RoomID:    roomID,
		SiteID:    h.siteID,
		MeetingID: meeting.ID,
		JoinURL:   meeting.JoinURL,
		CreatedAt: time.Now().UTC().UnixMilli(),
	}
	if err := h.teamsMeetingStore.InsertTeamsMeeting(ctx, record); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			// A concurrent winner already wrote the record — read it back and
			// return its meeting. Do NOT publish: the winner publishes.
			rec, found, readErr := h.teamsMeetingStore.GetTeamsMeeting(ctx, roomID, h.siteID)
			if readErr != nil {
				return nil, fmt.Errorf("read teams meeting record after duplicate key: %w", readErr)
			}
			if found && rec != nil && rec.JoinURL != "" {
				return &model.TeamsMeetingReply{ID: rec.MeetingID, JoinURL: rec.JoinURL}, nil
			}
			// Extremely unlikely: dup-key but no readable record. Fall back to
			// the Graph meeting (createOrGet already guaranteed it is the same one).
			return &model.TeamsMeetingReply{ID: meeting.ID, JoinURL: meeting.JoinURL}, nil
		}
		return nil, fmt.Errorf("insert teams meeting record: %w", err)
	}

	// This call created the record — the unique winner publishes exactly once.
	if err := h.publishTeamsMeetStarted(ctx, requestID, roomID, requesterAccount, meeting); err != nil {
		return nil, err
	}

	return &model.TeamsMeetingReply{ID: meeting.ID, JoinURL: meeting.JoinURL}, nil
}

// teamsMeetingExternalID builds the stable per-room idempotency key passed to
// Graph createOrGet (and matching the teams_meetings unique key dimensions).
// Scoped by siteID so the same roomID on different sites maps to distinct
// meetings.
func teamsMeetingExternalID(siteID, roomID string) string {
	return siteID + ":" + roomID
}

// publishTeamsMeetStarted writes the teams_meet_started system message through
// the canonical message path — the same flow room_restricted uses — so it is
// persisted by message-worker and fanned out to room members. The marker is the
// idempotency source for subsequent meetings calls.
func (h *Handler) publishTeamsMeetStarted(
	ctx context.Context,
	requestID, roomID, byAccount string,
	meeting *msgraph.OnlineMeeting,
) error {
	sysData, err := json.Marshal(model.TeamsMeetStartedSysData{
		MeetingID: meeting.ID,
		JoinURL:   meeting.JoinURL,
	})
	if err != nil {
		return fmt.Errorf("marshal teams_meet_started sys data: %w", err)
	}

	// Prefer the requester's display name; fall back to the account if the user
	// lookup fails (the record is already committed, so never fail the publish).
	byDisplay := byAccount
	if u, err := h.store.GetUser(ctx, byAccount); err == nil && u != nil {
		byDisplay = displayfmt.CombineWithFallback(u.EngName, u.ChineseName, u.Account)
	}

	now := time.Now().UTC()
	sysMsg := model.Message{
		ID:          idgen.MessageIDFromRequestID(requestID, "teams_meet_started"),
		RoomID:      roomID,
		UserAccount: byAccount,
		Type:        model.MessageTypeTeamsMeetStarted,
		Content:     fmt.Sprintf("%q started a Teams meeting", byDisplay),
		SysMsgData:  sysData,
		CreatedAt:   now,
	}
	msgEvt := model.MessageEvent{
		Event:     model.EventCreated,
		Message:   sysMsg,
		SiteID:    h.siteID,
		Timestamp: now.UnixMilli(),
	}
	msgEvtData, err := json.Marshal(msgEvt)
	if err != nil {
		return fmt.Errorf("marshal teams_meet_started message event: %w", err)
	}
	if err := h.publishToStream(ctx, subject.MsgCanonicalCreated(h.siteID), msgEvtData, natsutil.CanonicalDedupID(&msgEvt)); err != nil {
		return fmt.Errorf("publish teams_meet_started sys message: %w", err)
	}
	return nil
}

// membersToCallEmails returns deep-link emails for every individual member
// except self, preserving member order.
func membersToCallEmails(members []model.RoomMember, self, domain string) []string {
	out := make([]string, 0, len(members))
	for i := range members {
		entry := members[i].Member
		if entry.Type != model.RoomMemberIndividual || entry.Account == "" {
			continue
		}
		if entry.Account == self {
			continue
		}
		out = append(out, teamsEmail(entry.Account, domain))
	}
	return out
}

// membersToAttendeeEmails returns attendee emails for every individual member
// (organizer included is harmless; Graph dedups the organizer).
func membersToAttendeeEmails(members []model.RoomMember, domain string) []string {
	out := make([]string, 0, len(members))
	for i := range members {
		entry := members[i].Member
		if entry.Type != model.RoomMemberIndividual || entry.Account == "" {
			continue
		}
		out = append(out, teamsEmail(entry.Account, domain))
	}
	return out
}

// countIndividualMembers counts individual (human) members for the limit gate.
func countIndividualMembers(members []model.RoomMember) int {
	n := 0
	for i := range members {
		if members[i].Member.Type == model.RoomMemberIndividual {
			n++
		}
	}
	return n
}

// meetingSubject builds a human-friendly meeting title from the room.
func meetingSubject(room *model.Room) string {
	name := displayfmt.CombineWithFallback("", "", room.Name)
	if name == "" {
		return "Chat meeting"
	}
	return name
}
