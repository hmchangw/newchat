package main

import (
	"context"
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

type soakRoomMutationOutcome struct {
	Action      soakRPCAction
	Latency     time.Duration
	Retries     int
	ErrorClass  soakErrorClass
	ErrorReason soakErrorReason
	Accepted    bool
	RoomID      string
	Muted       bool
}

// soakRoomMutator issues the room and member mutations the soak lanes track in
// the evidence ledger.
//
// Every call uses soakRetryNever. These requests are not idempotent — a replayed
// remove drops a member the first attempt already removed, and a replayed mute
// toggle restores the original state — so an ambiguous transport error is left
// for the ledger to reconcile rather than resolved by sending again.
type soakRoomMutator struct {
	siteID  string
	rpc     *soakRPCClient
	timeout time.Duration
	now     func() time.Time
}

func newSoakRoomMutator(
	siteID string,
	rpc *soakRPCClient,
	timeout time.Duration,
	now func() time.Time,
) *soakRoomMutator {
	if timeout <= 0 {
		timeout = soakRequestTimeout
	}
	if now == nil {
		now = time.Now
	}
	return &soakRoomMutator{siteID: siteID, rpc: rpc, timeout: timeout, now: now}
}

func (m *soakRoomMutator) AddMember(
	ctx context.Context,
	requester, roomID, account string,
) (soakRoomMutationOutcome, error) {
	if requester == "" || roomID == "" || account == "" {
		return soakRoomMutationOutcome{Action: soakRPCMemberAdd},
			fmt.Errorf("add member requires a requester, room, and account")
	}
	var reply soakStatusReply
	return m.call(ctx, soakRPCRequest{
		Action:  soakRPCMemberAdd,
		Subject: subject.MemberAdd(requester, roomID, m.siteID),
		// The requester is who the RPC is addressed as; the member being added
		// is in the body, and naming it here would point at the wrong account.
		Account: requester, RoomID: roomID,
		Body:    soakAddMembersRequest{RoomID: roomID, Users: []string{account}},
		Timeout: m.timeout, RetryMode: soakRetryNever,
	}, &reply, roomID, func(outcome *soakRoomMutationOutcome) {
		outcome.Accepted = reply.Status == soakRoomStatusAccepted
	})
}

func (m *soakRoomMutator) RemoveMember(
	ctx context.Context,
	requester, roomID, account string,
) (soakRoomMutationOutcome, error) {
	if requester == "" || roomID == "" || account == "" {
		return soakRoomMutationOutcome{Action: soakRPCMemberRemove},
			fmt.Errorf("remove member requires a requester, room, and account")
	}
	var reply soakStatusReply
	return m.call(ctx, soakRPCRequest{
		Action:  soakRPCMemberRemove,
		Subject: subject.MemberRemove(requester, roomID, m.siteID),
		Body:    soakRemoveMemberRequest{RoomID: roomID, Account: account},
		Timeout: m.timeout, RetryMode: soakRetryNever,
	}, &reply, roomID, func(outcome *soakRoomMutationOutcome) {
		outcome.Accepted = reply.Status == soakRoomStatusAccepted
	})
}

func (m *soakRoomMutator) Rename(
	ctx context.Context,
	requester, roomID, newName string,
) (soakRoomMutationOutcome, error) {
	if requester == "" || roomID == "" || newName == "" {
		return soakRoomMutationOutcome{Action: soakRPCRoomRename},
			fmt.Errorf("rename requires a requester, room, and name")
	}
	var reply soakStatusReply
	return m.call(ctx, soakRPCRequest{
		Action:  soakRPCRoomRename,
		Subject: subject.RoomRename(requester, roomID, m.siteID),
		Account: requester, RoomID: roomID,
		Body:    soakRoomRenameRequest{NewName: newName},
		Timeout: m.timeout, RetryMode: soakRetryNever,
	}, &reply, roomID, func(outcome *soakRoomMutationOutcome) {
		outcome.Accepted = reply.Status == soakRoomStatusAccepted
	})
}

func (m *soakRoomMutator) ToggleMute(
	ctx context.Context,
	account, roomID string,
) (soakRoomMutationOutcome, error) {
	if account == "" || roomID == "" {
		return soakRoomMutationOutcome{Action: soakRPCMuteToggle},
			fmt.Errorf("mute toggle requires an account and room")
	}
	var reply soakMuteToggleReply
	return m.call(ctx, soakRPCRequest{
		Action:  soakRPCMuteToggle,
		Subject: subject.MuteToggle(account, roomID, m.siteID),
		Account: account, RoomID: roomID,
		Timeout: m.timeout, RetryMode: soakRetryNever,
	}, &reply, roomID, func(outcome *soakRoomMutationOutcome) {
		// Mute is applied inline, so a recognised reply is proof of the stored
		// state. An unrecognised one is not: an empty body decodes cleanly and
		// would otherwise be read as a confirmed toggle.
		outcome.Accepted = reply.Status == soakRoomStatusOK
		outcome.Muted = reply.Muted
	})
}

// MarkRead is the client's "I opened this room" signal. room-service applies it
// inline, so a reply proves the write was attempted against MongoDB.
func (m *soakRoomMutator) MarkRead(
	ctx context.Context,
	account, roomID string,
) (soakRoomMutationOutcome, error) {
	if account == "" || roomID == "" {
		return soakRoomMutationOutcome{Action: soakRPCMessageRead},
			fmt.Errorf("mark read requires an account and room")
	}
	var reply soakStatusReply
	return m.call(ctx, soakRPCRequest{
		Action:  soakRPCMessageRead,
		Subject: subject.MessageRead(account, roomID, m.siteID),
		Account: account, RoomID: roomID,
		Timeout: m.timeout, RetryMode: soakRetryNever,
	}, &reply, roomID, func(outcome *soakRoomMutationOutcome) {
		outcome.Accepted = reply.Status == soakRoomStatusAccepted
	})
}

func (m *soakRoomMutator) CreateRoom(
	ctx context.Context,
	requester, name string,
	users []string,
) (soakRoomMutationOutcome, error) {
	if requester == "" || name == "" || len(users) == 0 {
		return soakRoomMutationOutcome{Action: soakRPCRoomCreate},
			fmt.Errorf("create room requires a requester, name, and at least one member")
	}
	var reply soakCreateRoomReply
	return m.call(ctx, soakRPCRequest{
		Action:  soakRPCRoomCreate,
		Subject: subject.RoomCreate(requester, m.siteID),
		Account: requester,
		Body:    soakCreateRoomRequest{Name: name, Users: users},
		Timeout: m.timeout, RetryMode: soakRetryNever,
	}, &reply, "", func(outcome *soakRoomMutationOutcome) {
		outcome.RoomID = reply.RoomID
		outcome.Accepted = reply.Status == soakRoomStatusAccepted ||
			reply.Status == model.CreateRoomStatusExists
	})
}

const (
	soakRoomStatusAccepted = "accepted"
	// Mute answers "ok" rather than "accepted" because it is applied inline.
	soakRoomStatusOK = "ok"
)

//nolint:gocritic // hugeParam: the request carries the failure identity; the copy is nothing beside the round trip.
func (m *soakRoomMutator) call(
	ctx context.Context,
	request soakRPCRequest,
	reply any,
	roomID string,
	apply func(*soakRoomMutationOutcome),
) (soakRoomMutationOutcome, error) {
	outcome := soakRoomMutationOutcome{Action: request.Action, RoomID: roomID}
	if m.rpc == nil {
		return outcome, fmt.Errorf("soak room mutator requires an RPC client")
	}
	startedAt := m.now()
	result, err := m.rpc.Call(ctx, request, reply)
	outcome.Latency = m.now().Sub(startedAt)
	outcome.Retries = result.Retries
	outcome.ErrorClass = result.ErrorClass
	outcome.ErrorReason = result.ErrorReason
	if err != nil {
		return outcome, fmt.Errorf("room mutation lane: %w", err)
	}
	apply(&outcome)
	return outcome, nil
}
