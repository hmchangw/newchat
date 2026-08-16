package main

import (
	"context"
	"fmt"
	"time"
)

// Attribute keys the room and member lanes stamp on an operation so the
// observer can reconstruct the expected final state without holding run state
// in memory. They survive a restart because they live in the WAL record.
const (
	soakFailureAttributeTargetAccount  = "target_account"
	soakFailureAttributeExpectedMember = "expected_member"
	soakFailureAttributeExpectedName   = "expected_name"
	soakFailureAttributePreviousName   = "previous_name"
	soakFailureAttributeExpectedMuted  = "expected_muted"
	soakFailureAttributeRequester      = "requester_account"
)

type soakRoomStateResult string

const (
	soakRoomStateMatched  soakRoomStateResult = "matched"
	soakRoomStateMismatch soakRoomStateResult = "mismatch"
	soakRoomStateAbsent   soakRoomStateResult = "absent"
	soakRoomStateUnknown  soakRoomStateResult = "unknown"
)

const (
	soakRoomStateSourceRPC   = "room_service"
	soakRoomStateSourceStore = "mongo"
)

// soakRoomStateVerifier is the room_state observer body. It asks room-service
// first because that is the path a client uses, then the authoritative store.
//
// The store settles disagreements: room-service returning nothing can mean the
// read path is degraded, while a primary read that finds the state proves the
// write landed. Only agreement, or an unreachable store, may produce an
// absence claim.
type soakRoomStateVerifier struct {
	reader  *soakRoomReader
	store   soakRoomStateStore
	metrics *Metrics
	health  *failureObserverHealth
	now     func() time.Time
}

func newSoakRoomStateVerifier(
	reader *soakRoomReader,
	store soakRoomStateStore,
	metrics *Metrics,
	health *failureObserverHealth,
	now func() time.Time,
) *soakRoomStateVerifier {
	if now == nil {
		now = time.Now
	}
	return &soakRoomStateVerifier{
		reader: reader, store: store, metrics: metrics, health: health, now: now,
	}
}

func (v *soakRoomStateVerifier) Verify(
	ctx context.Context,
	operation *failureOperation,
) (soakRoomStateResult, failureReason, error) {
	if v == nil || v.store == nil {
		return soakRoomStateUnknown, failureReasonNone,
			fmt.Errorf("soak room state verifier is not configured")
	}
	if operation == nil {
		return soakRoomStateUnknown, failureReasonNone,
			fmt.Errorf("soak room state verification requires an operation")
	}
	roomID := operation.Targets["roomId"]
	if roomID == "" {
		return soakRoomStateUnknown, failureReasonNone,
			fmt.Errorf("failure operation %q has no room target", operation.ID)
	}

	switch operation.OperationType {
	case failureOperationMemberAdd, failureOperationMemberRemove:
		return v.verifyMember(ctx, operation, roomID)
	case failureOperationRoomRename:
		return v.verifyRename(ctx, operation, roomID)
	case failureOperationMuteToggle:
		return v.verifyMute(ctx, operation, roomID)
	case failureOperationRoomCreate:
		return v.verifyCreated(ctx, roomID)
	default:
		return soakRoomStateUnknown, failureReasonNone,
			fmt.Errorf("failure operation %q has no room state rule for type %q",
				operation.ID, operation.OperationType)
	}
}

func (v *soakRoomStateVerifier) verifyMember(
	ctx context.Context,
	operation *failureOperation,
	roomID string,
) (soakRoomStateResult, failureReason, error) {
	account := operation.Attributes[soakFailureAttributeTargetAccount]
	if account == "" {
		return soakRoomStateUnknown, failureReasonNone,
			fmt.Errorf("failure operation %q has no target account", operation.ID)
	}
	expectMember := operation.Attributes[soakFailureAttributeExpectedMember] == "true"

	rpc := soakRoomStateUnknown
	if requester, ok := v.readerAccount(roomID, operation); ok {
		if response, err := v.reader.RoomState(ctx, roomID, requester); err == nil {
			rpc = soakRoomStateAbsent
			for i := range response.Members {
				if response.Members[i].Member.Account == account {
					rpc = soakRoomStateMatched
					break
				}
			}
			if !expectMember {
				rpc = flipSoakRoomStatePresence(rpc)
			}
		}
	}
	v.countSource(soakRoomStateSourceRPC, rpc)

	authoritative := soakRoomStateUnknown
	member, err := v.store.IsRoomMember(ctx, roomID, account)
	switch {
	case err != nil:
		v.setHealth(false, "member_read")
	case member == expectMember:
		authoritative = soakRoomStateMatched
		v.setHealth(true, "member_read")
	default:
		authoritative = soakRoomStateAbsent
		v.setHealth(true, "member_read")
	}
	v.countSource(soakRoomStateSourceStore, authoritative)

	result := resolveSoakRoomState(rpc, authoritative)
	return result, soakRoomStateReasonFor(result, failureReasonMemberStateMismatch), nil
}

func (v *soakRoomStateVerifier) verifyRename(
	ctx context.Context,
	operation *failureOperation,
	roomID string,
) (soakRoomStateResult, failureReason, error) {
	expected := operation.Attributes[soakFailureAttributeExpectedName]
	previous := operation.Attributes[soakFailureAttributePreviousName]
	if expected == "" {
		return soakRoomStateUnknown, failureReasonNone,
			fmt.Errorf("failure operation %q has no expected room name", operation.ID)
	}

	rpc := soakRoomStateUnknown
	if info, err := v.reader.RoomInfoFor(ctx, roomID); err == nil {
		rpc = classifySoakRoomName(info.Found, info.Name, expected, previous)
	}
	v.countSource(soakRoomStateSourceRPC, rpc)

	authoritative := soakRoomStateUnknown
	name, found, err := v.store.RoomName(ctx, roomID)
	if err != nil {
		v.setHealth(false, "room_read")
	} else {
		authoritative = classifySoakRoomName(found, name, expected, previous)
		v.setHealth(true, "room_read")
	}
	v.countSource(soakRoomStateSourceStore, authoritative)

	result := resolveSoakRoomState(rpc, authoritative)
	return result, soakRoomStateReasonFor(result, failureReasonRoomNameMismatch), nil
}

func (v *soakRoomStateVerifier) verifyMute(
	ctx context.Context,
	operation *failureOperation,
	roomID string,
) (soakRoomStateResult, failureReason, error) {
	account := operation.Attributes[soakFailureAttributeTargetAccount]
	if account == "" {
		return soakRoomStateUnknown, failureReasonNone,
			fmt.Errorf("failure operation %q has no target account", operation.ID)
	}
	expectMuted := operation.Attributes[soakFailureAttributeExpectedMuted] == "true"

	rpc := soakRoomStateUnknown
	if response, err := v.reader.SubscriptionsFor(ctx, account); err == nil {
		rpc = soakRoomStateMismatch
		for i := range response.Subscriptions {
			if response.Subscriptions[i].RoomID != roomID {
				continue
			}
			rpc = soakRoomStateAbsent
			if response.Subscriptions[i].Muted == expectMuted {
				rpc = soakRoomStateMatched
			}
			break
		}
	}
	v.countSource(soakRoomStateSourceRPC, rpc)

	authoritative := soakRoomStateUnknown
	muted, found, err := v.store.SubscriptionMuted(ctx, roomID, account)
	switch {
	case err != nil:
		v.setHealth(false, "mute_read")
	case !found:
		// The subscription must exist for a room the pool owns; its absence is
		// an impossible state rather than an unapplied toggle.
		authoritative = soakRoomStateMismatch
		v.setHealth(true, "mute_read")
	case muted == expectMuted:
		authoritative = soakRoomStateMatched
		v.setHealth(true, "mute_read")
	default:
		authoritative = soakRoomStateAbsent
		v.setHealth(true, "mute_read")
	}
	v.countSource(soakRoomStateSourceStore, authoritative)

	result := resolveSoakRoomState(rpc, authoritative)
	return result, soakRoomStateReasonFor(result, failureReasonMuteStateMismatch), nil
}

func (v *soakRoomStateVerifier) verifyCreated(
	ctx context.Context,
	roomID string,
) (soakRoomStateResult, failureReason, error) {
	rpc := soakRoomStateUnknown
	if info, err := v.reader.RoomInfoFor(ctx, roomID); err == nil {
		rpc = soakRoomStateAbsent
		if info.Found {
			rpc = soakRoomStateMatched
		}
	}
	v.countSource(soakRoomStateSourceRPC, rpc)

	authoritative := soakRoomStateUnknown
	_, found, err := v.store.RoomName(ctx, roomID)
	switch {
	case err != nil:
		v.setHealth(false, "room_read")
	case found:
		authoritative = soakRoomStateMatched
		v.setHealth(true, "room_read")
	default:
		authoritative = soakRoomStateAbsent
		v.setHealth(true, "room_read")
	}
	v.countSource(soakRoomStateSourceStore, authoritative)

	result := resolveSoakRoomState(rpc, authoritative)
	return result, soakRoomStateReasonFor(result, failureReasonRoomStateMissing), nil
}

func (v *soakRoomStateVerifier) readerAccount(
	roomID string,
	operation *failureOperation,
) (string, bool) {
	if v.reader == nil {
		return "", false
	}
	if account, ok := v.reader.Account(roomID); ok {
		return account, true
	}
	requester := operation.Attributes[soakFailureAttributeRequester]
	return requester, requester != ""
}

func (v *soakRoomStateVerifier) countSource(source string, result soakRoomStateResult) {
	if v.metrics == nil {
		return
	}
	v.metrics.SoakRoomStateSources.WithLabelValues(source, string(result)).Inc()
}

func (v *soakRoomStateVerifier) setHealth(up bool, reason string) {
	if v.health == nil {
		return
	}
	v.health.Set(up, v.now(), reason)
	if v.metrics == nil {
		return
	}
	value := 0.0
	if up {
		value = 1
	}
	v.metrics.FailureObserverUp.WithLabelValues(string(failureObserverRoomState)).Set(value)
}

// resolveSoakRoomState merges the two sources. The authoritative store wins
// whenever it can answer: room-service reporting nothing may only mean its read
// path is degraded, so an absence claim needs the primary read behind it.
func resolveSoakRoomState(rpc, authoritative soakRoomStateResult) soakRoomStateResult {
	if authoritative != soakRoomStateUnknown {
		return authoritative
	}
	if rpc == soakRoomStateMatched {
		return soakRoomStateMatched
	}
	return soakRoomStateUnknown
}

// classifySoakRoomName separates "our rename did not land" from an impossible
// name. The pool serializes renames per room, so the only name that may legally
// appear other than the expected one is the previous one.
func classifySoakRoomName(found bool, actual, expected, previous string) soakRoomStateResult {
	switch {
	case !found:
		return soakRoomStateMismatch
	case actual == expected:
		return soakRoomStateMatched
	case previous == "" || actual == previous:
		return soakRoomStateAbsent
	default:
		return soakRoomStateMismatch
	}
}

func flipSoakRoomStatePresence(result soakRoomStateResult) soakRoomStateResult {
	switch result {
	case soakRoomStateMatched:
		return soakRoomStateAbsent
	case soakRoomStateAbsent:
		return soakRoomStateMatched
	default:
		return result
	}
}

func soakRoomStateReasonFor(
	result soakRoomStateResult,
	mismatchReason failureReason,
) failureReason {
	switch result {
	case soakRoomStateMismatch:
		return mismatchReason
	case soakRoomStateAbsent:
		return failureReasonRoomStateMissing
	default:
		return failureReasonNone
	}
}
