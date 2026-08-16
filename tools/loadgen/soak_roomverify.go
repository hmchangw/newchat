package main

import (
	"context"
	"fmt"
	"strconv"
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
	soakFailureAttributeReadBaseline   = "read_baseline_unix_ms"
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
// write landed. Only the store may produce an absence claim, and an unreachable
// store produces none at all — the result stays unknown and the operation is
// re-probed, because "we could not look" is not evidence of loss. The one thing
// the RPC settles alone is a positive: if room-service already shows the effect,
// the write landed.
type soakRoomStateVerifier struct {
	reader  *soakRoomReader
	store   soakRoomStateStore
	siteID  string
	metrics *Metrics
	health  *failureObserverHealth
	now     func() time.Time
}

func newSoakRoomStateVerifier(
	reader *soakRoomReader,
	store soakRoomStateStore,
	siteID string,
	metrics *Metrics,
	health *failureObserverHealth,
	now func() time.Time,
) *soakRoomStateVerifier {
	if now == nil {
		now = time.Now
	}
	return &soakRoomStateVerifier{
		reader: reader, store: store, siteID: siteID,
		metrics: metrics, health: health, now: now,
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
	// A created room is identified by the name journaled before the request,
	// because its ID does not exist until the server answers.
	if operation.OperationType == failureOperationRoomCreate {
		return v.verifyCreated(ctx, operation.Targets["roomName"])
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
	case failureOperationMessageRead:
		return v.verifyRead(ctx, operation, roomID)
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

	// The verifier accepts a nil reader, so every RPC source is guarded: without
	// it a store-only verifier panics instead of falling back to the store.
	rpc := soakRoomStateUnknown
	if v.reader != nil {
		if info, err := v.reader.RoomInfoFor(ctx, roomID); err == nil {
			rpc = classifySoakRoomName(info.Found, info.Name, expected, previous)
		}
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
	if v.reader != nil {
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

// verifyCreated answers by name. The store is the only source: the rooms-info
// RPC needs an ID the run does not have until the room is found, so asking it
// first would make the probe depend on the very answer it is looking for.
func (v *soakRoomStateVerifier) verifyCreated(
	ctx context.Context,
	roomName string,
) (soakRoomStateResult, failureReason, error) {
	if roomName == "" {
		return soakRoomStateUnknown, failureReasonNone,
			fmt.Errorf("room create operation has no room name target")
	}
	v.countSource(soakRoomStateSourceRPC, soakRoomStateUnknown)

	authoritative := soakRoomStateUnknown
	roomID, found, err := v.store.RoomIDByName(ctx, v.siteID, roomName)
	switch {
	case err != nil:
		v.setHealth(false, "room_read")
	case found && roomID != "":
		authoritative = soakRoomStateMatched
		v.setHealth(true, "room_read")
	default:
		authoritative = soakRoomStateAbsent
		v.setHealth(true, "room_read")
	}
	v.countSource(soakRoomStateSourceStore, authoritative)

	result := resolveSoakRoomState(soakRoomStateUnknown, authoritative)
	return result, soakRoomStateReasonFor(result, failureReasonRoomStateMissing), nil
}

// verifyRead compares the read cursor against the baseline the pool captured
// before the request. mark-read is monotonic, so "did not move" means the write
// never landed and "moved backwards" is a state that cannot legally occur.
func (v *soakRoomStateVerifier) verifyRead(
	ctx context.Context,
	operation *failureOperation,
	roomID string,
) (soakRoomStateResult, failureReason, error) {
	account := operation.Attributes[soakFailureAttributeTargetAccount]
	if account == "" {
		return soakRoomStateUnknown, failureReasonNone,
			fmt.Errorf("failure operation %q has no target account", operation.ID)
	}
	baseline, hasBaseline, parseErr := soakReadBaseline(operation)
	if parseErr != nil {
		return soakRoomStateUnknown, failureReasonNone, parseErr
	}

	rpc := soakRoomStateUnknown
	if v.reader != nil {
		if response, err := v.reader.SubscriptionsFor(ctx, account); err == nil {
			rpc = soakRoomStateMismatch
			for i := range response.Subscriptions {
				if response.Subscriptions[i].RoomID != roomID {
					continue
				}
				rpc = classifySoakReadCursor(
					response.Subscriptions[i].LastSeenAt, baseline, hasBaseline,
				)
				break
			}
		}
	}
	v.countSource(soakRoomStateSourceRPC, rpc)

	authoritative := soakRoomStateUnknown
	lastSeenAt, found, err := v.store.SubscriptionLastSeen(ctx, roomID, account)
	switch {
	case err != nil:
		v.setHealth(false, "read_cursor")
	case !found:
		authoritative = soakRoomStateMismatch
		v.setHealth(true, "read_cursor")
	default:
		observed := &lastSeenAt
		if lastSeenAt.IsZero() {
			observed = nil
		}
		authoritative = classifySoakReadCursor(observed, baseline, hasBaseline)
		v.setHealth(true, "read_cursor")
	}
	v.countSource(soakRoomStateSourceStore, authoritative)

	result := resolveSoakRoomState(rpc, authoritative)
	return result, soakRoomStateReasonFor(result, failureReasonReadStateRegressed), nil
}

// classifySoakReadCursor turns an observed cursor into a verdict. Without a
// trusted baseline the only defensible expectation is that some timestamp
// exists, so a stale value cannot be called a loss.
func classifySoakReadCursor(
	observed *time.Time,
	baseline time.Time,
	hasBaseline bool,
) soakRoomStateResult {
	if observed == nil || observed.IsZero() {
		return soakRoomStateAbsent
	}
	if !hasBaseline {
		return soakRoomStateMatched
	}
	switch {
	case observed.After(baseline):
		return soakRoomStateMatched
	case observed.Before(baseline):
		return soakRoomStateMismatch
	default:
		return soakRoomStateAbsent
	}
}

func soakReadBaseline(operation *failureOperation) (time.Time, bool, error) {
	raw := operation.Attributes[soakFailureAttributeReadBaseline]
	if raw == "" {
		return time.Time{}, false, nil
	}
	millis, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return time.Time{}, false, fmt.Errorf(
			"failure operation %q has an unreadable read baseline: %w", operation.ID, err,
		)
	}
	return time.UnixMilli(millis).UTC(), true, nil
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
