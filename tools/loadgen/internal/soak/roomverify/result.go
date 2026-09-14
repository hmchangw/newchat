package roomverify

import (
	"fmt"
	"strconv"
	"time"

	"github.com/hmchangw/chat/tools/loadgen/internal/failure"
)

const (
	AttributeTargetAccount  = "target_account"
	AttributeExpectedMember = "expected_member"
	AttributeExpectedName   = "expected_name"
	AttributePreviousName   = "previous_name"
	AttributeExpectedMuted  = "expected_muted"
	AttributeReadBaseline   = "read_baseline_unix_ms"
	AttributeRequester      = "requester_account"
)

type Result string

const (
	ResultMatched  Result = "matched"
	ResultMismatch Result = "mismatch"
	ResultAbsent   Result = "absent"
	ResultUnknown  Result = "unknown"
)

const (
	SourceRPC   = "room_service"
	SourceStore = "mongo"
)

func ClassifyReadCursor(observed *time.Time, baseline time.Time, hasBaseline bool) Result {
	if observed == nil || observed.IsZero() {
		return ResultAbsent
	}
	if !hasBaseline {
		return ResultMatched
	}
	switch {
	case observed.After(baseline):
		return ResultMatched
	case observed.Before(baseline):
		return ResultMismatch
	default:
		return ResultAbsent
	}
}

func ReadBaseline(operation *failure.Operation) (time.Time, bool, error) {
	raw := operation.Attributes[AttributeReadBaseline]
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

func Resolve(rpc, authoritative Result) Result {
	if authoritative != ResultUnknown {
		return authoritative
	}
	if rpc == ResultMatched {
		return ResultMatched
	}
	return ResultUnknown
}

func ClassifyName(found bool, actual, expected, previous string) Result {
	switch {
	case !found:
		return ResultMismatch
	case actual == expected:
		return ResultMatched
	case previous == "" || actual == previous:
		return ResultAbsent
	default:
		return ResultMismatch
	}
}

func FlipPresence(result Result) Result {
	switch result {
	case ResultMatched:
		return ResultAbsent
	case ResultAbsent:
		return ResultMatched
	default:
		return result
	}
}

func ReasonFor(result Result, mismatchReason failure.Reason) failure.Reason {
	switch result {
	case ResultMismatch:
		return mismatchReason
	case ResultAbsent:
		return failure.ReasonRoomStateMissing
	default:
		return failure.ReasonNone
	}
}
