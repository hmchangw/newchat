package main

import "errors"

// FailoverStatus is a site's operator-driven failover lifecycle state.
type FailoverStatus string

const (
	StatusHealthy     FailoverStatus = "healthy"
	StatusFailedOver  FailoverStatus = "failed_over"
	StatusFailingBack FailoverStatus = "failing_back"
)

// ServingTarget says where a site's users are served from right now. It is the
// entire contract SP3's routing override consumes.
type ServingTarget string

const (
	ServingHome   ServingTarget = "home"
	ServingBackup ServingTarget = "backup"
)

// FailoverAction is an operator-requested transition.
type FailoverAction string

const (
	ActionFailover FailoverAction = "failover" // healthy      -> failed_over
	ActionFailback FailoverAction = "failback" // failed_over  -> failing_back
	ActionComplete FailoverAction = "complete" // failing_back -> healthy
	ActionResume   FailoverAction = "resume"   // failed_over  -> healthy (false alarm)
)

// errIllegalTransition is returned when an action is not valid from the current
// status. errFailoverVersionConflict is returned by the store when an optimistic
// CAS loses a race. Both are matched with errors.Is at the HTTP boundary.
var (
	errIllegalTransition       = errors.New("illegal failover transition")
	errFailoverVersionConflict = errors.New("failover state version conflict")
)

// FailoverState is the sole authoritative record of a site's serving target.
// One document per site, _id = siteID. version is the optimistic-concurrency
// guard for the sole-writer CAS.
type FailoverState struct {
	SiteID    string         `json:"siteId"    bson:"_id"`
	Status    FailoverStatus `json:"status"    bson:"status"`
	Reason    string         `json:"reason"    bson:"reason"`
	Operator  string         `json:"operator"  bson:"operator"`
	Since     int64          `json:"since"     bson:"since"`
	Version   int64          `json:"version"   bson:"version"`
	Timestamp int64          `json:"timestamp" bson:"timestamp"`
}

// ServingTarget derives where this site is served from. Any status other than
// failed_over/failing_back (including an unknown value) is home — the fail-safe.
func (s *FailoverState) ServingTarget() ServingTarget {
	switch s.Status {
	case StatusFailedOver, StatusFailingBack:
		return ServingBackup
	default:
		return ServingHome
	}
}

// isKnownAction reports whether a is one of the four defined actions.
func isKnownAction(a FailoverAction) bool {
	switch a {
	case ActionFailover, ActionFailback, ActionComplete, ActionResume:
		return true
	default:
		return false
	}
}

// nextStatus returns the status resulting from applying action to cur, or
// errIllegalTransition if the action is not valid from cur.
func nextStatus(cur FailoverStatus, action FailoverAction) (FailoverStatus, error) {
	switch {
	case cur == StatusHealthy && action == ActionFailover:
		return StatusFailedOver, nil
	case cur == StatusFailedOver && action == ActionFailback:
		return StatusFailingBack, nil
	case cur == StatusFailedOver && action == ActionResume:
		return StatusHealthy, nil
	case cur == StatusFailingBack && action == ActionComplete:
		return StatusHealthy, nil
	default:
		return "", errIllegalTransition
	}
}

// applyAction computes the next FailoverState for an operator action. It bumps
// version by 1 and stamps operator/reason/since/timestamp. It does not persist —
// the caller CAS-writes the result via FailoverStore.Transition.
func applyAction(cur *FailoverState, action FailoverAction, operator, reason string, nowMs int64) (FailoverState, error) {
	ns, err := nextStatus(cur.Status, action)
	if err != nil {
		return FailoverState{}, err
	}
	return FailoverState{
		SiteID:    cur.SiteID,
		Status:    ns,
		Reason:    reason,
		Operator:  operator,
		Since:     nowMs,
		Version:   cur.Version + 1,
		Timestamp: nowMs,
	}, nil
}
