package main

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

const failureObserverRecipient failureObserver = "recipient_broadcast"

// failureObserverRoomState reconciles room and member final state. It is one
// observer with two sources — the room-service RPC and an authoritative
// MongoDB primary read — because both answer the same question about the same
// effect; splitting them would make an operation unverified whenever either
// source is down, even though the other one already proved the effect.
const failureObserverRoomState failureObserver = "room_state"

const failureObserverHealthIntervalLimit = 4096

type failureObserverMode string

const (
	failureObserverEvent failureObserverMode = "event"
	failureObserverQuery failureObserverMode = "query"
	failureObserverBoth  failureObserverMode = "both"
)

type failureObserverDefinition struct {
	Name                failureObserver
	Mode                failureObserverMode
	Effects             []failureEffect
	FinalReconciliation bool
}

var failureObserverRegistry = map[failureObserver]failureObserverDefinition{
	failureObserverAdmission: {Name: failureObserverAdmission, Mode: failureObserverEvent, Effects: []failureEffect{failureEffectAdmission}},
	failureObserverHistory:   {Name: failureObserverHistory, Mode: failureObserverQuery, Effects: []failureEffect{failureEffectMessagePersisted}, FinalReconciliation: true},
	failureObserverRecipient: {Name: failureObserverRecipient, Mode: failureObserverEvent, Effects: []failureEffect{failureEffectRecipientEvent}, FinalReconciliation: true},
	failureObserverRoomState: {
		Name: failureObserverRoomState, Mode: failureObserverQuery,
		Effects: []failureEffect{
			failureEffectMemberState, failureEffectRoomName,
			failureEffectSubscriptionMute, failureEffectRoomCreated,
			failureEffectSubscriptionRead,
		},
		FinalReconciliation: true,
	},
}

func validateRegisteredObservers(observers []failureObserver) error {
	seen := make(map[failureObserver]struct{}, len(observers))
	for _, observer := range observers {
		if _, ok := failureObserverRegistry[observer]; !ok {
			return fmt.Errorf("unsupported failure observer %q", observer)
		}
		if _, duplicate := seen[observer]; duplicate {
			return fmt.Errorf("duplicate failure observer %q", observer)
		}
		seen[observer] = struct{}{}
	}
	return nil
}

type failureHealthInterval struct {
	Start  time.Time `json:"start"`
	End    time.Time `json:"end"`
	Up     bool      `json:"up"`
	Reason string    `json:"reason,omitempty"`
}

type failureObserverHealthSnapshot struct {
	Observer             failureObserver         `json:"observer"`
	Up                   bool                    `json:"up"`
	LastSuccess          time.Time               `json:"lastSuccess,omitempty"`
	HistoryTruncated     bool                    `json:"historyTruncated,omitempty"`
	HistoryAvailableFrom time.Time               `json:"historyAvailableFrom,omitempty"`
	Intervals            []failureHealthInterval `json:"intervals"`
}

type failureObserverHealth struct {
	mu                   sync.Mutex
	observer             failureObserver
	up                   bool
	changedAt            time.Time
	reason               string
	lastSuccess          time.Time
	intervals            []failureHealthInterval
	historyTruncated     bool
	historyAvailableFrom time.Time
}

func newFailureObserverHealth(observer failureObserver, startedAt time.Time) *failureObserverHealth {
	return &failureObserverHealth{observer: observer, changedAt: startedAt.UTC(), reason: "startup"}
}

func (h *failureObserverHealth) Set(up bool, at time.Time, reason string) {
	if h == nil {
		return
	}
	at = at.UTC()
	h.mu.Lock()
	defer h.mu.Unlock()
	if at.Before(h.changedAt) {
		return
	}
	if up == h.up {
		if up && at.After(h.lastSuccess) {
			h.lastSuccess = at
		}
		return
	}
	h.intervals = append(h.intervals, failureHealthInterval{Start: h.changedAt, End: at, Up: h.up, Reason: h.reason})
	if len(h.intervals) > failureObserverHealthIntervalLimit {
		removed := h.intervals[0]
		copy(h.intervals, h.intervals[1:])
		h.intervals = h.intervals[:failureObserverHealthIntervalLimit]
		h.historyTruncated = true
		h.historyAvailableFrom = removed.End
	}
	h.up, h.changedAt, h.reason = up, at, reason
	if up {
		h.lastSuccess = at
	}
}

func (h *failureObserverHealth) HealthyThroughout(start, end time.Time) bool {
	snapshot := h.Snapshot(end)
	return failureHealthSnapshotCovers(&snapshot, start, end)
}

func (h *failureObserverHealth) Snapshot(end time.Time) failureObserverHealthSnapshot {
	if h == nil {
		return failureObserverHealthSnapshot{}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	end = end.UTC()
	rawIntervals := append([]failureHealthInterval(nil), h.intervals...)
	if end.After(h.changedAt) || end.Equal(h.changedAt) {
		rawIntervals = append(rawIntervals, failureHealthInterval{Start: h.changedAt, End: end, Up: h.up, Reason: h.reason})
	}
	historyTruncated := h.historyTruncated
	historyAvailableFrom := h.historyAvailableFrom
	if len(rawIntervals) > failureObserverHealthIntervalLimit {
		removed := rawIntervals[:len(rawIntervals)-failureObserverHealthIntervalLimit]
		rawIntervals = rawIntervals[len(rawIntervals)-failureObserverHealthIntervalLimit:]
		historyTruncated = true
		historyAvailableFrom = removed[len(removed)-1].End
	}
	intervals := make([]failureHealthInterval, 0, len(rawIntervals))
	upAtEnd := h.up
	for _, interval := range rawIntervals {
		if !end.Before(interval.Start) && !end.After(interval.End) {
			upAtEnd = interval.Up
		}
		if !interval.Start.Before(end) {
			continue
		}
		if interval.End.After(end) {
			interval.End = end
		}
		if interval.End.After(interval.Start) {
			intervals = append(intervals, interval)
		}
	}
	slices.SortFunc(intervals, func(a, b failureHealthInterval) int { return a.Start.Compare(b.Start) })
	return failureObserverHealthSnapshot{
		Observer: h.observer, Up: upAtEnd, LastSuccess: h.lastSuccess,
		HistoryTruncated: historyTruncated, HistoryAvailableFrom: historyAvailableFrom,
		Intervals: intervals,
	}
}

func failureHealthSnapshotCovers(
	snapshot *failureObserverHealthSnapshot,
	start time.Time,
	end time.Time,
) bool {
	if snapshot == nil || len(snapshot.Intervals) == 0 {
		return false
	}
	if snapshot.HistoryTruncated && start.Before(snapshot.HistoryAvailableFrom) {
		return false
	}
	coveredUntil := start
	for _, interval := range snapshot.Intervals {
		if !interval.End.After(start) || !interval.Start.Before(end) {
			continue
		}
		if !interval.Up || interval.Start.After(coveredUntil) {
			return false
		}
		if interval.End.After(coveredUntil) {
			coveredUntil = interval.End
		}
	}
	return !coveredUntil.Before(end)
}
