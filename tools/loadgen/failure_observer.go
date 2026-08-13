package main

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

const failureObserverRecipient failureObserver = "recipient_broadcast"

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
	Observer    failureObserver         `json:"observer"`
	Up          bool                    `json:"up"`
	LastSuccess time.Time               `json:"lastSuccess,omitempty"`
	Intervals   []failureHealthInterval `json:"intervals"`
}

type failureObserverHealth struct {
	mu          sync.Mutex
	observer    failureObserver
	up          bool
	changedAt   time.Time
	reason      string
	lastSuccess time.Time
	intervals   []failureHealthInterval
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
	h.up, h.changedAt, h.reason = up, at, reason
	if up {
		h.lastSuccess = at
	}
}

func (h *failureObserverHealth) HealthyThroughout(start, end time.Time) bool {
	snapshot := h.Snapshot(end)
	for _, interval := range snapshot.Intervals {
		if interval.End.After(start) && interval.Start.Before(end) && !interval.Up {
			return false
		}
	}
	return true
}

func (h *failureObserverHealth) Snapshot(end time.Time) failureObserverHealthSnapshot {
	if h == nil {
		return failureObserverHealthSnapshot{}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	intervals := append([]failureHealthInterval(nil), h.intervals...)
	if end.After(h.changedAt) || end.Equal(h.changedAt) {
		intervals = append(intervals, failureHealthInterval{Start: h.changedAt, End: end.UTC(), Up: h.up, Reason: h.reason})
	}
	slices.SortFunc(intervals, func(a, b failureHealthInterval) int { return a.Start.Compare(b.Start) })
	return failureObserverHealthSnapshot{Observer: h.observer, Up: h.up, LastSuccess: h.lastSuccess, Intervals: intervals}
}
