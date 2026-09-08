package failure

import (
	"slices"
	"sync"
	"time"
)

const ObserverHealthIntervalLimit = 4096

type HealthInterval struct {
	Start  time.Time `json:"start"`
	End    time.Time `json:"end"`
	Up     bool      `json:"up"`
	Reason string    `json:"reason,omitempty"`
}

type ObserverHealthSnapshot struct {
	Observer             Observer         `json:"observer"`
	Up                   bool             `json:"up"`
	LastSuccess          time.Time        `json:"lastSuccess,omitempty"`
	HistoryTruncated     bool             `json:"historyTruncated,omitempty"`
	HistoryAvailableFrom time.Time        `json:"historyAvailableFrom,omitempty"`
	Intervals            []HealthInterval `json:"intervals"`
}

type ObserverHealth struct {
	mu                   sync.Mutex
	observer             Observer
	up                   bool
	changedAt            time.Time
	reason               string
	lastSuccess          time.Time
	intervals            []HealthInterval
	historyTruncated     bool
	historyAvailableFrom time.Time
}

func NewObserverHealth(observer Observer, startedAt time.Time) *ObserverHealth {
	return &ObserverHealth{observer: observer, changedAt: startedAt.UTC(), reason: "startup"}
}

func (h *ObserverHealth) Set(up bool, at time.Time, reason string) {
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
	h.intervals = append(h.intervals, HealthInterval{Start: h.changedAt, End: at, Up: h.up, Reason: h.reason})
	if len(h.intervals) > ObserverHealthIntervalLimit {
		removed := h.intervals[0]
		copy(h.intervals, h.intervals[1:])
		h.intervals = h.intervals[:ObserverHealthIntervalLimit]
		h.historyTruncated = true
		h.historyAvailableFrom = removed.End
	}
	h.up, h.changedAt, h.reason = up, at, reason
	if up {
		h.lastSuccess = at
	}
}

func (h *ObserverHealth) HealthyThroughout(start, end time.Time) bool {
	snapshot := h.Snapshot(end)
	return HealthSnapshotCovers(&snapshot, start, end)
}

func (h *ObserverHealth) Snapshot(end time.Time) ObserverHealthSnapshot {
	if h == nil {
		return ObserverHealthSnapshot{}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	end = end.UTC()
	rawIntervals := slices.Clone(h.intervals)
	if end.After(h.changedAt) || end.Equal(h.changedAt) {
		rawIntervals = append(rawIntervals, HealthInterval{Start: h.changedAt, End: end, Up: h.up, Reason: h.reason})
	}
	historyTruncated := h.historyTruncated
	historyAvailableFrom := h.historyAvailableFrom
	if len(rawIntervals) > ObserverHealthIntervalLimit {
		removed := rawIntervals[:len(rawIntervals)-ObserverHealthIntervalLimit]
		rawIntervals = rawIntervals[len(rawIntervals)-ObserverHealthIntervalLimit:]
		historyTruncated = true
		historyAvailableFrom = removed[len(removed)-1].End
	}
	intervals := make([]HealthInterval, 0, len(rawIntervals))
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
	slices.SortFunc(intervals, func(a, b HealthInterval) int { return a.Start.Compare(b.Start) })
	return ObserverHealthSnapshot{
		Observer: h.observer, Up: upAtEnd, LastSuccess: h.lastSuccess,
		HistoryTruncated: historyTruncated, HistoryAvailableFrom: historyAvailableFrom,
		Intervals: intervals,
	}
}

func HealthSnapshotCovers(snapshot *ObserverHealthSnapshot, start, end time.Time) bool {
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
