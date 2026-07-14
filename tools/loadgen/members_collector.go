package main

import (
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
)

// MemberCollector correlates frontdoor replies (E1) and member_added
// events (E2) with publish times. Thread-safe.
//
// E1 key:  corrID (assigned by publisher per request)
// E2 key:  roomID + "|" + sortedJoin(accounts)
//
// E2 keying works because the candidate-pool fixture guarantees that
// concurrent requests against the same room never share user accounts.
type MemberCollector struct {
	m             *Metrics
	preset        string
	inject        InjectMode
	mu            sync.Mutex
	byCorr        map[string]memberPubEntry
	byE2Key       map[string]memberPubEntry
	e1            []sample
	e2            []sample
	rsErrs        int
	onMemberEvent func(roomID string, accounts []string)
}

type memberPubEntry struct {
	publishedAt time.Time
}

// NewMemberCollector returns a ready-to-use MemberCollector.
func NewMemberCollector(m *Metrics, preset string, inject InjectMode) *MemberCollector {
	return &MemberCollector{
		m: m, preset: preset, inject: inject,
		byCorr:  make(map[string]memberPubEntry),
		byE2Key: make(map[string]memberPubEntry),
	}
}

// RecordPublish stores publish time under both correlation keys.
func (c *MemberCollector) RecordPublish(corrID, roomID string, accounts []string, t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := memberPubEntry{publishedAt: t}
	if corrID != "" {
		c.byCorr[corrID] = e
	}
	c.byE2Key[e2Key(roomID, accounts)] = e
}

// RecordPublishFailed undoes a prior RecordPublish (call when the publish
// itself errored after we recorded it).
func (c *MemberCollector) RecordPublishFailed(corrID, roomID string, accounts []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if corrID != "" {
		delete(c.byCorr, corrID)
	}
	delete(c.byE2Key, e2Key(roomID, accounts))
}

// RecordReply matches a frontdoor reply by corrID. If body contains a non-empty
// "error" field, counts a room_service error but still records the E1 latency.
func (c *MemberCollector) RecordReply(corrID, body string, at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.byCorr[corrID]
	if !ok {
		return
	}
	delete(c.byCorr, corrID)
	d := at.Sub(e.publishedAt)
	c.e1 = append(c.e1, sample{publishedAt: e.publishedAt, latency: d})
	c.m.MemberE1Latency.WithLabelValues(c.preset, string(c.inject)).Observe(d.Seconds())

	if body != "" {
		var parsed struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal([]byte(body), &parsed); err == nil && parsed.Error != "" {
			c.rsErrs++
			c.m.MemberPublishErrors.WithLabelValues("room_service").Inc()
		}
	}
}

// RecordMemberEvent matches a member_added event by (roomID, sortedAccounts).
func (c *MemberCollector) RecordMemberEvent(roomID string, accounts []string, at time.Time) {
	c.mu.Lock()
	k := e2Key(roomID, accounts)
	e, ok := c.byE2Key[k]
	if !ok {
		c.mu.Unlock()
		return
	}
	delete(c.byE2Key, k)
	d := at.Sub(e.publishedAt)
	c.e2 = append(c.e2, sample{publishedAt: e.publishedAt, latency: d})
	c.m.MemberE2Latency.WithLabelValues(c.preset, string(c.inject)).Observe(d.Seconds())
	cb := c.onMemberEvent
	c.mu.Unlock()
	if cb != nil {
		cb(roomID, accounts)
	}
}

// Finalize returns counts of unmatched publishes — replies and member events.
func (c *MemberCollector) Finalize() (missingReplies int, missingEvents int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.byCorr), len(c.byE2Key)
}

// E1Count returns the number of matched E1 (reply) samples.
func (c *MemberCollector) E1Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.e1)
}

// E2Count returns the number of matched E2 (member-event) samples.
func (c *MemberCollector) E2Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.e2)
}

// RoomServiceErrorCount returns how many replies carried a non-empty error field.
func (c *MemberCollector) RoomServiceErrorCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rsErrs
}

// OnMemberEvent registers a callback fired after every matched member event,
// allowing the capacity generator to step its per-room loop.
func (c *MemberCollector) OnMemberEvent(fn func(roomID string, accounts []string)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onMemberEvent = fn
}

// DiscardBefore drops samples with publishedAt < cutoff (warmup pruning).
func (c *MemberCollector) DiscardBefore(cutoff time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.e1 = filterAtOrAfter(c.e1, cutoff)
	c.e2 = filterAtOrAfter(c.e2, cutoff)
}

// E1Samples returns a sorted copy of E1 latencies.
func (c *MemberCollector) E1Samples() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return snapshotLatencies(c.e1)
}

// E2Samples returns a sorted copy of E2 latencies.
func (c *MemberCollector) E2Samples() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return snapshotLatencies(c.e2)
}

// snapshotLatencies copies and sorts latencies from in. Shared by Collector
// and MemberCollector; callers must already hold the collector's mutex since
// in aliases the live sample slice.
func snapshotLatencies(in []sample) []time.Duration {
	out := make([]time.Duration, len(in))
	for i := range in {
		out[i] = in[i].latency
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func e2Key(roomID string, accounts []string) string {
	sorted := append([]string(nil), accounts...)
	sort.Strings(sorted)
	return roomID + "|" + strings.Join(sorted, ",")
}

// ParseMemberAddEvent decodes a model.MemberAddEvent payload and returns
// (roomID, accounts, true) when the event is type=member_added. Returns
// (_, _, false) on malformed input or non-added event types.
func ParseMemberAddEvent(data []byte) (string, []string, bool) {
	var evt model.MemberAddEvent
	if err := json.Unmarshal(data, &evt); err != nil {
		return "", nil, false
	}
	if evt.Type != "member_added" {
		return "", nil, false
	}
	return evt.RoomID, evt.Accounts, true
}
