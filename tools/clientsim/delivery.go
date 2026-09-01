package main

import (
	"encoding/json"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/hmchangw/chat/pkg/model"
)

// skewTolerance absorbs ordinary inter-host clock skew: a slightly-future
// timestamp within this window observes as ~0 latency instead of marking
// the whole run degraded via the invalid-timestamp counter.
const skewTolerance = 2 * time.Second

// deliveredEnvelope is the narrow projection of model.RoomEvent the delivery
// path needs — decoding the full struct would allocate Mentions/Message/
// EncryptedMessage per copy just to read three fields.
type deliveredEnvelope struct {
	Type           model.RoomEventType `json:"type"`
	Timestamp      int64               `json:"timestamp"`
	EventTimestamp int64               `json:"eventTimestamp"`
}

// handleDelivery records one received fan-out copy. The payload is counted,
// its cleartext envelope timestamps observed, and then dropped — never
// stored, never logged (spec §6.4).
func handleDelivery(m *metrics, lane string, data []byte, now time.Time) {
	m.delivered(lane).Inc()

	var evt deliveredEnvelope
	if err := json.Unmarshal(data, &evt); err != nil {
		m.DecodeFailures.Inc()
		return
	}
	// A new_message RoomEvent must carry the broadcast Timestamp; a zero
	// there is a contract violation. Other event types on the same subjects
	// legitimately omit these stamps and just skip observation.
	strict := evt.Type == model.RoomEventNewMessage
	observeAge(m, m.BroadcastLatency, evt.Timestamp, now, strict)
	observeAge(m, m.CanonicalLatency, evt.EventTimestamp, now, false)
}

// observeAge records now - tsMillis. A timestamp within skewTolerance of
// the future observes as zero (clock skew, not corruption); beyond it the
// claimed timestamp counts as invalid. A zero timestamp counts as invalid
// only when the event type requires the stamp (strict).
func observeAge(m *metrics, h prometheus.Histogram, tsMillis int64, now time.Time, strict bool) {
	// Non-positive, not just zero: a negative timestamp measured against the
	// 1970 epoch yields an enormous positive age that would be observed as a
	// real latency sample instead of being counted as the corruption it is.
	if tsMillis <= 0 {
		if strict {
			m.InvalidTimestamp.Inc()
		}
		return
	}
	age := now.Sub(time.UnixMilli(tsMillis))
	switch {
	case age > 0:
		h.Observe(age.Seconds())
	case age > -skewTolerance:
		h.Observe(0)
	default:
		m.InvalidTimestamp.Inc()
	}
}
