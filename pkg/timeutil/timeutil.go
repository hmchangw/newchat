// Package timeutil converts between nullable epoch-millis and *time.Time at the
// wire boundary: cross-language RPC payloads carry epoch millis (*int64), while
// the client wire and Mongo carry RFC3339 *time.Time. nil maps to nil so an
// absent timestamp stays absent.
package timeutil

import "time"

// MillisToTime converts nullable epoch millis to a nullable UTC timestamp.
func MillisToTime(ms *int64) *time.Time {
	if ms == nil {
		return nil
	}
	t := time.UnixMilli(*ms).UTC()
	return &t
}

// TimeToMillis converts a nullable timestamp to nullable epoch millis (UTC).
func TimeToMillis(t *time.Time) *int64 {
	if t == nil {
		return nil
	}
	ms := t.UTC().UnixMilli()
	return &ms
}
