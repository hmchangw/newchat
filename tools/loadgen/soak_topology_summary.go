package main

import "math"

// soakTopologySummary is the shape a run actually loaded, as opposed to the
// shape its knobs asked for. The two diverge whenever the site has fewer
// eligible users than SOAK_MAX_USERS, and nothing reported the difference —
// so a run could measure a workload nobody chose and look configured doing it.
type soakTopologySummary struct {
	BorrowedUsers int     `json:"borrowedUsers"`
	ActiveUsers   int     `json:"activeUsers"`
	Rooms         int     `json:"rooms"`
	Subscriptions int     `json:"subscriptions"`
	SubsPerUser   float64 `json:"subsPerUser"`
	// MessagesPerActiveUserPerDay is the derived rate a human would have to
	// sustain to produce SOAK_SEND_RATE. It is the one number that says
	// whether the load is shaped like a chat product or only sized like one.
	MessagesPerActiveUserPerDay float64 `json:"messagesPerActiveUserPerDay"`
}

// summarizeSoakTopology derives the run's effective shape. sendRate is the
// configured per-second send rate; a zero or absent denominator yields 0
// rather than an infinity that would render as null in the log.
func summarizeSoakTopology(topology *soakTopology, sendRate float64) soakTopologySummary {
	if topology == nil {
		return soakTopologySummary{}
	}
	summary := soakTopologySummary{
		BorrowedUsers: len(topology.BorrowedUsers),
		ActiveUsers:   len(topology.ActiveUsers),
		Rooms:         len(topology.Rooms),
		Subscriptions: len(topology.Subscriptions),
	}
	if summary.BorrowedUsers > 0 {
		summary.SubsPerUser = round2(
			float64(summary.Subscriptions) / float64(summary.BorrowedUsers),
		)
	}
	if summary.ActiveUsers > 0 && sendRate > 0 {
		summary.MessagesPerActiveUserPerDay = round2(
			sendRate * secondsPerDay / float64(summary.ActiveUsers),
		)
	}
	return summary
}

const secondsPerDay = 24 * 60 * 60

// round2 keeps the derived rates readable in a log line; the third decimal of
// a messages-per-day estimate carries no information.
func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

// LogValues renders the summary as slog key/value pairs.
func (s soakTopologySummary) LogValues() []any {
	return []any{
		"borrowed_users", s.BorrowedUsers,
		"active_users", s.ActiveUsers,
		"rooms", s.Rooms,
		"subscriptions", s.Subscriptions,
		"subs_per_user", s.SubsPerUser,
		"messages_per_active_user_per_day", s.MessagesPerActiveUserPerDay,
	}
}
