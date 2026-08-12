package mongorepo

import "go.mongodb.org/mongo-driver/v2/mongo/readpref"

// Option customizes repo construction.
type Option func(*settings)

type settings struct {
	readPref      *readpref.ReadPref
	showTeamsRoom bool
}

// WithReadPreference routes each repo's staleness-tolerant reads to rp; authz,
// dedup, read-after-write reads, and writes keep the default. Nil is a no-op.
func WithReadPreference(rp *readpref.ReadPref) Option {
	return func(s *settings) { s.readPref = rp }
}

// WithShowTeamsRoom controls whether SubscriptionRepo's list/count queries
// include Teams-migrated rooms (origin "teams"); false (the default) excludes
// them. Only consumed by SubscriptionRepo.
func WithShowTeamsRoom(show bool) Option {
	return func(s *settings) { s.showTeamsRoom = show }
}

func applyOptions(opts []Option) settings {
	var s settings
	for _, o := range opts {
		o(&s)
	}
	return s
}
